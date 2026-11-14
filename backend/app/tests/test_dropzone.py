from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.db as db
import app.services.dropzone_service as dz
from app.main import app
from app.seed import init_db

@pytest.fixture()
def client(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setattr(db, "DATA_DIR", data_dir)
    monkeypatch.setattr(db, "DB_PATH", data_dir / "app.db")
    monkeypatch.setattr(dz, "DATA_DIR", data_dir)
    init_db()
    return TestClient(app)


def _drop(client, name: str, content: str | None = "x") -> Path:
    cfg = client.get("/api/dropzone/config").json()
    d = Path(cfg["resolved_dir"])
    d.mkdir(parents=True, exist_ok=True)
    p = d / name
    p.write_text(content if content is not None else "", encoding="utf-8")
    return p


def test_scan_registers_valid_invalid_and_dedupes(client):
    _drop(client, "orders_2026-11-12.json", '{"signal": true}')
    _drop(client, "empty.json", "")
    _drop(client, "notes.txt", "not an allowed ext")

    r = client.post("/api/dropzone/scan").json()
    assert r["new_pending"] == 1
    assert r["new_invalid"] == 2
    assert r["total_files"] == 3

    # 再扫一遍：同一文件不得重复登记
    r2 = client.post("/api/dropzone/scan").json()
    assert r2["new_pending"] == 0 and r2["new_invalid"] == 0
    assert r2["already_known"] == 3

    batches = client.get("/api/dropzone/batches").json()
    assert len(batches) == 3
    by_name = {b["filename"]: b for b in batches}
    assert by_name["orders_2026-11-12.json"]["status"] == "pending"
    assert by_name["empty.json"]["status"] == "invalid"
    assert "空文件" in by_name["empty.json"]["status_note"]
    assert by_name["notes.txt"]["status"] == "invalid"
    assert all(b["discovered_at"] for b in batches)

    rec = client.get("/api/dropzone/reconcile").json()
    assert rec["ok"]
    assert rec["discovered"] == 3
    assert rec["pending"] == 1 and rec["invalid"] == 2


def test_confirm_trigger_launches_orchestration_and_blocks_duplicates(client):
    _drop(client, "go.json", '{"signal": true}')
    client.post("/api/dropzone/scan")
    batch = client.get("/api/dropzone/batches").json()[0]

    r = client.post(f"/api/dropzone/batches/{batch['id']}/trigger")
    assert r.status_code == 200
    body = r.json()
    assert body["batch"]["status"] == "triggered"
    assert body["batch"]["trigger_count"] == 1
    orch = body["orchestration"]
    assert orch["source"] == "dropzone"
    assert orch["trigger_batch_id"] == batch["id"]
    # 编排侧对账：成功+失败+跳过 == 作业数 == 尝试数
    assert orch["report"]["reconcile_ok"]
    assert orch["report"]["succeeded"] + orch["report"]["failed"] + orch["report"]["skipped"] == 3

    # 同一文件不得默默再开一趟
    r2 = client.post(f"/api/dropzone/batches/{batch['id']}/trigger")
    assert r2.status_code == 409

    # 显式重触发：允许，但必须留痕
    r3 = client.post(f"/api/dropzone/batches/{batch['id']}/retrigger")
    assert r3.status_code == 200
    assert r3.json()["batch"]["trigger_count"] == 2

    events = client.get(f"/api/dropzone/batches/{batch['id']}/events").json()
    names = [e["event"] for e in events]
    assert names == ["registered", "triggered", "retriggered"]
    assert events[2]["detail"]["previous_orchestration_id"] == orch["id"]

    rec = client.get("/api/dropzone/reconcile").json()
    assert rec["ok"]
    assert rec["triggered"] == 1
    assert rec["trigger_slots"] == 2 == rec["dropzone_orchestrations"]


def test_invalid_and_ignored_cannot_trigger(client):
    _drop(client, "bad.txt", "nope")
    _drop(client, "skip_me.json", '{"signal": true}')
    client.post("/api/dropzone/scan")
    batches = {b["filename"]: b for b in client.get("/api/dropzone/batches").json()}

    bad = batches["bad.txt"]
    assert bad["status"] == "invalid"
    assert client.post(f"/api/dropzone/batches/{bad['id']}/trigger").status_code == 409

    skip = batches["skip_me.json"]
    r = client.post(f"/api/dropzone/batches/{skip['id']}/ignore")
    assert r.status_code == 200 and r.json()["status"] == "ignored"
    assert client.post(f"/api/dropzone/batches/{skip['id']}/trigger").status_code == 409

    rec = client.get("/api/dropzone/reconcile").json()
    assert rec["ok"]
    assert rec["ignored"] == 1 and rec["invalid"] == 1
    assert rec["triggered"] == 0


def test_confirm_rejected_when_file_gone(client):
    p = _drop(client, "gone.json", '{"signal": true}')
    client.post("/api/dropzone/scan")
    batch = client.get("/api/dropzone/batches").json()[0]
    p.unlink()

    r = client.post(f"/api/dropzone/batches/{batch['id']}/trigger")
    assert r.status_code == 409
    batch = client.get("/api/dropzone/batches").json()[0]
    assert batch["status"] == "invalid"
    assert "不在磁盘" in batch["status_note"]


def test_config_update_and_scan_creates_dir(client, tmp_path):
    r = client.put("/api/dropzone/config", json={"dir": "inbox"})
    assert r.status_code == 200
    cfg = r.json()
    assert cfg["dir_setting"] == "inbox"
    assert cfg["resolved_dir"].endswith("inbox")

    scan = client.post("/api/dropzone/scan").json()
    assert scan["created_dir"] is True
    assert Path(scan["dir"]).is_dir()

    assert client.put("/api/dropzone/config", json={"dir": "  "}).status_code == 422


def _orchestrations(client):
    return client.get("/api/orchestrations").json()


def test_one_trigger_launches_exactly_one_run_with_correct_link(client):
    """一次确认只开一趟编排，批次挂的运行号必须就是这趟，且指回本批次。"""
    _drop(client, "once.json", '{"signal": true}')
    client.post("/api/dropzone/scan")
    batch = client.get("/api/dropzone/batches").json()[0]

    r = client.post(f"/api/dropzone/batches/{batch['id']}/trigger")
    assert r.status_code == 200
    body = r.json()
    orch_id = body["orchestration"]["id"]

    # 只创建了一趟 dropzone 编排
    orchs = _orchestrations(client)
    dz_orchs = [o for o in orchs if o["source"] == "dropzone"]
    assert len(dz_orchs) == 1
    # 批次挂的运行号 == 实际拉开的那趟，且编排侧指回同一批次
    refreshed = {b["id"]: b for b in client.get("/api/dropzone/batches").json()}[batch["id"]]
    assert refreshed["orchestration_id"] == orch_id
    assert dz_orchs[0]["id"] == orch_id
    assert dz_orchs[0]["trigger_batch_id"] == batch["id"]
    assert refreshed["trigger_count"] == 1


def test_retrigger_on_triggered_is_blocked_but_explicit_retrigger_audited(client):
    """已触发默认禁止再开；显式重触发每次只开一趟、次数单列、可审计。"""
    _drop(client, "again.json", '{"signal": true}')
    client.post("/api/dropzone/scan")
    batch = client.get("/api/dropzone/batches").json()[0]
    first = client.post(f"/api/dropzone/batches/{batch['id']}/trigger").json()["orchestration"]

    # 普通确认触发对已触发批次必须 409，且不产生新编排
    assert client.post(f"/api/dropzone/batches/{batch['id']}/trigger").status_code == 409
    assert len([o for o in _orchestrations(client) if o["source"] == "dropzone"]) == 1

    # 显式重触发成功：只多一趟，次数变 2，挂的是新运行号
    r = client.post(f"/api/dropzone/batches/{batch['id']}/retrigger")
    assert r.status_code == 200
    second = r.json()["orchestration"]
    assert second["id"] != first["id"]
    assert r.json()["batch"]["orchestration_id"] == second["id"]
    assert r.json()["batch"]["trigger_count"] == 2

    dz_orchs = [o for o in _orchestrations(client) if o["source"] == "dropzone"]
    assert len(dz_orchs) == 2
    assert {o["id"] for o in dz_orchs} == {first["id"], second["id"]}
    # 重触发事件可审计，记录了上一趟运行号
    events = client.get(f"/api/dropzone/batches/{batch['id']}/events").json()
    last = events[-1]
    assert last["event"] == "retriggered"
    assert last["detail"]["orchestration_id"] == second["id"]
    assert last["detail"]["previous_orchestration_id"] == first["id"]

    rec = client.get("/api/dropzone/reconcile").json()
    assert rec["ok"]
    assert rec["trigger_slots"] == 2 == rec["dropzone_orchestrations"]
    assert rec["mismatched_links"] == 0 and rec["dangling_orchestrations"] == 0


def test_invalid_batch_cannot_trigger_and_creates_no_run(client):
    """无效批次确认触发必须 409，且绝不拉起编排/不叠水位。"""
    _drop(client, "bad.txt", "nope")
    client.post("/api/dropzone/scan")
    batch = client.get("/api/dropzone/batches").json()[0]
    assert batch["status"] == "invalid"

    assert client.post(f"/api/dropzone/batches/{batch['id']}/trigger").status_code == 409
    assert [o for o in _orchestrations(client) if o["source"] == "dropzone"] == []
    # 重触发同样不允许
    assert client.post(f"/api/dropzone/batches/{batch['id']}/retrigger").status_code == 409
    assert [o for o in _orchestrations(client) if o["source"] == "dropzone"] == []


def test_ignore_does_not_launch_orchestration(client):
    """忽略只是人工处置，不得偷偷开编排。"""
    _drop(client, "skip.json", '{"signal": true}')
    client.post("/api/dropzone/scan")
    batch = client.get("/api/dropzone/batches").json()[0]

    r = client.post(f"/api/dropzone/batches/{batch['id']}/ignore")
    assert r.status_code == 200
    assert [o for o in _orchestrations(client) if o["source"] == "dropzone"] == []
    rec = client.get("/api/dropzone/reconcile").json()
    assert rec["ok"]
    assert rec["ignored"] == 1 and rec["trigger_slots"] == 0


def test_reconcile_counts_match_list_and_links_across_mixed_batches(client):
    """多状态混合：对账计数必须与批次列表、关联运行完全一致。"""
    _drop(client, "a.json", '{"signal": true}')
    _drop(client, "b.json", '{"signal": true}')
    _drop(client, "empty.json", "")
    _drop(client, "note.txt", "x")
    client.post("/api/dropzone/scan")
    batches = {b["filename"]: b for b in client.get("/api/dropzone/batches").json()}

    client.post(f"/api/dropzone/batches/{batches['a.json']['id']}/trigger")
    client.post(f"/api/dropzone/batches/{batches['note.txt']['id']}/ignore")
    # b.json 保持 pending，empty.json 保持 invalid

    rec = client.get("/api/dropzone/reconcile").json()
    assert rec["ok"]
    assert rec["discovered"] == 4
    assert rec["pending"] == 1
    assert rec["triggered"] == 1
    assert rec["ignored"] == 1
    assert rec["invalid"] == 1
    assert rec["failed"] == 0
    assert rec["valid"] == 2  # pending + triggered
    assert rec["trigger_slots"] == 1 == rec["dropzone_orchestrations"]
    assert rec["linked_triggered"] == 1
    assert rec["dangling_orchestrations"] == 0
    assert rec["mismatched_links"] == 0

    # 总览接口同口径
    dash = client.get("/api/dashboard").json()
    assert dash["dropzone_discovered"] == 4
    assert dash["dropzone_valid"] == 2
    assert dash["dropzone_batches"]["invalid"] == 1


def test_blocked_reoperation_does_not_create_second_run_or_watermarks(client):
    """对已触发文件再操作被拒后，不得新增编排运行；水位行数保持单趟结果（不叠第二套）。"""
    _drop(client, "wm.json", '{"signal": true}')
    client.post("/api/dropzone/scan")
    batch = client.get("/api/dropzone/batches").json()[0]
    client.post(f"/api/dropzone/batches/{batch['id']}/trigger")

    def counts():
        orchs = [o for o in _orchestrations(client) if o["source"] == "dropzone"]
        wm = client.get("/api/watermarks").json()
        watermarked = [w for w in wm if w["watermark_ts"]]
        return len(orchs), len(watermarked)

    runs_before, wm_rows_before = counts()
    assert runs_before == 1 and wm_rows_before == 3

    # 再次确认触发被 409 拒绝
    assert client.post(f"/api/dropzone/batches/{batch['id']}/trigger").status_code == 409
    assert counts() == (runs_before, wm_rows_before)

    # 显式重触发会另开一趟（可审计分列），但仍是每趟一套水位、按 job 归并不新增行
    r = client.post(f"/api/dropzone/batches/{batch['id']}/retrigger")
    assert r.status_code == 200
    runs_after, wm_rows_after = counts()
    assert runs_after == runs_before + 1
    assert wm_rows_after == wm_rows_before


def test_linked_run_never_points_to_another_file(client):
    """两个文件各触发一次，各自挂的运行号不能串号。"""
    _drop(client, "x.json", '{"signal": true}')
    _drop(client, "y.json", '{"signal": true}')
    client.post("/api/dropzone/scan")
    batches = {b["filename"]: b for b in client.get("/api/dropzone/batches").json()}

    rx = client.post(f"/api/dropzone/batches/{batches['x.json']['id']}/trigger").json()
    ry = client.post(f"/api/dropzone/batches/{batches['y.json']['id']}/trigger").json()
    assert rx["orchestration"]["id"] != ry["orchestration"]["id"]

    linked = {b["filename"]: b["orchestration_id"] for b in client.get("/api/dropzone/batches").json()}
    assert linked["x.json"] == rx["orchestration"]["id"]
    assert linked["y.json"] == ry["orchestration"]["id"]

    rec = client.get("/api/dropzone/reconcile").json()
    assert rec["ok"] and rec["mismatched_links"] == 0
