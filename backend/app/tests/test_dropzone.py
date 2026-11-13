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
