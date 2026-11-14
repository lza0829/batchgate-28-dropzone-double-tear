"""落盘目录扫描 → 触发批次登记 → 确认开编排。

文件只是触发信号：只记录文件名/大小/发现时间，绝不读取内容入库，
更不做业务键归并（那是 Feedstitch 的活，这里明确不碰）。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from app.config import DATA_DIR
from app.db import connect
from app.repositories import dropzone as dz_repo
from app.services.orchestrate_service import run_orchestration

BATCH_STATUSES = ("pending", "triggered", "ignored", "invalid", "failed")


class DropzoneError(Exception):
    """业务校验失败（目录未配、批次状态不允许该操作等）。"""

    def __init__(self, message: str, status_code: int = 409):
        super().__init__(message)
        self.status_code = status_code


def _get_setting(conn, key: str, default: str = "") -> str:
    r = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return r["value"] if r else default


def resolve_drop_dir(conn) -> Path:
    """相对路径基于 DATA_DIR（本地即工作区 backend/data/）解析；绝对路径原样用。"""
    raw = _get_setting(conn, "dropzone_dir", "dropzone").strip() or "dropzone"
    p = Path(raw)
    return p if p.is_absolute() else (DATA_DIR / p)


def _allowed_extensions(conn) -> set[str]:
    raw = _get_setting(conn, "dropzone_extensions", ".json")
    return {e.strip().lower() for e in raw.split(",") if e.strip()}


def get_config() -> dict[str, Any]:
    conn = connect()
    try:
        drop_dir = resolve_drop_dir(conn)
        files = [p for p in drop_dir.iterdir() if p.is_file()] if drop_dir.is_dir() else []
        return {
            "dir_setting": _get_setting(conn, "dropzone_dir", "dropzone"),
            "resolved_dir": str(drop_dir),
            "extensions": sorted(_allowed_extensions(conn)),
            "exists": drop_dir.is_dir(),
            "files_on_disk": len(files),
        }
    finally:
        conn.close()


def save_config(dir_value: str) -> dict[str, Any]:
    dir_value = dir_value.strip()
    if not dir_value:
        raise DropzoneError("目录不能为空", 422)
    conn = connect()
    try:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES ('dropzone_dir', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (dir_value,),
        )
        conn.commit()
    finally:
        conn.close()
    return get_config()


def _validate_file(path: Path, allowed: set[str]) -> str | None:
    """返回 None 表示可登记为待确认；否则返回无效原因。"""
    if path.suffix.lower() not in allowed:
        return f"扩展名 {path.suffix or '(无)'} 不在允许列表 {sorted(allowed)}"
    if path.stat().st_size == 0:
        return "空文件"
    return None


def scan_dropzone() -> dict[str, Any]:
    """扫目录：新文件登记批次（pending/invalid），已登记的按 (目录,文件名) 去重不再登记。"""
    conn = connect()
    try:
        drop_dir = resolve_drop_dir(conn)
        created_dir = not drop_dir.is_dir()
        drop_dir.mkdir(parents=True, exist_ok=True)
        allowed = _allowed_extensions(conn)
        dir_key = str(drop_dir)

        report: dict[str, Any] = {
            "dir": dir_key,
            "created_dir": created_dir,
            "total_files": 0,
            "new_pending": 0,
            "new_invalid": 0,
            "already_known": 0,
            "skipped_nonfiles": 0,
            "missing_from_disk": 0,
        }
        seen: set[str] = set()
        for path in sorted(drop_dir.iterdir()):
            if not path.is_file() or path.name.startswith("."):
                report["skipped_nonfiles"] += 1
                continue
            report["total_files"] += 1
            seen.add(path.name)
            if dz_repo.find_batch(conn, dir_key, path.name):
                report["already_known"] += 1
                continue
            reason = _validate_file(path, allowed)
            status = "pending" if reason is None else "invalid"
            batch_id = dz_repo.insert_batch(
                conn,
                drop_dir=dir_key,
                filename=path.name,
                size_bytes=path.stat().st_size,
                status=status,
                status_note=reason,
            )
            dz_repo.add_event(
                conn,
                batch_id,
                "registered",
                {"status": status, "size_bytes": path.stat().st_size, "note": reason},
            )
            report["new_pending" if status == "pending" else "new_invalid"] += 1

        for b in dz_repo.list_batches(conn):
            if b["drop_dir"] == dir_key and b["filename"] not in seen:
                if not (drop_dir / b["filename"]).is_file():
                    report["missing_from_disk"] += 1
        conn.commit()
        return report
    finally:
        conn.close()


def _load_batch_or_404(conn, batch_id: int) -> dict[str, Any]:
    batch = dz_repo.get_batch(conn, batch_id)
    if not batch:
        raise DropzoneError(f"批次 #{batch_id} 不存在", 404)
    return batch


def _file_path(batch: dict[str, Any]) -> Path:
    return Path(batch["drop_dir"]) / batch["filename"]


# 只有这两个状态允许（首次）确认触发；triggered/invalid/ignored 默认禁止再开运行，
# triggered 确需重开必须走显式 retrigger，次数单列、事件留痕。
_TRIGGERABLE_STATUSES = ("pending", "failed")
_RETRIGGERABLE_STATUSES = ("triggered",)


def confirm_trigger(batch_id: int) -> dict[str, Any]:
    """确认一个批次并开一趟编排。

    已触发/无效/已忽略批次一律拒绝（409），避免同一文件默默拉起第二趟编排、
    叠出第二套水位；已触发批次只能走 retrigger 显式重触发。
    """
    conn = connect()
    try:
        batch = _load_batch_or_404(conn, batch_id)
        if batch["status"] not in _TRIGGERABLE_STATUSES:
            raise DropzoneError(
                f"批次 #{batch_id} 当前状态为 {batch['status']}，不可确认触发"
                "（已触发批次请走显式重触发）"
            )
        if not _file_path(batch).is_file():
            dz_repo.update_batch_status(
                conn, batch_id, status="invalid", status_note="确认时文件已不在磁盘上"
            )
            dz_repo.add_event(conn, batch_id, "confirm_rejected", {"reason": "file_missing"})
            conn.commit()
            raise DropzoneError(f"批次 #{batch_id} 的文件已不在磁盘上，已标记 invalid")
        conn.commit()
    finally:
        conn.close()
    return _launch(batch_id, retrigger=False)


def retrigger(batch_id: int) -> dict[str, Any]:
    """显式重触发：只允许已触发批次，次数 +1 并留审计事件，每次只开一趟编排。"""
    conn = connect()
    try:
        batch = _load_batch_or_404(conn, batch_id)
        if batch["status"] not in _RETRIGGERABLE_STATUSES:
            raise DropzoneError(
                f"批次 #{batch_id} 当前状态为 {batch['status']}，只有 triggered 可重触发"
            )
        if not _file_path(batch).is_file():
            raise DropzoneError(f"批次 #{batch_id} 的文件已不在磁盘上，无法重触发")
    finally:
        conn.close()
    return _launch(batch_id, retrigger=True)


def _launch(batch_id: int, *, retrigger: bool) -> dict[str, Any]:
    """一次调用只开一趟编排，并把该趟真实运行号挂回本批次。"""
    conn = connect()
    try:
        batch = _load_batch_or_404(conn, batch_id)
        # 防御性二次门禁：编排启动前再确认状态，杜绝同批次并发/重复开跑。
        allowed = _RETRIGGERABLE_STATUSES if retrigger else _TRIGGERABLE_STATUSES
        if batch["status"] not in allowed:
            raise DropzoneError(
                f"批次 #{batch_id} 当前状态为 {batch['status']}，禁止启动编排"
            )
        previous_orchestration_id = batch["orchestration_id"]
    finally:
        conn.close()

    # 关键约束：无论首次触发还是重触发，一次操作只拉起一趟编排运行。
    try:
        orch = run_orchestration(source="dropzone", trigger_batch_id=batch_id)
    except Exception as exc:  # noqa: BLE001
        conn = connect()
        try:
            if retrigger:
                dz_repo.update_batch_status(
                    conn, batch_id, status="triggered", status_note=f"重触发失败: {exc}"
                )
            else:
                dz_repo.update_batch_status(
                    conn, batch_id, status="failed", status_note=f"编排启动失败: {exc}"
                )
            dz_repo.add_event(
                conn, batch_id, "trigger_failed", {"error": str(exc), "retrigger": retrigger}
            )
            conn.commit()
        finally:
            conn.close()
        raise DropzoneError(f"批次 #{batch_id} 触发编排失败: {exc}", 500) from exc

    conn = connect()
    try:
        # 只绑定本次真实拉开的运行号，绝不指向别的批次/别的文件的编排。
        linked_id = orch["id"]
        dz_repo.update_batch_status(
            conn,
            batch_id,
            status="triggered",
            status_note=None,
            orchestration_id=linked_id,
            bump_trigger_count=True,
        )
        dz_repo.add_event(
            conn,
            batch_id,
            "retriggered" if retrigger else "triggered",
            {
                "orchestration_id": linked_id,
                "previous_orchestration_id": previous_orchestration_id,
                "orchestration_status": orch["status"],
            },
        )
        conn.commit()
        batch = dz_repo.get_batch(conn, batch_id)
    finally:
        conn.close()
    return {"batch": batch, "orchestration": orch}


def ignore_batch(batch_id: int) -> dict[str, Any]:
    conn = connect()
    try:
        batch = _load_batch_or_404(conn, batch_id)
        if batch["status"] not in ("pending", "invalid", "triggered"):
            raise DropzoneError(
                f"批次 #{batch_id} 当前状态为 {batch['status']}，不可忽略"
            )
        # 忽略只是人工处置登记，绝不启动编排，避免产生无关联运行和额外水位。
        dz_repo.update_batch_status(conn, batch_id, status="ignored", status_note="人工忽略")
        dz_repo.add_event(conn, batch_id, "ignored", {"from_status": batch["status"]})
        conn.commit()
        return dz_repo.get_batch(conn, batch_id)
    finally:
        conn.close()


def list_batches() -> list[dict[str, Any]]:
    conn = connect()
    try:
        return dz_repo.list_batches(conn)
    finally:
        conn.close()


def list_events(batch_id: int) -> list[dict[str, Any]]:
    conn = connect()
    try:
        _load_batch_or_404(conn, batch_id)
        return dz_repo.list_events(conn, batch_id)
    finally:
        conn.close()


def reconcile() -> dict[str, Any]:
    """对账：发现数、各状态计数、触发次数与编排侧逐条对照。

    所有数字都直接从 trigger_batches 现状统计，不做任何加减修正，
    保证页面上的计数与批次列表、批次关联的编排运行号一致。
    """
    conn = connect()
    try:
        batches = dz_repo.list_batches(conn)
        counts = {s: 0 for s in BATCH_STATUSES}
        for b in batches:
            counts[b["status"]] = counts.get(b["status"], 0) + 1

        discovered = len(batches)
        # 有效 = 待确认（尚未触发的有效文件）+ 已触发（有效且已开跑）。
        valid = counts["pending"] + counts["triggered"]
        # 触发次数合计：每趟成功拉起的编排对应一次计数（重触发单列累加、事件留痕）。
        trigger_slots = sum(b["trigger_count"] for b in batches)

        dropzone_orchestrations = conn.execute(
            "SELECT COUNT(*) c FROM orchestrations WHERE source='dropzone'"
        ).fetchone()["c"]
        # 已触发批次必须挂着编排，且挂的运行号在编排侧真实存在。
        triggered_batches = [b for b in batches if b["status"] == "triggered"]
        linked_triggered = conn.execute(
            """
            SELECT COUNT(*) c FROM trigger_batches b
            JOIN orchestrations o ON o.id = b.orchestration_id
            WHERE b.status='triggered'
            """
        ).fetchone()["c"]
        # 反向核对：dropzone 编排的 trigger_batch_id 必须指回真实批次。
        dangling_orchestrations = conn.execute(
            """
            SELECT COUNT(*) c FROM orchestrations o
            LEFT JOIN trigger_batches b ON b.id = o.trigger_batch_id
            WHERE o.source='dropzone' AND b.id IS NULL
            """
        ).fetchone()["c"]
        # 每个批次挂的运行号确实属于它自己（不指到别的文件）。
        mismatched_links = conn.execute(
            """
            SELECT COUNT(*) c FROM orchestrations o
            JOIN trigger_batches b ON b.orchestration_id = o.id
            WHERE o.trigger_batch_id IS NOT b.id
            """
        ).fetchone()["c"]

        status_sum = sum(counts.values())
        checks = {
            "发现数=各状态之和": discovered == status_sum,
            "触发次数合计=dropzone来源编排数": trigger_slots == dropzone_orchestrations,
            "已触发批次均挂到真实编排": len(triggered_batches) == linked_triggered,
            "编排运行号均指回本批次": dangling_orchestrations == 0 and mismatched_links == 0,
            "有效数=pending+triggered": valid == counts["pending"] + counts["triggered"],
        }
        return {
            "discovered": discovered,
            "valid": valid,
            "pending": counts["pending"],
            "triggered": counts["triggered"],
            "ignored": counts["ignored"],
            "invalid": counts["invalid"],
            "failed": counts["failed"],
            "trigger_slots": trigger_slots,
            "dropzone_orchestrations": dropzone_orchestrations,
            "linked_triggered": linked_triggered,
            "dangling_orchestrations": dangling_orchestrations,
            "mismatched_links": mismatched_links,
            "checks": checks,
            "ok": all(checks.values()),
        }
    finally:
        conn.close()
