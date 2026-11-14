from __future__ import annotations

from typing import Any

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.dependency import ready_by_upstream_success, topological_layers
from app.engines.job_runner import run_fixture_job
from app.repositories import jobs as jobs_repo
from app.repositories import orchestrations as orch_repo
from app.repositories import watermarks as wm_repo


def dashboard_stats() -> dict[str, Any]:
    conn = connect()
    try:
        batch_counts = {
            r["status"]: r["c"]
            for r in conn.execute(
                "SELECT status, COUNT(*) c FROM trigger_batches GROUP BY status"
            ).fetchall()
        }
        discovered = sum(batch_counts.values())
        return {
            "job_count": conn.execute("SELECT COUNT(*) c FROM jobs").fetchone()["c"],
            "enabled_jobs": conn.execute(
                "SELECT COUNT(*) c FROM jobs WHERE enabled=1"
            ).fetchone()["c"],
            "orchestration_count": conn.execute(
                "SELECT COUNT(*) c FROM orchestrations"
            ).fetchone()["c"],
            "attempt_count": conn.execute(
                "SELECT COUNT(*) c FROM job_attempts"
            ).fetchone()["c"],
            "watermarked_jobs": conn.execute(
                "SELECT COUNT(*) c FROM watermarks WHERE watermark_ts IS NOT NULL"
            ).fetchone()["c"],
            "dropzone_batches": batch_counts,
            # 发现数就是批次总数；有效 = 待确认 + 已触发，与落盘页/对账口径一致。
            "dropzone_discovered": discovered,
            "dropzone_valid": batch_counts.get("pending", 0)
            + batch_counts.get("triggered", 0),
        }
    finally:
        conn.close()


def run_orchestration(
    *, source: str = "manual", trigger_batch_id: int | None = None
) -> dict[str, Any]:
    conn = connect()
    try:
        jobs = [j for j in jobs_repo.list_jobs(conn) if j["enabled"]]
        orch_id = orch_repo.create_orchestration(
            conn, source=source, trigger_batch_id=trigger_batch_id
        )
        success_names: set[str] = set()
        skipped = 0
        failed = 0
        succeeded = 0
        layers = topological_layers(jobs)

        for layer in layers:
            for job in layer:
                ready, reason = ready_by_upstream_success(job["depends_on"], success_names)
                if not ready:
                    orch_repo.insert_attempt(
                        conn,
                        orchestration_id=orch_id,
                        job_id=job["id"],
                        attempt_no=1,
                        status="skipped_dep",
                        detail={"reason": reason},
                    )
                    skipped += 1
                    continue
                try:
                    result = run_fixture_job(FIXTURES_DIR / job["fixture_path"])
                    orch_repo.insert_attempt(
                        conn,
                        orchestration_id=orch_id,
                        job_id=job["id"],
                        attempt_no=1,
                        status="success",
                        detail=result,
                    )
                    wm_repo.set_watermark(
                        conn, job["id"], result["watermark_ts"], result["artifact_rows"]
                    )
                    success_names.add(job["name"])
                    succeeded += 1
                except Exception as exc:  # noqa: BLE001
                    orch_repo.insert_attempt(
                        conn,
                        orchestration_id=orch_id,
                        job_id=job["id"],
                        attempt_no=1,
                        status="failed",
                        detail={"error": str(exc)},
                    )
                    failed += 1

        attempts = orch_repo.list_attempts(conn, orch_id)
        report = {
            "orchestration_id": orch_id,
            "source": source,
            "trigger_batch_id": trigger_batch_id,
            "job_total": len(jobs),
            "succeeded": succeeded,
            "failed": failed,
            "skipped": skipped,
            "attempt_count": len(attempts),
            "watermarked_jobs": conn.execute(
                "SELECT COUNT(*) c FROM watermarks WHERE watermark_ts IS NOT NULL"
            ).fetchone()["c"],
            "layer_count": len(layers),
        }
        # 对账：成功+失败+跳过 必须同时等于作业总数与尝试记录数
        report["reconcile_ok"] = (
            succeeded + failed + skipped == len(jobs) == len(attempts)
        )
        status = "ok" if failed == 0 else "partial"
        orch_repo.finish_orchestration(conn, orch_id, status, report)
        conn.commit()
        return orch_repo.get_orchestration(conn, orch_id) or report
    finally:
        conn.close()
