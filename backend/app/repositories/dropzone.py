from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def insert_batch(
    conn: sqlite3.Connection,
    *,
    drop_dir: str,
    filename: str,
    size_bytes: int,
    status: str,
    status_note: str | None,
) -> int:
    now = _now()
    cur = conn.execute(
        """
        INSERT INTO trigger_batches(drop_dir, filename, size_bytes, discovered_at, status, status_note, updated_at)
        VALUES (?,?,?,?,?,?,?)
        """,
        (drop_dir, filename, size_bytes, now, status, status_note, now),
    )
    return int(cur.lastrowid)


def get_batch(conn: sqlite3.Connection, batch_id: int) -> dict[str, Any] | None:
    r = conn.execute(
        "SELECT * FROM trigger_batches WHERE id=?", (batch_id,)
    ).fetchone()
    return dict(r) if r else None


def find_batch(
    conn: sqlite3.Connection, drop_dir: str, filename: str
) -> dict[str, Any] | None:
    r = conn.execute(
        "SELECT * FROM trigger_batches WHERE drop_dir=? AND filename=?",
        (drop_dir, filename),
    ).fetchone()
    return dict(r) if r else None


def list_batches(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT b.*, o.status AS orchestration_status
        FROM trigger_batches b
        LEFT JOIN orchestrations o ON o.id = b.orchestration_id
        ORDER BY b.id DESC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def update_batch_status(
    conn: sqlite3.Connection,
    batch_id: int,
    *,
    status: str,
    status_note: str | None = None,
    orchestration_id: int | None = None,
    bump_trigger_count: bool = False,
) -> None:
    conn.execute(
        """
        UPDATE trigger_batches
        SET status=?, status_note=?,
            orchestration_id=COALESCE(?, orchestration_id),
            trigger_count=trigger_count + ?,
            updated_at=?
        WHERE id=?
        """,
        (status, status_note, orchestration_id, 1 if bump_trigger_count else 0, _now(), batch_id),
    )


def link_orchestration(
    conn: sqlite3.Connection, batch_id: int, orchestration_id: int
) -> None:
    """Bind an orchestration id onto a trigger batch."""
    conn.execute(
        "UPDATE trigger_batches SET orchestration_id=?, updated_at=? WHERE id=?",
        (orchestration_id, _now(), batch_id),
    )


def discovery_stats(conn: sqlite3.Connection) -> dict[str, int]:
    """Discovery/valid counts used by scan/reconcile UIs.

    计数直接等于 trigger_batches 的状态分布，不做任何加减修正，
    与批次列表天然一致。
    """
    rows = status_counts(conn)
    discovered = sum(rows.values())
    valid = rows.get("pending", 0) + rows.get("triggered", 0)
    return {
        "discovered": discovered,
        "valid": valid,
        "ignored": rows.get("ignored", 0),
        "invalid": rows.get("invalid", 0),
        "failed": rows.get("failed", 0),
    }


def add_event(
    conn: sqlite3.Connection,
    batch_id: int,
    event: str,
    detail: dict[str, Any],
) -> None:
    conn.execute(
        "INSERT INTO trigger_events(batch_id, event, detail, created_at) VALUES (?,?,?,?)",
        (batch_id, event, json.dumps(detail, ensure_ascii=False), _now()),
    )


def list_events(conn: sqlite3.Connection, batch_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT id, batch_id, event, detail, created_at FROM trigger_events WHERE batch_id=? ORDER BY id",
        (batch_id,),
    ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["detail"] = json.loads(d["detail"])
        out.append(d)
    return out


def status_counts(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        "SELECT status, COUNT(*) c FROM trigger_batches GROUP BY status"
    ).fetchall()
    return {r["status"]: r["c"] for r in rows}
