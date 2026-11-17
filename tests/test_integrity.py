"""完整性核验：上传登记基线、旧卷补建、逐片状态、读取整页拒绝与恢复。"""
from __future__ import annotations

import hashlib
from pathlib import Path

import pyarrow as pa
import pytest

from app.config import DATA_DIR, SHARD_DIR
from app.db import connect, init_db


def upload_file(client, path, version: int, name: str | None = None):
    with open(path, "rb") as fh:
        return client.post(
            "/ingest/parquet",
            data={"schema_version": str(version)},
            files={
                "file": (
                    name or path.name,
                    fh.read(),
                    "application/vnd.apache.parquet",
                )
            },
        )


def make_two_shard_snapshot(client, make_parquet):
    v = client.post("/schema/indicators", json={"name": "浓度"}).json()["new_version"]
    f1 = make_parquet("first.parquet", {
        "station": ["A"], "sampled_at": ["2026-01-01T00:00:00Z"], "浓度": [1.0],
    }, types={"sampled_at": pa.string()})
    f2 = make_parquet("second.parquet", {
        "station": ["B"], "sampled_at": ["2026-01-02T00:00:00Z"], "浓度": [2.0],
    }, types={"sampled_at": pa.string()})
    r1 = upload_file(client, f1, v)
    r2 = upload_file(client, f2, v)
    assert r1.status_code == 200 and r2.status_code == 200, (r1.text, r2.text)
    snap = client.get("/rows", params={"page_size": 100}).json()["snapshot_id"]
    return snap, r1.json(), r2.json()


def shard_path(shard_id: int) -> Path:
    return DATA_DIR / "shards" / f"shard-{shard_id:010d}.parquet"


def tamper(path: Path, extra: bytes = b"TAMPERED") -> bytes:
    original = path.read_bytes()
    path.write_bytes(original + extra)
    return original


def clear_baseline(shard_id: int) -> None:
    conn = connect()
    try:
        conn.execute(
            "UPDATE shards SET byte_size = NULL, sha256 = NULL WHERE shard_id = ?",
            (shard_id,),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 上传即登记基线
# ---------------------------------------------------------------------------

def test_upload_registers_byte_size_and_sha256(client, make_parquet):
    v = client.post("/schema/indicators", json={"name": "x"}).json()["new_version"]
    f = make_parquet("a.parquet", {
        "station": ["A"], "sampled_at": ["2026-01-01T00:00:00Z"], "x": [3.5],
    }, types={"sampled_at": pa.string()})
    r = upload_file(client, f, v)
    assert r.status_code == 200, r.text
    body = r.json()
    stored = Path(body["file_path"]).read_bytes()
    assert body["byte_size"] == len(stored)
    assert body["sha256"] == hashlib.sha256(stored).hexdigest()

    # 登记值与数据库一致，且核验接口返回 ok + 原上传名
    conn = connect()
    try:
        row = conn.execute(
            "SELECT byte_size, sha256, original_name FROM shards WHERE shard_id = ?",
            (body["shard_id"],),
        ).fetchone()
    finally:
        conn.close()
    assert row["byte_size"] == body["byte_size"]
    assert row["sha256"] == body["sha256"]

    snap = client.get("/rows").json()["snapshot_id"]
    report = client.get(f"/snapshots/{snap}/integrity")
    assert report.status_code == 200, report.text
    data = report.json()
    assert data["all_ok"] is True
    assert data["counts"]["ok"] == 1
    assert data["shards"][0]["status"] == "ok"
    assert data["shards"][0]["original_name"] == "a.parquet"
    assert data["shards"][0]["sha256"] == body["sha256"]


# ---------------------------------------------------------------------------
# 摘要不符 / 缺失 / 恢复
# ---------------------------------------------------------------------------

def test_digest_mismatch_rejects_whole_page_then_restore_recovers(client, make_parquet):
    snap, r1, r2 = make_two_shard_snapshot(client, make_parquet)
    path2 = shard_path(r2["shard_id"])
    original = tamper(path2)

    report = client.get(f"/snapshots/{snap}/integrity").json()
    by_id = {s["shard_id"]: s for s in report["shards"]}
    assert by_id[r1["shard_id"]]["status"] == "ok"
    assert by_id[r2["shard_id"]]["status"] == "digest_mismatch"
    assert by_id[r2["shard_id"]]["original_name"] == "second.parquet"
    assert report["all_ok"] is False
    assert report["counts"]["digest_mismatch"] == 1

    # 第 2 页实际用到被改写的分片 -> 整页拒绝，无部分行
    r = client.get("/rows", params={"snapshot_id": snap, "page": 2, "page_size": 1})
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    assert detail["snapshot_id"] == snap
    assert detail["shards"][0]["status"] == "digest_mismatch"
    assert detail["shards"][0]["shard_id"] == r2["shard_id"]
    assert "rows" not in r.json()

    # 一页横跨两个分片：只要含异常分片，整页拒绝（不返回分片 1 的行）
    r = client.get("/rows", params={"snapshot_id": snap, "page": 1, "page_size": 10})
    assert r.status_code == 409
    assert "rows" not in r.json()

    # 未使用被改写分片的页仍可读（只核验本次实际使用的文件）
    ok_page = client.get("/rows", params={"snapshot_id": snap, "page": 1, "page_size": 1})
    assert ok_page.status_code == 200
    assert [row["station"] for row in ok_page.json()["rows"]] == ["A"]

    # 核验失败不改动快照：分片集合与元数据保持原样
    meta_after = client.get(f"/snapshots/{snap}").json()
    assert meta_after["shard_ids"] == [r1["shard_id"], r2["shard_id"]]

    # 恢复原字节 -> 摘要重新吻合，旧快照继续可读
    path2.write_bytes(original)
    assert client.get(f"/snapshots/{snap}/integrity").json()["all_ok"] is True
    restored = client.get("/rows", params={"snapshot_id": snap, "page_size": 10})
    assert restored.status_code == 200
    assert {row["station"] for row in restored.json()["rows"]} == {"A", "B"}


def test_missing_shard_reports_missing_and_rejects(client, make_parquet):
    snap, r1, r2 = make_two_shard_snapshot(client, make_parquet)
    shard_path(r2["shard_id"]).unlink()

    report = client.get(f"/snapshots/{snap}/integrity").json()
    by_id = {s["shard_id"]: s for s in report["shards"]}
    assert by_id[r2["shard_id"]]["status"] == "missing"
    assert by_id[r2["shard_id"]]["original_name"] == "second.parquet"

    r = client.get("/rows", params={"snapshot_id": snap, "page_size": 10})
    assert r.status_code == 409
    assert r.json()["detail"]["shards"][0]["status"] == "missing"


# ---------------------------------------------------------------------------
# 旧卷分片：结构 + 登记行数核对通过才补建基线
# ---------------------------------------------------------------------------

def test_legacy_shard_backfilled_only_after_structure_and_rowcount_check(client, make_parquet):
    snap, r1, _ = make_two_shard_snapshot(client, make_parquet)
    clear_baseline(r1["shard_id"])

    # 结构 + 登记行数核对通过 -> 核验接口补建基线并转为 ok
    report = client.get(f"/snapshots/{snap}/integrity").json()
    item = next(s for s in report["shards"] if s["shard_id"] == r1["shard_id"])
    assert item["status"] == "ok"
    assert item["baseline_created"] is True
    assert item["sha256"] is not None and item["byte_size"] is not None
    assert item["original_name"] == "first.parquet"

    # 之后旧快照可继续读取
    page = client.get("/rows", params={"snapshot_id": snap, "page": 1, "page_size": 1})
    assert page.status_code == 200
    assert page.json()["rows"][0]["station"] == "A"

    # 再次核验不再重复补建
    again = client.get(f"/snapshots/{snap}/integrity").json()
    item = next(s for s in again["shards"] if s["shard_id"] == r1["shard_id"])
    assert item["status"] == "ok" and item["baseline_created"] is False


def test_legacy_shard_backfilled_on_first_read_when_structure_ok(client, make_parquet):
    """/rows 读取旧卷分片时执行同样核对并补建基线，无需先手工核验。"""
    snap, r1, _ = make_two_shard_snapshot(client, make_parquet)
    clear_baseline(r1["shard_id"])

    page = client.get("/rows", params={"snapshot_id": snap, "page": 1, "page_size": 1})
    assert page.status_code == 200, page.text
    assert page.json()["rows"][0]["station"] == "A"

    conn = connect()
    try:
        row = conn.execute(
            "SELECT byte_size, sha256 FROM shards WHERE shard_id = ?", (r1["shard_id"],)
        ).fetchone()
    finally:
        conn.close()
    assert row["byte_size"] is not None and row["sha256"] is not None


def test_legacy_shard_rowcount_mismatch_stays_unavailable(client, make_parquet):
    snap, r1, r2 = make_two_shard_snapshot(client, make_parquet)
    clear_baseline(r1["shard_id"])
    conn = connect()
    try:
        conn.execute(
            "UPDATE shards SET row_count = row_count + 5 WHERE shard_id = ?",
            (r1["shard_id"],),
        )
        conn.commit()
    finally:
        conn.close()

    report = client.get(f"/snapshots/{snap}/integrity").json()
    item = next(s for s in report["shards"] if s["shard_id"] == r1["shard_id"])
    assert item["status"] == "unverifiable"
    assert "行数" in item["detail"]
    assert item["baseline_created"] is False

    # 仍无基线，不可供数；且没有被“读取成功”洗白
    assert client.get("/rows", params={"snapshot_id": snap, "page": 1, "page_size": 1}).status_code == 409
    conn = connect()
    try:
        row = conn.execute(
            "SELECT sha256 FROM shards WHERE shard_id = ?", (r1["shard_id"],)
        ).fetchone()
    finally:
        conn.close()
    assert row["sha256"] is None


def test_legacy_shard_broken_structure_stays_unavailable(client, make_parquet):
    snap, r1, _ = make_two_shard_snapshot(client, make_parquet)
    clear_baseline(r1["shard_id"])
    shard_path(r1["shard_id"]).write_bytes(b"not a parquet at all")

    report = client.get(f"/snapshots/{snap}/integrity").json()
    item = next(s for s in report["shards"] if s["shard_id"] == r1["shard_id"])
    assert item["status"] == "unverifiable"
    assert item["baseline_created"] is False

    r = client.get("/rows", params={"snapshot_id": snap, "page": 1, "page_size": 1})
    assert r.status_code == 409
    assert r.json()["detail"]["shards"][0]["status"] == "unverifiable"


def test_legacy_shard_missing_indicator_column_unverifiable(client, make_parquet):
    """结构完整但缺少已登记指标列，同样不予补建基线。"""
    snap, r1, _ = make_two_shard_snapshot(client, make_parquet)
    clear_baseline(r1["shard_id"])
    # 再建一个指标（身份 2），登记到分片但文件里并没有 i2 列
    other_id = client.post("/schema/indicators", json={"name": "other"}).json()["indicator_id"]
    conn = connect()
    try:
        conn.execute(
            "INSERT INTO shard_indicators (shard_id, indicator_id) VALUES (?, ?)",
            (r1["shard_id"], other_id),
        )
        conn.commit()
    finally:
        conn.close()

    item = next(
        s for s in client.get(f"/snapshots/{snap}/integrity").json()["shards"]
        if s["shard_id"] == r1["shard_id"]
    )
    assert item["status"] == "unverifiable"
    assert f"i{other_id}" in item["detail"]


def test_legacy_shard_wrong_physical_type_unverifiable(client, make_parquet):
    """结构核对包含物理类型：固定列/指标列类型不对也不补建基线。"""
    snap, r1, _ = make_two_shard_snapshot(client, make_parquet)
    clear_baseline(r1["shard_id"])
    # 用错误类型覆盖分片：station 变整数、指标列变字符串
    bad = pa.table({
        "station": pa.array([1], pa.int64()),
        "sampled_at": pa.array([0], pa.timestamp("us", tz="UTC")),
        "i1": pa.array(["oops"], pa.string()),
    })
    import pyarrow.parquet as pq

    pq.write_table(bad, shard_path(r1["shard_id"]))

    item = next(
        s for s in client.get(f"/snapshots/{snap}/integrity").json()["shards"]
        if s["shard_id"] == r1["shard_id"]
    )
    assert item["status"] == "unverifiable"
    assert "类型" in item["detail"]


# ---------------------------------------------------------------------------
# 未知快照 / 空快照
# ---------------------------------------------------------------------------

def test_integrity_unknown_snapshot_404_and_empty_snapshot_ok(client):
    assert client.get("/snapshots/999/integrity").status_code == 404
    snap = client.get("/rows").json()["snapshot_id"]
    report = client.get(f"/snapshots/{snap}/integrity").json()
    assert report["all_ok"] is True
    assert report["shards"] == []
    assert report["counts"]["ok"] == 0


# ---------------------------------------------------------------------------
# 旧库迁移：既有 shards 表补列后，旧分片仍为无基线状态
# ---------------------------------------------------------------------------

def test_init_db_migrates_old_shards_table(tmp_path, monkeypatch):
    import sqlite3

    from app.config import DB_PATH

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()
    old = sqlite3.connect(DB_PATH)
    old.executescript(
        """
        CREATE TABLE schema_versions (
            version_id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            change_note TEXT NOT NULL
        );
        CREATE TABLE shards (
            shard_id INTEGER PRIMARY KEY AUTOINCREMENT,
            version_id INTEGER NOT NULL,
            file_path TEXT NOT NULL UNIQUE,
            row_count INTEGER NOT NULL,
            original_name TEXT NOT NULL,
            uploaded_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        );
        INSERT INTO schema_versions (change_note) VALUES ('old');
        INSERT INTO shards (version_id, file_path, row_count, original_name)
            VALUES (1, '/tmp/legacy.parquet', 3, 'legacy.parquet');
        """
    )
    old.commit()
    old.close()

    init_db()

    conn = connect()
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(shards)").fetchall()}
        row = conn.execute(
            "SELECT byte_size, sha256 FROM shards WHERE shard_id = 1"
        ).fetchone()
    finally:
        conn.close()
    assert {"byte_size", "sha256"} <= cols
    assert row["byte_size"] is None and row["sha256"] is None  # 旧分片等待核验补建


# ---------------------------------------------------------------------------
# 并发：核验与上传交错只依据已提交分片，不破坏快照
# ---------------------------------------------------------------------------

def test_integrity_and_uploads_interleave_consistently(client, make_parquet):
    import threading

    v = client.post("/schema/indicators", json={"name": "k"}).json()["new_version"]
    base = make_parquet("base.parquet", {
        "station": ["S0"], "sampled_at": ["2026-01-01T00:00:00Z"], "k": [0.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, base, v).status_code == 200
    snap = client.get("/rows").json()["snapshot_id"]

    barrier = threading.Barrier(5)
    errors: list[Exception] = []

    def uploader(suffix: int):
        try:
            barrier.wait()
            for i in range(4):
                f = make_parquet(f"u-{suffix}-{i}.parquet", {
                    "station": [f"S{suffix}{i}"],
                    "sampled_at": [f"2026-02-0{suffix}T00:00:0{i}Z"],
                    "k": [float(i)],
                }, types={"sampled_at": pa.string()})
                r = upload_file(client, f, v)
                assert r.status_code == 200, r.text
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def checker():
        try:
            barrier.wait()
            for _ in range(8):
                r = client.get(f"/snapshots/{snap}/integrity")
                assert r.status_code == 200, r.text
                body = r.json()
                # 核验集合永远是快照固化的那一批，且全部可核验
                assert body["all_ok"] is True
                assert [s["shard_id"] for s in body["shards"]] == [1]
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=uploader, args=(s,)) for s in (1, 2, 3, 4)]
    threads.append(threading.Thread(target=checker))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors

    # 固定快照仍只含原分片；新查询能看到全部新分片且核验通过
    assert client.get(f"/snapshots/{snap}").json()["shard_ids"] == [1]
    fresh = client.get("/rows", params={"page_size": 100}).json()
    assert fresh["total_rows"] == 17
    fresh_report = client.get(f"/snapshots/{fresh['snapshot_id']}/integrity").json()
    assert fresh_report["all_ok"] is True
    assert len(fresh_report["shards"]) == 17
