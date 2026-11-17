"""分片完整性核验：上传登记基线、旧卷补建基线、快照逐片核验与读取时校验。

基线语义
========
* 新上传的分片在落盘时登记 ``byte_size`` 与 ``sha256``，作为不可变基线。
* 旧卷分片（无基线）只有在 **可解析 + 固定列齐全 + 已登记指标列齐全 +
  实际行数等于登记行数** 全部通过后，才用*当前文件字节*补建基线；
  任一核对失败则保持无基线、状态 ``unverifiable``，始终不可供数。
* 有基线的分片逐字节核验：文件缺失 → ``missing``；大小或 SHA-256 不符 →
  ``digest_mismatch``。把原字节恢复后摘要重新吻合，旧快照即可继续读取。

读取一致性
==========
``read_verified_bytes`` 只读取一次文件字节，对**同一份字节**计算摘要并解析，
避免“先校验、后读取”之间文件被替换的竞态；任一被本次查询实际用到的分片
异常，调用方必须整页拒绝，不返回部分行。
"""
from __future__ import annotations

import hashlib
import io
import sqlite3
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .config import FIXED_COLUMNS, INDICATOR_COLUMN_PREFIX

STATUS_OK = "ok"
STATUS_MISSING = "missing"
STATUS_DIGEST_MISMATCH = "digest_mismatch"
STATUS_UNVERIFIABLE = "unverifiable"


class ShardIntegrityFailure(Exception):
    """单个分片核验失败；``status`` 取四种状态之一。"""

    def __init__(self, shard_id: int, status: str, detail: str) -> None:
        self.shard_id = shard_id
        self.status = status
        self.detail = detail
        super().__init__(f"分片 {shard_id} 完整性核验失败（{status}）：{detail}")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _structure_problem(conn: sqlite3.Connection, shard: sqlite3.Row, data: bytes) -> Optional[str]:
    """旧卷补建基线前的结构 / 登记行数核对；返回问题描述，None 表示通过。"""
    try:
        table = pq.read_table(io.BytesIO(data))
    except Exception as exc:  # noqa: BLE001 - Arrow 异常类型较多
        return f"无法解析 Parquet：{exc}"

    missing_fixed = [c for c in FIXED_COLUMNS if c not in table.column_names]
    if missing_fixed:
        return f"缺少固定字段列 {missing_fixed}"

    if table.num_rows != int(shard["row_count"]):
        return f"登记行数 {shard['row_count']} 与实际行数 {table.num_rows} 不一致"

    station_type = table.schema.field("station").type
    if not (pa.types.is_string(station_type) or pa.types.is_large_string(station_type)):
        return f"固定字段 'station' 类型应为字符串，实际为 {station_type}"

    sampled_type = table.schema.field("sampled_at").type
    if not pa.types.is_timestamp(sampled_type):
        return f"固定字段 'sampled_at' 类型应为 timestamp，实际为 {sampled_type}"

    recorded = conn.execute(
        "SELECT indicator_id FROM shard_indicators WHERE shard_id = ? ORDER BY indicator_id",
        (shard["shard_id"],),
    ).fetchall()
    for r in recorded:
        column = f"{INDICATOR_COLUMN_PREFIX}{r['indicator_id']}"
        if column not in table.column_names:
            return f"缺少已登记指标列 {column}"
        kind = table.schema.field(column).type
        if not (
            pa.types.is_floating(kind)
            or pa.types.is_integer(kind)
            or pa.types.is_null(kind)
        ):
            return f"指标列 {column} 类型应为数值，实际为 {kind}"
    return None


def _evaluate(
    conn: sqlite3.Connection,
    shard: sqlite3.Row,
    data: bytes,
    *,
    build_baseline: bool,
) -> dict:
    """对一份已读取字节做基线核验；必要时为旧卷分片补建基线。

    调用方须持有写锁并处于事务中才可传 ``build_baseline=True``。
    返回状态字典（不改动快照）；无基线且未补建时状态为 ``unverifiable``。
    """
    shard_id = int(shard["shard_id"])
    result = {
        "shard_id": shard_id,
        "original_name": shard["original_name"],
        "status": STATUS_OK,
        "detail": None,
        "byte_size": shard["byte_size"],
        "sha256": shard["sha256"],
        "baseline_created": False,
    }

    expected_hash = shard["sha256"]
    expected_size = shard["byte_size"]

    if expected_hash is not None:
        actual_hash = sha256_hex(data)
        if expected_size is None or len(data) != int(expected_size) or actual_hash != expected_hash:
            result.update(
                status=STATUS_DIGEST_MISMATCH,
                detail=(
                    f"摘要不符：登记 {expected_size} 字节 / {expected_hash}，"
                    f"实际 {len(data)} 字节 / {actual_hash}"
                ),
            )
        return result

    # 无基线（旧卷分片）：只有结构 + 登记行数核对通过才补建基线并供数
    problem = _structure_problem(conn, shard, data)
    if problem is not None:
        result.update(status=STATUS_UNVERIFIABLE, detail=problem)
        return result

    actual_hash = sha256_hex(data)
    if not build_baseline:
        result.update(
            status=STATUS_UNVERIFIABLE,
            detail="旧卷分片尚未补建基线；请先调用快照完整性核验",
        )
        return result

    conn.execute(
        "UPDATE shards SET byte_size = ?, sha256 = ? WHERE shard_id = ?",
        (len(data), actual_hash, shard_id),
    )
    result.update(byte_size=len(data), sha256=actual_hash, baseline_created=True)
    return result


def verify_shard(
    conn: sqlite3.Connection,
    shard: sqlite3.Row,
    *,
    build_baseline: bool,
) -> dict:
    """核验单个分片，返回状态字典；``build_baseline`` 时可为旧卷分片补建基线。

    仅当调用方已持有写锁并处于事务中时才应传 ``build_baseline=True``；
    补建只更新 ``shards`` 的基线列，绝不触碰任何快照。
    """
    try:
        data = Path(shard["file_path"]).read_bytes()
    except FileNotFoundError:
        return {
            "shard_id": int(shard["shard_id"]),
            "original_name": shard["original_name"],
            "status": STATUS_MISSING,
            "detail": "分片文件不存在",
            "byte_size": shard["byte_size"],
            "sha256": shard["sha256"],
            "baseline_created": False,
        }
    except OSError as exc:
        return {
            "shard_id": int(shard["shard_id"]),
            "original_name": shard["original_name"],
            "status": STATUS_UNVERIFIABLE,
            "detail": f"分片文件无法读取：{exc}",
            "byte_size": shard["byte_size"],
            "sha256": shard["sha256"],
            "baseline_created": False,
        }
    return _evaluate(conn, shard, data, build_baseline=build_baseline)


def check_snapshot(conn: sqlite3.Connection, snapshot_id: int, shard_ids: list[int]) -> list[dict]:
    """逐片核验快照，可为旧卷分片补建基线。调用方须持写锁 + 立即事务。"""
    results: list[dict] = []
    for position, shard_id in enumerate(shard_ids):
        shard = conn.execute(
            "SELECT * FROM shards WHERE shard_id = ?", (shard_id,)
        ).fetchone()
        if shard is None:
            results.append(
                {
                    "shard_id": shard_id,
                    "original_name": "",
                    "position": position,
                    "status": STATUS_MISSING,
                    "detail": "分片登记记录不存在",
                    "byte_size": None,
                    "sha256": None,
                    "baseline_created": False,
                }
            )
            continue
        item = verify_shard(conn, shard, build_baseline=True)
        item["position"] = position
        results.append(item)
    return results


def read_verified_bytes(conn: sqlite3.Connection, shard_id: int) -> tuple[bytes, dict]:
    """读取分片字节，并在同一份字节上完成基线核验。

    * 有基线：按登记字节数 / SHA-256 核验，不符即失败。
    * 无基线（旧卷分片）：执行与核验接口完全相同的“结构 + 登记行数”核对；
      通过则就地补建基线（调用方须持写锁 + 事务），失败则不可供数。
    * 任何情况下都不存在“先校验一个文件、再读取另一个文件”的窗口。

    :raises ShardIntegrityFailure: 缺失 / 摘要不符 / 不可校验
    """
    shard = conn.execute("SELECT * FROM shards WHERE shard_id = ?", (shard_id,)).fetchone()
    if shard is None:
        raise ShardIntegrityFailure(shard_id, STATUS_MISSING, "分片登记记录不存在")
    try:
        data = Path(shard["file_path"]).read_bytes()
    except FileNotFoundError:
        raise ShardIntegrityFailure(shard_id, STATUS_MISSING, "分片文件不存在") from None
    except OSError as exc:
        raise ShardIntegrityFailure(shard_id, STATUS_UNVERIFIABLE, f"分片文件无法读取：{exc}") from exc

    item = _evaluate(conn, shard, data, build_baseline=True)
    if item["status"] != STATUS_OK:
        raise ShardIntegrityFailure(shard_id, item["status"], item["detail"])
    return data, item


def table_from_verified_bytes(shard_id: int, data: bytes) -> pa.Table:
    """解析已核验的同一份字节；解析失败仍按不可校验处理。"""
    try:
        return pq.read_table(io.BytesIO(data))
    except Exception as exc:  # noqa: BLE001
        raise ShardIntegrityFailure(
            shard_id, STATUS_UNVERIFIABLE, f"无法解析 Parquet：{exc}"
        ) from exc


def file_sha256_and_size(path: str) -> tuple[int, str]:
    """上传落盘后计算文件基线；分块读取避免额外整份驻留。"""
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return size, digest.hexdigest()


def counts(results: list[dict]) -> dict:
    out = {STATUS_OK: 0, STATUS_MISSING: 0, STATUS_DIGEST_MISMATCH: 0, STATUS_UNVERIFIABLE: 0}
    for item in results:
        out[item["status"]] = out.get(item["status"], 0) + 1
    return out


__all__ = [
    "ShardIntegrityFailure",
    "STATUS_OK",
    "STATUS_MISSING",
    "STATUS_DIGEST_MISMATCH",
    "STATUS_UNVERIFIABLE",
    "sha256_hex",
    "verify_shard",
    "check_snapshot",
    "read_verified_bytes",
    "table_from_verified_bytes",
    "file_sha256_and_size",
    "counts",
]
