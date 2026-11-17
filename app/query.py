"""查询：首次查询固化字段与分片生成快照，之后按快照分页合读。

快照语义
========
* 首次查询（不带 ``snapshot_id``）把*当前字段名集合*与*当前全部分片*
  写入 ``snapshots`` / ``snapshot_fields`` / ``snapshot_shards``。
* 之后的分页、字段改名/删除/新增、新上传乃至进程重启，都不会改变该快照
  看到的字段与分片；读取完全以快照表为准。
* 合读按指标身份列对齐：新增指标在旧分片为 NULL；同名复用得到新身份，
  旧分片没有该身份列，同样为 NULL，不会继承旧指标的值。
"""
from __future__ import annotations

import sqlite3
from typing import List, Optional

import pyarrow as pa

from . import integrity
from .config import FIXED_COLUMNS, INDICATOR_COLUMN_PREFIX, SAMPLED_AT_COLUMN, STATION_COLUMN
from .models import ConflictError, IntegrityError, NotFound


def create_snapshot(conn: sqlite3.Connection) -> dict:
    """在调用方写事务内创建快照，固化当前字段与当前全部分片。"""
    cur = conn.execute("INSERT INTO snapshots DEFAULT VALUES")
    snapshot_id = int(cur.lastrowid)

    fields = conn.execute(
        "SELECT indicator_id, name FROM indicators WHERE active = 1 ORDER BY indicator_id"
    ).fetchall()
    for position, row in enumerate(fields):
        conn.execute(
            "INSERT INTO snapshot_fields (snapshot_id, indicator_id, name, position) "
            "VALUES (?, ?, ?, ?)",
            (snapshot_id, row["indicator_id"], row["name"], position),
        )

    shards = conn.execute("SELECT shard_id FROM shards ORDER BY shard_id").fetchall()
    for position, row in enumerate(shards):
        conn.execute(
            "INSERT INTO snapshot_shards (snapshot_id, shard_id, position) VALUES (?, ?, ?)",
            (snapshot_id, row["shard_id"], position),
        )

    created_at = conn.execute(
        "SELECT created_at FROM snapshots WHERE snapshot_id = ?", (snapshot_id,)
    ).fetchone()["created_at"]

    return {
        "snapshot_id": snapshot_id,
        "created_at": created_at,
        "indicator_names": [r["name"] for r in fields],
        "indicator_ids": [r["indicator_id"] for r in fields],
        "shard_ids": [r["shard_id"] for r in shards],
    }


def get_snapshot_meta(conn: sqlite3.Connection, snapshot_id: int) -> dict:
    snap = conn.execute(
        "SELECT snapshot_id, created_at FROM snapshots WHERE snapshot_id = ?",
        (snapshot_id,),
    ).fetchone()
    if snap is None:
        raise NotFound(f"快照 {snapshot_id} 不存在")
    fields = conn.execute(
        "SELECT indicator_id, name FROM snapshot_fields "
        "WHERE snapshot_id = ? ORDER BY position",
        (snapshot_id,),
    ).fetchall()
    shards = conn.execute(
        "SELECT shard_id FROM snapshot_shards WHERE snapshot_id = ? ORDER BY position",
        (snapshot_id,),
    ).fetchall()
    return {
        "snapshot_id": snapshot_id,
        "created_at": snap["created_at"],
        "indicator_names": [r["name"] for r in fields],
        "indicator_ids": [r["indicator_id"] for r in fields],
        "shard_ids": [r["shard_id"] for r in shards],
    }


def _format_timestamp(value) -> str:
    # Arrow 转出来的是带 tzinfo 的 datetime（UTC），统一输出带微秒与 Z 后缀的 ISO-8601
    if value is None:
        return None
    return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _read_shard_window(
    table: pa.Table,
    indicator_ids: List[int],
    skip: int,
    take: int,
) -> list[dict]:
    """从**已核验的同一份分片字节**解析出的表读取窗口行（按站点、采样时刻稳定排序）。

    缺失的指标身份列补 NULL；只物化窗口区间，避免对无关分片全量读取。
    """
    shard_total = table.num_rows

    # 固定列
    station = table.column(STATION_COLUMN).combine_chunks()
    sampled = table.column(SAMPLED_AT_COLUMN).combine_chunks()

    # 指标身份列；旧分片没有的身份 -> 全 NULL 列
    columns: dict[str, pa.ChunkedArray] = {
        STATION_COLUMN: pa.chunked_array([station]),
        SAMPLED_AT_COLUMN: pa.chunked_array([sampled]),
    }
    for ind_id in indicator_ids:
        canonical = f"{INDICATOR_COLUMN_PREFIX}{ind_id}"
        if canonical in table.column_names:
            columns[canonical] = table.column(canonical)
        else:
            columns[canonical] = pa.chunked_array(
                [pa.nulls(shard_total, type=pa.float64())]
            )

    aligned = pa.table(columns)

    # 稳定排序，保证分页确定性
    order = pa.compute.sort_indices(
        aligned,
        sort_keys=[(STATION_COLUMN, "ascending"), (SAMPLED_AT_COLUMN, "ascending")],
    )
    aligned = aligned.take(order)

    window = aligned.slice(skip, take) if skip < shard_total else aligned.slice(0, 0)

    rows: list[dict] = []
    stations = window.column(STATION_COLUMN).to_pylist()
    times = window.column(SAMPLED_AT_COLUMN).to_pylist()
    indicator_py = {
        ind_id: window.column(f"{INDICATOR_COLUMN_PREFIX}{ind_id}").to_pylist()
        for ind_id in indicator_ids
    }
    for i in range(window.num_rows):
        row = {
            STATION_COLUMN: stations[i],
            SAMPLED_AT_COLUMN: _format_timestamp(times[i]),
        }
        for ind_id in indicator_ids:
            row[INDICATOR_COLUMN_PREFIX + str(ind_id)] = indicator_py[ind_id][i]
        rows.append(row)
    return rows


def query_page(
    conn: sqlite3.Connection,
    snapshot_id: Optional[int],
    page: int,
    page_size: int,
) -> dict:
    """执行分页查询。``snapshot_id`` 为空时先创建快照。"""
    if page < 1:
        raise ConflictError("page 必须 >= 1")
    if page_size < 1:
        raise ConflictError("page_size 必须 >= 1")

    created = False
    if snapshot_id is None:
        meta = create_snapshot(conn)
        created = True
    else:
        meta = get_snapshot_meta(conn, snapshot_id)

    sid = meta["snapshot_id"]
    indicator_ids = meta["indicator_ids"]
    indicator_names = meta["indicator_names"]
    shard_ids = meta["shard_ids"]

    # 总分片行数（快照固化后是定值）
    if shard_ids:
        placeholders = ",".join("?" for _ in shard_ids)
        total_rows = conn.execute(
            f"SELECT COALESCE(SUM(row_count), 0) AS n FROM shards WHERE shard_id IN ({placeholders})",
            shard_ids,
        ).fetchone()["n"]
    else:
        total_rows = 0
    total_pages = (total_rows + page_size - 1) // page_size if total_rows else 0

    rows: list[dict] = []
    if total_rows:
        offset = (page - 1) * page_size
        remaining = page_size
        # 快照分片顺序固定；先据登记行数定位“本页实际会用到”的分片
        cumulative = 0
        used: list[tuple[int, int, int, int]] = []  # (shard_id, position, local_skip, local_take)
        for position, shard_id in enumerate(shard_ids):
            shard = conn.execute(
                "SELECT * FROM shards WHERE shard_id = ?", (shard_id,)
            ).fetchone()
            if shard is None:
                raise IntegrityError(
                    sid,
                    [
                        {
                            "shard_id": shard_id,
                            "original_name": None,
                            "position": position,
                            "status": integrity.STATUS_MISSING,
                            "detail": "分片登记记录不存在",
                        }
                    ],
                )
            shard_total = shard["row_count"]
            start = cumulative
            end = cumulative + shard_total
            cumulative = end

            if offset >= end or remaining <= 0:
                continue
            local_skip = max(offset - start, 0)
            local_take = min(remaining, shard_total - local_skip)
            used.append((shard_id, position, local_skip, local_take))
            remaining -= local_take

        # 先对**本次实际使用**的每个分片做完整性核验；任一片异常即整页拒绝，
        # 此时尚未产生任何行，绝不返回部分结果，也不改动快照。
        failures: list[dict] = []
        loaded: dict[int, tuple[pa.Table, int, int]] = {}
        for shard_id, position, local_skip, local_take in used:
            try:
                data, _item = integrity.read_verified_bytes(conn, shard_id)
                table = integrity.table_from_verified_bytes(shard_id, data)
            except integrity.ShardIntegrityFailure as exc:
                shard = conn.execute(
                    "SELECT original_name FROM shards WHERE shard_id = ?", (shard_id,)
                ).fetchone()
                failures.append(
                    {
                        "shard_id": shard_id,
                        "original_name": shard["original_name"] if shard else None,
                        "position": position,
                        "status": exc.status,
                        "detail": exc.detail,
                    }
                )
                continue
            loaded[shard_id] = (table, local_skip, local_take)

        if failures:
            raise IntegrityError(sid, failures)

        for shard_id, _position, _skip, _take in used:
            table, local_skip, local_take = loaded[shard_id]
            part = _read_shard_window(table, indicator_ids, local_skip, local_take)
            rows.extend(part)

    # 把身份列键替换为快照固化的当前指标名
    name_by_id = dict(zip(indicator_ids, indicator_names))
    named_rows: list[dict] = []
    for row in rows:
        named = {STATION_COLUMN: row[STATION_COLUMN], SAMPLED_AT_COLUMN: row[SAMPLED_AT_COLUMN]}
        for ind_id in indicator_ids:
            named[name_by_id[ind_id]] = row[INDICATOR_COLUMN_PREFIX + str(ind_id)]
        named_rows.append(named)

    return {
        "snapshot_id": sid,
        "snapshot_created": created,
        "page": page,
        "page_size": page_size,
        "total_rows": total_rows,
        "total_pages": total_pages,
        "fixed_columns": list(FIXED_COLUMNS),
        "indicator_names": indicator_names,
        "rows": named_rows,
    }
