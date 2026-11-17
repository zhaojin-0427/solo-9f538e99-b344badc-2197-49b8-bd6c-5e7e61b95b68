"""Pydantic 响应/请求模型与统一错误类型。"""
from __future__ import annotations

from typing import List, Literal, Optional

from fastapi import HTTPException
from pydantic import BaseModel, Field


class ConflictError(HTTPException):
    """业务规则冲突（409）：请求本身合法，但违反数仓不变量。"""

    def __init__(self, detail: str) -> None:
        super().__init__(status_code=409, detail=detail)


class IntegrityError(HTTPException):
    """快照分片完整性核验失败（409）：整页拒绝，不返回任何部分行。"""

    def __init__(self, snapshot_id: int, failures: List[dict]) -> None:
        super().__init__(
            status_code=409,
            detail={
                "message": "快照分片完整性核验失败，整页拒绝",
                "snapshot_id": snapshot_id,
                "shards": failures,
            },
        )


class NotFound(HTTPException):
    """资源不存在（404）。"""

    def __init__(self, detail: str) -> None:
        super().__init__(status_code=404, detail=detail)


class IndicatorOut(BaseModel):
    indicator_id: int = Field(..., description="指标稳定身份 ID；改名不变，同名复用会得到新 ID")
    name: str = Field(..., description="当前指标名")
    active: bool


class SchemaOut(BaseModel):
    current_version: int = Field(..., description="当前字段版号")
    fixed_columns: List[str] = Field(..., description="固定字段")
    indicators: List[IndicatorOut] = Field(..., description="当前存续指标（按指标 ID 排序）")


class VersionIndicatorOut(BaseModel):
    indicator_id: int
    name: str
    active: bool


class VersionOut(BaseModel):
    version_id: int
    created_at: str
    change_note: str
    indicators: List[VersionIndicatorOut]


class RenameRequest(BaseModel):
    old_name: str = Field(..., min_length=1, description="当前指标名（例：浓度）")
    new_name: str = Field(..., min_length=1, description="新指标名（例：硝酸盐）")


class CreateIndicatorRequest(BaseModel):
    name: str = Field(..., min_length=1, description="新指标名；若与历史指标同名则视为全新指标，不继承旧值")


class IndicatorChangeResponse(BaseModel):
    indicator_id: int
    name: str
    active: bool
    new_version: int = Field(..., description="本次变更产生的字段版号")


class UploadResponse(BaseModel):
    shard_id: int
    schema_version: int = Field(..., description="本次上传指定并裁决所用的字段版")
    row_count: int
    stored_columns: List[str] = Field(..., description="分片实际落盘的指标身份列")
    file_path: str
    byte_size: int = Field(..., description="登记的分片字节数，用于完整性核验")
    sha256: str = Field(..., description="登记的分片 SHA-256，用于完整性核验")


class ShardIntegrityStatus(BaseModel):
    shard_id: int
    original_name: str = Field(..., description="上传时的原始文件名")
    position: int = Field(..., description="该分片在快照中的顺序")
    status: Literal["ok", "missing", "digest_mismatch", "unverifiable"] = Field(
        ...,
        description=(
            "ok=与登记一致；missing=文件不存在；digest_mismatch=字节数或 SHA-256 不符；"
            "unverifiable=无可用基线（旧卷分片且结构/登记行数核对失败），不可供数"
        ),
    )
    detail: Optional[str] = Field(None, description="异常说明；ok 时为 null")
    byte_size: Optional[int] = Field(None, description="登记字节数；无基线为 null")
    sha256: Optional[str] = Field(None, description="登记 SHA-256；无基线为 null")
    baseline_created: bool = Field(False, description="本次核验是否为旧卷分片补建了基线")


class SnapshotIntegrityOut(BaseModel):
    snapshot_id: int
    checked_at: str
    all_ok: bool = Field(..., description="是否全部分片通过；为 false 时 /rows 会整页拒绝")
    counts: dict = Field(..., description="各状态计数")
    shards: List[ShardIntegrityStatus]


class SnapshotMeta(BaseModel):
    snapshot_id: int
    created_at: str
    indicator_names: List[str]
    shard_ids: List[int]


class RowPage(BaseModel):
    snapshot_id: int
    snapshot_created: bool = Field(..., description="本次调用是否新建了快照（首次查询为 true）")
    page: int
    page_size: int
    total_rows: int
    total_pages: int
    fixed_columns: List[str]
    indicator_names: List[str] = Field(..., description="快照固化的当前指标名；旧分片没有的指标为 null")
    rows: List[dict]
