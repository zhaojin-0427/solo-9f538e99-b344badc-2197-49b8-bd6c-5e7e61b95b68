# 水质数仓化验表 API

固定站点、UTC 采样时刻，外加至多 8 个可空浮点指标的化验表服务。
技术栈：**FastAPI + PyArrow + SQLite + Parquet 分片**，可通过 Docker Compose 一键启动。

## 1. 核心模型（为什么改名/删名不会串列）

| 概念 | 说明 |
| --- | --- |
| 指标身份 `indicator_id` | 指标的稳定身份。**改名只改名字，不换身份**；删除只是 `active=0`，身份永不复用。 |
| 字段版 `schema_version` | 每次新增/改名/删除都产生一个不可变新版本，版内固化“字段名 → 指标身份”的完整映射。 |
| 分片 shard | 每次 Parquet 上传产生一个分片，物理列名为身份列 `i{indicator_id}`，与当前叫什么名字完全解耦。 |
| 快照 snapshot | 首次查询时固化“当时的字段名集合 + 当时的全部分片”，此后分页、字段变更、上传、重启都不改变它。 |

典型故事（与需求一致）：

1. 建指标 **浓度**（身份 1，字段版 2），上传历史分片 → 物理列 `i1`。
2. **浓度 改名为 硝酸盐**（身份仍是 1，字段版 3）。
3. 再建指标 **浓度**（同名复用 → 身份 2，字段版 4，语义为磷酸盐，不继承任何旧值）。
4. 用**旧字段版 2** 上传：旧版“浓度”映射到身份 1，数据进 `i1`（硝酸盐），不串列。
5. 用**字段版 4** 上传：当前“浓度”映射到身份 2，数据进 `i2`（磷酸盐）。
6. 查询按当前名合读：旧分片没有 `i2` → 磷酸盐列为 `NULL`；新分片没有 `i1` → 硝酸盐列为 `NULL`。

## 2. 启动

### Docker Compose（推荐）

```bash
docker compose up --build
# 服务：http://localhost:8000
# 交互式文档（Swagger）：http://localhost:8000/docs
# OpenAPI：http://localhost:8000/openapi.json
# 健康检查：http://localhost:8000/health
```

数据持久化在命名卷 `water_quality_data`（容器内 `/data`，含 `warehouse.db` 与 `shards/*.parquet`），
重启容器后字段、分片、快照全部保留。

### 本地直接运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 3. API 一览

| 方法/路径 | 作用 |
| --- | --- |
| `GET /schema` | 当前字段版号、固定字段、存续指标（含身份 ID） |
| `GET /schema/versions/{version_id}` | 查看任意字段版的不可变映射（含已删指标） |
| `POST /schema/indicators` | 新增指标（body `{"name": "..."}`）；同名复用得到**新身份** |
| `POST /schema/indicators/rename` | 改名（body `{"old_name": "...", "new_name": "..."}`），身份不变 |
| `DELETE /schema/indicators/{name}` | 删除指标（历史分片保留） |
| `POST /ingest/parquet` | 表单上传：`schema_version`（必填）+ `file`（Parquet）；成功即登记字节数与 SHA-256 |
| `GET /rows` | 分页查询；首次不带 `snapshot_id` 即建快照，之后带快照号翻页 |
| `GET /snapshots/{snapshot_id}` | 快照元数据（固化的字段名与分片） |
| `GET /snapshots/{snapshot_id}/integrity` | 逐片完整性核验：通过/缺失/摘要不符/不可校验，含原上传名 |
| `GET /docs` | Swagger UI |

### Parquet 上传规则

* 必含固定列：`station`（非空字符串）、`sampled_at`（UTC 时刻；支持带时区 timestamp、
  ISO-8601 字符串含 `Z`/偏移量、date；无时区值按 UTC 处理）。
* 指标列必须可转 `float64`（整数允许、布尔拒绝、全 NULL 允许），至多 8 个。
* 列名按**指定字段版**裁决：
  * 版内不存在的列（未知列）→ **整次拒绝（409）**；
  * 列映射到已删除指标 → **整次拒绝（409）**；
  * 版内未包含的指标可以缺列（指标本身可空）。
* 空文件、0 行分片、无法解析的 Parquet → 409；字段版不存在 → 404。
* 失败只回滚事务并清理临时文件，**不改动任何已有字段、数据与快照**。

### 快照完整性核验

* **上传即登记**：分片落盘后在同一事务内计算并登记 `byte_size` 与 `sha256`；
  要么“字节数 + 摘要 + 登记行”一起生效，要么整体不生效。
* **`GET /snapshots/{id}/integrity`** 按快照固化顺序逐片返回：

  | 状态 | 含义 |
  | --- | --- |
  | `ok` | 字节数与 SHA-256 均与登记一致 |
  | `missing` | 分片文件不存在（或登记记录丢失） |
  | `digest_mismatch` | 文件存在但字节数或 SHA-256 与登记不符（被改写/截断） |
  | `unverifiable` | 无可用基线：旧卷分片且结构/登记行数核对失败，**不可供数** |

  每片同时返回原上传名 `original_name`、快照内位置与登记基线。
* **旧卷分片补建基线**：无基线的历史分片仅在“可解析 + 固定列与物理类型正确 +
  已登记指标列齐全且为数值 + 实际行数等于登记行数”全部通过后，用当前字节补建基线
  （响应中 `baseline_created=true`）；任一核对失败则保持无基线、始终不可供数。
  核验接口与 `/rows` 读取执行完全相同的核对：首次读取旧卷分片时若核对通过会
  一并补建基线，核对失败则整页拒绝。补建只写分片基线列，不改动快照。
* **`/rows` 读取核验**：每次分页只对**本次实际使用**的分片做核验，且校验与解析
  针对**同一份已读取字节**（避免校验后文件被替换的竞态）。任一片异常即
  `409` 整页拒绝，`detail.shards` 给出异常分片；**不返回任何部分行**。
  未使用异常分片的页仍可读取。
* **恢复**：把被改写/删除的分片恢复为原字节后，摘要重新吻合，旧快照即可继续读取，
  无需重建快照。
* **并发**：核验、上传与建快照共用同一进程写锁 + `BEGIN IMMEDIATE` 事务，
  核验只依据完整已提交的分片集合；核验或读取失败均不改动原快照。

### 查询与快照语义

* `GET /rows?page=1&page_size=100`：首次调用返回 `snapshot_id` 与 `snapshot_created=true`。
* 之后 `GET /rows?snapshot_id=...&page=2&page_size=100` 始终读同一批分片、同一组字段名；
  新字段在旧分片为 `NULL`，同名复用的新身份在旧分片同样为 `NULL`。
* 不带快照号的新查询会生成新快照，反映最新字段与分片。
* 分片内按 `(station, sampled_at)` 稳定排序后分页；`page_size` 上限 10000。
* 每次翻页都会核验本页所使用的分片，异常时 409 整页拒绝（见上节）。

### 并发与一致性

所有字段变更、上传与快照创建都在同一把进程写锁 + SQLite `BEGIN IMMEDIATE` 事务内裁决：
字段变更与上传交错时，每个请求看到的都是一个完整的已提交字段状态，不存在半份结果。

## 4. 目录结构

```
app/
  config.py    # 常量与数据目录
  db.py        # SQLite 建表、迁移、连接、立即事务
  models.py    # Pydantic 模型与错误类型
  catalog.py   # 指标身份与字段版（新增/改名/删除）
  ingest.py    # Parquet 校验、按字段版映射身份、原子落盘、登记基线
  integrity.py # 完整性核验：补建基线、逐片状态、读取时字节核验
  query.py     # 快照创建与分页合读（读取前核验实际使用的分片）
  main.py      # FastAPI 路由与并发控制
tests/         # pytest 自测 + 独立 HTTP 冒烟脚本
Dockerfile
docker-compose.yml
```

## 5. 自测

```bash
pip install -r requirements.txt
pip install pytest httpx
python -m pytest tests/ -v
# 对已运行的服务做独立 HTTP 冒烟：
python tests/manual_smoke.py
```
