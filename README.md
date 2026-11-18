# 事故诊断包断点续传服务

值班工程师接收事故诊断包的上传网页：网络断开可续传，交接时只能下载校验通过的完整文件。
已发布的 ZIP 诊断包可一键建立 **.jsonl 日志索引**，之后按 **message 字段的字面子串** 在网页上检索，定位到任务、ZIP 条目、行号与原文。

## 功能

- 浏览器创建任务时提交 **文件大小 + SHA-256**，按 **1 MiB** 分块上传
- 重开页面可查看各任务 **已收/缺失分块** 并补齐（断点续传）
- 相同分块同内容重传 **幂等去重**；异内容 **拒绝并保留原块**（409）
- 提交完成时服务端核对 **分块齐全、总大小、整体 SHA-256**，通过才发布下载
- 缺块可补传；摘要不符 **拒绝发布** 并提示重新创建任务
- 多进程并发上传/提交、磁盘写失败、进程重启均不会发布残缺文件或产生两份成品
- 已发布的包重启后仍可下载
- **日志检索（新增）**
  - 对已发布的 ZIP 包建立可复用的 `indexes/<task_id>.jsonl` 索引：逐行解析 ZIP 内每个常规条目的 UTF-8 JSONL，每行须为含字符串 `message` 字段的 JSON 对象
  - 建索引前核对整包 **登记的大小与 SHA-256**；**累计解压不得超过 32 MiB**（按实际解压字节计数，超限即中止）
  - **坏日志、重复条目路径、损坏的 ZIP** 一律整体失败（422），不在磁盘留下部分索引；失败可直接重试
  - 两个进程同时建同一包：per-task flock 串行 + 原子改名，**只出现一版完整索引**，后到者幂等复用
  - 按 message **字面子串**（区分大小写）跨所有已索引的包检索；结果显示任务、ZIP 条目、行号、原文
  - 搜索时对每个已索引的包 **重新核对登记摘要**：原包被改坏 → 不返回旧命中并移除失效索引；修复原包后可重建索引

## 快速开始

### Docker Compose

```bash
docker compose up --build
# 打开 http://localhost:8000
```

容器内以 `--workers 2` 运行两个服务进程，共享挂载卷中的 SQLite 与数据目录。

### 本地开发

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
DATA_DIR=data .venv/bin/uvicorn app.main:app --port 8000
# 打开 http://localhost:8000
```

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/tasks` | 创建任务 `{filename, size, sha256}`，返回 `task_id` 与分块参数 |
| GET | `/api/tasks` | 任务列表（状态、已收分块数；已发布任务含索引状态） |
| GET | `/api/tasks/{id}` | 任务详情：`received_chunks` / `missing_chunks` / `state` / `index` |
| PUT | `/api/tasks/{id}/chunks/{i}` | 上传第 i 块（1 MiB，最后一块按实际大小）。同内容重传返回 `deduplicated:true`；异内容 409 |
| POST | `/api/tasks/{id}/finalize` | 核对分块+大小+摘要，通过则发布；缺块返回 409+`missing_chunks`；摘要不符标记 `failed` |
| GET | `/api/tasks/{id}/download` | 下载已发布的包（未发布返回 409） |
| POST | `/api/tasks/{id}/index?force=` | 为已发布的包建立 .jsonl 索引。已存在且包未变时幂等返回 `built:false`；`force=true` 强制重建。包与登记摘要不符返回 409 并移除失效索引；坏日志/重复路径/坏 ZIP/解压超限返回 422+`reason`，不留部分索引 |
| GET | `/api/search?q=&task_id=&limit=` | 按 message 字面子串（区分大小写）检索。默认覆盖全部已索引的已发布包；`task_id` 可指定单个包。返回 `hits`（任务、ZIP 条目、行号、message、原文）与逐包状态 `packages`（`searched`/`not_indexed`/`stale`/`index_error`） |

任务状态机：`uploading` → `published`（终态）/ `failed`（终态，摘要不符）。

### 索引文件格式（`indexes/<task_id>.jsonl`）

```
{"_meta": true, "format": 1, "task_id": "…", "sha256": "…", "size": N,
 "entries": E, "lines": L, "uncompressed": B, "built_at": "…"}
{"entry": "logs/app.log", "line": 2, "message": "ERROR disk full", "text": "{…原始行…}"}
```

首行为 meta，其后每行一条日志记录。索引经临时文件 + `os.replace` 原子发布，任何时刻要么不存在、要么完整。

## 一致性设计

- **分块幂等**：`(task_id, chunk_index)` 主键。先写临时文件 → INSERT 抢主键 → 赢者原子改名落盘；撞主键则比对内容哈希，相同去重、不同 409 保留原块。
- **发布原子性**：成品先在临时目录完整拼装并 fsync，**校验通过后**才 `os.replace` 原子改名到 published 路径；任何时刻 published 路径要么不存在、要么是完整成品。
- **多进程互斥**：finalize 与索引构建共用 per-task `flock` 串行化；SQLite WAL + busy_timeout 处理并发写。两个进程同时提交/建索引，只会有一个完成校验-改名，另一个读到已完成状态直接复用。
- **索引原子性**：建索引前先核对整包登记的大小与 SHA-256；解析全部条目写入临时文件，再原子改名到 `indexes/<task_id>.jsonl`。坏日志、重复路径、坏 ZIP、解压超限或崩溃都只残留会被清理的临时文件，绝不出现部分索引。
- **失效防护**：搜索与建索引都会重新核对包文件与登记摘要；不符则不返回旧命中、移除失效索引，修复原包后可重建。
- **崩溃恢复**：状态全在 SQLite（synchronous=FULL）与磁盘。重启后已收分块保留；DB 有记录但文件丢失的块（INSERT 后、改名前崩溃）在 finalize 时被发现并允许补传。
- **磁盘写失败**：改名失败即回滚 DB 记录并返回 500，任务保持 `uploading`，不会产生半成品。
- **启动竞态**：多进程同时首次初始化全新 SQLite 文件时，`PRAGMA journal_mode=WAL` 可能立即报 `database is locked`（该锁不经 busy_timeout）；`create_app` 用 flock 串行化一次性建库，`init_db` 再带有限重试兜底。

## 测试

```bash
.venv/bin/python -m pytest tests/ -v
```

- `tests/test_app.py`（14 项）：完整流程、续传、幂等去重、异内容拒绝、缺块补传、摘要拒绝、磁盘失败注入（chunk 上传 / finalize 两条路径）、重启恢复、空文件、参数校验。
- `tests/test_search.py`（18 项）：建索引+搜索命中（任务/条目/行号/原文）、字面（非正则）匹配、跨包与指定包搜索、命中上限、幂等与强制重建、坏日志/重复路径/坏 ZIP/解压超限整体失败且不留部分索引、包被改坏或删除后无旧命中且修复后可重建、索引文件损坏不服务、磁盘失败注入、重启后索引可用、空 ZIP/目录条目/空行与 CRLF 行号。
- `tests/test_live.py`（6 项）：两个 uvicorn 进程并发上传/并发提交同一任务、kill -9 后重启续传、已发布包重启后可下载、跨进程同块并发去重、**两进程并发建同一包索引只出一版完整索引**、**建索引与搜索并发不出现部分命中**、**原包改坏后跨进程搜索无旧命中且修复后可重建**。

## 数据布局（`DATA_DIR`）

```
uploader.db            SQLite（WAL）
chunks/<task>/         已收分块
published/<task>       校验通过后的成品（原子改名出现）
indexes/<task>.jsonl   日志索引（首行 meta + 每行一条记录，原子改名出现）
tmp/                   临时文件（启动时清理超过 1 小时的残留）
locks/                 per-task flock 文件（含一次性建库锁）
```
