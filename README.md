# 事故诊断包断点续传服务

值班工程师接收事故诊断包的上传网页：网络断开可续传，交接时只能下载校验通过的完整文件。

## 功能

- 浏览器创建任务时提交 **文件大小 + SHA-256**，按 **1 MiB** 分块上传
- 重开页面可查看各任务 **已收/缺失分块** 并补齐（断点续传）
- 相同分块同内容重传 **幂等去重**；异内容 **拒绝并保留原块**（409）
- 提交完成时服务端核对 **分块齐全、总大小、整体 SHA-256**，通过才发布下载
- 缺块可补传；摘要不符 **拒绝发布** 并提示重新创建任务
- 多进程并发上传/提交、磁盘写失败、进程重启均不会发布残缺文件或产生两份成品
- 已发布的包重启后仍可下载

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
| GET | `/api/tasks` | 任务列表（状态、已收分块数） |
| GET | `/api/tasks/{id}` | 任务详情：`received_chunks` / `missing_chunks` / `state` |
| PUT | `/api/tasks/{id}/chunks/{i}` | 上传第 i 块（1 MiB，最后一块按实际大小）。同内容重传返回 `deduplicated:true`；异内容 409 |
| POST | `/api/tasks/{id}/finalize` | 核对分块+大小+摘要，通过则发布；缺块返回 409+`missing_chunks`；摘要不符标记 `failed` |
| GET | `/api/tasks/{id}/download` | 下载已发布的包（未发布返回 409） |

任务状态机：`uploading` → `published`（终态）/ `failed`（终态，摘要不符）。

## 一致性设计

- **分块幂等**：`(task_id, chunk_index)` 主键。先写临时文件 → INSERT 抢主键 → 赢者原子改名落盘；撞主键则比对内容哈希，相同去重、不同 409 保留原块。
- **发布原子性**：成品先在临时目录完整拼装并 fsync，**校验通过后**才 `os.replace` 原子改名到 published 路径；任何时刻 published 路径要么不存在、要么是完整成品。
- **多进程互斥**：finalize 用 per-task `flock` 串行化；SQLite WAL + busy_timeout 处理并发写。两个进程同时提交，只会有一个完成校验-改名-置态，另一个读到已发布直接返回。
- **崩溃恢复**：状态全在 SQLite（synchronous=FULL）与磁盘。重启后已收分块保留；DB 有记录但文件丢失的块（INSERT 后、改名前崩溃）在 finalize 时被发现并允许补传。
- **磁盘写失败**：改名失败即回滚 DB 记录并返回 500，任务保持 `uploading`，不会产生半成品。

## 测试

```bash
.venv/bin/python -m pytest tests/ -v
```

- `tests/test_app.py`（14 项）：完整流程、续传、幂等去重、异内容拒绝、缺块补传、摘要拒绝、磁盘失败注入（chunk 上传 / finalize 两条路径）、重启恢复、空文件、参数校验。
- `tests/test_live.py`（3 项）：两个 uvicorn 进程并发上传/并发提交同一任务、kill -9 后重启续传、已发布包重启后可下载、跨进程同块并发去重。

## 数据布局（`DATA_DIR`）

```
uploader.db        SQLite（WAL）
chunks/<task>/     已收分块
published/<task>   校验通过后的成品（原子改名出现）
tmp/               临时文件（启动时清理超过 1 小时的残留）
locks/             per-task flock 文件
```
