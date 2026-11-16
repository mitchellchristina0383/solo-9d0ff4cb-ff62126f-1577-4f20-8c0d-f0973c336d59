"""Resumable chunked upload service for incident diagnostic packages.

Flow:
  1. POST /api/tasks            create a task with filename, size, sha256
  2. PUT  /api/tasks/{id}/chunks/{i}   upload 1 MiB chunks (idempotent)
  3. POST /api/tasks/{id}/finalize     verify chunks+size+sha256, then publish
  4. GET  /api/tasks/{id}/download     download the published package

Safety properties:
  * a chunk recorded in the DB and present on disk is immutable: re-uploading
    identical content is a no-op, different content is rejected with 409 and
    the original chunk is kept;
  * finalization is serialized per task across processes with an flock, the
    published file appears only via an atomic rename after full
    size+sha256 verification, so a crash or a disk error can never publish a
    partial file and two processes can never produce two different products;
  * all state lives in SQLite (WAL) and on disk, so restarts resume cleanly
    and already published packages stay downloadable.
"""
from __future__ import annotations

import fcntl
import hashlib
import os
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from . import storage as storage_mod
from .db import connect, init_db
from .storage import Storage

CHUNK_SIZE = 1024 * 1024  # 1 MiB
SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
STATIC_DIR = Path(__file__).resolve().parent / "static"
READ_BUF = 1024 * 1024


class CorruptChunk(Exception):
    def __init__(self, index: int):
        super().__init__(f"chunk {index} corrupted on disk")
        self.index = index


class CreateTask(BaseModel):
    filename: str = Field(min_length=1, max_length=255)
    size: int = Field(ge=0)
    sha256: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sanitize_filename(name: str) -> str:
    name = name.replace("\\", "/").split("/")[-1].strip().strip("\x00")
    return (name or "package.bin")[:200]


def _expected_chunk_size(task, index: int) -> int:
    if index < task["total_chunks"] - 1:
        return task["chunk_size"]
    return task["size"] - task["chunk_size"] * (task["total_chunks"] - 1)


def create_app(data_dir=None) -> FastAPI:
    data_dir = Path(data_dir or os.environ.get("DATA_DIR", "data")).resolve()
    storage = Storage(data_dir)
    storage.ensure_dirs()
    init_db(storage.db_path)
    storage.cleanup_stale_tmp()

    app = FastAPI(title="Incident Package Uploader")

    # ------------------------------------------------------------- helpers
    def get_task_or_404(conn, task_id: str):
        row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "task not found")
        return row

    def status_payload(conn, task) -> dict:
        rows = conn.execute(
            "SELECT chunk_index FROM chunks WHERE task_id = ? ORDER BY chunk_index",
            (task["id"],),
        ).fetchall()
        received = [r["chunk_index"] for r in rows]
        received_set = set(received)
        missing = [i for i in range(task["total_chunks"]) if i not in received_set]
        payload = {
            "task_id": task["id"],
            "filename": task["filename"],
            "size": task["size"],
            "sha256": task["sha256"],
            "chunk_size": task["chunk_size"],
            "total_chunks": task["total_chunks"],
            "received_count": len(received),
            "received_chunks": received,
            "missing_chunks": missing,
            "state": task["state"],
            "created_at": task["created_at"],
            "published_at": task["published_at"],
        }
        if task["state"] == "published":
            payload["download_url"] = f"/api/tasks/{task['id']}/download"
        return payload

    # ------------------------------------------------------------- API
    @app.get("/api/health")
    def health():
        return {"ok": True}

    @app.post("/api/tasks", status_code=201)
    def create_task(body: CreateTask):
        sha = body.sha256.lower()
        if not SHA256_RE.match(sha):
            raise HTTPException(400, "sha256 must be 64 hex characters")
        total_chunks = (body.size + CHUNK_SIZE - 1) // CHUNK_SIZE
        task_id = uuid.uuid4().hex
        conn = connect(storage.db_path)
        try:
            conn.execute(
                "INSERT INTO tasks (id, filename, size, sha256, chunk_size,"
                " total_chunks, state, created_at) VALUES (?,?,?,?,?,?, 'uploading', ?)",
                (
                    task_id,
                    _sanitize_filename(body.filename),
                    body.size,
                    sha,
                    CHUNK_SIZE,
                    total_chunks,
                    _now_iso(),
                ),
            )
            task = get_task_or_404(conn, task_id)
            return JSONResponse(status_code=201, content=status_payload(conn, task))
        finally:
            conn.close()

    @app.get("/api/tasks")
    def list_tasks():
        conn = connect(storage.db_path)
        try:
            rows = conn.execute(
                "SELECT t.*, (SELECT COUNT(*) FROM chunks c WHERE c.task_id = t.id)"
                " AS received FROM tasks t ORDER BY created_at DESC"
            ).fetchall()
            return {
                "tasks": [
                    {
                        "task_id": r["id"],
                        "filename": r["filename"],
                        "size": r["size"],
                        "sha256": r["sha256"],
                        "total_chunks": r["total_chunks"],
                        "received_count": r["received"],
                        "state": r["state"],
                        "created_at": r["created_at"],
                        "published_at": r["published_at"],
                    }
                    for r in rows
                ]
            }
        finally:
            conn.close()

    @app.get("/api/tasks/{task_id}")
    def get_task(task_id: str):
        conn = connect(storage.db_path)
        try:
            return status_payload(conn, get_task_or_404(conn, task_id))
        finally:
            conn.close()

    @app.put("/api/tasks/{task_id}/chunks/{index}")
    async def put_chunk(task_id: str, index: int, request: Request):
        data = await request.body()
        conn = connect(storage.db_path)
        try:
            task = get_task_or_404(conn, task_id)
            if task["state"] != "uploading":
                raise HTTPException(
                    409, f"task is '{task['state']}'; not accepting chunks"
                )
            if not 0 <= index < task["total_chunks"]:
                raise HTTPException(400, "chunk index out of range")
            expected = _expected_chunk_size(task, index)
            if len(data) != expected:
                raise HTTPException(
                    400, f"chunk size mismatch: expected {expected}, got {len(data)}"
                )

            digest = hashlib.sha256(data).hexdigest()
            final_path = storage.chunk_path(task_id, index)
            final_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = storage_mod.write_temp(storage.tmp_dir, data)

            # Record first, then rename. The PRIMARY KEY serializes concurrent
            # uploads of the same chunk across processes: exactly one INSERT
            # wins, and only the winner's content reaches the final path.
            try:
                conn.execute(
                    "INSERT INTO chunks (task_id, chunk_index, sha256, size)"
                    " VALUES (?,?,?,?)",
                    (task_id, index, digest, len(data)),
                )
                inserted = True
            except sqlite3.IntegrityError:
                inserted = False

            if not inserted:
                row = conn.execute(
                    "SELECT sha256 FROM chunks WHERE task_id = ? AND chunk_index = ?",
                    (task_id, index),
                ).fetchone()
                if row["sha256"] != digest:
                    storage_mod.unlink_quiet(tmp)
                    raise HTTPException(
                        409,
                        "chunk already exists with different content;"
                        " original chunk kept",
                    )
                if not final_path.exists():
                    # Heal: row exists but a previous crash lost the file.
                    try:
                        storage_mod.replace(str(tmp), str(final_path))
                        storage_mod.fsync_dir(final_path.parent)
                    except OSError as exc:
                        storage_mod.unlink_quiet(tmp)
                        raise HTTPException(500, f"disk write failed: {exc}")
                else:
                    storage_mod.unlink_quiet(tmp)
                return {"status": "ok", "index": index, "deduplicated": True}

            try:
                storage_mod.replace(str(tmp), str(final_path))
                storage_mod.fsync_dir(final_path.parent)
            except OSError as exc:
                conn.execute(
                    "DELETE FROM chunks WHERE task_id = ? AND chunk_index = ?",
                    (task_id, index),
                )
                storage_mod.unlink_quiet(tmp)
                raise HTTPException(500, f"disk write failed: {exc}")
            return {"status": "ok", "index": index, "deduplicated": False}
        finally:
            conn.close()

    @app.post("/api/tasks/{task_id}/finalize")
    def finalize(task_id: str):
        # Serialize finalization of this task across all service processes.
        lock_file = open(storage.lock_path(task_id), "w")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            conn = connect(storage.db_path)
            try:
                task = get_task_or_404(conn, task_id)
                if task["state"] == "published":
                    return {
                        "state": "published",
                        "download_url": f"/api/tasks/{task_id}/download",
                        "detail": "already published",
                    }
                if task["state"] == "failed":
                    raise HTTPException(
                        409,
                        "task previously failed digest verification;"
                        " please create a new task",
                    )

                rows = conn.execute(
                    "SELECT chunk_index, sha256 FROM chunks WHERE task_id = ?",
                    (task_id,),
                ).fetchall()
                hashes = {r["chunk_index"]: r["sha256"] for r in rows}

                missing = []
                for i in range(task["total_chunks"]):
                    if i not in hashes:
                        missing.append(i)
                    elif not storage.chunk_path(task_id, i).exists():
                        # DB row without file (crash between insert and
                        # rename): drop the row so the chunk can be re-sent.
                        conn.execute(
                            "DELETE FROM chunks WHERE task_id = ? AND chunk_index = ?",
                            (task_id, i),
                        )
                        missing.append(i)
                if missing:
                    return JSONResponse(
                        status_code=409,
                        content={
                            "state": "uploading",
                            "missing_chunks": missing,
                            "detail": "chunks missing; upload them and finalize again",
                        },
                    )

                # Assemble into a temp file; rename to the published path
                # only AFTER size+sha256 verification passes.
                total = 0
                digest = hashlib.sha256()
                tmp = storage.tmp_dir / f".pub-{uuid.uuid4().hex}"
                try:
                    with open(tmp, "wb") as out:
                        for i in range(task["total_chunks"]):
                            chunk_hash = hashlib.sha256()
                            with open(storage.chunk_path(task_id, i), "rb") as f:
                                while True:
                                    buf = f.read(READ_BUF)
                                    if not buf:
                                        break
                                    out.write(buf)
                                    digest.update(buf)
                                    chunk_hash.update(buf)
                                    total += len(buf)
                            if chunk_hash.hexdigest() != hashes[i]:
                                raise CorruptChunk(i)
                        out.flush()
                        os.fsync(out.fileno())
                except CorruptChunk as cc:
                    storage_mod.unlink_quiet(tmp)
                    conn.execute(
                        "DELETE FROM chunks WHERE task_id = ? AND chunk_index = ?",
                        (task_id, cc.index),
                    )
                    return JSONResponse(
                        status_code=409,
                        content={
                            "state": "uploading",
                            "missing_chunks": [cc.index],
                            "detail": "chunk corrupted on disk; please re-upload it",
                        },
                    )
                except OSError as exc:
                    storage_mod.unlink_quiet(tmp)
                    raise HTTPException(500, f"disk write failed: {exc}")

                actual = digest.hexdigest()
                if total != task["size"] or actual != task["sha256"]:
                    storage_mod.unlink_quiet(tmp)
                    conn.execute(
                        "UPDATE tasks SET state = 'failed' WHERE id = ?", (task_id,)
                    )
                    return JSONResponse(
                        status_code=409,
                        content={
                            "state": "failed",
                            "expected_sha256": task["sha256"],
                            "actual_sha256": actual,
                            "expected_size": task["size"],
                            "actual_size": total,
                            "detail": "size or sha256 mismatch; task rejected,"
                            " please create a new task",
                        },
                    )

                # Verification passed: atomically publish, then record state.
                published_path = storage.published_path(task_id)
                published_path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    storage_mod.replace(str(tmp), str(published_path))
                    storage_mod.fsync_dir(published_path.parent)
                except OSError as exc:
                    storage_mod.unlink_quiet(tmp)
                    raise HTTPException(500, f"disk write failed: {exc}")

                conn.execute(
                    "UPDATE tasks SET state = 'published', published_at = ?"
                    " WHERE id = ?",
                    (_now_iso(), task_id),
                )
                return {
                    "state": "published",
                    "download_url": f"/api/tasks/{task_id}/download",
                }
            finally:
                conn.close()
        finally:
            lock_file.close()

    @app.get("/api/tasks/{task_id}/download")
    def download(task_id: str):
        conn = connect(storage.db_path)
        try:
            task = get_task_or_404(conn, task_id)
        finally:
            conn.close()
        if task["state"] != "published":
            raise HTTPException(409, f"task is '{task['state']}'; not downloadable")
        path = storage.published_path(task_id)
        if not path.exists():
            raise HTTPException(500, "published file missing on disk")
        return FileResponse(
            str(path),
            filename=task["filename"],
            media_type="application/octet-stream",
        )

    @app.get("/", response_class=HTMLResponse)
    def index_page():
        return HTMLResponse(
            (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        )

    return app


app = create_app()
