"""Functional tests (TestClient): flow, resume, idempotency, conflict,
finalize verification, disk-failure injection, and restart recovery."""
import hashlib
import random
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import storage as storage_mod  # noqa: E402
from app.main import CHUNK_SIZE, create_app  # noqa: E402


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def make_client(tmp_path: Path) -> TestClient:
    return TestClient(create_app(tmp_path / "data"))


def make_task(client: TestClient, data: bytes, filename="pkg.bin", sha=None) -> str:
    r = client.post(
        "/api/tasks",
        json={"filename": filename, "size": len(data), "sha256": sha or sha256(data)},
    )
    assert r.status_code == 201, r.text
    return r.json()["task_id"]


def chunks_of(data: bytes):
    return [data[i : i + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]


def put(client, tid, idx, payload):
    return client.put(f"/api/tasks/{tid}/chunks/{idx}", content=payload)


def upload_all(client, tid, data):
    for i, c in enumerate(chunks_of(data)):
        r = put(client, tid, i, c)
        assert r.status_code == 200, (i, r.text)


DATA = random.Random(42).randbytes(2 * CHUNK_SIZE + 123)  # 3 chunks


def test_full_flow(tmp_path):
    c = make_client(tmp_path)
    tid = make_task(c, DATA, filename="diag.tar.gz")
    upload_all(c, tid, DATA)

    st = c.get(f"/api/tasks/{tid}").json()
    assert st["state"] == "uploading"
    assert st["received_count"] == 3
    assert st["missing_chunks"] == []

    r = c.post(f"/api/tasks/{tid}/finalize")
    assert r.status_code == 200 and r.json()["state"] == "published"

    dl = c.get(f"/api/tasks/{tid}/download")
    assert dl.status_code == 200
    assert dl.content == DATA
    assert "diag.tar.gz" in dl.headers["content-disposition"]


def test_resume_and_status(tmp_path):
    c = make_client(tmp_path)
    data = random.Random(1).randbytes(3 * CHUNK_SIZE + 7)  # 4 chunks
    tid = make_task(c, data)
    put(c, tid, 0, chunks_of(data)[0])
    put(c, tid, 2, chunks_of(data)[2])

    st = c.get(f"/api/tasks/{tid}").json()
    assert st["received_chunks"] == [0, 2]
    assert st["missing_chunks"] == [1, 3]

    put(c, tid, 1, chunks_of(data)[1])
    put(c, tid, 3, chunks_of(data)[3])
    assert c.post(f"/api/tasks/{tid}/finalize").json()["state"] == "published"
    assert c.get(f"/api/tasks/{tid}/download").content == data


def test_duplicate_same_content_is_idempotent(tmp_path):
    c = make_client(tmp_path)
    tid = make_task(c, DATA)
    first = chunks_of(DATA)[0]
    r1 = put(c, tid, 0, first)
    r2 = put(c, tid, 0, first)
    assert r1.status_code == 200 and r1.json()["deduplicated"] is False
    assert r2.status_code == 200 and r2.json()["deduplicated"] is True
    st = c.get(f"/api/tasks/{tid}").json()
    assert st["received_chunks"] == [0]


def test_conflicting_content_rejected_original_kept(tmp_path):
    c = make_client(tmp_path)
    tid = make_task(c, DATA)
    original = chunks_of(DATA)[0]
    assert put(c, tid, 0, original).status_code == 200

    evil = bytearray(original)
    evil[0] ^= 0xFF
    r = put(c, tid, 0, bytes(evil))
    assert r.status_code == 409

    upload_all(c, tid, DATA)  # chunk 0 dedup-hits the original
    assert c.post(f"/api/tasks/{tid}/finalize").json()["state"] == "published"
    assert c.get(f"/api/tasks/{tid}/download").content == DATA


def test_wrong_chunk_size_and_index(tmp_path):
    c = make_client(tmp_path)
    tid = make_task(c, DATA)
    assert put(c, tid, 0, b"too short").status_code == 400
    assert put(c, tid, 99, chunks_of(DATA)[0]).status_code == 400
    assert put(c, "0" * 32, 0, chunks_of(DATA)[0]).status_code == 404


def test_finalize_with_missing_chunks_then_complete(tmp_path):
    c = make_client(tmp_path)
    tid = make_task(c, DATA)
    put(c, tid, 0, chunks_of(DATA)[0])

    r = c.post(f"/api/tasks/{tid}/finalize")
    assert r.status_code == 409
    body = r.json()
    assert body["state"] == "uploading"
    assert body["missing_chunks"] == [1, 2]
    # 缺块可补传
    put(c, tid, 1, chunks_of(DATA)[1])
    put(c, tid, 2, chunks_of(DATA)[2])
    assert c.post(f"/api/tasks/{tid}/finalize").json()["state"] == "published"


def test_digest_mismatch_rejects_publish(tmp_path):
    c = make_client(tmp_path)
    tid = make_task(c, DATA, sha=sha256(b"not the real content"))
    upload_all(c, tid, DATA)

    r = c.post(f"/api/tasks/{tid}/finalize")
    assert r.status_code == 409
    assert r.json()["state"] == "failed"

    st = c.get(f"/api/tasks/{tid}").json()
    assert st["state"] == "failed"
    # 拒绝发布：published 目录不得留有成品
    assert not (tmp_path / "data" / "published" / tid).exists()
    assert list((tmp_path / "data" / "published").iterdir()) == []
    # 失败后：不可下载、不可再传、不可再提交
    assert c.get(f"/api/tasks/{tid}/download").status_code == 409
    assert put(c, tid, 0, chunks_of(DATA)[0]).status_code == 409
    assert c.post(f"/api/tasks/{tid}/finalize").status_code == 409


def test_published_task_is_immutable_and_finalize_idempotent(tmp_path):
    c = make_client(tmp_path)
    tid = make_task(c, DATA)
    upload_all(c, tid, DATA)
    assert c.post(f"/api/tasks/{tid}/finalize").json()["state"] == "published"

    assert put(c, tid, 0, chunks_of(DATA)[0]).status_code == 409
    r = c.post(f"/api/tasks/{tid}/finalize")
    assert r.status_code == 200 and r.json()["state"] == "published"
    assert c.get(f"/api/tasks/{tid}/download").content == DATA


def test_disk_failure_on_chunk_upload_not_recorded(tmp_path, monkeypatch):
    c = make_client(tmp_path)

    def boom(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(storage_mod, "replace", boom)
    r = put(c, tid := make_task(c, DATA), 0, chunks_of(DATA)[0])
    assert r.status_code == 500
    st = c.get(f"/api/tasks/{tid}").json()
    assert st["received_chunks"] == []  # 未记入

    monkeypatch.undo()
    assert put(c, tid, 0, chunks_of(DATA)[0]).status_code == 200


def test_disk_failure_on_finalize_keeps_uploading(tmp_path, monkeypatch):
    c = make_client(tmp_path)
    tid = make_task(c, DATA)
    upload_all(c, tid, DATA)

    def boom(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(storage_mod, "replace", boom)
    r = c.post(f"/api/tasks/{tid}/finalize")
    assert r.status_code == 500
    assert c.get(f"/api/tasks/{tid}").json()["state"] == "uploading"
    assert not (tmp_path / "data" / "published" / tid).exists()  # 无残缺成品

    monkeypatch.undo()
    assert c.post(f"/api/tasks/{tid}/finalize").json()["state"] == "published"
    assert c.get(f"/api/tasks/{tid}/download").content == DATA


def test_restart_resumes_and_published_survives(tmp_path):
    data = random.Random(9).randbytes(2 * CHUNK_SIZE + 5)
    c1 = make_client(tmp_path)
    tid = make_task(c1, data)
    put(c1, tid, 0, chunks_of(data)[0])

    # 进程重启：同一数据目录新建 app
    c2 = make_client(tmp_path)
    st = c2.get(f"/api/tasks/{tid}").json()
    assert st["received_chunks"] == [0]
    put(c2, tid, 1, chunks_of(data)[1])
    put(c2, tid, 2, chunks_of(data)[2])
    assert c2.post(f"/api/tasks/{tid}/finalize").json()["state"] == "published"

    c3 = make_client(tmp_path)
    assert c3.get(f"/api/tasks/{tid}/download").content == data


def test_empty_file(tmp_path):
    c = make_client(tmp_path)
    tid = make_task(c, b"")
    st = c.get(f"/api/tasks/{tid}").json()
    assert st["total_chunks"] == 0
    assert c.post(f"/api/tasks/{tid}/finalize").json()["state"] == "published"
    assert c.get(f"/api/tasks/{tid}/download").content == b""


def test_validation(tmp_path):
    c = make_client(tmp_path)
    assert c.post("/api/tasks", json={
        "filename": "x", "size": 1, "sha256": "zz"}).status_code == 400
    assert c.post("/api/tasks", json={
        "filename": "x", "size": -1, "sha256": sha256(b"")}).status_code == 422
    assert c.get(f"/api/tasks/{'0'*32}").status_code == 404
    assert c.post(f"/api/tasks/{'0'*32}/finalize").status_code == 404
    assert c.get(f"/api/tasks/{'0'*32}/download").status_code == 404


def test_index_page_served(tmp_path):
    c = make_client(tmp_path)
    r = c.get("/")
    assert r.status_code == 200 and "断点续传" in r.text
