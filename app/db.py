"""SQLite access helpers.

WAL mode allows concurrent readers with a single writer across processes;
busy_timeout makes the loser of a write race wait instead of failing.
Connections run in autocommit mode (isolation_level=None); every state
mutation is a single statement, hence atomic.
"""
from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id            TEXT PRIMARY KEY,
    filename      TEXT NOT NULL,
    size          INTEGER NOT NULL,
    sha256        TEXT NOT NULL,
    chunk_size    INTEGER NOT NULL,
    total_chunks  INTEGER NOT NULL,
    state         TEXT NOT NULL DEFAULT 'uploading'
                  CHECK (state IN ('uploading', 'published', 'failed')),
    created_at    TEXT NOT NULL,
    published_at  TEXT
);

CREATE TABLE IF NOT EXISTS chunks (
    task_id     TEXT NOT NULL REFERENCES tasks(id),
    chunk_index INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    size        INTEGER NOT NULL,
    PRIMARY KEY (task_id, chunk_index)
);
"""


def connect(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(
        str(db_path), timeout=30.0, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()
