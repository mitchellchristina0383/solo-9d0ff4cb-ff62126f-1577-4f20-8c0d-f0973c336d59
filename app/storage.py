"""Filesystem layout and atomic-write helpers.

Durability / crash-safety rules implemented here:
  * data is always written to a temp file first, fsync'ed, then atomically
    renamed into place (os.replace), followed by an fsync of the directory;
  * a partially written file therefore never appears at its final path;
  * `replace` is the single choke point for the final rename so tests can
    simulate disk failures by monkeypatching it.
"""
from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path


def replace(src: str, dst: str) -> None:
    """Atomic rename. Monkeypatched by tests to simulate disk failure."""
    os.replace(src, dst)


def fsync_dir(path) -> None:
    fd = os.open(str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def unlink_quiet(path) -> None:
    try:
        os.unlink(str(path))
    except OSError:
        pass


def write_temp(tmp_dir: Path, data: bytes) -> Path:
    """Write *data* to a fresh temp file in *tmp_dir* (fsync'ed)."""
    fd, tmp = tempfile.mkstemp(dir=str(tmp_dir), prefix=".tmp-")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
    except BaseException:
        unlink_quiet(tmp)
        raise
    return Path(tmp)


class Storage:
    """Path layout under the data root."""

    def __init__(self, root) -> None:
        self.root = Path(root)
        self.db_path = self.root / "uploader.db"
        self.chunks_dir = self.root / "chunks"
        self.published_dir = self.root / "published"
        self.tmp_dir = self.root / "tmp"
        self.locks_dir = self.root / "locks"

    def ensure_dirs(self) -> None:
        for d in (self.chunks_dir, self.published_dir, self.tmp_dir, self.locks_dir):
            d.mkdir(parents=True, exist_ok=True)

    def chunk_path(self, task_id: str, index: int) -> Path:
        return self.chunks_dir / task_id / f"{index:08d}.part"

    def published_path(self, task_id: str) -> Path:
        return self.published_dir / task_id

    def lock_path(self, task_id: str) -> Path:
        return self.locks_dir / f"{task_id}.lock"

    def cleanup_stale_tmp(self, max_age_sec: float = 3600.0) -> None:
        """Remove orphaned temp files older than *max_age_sec*.

        Age-based so a freshly started process never deletes a temp file
        that a concurrently running process is about to rename.
        """
        now = time.time()
        for pattern in (".tmp-*", ".pub-*"):
            for p in self.tmp_dir.glob(pattern):
                try:
                    if now - p.stat().st_mtime > max_age_sec:
                        p.unlink()
                except OSError:
                    pass
