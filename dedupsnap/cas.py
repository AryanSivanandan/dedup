# dedupsnap/cas.py
"""
Content-Addressable Store.
Chunks are stored under .dedupsnap/cas/<hex[:2]>/<hex>.
The cuckoo filter (on Repo) provides a fast O(1) pre-check so we skip the
DB round-trip for chunks that are almost certainly already stored.
"""
import os
import sqlite3
import tempfile
import time
from typing import Optional

from dedupsnap.hasher import hash_chunk


def cas_path_for_chunk(cas_root: str, chunk_id: bytes) -> str:
    h = chunk_id.hex()
    return os.path.join(cas_root, h[:2], h)


def _ensure_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def put_blob(repo, chunk_bytes: bytes, conn: sqlite3.Connection) -> bytes:
    """
    Write *chunk_bytes* to the CAS (if not present) and update the chunks table.

    Fast path (cuckoo hit + DB confirm): increment refcount only.
    Slow path (cuckoo miss): write blob atomically, insert DB row.

    Returns chunk_id as bytes (for FK storage in file_chunks).
    """
    chunk_hash_hex = hash_chunk(chunk_bytes)          # str, new hasher API
    chunk_id = bytes.fromhex(chunk_hash_hex)          # bytes for DB / file path

    # ------------------------------------------------------------------
    # Fast path: cuckoo filter says "probably present"
    # ------------------------------------------------------------------
    if repo.cuckoo.contains(chunk_hash_hex):
        row = conn.execute(
            "SELECT refcount FROM chunks WHERE chunk_id = ?",
            (sqlite3.Binary(chunk_id),),
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE chunks SET refcount = refcount + 1 WHERE chunk_id = ?",
                (sqlite3.Binary(chunk_id),),
            )
            repo._cache_hits += 1
            return chunk_id
        # Cuckoo false-positive: fall through to slow path

    repo._cache_misses += 1

    # ------------------------------------------------------------------
    # Slow path: check DB directly, write to CAS if truly new
    # ------------------------------------------------------------------
    row = conn.execute(
        "SELECT refcount FROM chunks WHERE chunk_id = ?",
        (sqlite3.Binary(chunk_id),),
    ).fetchone()

    if row:
        conn.execute(
            "UPDATE chunks SET refcount = refcount + 1 WHERE chunk_id = ?",
            (sqlite3.Binary(chunk_id),),
        )
        repo.cuckoo.insert(chunk_hash_hex)
        return chunk_id

    # Truly new chunk — write atomically to CAS
    path = cas_path_for_chunk(repo.cas_dir, chunk_id)
    _ensure_dir(path)
    tmp_fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(path))
    try:
        with os.fdopen(tmp_fd, "wb") as fh:
            fh.write(chunk_bytes)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.execute(
        "INSERT INTO chunks(chunk_id, size, refcount, stored, created_at) "
        "VALUES (?, ?, 1, 1, ?)",
        (sqlite3.Binary(chunk_id), len(chunk_bytes), now),
    )
    repo.cuckoo.insert(chunk_hash_hex)
    return chunk_id


def get_blob(repo, chunk_id: bytes) -> Optional[bytes]:
    """Read and integrity-verify a chunk from the CAS by its bytes chunk_id."""
    path = cas_path_for_chunk(repo.cas_dir, chunk_id)
    if not os.path.exists(path):
        return None
    with open(path, "rb") as fh:
        data = fh.read()
    if bytes.fromhex(hash_chunk(data)) != chunk_id:
        raise RuntimeError("CAS blob hash mismatch for " + chunk_id.hex())
    return data
