# dedupsnap/restore.py
import os
import sqlite3
from pathlib import Path
from dedupsnap.cas import get_blob
import struct

def _to_bytes(x):
    if isinstance(x, (bytes, bytearray)):
        return bytes(x)
    try:
        return bytes(x)
    except Exception:
        return x

def restore_snapshot(repo, snapshot_id: str, target_dir: str):
    conn: sqlite3.Connection = repo.conn
    cur = conn.execute(
        "SELECT path, metadata, chunk_count FROM files WHERE snapshot_id = ? ORDER BY path",
        (snapshot_id,)
    )
    files = cur.fetchall()
    os.makedirs(target_dir, exist_ok=True)
    for row in files:
        path, metadata_blob, chunk_count = row
        rel_path = path.replace("/", os.sep)
        target_path = os.path.normpath(os.path.join(target_dir, rel_path))
        parent = os.path.dirname(target_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        chunks = []
        ccur = conn.execute(
            "SELECT chunk_index, chunk_id FROM file_chunks WHERE snapshot_id = ? AND path = ? ORDER BY chunk_index",
            (snapshot_id, path)
        )
        for idx, chunk_id in ccur:
            cid = _to_bytes(chunk_id)
            if not isinstance(cid, (bytes, bytearray)):
                raise RuntimeError(f"Invalid chunk id type for {path}: {type(chunk_id)}")
            chunks.append(cid)
        try:
            with open(target_path, "wb") as out:
                for cid in chunks:
                    data = get_blob(repo, cid)
                    if data is None:
                        hexid = cid.hex() if isinstance(cid, (bytes,bytearray)) else repr(cid)
                        raise RuntimeError(f"Missing chunk {hexid} for {rel_path}")
                    out.write(data)
        except Exception:
            raise
        try:
            mode, uid, gid, mtime, fsize = struct.unpack(">I I I Q Q", metadata_blob)
            try:
                os.utime(target_path, (mtime, mtime))
            except Exception:
                pass
            try:
                os.chmod(target_path, mode)
            except Exception:
                pass
        except Exception:
            pass
