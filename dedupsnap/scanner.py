# dedupsnap/scanner.py

import os
import unicodedata
from pathlib import Path
import struct
import stat
from typing import List, Tuple

def normalize_path_bytes(root: Path, p: Path) -> bytes:
    rel = p.relative_to(root)
    s = str(rel).replace(os.path.sep, "/")
    s = unicodedata.normalize("NFC", s)
    return s.encode("utf-8")

def metadata_blob_from_stat(st: os.stat_result) -> bytes:
    return struct.pack(">I I I Q Q",
                      st.st_mode & 0xFFFFFFFF,
                      getattr(st, "st_uid", 0),
                      getattr(st, "st_gid", 0),
                      int(st.st_mtime),
                      st.st_size)

def canonical_scan(scan_path: str):
    """
    Yields list of (path_bytes, metadata_blob, full_path_str) in sorted order.
    Handles both a single file or a directory.
    """
    p_scan = Path(scan_path).resolve()
    files = []

    if not p_scan.exists():
        raise FileNotFoundError(f"Path not found: {scan_path}")

    if p_scan.is_file():
        root_dir = p_scan.parent
        try:
            st = p_scan.stat()
            path_bytes = normalize_path_bytes(root_dir, p_scan) 
            meta = metadata_blob_from_stat(st)
            files.append((path_bytes, meta, str(p_scan)))
        except OSError as e:
            raise RuntimeError(f"Could not scan file {p_scan}: {e}")
    
    elif p_scan.is_dir():
        root_dir = p_scan
        for p in root_dir.rglob("*"):
            if p.is_file():
                try:
                    st = p.stat()
                except OSError:
                    continue
                path_bytes = normalize_path_bytes(root_dir, p)
                meta = metadata_blob_from_stat(st)
                files.append((path_bytes, meta, str(p)))
    
    files.sort(key=lambda x: x[0])
    return files