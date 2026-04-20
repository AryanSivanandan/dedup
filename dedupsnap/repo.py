# dedupsnap/repo.py
import json
import os
import time
from pathlib import Path

from dedupsnap.db import open_conn, init_schema
from dedupsnap.mmr import MMR
from dedupsnap.cuckoo_filter import CuckooFilter


class Repo:
    def __init__(self, repo_path: str) -> None:
        self.repo_path = str(Path(repo_path).resolve())
        self.meta_dir  = os.path.join(self.repo_path, ".dedupsnap")
        self.db_path   = os.path.join(self.meta_dir, "metadata.db")
        self.cas_dir   = os.path.join(self.meta_dir, "cas")

        os.makedirs(self.meta_dir, exist_ok=True)
        os.makedirs(self.cas_dir,  exist_ok=True)

        self.conn   = open_conn(self.db_path)
        self.mmr    = MMR(os.path.join(self.meta_dir, "mmr.json"))
        self.cuckoo = CuckooFilter(os.path.join(self.meta_dir, "cuckoo.json"))

        # Per-backup counters (reset in write_stats_cache)
        self._cache_hits:   int = 0
        self._cache_misses: int = 0

    def write_stats_cache(self, extra: dict = None) -> None:
        """Refresh .dedupsnap/stats_cache.json so the frontend can read it."""
        conn = self.conn

        snap_count = conn.execute(
            "SELECT COUNT(*) FROM snapshots WHERE status='committed'"
        ).fetchone()[0]

        chunk_row = conn.execute(
            "SELECT COUNT(*), SUM(size), SUM(size * refcount) FROM chunks"
        ).fetchone()
        unique_chunks     = chunk_row[0] or 0
        physical_bytes    = chunk_row[1] or 0
        logical_bytes     = chunk_row[2] or 0
        dedup_ratio       = round(logical_bytes / physical_bytes, 4) if physical_bytes else 1.0

        # Per-snapshot stats for charts
        snap_rows = conn.execute(
            "SELECT id, timestamp, name, gsr FROM snapshots "
            "WHERE status='committed' ORDER BY timestamp"
        ).fetchall()
        per_snap = []
        for sid, ts, sname, sgsr in snap_rows:
            fc = conn.execute(
                "SELECT COUNT(*) FROM files WHERE snapshot_id=?", (sid,)
            ).fetchone()[0]
            cc = conn.execute(
                "SELECT COUNT(DISTINCT chunk_id) FROM file_chunks WHERE snapshot_id=?",
                (sid,),
            ).fetchone()[0]
            sz = conn.execute(
                "SELECT SUM(c.size) FROM file_chunks fc "
                "JOIN chunks c ON fc.chunk_id = c.chunk_id "
                "WHERE fc.snapshot_id=?",
                (sid,),
            ).fetchone()[0] or 0
            policy_rows = conn.execute(
                "SELECT chunking_policy, COUNT(*) FROM files "
                "WHERE snapshot_id=? GROUP BY chunking_policy",
                (sid,),
            ).fetchall()
            policies = {p or "unknown": n for p, n in policy_rows}
            per_snap.append(
                {
                    "id": sid,
                    "timestamp": ts,
                    "name": sname or "",
                    "gsr": sgsr or "",
                    "file_count": fc,
                    "unique_chunks": cc,
                    "physical_bytes": sz,
                    "policies": policies,
                }
            )

        # Chunk size distribution (sample: all chunks)
        chunk_sizes = [
            r[0]
            for r in conn.execute("SELECT size FROM chunks ORDER BY size").fetchall()
        ]
        refcounts = [
            r[0]
            for r in conn.execute(
                "SELECT refcount FROM chunks ORDER BY refcount"
            ).fetchall()
        ]

        # If this session processed no chunks (e.g. called from `stats` command),
        # preserve the counters written by the last backup rather than zeroing them.
        cache_path = os.path.join(self.meta_dir, "stats_cache.json")
        if self._cache_hits + self._cache_misses == 0 and os.path.exists(cache_path):
            try:
                with open(cache_path) as fh:
                    prev = json.load(fh)
                hits   = prev.get("cache_hits",   0)
                misses = prev.get("cache_misses", 0)
            except (json.JSONDecodeError, KeyError):
                hits = misses = 0
        else:
            hits   = self._cache_hits
            misses = self._cache_misses

        cache_total = hits + misses
        cache_hit_ratio = round(hits / cache_total, 4) if cache_total else 0.0

        stats = {
            "total_snapshots":   snap_count,
            "total_unique_chunks": unique_chunks,
            "total_logical_bytes": logical_bytes,
            "total_physical_bytes": physical_bytes,
            "dedup_ratio":       dedup_ratio,
            "cache_hits":        hits,
            "cache_misses":      misses,
            "cache_hit_ratio":   cache_hit_ratio,
            "mmr_log_root":      self.mmr.get_log_root(),
            "updated_at":        time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "per_snapshot":      per_snap,
            "chunk_sizes":       chunk_sizes,
            "refcounts":         refcounts,
        }
        if extra:
            stats.update(extra)

        cache_path = os.path.join(self.meta_dir, "stats_cache.json")
        with open(cache_path, "w") as fh:
            json.dump(stats, fh, indent=2)


def init_repo(path: str) -> "Repo":
    repo = Repo(path)
    init_schema(repo.conn)
    return repo
