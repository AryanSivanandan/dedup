# dedupsnap/cuckoo_filter.py
"""
Cuckoo Filter for chunk fingerprint deduplication.
See Section IV-C: Index Data Structure.

Properties:
  * O(1) lookup / insert / delete
  * ~8-bit fingerprint per entry  → ~2 % false-positive rate at 90 % load
  * Supports deletion (unlike Bloom filters)  — critical for GC

Bucket layout: list of BUCKET_SIZE (4) 8-bit integer tags.
Alternate-bucket formula: i2 = (i1 XOR fp) % num_buckets
  (XOR with fp is the standard partial-key cuckoo trick; it is its own inverse
  so we can recover i1 from i2 without storing the original item.)
"""
import hashlib
import json
import os
from typing import List, Optional


def _full_hash(item: str) -> int:
    return int(hashlib.sha256(item.encode()).hexdigest(), 16)


def fingerprint(item: str) -> int:
    """8-bit fingerprint: low byte of SHA-256(item).  Must be ≥ 1."""
    fp = _full_hash(item) & 0xFF
    return fp if fp != 0 else 1  # 0 is reserved as "empty slot"


class CuckooFilter:
    """
    Cuckoo Filter with configurable bucket count.
    Call *save()* explicitly after a batch of inserts to persist to disk.
    """

    BUCKET_SIZE = 4
    MAX_KICKS   = 500

    def __init__(self, path: str, num_buckets: int = 10_000) -> None:
        self.path = path
        self.num_buckets = num_buckets
        self._buckets: List[List[int]] = [[] for _ in range(num_buckets)]
        self._load()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self) -> None:
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path) as fh:
                d = json.load(fh)
            nb = d.get("num_buckets", self.num_buckets)
            loaded: List[List[int]] = d.get("buckets", [])
            self.num_buckets = nb
            self._buckets = [list(b) for b in loaded]
            while len(self._buckets) < self.num_buckets:
                self._buckets.append([])
        except (json.JSONDecodeError, KeyError, ValueError):
            pass

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w") as fh:
            json.dump(
                {"num_buckets": self.num_buckets, "buckets": self._buckets},
                fh,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _i1(self, item: str) -> int:
        return _full_hash(item) % self.num_buckets

    def _i2(self, i1: int, fp: int) -> int:
        return (i1 ^ fp) % self.num_buckets

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def contains(self, item: str) -> bool:
        """Return True if *item* is (probably) in the filter."""
        fp = fingerprint(item)
        i1 = self._i1(item)
        i2 = self._i2(i1, fp)
        return fp in self._buckets[i1] or fp in self._buckets[i2]

    def insert(self, item: str) -> bool:
        """
        Insert *item* into the filter.
        Returns True on success, False if the filter is full after MAX_KICKS.
        Does *not* auto-save; call save() after a batch of inserts.
        """
        fp = fingerprint(item)
        i1 = self._i1(item)
        i2 = self._i2(i1, fp)

        # Direct insert if a bucket has room
        if len(self._buckets[i1]) < self.BUCKET_SIZE:
            self._buckets[i1].append(fp)
            return True
        if len(self._buckets[i2]) < self.BUCKET_SIZE:
            self._buckets[i2].append(fp)
            return True

        # Cuckoo eviction: kick out an existing entry, re-insert elsewhere
        cur_i = i1
        cur_fp = fp
        for _ in range(self.MAX_KICKS):
            evict_idx = 0  # always evict first slot (deterministic)
            evicted_fp = self._buckets[cur_i][evict_idx]
            self._buckets[cur_i][evict_idx] = cur_fp
            cur_fp = evicted_fp

            alt_i = (cur_i ^ cur_fp) % self.num_buckets
            if len(self._buckets[alt_i]) < self.BUCKET_SIZE:
                self._buckets[alt_i].append(cur_fp)
                return True
            cur_i = alt_i

        return False  # filter is full

    def delete(self, item: str) -> bool:
        """
        Remove one occurrence of *item* from the filter.
        Critical for GC: allows ref-count-zero chunks to be evicted.
        Returns True if removed, False if not found.
        """
        fp = fingerprint(item)
        i1 = self._i1(item)
        i2 = self._i2(i1, fp)
        if fp in self._buckets[i1]:
            self._buckets[i1].remove(fp)
            return True
        if fp in self._buckets[i2]:
            self._buckets[i2].remove(fp)
            return True
        return False
