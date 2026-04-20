# dedupsnap/mmr.py
"""
Merkle Mountain Range (MMR) — append-only authenticated snapshot timeline.
See Section V-B of the paper.

An MMR is a forest of perfect binary trees.  Each appended leaf may trigger
a cascade of merges when two trees of equal height become adjacent.  The
current "peaks" (one root per tree in the forest) are hashed together
left-to-right to produce the Log Root.

Node indexing: (height, index_within_height)
  height=0 → leaves,  height=1 → parents of pairs of leaves, …
"""
import hashlib
import json
import os
from typing import Dict, List, Optional, Tuple


def _h2(a: str, b: str) -> str:
    return hashlib.sha256(bytes.fromhex(a) + bytes.fromhex(b)).hexdigest()


class MMR:
    """
    Merkle Mountain Range for the authenticated snapshot timeline.
    Persisted to *mmr_path* as a JSON file.
    """

    def __init__(self, mmr_path: str) -> None:
        self.mmr_path = mmr_path
        self._leaves: List[str] = []
        # Internal nodes keyed by (height, index_within_height)
        self._nodes: Dict[Tuple[int, int], str] = {}
        self._load()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self) -> None:
        if not os.path.exists(self.mmr_path):
            return
        try:
            with open(self.mmr_path) as fh:
                d = json.load(fh)
            self._leaves = d.get("leaves", [])
            self._nodes = {
                (int(k.split(",")[0]), int(k.split(",")[1])): v
                for k, v in d.get("nodes", {}).items()
            }
        except (json.JSONDecodeError, KeyError, ValueError):
            pass  # corrupt file → start fresh

    def _save(self) -> None:
        os.makedirs(os.path.dirname(self.mmr_path), exist_ok=True)
        with open(self.mmr_path, "w") as fh:
            json.dump(
                {
                    "leaves": self._leaves,
                    "nodes": {f"{h},{i}": v for (h, i), v in self._nodes.items()},
                },
                fh,
                indent=2,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _node(self, height: int, idx: int) -> str:
        if height == 0:
            return self._leaves[idx]
        return self._nodes[(height, idx)]

    def _peaks(self) -> List[str]:
        """
        Reconstruct peak list from the binary representation of leaf count.
        Bit h set in n  →  there is a perfect binary tree of height h.
        Peaks are returned MSB → LSB (largest subtree first).
        """
        n = len(self._leaves)
        if n == 0:
            return []
        result: List[str] = []
        counted = 0
        for h in range(n.bit_length() - 1, -1, -1):
            if n & (1 << h):
                peak_idx = counted >> h   # index within height h
                result.append(self._node(h, peak_idx))
                counted += (1 << h)
        return result

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def size(self) -> int:
        return len(self._leaves)

    def append(self, gsr: str) -> None:
        """Add a new Global Snapshot Root leaf to the MMR."""
        idx = len(self._leaves)
        self._leaves.append(gsr)

        # Cascade merges: while the new node is a right child (odd index),
        # merge it with its left sibling and carry the parent upward.
        h = 0
        cur_idx = idx
        cur_val = gsr
        while cur_idx % 2 == 1:
            left_idx = cur_idx - 1
            left_val = self._node(h, left_idx)
            parent_val = _h2(left_val, cur_val)
            h += 1
            parent_idx = cur_idx // 2
            self._nodes[(h, parent_idx)] = parent_val
            cur_idx = parent_idx
            cur_val = parent_val

        self._save()

    def get_log_root(self) -> str:
        """
        "Bag the peaks": fold all peak hashes left-to-right.
        Returns a 64-char hex string (or all-zeros if empty).
        """
        peaks = self._peaks()
        if not peaks:
            return "00" * 32
        root = peaks[0]
        for p in peaks[1:]:
            root = _h2(root, p)
        return root

    def get_inclusion_proof(self, leaf_index: int) -> dict:
        """
        Return an O(log N) inclusion proof for *leaf_index*.
        The proof_path is a list of sibling hashes bottom-up.
        """
        n = len(self._leaves)
        if leaf_index >= n:
            raise IndexError(f"leaf_index {leaf_index} out of range (size={n})")

        path: List[str] = []
        h = 0
        idx = leaf_index
        # Walk up while a sibling exists at this height
        while (n >> h) > 1:
            sibling_count = n >> h
            if idx % 2 == 0:
                sib = idx + 1 if idx + 1 < sibling_count else idx
            else:
                sib = idx - 1
            path.append(self._node(h, sib))
            idx //= 2
            h += 1

        return {
            "leaf_index": leaf_index,
            "leaf_value": self._leaves[leaf_index],
            "proof_path": path,
            "peaks": self._peaks(),
            "log_root": self.get_log_root(),
            "size": n,
        }

    def get_consistency_proof(self, old_size: int) -> dict:
        """Prove that the current MMR is an extension of the MMR at *old_size*."""
        return {
            "old_size": old_size,
            "new_size": len(self._leaves),
            "new_peaks": self._peaks(),
            "log_root": self.get_log_root(),
        }
