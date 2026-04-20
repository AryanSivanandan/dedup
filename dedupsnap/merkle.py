# dedupsnap/merkle.py
"""
Two-Tier Hierarchical Merkle Manifest.
See Section V-A of the paper.

Tier 2  (file level)  : chunk hashes → File Digest (FD)
Tier 1  (snapshot level): (FD, metadata) pairs → Global Snapshot Root (GSR)

The `levels` returned by build_* functions are ordered leaf→root so that
get_inclusion_proof and verify_proof can walk them uniformly.
"""
from typing import List, Tuple
from dedupsnap.hasher import hash_file_leaf, hash_node, hash_snap_leaf

_ZERO_HASH = "00" * 32


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_tree(leaves: List[str]) -> Tuple[str, List[List[str]]]:
    """
    Build a binary Merkle tree over *leaves* (hex strings).
    Odd-length levels duplicate the last node (standard RFC 6962 convention).
    Returns (root_hex, levels) where levels[0] == leaves, levels[-1] == [root].
    """
    if not leaves:
        root = hash_node(_ZERO_HASH, _ZERO_HASH)
        return root, [[root]]

    levels: List[List[str]] = [leaves[:]]
    current = leaves[:]

    while len(current) > 1:
        next_level: List[str] = []
        for i in range(0, len(current), 2):
            left = current[i]
            right = current[i + 1] if i + 1 < len(current) else left
            next_level.append(hash_node(left, right))
        levels.append(next_level)
        current = next_level

    return current[0], levels


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_file_tree(chunk_hashes: List[str]) -> Tuple[str, List[List[str]]]:
    """
    Tier-2: File-Level Chunk Manifest.
    Leaves  = hash_file_leaf(chunk_hash)  for each chunk hash.
    Root    = File Digest (FD).
    See Section V-A.
    """
    if not chunk_hashes:
        return _build_tree([])
    leaves = [hash_file_leaf(ch) for ch in chunk_hashes]
    return _build_tree(leaves)


def build_snapshot_tree(
    file_entries: List[Tuple[str, dict]],
) -> Tuple[str, List[List[str]]]:
    """
    Tier-1: Snapshot-Level File Manifest.
    Leaves  = hash_snap_leaf(fd, path, mtime, mode)  for each (fd, meta) pair.
    Root    = Global Snapshot Root (GSR).
    See Section V-A.

    Each *meta* dict should contain: path (str), mtime (int), mode (int).
    """
    if not file_entries:
        return _build_tree([])
    leaves = [
        hash_snap_leaf(
            fd,
            meta.get("path", ""),
            int(meta.get("mtime", 0)),
            int(meta.get("mode", 0)),
        )
        for fd, meta in file_entries
    ]
    return _build_tree(leaves)


def get_inclusion_proof(leaf_index: int, levels: List[List[str]]) -> List[str]:
    """
    Return the sibling-hash path from *leaf_index* up to (but not including)
    the root.  Pass the *levels* list from build_file_tree / build_snapshot_tree.
    """
    proof: List[str] = []
    idx = leaf_index
    for level in levels[:-1]:  # every level except the single-element root level
        n = len(level)
        if idx % 2 == 0:
            sibling = idx + 1 if idx + 1 < n else idx   # duplicate if last
        else:
            sibling = idx - 1
        proof.append(level[sibling])
        idx //= 2
    return proof


# Aliases matching the spec names
get_file_inclusion_proof     = get_inclusion_proof
get_snapshot_inclusion_proof = get_inclusion_proof


def verify_proof(
    leaf_hash: str,
    proof: List[str],
    root: str,
    leaf_index: int,
) -> bool:
    """
    Verify a Merkle inclusion proof.
    *leaf_hash*  : the hash of the leaf being proved (hex).
    *proof*      : sibling hashes from get_inclusion_proof (hex list).
    *root*       : expected Merkle root (hex).
    *leaf_index* : original index of the leaf in the tree.
    Returns True iff the proof is valid and the recomputed root matches *root*.
    """
    current = leaf_hash
    idx = leaf_index
    for sibling in proof:
        if idx % 2 == 0:
            current = hash_node(current, sibling)
        else:
            current = hash_node(sibling, current)
        idx //= 2
    return current == root
