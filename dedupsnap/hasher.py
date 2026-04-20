# dedupsnap/hasher.py
"""
Domain-tagged SHA-256 hashing per Section IV-A / V-A of the paper.

Tags prevent second-preimage attacks across hash domains:
  \x01  CAS chunk leaf
  \x02  Tier-2 file-chunk leaf (wraps a chunk hash)
  \x03  Merkle internal node (both tiers)
  \x04  Tier-1 snapshot file leaf (wraps FD + metadata)
"""
import hashlib

TAG_CHUNK = b'\x01'
TAG_FILE  = b'\x02'
TAG_NODE  = b'\x03'
TAG_SNAP  = b'\x04'


def _sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def hash_chunk(data: bytes) -> str:
    """Hash raw chunk bytes → hex string.  Uses \\x01 domain tag."""
    return _sha256_hex(TAG_CHUNK + data)


def hash_file_leaf(chunk_hash: str) -> str:
    """Wrap a chunk hash as a Tier-2 Merkle leaf.  Uses \\x02 domain tag.
    See Section V-A: Two-Tier Hierarchical Merkle Manifest."""
    return _sha256_hex(TAG_FILE + bytes.fromhex(chunk_hash))


def hash_node(left: str, right: str) -> str:
    """Hash two Merkle children → hex string.  Uses \\x03 domain tag."""
    return _sha256_hex(TAG_NODE + bytes.fromhex(left) + bytes.fromhex(right))


def hash_snap_leaf(fd: str, path: str, mtime: int, mode: int) -> str:
    """Hash a (file-digest, metadata) pair as a Tier-1 leaf.  Uses \\x04 domain tag."""
    payload = f"{fd}|{path}|{mtime}|{mode}".encode("utf-8")
    return _sha256_hex(TAG_SNAP + payload)
