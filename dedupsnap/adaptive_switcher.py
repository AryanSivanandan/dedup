# dedupsnap/adaptive_switcher.py
"""
Adaptive Chunking Strategy Switcher.
See Section IV-B: Magic-Byte Classifier + Shannon Entropy Fallback.

Stage 1: magic-byte header lookup for known high-entropy container formats.
Stage 2: Shannon entropy on the first 4 KB sample to classify unknown files.
"""
import math
from dedupsnap.chunker import ChunkingPolicy


# (magic_prefix_bytes, policy)
_MAGIC_TABLE: list = [
    (b'\x50\x4B\x03\x04', ChunkingPolicy.LARGE_FSC),  # ZIP
    (b'\x1F\x8B',         ChunkingPolicy.LARGE_FSC),  # GZIP
    (b'\x25\x50\x44\x46', ChunkingPolicy.LARGE_FSC),  # PDF  (%PDF)
    (b'\x89\x50\x4E\x47', ChunkingPolicy.LARGE_FSC),  # PNG
    (b'\xFF\xD8\xFF',     ChunkingPolicy.LARGE_FSC),  # JPEG
    (b'\x37\x7A\xBC\xAF', ChunkingPolicy.LARGE_FSC),  # 7-Zip
    (b'\x28\xB5\x2F\xFD', ChunkingPolicy.LARGE_FSC),  # Zstandard
]

_ENTROPY_HIGH = 7.5   # → LARGE_FSC  (nearly random / encrypted)
_ENTROPY_LOW  = 5.0   # → FINE_CDC   (highly redundant text/source)
_SAMPLE_SIZE  = 4096


def _shannon_entropy(data: bytes) -> float:
    """H = -Σ p(b) · log₂ p(b) over all 256 byte values."""
    if not data:
        return 0.0
    counts = [0] * 256
    for b in data:
        counts[b] += 1
    n = len(data)
    h = 0.0
    for c in counts:
        if c:
            p = c / n
            h -= p * math.log2(p)
    return h


def classify(file_path: str) -> ChunkingPolicy:
    """
    Return the recommended ChunkingPolicy for *file_path*.

    Stage 1: match magic bytes (O(1) header read).
    Stage 2: Shannon entropy on first 4 KB sample.
    Falls back to STANDARD_CDC on any I/O error.
    """
    try:
        with open(file_path, "rb") as fh:
            header = fh.read(8)
            fh.seek(0)
            sample = fh.read(_SAMPLE_SIZE)
    except (IOError, OSError):
        return ChunkingPolicy.STANDARD_CDC

    # Stage 1: magic-byte lookup
    for magic, policy in _MAGIC_TABLE:
        if header[: len(magic)] == magic:
            return policy

    # Stage 2: entropy-based classification
    entropy = _shannon_entropy(sample)
    if entropy > _ENTROPY_HIGH:
        return ChunkingPolicy.LARGE_FSC
    if entropy < _ENTROPY_LOW:
        return ChunkingPolicy.FINE_CDC
    return ChunkingPolicy.STANDARD_CDC
