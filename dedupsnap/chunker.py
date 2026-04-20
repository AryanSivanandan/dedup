# dedupsnap/chunker.py
"""
Content-Defined Chunking using a Gear-hash rolling hash.
See Section IV-A: SIMD-Accelerated Gear-Hash CDC.

# SIMD_STUB:
# AVX-512 vectorization would parallelize the Gear-hash rolling window using
# the parallel prefix strategy per Eq. 2 of the paper.  The 256-bit SIMD
# registers would process 4 uint64 lanes simultaneously, computing
#   gear[data[i]] << 1
# in parallel across lanes, then reducing with horizontal OR to find the
# first boundary position within the 4-byte window.  A scalar fallback
# handles the remainder bytes at the end of each 4-lane group.
"""
import hashlib
from enum import Enum
from typing import Iterator


class ChunkingPolicy(Enum):
    FINE_CDC     = "fine_cdc"      # ~4 KB average  (text, source code)
    STANDARD_CDC = "standard_cdc"  # ~8 KB average  (general data)
    LARGE_FSC    = "large_fsc"     # 64 KB fixed    (already-compressed data)


# 256-entry Gear table: deterministic uint64 values derived from SHA-256 of
# each byte value so the table is reproducible across platforms.
_GEAR_TABLE: list = [
    int.from_bytes(hashlib.sha256(bytes([i])).digest()[:8], "big")
    for i in range(256)
]

_UINT64_MASK = 0xFFFFFFFFFFFFFFFF

# (avg_size, min_size, max_size, mask)
_POLICY_PARAMS = {
    ChunkingPolicy.FINE_CDC:     (4096,  1024,  32768),
    ChunkingPolicy.STANDARD_CDC: (8192,  2048,  65536),
    ChunkingPolicy.LARGE_FSC:    (65536, 65536, 65536),
}


def _mask_for_avg(avg: int) -> int:
    """Return a bitmask with (log2(avg)) low bits set."""
    bits = avg.bit_length() - 1
    return (1 << bits) - 1


def iter_chunks(
    data: bytes,
    policy: ChunkingPolicy = ChunkingPolicy.STANDARD_CDC,
) -> Iterator[bytes]:
    """
    Yield variable-length (or fixed-length for LARGE_FSC) chunks from *data*.

    For FINE_CDC / STANDARD_CDC: scalar Gear-hash CDC.
    For LARGE_FSC: simple fixed-size split (high-entropy / pre-compressed data).

    See Section IV-A: SIMD-Accelerated Gear-Hash CDC.
    """
    avg, min_size, max_size = _POLICY_PARAMS[policy]

    if policy is ChunkingPolicy.LARGE_FSC:
        for i in range(0, max(len(data), 1), max_size):
            chunk = data[i : i + max_size]
            if chunk:
                yield chunk
        return

    mask = _mask_for_avg(avg)
    n = len(data)
    pos = 0

    while pos < n:
        end = min(pos + max_size, n)

        if end == n and (end - pos) <= min_size:
            # Tail: emit whatever remains
            yield data[pos:end]
            return

        # Roll Gear hash from (pos + min_size) up to (pos + max_size)
        gear: int = 0
        cut = min(pos + min_size, end)

        while cut < end:
            gear = ((gear << 1) + _GEAR_TABLE[data[cut]]) & _UINT64_MASK
            cut += 1
            if (gear & mask) == 0:
                break  # boundary found

        yield data[pos:cut]
        pos = cut
