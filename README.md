# DedupSnap

Content-addressable deduplicating backup with a two-tier Merkle manifest,
Merkle Mountain Range timeline, and Cuckoo-Filter index.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        CLI  (cli.py)                        │
│   init │ backup │ restore │ audit │ verify │ stats          │
└──────────────┬──────────────────────────────────────────────┘
               │
       ┌ ──────▼──────── ┐
       │  Repo (repo.py)│  ←  MMR + CuckooFilter on disk
       └──┬───────┬──────┘
          │       │
  ┌───────▼──┐ ┌──▼────────────┐
  │ Scanner  │ │  Adaptive     │
  │(scanner) │ │  Switcher     │  Stage 1: magic bytes
  └──────────┘ │(adaptive_sw.) │  Stage 2: Shannon entropy
               └──┬────────────┘
                  │ ChunkingPolicy
         ┌────────▼────────────┐
         │ Gear-hash CDC       │  SIMD_STUB: AVX-512 parallel
         │ (chunker.py)        │  prefix strategy (Eq. 2)
         └────────┬────────────┘
                  │ chunks (bytes)
      ┌───────────▼───────────────┐
      │  CAS  (cas.py)            │
      │  .dedupsnap/cas/<h2>/<h>  │  atomic write, hash verify
      └──────────┬────────────────┘
                 │ chunk_id (bytes)
  ┌──────────────▼──────────────────────────────────┐
  │          Hasher (hasher.py)                     │
  │  \x01 chunk  \x02 file-leaf  \x03 node          │
  │  \x04 snap-leaf  (domain separation)            │
  └──────────────┬──────────────────────────────────┘
                 │
  ┌──────────────▼──────────────────────────────────┐
  │        Two-Tier Merkle (merkle.py)              │
  │  Tier 2: chunk hashes  →  File Digest (FD)      │
  │  Tier 1: (FD, meta)    →  Global Snap Root (GSR)│
  └──────────────┬──────────────────────────────────┘
                 │ GSR
  ┌──────────────▼──────────────────────────────────┐
  │   MMR (mmr.py)  —  .dedupsnap/mmr.json          │
  │   append(GSR)  →  Log Root (tamper-evident)     │
  └─────────────────────────────────────────────────┘

  CuckooFilter (.dedupsnap/cuckoo.json)  fast O(1) dedup lookup
  SQLite DB    (.dedupsnap/metadata.db)  snapshots/files/chunks
  Stats cache  (.dedupsnap/stats_cache.json)  for dashboard
```

---

## Quick Start

```bash
# Install
pip install -e .
pip install streamlit plotly pandas   # for frontend

# Init a repo
dedupsnap init ./my_repo

# Back up a directory
dedupsnap backup ./my_repo ./my_data --name "v1"

# List snapshots
dedupsnap list ./my_repo

# Restore a snapshot
dedupsnap restore ./my_repo <snapshot_id> ./restored

# Audit (P-PoR — 460 samples → 99% confidence at 1% corruption)
dedupsnap audit ./my_repo <snapshot_id> --samples 460

# Verify a single file
dedupsnap verify ./my_repo <snapshot_id> path/to/file.txt

# Print stats
dedupsnap stats ./my_repo

# Launch the Streamlit dashboard
bash frontend/run.sh
# or: streamlit run frontend/app.py
```

---

## Module Reference

| Module | Role |
|---|---|
| `cli.py` | Click CLI entry points |
| `repo.py` | `Repo` — opens DB, MMR, CuckooFilter; writes stats cache |
| `scanner.py` | Recursive directory scan, normalised UTF-8 NFC paths |
| `adaptive_switcher.py` | Magic-byte + Shannon entropy → `ChunkingPolicy` |
| `chunker.py` | Gear-hash CDC (scalar; SIMD stub) + fixed-size FSC |
| `hasher.py` | Domain-tagged SHA-256: `\x01` chunk, `\x02` file-leaf, `\x03` node, `\x04` snap-leaf |
| `cas.py` | Content-addressable blob store with atomic writes |
| `merkle.py` | Two-tier Merkle manifest + inclusion proof + verification |
| `mmr.py` | Merkle Mountain Range append-only log |
| `cuckoo_filter.py` | Cuckoo Filter for O(1) chunk dedup + GC-safe delete |
| `db.py` | SQLite schema, migrations, helpers |
| `restore.py` | Reassemble files from CAS, restore mtime/chmod |
| `frontend/app.py` | Streamlit dashboard (5 pages) |

---

## Paper Reference

This implementation follows the architecture described in:

> *"Towards Efficient and Trustworthy Deduplication-Based Backup Systems:
> A Content-Defined Chunking and Hierarchical Merkle Approach"*

Key sections implemented:
- **Section IV-A** — SIMD-Accelerated Gear-Hash CDC  
- **Section IV-B** — Adaptive Chunking Strategy Switcher  
- **Section IV-C** — Cuckoo Filter Index  
- **Section V-A**  — Two-Tier Hierarchical Merkle Manifest  
- **Section V-B**  — Merkle Mountain Range authenticated log  
- **Section VI**   — Probabilistic Proof of Retrievability (P-PoR)
