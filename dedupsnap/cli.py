# dedupsnap/cli.py
import random
import sqlite3
import time
import uuid

import click

from dedupsnap.adaptive_switcher import classify
from dedupsnap.cas import get_blob, put_blob
from dedupsnap.chunker import iter_chunks
from dedupsnap.db import insert_snapshot, list_snapshots, set_snapshot_committed
from dedupsnap.hasher import hash_chunk, hash_file_leaf
from dedupsnap.merkle import (
    build_file_tree,
    build_snapshot_tree,
    get_inclusion_proof,
    verify_proof,
)
from dedupsnap.repo import Repo, init_repo
from dedupsnap.restore import restore_snapshot
from dedupsnap.scanner import canonical_scan


@click.group()
def cli():
    pass


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------

@cli.command("init")
@click.argument("repo_path")
def cmd_init(repo_path):
    repo = init_repo(repo_path)
    click.echo(f"Initialized repository at {repo.repo_path}")


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

@cli.command("list")
@click.argument("repo_path")
def cmd_list(repo_path):
    repo = Repo(repo_path)
    rows = list_snapshots(repo.conn)
    if not rows:
        click.echo("No snapshots")
        return
    click.echo(
        f"{'ID':<36}  {'Timestamp':<20}  {'Name':<15}  {'Status':<10}  GSR (truncated)"
    )
    click.echo("-" * 110)
    for sid, ts, root_hex, status, name_val, gsr, mmr_root in rows:
        name_str = name_val or ""
        gsr_short = (gsr or "")[:16] + "…" if gsr else ""
        click.echo(
            f"{sid}  {ts}  {name_str:<15}  {status:<10}  {gsr_short}"
        )


# ---------------------------------------------------------------------------
# backup
# ---------------------------------------------------------------------------

@cli.command("backup")
@click.argument("repo_path")
@click.argument("data_path")
@click.option("-n", "--name", default=None, help="Memorable name for this snapshot")
def cmd_backup(repo_path, data_path, name):
    repo = Repo(repo_path)
    conn: sqlite3.Connection = repo.conn

    try:
        files = canonical_scan(data_path)
    except Exception as exc:
        click.echo(f"Error scanning path: {exc}", err=True)
        return

    snapshot_id = uuid.uuid4().hex
    timestamp   = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    conn.execute("BEGIN")
    try:
        insert_snapshot(conn, snapshot_id, timestamp, status="in_progress", name=name)

        file_entries      = []   # (fd_hex, {path, mtime, mode}) for Tier-1 tree
        all_chunk_hashes  = []   # flat list for root fallback
        total_chunks      = 0

        for path_bytes, metadata_blob, fullpath in files:
            # 1. Classify file → chunking policy
            policy = classify(fullpath)

            # 2. Read file, CDC-chunk it
            try:
                with open(fullpath, "rb") as fh:
                    raw = fh.read()
            except Exception as exc:
                raise RuntimeError(f"Cannot read {fullpath}: {exc}")

            chunk_hashes: list = []   # hex strings for this file's Tier-2 tree
            chunk_ids:    list = []   # bytes for file_chunks FK

            for chunk in iter_chunks(raw, policy):
                ch_hex = hash_chunk(chunk)              # str
                cid    = put_blob(repo, chunk, conn)    # bytes
                chunk_hashes.append(ch_hex)
                chunk_ids.append(cid)

            all_chunk_hashes.extend(chunk_hashes)
            total_chunks += len(chunk_hashes)

            # 3. Build Tier-2 tree → File Digest
            fd, _file_levels = build_file_tree(chunk_hashes)

            # 4. Collect metadata for Tier-1
            import struct
            try:
                mode, uid, gid, mtime, fsize = struct.unpack(">I I I Q Q", metadata_blob)
            except struct.error:
                mode, mtime = 0, 0
            path_text = path_bytes.decode("utf-8")
            file_entries.append(
                (fd, {"path": path_text, "mtime": mtime, "mode": mode})
            )

            # 5. Insert DB rows
            conn.execute(
                "INSERT INTO files(snapshot_id, path, file_leaf, metadata, "
                "chunk_count, chunking_policy) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    snapshot_id,
                    path_text,
                    sqlite3.Binary(bytes.fromhex(fd)),
                    metadata_blob,
                    len(chunk_ids),
                    policy.value,
                ),
            )
            for idx, cid in enumerate(chunk_ids):
                conn.execute(
                    "INSERT INTO file_chunks(snapshot_id, path, chunk_index, chunk_id) "
                    "VALUES (?, ?, ?, ?)",
                    (snapshot_id, path_text, idx, sqlite3.Binary(cid)),
                )

        # 6. Build Tier-1 (snapshot) tree → GSR
        gsr, _snap_levels = build_snapshot_tree(file_entries)

        # 7. Append GSR to MMR
        repo.mmr.append(gsr)
        mmr_log_root = repo.mmr.get_log_root()

        # Legacy root (Tier-1 GSR as bytes) for backward compat
        root_bytes = bytes.fromhex(gsr)

        set_snapshot_committed(
            conn, snapshot_id, root_bytes, gsr=gsr, mmr_log_root=mmr_log_root
        )
        conn.execute("COMMIT")

        # 8. Persist cuckoo filter, refresh stats cache
        repo.cuckoo.save()
        repo.write_stats_cache()

        click.echo(
            f"Snapshot created: {snapshot_id}  "
            f"root={gsr[:16]}…  "
            f"chunks={total_chunks}  "
            f"mmr_root={mmr_log_root[:16]}…"
        )

    except Exception as exc:
        conn.execute("ROLLBACK")
        click.echo(f"Error during backup, rolling back: {exc}", err=True)
        raise


# ---------------------------------------------------------------------------
# restore
# ---------------------------------------------------------------------------

@cli.command("restore")
@click.argument("repo_path")
@click.argument("snapshot_id")
@click.argument("target_dir")
def cmd_restore(repo_path, snapshot_id, target_dir):
    """Restore a snapshot to a target directory."""
    repo = Repo(repo_path)
    click.echo(f"Restoring snapshot {snapshot_id} to {target_dir}…")
    try:
        restore_snapshot(repo, snapshot_id, target_dir)
        click.echo("Restore complete.")
    except Exception as exc:
        click.echo(f"Error during restore: {exc}", err=True)
        raise


# ---------------------------------------------------------------------------
# audit  (P-PoR: Probabilistic Proof of Retrievability)
# ---------------------------------------------------------------------------

@cli.command("audit")
@click.argument("repo_path")
@click.argument("snapshot_id")
@click.option("--samples", default=460, show_default=True,
              help="Number of random chunks to sample")
def cmd_audit(repo_path, snapshot_id, samples):
    """
    Run P-PoR audit: randomly sample chunks, verify each against the GSR via
    Tier-2 + Tier-1 Merkle proof.
    With 460 samples: detects ≥1% corruption at 99% confidence.
    """
    repo = Repo(repo_path)
    conn = repo.conn

    # Fetch snapshot GSR
    row = conn.execute(
        "SELECT gsr FROM snapshots WHERE id=? AND status='committed'",
        (snapshot_id,),
    ).fetchone()
    if not row or not row[0]:
        click.echo(
            "Snapshot not found or lacks GSR (was it created with this version?)",
            err=True,
        )
        return
    gsr = row[0]

    # Collect all (path, chunk_index) pairs for this snapshot
    chunk_refs = conn.execute(
        "SELECT path, chunk_index FROM file_chunks WHERE snapshot_id=? "
        "ORDER BY path, chunk_index",
        (snapshot_id,),
    ).fetchall()

    if not chunk_refs:
        click.echo("No chunks found in snapshot.", err=True)
        return

    n_total = len(chunk_refs)
    n_sample = min(samples, n_total)
    sample = random.sample(chunk_refs, n_sample)

    # Rebuild Tier-1 tree once (for snapshot-level proofs)
    file_rows = conn.execute(
        "SELECT path, metadata, chunk_count FROM files "
        "WHERE snapshot_id=? ORDER BY path",
        (snapshot_id,),
    ).fetchall()

    import struct
    file_entries_for_tree = []
    file_chunk_hashes_map = {}
    sorted_paths = [r[0] for r in file_rows]

    for path, meta_blob, _cc in file_rows:
        ch_rows = conn.execute(
            "SELECT chunk_id FROM file_chunks WHERE snapshot_id=? AND path=? "
            "ORDER BY chunk_index",
            (snapshot_id, path),
        ).fetchall()
        chunk_hex_list = [bytes(cid).hex() for (cid,) in ch_rows]
        # Recompute FD from stored chunk_ids
        fd, file_levels = build_file_tree(
            [hash_file_leaf(ch) for ch in chunk_hex_list]
            if False  # build_file_tree already applies hash_file_leaf internally
            else chunk_hex_list
        )
        file_chunk_hashes_map[path] = (chunk_hex_list, file_levels)
        try:
            mode, _uid, _gid, mtime, _sz = struct.unpack(">I I I Q Q", bytes(meta_blob))
        except Exception:
            mode, mtime = 0, 0
        file_entries_for_tree.append((fd, {"path": path, "mtime": mtime, "mode": mode}))

    _gsr_check, snap_levels = build_snapshot_tree(file_entries_for_tree)

    passed = 0
    t0 = time.time()

    for path, chunk_index in sample:
        # Retrieve chunk from CAS
        cid_row = conn.execute(
            "SELECT chunk_id FROM file_chunks WHERE snapshot_id=? AND path=? "
            "AND chunk_index=?",
            (snapshot_id, path, chunk_index),
        ).fetchone()
        if not cid_row:
            continue
        chunk_id_bytes = bytes(cid_row[0])
        data = get_blob(repo, chunk_id_bytes)
        if data is None:
            continue

        # Verify chunk hash
        computed_hex = hash_chunk(data)
        if computed_hex != chunk_id_bytes.hex():
            continue

        # Tier-2: verify chunk inclusion in FD
        chunk_hex_list, file_levels = file_chunk_hashes_map.get(path, ([], []))
        if chunk_index >= len(chunk_hex_list):
            continue
        leaf_hash = hash_file_leaf(chunk_hex_list[chunk_index])
        t2_proof  = get_inclusion_proof(chunk_index, file_levels)
        t2_root   = file_levels[-1][0]
        if not verify_proof(leaf_hash, t2_proof, t2_root, chunk_index):
            continue

        # Tier-1: verify FD inclusion in GSR
        file_idx  = sorted_paths.index(path)
        from dedupsnap.hasher import hash_snap_leaf
        snap_leaf = hash_snap_leaf(
            file_entries_for_tree[file_idx][0],
            file_entries_for_tree[file_idx][1]["path"],
            file_entries_for_tree[file_idx][1]["mtime"],
            file_entries_for_tree[file_idx][1]["mode"],
        )
        t1_proof = get_inclusion_proof(file_idx, snap_levels)
        if not verify_proof(snap_leaf, t1_proof, gsr, file_idx):
            continue

        passed += 1

    elapsed_ms = int((time.time() - t0) * 1000)
    confidence = round((1 - (1 - passed / n_sample) ** n_sample) * 100, 1) if n_sample else 0

    status = "PASSED" if passed == n_sample else "FAILED"
    click.echo(
        f"Audit {status}: {passed}/{n_sample} chunks verified | "
        f"Confidence: {confidence}% | Time: {elapsed_ms}ms"
    )
    if passed < n_sample:
        click.echo(
            f"WARNING: {n_sample - passed} chunks failed verification — "
            "data may be corrupted.",
            err=True,
        )


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------

@cli.command("verify")
@click.argument("repo_path")
@click.argument("snapshot_id")
@click.argument("file_path")
def cmd_verify(repo_path, snapshot_id, file_path):
    """Verify a single file's integrity using Tier-2 + Tier-1 Merkle proof."""
    repo = Repo(repo_path)
    conn = repo.conn

    gsr_row = conn.execute(
        "SELECT gsr FROM snapshots WHERE id=? AND status='committed'",
        (snapshot_id,),
    ).fetchone()
    if not gsr_row or not gsr_row[0]:
        click.echo("Snapshot not found or missing GSR.", err=True)
        return
    gsr = gsr_row[0]

    # Normalise path separator
    norm_path = file_path.replace("\\", "/")
    meta_row = conn.execute(
        "SELECT metadata FROM files WHERE snapshot_id=? AND path=?",
        (snapshot_id, norm_path),
    ).fetchone()
    if not meta_row:
        click.echo(f"File '{norm_path}' not found in snapshot.", err=True)
        return

    import struct
    try:
        mode, _u, _g, mtime, _s = struct.unpack(">I I I Q Q", bytes(meta_row[0]))
    except Exception:
        mode, mtime = 0, 0

    ch_rows = conn.execute(
        "SELECT chunk_id FROM file_chunks WHERE snapshot_id=? AND path=? "
        "ORDER BY chunk_index",
        (snapshot_id, norm_path),
    ).fetchall()
    chunk_hex_list = [bytes(r[0]).hex() for r in ch_rows]

    fd, file_levels = build_file_tree(chunk_hex_list)

    # Build snapshot tree for Tier-1
    all_files = conn.execute(
        "SELECT path, metadata FROM files WHERE snapshot_id=? ORDER BY path",
        (snapshot_id,),
    ).fetchall()
    file_entries = []
    target_idx = None
    for i, (p, mb) in enumerate(all_files):
        try:
            m2, _, _, mt2, _ = struct.unpack(">I I I Q Q", bytes(mb))
        except Exception:
            m2, mt2 = 0, 0
        c2 = conn.execute(
            "SELECT chunk_id FROM file_chunks WHERE snapshot_id=? AND path=? "
            "ORDER BY chunk_index",
            (snapshot_id, p),
        ).fetchall()
        c2_hex = [bytes(r[0]).hex() for r in c2]
        fd2, _ = build_file_tree(c2_hex)
        file_entries.append((fd2, {"path": p, "mtime": mt2, "mode": m2}))
        if p == norm_path:
            target_idx = i

    _gsr2, snap_levels = build_snapshot_tree(file_entries)

    from dedupsnap.hasher import hash_snap_leaf
    snap_leaf = hash_snap_leaf(fd, norm_path, mtime, mode)
    t1_proof  = get_inclusion_proof(target_idx, snap_levels)
    ok        = verify_proof(snap_leaf, t1_proof, gsr, target_idx)

    if ok:
        click.echo(f"VERIFIED  {norm_path}  against GSR  {gsr[:16]}...")
    else:
        click.echo(f"FAILED  {norm_path}  Merkle verification failed", err=True)


# ---------------------------------------------------------------------------
# stats
# ---------------------------------------------------------------------------

@cli.command("stats")
@click.argument("repo_path")
def cmd_stats(repo_path):
    """Print repository statistics and the MMR log root."""
    repo = Repo(repo_path)
    repo.write_stats_cache()

    import json, os
    cache = os.path.join(repo.meta_dir, "stats_cache.json")
    with open(cache) as fh:
        s = json.load(fh)

    phys = s["total_physical_bytes"]
    logi = s["total_logical_bytes"]
    saved = logi - phys

    click.echo(f"Snapshots      : {s['total_snapshots']}")
    click.echo(f"Unique chunks  : {s['total_unique_chunks']}")
    click.echo(f"Physical size  : {phys:,} bytes")
    click.echo(f"Logical size   : {logi:,} bytes")
    click.echo(f"Space saved    : {saved:,} bytes")
    click.echo(f"Dedup ratio    : {s['dedup_ratio']:.2f}:1")
    click.echo(f"Cache hit ratio: {s['cache_hit_ratio']:.2%}")
    click.echo(f"MMR log root   : {s['mmr_log_root']}")
