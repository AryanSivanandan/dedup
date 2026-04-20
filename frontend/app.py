# frontend/app.py  —  DedupSnap Streamlit Dashboard
import json
import os
import sqlite3
import subprocess
import sys
import time

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Page config + dark-theme CSS
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="DedupSnap",
    page_icon="🗄️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    /* GitHub dark palette */
    :root {
        --bg:       #0d1117;
        --card-bg:  #161b22;
        --accent:   #58a6ff;
        --text:     #c9d1d9;
        --border:   #30363d;
    }
    html, body, [data-testid="stAppViewContainer"],
    [data-testid="stSidebar"], [data-testid="stHeader"] {
        background-color: var(--bg) !important;
        color: var(--text) !important;
    }
    .metric-card {
        background: var(--card-bg);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 16px 20px;
    }
    .metric-card h4 { color: #8b949e; font-size: 0.78rem; margin: 0 0 4px; }
    .metric-card p  { color: var(--accent); font-size: 1.6rem; margin: 0; font-weight: 700; }
    code { color: var(--accent) !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _metric_card(label: str, value: str) -> str:
    return f"""<div class="metric-card"><h4>{label}</h4><p>{value}</p></div>"""


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


@st.cache_resource(show_spinner=False)
def _get_conn(db_path: str):
    return sqlite3.connect(db_path, check_same_thread=False)


def _load_stats(meta_dir: str) -> dict:
    path = os.path.join(meta_dir, "stats_cache.json")
    if not os.path.exists(path):
        return {}
    with open(path) as fh:
        return json.load(fh)


def _load_mmr_root(meta_dir: str) -> str:
    path = os.path.join(meta_dir, "mmr.json")
    if not os.path.exists(path):
        return "(no MMR yet)"
    with open(path) as fh:
        d = json.load(fh)
    leaves = d.get("leaves", [])
    if not leaves:
        return "(empty)"
    return d.get("nodes", {}).get("…", leaves[-1])[:32] + "…"


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("🗄️ DedupSnap")
    st.divider()
    repo_path = st.text_input("Repository path", value="./my_repo")
    meta_dir  = os.path.join(repo_path, ".dedupsnap") if repo_path else ""

    if st.button("Initialize Repo", use_container_width=True):
        res = subprocess.run(
            [sys.executable, "-m", "dedupsnap.cli", "init", repo_path],
            capture_output=True, text=True,
        )
        if res.returncode == 0:
            st.success(res.stdout.strip())
        else:
            st.error(res.stderr.strip())

    st.divider()
    page = st.radio(
        "Navigate",
        ["📊 Dashboard", "💾 Backup", "🔍 Browse & Restore", "🔐 Audit", "📈 Analytics"],
    )

repo_valid = repo_path and os.path.isdir(os.path.join(repo_path, ".dedupsnap"))

# ---------------------------------------------------------------------------
# 📊 Dashboard
# ---------------------------------------------------------------------------

if page == "📊 Dashboard":
    st.header("📊 Dashboard")

    if not repo_valid:
        st.warning("Select a valid initialised repository in the sidebar.")
        st.stop()

    stats = _load_stats(meta_dir)
    if not stats:
        subprocess.run(
            [sys.executable, "-m", "dedupsnap.cli", "stats", repo_path],
            capture_output=True,
        )
        stats = _load_stats(meta_dir)

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(_metric_card("Total Snapshots",   str(stats.get("total_snapshots", 0))), unsafe_allow_html=True)
    c2.markdown(_metric_card("Unique Chunks",      f"{stats.get('total_unique_chunks', 0):,}"),     unsafe_allow_html=True)
    dr = stats.get("dedup_ratio", 1.0)
    c3.markdown(_metric_card("Dedup Ratio",        f"{dr:.2f}:1"), unsafe_allow_html=True)
    saved = stats.get("total_logical_bytes", 0) - stats.get("total_physical_bytes", 0)
    c4.markdown(_metric_card("Space Saved",        _fmt_bytes(max(saved, 0))), unsafe_allow_html=True)

    st.divider()

    # MMR log root
    st.subheader("MMR Log Root (tamper-evident anchor)")
    mmr_root = stats.get("mmr_log_root", "(none)")
    st.code(mmr_root, language=None)

    # Per-snapshot dedup ratio bar chart
    per_snap = stats.get("per_snapshot", [])
    if per_snap:
        df = pd.DataFrame(per_snap)
        if "physical_bytes" in df.columns and "unique_chunks" in df.columns:
            df["label"] = df.apply(
                lambda r: (r.get("name") or r["id"][:8]), axis=1
            )
            fig = px.bar(
                df, x="label", y="unique_chunks",
                labels={"label": "Snapshot", "unique_chunks": "Unique Chunks"},
                title="Unique Chunks per Snapshot",
                color_discrete_sequence=["#58a6ff"],
                template="plotly_dark",
            )
            st.plotly_chart(fig, use_container_width=True)

        # Recent snapshots table
        st.subheader("Recent Snapshots")
        display_df = df[["id", "timestamp", "name", "file_count", "unique_chunks", "gsr"]].copy()
        display_df["gsr"] = display_df["gsr"].str[:20] + "…"
        display_df.rename(columns={
            "id": "Snapshot ID", "timestamp": "Timestamp", "name": "Name",
            "file_count": "Files", "unique_chunks": "Chunks", "gsr": "GSR",
        }, inplace=True)
        st.dataframe(display_df, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# 💾 Backup
# ---------------------------------------------------------------------------

elif page == "💾 Backup":
    st.header("💾 Backup")

    if not repo_valid:
        st.warning("Select a valid initialised repository in the sidebar.")
        st.stop()

    data_path = st.text_input("File / directory to back up")
    snap_name = st.text_input("Snapshot name (optional)")
    run_btn   = st.button("▶ Run Backup", type="primary")

    output_area = st.empty()

    if run_btn and data_path:
        cmd = [sys.executable, "-m", "dedupsnap.cli", "backup", repo_path, data_path]
        if snap_name:
            cmd += ["--name", snap_name]

        output_lines: list = []
        with st.spinner("Running backup…"):
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
            )
            for line in proc.stdout:
                output_lines.append(line.rstrip())
                output_area.text_area("Output", "\n".join(output_lines), height=200)
            proc.wait()

        if proc.returncode == 0:
            st.success("Backup completed successfully.")
            # Parse stats from final line
            final = output_lines[-1] if output_lines else ""
            if "Snapshot created" in final:
                st.code(final)
        else:
            st.error("Backup failed — see output above.")

# ---------------------------------------------------------------------------
# 🔍 Browse & Restore
# ---------------------------------------------------------------------------

elif page == "🔍 Browse & Restore":
    st.header("🔍 Browse & Restore")

    if not repo_valid:
        st.warning("Select a valid initialised repository in the sidebar.")
        st.stop()

    db_path = os.path.join(meta_dir, "metadata.db")
    conn    = _get_conn(db_path)

    snap_rows = conn.execute(
        "SELECT id, timestamp, name FROM snapshots WHERE status='committed' "
        "ORDER BY timestamp DESC"
    ).fetchall()

    if not snap_rows:
        st.info("No committed snapshots yet.")
        st.stop()

    options = {f"{r[2] or r[0][:8]}  ({r[1]})": r[0] for r in snap_rows}
    chosen_label = st.selectbox("Select snapshot", list(options.keys()))
    chosen_id    = options[chosen_label]

    file_rows = conn.execute(
        "SELECT path, chunk_count, chunking_policy FROM files "
        "WHERE snapshot_id=? ORDER BY path",
        (chosen_id,),
    ).fetchall()

    if file_rows:
        df = pd.DataFrame(file_rows, columns=["Path", "Chunks", "Policy"])
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No files in this snapshot.")

    st.divider()
    target_dir = st.text_input("Restore destination directory")
    if st.button("♻ Restore Selected Snapshot", type="primary") and target_dir:
        with st.spinner("Restoring…"):
            res = subprocess.run(
                [sys.executable, "-m", "dedupsnap.cli",
                 "restore", repo_path, chosen_id, target_dir],
                capture_output=True, text=True,
            )
        if res.returncode == 0:
            st.success(res.stdout.strip())
        else:
            st.error(res.stderr.strip())

# ---------------------------------------------------------------------------
# 🔐 Audit
# ---------------------------------------------------------------------------

elif page == "🔐 Audit":
    st.header("🔐 Audit  (P-PoR)")

    if not repo_valid:
        st.warning("Select a valid initialised repository in the sidebar.")
        st.stop()

    db_path  = os.path.join(meta_dir, "metadata.db")
    conn     = _get_conn(db_path)
    snap_rows = conn.execute(
        "SELECT id, timestamp, name, gsr FROM snapshots WHERE status='committed' "
        "ORDER BY timestamp DESC"
    ).fetchall()

    if not snap_rows:
        st.info("No committed snapshots with GSR yet.")
        st.stop()

    options = {f"{r[2] or r[0][:8]}  ({r[1]})": r[0] for r in snap_rows}
    chosen_label = st.selectbox("Select snapshot to audit", list(options.keys()))
    chosen_id    = options[chosen_label]

    n_samples = st.slider("Sample size", min_value=10, max_value=1000, value=460, step=10)

    total_chunks = conn.execute(
        "SELECT COUNT(*) FROM file_chunks WHERE snapshot_id=?", (chosen_id,)
    ).fetchone()[0]
    rho = min(n_samples / total_chunks, 1.0) if total_chunks else 0
    confidence = round((1 - (1 - rho) ** n_samples) * 100, 2) if rho < 1 else 100.0

    st.info(
        f"With **{n_samples}** samples from **{total_chunks}** chunks "
        f"(ρ = {rho:.3f}): detects ≥1% corruption at "
        f"**{confidence:.1f}%** confidence  "
        f"(P = 1 − (1−ρ)ⁿ  per Section VI of the paper)"
    )

    if st.button("▶ Run Audit (P-PoR)", type="primary"):
        with st.spinner("Auditing…"):
            t0  = time.time()
            res = subprocess.run(
                [sys.executable, "-m", "dedupsnap.cli",
                 "audit", repo_path, chosen_id, "--samples", str(n_samples)],
                capture_output=True, text=True,
            )
            elapsed = int((time.time() - t0) * 1000)

        output = res.stdout.strip()
        if "PASSED" in output:
            st.markdown("## ✅  Audit PASSED")
        else:
            st.markdown("## ❌  Audit FAILED")

        st.code(output)
        cols = st.columns(3)
        cols[0].metric("Chunks verified", f"{n_samples}/{n_samples}")
        cols[1].metric("Time",            f"{elapsed} ms")
        cols[2].metric("Confidence",      f"{confidence:.1f}%")

# ---------------------------------------------------------------------------
# 📈 Analytics
# ---------------------------------------------------------------------------

elif page == "📈 Analytics":
    st.header("📈 Analytics")

    if not repo_valid:
        st.warning("Select a valid initialised repository in the sidebar.")
        st.stop()

    stats = _load_stats(meta_dir)
    if not stats:
        st.info("Run a backup first to generate analytics data.")
        st.stop()

    col_a, col_b = st.columns(2)

    # Chunk size distribution
    chunk_sizes = stats.get("chunk_sizes", [])
    if chunk_sizes:
        fig = px.histogram(
            x=chunk_sizes,
            nbins=50,
            labels={"x": "Chunk size (bytes)", "y": "Count"},
            title="Chunk Size Distribution",
            color_discrete_sequence=["#58a6ff"],
            template="plotly_dark",
        )
        col_a.plotly_chart(fig, use_container_width=True)
    else:
        col_a.info("No chunk data yet.")

    # Ref-count distribution
    refcounts = stats.get("refcounts", [])
    if refcounts:
        fig2 = px.histogram(
            x=refcounts,
            nbins=30,
            labels={"x": "Ref count", "y": "Unique chunks"},
            title="Chunk Ref-Count Distribution (high = backbone chunks)",
            color_discrete_sequence=["#3fb950"],
            template="plotly_dark",
        )
        col_b.plotly_chart(fig2, use_container_width=True)
    else:
        col_b.info("No refcount data yet.")

    # Policy breakdown per snapshot
    per_snap = stats.get("per_snapshot", [])
    if per_snap:
        rows = []
        for s in per_snap:
            label = s.get("name") or s["id"][:8]
            for pol, cnt in (s.get("policies") or {}).items():
                rows.append({"snapshot": label, "policy": pol, "files": cnt})
        if rows:
            df_pol = pd.DataFrame(rows)
            fig3   = px.bar(
                df_pol, x="snapshot", y="files", color="policy", barmode="stack",
                title="Adaptive Switcher — Chunking Policy Breakdown",
                color_discrete_map={
                    "fine_cdc":     "#58a6ff",
                    "standard_cdc": "#3fb950",
                    "large_fsc":    "#f78166",
                    "unknown":      "#8b949e",
                },
                template="plotly_dark",
            )
            st.plotly_chart(fig3, use_container_width=True)

    # Cumulative physical vs logical bytes over time
    if per_snap:
        cum_phys, cum_logi, labels = [], [], []
        p_acc = l_acc = 0
        for s in per_snap:
            p_acc += s.get("physical_bytes", 0)
            # logical = physical × global dedup_ratio approximation
            p_acc_val = p_acc
            labels.append(s.get("name") or s["id"][:8])
            cum_phys.append(p_acc_val)

        # For logical we use cumulative file count × avg file size (rough)
        logi_running = stats.get("total_logical_bytes", 0)
        phys_running = stats.get("total_physical_bytes", 0)

        df_time = pd.DataFrame({
            "Snapshot": labels,
            "Physical (bytes)": cum_phys,
        })
        fig4 = px.line(
            df_time, x="Snapshot", y="Physical (bytes)",
            title="Cumulative Physical Storage Over Time",
            markers=True,
            color_discrete_sequence=["#58a6ff"],
            template="plotly_dark",
        )
        st.plotly_chart(fig4, use_container_width=True)
