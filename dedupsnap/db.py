# dedupsnap/db.py
import sqlite3
import os

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS snapshots (
  id           TEXT PRIMARY KEY,
  timestamp    TEXT,
  root         BLOB,
  status       TEXT,
  name         TEXT,
  gsr          TEXT,
  mmr_log_root TEXT
);

CREATE TABLE IF NOT EXISTS files (
  snapshot_id     TEXT,
  path            TEXT,
  file_leaf       BLOB,
  metadata        BLOB,
  chunk_count     INTEGER,
  chunking_policy TEXT,
  PRIMARY KEY(snapshot_id, path),
  FOREIGN KEY(snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS file_chunks (
  snapshot_id TEXT,
  path        TEXT,
  chunk_index INTEGER,
  chunk_id    BLOB,
  PRIMARY KEY(snapshot_id, path, chunk_index),
  FOREIGN KEY(snapshot_id, path) REFERENCES files(snapshot_id, path) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS chunks (
  chunk_id   BLOB PRIMARY KEY,
  size       INTEGER,
  refcount   INTEGER DEFAULT 1,
  stored     INTEGER DEFAULT 0,
  created_at TEXT
);
"""

# Migrations for existing repos (ALTER TABLE silently fails if column exists)
_MIGRATIONS = [
    "ALTER TABLE snapshots ADD COLUMN gsr TEXT",
    "ALTER TABLE snapshots ADD COLUMN mmr_log_root TEXT",
    "ALTER TABLE files ADD COLUMN chunking_policy TEXT",
]


def _run_migrations(conn: sqlite3.Connection) -> None:
    for sql in _MIGRATIONS:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass  # column already present
    conn.commit()


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(_SCHEMA)
    _run_migrations(conn)
    conn.commit()


def open_conn(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(
        db_path, isolation_level=None, detect_types=sqlite3.PARSE_DECLTYPES
    )
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    init_schema(conn)
    return conn


# ---------------------------------------------------------------------------
# Snapshot helpers
# ---------------------------------------------------------------------------

def insert_snapshot(
    conn: sqlite3.Connection,
    snapshot_id: str,
    timestamp: str,
    status: str = "in_progress",
    name: str = None,
) -> None:
    conn.execute(
        "INSERT INTO snapshots(id, timestamp, status, name) VALUES (?, ?, ?, ?)",
        (snapshot_id, timestamp, status, name),
    )


def set_snapshot_committed(
    conn: sqlite3.Connection,
    snapshot_id: str,
    root: bytes,
    gsr: str = None,
    mmr_log_root: str = None,
) -> None:
    conn.execute(
        "UPDATE snapshots SET root=?, status='committed', gsr=?, mmr_log_root=? WHERE id=?",
        (root, gsr, mmr_log_root, snapshot_id),
    )


def list_snapshots(conn: sqlite3.Connection) -> list:
    cur = conn.execute(
        "SELECT id, timestamp, hex(root), status, name, gsr, mmr_log_root "
        "FROM snapshots ORDER BY timestamp DESC"
    )
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Chunk ref-count helpers
# ---------------------------------------------------------------------------

def increment_ref_count(conn: sqlite3.Connection, chunk_id: bytes) -> None:
    conn.execute(
        "UPDATE chunks SET refcount = refcount + 1 WHERE chunk_id = ?",
        (sqlite3.Binary(chunk_id),),
    )


def decrement_ref_count(conn: sqlite3.Connection, chunk_id: bytes) -> int:
    conn.execute(
        "UPDATE chunks SET refcount = refcount - 1 WHERE chunk_id = ?",
        (sqlite3.Binary(chunk_id),),
    )
    row = conn.execute(
        "SELECT refcount FROM chunks WHERE chunk_id = ?", (sqlite3.Binary(chunk_id),)
    ).fetchone()
    return row[0] if row else 0


def get_high_ref_chunks(conn: sqlite3.Connection, threshold: int = 5) -> list:
    """Return (chunk_id_hex, size, refcount) for TFR-LRC identification."""
    cur = conn.execute(
        "SELECT hex(chunk_id), size, refcount FROM chunks WHERE refcount >= ? "
        "ORDER BY refcount DESC",
        (threshold,),
    )
    return cur.fetchall()
