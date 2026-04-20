# tests/test_dedupe.py
import os
from click.testing import CliRunner
from dedupsnap.cli import cli
import tempfile

def test_dedupe(tmp_path):
    repo_dir = str(tmp_path / "repo")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    # create two files with same content
    with open(os.path.join(data_dir, "x.txt"), "wb") as f:
        f.write(b"A"*40000)  # > 32KB so spans two chunks
    with open(os.path.join(data_dir, "y.txt"), "wb") as f:
        f.write(b"A"*40000)
    runner = CliRunner()
    runner.invoke(cli, ["init", repo_dir])
    r = runner.invoke(cli, ["backup", repo_dir, data_dir])
    assert r.exit_code == 0
    # open DB and count chunks
    import sqlite3
    dbpath = os.path.join(repo_dir, ".dedupsnap", "metadata.db")
    conn = sqlite3.connect(dbpath)
    cur = conn.execute("SELECT COUNT(*) FROM chunks").fetchone()
    # because both files identical, chunk count should equal unique chunks (2)
    assert cur[0] == 2
