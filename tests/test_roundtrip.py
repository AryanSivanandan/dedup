# tests/test_roundtrip.py
import os
import tempfile
import shutil
from click.testing import CliRunner
from dedupsnap.cli import cli

def write_sample(tmpdir):
    d = os.path.join(tmpdir, "data")
    os.makedirs(d, exist_ok=True)
    p1 = os.path.join(d, "a.txt")
    p2 = os.path.join(d, "sub", "b.txt")
    os.makedirs(os.path.dirname(p2), exist_ok=True)
    with open(p1, "wb") as f:
        f.write(b"hello world\n")
    with open(p2, "wb") as f:
        f.write(b"another file\n")
    return d

def test_backup_restore_roundtrip(tmp_path):
    repo_dir = str(tmp_path / "repo")
    data_dir = write_sample(str(tmp_path))
    runner = CliRunner()
    # init
    r = runner.invoke(cli, ["init", repo_dir])
    assert r.exit_code == 0
    # backup
    r = runner.invoke(cli, ["backup", repo_dir, data_dir])
    assert r.exit_code == 0
    out = r.output
    # parse snapshot id
    sid = out.split()[2]
    # restore
    target = os.path.join(str(tmp_path), "restored")
    r = runner.invoke(cli, ["restore", repo_dir, sid, target])
    assert r.exit_code == 0
    # compare files
    with open(os.path.join(data_dir, "a.txt"), "rb") as a1:
        with open(os.path.join(target, "a.txt"), "rb") as a2:
            assert a1.read() == a2.read()
    with open(os.path.join(data_dir, "sub", "b.txt"), "rb") as b1:
        with open(os.path.join(target, "sub", "b.txt"), "rb") as b2:
            assert b1.read() == b2.read()
