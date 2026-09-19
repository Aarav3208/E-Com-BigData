import subprocess
from pathlib import Path

from .config import HDFS_BASE_PATH, HDFS_NAMENODE


def _hdfs_path(name: str) -> str:
    return f"{HDFS_NAMENODE}{HDFS_BASE_PATH}/{name}"


def run_hdfs_command(arguments: list[str]) -> bool:
    try:
        result = subprocess.run(["hdfs", "dfs", *arguments], text=True, capture_output=True)
    except FileNotFoundError:
        print("HDFS command not found. Start Hadoop and add its bin directory to PATH.")
        return False
    if result.returncode != 0:
        print(f"HDFS command failed: {result.stderr.strip()}")
        print("Start the NameNode/DataNode and check HDFS_NAMENODE in src/config.py.")
        return False
    print(result.stdout.strip())
    return True


def setup_hdfs(raw_file: str | Path | None = None) -> bool:
    ok = run_hdfs_command(["-mkdir", "-p", _hdfs_path("raw")])
    ok = run_hdfs_command(["-mkdir", "-p", _hdfs_path("processed")]) and ok
    ok = run_hdfs_command(["-mkdir", "-p", _hdfs_path("analytics")]) and ok
    if raw_file:
        ok = run_hdfs_command(["-put", "-f", str(raw_file), _hdfs_path("raw")]) and ok
    print("HDFS verification:")
    return run_hdfs_command(["-ls", "-R", f"{HDFS_NAMENODE}{HDFS_BASE_PATH}"]) and ok
