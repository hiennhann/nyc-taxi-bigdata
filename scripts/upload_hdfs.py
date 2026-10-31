from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FILES = ("yellow_tripdata_2023-01.parquet", "taxi_zone_lookup.csv")


def run(*args: str) -> str:
    result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or f"Lệnh lỗi: {' '.join(args)}")
    return result.stdout.strip()


def main() -> int:
    try:
        for name in FILES:
            source = ROOT / "data" / "raw" / name
            if not source.is_file() or source.stat().st_size <= 0:
                raise RuntimeError(f"Thiếu file nguồn hợp lệ trên Windows: {source}")

        run("docker", "compose", "exec", "-T", "namenode", "mkdir", "-p", "/data/raw")
        run("docker", "compose", "exec", "-T", "--user", "hadoop", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/taxi/raw")
        run("docker", "compose", "exec", "-T", "--user", "hadoop", "namenode", "hdfs", "dfs", "-chmod", "755", "/taxi", "/taxi/raw")
        for name in FILES:
            source = f"/data/raw/{name}"
            run("docker", "compose", "cp", str(ROOT / "data" / "raw" / name), f"namenode:{source}")
            run("docker", "compose", "exec", "-T", "--user", "hadoop", "namenode", "hdfs", "dfs", "-put", "-f", source, f"/taxi/raw/{name}")
            size = run("docker", "compose", "exec", "-T", "--user", "hadoop", "namenode", "hdfs", "dfs", "-stat", "%b", f"/taxi/raw/{name}")
            if not size.isdigit() or int(size) <= 0:
                raise RuntimeError(f"Kích thước trên HDFS không hợp lệ cho {name}: {size!r}")
            print(f"HDFS /taxi/raw/{name}: {int(size):,} bytes")
    except Exception as exc:
        print(f"LỖI upload HDFS: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
