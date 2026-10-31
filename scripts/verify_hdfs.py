from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FILES = ("yellow_tripdata_2023-01.parquet", "taxi_zone_lookup.csv")


def command(label: str, *args: str) -> str:
    result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, timeout=240)
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
        raise RuntimeError(f"{label} thất bại: {detail}")
    return result.stdout.strip()


def check_url(label: str, url: str) -> dict:
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status}")
            return json.load(response)
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{label} không truy cập được tại {url}: {exc}") from exc


def wait_for_live_datanode(timeout_seconds: int = 90, interval_seconds: int = 5) -> str:
    """Allow the DataNode time to register after Compose reports containers started."""
    deadline = time.monotonic() + timeout_seconds
    report = ""
    while True:
        report = command(
            "NameNode/DataNode",
            "docker", "compose", "exec", "-T", "--user", "hadoop", "namenode",
            "hdfs", "dfsadmin", "-report",
        )
        match = re.search(r"Live datanodes \((\d+)\)", report)
        if match and int(match.group(1)) >= 1:
            return report
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(
                "DataNode: vẫn chưa có Live DataNode sau "
                f"{timeout_seconds} giây. Báo cáo cuối từ NameNode:\n{report}"
            )
        print(f"Đang chờ DataNode đăng ký với NameNode (còn tối đa {int(remaining)} giây)...")
        time.sleep(min(interval_seconds, remaining))


def main() -> int:
    try:
        nn = wait_for_live_datanode()
        print("PASS: NameNode trả lời lệnh dfsadmin -report")
        print("PASS: có ít nhất 1 Live DataNode")

        command("Thư mục HDFS /taxi/raw", "docker", "compose", "exec", "-T", "--user", "hadoop", "namenode", "hdfs", "dfs", "-test", "-d", "/taxi/raw")
        print("PASS: thư mục /taxi/raw tồn tại")
        for name in FILES:
            size = command("File HDFS", "docker", "compose", "exec", "-T", "--user", "hadoop", "namenode", "hdfs", "dfs", "-stat", "%b", f"/taxi/raw/{name}")
            if not size.isdigit() or int(size) <= 0:
                raise RuntimeError(f"File HDFS {name} có kích thước không hợp lệ: {size!r}")
            print(f"PASS: /taxi/raw/{name} ({int(size):,} bytes)")

        ui_port = os.environ.get("SPARK_MASTER_UI_PORT", "8080")
        master = check_url("Spark Master", f"http://localhost:{ui_port}/json/")
        workers = master.get("workers", [])
        if not any(worker.get("state") == "ALIVE" for worker in workers):
            raise RuntimeError("Spark Master không có Worker ở trạng thái ALIVE.")
        print(f"PASS: Spark Master có {sum(w.get('state') == 'ALIVE' for w in workers)} Worker ALIVE")

        result = command(
            "PySpark đọc và đếm Parquet",
            "docker", "compose", "exec", "-T", "spark-master",
            "/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077",
            "--conf", "spark.driver.host=spark-master",
            "--conf", "spark.driver.bindAddress=0.0.0.0",
            "--conf", "spark.driver.port=39000",
            "--conf", "spark.blockManager.port=39001",
            "/opt/project/src/check_hdfs.py",
        )
        print(result)
        if "PHASE1_SPARK_READ_OK rows=" not in result:
            raise RuntimeError("PySpark lệnh kết thúc nhưng không có thông báo xác nhận số bản ghi.")
        print("PASS: PySpark đọc HDFS, đếm, schema và 5 dòng mẫu")
    except Exception as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    print("TẤT CẢ KIỂM TRA PHASE 1 ĐÃ PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
