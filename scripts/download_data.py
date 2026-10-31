from __future__ import annotations

import sys
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
FILES = {
    "yellow_tripdata_2023-01.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
    "taxi_zone_lookup.csv": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
}


def download(name: str, url: str) -> None:
    destination = RAW_DIR / name
    if destination.exists():
        if destination.is_file() and destination.stat().st_size > 0:
            print(f"Đã có file hợp lệ, bỏ qua tải: {destination.name} ({destination.stat().st_size:,} bytes)")
            return
        raise RuntimeError(f"File đích tồn tại nhưng rỗng hoặc không phải file: {destination}")

    temporary = destination.with_name(destination.name + ".part")
    request = urllib.request.Request(url, headers={"User-Agent": "nyc-taxi-bigdata-phase1/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status} khi tải {url}")
            with temporary.open("wb") as output:
                while chunk := response.read(1024 * 1024):
                    output.write(chunk)
        size = temporary.stat().st_size
        if size <= 0:
            raise RuntimeError(f"File tải về rỗng: {name}")
        temporary.replace(destination)
        print(f"Đã tải {destination.name} ({size:,} bytes)")
    except (urllib.error.URLError, OSError, RuntimeError) as exc:
        temporary.unlink(missing_ok=True)
        raise RuntimeError(f"Tải thất bại {name}: {exc}") from exc


def main() -> int:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    try:
        for name, url in FILES.items():
            download(name, url)
    except Exception as exc:
        print(f"LỖI: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
