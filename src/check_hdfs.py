from __future__ import annotations

import sys

from pyspark.sql import SparkSession

HDFS_FILE = "hdfs://namenode:9000/taxi/raw/yellow_tripdata_2023-01.parquet"


def main() -> int:
    spark = None
    try:
        spark = (
            SparkSession.builder.appName("NYC-Taxi-Phase1-HDFS-Check")
            .master("spark://spark-master:7077")
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
            .config("spark.driver.host", "spark-master")
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.driver.port", "39000")
            .config("spark.blockManager.port", "39001")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        frame = spark.read.parquet(HDFS_FILE)
        count = frame.count()
        if count <= 0:
            raise RuntimeError("Parquet đọc được nhưng không có bản ghi nào.")
        print(f"PySpark đọc thành công: {HDFS_FILE}")
        print(f"Tổng số bản ghi: {count:,}")
        print("Schema:")
        frame.printSchema()
        print("5 dòng mẫu:")
        frame.show(5, truncate=False)
        print(f"PHASE1_SPARK_READ_OK rows={count}")
        return 0
    except Exception as exc:
        print(f"LỖI PySpark/HDFS: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
