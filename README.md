# NYC Yellow Taxi Big Data — Phase 1

Phase 1 dựng môi trường Hadoop HDFS và Spark bằng Docker, tải dữ liệu thật tháng 01/2023 từ NYC TLC, lưu bản thô lên HDFS và kiểm tra PySpark đọc được Parquet. Dữ liệu thô không bị chỉnh sửa. Các giai đoạn làm sạch, KPI, benchmark và triển khai tự động nằm ngoài phạm vi Phase 1.

## Công nghệ

- Docker Desktop, Docker Compose
- Apache Hadoop 3.4.3: NameNode, DataNode
- Apache Spark 3.5.7 (Python 3, Hadoop 3 build): Master, Worker
- Python 3.10+ trên Windows để chạy script

Compose dùng image do Apache công bố. Spark dùng build Hadoop 3 tương thích với HDFS Hadoop 3.x; Spark nhận `core-site.xml` và `hdfs-site.xml` qua `HADOOP_CONF_DIR`. Driver chạy trong container `spark-master`, còn worker và driver cùng nhìn thấy hostname `namenode`, `datanode` trên mạng Compose.

## Cấu trúc

```text
config/       Hadoop client và HDFS settings
data/raw/     Dữ liệu nguồn tải về (không commit Git)
scripts/      Tải dữ liệu, upload và kiểm chứng
src/          Chương trình PySpark đọc HDFS
docker-compose.yml
```

## Yêu cầu

- Windows 10/11, Docker Desktop đang chạy ở chế độ Linux containers
- Docker Engine và Docker Compose plugin
- Python 3.10 trở lên
- Tối thiểu khoảng 8 GB RAM được cấp cho Docker Desktop và đủ dung lượng đĩa cho image, Parquet và HDFS
- Các cổng giao diện mặc định 9870, 9864, 8080, 8081, 4040 còn trống

Nếu cổng giao diện bị chiếm, đặt cổng host khác trong PowerShell trước khi chạy Compose, ví dụ `$env:HDFS_NAMENODE_UI_PORT=9871`; các dịch vụ nội bộ vẫn dùng cổng container cố định. Có biến tương ứng `HDFS_DATANODE_UI_PORT`, `SPARK_MASTER_UI_PORT`, `SPARK_WORKER_UI_PORT`. Cổng 4040 là giao diện job của Spark và chỉ phục vụ khi ứng dụng đang chạy. HDFS RPC (9000) và Spark Master (7077) chỉ mở trên mạng Docker, không publish ra Windows.

## Chạy Phase 1

Mở PowerShell tại thư mục gốc repo.

```powershell
docker --version
docker compose version
docker compose config
docker compose up -d
docker compose ps
```

Tải hai file từ đường dẫn TLC chính thức:

```powershell
python scripts/download_data.py
```

Script giữ nguyên file đã tồn tại nếu file có kích thước lớn hơn 0. Nếu tải lỗi, script báo lỗi và không dùng dữ liệu thay thế.

Upload lên HDFS và xác nhận kích thước:

```powershell
python scripts/upload_hdfs.py
```

Chạy kiểm tra NameNode, DataNode, Spark Worker, HDFS và Spark đọc Parquet:

```powershell
python scripts/verify_hdfs.py
```

Chương trình đọc `hdfs://namenode:9000/taxi/raw/yellow_tripdata_2023-01.parquet`, in schema, tổng số dòng và 5 dòng mẫu. Có thể chạy riêng bằng lệnh:

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 --conf spark.driver.port=39000 --conf spark.blockManager.port=39001 /opt/project/src/check_hdfs.py
```

## Giao diện

- Hadoop NameNode: <http://localhost:9870>
- Hadoop DataNode: <http://localhost:9864>
- Spark Master: <http://localhost:8080>
- Spark Worker: <http://localhost:8081>
- Spark application UI: <http://localhost:4040> (chỉ khi job đang chạy)

## Dữ liệu và dừng hệ thống

- Windows: `data/raw/` giữ bản TLC tải xuống.
- Container NameNode: file staging upload tạm ở `/data/raw/`.
- HDFS: bản dữ liệu dùng để xử lý nằm ở `/taxi/raw/`.
- HDFS lưu trên named volumes `nyc-taxi-bigdata_hdfs_namenode` và `nyc-taxi-bigdata_hdfs_datanode`.

Dừng các container mà vẫn giữ dữ liệu HDFS:

```powershell
docker compose down
```

Khởi động lại bằng `docker compose up -d`. Không chạy `docker compose down -v` nếu muốn giữ HDFS; lệnh đó xóa named volumes.

## Lỗi thường gặp

- **Docker Engine không kết nối được:** mở Docker Desktop, chờ Engine sẵn sàng rồi thử `docker info`.
- **Cổng đã dùng:** đặt biến cổng host ở phần Yêu cầu rồi khởi động lại Compose.
- **DataNode chưa Live:** xem `docker compose logs namenode datanode`; đợi thêm một phút rồi chạy lại `python scripts/verify_hdfs.py`.
- **Thiếu file hoặc tải lỗi:** kiểm tra Internet rồi chạy lại `python scripts/download_data.py`; file tải dở `.part` được xóa khi lỗi.
- **PySpark không đọc HDFS:** kiểm tra trạng thái DataNode và cấu hình XML được mount; xem `docker compose logs spark-master spark-worker namenode datanode`.

## Nguồn dữ liệu

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Yellow Taxi tháng 01/2023 (Parquet)](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet)
- [Taxi zone lookup (CSV)](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)
