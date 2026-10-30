# Data Pipeline — Collector

Collector lấy dữ liệu Binance Futures và tin tức CoinDesk, chuẩn hóa về JSON, kiểm tra chất lượng, loại bản ghi trùng và tùy chọn gửi vào Kafka.

## Chạy local

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
python -m collector --source all --once
```

File được chia theo ngày, ví dụ `data/samples/market/2026/10/22.jsonl` và `data/samples/news/2026/10/22.jsonl`.

Bỏ `--once` để collector chạy liên tục, mỗi `COLLECTOR_POLL_SECONDS` giây (mặc định 60) lấy một lượt; dừng bằng Ctrl+C.

## Gửi Kafka

```powershell
python -m collector --source all --once --send-kafka
```

## Lấy dữ liệu lịch sử

Thời gian dùng UTC. Khoảng lấy dữ liệu là `[start, end)`, tức có `start` và không bao gồm `end`:

```powershell
python -m collector --source market --start 2026-10-15T00:00:00Z --end 2026-10-22T00:00:00Z
```

Với 2 symbol và nến `1m`, lệnh trên dự kiến tạo khoảng 20.160 record market. Collector tự phân trang Binance (`BINANCE_HISTORY_LIMIT` nến mỗi request, tối đa 1500), ghi file theo ngày và bỏ qua record đã tồn tại nếu chạy lại.

CoinDesk RSS chỉ chứa các bài mới nhất, nên `--start/--end` với nguồn news chỉ lọc trong danh sách đó, không lấy được tin cũ.

## Quy tắc chuẩn hóa

- Timestamp dùng ISO-8601 UTC, độ chính xác mili giây, hậu tố `Z`, ví dụ `2026-10-22T08:00:00.000Z`.
- Chỉ lưu nến đã đóng (`close_time` trước thời điểm lấy); nến đang chạy bị bỏ qua và được lấy ở lượt sau.
- Bản ghi giá duy nhất theo `source + symbol + interval + open_time`.
- Bản ghi tin tức duy nhất theo `source + article_id`; `article_id` là hash của URL.
- Bản ghi thiếu trường, sai kiểu, giá không dương, giá trị NaN/vô hạn, volume hoặc `trade_count` âm bị loại và ghi log.
- `published_at` là thời gian bài viết; `collected_at` là thời gian collector lấy dữ liệu.
- Tin tức được gắn `symbols` theo từ khóa trong tiêu đề/tóm tắt (ví dụ "bitcoin" → `BTCUSDT`); HTML trong tóm tắt được loại bỏ.
- Các lần chạy sau append vào file ngày tương ứng và bỏ qua bản ghi đã tồn tại.
- Với `--send-kafka`, chỉ bản ghi mới ghi vào file được gửi Kafka, nên chạy lại không gửi trùng.

## Kiểm thử

```powershell
pytest
```

## Kafka topics

- `market_data`: dữ liệu nến chuẩn hóa.
- `news_data`: dữ liệu tin tức chuẩn hóa.

API key và mật khẩu chỉ đặt trong `.env`; không commit file `.env`.

## Hợp đồng dữ liệu

`contracts/v1/` là **bản sao** của `infrastructure/contracts/v1/` (nguồn gốc). Không sửa tay; sau khi hợp đồng ở infrastructure thay đổi:

```powershell
python scripts/sync_contracts.py          # chép lại
python scripts/sync_contracts.py --check  # chỉ kiểm tra
```

`tests/test_contract_sync.py` fail nếu bản sao bị sửa tay hoặc lệch với bản gốc ở `../infrastructure`. Spark lấy tên trường, kiểu, pattern, enum, giới hạn số từ các file JSON Schema này (`spark_jobs/common/contracts.py`), không tự định nghĩa lại.

## Spark jobs

Code trong `spark_jobs/`, chạy trên cụm Spark của repo `infrastructure` (repo này được mount vào container tại `/opt/pipeline`, nên **clone 4 repo cạnh nhau**).

| Job | Loại | Đọc | Ghi |
|---|---|---|---|
| `raw_to_minio` | streaming | Kafka `market_data`, `news_data` | MinIO `raw-data/{market,news}/year=/month=/day=/` (JSON, kèm metadata Kafka + `ingested_at`) |
| `market_to_clickhouse` | streaming | Kafka `market_data` | ClickHouse `market_data` (+ `processed_at`), bản ghi lỗi vào `market_data_rejects` |
| `news_to_clickhouse` | streaming | Kafka `news_data` | ClickHouse `news_data`, bản ghi lỗi vào `news_data_rejects` |
| `volume_anomaly` | streaming | Kafka `market_data` + 20 nến trước từ ClickHouse | ClickHouse `signals` |
| `batch_ohlcv_5m` | batch | MinIO raw market theo khoảng ngày | ClickHouse `candles_5m` |
| `batch_indicators` | batch | MinIO raw market theo khoảng ngày (+1 ngày trước) | ClickHouse `indicators` (`ma20`) |

```
spark_jobs/
  common/      config (env), session (UTC), contracts (schema từ JSON Schema), validate, kafka_io, clickhouse (JDBC), raw_zone
  transforms.py  logic thuần (validate, dedupe, gộp 5 phút, MA20, volume bất thường, raw record) — được unit test
  jobs/        mỗi job một entrypoint
```

### Thứ tự chạy

1. Trong `infrastructure`: `docker compose up -d --build`, rồi `./scripts/migrate_clickhouse.ps1` nếu ClickHouse đã có dữ liệu từ trước.
2. Collector gửi Kafka: `python -m collector --source all --send-kafka` hoặc để service `collector` trong Docker Compose chạy liên tục.
3. Streaming (chạy nền, mỗi job một checkpoint riêng ở `s3a://checkpoints/<job>`):

   ```powershell
   .\scripts\spark.ps1 start raw_to_minio
   .\scripts\spark.ps1 start market_to_clickhouse
   .\scripts\spark.ps1 start news_to_clickhouse
   .\scripts\spark.ps1 start volume_anomaly
   .\scripts\spark.ps1 logs market_to_clickhouse      # xem log
   .\scripts\spark.ps1 stop market_to_clickhouse      # dừng; start lại sẽ chạy tiếp từ checkpoint
   ```

4. Batch (sau khi `raw_to_minio` đã ghi dữ liệu; ngày UTC, có cả hai đầu):

   ```powershell
   .\scripts\spark.ps1 run batch_ohlcv_5m --start 2026-10-27 --end 2026-10-28
   .\scripts\spark.ps1 run batch_indicators --start 2026-10-27 --end 2026-10-28
   ```

Trên Windows có thể chạy toàn bộ quy trình bằng một lệnh. Script sẽ khởi động hạ tầng và
Collector, start các job streaming, chờ có object trong MinIO, chạy hai batch job và in số
dòng hiện có trong ClickHouse:

```powershell
.\scripts\run_pipeline.ps1 -Start 2026-10-27 -End 2026-10-28
```

Nếu vừa sửa Dockerfile hoặc cấu hình hạ tầng, thêm `-BuildInfrastructure`.

Trên Linux/macOS dùng `scripts/spark.sh` (cùng tham số) hoặc `make streaming`, `make ohlcv-5m START=... END=...`. Lệnh bên dưới là `spark-submit` thật mà script chạy trong container `spark-master` (connector đã bake trong image, không cần `--packages`):

```bash
/opt/spark/bin/spark-submit --master spark://spark-master:7077 --name market_to_clickhouse \
  /opt/pipeline/spark_jobs/jobs/market_to_clickhouse.py
```

### Spark UI

- Master: <http://localhost:8080> — danh sách ứng dụng, core/RAM đang dùng.
- Từng job: <http://localhost:4040>, `4041`, … theo thứ tự submit — tab **Structured Streaming** có tốc độ xử lý, thời gian mỗi micro-batch.

### Cấu hình (biến môi trường)

Kết nối Kafka/MinIO/ClickHouse do compose cấp cho container Spark (xem `infrastructure/.env.example`), không hard-code trong code. Biến tinh chỉnh job đặt trong shell trước khi gọi `scripts/spark.*` (script chuyển tiếp vào container), xem `.env.example`:

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `SPARK_STARTING_OFFSETS` | `earliest` | Offset Kafka khi job chạy **lần đầu** (chưa có checkpoint). Ghi đè riêng: `MARKET_TO_CLICKHOUSE_STARTING_OFFSETS`, ... |
| `SPARK_TRIGGER_INTERVAL` | job tự chọn | `raw_to_minio` 1 phút (ít file nhỏ), `market_to_clickhouse`/`news_to_clickhouse` 10 giây, `volume_anomaly` 30 giây |
| `SPARK_MAX_OFFSETS_PER_TRIGGER` | không giới hạn | Giới hạn số message mỗi micro-batch khi đọc lại lịch sử |
| `SPARK_FAIL_ON_DATA_LOSS` | `false` | Có dừng job khi offset trong checkpoint đã bị Kafka xóa không |
| `ANOMALY_PERIODS`, `ANOMALY_STDDEV_K`, `ANOMALY_LOOKBACK_HOURS` | `20`, `3`, `24` | Quy tắc volume bất thường |

### Thời gian

Kafka giữ chuỗi ISO-8601 UTC có đúng 3 chữ số mili giây và `Z`. Spark đặt `spark.sql.session.timeZone=UTC`, chỉ parse chuỗi đúng pattern của hợp đồng bằng format tường minh `yyyy-MM-dd'T'HH:mm:ss.SSSX`; chuỗi sai định dạng (thiếu ms, `+07:00`, epoch) bị đưa vào dead-letter, không bị đoán. ClickHouse lưu `DateTime64(3, 'UTC')`.

### Validate và dead-letter

`market_to_clickhouse` kiểm tra từng message theo `contracts/v1/market_kline.schema.json` (thiếu trường, sai kiểu, sai pattern/enum, giá ≤ 0, volume âm, NaN/Infinity, trường lạ) và quy tắc nghiệp vụ (`low ≤ open/close ≤ high`, `close_time > open_time`). Bản ghi lỗi được ghi vào `pipeline.market_data_rejects` kèm `raw_value`, mã lỗi (`missing:close; range:volume`), topic/partition/offset — không bị bỏ lặng lẽ.

### Khởi động lại không mất, không trùng

- Mỗi query có checkpoint riêng trên MinIO; khởi động lại sẽ đọc tiếp từ offset đã commit.
- `raw_to_minio` dùng file sink: exactly-once, batch đã commit không bị ghi lại.
- Các job ghi ClickHouse dùng `foreachBatch` (at-least-once: batch cuối có thể được ghi lại sau khi khởi động lại). Mọi bảng đích là `ReplacingMergeTree` với `ORDER BY` là khóa tự nhiên, nên bản ghi lặp gộp lại; truy vấn dùng `FINAL`.

### Vì sao `volume_anomaly` dùng `foreachBatch` + ClickHouse

Structured Streaming không hỗ trợ cửa sổ theo số dòng (`rowsBetween`) trên stream. Có hai cách: (1) cửa sổ thời gian trượt 20 phút có watermark, (2) `foreachBatch` lấy 20 nến trước từ ClickHouse rồi tính bằng window thường. Chọn **(2)** vì:

- đúng định nghĩa "20 **nến** trước đó" — cửa sổ thời gian sẽ chứa ít hơn 20 nến khi thiếu dữ liệu mà vẫn tính;
- không giữ state trong Spark: lịch sử nằm ở ClickHouse, khởi động lại cho cùng kết quả; tín hiệu ghi vào `signals` (`ReplacingMergeTree ORDER BY (symbol, signal_type, open_time)`) nên chạy lại không sinh trùng;
- không có aggregation trên stream nên không cần watermark.

Quy tắc: `volume > mean + 3 × stddev_samp` của đúng 20 nến **liền** trước (20 phút liên tục). Nếu trong 20 phút đó thiếu nến (mất dữ liệu, hoặc `market_to_clickhouse` chưa ghi kịp) thì không phát tín hiệu cho nến đó thay vì so với một cửa sổ sai.

### Unit test

Test Spark nằm trong `tests/spark/` (gộp 5 phút, MA20, validate, timestamp UTC, volume bất thường, raw record) — mỗi phép tính có ví dụ tính tay trong comment. Spark 3.5 chỉ hỗ trợ chính thức Java 8/11/17:

```powershell
# Cách 1: trong image Spark (JDK 17 + pyspark của image), không cần cài gì thêm
.\scripts\spark.ps1 test

# Cách 2: trên Windows, với JDK 17 (winget install Microsoft.OpenJDK.17)
pip install -r requirements.txt -r requirements-spark.txt   # pyspark==3.5.9, đúng bằng Spark trong image
.\scripts\test_spark.ps1                                    # tự tìm JDK 17, đặt JAVA_HOME/SPARK_HOME
```

`scripts/test_spark.ps1` xử lý hai lỗi đã gặp trên Windows: launcher `.cmd` của Spark hỏng khi đường dẫn có ký tự tiếng Việt (dùng đường dẫn ngắn 8.3), và JDK 17 báo `Unable to establish loopback connection` khi tạo socket trong `%TEMP%` (chuyển sang `C:\Windows\Temp`). Trên Linux/macOS chỉ cần `JAVA_HOME` trỏ JDK 17 rồi `python -m pytest -q`.

Nếu chưa cài `pyspark`, `pytest` vẫn chạy test collector và bỏ qua `tests/spark/`.
