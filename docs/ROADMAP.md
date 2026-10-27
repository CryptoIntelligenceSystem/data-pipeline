# data-pipeline — Lộ trình triển khai

> Tham chiếu: [infrastructure/docs/PROJECT_REVIEW.md](https://github.com/CryptoIntelligenceSystem/infrastructure/blob/main/docs/PROJECT_REVIEW.md) — hợp đồng chung v1 (`infrastructure/contracts/v1/`), lịch tích hợp, câu hỏi mở. Repo này **không tự định nghĩa schema**; mọi định dạng message theo `market_kline` v1 / `news_article` v1.
> Lịch: tuần 1 = 19–25/10/2026. Phụ trách chính: **A (Ingestion)**. Trích dẫn `file:dòng` theo `main@a7a0df4`.

## 1. Mục tiêu cuối (khi demo)

- Collector chạy trong Docker (profile `ingest` của infrastructure), 3 chế độ:
  - `stream`: WebSocket Binance `<symbol>@kline_1m`, chỉ phát nến `k.x == true`, tự reconnect với backoff có jitter, tự backfill khoảng trống qua REST khi khởi động/reconnect.
  - `backfill`: REST phân trang `[start, end)`.
  - `news`: poll tin tức mỗi 5 phút, có nguồn dự phòng.
- Gửi Kafka `market_data` (key `symbol`) / `news_data` (key `article_id`) với `acks=all` + idempotence; **không mất bản ghi** khi Kafka tạm ngừng.
- Mọi bản ghi được validate theo JSON Schema v1 (vendor từ infrastructure) trước khi gửi.
- Có công cụ `tools/replay_samples.py` để các repo khác phát triển khi chưa có dữ liệu thật.
- Test ≥ 80% module collector; CI xanh.

## 2. Hiện trạng

- Python 3.11, `requests`, `python-dotenv`, `kafka-python` 2.3.2, `pytest`; 22 test pass.
- Có: REST Binance **Futures** (`/fapi/v1/klines`) cả realtime (poll 60 s, `limit=100`) và lịch sử phân trang; chỉ lưu nến đã đóng; retry/backoff tôn trọng `Retry-After`; CoinDesk RSS; validate, dedupe, ghi JSONL theo ngày; gửi Kafka `acks=all`, key `symbol`.
- Chưa có: WebSocket, gap-fill tự động, idempotence, Dockerfile, CI, lint, hợp đồng dùng chung.
- Lỗi nghiêm trọng: mất bản ghi ở Kafka khi gửi thất bại (C1).
- PR `#3` (`fix/data-pipeline`) đã bị `a7a0df4` thay thế và đang xung đột → đóng. Issue `#2` (Spark) thuộc infrastructure (INF-M4/M5).

## 3. Kiến trúc và cấu trúc thư mục đề xuất (chỉ phần thay đổi)

```
collector/
  binance_rest.py        # TÁCH từ collectors.py (BinanceCollector)
  binance_ws.py          # MỚI — WebSocket kline + reconnect
  news.py                # TÁCH từ collectors.py (CoinDesk + nguồn dự phòng)
  gapfill.py             # MỚI — tìm và lấp khoảng trống theo state
  state.py               # MỚI — last_open_time/symbol, offset gửi Kafka (file JSON trong volume)
  kafka.py               # producer dùng lại, idempotent
  validation.py          # validate bằng jsonschema theo contracts/v1
  main.py                # CLI: --mode stream|poll|backfill|news
contracts/v1/            # MỚI — bản vendor từ infrastructure (không sửa tay)
scripts/sync_contracts.py
tools/replay_samples.py  # MỚI — đẩy file JSONL vào Kafka, tùy chọn dịch thời gian về hiện tại
tests/fixtures/          # MỚI — market_sample.jsonl (2 symbol × 120 nến), news_sample.jsonl, rss_sample.xml, ws_kline_*.json
Dockerfile, .dockerignore, pyproject.toml (ruff, pytest)
.github/workflows/ci.yml
```

Lý do:
- Tách `collectors.py` theo nguồn để thêm WebSocket mà không phình 1 file.
- Trạng thái gửi Kafka tách khỏi file mẫu (`data/samples`) để sửa C1: file mẫu chỉ để xem/kiểm tra, không quyết định việc gửi.
- Vendor hợp đồng để CI chạy độc lập (repo infrastructure là private).

## 4. Milestone

### DP-M0 — Dọn dẹp, tuân thủ hợp đồng, công cụ replay
- **Tuần**: 2 · **Phụ trách**: A
- **Phụ thuộc**: INF-M0.1 (schema v1) cho DP-M0.2. DP-M0.1, DP-M0.3 làm ngay.
- **Mock**: trước khi INF-M0 merge, validate bằng `validation.py` hiện có.
- **Chặn**: INF-M2, INF-M4 (cần replay).

| Task | File | Mô tả | Công | Nhánh |
|---|---|---|---|---|
| DP-M0.1 | GitHub | Đóng PR `#3` (ghi "superseded by a7a0df4"); xóa nhánh `feat/Collector_Handle_data`, `fix/data-pipeline` | S | — |
| DP-M0.2 | `scripts/sync_contracts.py`, `contracts/v1/`, `collector/validation.py` | Vendor schema; validate thêm bằng `jsonschema` (giữ kiểm tra nghiệp vụ hiện có) | S | `feat/contracts-v1-validation` |
| DP-M0.3 | `tools/replay_samples.py`, `tests/fixtures/market_sample.jsonl`, `news_sample.jsonl` | Đẩy JSONL vào Kafka; `--shift-to-now`, `--rate`, `--loop` | M | `feat/replay-samples-tool` |
| DP-M0.4 | `pyproject.toml`, `.github/workflows/ci.yml` | ruff + pytest + kiểm tra `contracts/v1/VERSION` | S | `chore/ci-lint` |

**Hoàn thành khi**:
```bash
ruff check . && pytest -q
python scripts/sync_contracts.py --check        # bản vendor khớp ../infrastructure/contracts/v1
python tools/replay_samples.py --topic market_data --file tests/fixtures/market_sample.jsonl --shift-to-now
docker compose -f ../infrastructure/docker-compose.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic market_data --from-beginning --max-messages 3 --property print.key=true
```

### DP-M1 — Gửi Kafka tin cậy và chốt nguồn Binance
- **Tuần**: 3 · **Phụ trách**: A
- **Phụ thuộc**: DP-M0.2; Q1 (Spot/Futures) đã chốt. Sang repo khác: Kafka từ `infrastructure` (đã có).
- **Mock**: Kafka thật bằng `docker compose --profile core up -d` (đã chạy được); test unit dùng producer giả.

| Task | File | Mô tả | Công | Nhánh |
|---|---|---|---|---|
| DP-M1.1 | `collector/main.py:46-55`, `collector/state.py` | Sửa C1: gửi Kafka trước, chỉ đánh dấu "đã gửi" khi future thành công; lần chạy sau gửi lại bản ghi chưa gửi | M | `fix/kafka-publish-before-mark` |
| DP-M1.2 | `collector/kafka.py:12-47` | Producer dùng lại suốt vòng đời; `enable_idempotence=True`, `acks="all"`, `linger_ms=50`, `compression_type="gzip"` (M2) | S | `fix/kafka-idempotent-producer` |
| DP-M1.3 | `collector/collectors.py:129`, `config.py:15`, `.env.example:1` | Theo Q1: Spot `https://api.binance.com/api/v3/klines`, limit ≤ 1000, `source="binance_spot"` (H6) | S | `feat/binance-spot` |
| DP-M1.4 | `collector/config.py:13-26` | `Settings.from_env()` thay vì đọc env lúc import (M8) | S | `refactor/settings-from-env` |
| DP-M1.5 | `tests/test_kafka.py`, `tests/test_main.py` | Test: Kafka lỗi → lần sau gửi lại; không gửi trùng khi thành công | S | `fix/kafka-publish-before-mark` |

**Hoàn thành khi**:
```bash
pytest -q tests/test_kafka.py tests/test_main.py
# kịch bản thủ công:
docker compose -f ../infrastructure/docker-compose.yml stop kafka
python -m collector --source market --once --send-kafka      # log lỗi Kafka, không crash
docker compose -f ../infrastructure/docker-compose.yml start kafka
python -m collector --source market --once --send-kafka      # gửi lại phần còn thiếu
# đếm message theo key trên topic = số bản ghi trong data/samples của ngày đó
```

### DP-M2 — WebSocket realtime và lấp khoảng trống
- **Tuần**: 4 · **Phụ trách**: A
- **Phụ thuộc**: DP-M1. Sang repo khác: không.
- **Mock**: test bằng file `tests/fixtures/ws_kline_open.json`, `ws_kline_closed.json` và server WebSocket giả (`websockets.serve` trong test).

| Task | File | Mô tả | Công | Nhánh |
|---|---|---|---|---|
| DP-M2.1 | `collector/binance_ws.py` | Kết nối combined stream `wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1m/ethusdt@kline_1m`; chỉ phát khi `k.x == true`; map `k.t, k.T, k.o, k.h, k.l, k.c, k.v, k.q, k.n` sang `market_kline` v1 | M | `feat/binance-ws-kline` |
| DP-M2.2 | `collector/binance_ws.py` | Reconnect: backoff `min(60, 2^n) + jitter`; ping/pong; tự reconnect trước mốc 24 h Binance ngắt | S | `feat/binance-ws-kline` |
| DP-M2.3 | `collector/gapfill.py`, `collector/state.py` | Khi khởi động/reconnect: REST `[last_open_time + 1m, now)` (≤ 1000 nến/lần, phân trang), rồi mới nhận WS (M1) | M | `feat/gapfill-on-reconnect` |
| DP-M2.4 | `collector/main.py` | `--mode stream` (mặc định trong Docker), giữ `poll` làm dự phòng | S | `feat/binance-ws-kline` |
| DP-M2.5 | `tests/test_binance_ws.py`, `tests/test_gapfill.py` | Bỏ nến chưa đóng; reconnect; gap 10 phút được lấp đủ 10 nến | M | `test/ws-gapfill` |

**Hoàn thành khi**:
```bash
pytest -q tests/test_binance_ws.py tests/test_gapfill.py
python -m collector --mode stream --send-kafka     # log mỗi phút 2 nến (BTC, ETH)
# ngắt mạng 5 phút rồi bật lại → log "gap-fill: 5 candles" cho mỗi symbol
```
```sql
-- sau khi Spark nạp (IP3): không khoảng trống > 1 phút trong 1 giờ có ngắt mạng
SELECT count() FROM (SELECT open_time, lagInFrame(open_time) OVER (PARTITION BY symbol ORDER BY open_time) AS prev
  FROM pipeline.market_data FINAL WHERE open_time > now() - INTERVAL 1 HOUR)
WHERE prev > toDateTime64(0,3) AND dateDiff('second', prev, open_time) > 60;   -- 0
```

### DP-M3 — Tin tức có nguồn dự phòng
- **Tuần**: 5 · **Phụ trách**: A
- **Phụ thuộc**: DP-M1; Q3 (nguồn tin) đã chốt.
- **Mock**: `tests/fixtures/rss_sample.xml`, `coindesk_api_sample.json`.

| Task | File | Mô tả | Công | Nhánh |
|---|---|---|---|---|
| DP-M3.1 | `collector/news.py` | Tách CoinDesk khỏi `collectors.py:202-239`; nếu có `COINDESK_API_KEY` dùng API, không thì RSS | M | `feat/news-sources` |
| DP-M3.2 | `collector/news.py`, `.env.example` | Nguồn dự phòng RSS (vd Cointelegraph) khi nguồn chính lỗi 2 lượt liên tiếp; `source` theo enum hợp đồng (H8) | S | `feat/news-sources` |
| DP-M3.3 | `collector/main.py` | Chu kỳ news riêng (`NEWS_POLL_SECONDS=300`), tách khỏi market | S | `feat/news-sources` |
| DP-M3.4 | `tests/test_news.py` | Parse API/RSS, fallback, dedupe theo `article_id` | S | `feat/news-sources` |

**Hoàn thành khi**:
```bash
pytest -q tests/test_news.py
COINDESK_RSS_URL=http://127.0.0.1:9/invalid python -m collector --source news --once   # log "fallback to cointelegraph_rss", vẫn ghi được tin
```

### DP-M4 — Đóng gói Docker
- **Tuần**: 5 · **Phụ trách**: A (D review phần compose)
- **Phụ thuộc**: DP-M2. Sang repo khác: INF-M1 (profile `ingest`) thêm service `collector` build từ repo này.

| Task | File | Mô tả | Công | Nhánh |
|---|---|---|---|---|
| DP-M4.1 | `Dockerfile`, `.dockerignore` | `python:3.11-slim`, user không phải root, `CMD ["python","-m","collector","--mode","stream","--send-kafka"]` | S | `feat/collector-docker` |
| DP-M4.2 | `collector/state.py`, README | State + `data/` trong volume `/data`; healthcheck bằng file heartbeat | S | `feat/collector-docker` |
| DP-M4.3 | `collector/main.py` | Log thống kê mỗi 5 phút: nến nhận/gửi/lỗi, gap-fill (dùng cho E2E_REPORT) | S | `feat/collector-metrics-log` |

**Hoàn thành khi**:
```bash
docker build -t cis-collector .
docker compose -f ../infrastructure/docker-compose.yml --profile core --profile ingest up -d collector
docker compose -f ../infrastructure/docker-compose.yml logs --tail 20 collector    # thấy nến đóng mỗi phút
docker compose -f ../infrastructure/docker-compose.yml restart collector           # không gửi trùng, gap-fill nếu cần
```

### DP-M5 — Kiểm thử lỗi phía collector và số liệu
- **Tuần**: 7–8 · **Phụ trách**: A
- **Phụ thuộc**: DP-M4, INF-M4 (để đối chiếu ClickHouse). Đóng góp vào INF-M6.

| Task | File | Mô tả | Công | Nhánh |
|---|---|---|---|---|
| DP-M5.1 | `tests/test_failures.py` | Binance 429/418, mất WS giữa chừng, JSON hỏng, Kafka down | M | `test/collector-failures` |
| DP-M5.2 | `docs/OPERATIONS.md` | Cách chạy, biến môi trường, xử lý sự cố | S | `docs/collector-operations` |
| DP-M5.3 | — | Chạy 24 h, ghi số nến nhận/gửi/gap-fill vào `infrastructure/docs/E2E_REPORT.md` | S | — |

**Hoàn thành khi**: `pytest -q` xanh; số nến trong 24 h mỗi symbol = 1440 trên ClickHouse (`SELECT symbol, count() FROM pipeline.market_data FINAL WHERE open_time >= today() - 1 AND open_time < today() GROUP BY symbol`).

## 5. Test và CI tối thiểu

- `pyproject.toml`: cấu hình `ruff` (E, F, I, B) và `pytest` (`testpaths=tests`).
- `.github/workflows/ci.yml` (mọi PR, Python 3.11):
  1. `pip install -r requirements.txt ruff jsonschema`
  2. `ruff check .`
  3. `pytest -q --cov=collector --cov-fail-under=70`
  4. `python scripts/sync_contracts.py --verify-only` (bản vendor có `VERSION` hợp lệ, ví dụ trong `contracts/v1/examples` pass `validation.py`)
- Không gọi API Binance/CoinDesk thật trong CI — mọi test dùng fixture/mock.

## 6. Rủi ro riêng và giảm thiểu

| Rủi ro | Giảm thiểu |
|---|---|
| Binance chặn theo vùng/IP hoặc 418/429 | WebSocket giảm số request REST; backoff đã có; `tests/fixtures` + replay để demo offline |
| WS ngắt định kỳ (Binance ngắt sau 24 h) hoặc im lặng | Ping/pong, timeout không nhận tin 90 s → reconnect + gap-fill |
| Nguồn tin thay đổi định dạng/điều khoản | 2 nguồn, parse có kiểm tra; test với fixture |
| Hợp đồng đổi mà bản vendor không cập nhật | CI so `VERSION`; PR đổi hợp đồng ở infrastructure kèm PR sync ở đây |
| A vừa làm DP vừa raw-sink (INF-M2) | INF-M2 bắt đầu sau DP-M1 (nửa sau tuần 3); DP-M3 có thể lùi vì news ít ảnh hưởng đường găng |

## 7. Vấn đề rà soát thuộc repo này

### Nghiêm trọng
| # | Vấn đề | Vị trí | Sửa ở |
|---|---|---|---|
| C1 | Ghi file local trước, gửi Kafka sau; Kafka lỗi thì bản ghi không bao giờ được gửi lại | `collector/main.py:46-55`, `collector/output.py:54-91` | DP-M1.1 |
| C4 | Schema chỉ là dataclass nội bộ, không phải hợp đồng dùng chung | `collector/models.py:17-56` | DP-M0.2 (theo INF-M0) |

### Cao
| # | Vấn đề | Vị trí | Sửa ở |
|---|---|---|---|
| H2 | Không có WebSocket; realtime là polling REST 60 s | `collector/main.py:107-115` | DP-M2 |
| H6 | Dùng Binance Futures thay vì Spot như kế hoạch | `collector/collectors.py:129`, `collector/config.py:15`, `.env.example:1` | DP-M1.3 |
| H8 | Tin tức chỉ có RSS, không lịch sử, không dự phòng | `collector/collectors.py:202-239`, `collector/main.py:75-85` | DP-M3 |

### Trung bình
| # | Vấn đề | Vị trí | Sửa ở |
|---|---|---|---|
| M1 | Không phát hiện khoảng trống; mất kết nối > 100 phút là thiếu dữ liệu vĩnh viễn | `collector/main.py:58-72`, `.env.example:4` | DP-M2.3 |
| M2 | Tạo `KafkaProducer` mỗi lượt; idempotence tắt (mặc định kafka-python 2.3.2) | `collector/kafka.py:24-28` | DP-M1.2 |
| M6 | Chưa có Docker; trạng thái dedupe nằm ở file local | `collector/output.py:54-91` | DP-M4 |
| M8 | `Settings` đọc env lúc import | `collector/config.py:13-26` | DP-M1.4 |
| M9 | Commit `a7a0df4` đẩy thẳng `main`, chưa có branch protection | lịch sử git | DP-M0 / PROJECT_REVIEW Q11 |

### Thấp
| # | Vấn đề | Vị trí | Sửa ở |
|---|---|---|---|
| L3 | Không có lint/CI | — | DP-M0.4 |
| L7 | Logic parse ISO/khóa bản ghi lặp ở nhiều nơi | `collector/output.py:42-51`, `collector/main.py:39-43` | DP-M1.1 (gom vào `models.py`) |
| L8 | PR `#3` mở nhưng đã bị thay thế, đang xung đột | nhánh `fix/data-pipeline` | DP-M0.1 |
