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

## Gửi Kafka

```powershell
python -m collector --source all --once --send-kafka
```

## Lấy dữ liệu lịch sử

Thời gian dùng UTC. Khoảng lấy dữ liệu là `[start, end)`, tức có `start` và không bao gồm `end`:

```powershell
python -m collector --source market --start 2026-10-15T00:00:00Z --end 2026-10-22T00:00:00Z
```

Với 2 symbol và nến `1m`, lệnh trên dự kiến tạo khoảng 20.160 record market. Collector tự phân trang Binance, ghi file theo ngày và bỏ qua record đã tồn tại nếu chạy lại.

## Quy tắc chuẩn hóa

- Timestamp dùng ISO-8601 UTC với hậu tố `Z`.
- Bản ghi giá duy nhất theo `source + symbol + interval + open_time`.
- Bản ghi tin tức duy nhất theo `source + article_id`; `article_id` là hash của URL.
- Bản ghi thiếu trường, sai kiểu, giá không hợp lệ hoặc volume âm bị loại và ghi log.
- `published_at` là thời gian bài viết; `collected_at` là thời gian collector lấy dữ liệu.
- Các lần chạy sau append vào file ngày tương ứng và bỏ qua bản ghi đã tồn tại.

## Kiểm thử

```powershell
pytest
```

## Kafka topics

- `market_data`: dữ liệu nến chuẩn hóa.
- `news_data`: dữ liệu tin tức chuẩn hóa.

API key và mật khẩu chỉ đặt trong `.env`; không commit file `.env`.
