# Oil Liquidity Sniper Pro 2.1

Web app có backend Node.js, Binance USDⓈ-M Futures public market data, RSS news ingestion và OpenAI Responses API tùy chọn.

## Có gì trong bản này
- Kết nối Binance Futures WebSocket market stream (all-market ticker).
- Tự đọc `exchangeInfo` để xác định các hợp đồng USDT perpetual đang giao dịch.
- Tự dò mã dầu theo tên CL, WTI, Brent, BZ, Oil, Crude và tách mã khí tự nhiên (GAS/NATGAS) thành nhóm riêng; tên mã chỉ là dấu hiệu dò, không tự xác nhận hợp đồng đó là giá dầu chuẩn.
- Tải 250 nến 1m cho nhóm hợp đồng USDT thanh khoản cao nhất; khi chọn mã khác, tải nến riêng và theo dõi realtime cho mã/khung đang xem. Tính EMA36/89/200 và RSI12 từ 1m history.
- Xếp hạng LONG/SHORT/NEUTRAL bằng quy tắc kỹ thuật; mã thiếu 200 nến 1m phải ở trạng thái WAIT/CHỜ NẾN, không bị gán LONG/SHORT chỉ từ biến động 24h.
- Thu thập RSS theo nhóm dầu, EIA/API, OPEC+, Fed/USD và địa chính trị.
- OpenAI Responses API phân tích dữ liệu thị trường + tin tức, khi có `OPENAI_API_KEY`.
- API key chỉ nằm ở backend trong `.env`, không đưa vào trình duyệt.
- Chỉ đọc dữ liệu, không đặt lệnh và không yêu cầu Binance API secret.

## Yêu cầu
- Node.js 20+
- Kết nối internet tới Binance, nguồn RSS và (nếu bật AI) OpenAI API.

## Chạy local
```bash
npm install
cp .env.example .env
# Mở .env và thêm OPENAI_API_KEY nếu muốn bật AI
npm start
```
Mở http://localhost:3000

## Cấu hình AI
Trong `.env`:
```env
OPENAI_API_KEY=your_key_here
OPENAI_MODEL=gpt-6-luna
```
Nếu model không có quyền trong project API của bạn, thay `OPENAI_MODEL` bằng model được bật trong tài khoản API. Không bao giờ commit `.env`.

## Cấu hình scan
```env
PORT=3000
NEWS_REFRESH_MS=300000
AI_REQUESTS_PER_DAY=12
KLINE_SYMBOL_LIMIT=30
```
- `KLINE_SYMBOL_LIMIT`: số hợp đồng thanh khoản cao nhất được tải 250 nến 1m để tính chỉ báo.
- All-market ticker được nhận từ stream. Lịch sử 1m được preload cho nhóm top volume; mã khác được nạp khi người dùng mở mã đó. Scanner đánh dấu CHỜ NẾN cho các mã thiếu 200 bars.
- AI chỉ chạy khi người dùng nhấn “AI phân tích ngay”; giới hạn mặc định 12 lượt/ngày để kiểm soát chi phí.
- Khi app đang mở, trình duyệt gửi WebSocket heartbeat mỗi 25 giây để giúp Render Free nhận traffic; iOS có thể tạm dừng timer khi app chạy nền nên không đảm bảo uptime 24/7.

## API
- `GET /api/health`
- `GET /api/market`
- `GET /api/symbols`
- `GET /api/news`
- `GET /api/ai`
- `POST /api/ai/analyze`
- WebSocket `/live`

## Quan trọng về mã dầu
Binance Futures không đảm bảo có hợp đồng WTI/Brent dạng perpetual hoặc mã `CLUSDT`/`BZUSDT`. App kiểm tra danh sách thực tế qua `exchangeInfo`; nếu không tìm thấy, hiển thị 0 mã. Để có giá dầu chuẩn cần thêm nguồn market data hàng hóa riêng (ví dụ nhà cung cấp dữ liệu futures/commodities có giấy phép phù hợp).

## Ghi chú kỹ thuật và giới hạn
- RSS Google News là lớp tổng hợp tin; độ trễ, nội dung và khả năng truy cập tùy khu vực/nguồn.
- Điểm kỹ thuật là bộ lọc quy tắc, không phải xác suất chiến thắng.
- AI chỉ tóm tắt và diễn giải dữ liệu được cung cấp; có thể sai hoặc bỏ sót. Cần xác minh tin gốc, thời gian xuất bản và dữ liệu.
- Ứng dụng chưa có đặt lệnh, quản lý tài khoản, backtest, lưu trữ dữ liệu lịch sử dài hạn hay cảnh báo push.
- Hạn mức AI là bộ đếm trong bộ nhớ tiến trình, có thể reset sau khi máy chủ khởi động lại; không xem đây là giải pháp chống lạm dụng production hoàn chỉnh.

## Deploy
Có thể triển khai trên VPS hoặc nền tảng Node.js hỗ trợ WebSocket. Đặt biến môi trường trong dashboard hosting, không upload `.env` công khai. Mở cổng ứng dụng và bật HTTPS/WSS qua reverse proxy.


## iPhone / Android home-screen app
This project now includes a PWA manifest, service worker and app icon. After deploying it to a public HTTPS URL:
- iPhone: open the URL in Safari -> Share -> Add to Home Screen.
- Android: open the URL in Chrome -> menu -> Install app / Add to Home screen.
The app shell can be cached, but live prices, news and AI require an internet connection and the Node backend to remain running.

## Render deployment
The repository includes `render.yaml`. Push the project to a GitHub repository, then in Render choose New -> Blueprint and select that repository. Add `OPENAI_API_KEY` in the Render environment settings. Keep it private. Once deployed, open the HTTPS service URL on the phone and add it to the home screen.


## Mobile reliability updates (v2.1)

- Binance 1m candle preload starts only after the ticker stream is populated.
- The app caches 1m candles for any selected USDT perpetual, calculates its indicators, and subscribes the selected symbol/timeframe to a live kline stream.
- LONG/SHORT labels are held at CHỜ NẾN until 200 one-minute candles are available.
- Crude-oil candidates and natural-gas candidates are classified separately; a symbol named GAS is not reported as an oil contract.
- The complete USDT perpetual universe is sent to the client. The market and scanner tables support searching across the full received universe.
- AI is manual-trigger only (no background calls) and capped at 12 requests per UTC day by default to reduce unexpected usage.
- The default model is `gpt-6-luna`, a lower-cost model supported by the OpenAI Responses API. OpenAI API usage is billed separately from Render hosting.


## Giới hạn phạm vi dữ liệu

- Universe hiện tại là Binance USDⓈ-M Futures, USDT perpetual đang giao dịch; chưa bao gồm COIN-M Futures.
- Biểu đồ/indicator 1m/5m/15m/1h lấy trực tiếp từ Binance Futures. Phần giá WTI/Brent chuẩn chưa được tích hợp; app báo riêng nếu chỉ phát hiện mã khí hoặc mã có tên tương tự.
- Render Free có thể spin down sau 15 phút không có inbound HTTP hoặc WebSocket traffic; lần truy cập tiếp theo cần cold start. Vì vậy bản miễn phí không bảo đảm theo dõi 24/7.
