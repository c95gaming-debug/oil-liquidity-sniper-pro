# Oil Liquidity Sniper Pro 2.0

Web app có backend Node.js, Binance USDⓈ-M Futures public market data, RSS news ingestion và OpenAI Responses API tùy chọn.

## Có gì trong bản này
- Kết nối Binance Futures WebSocket market stream (all-market ticker).
- Tự đọc `exchangeInfo` để xác định các hợp đồng USDT perpetual đang giao dịch.
- Tự dò mã có tên/base asset liên quan CL, WTI, Brent, BZ, Oil, Gas, NATGAS. Không tự bịa mã dầu nếu Binance không niêm yết.
- Tải 250 nến 1m cho nhóm hợp đồng USDT có quote volume cao nhất, tính EMA36/89/200 và RSI12; cập nhật nến qua WebSocket.
- Xếp hạng sơ bộ LONG/SHORT/NEUTRAL bằng quy tắc kỹ thuật minh bạch.
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
OPENAI_MODEL=gpt-6.2-astra
```
Nếu model không có quyền trong project API của bạn, thay `OPENAI_MODEL` bằng model được bật trong tài khoản API. Không bao giờ commit `.env`.

## Cấu hình scan
```env
PORT=3000
NEWS_REFRESH_MS=300000
AI_REFRESH_MS=300000
KLINE_SYMBOL_LIMIT=30
```
- `KLINE_SYMBOL_LIMIT`: số hợp đồng thanh khoản cao nhất được tải 250 nến 1m để tính chỉ báo.
- All-market ticker vẫn được nhận từ stream; chỉ nhóm top volume được tính EMA/RSI chi tiết.
- AI tự chạy theo chu kỳ khi đã cấu hình key; nút “AI phân tích ngay” gọi thủ công.

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
- Cần bổ sung giám sát, rate-limit, cache, logging, bảo vệ endpoint và kiểm thử trước khi triển khai production.

## Deploy
Có thể triển khai trên VPS hoặc nền tảng Node.js hỗ trợ WebSocket. Đặt biến môi trường trong dashboard hosting, không upload `.env` công khai. Mở cổng ứng dụng và bật HTTPS/WSS qua reverse proxy.


## iPhone / Android home-screen app
This project now includes a PWA manifest, service worker and app icon. After deploying it to a public HTTPS URL:
- iPhone: open the URL in Safari -> Share -> Add to Home Screen.
- Android: open the URL in Chrome -> menu -> Install app / Add to Home screen.
The app shell can be cached, but live prices, news and AI require an internet connection and the Node backend to remain running.

## Render deployment
The repository includes `render.yaml`. Push the project to a GitHub repository, then in Render choose New -> Blueprint and select that repository. Add `OPENAI_API_KEY` in the Render environment settings. Keep it private. Once deployed, open the HTTPS service URL on the phone and add it to the home screen.
