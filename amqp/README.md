# Thực hành Python với AMQP / RabbitMQ

Ba bài thực hành dùng Python **3.9 trở lên**, `pika==1.3.2` và RabbitMQ qua AMQP 0-9-1: gửi lời chào, giám sát cảm biến và định tuyến cảnh báo. Cảm biến là mô phỏng; không cần phần cứng.

| Bài | Luồng thông điệp |
| --- | --- |
| 1 | Producer → default exchange → `iot_lab_queue` → consumer |
| 2 | Sensor producer → default exchange → `sensor_data_queue` → monitor |
| 3 | Alert producer → direct exchange `iot_alert_exchange` → `warning_queue` / `critical_queue` theo routing key |

## 1. Cài đặt và chạy broker

Mở terminal trong thư mục `amqp/`. Trên macOS/Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Trên Windows PowerShell:

```powershell
py -3 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Mỗi terminal mới đều cần vào `amqp/` và kích hoạt `.venv`. Nếu dùng broker cục bộ, cài và mở Docker Desktop, rồi chạy:

```bash
docker compose up -d
docker compose ps
```

Đợi RabbitMQ sẵn sàng trước khi chạy Python. `compose.yaml` dùng image `rabbitmq:4-management`, mở AMQP tại `localhost:5672` và trang quản trị tại [http://localhost:15672](http://localhost:15672). Tài khoản demo: **lab / lab**. Dừng broker khi làm xong bằng `docker compose stop`.

Mặc định mọi chương trình dùng `amqp://lab:lab@127.0.0.1:5672/%2F`, trong đó `%2F` là vhost `/`. Nếu có broker của lớp, bỏ qua Docker và đặt cùng `AMQP_URL` trong **từng terminal**. Ví dụ macOS/Linux:

```bash
export AMQP_URL='amqp://USER:PASSWORD@HOST:5672/VHOST'
export STUDENT_NAME='Họ tên của bạn'
export STUDENT_ID='Mã sinh viên của bạn'
```

PowerShell:

```powershell
$env:AMQP_URL='amqp://USER:PASSWORD@HOST:5672/VHOST'
$env:STUDENT_NAME='Họ tên của bạn'
$env:STUDENT_ID='Mã sinh viên của bạn'
```

Thay các giá trị viết hoa bằng cấu hình thực tế; URL-encode riêng tên đăng nhập, mật khẩu và tên vhost nếu có ký tự đặc biệt. Tệp `.env` **không được chương trình Python tự động nạp**. Không đưa mật khẩu thật vào Git. Khi dùng broker chung, nên xin vhost riêng: các consumer cùng queue trong cùng vhost sẽ chia nhau nhận thông điệp.

## 2. Bài 1 — Gửi và nhận lời chào

Terminal A:

```bash
python consumer_bai1.py
```

Terminal B:

```bash
python producer_bai1.py --name "Họ tên của bạn" --student-id "Mã sinh viên của bạn"
```

Producer ưu tiên `--name` và `--student-id`, sau đó đến `STUDENT_NAME` / `STUDENT_ID`; thông tin còn thiếu được hỏi từ bàn phím. Mặc định gửi một lời chào. Gửi ba lần cách nhau một giây:

```bash
python producer_bai1.py --count 3 --interval 1
```

`--count` ở bài 1 phải lớn hơn 0; `--interval` mặc định là 1 giây. Consumer in lời chào chứa họ tên, mã sinh viên và thời điểm nhận theo giờ máy chạy consumer.

Ví dụ kết quả mong đợi, với thời gian nhận minh họa:

```text
Da nhan message: Xin chao tu ung dung Python AMQP - Mã sinh viên của bạn - Họ tên của bạn
Thoi gian nhan: 10:15:20
```

## 3. Bài 2 — Mô phỏng cảm biến và cảnh báo

Terminal A:

```bash
python monitor_consumer_bai2.py
```

Terminal B:

```bash
python sensor_producer_bai2.py
```

Producer mặc định gửi mỗi **3 giây**, chạy liên tục (`--count 0`), với `--device-id sensor01`. Nhiệt độ ngẫu nhiên trong 20–40 °C, độ ẩm trong 30–80%. JSON có đúng ba trường:

```json
{"device_id": "sensor01", "temperature": 36.1, "humidity": 38.7}
```

Để thử chắc chắn cả hai điều kiện cảnh báo:

```bash
python sensor_producer_bai2.py --temperature 36.1 --humidity 38.7 --count 3 --interval 1
```

Kết quả mong đợi trên monitor:

```text
Device: sensor01
Temperature: 36.1
Humidity: 38.7
CANH BAO: Nhiet do cao
CANH BAO: Do am thap
```

Cảnh báo chỉ xuất hiện khi nhiệt độ **> 35** hoặc độ ẩm **< 40**; tại đúng 35 °C và 40% thì không cảnh báo. `--temperature` / `--humidity` cố định giá trị để demo, bỏ cờ nào thì trường đó được sinh ngẫu nhiên. Monitor từ chối payload không hợp lệ và tiếp tục chờ thông điệp mới.

## 4. Bài 3 — Định tuyến theo mức cảnh báo

Terminal A:

```bash
python warning_consumer_bai3.py
```

Terminal B:

```bash
python critical_consumer_bai3.py
```

Terminal C:

```bash
python alert_producer_bai3.py
```

Mặc định producer gửi ba mức `info`, `warning`, `critical`. Direct exchange đưa thông điệp vào queue có binding khớp chính xác routing key: consumer warning chỉ nhận `warning`, consumer critical chỉ nhận `critical`. `info` không có queue đích trong bài này nên bị loại bỏ có chủ đích, trừ khi bạn thêm binding tương ứng.

Gửi riêng một mức với nội dung tự chọn:

```bash
python alert_producer_bai3.py --level warning --message "Nhiệt độ vượt ngưỡng"
python alert_producer_bai3.py --level critical --message "Cần kiểm tra thiết bị ngay"
```

Kết quả mong đợi từ hai lệnh trên:

```text
# Terminal A
[warning_queue] Da nhan: Nhiệt độ vượt ngưỡng
# Terminal B
[critical_queue] Da nhan: Cần kiểm tra thiết bị ngay
```

Producer cũng khai báo cả hai queue và binding, nên chạy producer trước vẫn lưu được warning/critical để consumer nhận sau.

## 5. Dừng, kiểm thử và ý nghĩa lưu trữ

Nhấn **Ctrl+C** để dừng chương trình đang chờ hoặc phát liên tục. Mọi consumer hỗ trợ `--max-messages N`: mặc định `0` là chờ liên tục; số dương khiến consumer dừng sau N thông điệp **hợp lệ**. Ví dụ `python consumer_bai1.py --max-messages 1`. Mọi chương trình hỗ trợ `--help`.

Queue và exchange do bài tạo được khai báo durable; thông điệp được gửi persistent, producer chờ publisher confirm và consumer xác nhận thủ công sau khi xử lý. Các thiết lập này hỗ trợ giữ thông điệp qua lần khởi động lại broker có giữ dữ liệu. Cơ chế giao lại có thể tạo bản sao (at-least-once), nên không coi mỗi dòng log là một sự kiện duy nhất. Publisher confirm cũng không có nghĩa consumer đã xử lý xong.

Chạy kiểm thử đơn vị trong `amqp/`:

```bash
python -m unittest discover -s tests -v
```

Chạy kiểm thử đầy đủ với broker thật khi Docker đang hoạt động:

```bash
python tests/integration.py
```

Lệnh này tự tạo một RabbitMQ container riêng trên cổng localhost ngẫu nhiên, chạy cả bảy chương trình rồi dừng và xóa container thử nghiệm. Lần đầu có thể cần tải image. Không dùng broker hay queue hiện có của lớp. Phần kiểm thử Ctrl+C tự bỏ qua trên Windows.

### Kết quả đã kiểm chứng ngày 10/10/2026

Đã chạy thành công **14 kiểm thử đơn vị và 6 kiểm thử tích hợp** trên macOS với Python 3.14, Pika 1.3.2 và RabbitMQ 4.3.6:

| Nội dung | Kết quả thực tế |
| --- | --- |
| Bài 1 | Gửi và nhận đủ 3 lời chào có tên tiếng Việt, mã thử nghiệm và thời điểm nhận |
| Bài 2 | Gửi 2 JSON ngẫu nhiên với chu kỳ mặc định 3 giây; đúng tên trường và khoảng giá trị |
| Ngưỡng cảnh báo | Đúng 35 °C và 40% không cảnh báo; kiểm tra riêng từng ngưỡng và cả hai ngưỡng |
| Payload lỗi | Monitor loại JSON sai, tiếp tục nhận và xử lý 4 thông điệp hợp lệ |
| Bài 3 | Warning và critical đến đúng queue; info không đến hai queue này; producer chạy trước consumer vẫn nhận đủ |
| Dừng và lỗi kết nối | Ctrl+C thoát gọn; broker không có sẵn trả mã lỗi 1 và hướng dẫn kiểm tra |

Các khối output ở phần hướng dẫn phía trên là ví dụ minh họa; giá trị ngẫu nhiên và thời gian thực tế sẽ khác. Họ tên và mã sinh viên khi nộp bài do bạn truyền vào ở Bài 1.

## 6. Xử lý lỗi thường gặp

| Triệu chứng | Cách kiểm tra |
| --- | --- |
| `Connection refused` hoặc không kết nối được | Kiểm tra `docker compose ps`, đợi broker sẵn sàng; kiểm tra host/cổng trong `AMQP_URL`. Cổng 15672 dành cho quản trị, không dùng để kết nối AMQP. |
| Lỗi đăng nhập / quyền truy cập vhost | Kiểm tra user, mật khẩu, tên vhost đã URL-encode và quyền của tài khoản trên vhost đó. |
| `ModuleNotFoundError: No module named 'pika'` | Kích hoạt đúng `.venv`, chạy `python -m pip install -r requirements.txt`. |
| `PRECONDITION_FAILED` | Queue/exchange cùng tên đã tồn tại với cấu hình khác, ví dụ durable hoặc loại exchange khác. Chọn một vhost riêng được cấp quyền hoặc thống nhất cấu hình với quản trị viên; không xóa queue dùng chung. |
| Consumer không thấy thông điệp | Đảm bảo producer và consumer dùng cùng broker/vhost. Kiểm tra queue trong trang quản trị; consumer khác cùng queue có thể đã nhận. Bài 3 không nhận `info` là đúng thiết kế. |

## 7. Tệp chương trình và tài liệu tham khảo

Bảy chương trình của bài thực hành: `producer_bai1.py`, `consumer_bai1.py`, `sensor_producer_bai2.py`, `monitor_consumer_bai2.py`, `alert_producer_bai3.py`, `warning_consumer_bai3.py`, `critical_consumer_bai3.py`. Giữ kèm module dùng chung `amqp_common.py`, `requirements.txt`, `compose.yaml` và tài liệu này khi sao chép dự án.

- [RabbitMQ: gửi và nhận thông điệp bằng Python](https://www.rabbitmq.com/tutorials/tutorial-one-python) giải thích producer, queue và consumer.
- [RabbitMQ: direct exchange và routing key](https://www.rabbitmq.com/tutorials/tutorial-four-python) giải thích định tuyến theo mức cảnh báo.
- [Pika BlockingConnection](https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html) mô tả API kết nối, publisher confirm và consumer.
