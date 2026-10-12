# Thực hành Python với MQTT

## 1. Thông tin sinh viên

| Họ và tên | Mã sinh viên |
| :--- | :---: |
| Lê Đình Thức | B23DCCN803 |
| Đỗ Minh Hoàng | B23DCCN328 |
| Nguyễn Hoàng Quốc Việt | B22DCCN897 |

## 2. Cài đặt

Các chương trình MQTT dùng Python 3.9 trở lên và `paho-mqtt`. Broker Python cục bộ và kiểm thử cần Python 3.10 trở lên.

Trên macOS/Linux, mở terminal trong thư mục dự án:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-broker.txt
```

Trên Windows PowerShell:

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements-broker.txt
```

Mỗi terminal mới cần kích hoạt `.venv`. Nếu đã có broker của lớp, chỉ cần cài `python -m pip install -r requirements.txt`.

## 3. MQTT broker

Mặc định tất cả chương trình kết nối `127.0.0.1:1883`, MQTT 3.1.1 qua TCP, không yêu cầu tài khoản. Chạy broker cục bộ trong một terminal riêng và giữ terminal này mở:

```bash
python broker_local.py
```

Broker này dùng aMQTT, chỉ nhận kết nối trên máy đang chạy. Đèn và cảm biến là mô phỏng phần mềm; không cần phần cứng.

Nếu port 1883 đang được sử dụng:

```bash
python broker_local.py --port 1884
```

Khi đó thêm `--port 1884` cho **mọi** chương trình bên dưới. Nếu dùng broker bên ngoài, bỏ qua `broker_local.py` và thêm cùng `--host DIA_CHI_BROKER --port PORT` cho cả hai chương trình của từng bài.

Các biến môi trường tùy chọn:

| Biến | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `MQTT_HOST` | `127.0.0.1` | Địa chỉ broker |
| `MQTT_PORT` | `1883` | Cổng MQTT |
| `MQTT_USERNAME` | Không đặt | Tài khoản broker |
| `MQTT_PASSWORD` | Không đặt | Mật khẩu broker |
| `MQTT_TLS` | `0` | Đặt `1` để bật TLS |
| `MQTT_CA_CERT` | Không đặt | Đường dẫn CA riêng; mặc định dùng CA hệ thống |
| `STUDENT_NAME` | Không đặt | Họ tên sinh viên |
| `STUDENT_ID` | Không đặt | Mã sinh viên |

Ví dụ cấu hình broker của lớp trên macOS/Linux; thay địa chỉ bằng broker thực tế:

```bash
export MQTT_HOST="dia-chi-broker-cua-lop"
export MQTT_PORT="1883"
export STUDENT_NAME="Nguyễn Hoàng Quốc Việt"
export STUDENT_ID="B22DCCN897"
```

PowerShell dùng `$env:MQTT_HOST="dia-chi-broker-cua-lop"` và tương tự cho các biến khác. Các biến chỉ áp dụng trong terminal đã đặt chúng. Mọi chương trình hỗ trợ `--host`, `--port`, `--username`, `--password`, `--tls`, `--ca-cert`; dùng `--help` để xem tùy chọn. Khi broker yêu cầu TLS, bật `--tls` và đặt đúng port do broker cung cấp, thường là 8883. Cấu hình tài khoản qua biến môi trường để tránh ghi mật khẩu vào mã nguồn hoặc Git.

## 4. Bài 1 gửi và nhận thông điệp

Terminal A, chạy subscriber trước và chờ dòng `Đang lắng nghe`:

```bash
python subscriber_bai1.py
```

Terminal B:

```bash
python publisher_bai1.py --name "Nguyễn Hoàng Quốc Việt" --student-id "B22DCCN897"
```

Nếu bỏ `--name`/`--student-id` và chưa đặt biến môi trường, chương trình hỏi từ bàn phím. Để gửi nhiều lần:

```bash
python publisher_bai1.py --count 3 --interval 1
```

Topic: `iot/lab/message`. Subscriber hiển thị topic, lời chào kèm danh tính và thời điểm nhận theo giờ máy đang chạy:

```text
Nhan duoc message:
Topic: iot/lab/message
Payload: Xin chao tu client Python MQTT - B22DCCN897 - Nguyễn Hoàng Quốc Việt
Time: 10:15:20
```

## 5. Bài 2 mô phỏng cảm biến

Terminal A:

```bash
python monitor_subscriber_bai2.py
```

Terminal B:

```bash
python sensor_publisher_bai2.py
```

Publisher gửi JSON lên `iot/lab/sensor01/data` mỗi 3 giây, nhiệt độ ngẫu nhiên 20–40 °C và độ ẩm 30–80%. Payload gồm đúng ba trường `device_id`, `temperature`, `humidity`.

Để chắc chắn xuất hiện cả hai cảnh báo, chạy:

```bash
python sensor_publisher_bai2.py --temperature 36.1 --humidity 38.7 --count 3
```

Kết quả mong đợi trên monitor:

```text
Device: sensor01
Temperature: 36.1 C
Humidity: 38.7 %
CANH BAO: Nhiet do cao
CANH BAO: Do am thap
```

Cảnh báo khi nhiệt độ **> 35** và độ ẩm **< 40**. Tại đúng 35 °C và 40% không cảnh báo. Dữ liệu JSON sai hoặc thiếu trường được thông báo và bỏ qua; monitor tiếp tục chạy. `--count 0` là chạy liên tục, `--interval` đổi chu kỳ nếu cần thử nhanh.

## 6. Bài 3 điều khiển đèn

Terminal A, chạy thiết bị trước và chờ dòng `Thiết bị light01 sẵn sàng`:

```bash
python device_bai3.py
```

Terminal B:

```bash
python controller_bai3.py
```

Nhập `ON`, `OFF`, sau đó `EXIT` để thoát controller. Thiết bị subscribe `iot/lab/light01/cmd` và publish JSON trên `iot/lab/light01/status` sau **mỗi** lệnh hợp lệ, kể cả lệnh lặp lại:

```json
{"device_id": "light01", "status": "ON"}
```

Thiết bị bắt đầu ở trạng thái `OFF`. Topic trạng thái có `retain=True`, nên controller mở sau thiết bị vẫn nhận trạng thái mới nhất. Lệnh điều khiển không retained. Lệnh sai không thay đổi đèn; controller và thiết bị đều báo lỗi. Controller chờ tối đa 5 giây cho phản hồi; nếu chưa có, kiểm tra thiết bị đang chạy và cả hai dùng cùng broker.

Demo không cần nhập bàn phím:

```bash
python controller_bai3.py --commands ON OFF EXIT
```

Mô hình này dùng một thiết bị `light01` và một controller. Trạng thái retained chỉ là trạng thái được gửi gần nhất, không phải dấu hiệu thiết bị còn online.

## 7. Kiểm tra và kết quả

Nhấn `Ctrl+C` để dừng subscriber, sensor, thiết bị và broker. Publisher hữu hạn tự thoát sau khi broker xác nhận các thông điệp QoS 1. Subscribe được đăng ký lại khi client tự kết nối lại.

Chạy kiểm thử tự động; bộ kiểm thử mở broker riêng trên cổng tạm, khởi chạy sáu chương trình thật và dọn các tiến trình khi xong:

```bash
python -m unittest discover -s tests -v
```

Bộ kiểm thử kiểm tra lời chào/danh tính/thời gian, chu kỳ mặc định 3 giây, JSON cảm biến, điều kiện cảnh báo ở biên, dữ liệu sai, lệnh ON/OFF lặp lại, lệnh sai và lỗi khi broker không chạy.

Kết quả kiểm tra ngày 10/10/2026 trên macOS với Python 3.14.7: **7/7 kiểm thử đạt**. Cả ba bài đã trao đổi thông điệp qua broker cục bộ. Kiểm thử cũng xác nhận trạng thái retained không được coi là phản hồi cho lệnh mới khi thiết bị offline. Broker có tài khoản/TLS bên ngoài chưa được kiểm tra.

Nếu gặp `Connection refused`, chạy broker trước và kiểm tra host/port. Nếu gặp `ModuleNotFoundError`, kích hoạt `.venv` và cài file requirements phù hợp. Hai chương trình trong một bài phải dùng cùng broker; chạy subscriber trước publisher để nhận các thông điệp không retained. QoS 1 có thể giao thông điệp nhiều hơn một lần, nên log có thể lặp lại.

## 8. Các file nộp bài

- `publisher_bai1.py`, `subscriber_bai1.py`
- `sensor_publisher_bai2.py`, `monitor_subscriber_bai2.py`
- `device_bai3.py`, `controller_bai3.py`
- `mqtt_common.py` — module dùng chung, cần đi cùng sáu chương trình
- `requirements.txt`, `README.md`, `README.txt`
- `broker_local.py`, `requirements-broker.txt`, `tests/test_lab.py` — broker và kiểm thử tùy chọn

## Tài liệu thư viện

- [Eclipse Paho Python](https://eclipse.dev/paho/clients/python/docs/)
- [aMQTT broker API](https://amqtt.readthedocs.io/en/v0.11.3/references/broker/)
- [aMQTT broker configuration](https://amqtt.readthedocs.io/en/v0.11.3/references/broker_config/)
