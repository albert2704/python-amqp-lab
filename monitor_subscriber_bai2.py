"""Bài 2: Phân tích dữ liệu JSON và cảnh báo nhiệt độ/độ ẩm."""

import threading

from mqtt_common import SENSOR_TOPIC, Session, alerts, parser, run, sensor_values


def on_message(client, userdata, message):
    try:
        data, temperature, humidity = sensor_values(message.payload)
    except (ValueError, UnicodeDecodeError) as exc:
        print(f"Bỏ qua dữ liệu không hợp lệ: {exc}", flush=True)
        return
    lines = [
        f"Device: {data['device_id']}",
        f"Temperature: {temperature:.1f} C",
        f"Humidity: {humidity:.1f} %",
        *alerts(temperature, humidity),
    ]
    print("\n".join(lines) + "\n", flush=True)


def main():
    args = parser(__doc__).parse_args()
    with Session(args, "monitor-bai2", (SENSOR_TOPIC,), on_message):
        threading.Event().wait()


if __name__ == "__main__":
    run(main)
