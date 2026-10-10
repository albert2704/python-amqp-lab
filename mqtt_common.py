"""Cấu hình và vòng lặp MQTT dùng chung cho cả ba bài thực hành."""

import argparse
import json
import math
import os
import sys
import threading
import uuid

import paho.mqtt.client as mqtt

MESSAGE_TOPIC = "iot/lab/message"
SENSOR_TOPIC = "iot/lab/sensor01/data"
COMMAND_TOPIC = "iot/lab/light01/cmd"
STATUS_TOPIC = "iot/lab/light01/status"


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("Giá trị phải là số nguyên dương")
    return number


def nonnegative_int(value):
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("Giá trị phải >= 0")
    return number


def positive_float(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("Giá trị phải là số hữu hạn > 0")
    return number


def parser(description):
    result = argparse.ArgumentParser(description=description)
    result.add_argument("--host", default=os.getenv("MQTT_HOST", "127.0.0.1"))
    result.add_argument("--port", type=positive_int,
                        default=os.getenv("MQTT_PORT", "1883"))
    result.add_argument("--username", default=os.getenv("MQTT_USERNAME"))
    result.add_argument("--password", default=os.getenv("MQTT_PASSWORD"))
    result.add_argument("--tls", action="store_true",
                        default=os.getenv("MQTT_TLS", "0") == "1")
    result.add_argument("--ca-cert", default=os.getenv("MQTT_CA_CERT"))
    return result


class Session:
    """Chờ CONNACK/SUBACK trước khi sử dụng; tự subscribe lại khi kết nối lại."""

    def __init__(self, args, label, subscriptions=(), on_message=None):
        self.args = args
        self.subscriptions = subscriptions
        self.ready = threading.Event()
        self.error = None
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"{label}-{uuid.uuid4().hex[:8]}",
            protocol=mqtt.MQTTv311,
        )
        if args.username:
            self.client.username_pw_set(args.username, args.password)
        if args.tls:
            self.client.tls_set(ca_certs=args.ca_cert)
        self.client.reconnect_delay_set(min_delay=1, max_delay=10)
        self.client.connect_timeout = 5
        self.client.on_connect = self._on_connect
        self.client.on_subscribe = self._on_subscribe
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = on_message

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        self.error = None
        if reason_code.is_failure:
            self.error = f"Broker từ chối kết nối: {reason_code}"
            self.ready.set()
            return
        print(f"Đã kết nối MQTT: {self.args.host}:{self.args.port}", flush=True)
        if self.subscriptions:
            rc, _ = client.subscribe([(topic, 1) for topic in self.subscriptions])
            if rc != mqtt.MQTT_ERR_SUCCESS:
                self.error = f"Không thể subscribe: {mqtt.error_string(rc)}"
                self.ready.set()
        else:
            self.ready.set()

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties):
        if any(code.is_failure for code in reason_codes):
            self.error = "Broker từ chối subscribe"
        else:
            print("Đang lắng nghe: " + ", ".join(self.subscriptions), flush=True)
        self.ready.set()

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.ready.clear()
        if reason_code.is_failure:
            print(f"Mất kết nối ({reason_code}); đang thử kết nối lại...", flush=True)

    def __enter__(self):
        try:
            self.client.connect(self.args.host, self.args.port, keepalive=60)
            self.client.loop_start()
            if not self.ready.wait(10):
                raise TimeoutError("Hết thời gian chờ phản hồi kết nối/subscribe")
            if self.error:
                raise RuntimeError(self.error)
            return self
        except BaseException:
            self.client.disconnect()
            self.client.loop_stop()
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.disconnect()
        self.client.loop_stop()

    def publish(self, topic, payload, retain=False):
        """Chỉ gọi từ luồng chính: chờ broker xác nhận QoS 1 trước khi thoát."""
        info = self.client.publish(topic, payload, qos=1, retain=retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"Không thể publish: {mqtt.error_string(info.rc)}")
        info.wait_for_publish(timeout=10)
        if not info.is_published():
            raise TimeoutError("Broker chưa xác nhận thông điệp sau 10 giây")


def json_object(payload):
    data = json.loads(payload.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Payload phải là JSON object")
    return data


def sensor_values(payload):
    data = json_object(payload)
    if data.get("device_id") != "sensor01":
        raise ValueError("device_id phải là sensor01")
    values = []
    for key in ("temperature", "humidity"):
        value = data.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{key} phải là số")
        if not math.isfinite(value):
            raise ValueError(f"{key} phải hữu hạn")
        values.append(value)
    if not 0 <= values[1] <= 100:
        raise ValueError("humidity phải trong khoảng 0..100")
    return data, values[0], values[1]


def alerts(temperature, humidity):
    result = []
    if temperature > 35:
        result.append("CANH BAO: Nhiet do cao")
    if humidity < 40:
        result.append("CANH BAO: Do am thap")
    return result


def run(main):
    """Các chương trình thoát gọn khi Ctrl+C; lỗi thật trả exit code 1."""
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\nĐã dừng chương trình.")
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"Lỗi: {exc}. Kiểm tra broker và cấu hình MQTT.", file=sys.stderr)
        sys.exit(1)
