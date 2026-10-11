"""Kết nối RabbitMQ và các hàm dùng chung cho ba bài thực hành AMQP."""

import argparse
from contextlib import contextmanager
from datetime import datetime
import json
import math
import os
import sys

import pika

MESSAGE_QUEUE = "iot_lab_queue"
SENSOR_QUEUE = "sensor_data_queue"
ALERT_EXCHANGE = "iot_alert_exchange"
TOPIC_ALERT_EXCHANGE = "iot_alert_topic_exchange"
ALL_ALERTS_QUEUE = "all_alerts_queue"
ALERT_QUEUES = {"warning": "warning_queue", "critical": "critical_queue"}
DEFAULT_URL = "amqp://lab:lab@127.0.0.1:5672/%2F"


def nonnegative_int(value):
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("Giá trị phải >= 0")
    return number


def positive_int(value):
    number = nonnegative_int(value)
    if number == 0:
        raise argparse.ArgumentTypeError("Giá trị phải > 0")
    return number


def finite_float(value):
    number = float(value)
    if not math.isfinite(number):
        raise argparse.ArgumentTypeError("Giá trị phải là số hữu hạn")
    return number


def positive_float(value):
    number = finite_float(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("Giá trị phải > 0")
    return number


def humidity_float(value):
    number = finite_float(value)
    if not 0 <= number <= 100:
        raise argparse.ArgumentTypeError("Độ ẩm phải trong khoảng 0..100")
    return number


def nonempty(value):
    value = value.strip()
    if not value:
        raise argparse.ArgumentTypeError("Giá trị không được để trống")
    return value


def parser(description, consumer=False):
    result = argparse.ArgumentParser(
        description=description,
        epilog="Cấu hình broker bằng biến môi trường AMQP_URL (xem README.md).",
    )
    if consumer:
        result.add_argument("--max-messages", type=nonnegative_int, default=0,
                            help="Dừng sau N message hợp lệ; 0 = chạy liên tục")
    return result


@contextmanager
def connection():
    """Giới hạn thời gian kết nối và luôn đóng kết nối khi thoát."""
    try:
        params = pika.URLParameters(os.getenv("AMQP_URL", DEFAULT_URL))
    except (ValueError, TypeError, KeyError):
        # Không đưa URL chứa mật khẩu vào thông báo lỗi.
        raise ValueError("AMQP_URL không hợp lệ; xem mẫu trong README.md") from None
    params.heartbeat = 60
    params.socket_timeout = 5
    params.stack_timeout = 10
    params.blocked_connection_timeout = 10
    params.connection_attempts = 1
    conn = pika.BlockingConnection(params)
    try:
        yield conn
    finally:
        if conn.is_open:
            conn.close()


def declare_alert_exchange(channel):
    channel.exchange_declare(exchange=ALERT_EXCHANGE,
                             exchange_type="direct", durable=True)


def bind_alert_queue(channel, level):
    queue = ALERT_QUEUES[level]
    channel.queue_declare(queue=queue, durable=True)
    channel.queue_bind(queue=queue, exchange=ALERT_EXCHANGE, routing_key=level)
    return queue


def declare_all_alerts_queue(channel):
    """Topic exchange riêng để không đổi kiểu direct exchange đã tồn tại."""
    channel.exchange_declare(exchange=TOPIC_ALERT_EXCHANGE,
                             exchange_type="topic", durable=True)
    channel.queue_declare(queue=ALL_ALERTS_QUEUE, durable=True)
    channel.queue_bind(queue=ALL_ALERTS_QUEUE, exchange=TOPIC_ALERT_EXCHANGE,
                       routing_key="#")
    return ALL_ALERTS_QUEUE


def publish(channel, routing_key, body, exchange="", json_payload=False,
            mandatory=True):
    """Gọi channel.confirm_delivery() trước khi gửi để chờ broker xác nhận."""
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=body.encode("utf-8"),
        properties=pika.BasicProperties(
            content_type="application/json" if json_payload else "text/plain",
            content_encoding="utf-8",
            delivery_mode=2,
        ),
        mandatory=mandatory,
    )


def consume(channel, queue, handler, max_messages=0, *, include_routing_key=False):
    """ACK sau xử lý thành công; loại message lỗi để tránh vòng lặp vô hạn."""
    count = 0

    def callback(ch, method, properties, body):
        nonlocal count
        try:
            if include_routing_key:
                handler(body, method.routing_key)
            else:
                handler(body)
        except (ValueError, UnicodeError) as exc:
            print(f"Bo qua message khong hop le: {exc}", file=sys.stderr, flush=True)
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return
        ch.basic_ack(delivery_tag=method.delivery_tag)
        count += 1
        if max_messages and count >= max_messages:
            ch.stop_consuming()

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=False)
    print(f"Dang lang nghe {queue}. Nhan Ctrl+C de dung.", flush=True)
    channel.start_consuming()


def sensor_values(payload):
    data = json.loads(payload.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Payload phải là JSON object")
    if not isinstance(data.get("device_id"), str) or not data["device_id"].strip():
        raise ValueError("device_id phải là chuỗi không rỗng")
    for key in ("temperature", "humidity"):
        value = data.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{key} phải là số")
        try:
            finite = math.isfinite(value)
        except OverflowError:
            finite = False
        if not finite:
            raise ValueError(f"{key} phải là số hữu hạn")
    if not 0 <= data["humidity"] <= 100:
        raise ValueError("humidity phải trong khoảng 0..100")
    if "sent_at" in data:
        value = data["sent_at"]
        if not isinstance(value, str):
            raise ValueError("sent_at phải là thời gian ISO 8601 có múi giờ")
        try:
            sent_at = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("sent_at phải là thời gian ISO 8601 có múi giờ") from None
        if sent_at.tzinfo is None:
            raise ValueError("sent_at phải có múi giờ")
    return data


def alerts(temperature, humidity):
    result = []
    if temperature > 35:
        result.append("CANH BAO: Nhiet do cao")
    if humidity < 40:
        result.append("CANH BAO: Do am thap")
    return result


def run(main):
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\nDa dung chuong trinh.", flush=True)
    except pika.exceptions.AMQPError as exc:
        print(f"Loi AMQP ({type(exc).__name__}). Kiem tra broker, AMQP_URL "
              "va cau hinh queue/exchange (xem README.md).", file=sys.stderr)
        sys.exit(1)
    except (OSError, ValueError) as exc:
        print(f"Loi: {exc}", file=sys.stderr)
        sys.exit(1)
