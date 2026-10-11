"""Bài 2: mô phỏng cảm biến gửi dữ liệu JSON mỗi 3 giây."""

from datetime import datetime
import json
import random

from amqp_common import (SENSOR_QUEUE, connection, finite_float, humidity_float,
                         nonempty, nonnegative_int, parser, positive_float,
                         publish, run)


def make_reading(device_id="sensor01", temperature=None, humidity=None):
    return {
        "device_id": device_id,
        "sent_at": datetime.now().astimezone().isoformat(timespec="milliseconds"),
        "temperature": round(random.uniform(20, 40), 1) if temperature is None else temperature,
        "humidity": round(random.uniform(30, 80), 1) if humidity is None else humidity,
    }


def main():
    cli = parser(__doc__)
    devices = cli.add_mutually_exclusive_group()
    devices.add_argument("--device-id", type=nonempty, default="sensor01")
    devices.add_argument("--device-ids", nargs="+", type=nonempty,
                         help="Mô phỏng 2 hoặc 3 mã cảm biến khác nhau")
    cli.add_argument("--interval", type=positive_float, default=3)
    cli.add_argument("--count", type=nonnegative_int, default=0,
                     help="0 = gửi liên tục; N > 0 = gửi N message cho mỗi cảm biến")
    cli.add_argument("--temperature", type=finite_float, help="Cố định nhiệt độ để thử cảnh báo")
    cli.add_argument("--humidity", type=humidity_float, help="Cố định độ ẩm để thử cảnh báo")
    args = cli.parse_args()
    device_ids = args.device_ids or [args.device_id]
    if args.device_ids and not 2 <= len(device_ids) <= 3:
        cli.error("--device-ids cần 2 hoặc 3 mã cảm biến")
    if len(set(device_ids)) != len(device_ids):
        cli.error("Mỗi cảm biến phải có một device_id khác nhau")

    with connection() as conn:
        channel = conn.channel()
        channel.queue_declare(queue=SENSOR_QUEUE, durable=True)
        channel.confirm_delivery()
        sent = 0
        while args.count == 0 or sent < args.count:
            for device_id in device_ids:
                data = make_reading(device_id, args.temperature, args.humidity)
                body = json.dumps(data, ensure_ascii=False, allow_nan=False)
                publish(channel, SENSOR_QUEUE, body, json_payload=True)
                print(f"Da gui: {body}", flush=True)
            sent += 1
            if args.count == 0 or sent < args.count:
                conn.sleep(args.interval)


if __name__ == "__main__":
    run(main)
