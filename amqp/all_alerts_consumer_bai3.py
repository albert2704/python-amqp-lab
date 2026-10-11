"""Mở rộng Bài 3: nhận mọi cảnh báo qua topic exchange và ghi log JSON Lines."""

from datetime import datetime
from functools import partial
import json
import os
from pathlib import Path

from amqp_common import (ALL_ALERTS_QUEUE, connection, consume,
                         declare_all_alerts_queue, nonempty, parser, run)


def show_alert(body, routing_key, *, log_file):
    message = body.decode("utf-8")
    record = {
        "received_at": datetime.now().astimezone().isoformat(timespec="milliseconds"),
        "routing_key": routing_key,
        "message": message,
    }
    path = Path(log_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Ghi nối tiếp và flush xuống đĩa trước khi consume() ACK cho broker.
    # Nếu lỗi ghi file, exception thoát ra để kết nối đóng và message được giao lại.
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    print(f"[{ALL_ALERTS_QUEUE}] [{routing_key}] Da nhan: {message}", flush=True)


def main():
    cli = parser(__doc__, consumer=True)
    cli.add_argument("--log-file", type=nonempty, default="logs/alerts.jsonl",
                     help="File UTF-8 ghi nối tiếp; mặc định logs/alerts.jsonl")
    args = cli.parse_args()
    with connection() as conn:
        channel = conn.channel()
        queue = declare_all_alerts_queue(channel)
        consume(channel, queue, partial(show_alert, log_file=args.log_file),
                args.max_messages, include_routing_key=True)


if __name__ == "__main__":
    run(main)
