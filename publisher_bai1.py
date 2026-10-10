"""Bài 1: Gửi lời chào kèm họ tên và mã sinh viên."""

import os
import time

from mqtt_common import MESSAGE_TOPIC, Session, parser, positive_float, positive_int, run


def main():
    cli = parser(__doc__)
    cli.add_argument("--name", default=os.getenv("STUDENT_NAME", ""))
    cli.add_argument("--student-id", default=os.getenv("STUDENT_ID", ""))
    cli.add_argument("--message", default="Xin chao tu client Python MQTT")
    cli.add_argument("--count", type=positive_int, default=1)
    cli.add_argument("--interval", type=positive_float, default=1.0)
    args = cli.parse_args()
    name = (args.name or input("Họ tên sinh viên: ")).strip()
    student_id = (args.student_id or input("Mã sinh viên: ")).strip()
    if not name or not student_id:
        cli.error("Họ tên và mã sinh viên không được để trống")
    payload = f"{args.message} - {student_id} - {name}"
    with Session(args, "publisher-bai1") as session:
        for index in range(args.count):
            session.publish(MESSAGE_TOPIC, payload)
            print(f"Đã gửi [{index + 1}]: {MESSAGE_TOPIC}\nPayload: {payload}", flush=True)
            if index + 1 < args.count:
                time.sleep(args.interval)


if __name__ == "__main__":
    run(main)
