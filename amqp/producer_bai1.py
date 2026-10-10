"""Bài 1: gửi lời chào có họ tên và mã sinh viên vào iot_lab_queue."""

import os

from amqp_common import (MESSAGE_QUEUE, connection, nonempty, parser,
                         positive_float, positive_int, publish, run)


def main():
    cli = parser(__doc__)
    cli.add_argument("--name", type=nonempty, default=os.getenv("STUDENT_NAME"))
    cli.add_argument("--student-id", type=nonempty, default=os.getenv("STUDENT_ID"))
    cli.add_argument("--count", type=positive_int, default=1)
    cli.add_argument("--interval", type=positive_float, default=1)
    args = cli.parse_args()
    name = args.name or input("Ho ten sinh vien: ").strip()
    student_id = args.student_id or input("Ma sinh vien: ").strip()
    if not name or not student_id:
        cli.error("Cần nhập cả họ tên và mã sinh viên")
    message = f"Xin chao tu ung dung Python AMQP - {student_id} - {name}"

    with connection() as conn:
        channel = conn.channel()
        channel.queue_declare(queue=MESSAGE_QUEUE, durable=True)
        channel.confirm_delivery()
        for index in range(args.count):
            publish(channel, MESSAGE_QUEUE, message)
            print(f"Da gui: {message}", flush=True)
            if index + 1 < args.count:
                conn.sleep(args.interval)  # Vẫn xử lý heartbeat khi chờ.


if __name__ == "__main__":
    run(main)
