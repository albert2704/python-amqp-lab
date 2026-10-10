"""Bài 1: nhận lời chào và in thời điểm nhận theo giờ máy đang chạy."""

from datetime import datetime

from amqp_common import MESSAGE_QUEUE, connection, consume, parser, run


def show_message(body):
    message = body.decode("utf-8")
    print(f"Da nhan message: {message}", flush=True)
    print(f"Thoi gian nhan: {datetime.now():%H:%M:%S}", flush=True)


def main():
    args = parser(__doc__, consumer=True).parse_args()
    with connection() as conn:
        channel = conn.channel()
        channel.queue_declare(queue=MESSAGE_QUEUE, durable=True)
        consume(channel, MESSAGE_QUEUE, show_message, args.max_messages)


if __name__ == "__main__":
    run(main)
