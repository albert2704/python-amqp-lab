"""Bài 3: chỉ nhận cảnh báo có routing key critical."""

from amqp_common import (bind_alert_queue, connection, consume,
                         declare_alert_exchange, parser, run)


def show_critical(body):
    print(f"[critical_queue] Da nhan: {body.decode('utf-8')}", flush=True)


def main():
    args = parser(__doc__, consumer=True).parse_args()
    with connection() as conn:
        channel = conn.channel()
        declare_alert_exchange(channel)
        queue = bind_alert_queue(channel, "critical")
        consume(channel, queue, show_critical, args.max_messages)


if __name__ == "__main__":
    run(main)
