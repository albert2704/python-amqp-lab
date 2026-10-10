"""Bài 3: gửi info, warning và critical qua direct exchange."""

from amqp_common import (ALERT_EXCHANGE, ALERT_QUEUES, bind_alert_queue,
                         connection, declare_alert_exchange, nonempty,
                         parser, publish, run)

MESSAGES = {
    "info": "He thong giam sat dang hoat dong info",
    "warning": "Nhiet do phong may vuot nguong warning",
    "critical": "Cam bien kho lanh mat ket noi critical",
}


def main():
    cli = parser(__doc__)
    cli.add_argument("--level", choices=MESSAGES, help="Mặc định gửi cả ba mức")
    cli.add_argument("--message", type=nonempty, help="Thay nội dung cảnh báo")
    args = cli.parse_args()

    with connection() as conn:
        channel = conn.channel()
        declare_alert_exchange(channel)
        # Tạo binding trước để warning/critical được giữ cả khi consumer chưa chạy.
        for level in ALERT_QUEUES:
            bind_alert_queue(channel, level)
        channel.confirm_delivery()
        for level in ([args.level] if args.level else MESSAGES):
            message = args.message or MESSAGES[level]
            # info không có queue nhận theo đề bài; cho phép broker bỏ message đó.
            publish(channel, level, message, exchange=ALERT_EXCHANGE,
                    mandatory=level != "info")
            print(f"Da gui [{level}]: {message}", flush=True)


if __name__ == "__main__":
    run(main)
