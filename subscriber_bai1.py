"""Bài 1: Nhận lời chào, hiển thị topic, payload và thời điểm nhận."""

from datetime import datetime
import threading

from mqtt_common import MESSAGE_TOPIC, Session, parser, run


def on_message(client, userdata, message):
    print(
        "Nhan duoc message:\n"
        f"Topic: {message.topic}\n"
        f"Payload: {message.payload.decode('utf-8', errors='replace')}\n"
        f"Time: {datetime.now().strftime('%H:%M:%S')}\n",
        flush=True,
    )


def main():
    args = parser(__doc__).parse_args()
    with Session(args, "subscriber-bai1", (MESSAGE_TOPIC,), on_message):
        threading.Event().wait()  # Chạy liên tục đến khi nhấn Ctrl+C.


if __name__ == "__main__":
    run(main)
