"""Bài 3: Nhập ON/OFF/EXIT và nhận phản hồi trạng thái từ thiết bị đèn."""

import json
import queue
import time

from mqtt_common import COMMAND_TOPIC, STATUS_TOPIC, Session, json_object, parser, run


def main():
    cli = parser(__doc__)
    cli.add_argument("--commands", nargs="+", help="Danh sách lệnh cho demo tự động")
    args = cli.parse_args()
    responses = queue.Queue()

    def on_message(client, userdata, message):
        try:
            data = json_object(message.payload)
            if data.get("device_id") != "light01" or data.get("status") not in ("ON", "OFF"):
                raise ValueError("Trạng thái light01 không hợp lệ")
        except (ValueError, UnicodeDecodeError) as exc:
            print(f"Bỏ qua phản hồi không hợp lệ: {exc}", flush=True)
            return
        # Bản retained là trạng thái cũ, không phải phản hồi cho lệnh vừa gửi.
        if not message.retain:
            responses.put(data["status"])
        print(f"Trang thai nhan duoc:\n{json.dumps(data, ensure_ascii=False)}", flush=True)

    with Session(args, "controller-bai3", (STATUS_TOPIC,), on_message) as session:
        commands = iter(args.commands) if args.commands else None
        while True:
            command = (next(commands, "EXIT") if commands is not None else input("Nhap lenh (ON/OFF/EXIT): ")).strip().upper()
            if command == "EXIT":
                break
            if command not in ("ON", "OFF"):
                print("Lệnh không hợp lệ. Hãy nhập ON, OFF hoặc EXIT.", flush=True)
                continue
            while True:
                try:
                    responses.get_nowait()  # Bỏ phản hồi cũ trước khi gửi lệnh mới.
                except queue.Empty:
                    break
            session.publish(COMMAND_TOPIC, command)
            print(f"Da gui lenh {command} toi light01", flush=True)
            deadline = time.monotonic() + 5
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    print("Chưa nhận phản hồi. Kiểm tra device_bai3.py đang chạy.", flush=True)
                    break
                try:
                    if responses.get(timeout=remaining) == command:
                        break
                except queue.Empty:
                    print("Chưa nhận phản hồi. Kiểm tra device_bai3.py đang chạy.", flush=True)
                    break


if __name__ == "__main__":
    run(main)
