"""Bài 3: Thiết bị đèn nhận ON/OFF và gửi trạng thái sau mỗi lệnh hợp lệ."""

import json
import threading

import paho.mqtt.client as mqtt

from mqtt_common import COMMAND_TOPIC, STATUS_TOPIC, Session, parser, run


def main():
    args = parser(__doc__).parse_args()
    state = {"device_id": "light01", "status": "OFF"}

    def on_message(client, userdata, message):
        try:
            command = message.payload.decode("utf-8").strip().upper()
        except UnicodeDecodeError:
            print("Bỏ qua lệnh không phải UTF-8", flush=True)
            return
        if command not in ("ON", "OFF"):
            print(f"Lệnh không hợp lệ: {command!r}. Chỉ chấp nhận ON/OFF.", flush=True)
            return
        state["status"] = command
        payload = json.dumps(state)
        # Callback chạy trên luồng mạng: không wait_for_publish ở đây để tránh deadlock.
        info = client.publish(STATUS_TOPIC, payload, qos=1, retain=True)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"Lỗi gửi trạng thái: {mqtt.error_string(info.rc)}", flush=True)
        else:
            print(f"Đèn: {command}\nPhản hồi: {payload}", flush=True)

    with Session(args, "device-bai3", (COMMAND_TOPIC,), on_message) as session:
        # Trạng thái retained giúp controller nhận trạng thái khi mở sau thiết bị.
        session.publish(STATUS_TOPIC, json.dumps(state), retain=True)
        print("Thiết bị light01 sẵn sàng. Trạng thái ban đầu: OFF", flush=True)
        threading.Event().wait()


if __name__ == "__main__":
    run(main)
