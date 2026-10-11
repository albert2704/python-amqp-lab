"""Kiểm thử cả 8 chương trình với RabbitMQ tạm thời, cần Docker đang chạy.

Chạy từ amqp/: python tests/integration.py
Tự tạo container trên cổng localhost ngẫu nhiên và xóa khi kiểm thử xong.
"""

from datetime import datetime
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

import pika

LAB = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB))
from amqp_common import (MESSAGE_QUEUE, SENSOR_QUEUE, ALERT_QUEUES,
                         ALL_ALERTS_QUEUE, TOPIC_ALERT_EXCHANGE, declare_all_alerts_queue)


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


class RabbitMQLabTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container = "amqp-lab-test-" + uuid.uuid4().hex[:10]
        # Đăng ký dọn dẹp trước, kể cả trường hợp chờ broker thất bại.
        cls.addClassCleanup(cls.stop_broker)
        docker("run", "--detach", "--rm", "--name", cls.container,
               "--publish", "127.0.0.1::5672",
               "--env", "RABBITMQ_DEFAULT_USER=lab",
               "--env", "RABBITMQ_DEFAULT_PASS=lab", "rabbitmq:4-management")
        port = docker("port", cls.container, "5672/tcp").rsplit(":", 1)[1]
        cls.url = f"amqp://lab:lab@127.0.0.1:{port}/%2F"
        cls.env = dict(os.environ, AMQP_URL=cls.url, PYTHONUNBUFFERED="1")
        deadline = time.monotonic() + 60
        while True:
            try:
                conn = pika.BlockingConnection(pika.URLParameters(cls.url))
                conn.close()
                break
            except pika.exceptions.AMQPConnectionError:
                if time.monotonic() >= deadline:
                    raise RuntimeError("RabbitMQ chưa sẵn sàng sau 60 giây") from None
                time.sleep(1)
        print("RabbitMQ:", docker("exec", cls.container,
                                   "rabbitmq-diagnostics", "-q", "server_version"))

    @classmethod
    def stop_broker(cls):
        subprocess.run(["docker", "stop", cls.container], capture_output=True, timeout=30)

    def run_script(self, script, *args, env=None, timeout=20):
        result = subprocess.run(
            [sys.executable, str(LAB / script), *args],
            env=env or self.env, capture_output=True, text=True, timeout=timeout,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def test_01_greeting_and_receipt_time(self):
        self.run_script("producer_bai1.py", "--name", "Nguyễn Văn An",
                        "--student-id", "TEST001", "--count", "3", "--interval", "0.01")
        output = self.run_script("consumer_bai1.py", "--max-messages", "3")
        self.assertEqual(output.count("Da nhan message:"), 3)
        self.assertEqual(output.count("TEST001 - Nguyễn Văn An"), 3)
        self.assertRegex(output, r"Thoi gian nhan: \d{2}:\d{2}:\d{2}")

    def test_02_random_sensor_default_interval(self):
        with pika.BlockingConnection(pika.URLParameters(self.url)) as conn:
            ch = conn.channel()
            ch.queue_declare(queue=SENSOR_QUEUE, durable=True)
            start = time.monotonic()
            self.run_script("sensor_producer_bai2.py", "--count", "2")
            self.assertGreaterEqual(time.monotonic() - start, 2.9)
            readings = []
            for _ in range(2):
                method, _, body = ch.basic_get(SENSOR_QUEUE, auto_ack=True)
                self.assertIsNotNone(method)
                data = json.loads(body)
                self.assertEqual(set(data), {"device_id", "temperature", "humidity", "sent_at"})
                self.assertEqual(data["device_id"], "sensor01")
                self.assertTrue(20 <= data["temperature"] <= 40)
                self.assertTrue(30 <= data["humidity"] <= 80)
                readings.append(data)
            sent = [datetime.fromisoformat(row["sent_at"]) for row in readings]
            self.assertTrue(all(stamp.tzinfo is not None for stamp in sent))
            self.assertGreaterEqual((sent[1] - sent[0]).total_seconds(), 2.9)
            print("Random sensor samples:", readings)

    def test_03_monitor_thresholds_and_invalid_payload(self):
        with pika.BlockingConnection(pika.URLParameters(self.url)) as conn:
            ch = conn.channel()
            ch.queue_declare(queue=SENSOR_QUEUE, durable=True)
            ch.basic_publish(exchange="", routing_key=SENSOR_QUEUE, body=b"not-json")
        for temperature, humidity in [(35, 40), (36, 40), (35, 39), (36, 39)]:
            self.run_script("sensor_producer_bai2.py", "--count", "1",
                            "--temperature", str(temperature), "--humidity", str(humidity))
        result = subprocess.run(
            [sys.executable, str(LAB / "monitor_consumer_bai2.py"), "--max-messages", "4"],
            env=self.env, capture_output=True, text=True, timeout=20,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Bo qua message khong hop le", result.stderr)
        self.assertEqual(result.stdout.count("Device: sensor01"), 4)
        self.assertEqual(result.stdout.count("CANH BAO: Nhiet do cao"), 2)
        self.assertEqual(result.stdout.count("CANH BAO: Do am thap"), 2)
        with pika.BlockingConnection(pika.URLParameters(self.url)) as conn:
            count = conn.channel().queue_declare(queue=SENSOR_QUEUE, passive=True).method.message_count
            self.assertEqual(count, 0)

    def test_04_direct_and_topic_exchange_routing(self):
        output = self.run_script("alert_producer_bai3.py")
        for level in ("info", "warning", "critical"):
            self.assertIn(f"Da gui [{level}]", output)
        with pika.BlockingConnection(pika.URLParameters(self.url)) as conn:
            ch = conn.channel()
            for queue in ALERT_QUEUES.values():
                self.assertEqual(ch.queue_declare(queue=queue, passive=True).method.message_count, 1)
        warning = self.run_script("warning_consumer_bai3.py", "--max-messages", "1")
        critical = self.run_script("critical_consumer_bai3.py", "--max-messages", "1")
        self.assertIn("[warning_queue] Da nhan: Nhiet do phong may vuot nguong warning", warning)
        self.assertNotIn("critical", warning)
        self.assertIn("[critical_queue] Da nhan: Cam bien kho lanh mat ket noi critical", critical)
        self.assertNotIn("warning", critical)
        self.run_script("alert_producer_bai3.py", "--level", "info")
        with pika.BlockingConnection(pika.URLParameters(self.url)) as conn:
            ch = conn.channel()
            for queue in ALERT_QUEUES.values():
                self.assertEqual(ch.queue_declare(queue=queue, passive=True).method.message_count, 0)

        with tempfile.TemporaryDirectory() as directory:
            log_file = Path(directory) / "alerts.jsonl"
            all_alerts = self.run_script("all_alerts_consumer_bai3.py", "--max-messages", "4",
                                         "--log-file", str(log_file))
            records = [json.loads(line) for line in log_file.read_text().splitlines()]
            self.assertEqual([row["routing_key"] for row in records],
                             ["info", "warning", "critical", "info"])
            self.assertEqual(all_alerts.count("Da nhan:"), 4)
            with pika.BlockingConnection(pika.URLParameters(self.url)) as conn:
                self.assertEqual(conn.channel().queue_declare(
                    queue=ALL_ALERTS_QUEUE, passive=True).method.message_count, 0)

    @unittest.skipIf(os.name == "nt", "Kiểm thử SIGINT này dành cho macOS/Linux")
    def test_05_ctrl_c_shutdown(self):
        proc = subprocess.Popen(
            [sys.executable, str(LAB / "consumer_bai1.py")],
            env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            # Đợi đăng ký consumer tại broker, không dùng readline có thể chờ vô hạn.
            deadline = time.monotonic() + 10
            with pika.BlockingConnection(pika.URLParameters(self.url)) as conn:
                ch = conn.channel()
                while ch.queue_declare(queue=MESSAGE_QUEUE, passive=True).method.consumer_count == 0:
                    if time.monotonic() >= deadline or proc.poll() is not None:
                        self.fail("Consumer chưa sẵn sàng")
                    time.sleep(0.05)
            proc.send_signal(signal.SIGINT)
            out, err = proc.communicate(timeout=10)
            self.assertEqual(proc.returncode, 0, err)
            self.assertIn("Da dung chuong trinh", out)
            self.assertNotIn("Traceback", err)
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.communicate()

    def test_06_unavailable_broker(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))  # Giữ cổng nhưng không listen.
            url = f"amqp://lab:lab@127.0.0.1:{sock.getsockname()[1]}/%2F"
            result = subprocess.run(
                [sys.executable, str(LAB / "consumer_bai1.py")],
                env=dict(self.env, AMQP_URL=url), capture_output=True, text=True, timeout=15,
            )
        self.assertEqual(result.returncode, 1)
        self.assertIn("Loi AMQP", result.stderr)
        self.assertNotIn("Traceback", result.stderr)


    def test_07_two_and_three_sensor_streams(self):
        for devices in (("sensor01", "sensor02"), ("sensor01", "sensor02", "sensor03")):
            with self.subTest(devices=devices):
                output = self.run_script("sensor_producer_bai2.py", "--device-ids", *devices,
                                         "--count", "2")
                readings = [json.loads(line.removeprefix("Da gui: "))
                            for line in output.splitlines() if line.startswith("Da gui: ")]
                self.assertEqual([row["device_id"] for row in readings], list(devices) * 2)
                for device in devices:
                    stamps = [datetime.fromisoformat(row["sent_at"]) for row in readings
                              if row["device_id"] == device]
                    self.assertGreaterEqual((stamps[1] - stamps[0]).total_seconds(), 2.9)
                monitor = self.run_script("monitor_consumer_bai2.py", "--max-messages",
                                          str(2 * len(devices)))
                for device in devices:
                    self.assertEqual(monitor.count(f"Device: {device}\n"), 2)
                self.assertEqual(monitor.count("Sent at:"), 2 * len(devices))

    def test_08_topic_wildcard_and_append_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            log_file = Path(directory) / "alerts.jsonl"
            for key in ("sensor.room.warning", "info"):
                with pika.BlockingConnection(pika.URLParameters(self.url)) as conn:
                    ch = conn.channel()
                    declare_all_alerts_queue(ch)
                    ch.confirm_delivery()
                    ch.basic_publish(exchange=TOPIC_ALERT_EXCHANGE, routing_key=key,
                                     body='Cảnh báo nhiều dòng\nDòng hai'.encode(), mandatory=True)
                self.run_script("all_alerts_consumer_bai3.py", "--max-messages", "1",
                                "--log-file", str(log_file))
            records = [json.loads(line) for line in log_file.read_text(encoding="utf-8").splitlines()]
            self.assertEqual([row["routing_key"] for row in records], ["sensor.room.warning", "info"])
            self.assertTrue(all(row["message"] == 'Cảnh báo nhiều dòng\nDòng hai' for row in records))

    def test_09_log_failure_redelivers_alert(self):
        with pika.BlockingConnection(pika.URLParameters(self.url)) as conn:
            ch = conn.channel()
            declare_all_alerts_queue(ch)
            ch.confirm_delivery()
            ch.basic_publish(exchange=TOPIC_ALERT_EXCHANGE, routing_key="warning",
                             body=b"Retry after log failure", mandatory=True)
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                [sys.executable, str(LAB / "all_alerts_consumer_bai3.py"),
                 "--max-messages", "1", "--log-file", directory],
                env=self.env, capture_output=True, text=True, timeout=20,
            )
            self.assertEqual(result.returncode, 1)
            self.assertNotIn("Traceback", result.stderr)
            log_file = Path(directory) / "recovered.jsonl"
            self.run_script("all_alerts_consumer_bai3.py", "--max-messages", "1",
                            "--log-file", str(log_file))
            self.assertEqual(json.loads(log_file.read_text())["message"], "Retry after log failure")


if __name__ == "__main__":
    unittest.main(verbosity=2)
