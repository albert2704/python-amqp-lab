"""Kiểm thử logic và CLI, không giả lập định tuyến của RabbitMQ."""

from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
import io
import json
import os
from pathlib import Path
import random
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import amqp_common as common
from consumer_bai1 import show_message
from critical_consumer_bai3 import show_critical
from monitor_consumer_bai2 import show_reading
from sensor_producer_bai2 import make_reading
from warning_consumer_bai3 import show_warning


def encoded_reading(**updates):
    data = {"device_id": "sensor01", "temperature": 29.5, "humidity": 62.1}
    data.update(updates)
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


class SensorTests(unittest.TestCase):
    def test_thresholds_are_strict_and_independent(self):
        for temperature, humidity, expected in (
            (35, 40, []),
            (35.1, 40, ["CANH BAO: Nhiet do cao"]),
            (35, 39.9, ["CANH BAO: Do am thap"]),
            (36, 39, ["CANH BAO: Nhiet do cao", "CANH BAO: Do am thap"]),
        ):
            with self.subTest(temperature=temperature, humidity=humidity):
                self.assertEqual(common.alerts(temperature, humidity), expected)

    def test_valid_sensor_values_preserve_unicode_and_numeric_boundaries(self):
        for humidity in (0, 100):
            with self.subTest(humidity=humidity):
                data = common.sensor_values(encoded_reading(
                    device_id="cảm biến 01", temperature=-10, humidity=humidity))
                self.assertEqual(data, {
                    "device_id": "cảm biến 01", "temperature": -10, "humidity": humidity,
                })

    def test_bad_sensor_payloads_are_rejected(self):
        invalid = [b"not JSON", b"\xff", b"[]", b"null", b"{}"]
        invalid.extend(encoded_reading(**update) for update in (
            {"device_id": " "}, {"device_id": 1},
            {"temperature": None}, {"temperature": True}, {"temperature": "35"},
            {"temperature": float("nan")}, {"temperature": float("inf")},
            {"temperature": 10 ** 400}, {"humidity": False},
            {"humidity": float("-inf")}, {"humidity": -0.1}, {"humidity": 100.1},
        ))
        for payload in invalid:
            with self.subTest(payload=payload[:100]):
                with self.assertRaises(ValueError):
                    common.sensor_values(payload)

    def test_random_readings_have_required_fields_and_documented_bounds(self):
        # Riêng nguồn ngẫu nhiên được cố định; vẫn chạy hàm tạo dữ liệu thật.
        rng = random.Random(1234)
        with patch("sensor_producer_bai2.random.uniform", side_effect=rng.uniform):
            samples = [make_reading() for _ in range(30)]
        for data in samples:
            self.assertEqual(set(data), {"device_id", "temperature", "humidity"})
            self.assertEqual(data["device_id"], "sensor01")
            self.assertTrue(20 <= data["temperature"] <= 40)
            self.assertTrue(30 <= data["humidity"] <= 80)
            self.assertEqual(common.sensor_values(json.dumps(data).encode()), data)
        self.assertGreater(len({sample["temperature"] for sample in samples}), 1)
        self.assertGreater(len({sample["humidity"] for sample in samples}), 1)

    def test_fixed_values_include_zero_without_becoming_random(self):
        self.assertEqual(make_reading("sensor02", 0, 0), {
            "device_id": "sensor02", "temperature": 0, "humidity": 0,
        })


class OutputTests(unittest.TestCase):
    def test_greeting_preserves_unicode_and_prints_receipt_time(self):
        greeting = "Xin chao tu ung dung Python AMQP - B21001 - Nguyễn Văn An"
        output = io.StringIO()
        with patch("consumer_bai1.datetime") as clock, redirect_stdout(output):
            clock.now.return_value = datetime(2026, 10, 10, 14, 5, 21)
            show_message(greeting.encode("utf-8"))
        self.assertEqual(output.getvalue().splitlines(), [
            f"Da nhan message: {greeting}", "Thoi gian nhan: 14:05:21",
        ])

    def test_monitor_prints_reading_and_both_alerts(self):
        output = io.StringIO()
        with redirect_stdout(output):
            show_reading(encoded_reading(temperature=36.4, humidity=38.9))
        self.assertEqual(output.getvalue().splitlines(), [
            "Device: sensor01", "Temperature: 36.4", "Humidity: 38.9",
            "CANH BAO: Nhiet do cao", "CANH BAO: Do am thap",
        ])

    def test_alert_output_identifies_queue_and_preserves_unicode(self):
        for handler, queue in ((show_warning, "warning_queue"),
                               (show_critical, "critical_queue")):
            with self.subTest(queue=queue):
                output = io.StringIO()
                with redirect_stdout(output):
                    handler("Cảnh báo thử nghiệm".encode("utf-8"))
                self.assertEqual(output.getvalue().strip(),
                                 f"[{queue}] Da nhan: Cảnh báo thử nghiệm")


class DeliveryTests(unittest.TestCase):
    def test_bad_delivery_is_rejected_then_valid_deliveries_are_acked(self):
        # Gọi callback thực tại biên Pika; không mô phỏng exchange hay broker.
        channel = Mock()
        output, errors = io.StringIO(), io.StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            common.consume(channel, common.SENSOR_QUEUE, show_reading, max_messages=2)
            callback = channel.basic_consume.call_args.kwargs["on_message_callback"]
            self.assertFalse(channel.basic_consume.call_args.kwargs["auto_ack"])
            callback(channel, SimpleNamespace(delivery_tag=1), None, b"invalid")
            channel.basic_reject.assert_called_once_with(delivery_tag=1, requeue=False)
            channel.basic_ack.assert_not_called()
            callback(channel, SimpleNamespace(delivery_tag=2), None, encoded_reading())
            channel.basic_ack.assert_called_once_with(delivery_tag=2)
            channel.stop_consuming.assert_not_called()
            callback(channel, SimpleNamespace(delivery_tag=3), None, encoded_reading())
        self.assertEqual(channel.basic_ack.call_count, 2)
        channel.basic_ack.assert_called_with(delivery_tag=3)
        channel.stop_consuming.assert_called_once()
        self.assertIn("Bo qua message khong hop le", errors.getvalue())
        self.assertEqual(output.getvalue().count("Device: sensor01"), 2)

    def test_unexpected_processing_error_does_not_ack_or_discard_message(self):
        channel = Mock()
        handler = Mock(side_effect=RuntimeError("processing failed"))
        with redirect_stdout(io.StringIO()):
            common.consume(channel, common.MESSAGE_QUEUE, handler)
        callback = channel.basic_consume.call_args.kwargs["on_message_callback"]
        with self.assertRaisesRegex(RuntimeError, "processing failed"):
            callback(channel, SimpleNamespace(delivery_tag=4), None, b"message")
        channel.basic_ack.assert_not_called()
        channel.basic_reject.assert_not_called()

    def test_published_payload_uses_utf8_and_json_content_type(self):
        channel = Mock()
        body = '{"device_id": "cảm biến 01", "temperature": 30, "humidity": 50}'
        common.publish(channel, common.SENSOR_QUEUE, body, json_payload=True)
        sent = channel.basic_publish.call_args.kwargs
        self.assertEqual(sent["body"].decode("utf-8"), body)
        self.assertEqual(sent["routing_key"], common.SENSOR_QUEUE)
        self.assertEqual(sent["properties"].content_encoding, "utf-8")
        self.assertEqual(sent["properties"].content_type, "application/json")


class CLITests(unittest.TestCase):
    def launch(self, script, *args):
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("AMQP_", "STUDENT_"))}
        # Nếu CLI vô tình kết nối, URL lỗi khiến tiến trình dừng trước mạng.
        env["AMQP_URL"] = "invalid-url"
        return subprocess.run([sys.executable, str(ROOT / script), *args],
                              cwd=ROOT, env=env, stdin=subprocess.DEVNULL,
                              capture_output=True, text=True, timeout=10)

    def test_all_seven_scripts_expose_help_without_a_broker(self):
        scripts = (
            "producer_bai1.py", "consumer_bai1.py", "sensor_producer_bai2.py",
            "monitor_consumer_bai2.py", "alert_producer_bai3.py",
            "warning_consumer_bai3.py", "critical_consumer_bai3.py",
        )
        for script in scripts:
            with self.subTest(script=script):
                result = self.launch(script, "--help")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("AMQP_URL", result.stdout)

    def test_invalid_cli_values_fail_before_connecting(self):
        for script, args in (
            ("producer_bai1.py", ["--count", "0"]),
            ("producer_bai1.py", ["--name", " "]),
            ("consumer_bai1.py", ["--max-messages", "-1"]),
            ("sensor_producer_bai2.py", ["--interval", "0"]),
            ("sensor_producer_bai2.py", ["--temperature", "nan"]),
            ("sensor_producer_bai2.py", ["--humidity", "101"]),
            ("alert_producer_bai3.py", ["--level", "unknown"]),
        ):
            with self.subTest(script=script, args=args):
                result = self.launch(script, *args)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn("error:", result.stderr)
                self.assertNotIn("Traceback", result.stderr)

    def test_invalid_broker_url_does_not_leak_credentials(self):
        output = io.StringIO()
        url = "amqp://student:secret-to-hide@localhost:not-a-port/%2F"
        def connect():
            with common.connection():
                pass
        with patch.dict(os.environ, {"AMQP_URL": url}), redirect_stderr(output):
            with self.assertRaises(SystemExit) as exit_info:
                common.run(connect)
        self.assertEqual(exit_info.exception.code, 1)
        self.assertIn("AMQP_URL", output.getvalue())
        self.assertNotIn("secret-to-hide", output.getvalue())


if __name__ == "__main__":
    unittest.main()
