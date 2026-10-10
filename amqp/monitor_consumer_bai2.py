"""Bài 2: đọc dữ liệu cảm biến và cảnh báo theo ngưỡng của đề bài."""

from amqp_common import (SENSOR_QUEUE, alerts, connection, consume, parser,
                         run, sensor_values)


def show_reading(body):
    data = sensor_values(body)
    print(f"Device: {data['device_id']}", flush=True)
    print(f"Temperature: {data['temperature']}", flush=True)
    print(f"Humidity: {data['humidity']}", flush=True)
    for warning in alerts(data["temperature"], data["humidity"]):
        print(warning, flush=True)


def main():
    args = parser(__doc__, consumer=True).parse_args()
    with connection() as conn:
        channel = conn.channel()
        channel.queue_declare(queue=SENSOR_QUEUE, durable=True)
        consume(channel, SENSOR_QUEUE, show_reading, args.max_messages)


if __name__ == "__main__":
    run(main)
