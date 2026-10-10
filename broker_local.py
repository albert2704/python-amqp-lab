"""Broker MQTT cục bộ tùy chọn, chỉ lắng nghe trên 127.0.0.1."""

import argparse
import asyncio
import logging

from amqtt.broker import Broker


async def serve(port):
    broker = Broker({
        "listeners": {"default": {"type": "tcp", "bind": f"127.0.0.1:{port}"}},
        "plugins": {
            "amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True},
        },
    })
    await broker.start()
    print(f"Broker sẵn sàng: 127.0.0.1:{port}. Nhấn Ctrl+C để dừng.", flush=True)
    try:
        await asyncio.Event().wait()
    finally:
        await broker.shutdown()


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--port", type=int, default=1883)
    args = cli.parse_args()
    if not 1 <= args.port <= 65535:
        cli.error("Port phải trong khoảng 1..65535")
    logging.basicConfig(level=logging.ERROR)
    try:
        asyncio.run(serve(args.port))
    except KeyboardInterrupt:
        print("\nĐã dừng broker.")


if __name__ == "__main__":
    main()
