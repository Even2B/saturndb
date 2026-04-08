import asyncio
from saturn import SaturnDBClient


def on_event(stream: str, payload: dict) -> None:
    print(f"[watch] {stream} → {payload}")


async def main() -> None:
    client = SaturnDBClient("127.0.0.1", 7379, token="saturn-admin-secret")
    await client.connect()
    print("connected to saturn")

    print("ping →", await client.ping())

    await client.watch("users:*", on_event)

    await client.emit("users:1", {"name": "evan", "status": "online"})
    await client.emit("users:2", {"name": "john", "status": "away"})
    await client.emit("orders:1", {"total": 99})

    val = await client.get("users:1")
    print("get users:1 →", val)

    await asyncio.sleep(0.2)
    await client.disconnect()


asyncio.run(main())
