import asyncio
import json


def _matches_pattern(pattern: str, stream: str) -> bool:
    if pattern == stream:
        return True
    if pattern.endswith("*"):
        return stream.startswith(pattern[:-1])
    return False


class SaturnDBClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 7379, token: str = ""):
        self._host     = host
        self._port     = port
        self._token    = token
        self._reader   = None
        self._writer   = None
        self._handlers: dict[str, callable] = {}
        self._closing  = False
        self._reconnect_delay = 0.5
        self._listen_task     = None

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(self._host, self._port)
        self._writer.transport.set_write_buffer_limits(0)
        self._reconnect_delay = 0.5
        if self._token:
            await self._send(f"AUTH {self._token}")
        self._listen_task = asyncio.create_task(self._listen())

    async def emit(self, stream: str, payload: dict) -> None:
        await self._send(f"EMIT {stream} {json.dumps(payload)}")

    async def get(self, stream: str) -> dict | None:
        response = await self._send(f"GET {stream}")
        if response.startswith("VALUE "):
            return json.loads(response[6:])
        return None

    async def since(self, stream: str, ts: int) -> list[dict]:
        response = await self._send(f"SINCE {stream} {ts}")
        if response == "EMPTY":
            return []
        events = []
        for line in response.splitlines():
            if line.startswith("EVENT "):
                rest     = line[6:]
                space_at = rest.index(" ")
                events.append({
                    "stream":  rest[:space_at],
                    "payload": json.loads(rest[space_at + 1:]),
                })
        return events

    async def watch(self, pattern: str, handler: callable) -> None:
        self._handlers[pattern] = handler
        await self._send(f"WATCH {pattern}")

    async def ping(self) -> str:
        return await self._send("PING")

    async def disconnect(self) -> None:
        self._closing = True
        if self._listen_task:
            self._listen_task.cancel()
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()

    async def _send(self, line: str) -> str:
        self._writer.write(f"{line}\n".encode())
        await self._writer.drain()
        return await self._read_response()

    async def _read_response(self) -> str:
        while True:
            line = (await self._reader.readline()).decode().strip()
            if not line:
                continue
            if line.startswith("EVENT "):
                self._dispatch(line)
                continue
            return line

    def _dispatch(self, line: str) -> None:
        rest     = line[6:]
        space_at = rest.index(" ")
        stream   = rest[:space_at]
        payload  = json.loads(rest[space_at + 1:])
        for pattern, handler in self._handlers.items():
            if _matches_pattern(pattern, stream):
                handler(stream, payload)

    async def _listen(self) -> None:
        try:
            while True:
                line = (await self._reader.readline()).decode().strip()
                if not line:
                    continue
                if line.startswith("EVENT "):
                    self._dispatch(line)
        except (asyncio.CancelledError, ConnectionResetError, EOFError):
            if not self._closing:
                await self._reconnect()

    async def _reconnect(self) -> None:
        print(f"[saturn] disconnected — reconnecting in {self._reconnect_delay}s")
        await asyncio.sleep(self._reconnect_delay)
        self._reconnect_delay = min(self._reconnect_delay * 2, 10)
        try:
            await self.connect()
            for pattern in self._handlers:
                await self._send(f"WATCH {pattern}")
            print("[saturn] reconnected")
        except Exception:
            await self._reconnect()
