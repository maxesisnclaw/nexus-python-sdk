from __future__ import annotations

import socket
import struct
import threading
from typing import Any

import msgpack

from nexus_sdk.registry import RegistryClient


def test_registry_register_lookup_with_mock_server(mock_registry: Any, registry_socket_path: str) -> None:
    client = RegistryClient(registry_socket_path)
    client.register(
        name="echo",
        id="echo-1",
        endpoints=[{"type": "uds", "addr": "/tmp/echo.sock"}],
        capabilities=["rpc"],
    )

    instances = client.lookup("echo")
    assert len(instances) == 1
    assert instances[0]["id"] == "echo-1"
    assert instances[0]["endpoints"][0]["type"] == "uds"

    client.unregister("echo-1")
    assert client.lookup("echo") == []
    client.close()


def test_registry_watch_receives_events(mock_registry: Any, registry_socket_path: str) -> None:
    client = RegistryClient(registry_socket_path)
    got: list[dict[str, Any]] = []
    done = threading.Event()

    def on_event(event: dict[str, Any]) -> None:
        got.append(event)
        done.set()

    unsubscribe = client.watch("echo", on_event)
    client.register(
        name="echo",
        id="echo-watch-1",
        endpoints=[{"type": "uds", "addr": "/tmp/echo-watch.sock"}],
        capabilities=[],
    )

    assert done.wait(1.0)
    assert got[0]["event"] == "up"
    assert got[0]["instance"]["id"] == "echo-watch-1"

    unsubscribe()
    client.close()


class _FakeSocket:
    def __init__(self):
        self.connected_to = ""
        self.timeout: float | None = None
        self.closed = False
        self._inbound = bytearray()
        self._outbound = bytearray()
        self.requests: list[dict[str, Any]] = []

    def connect(self, path: str) -> None:
        self.connected_to = path

    def settimeout(self, timeout: float) -> None:
        self.timeout = timeout

    def sendall(self, data: bytes) -> None:
        self._inbound.extend(data)
        self._drain_frames()

    def recv_into(self, buf: memoryview) -> int:
        if not self._outbound:
            return 0
        n = min(len(buf), len(self._outbound))
        buf[:n] = self._outbound[:n]
        del self._outbound[:n]
        return n

    def close(self) -> None:
        self.closed = True

    def _drain_frames(self) -> None:
        while True:
            if len(self._inbound) < 4:
                return
            size = struct.unpack(">I", self._inbound[:4])[0]
            if len(self._inbound) < 4 + size:
                return
            body = bytes(self._inbound[4 : 4 + size])
            del self._inbound[: 4 + size]

            req = msgpack.unpackb(body, raw=False)
            self.requests.append(req)
            cmd = req.get("cmd")
            if cmd == "register":
                resp = {"ok": True}
            elif cmd == "lookup":
                resp = {"instances": [{"id": "svc-1"}]}
            else:
                resp = {"ok": True}

            payload = msgpack.packb(resp, use_bin_type=True)
            self._outbound.extend(struct.pack(">I", len(payload)))
            self._outbound.extend(payload)


def test_registry_client_mocks_socket_connection(monkeypatch: Any) -> None:
    created: list[_FakeSocket] = []

    def fake_socket(_family: int, _type: int) -> _FakeSocket:
        sock = _FakeSocket()
        created.append(sock)
        return sock

    monkeypatch.setattr(socket, "socket", fake_socket)

    client = RegistryClient("/tmp/mock-registry.sock")
    client.register(
        name="svc",
        id="svc-1",
        endpoints=[{"type": "uds", "addr": "/tmp/svc.sock"}],
        capabilities=["cap-a"],
    )
    instances = client.lookup("svc")

    assert len(created) == 2
    assert created[0].requests[0]["cmd"] == "register"
    assert created[1].requests[0]["cmd"] == "lookup"
    assert instances[0]["id"] == "svc-1"
