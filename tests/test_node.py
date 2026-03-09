from __future__ import annotations

import socket
import time
from typing import Any

import pytest

import nexus_sdk.node as node_module
from nexus_sdk.node import Node, Request, Response
from nexus_sdk.registry import RegistryClient
from nexus_sdk.transport import recv_rpc_message, send_rpc_message


def _wait_for_service(registry_addr: str, name: str, timeout: float = 2.0) -> None:
    client = RegistryClient(registry_addr)
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            if client.lookup(name):
                return
            time.sleep(0.02)
    finally:
        client.close()
    raise TimeoutError(f"service {name} not registered in time")


def test_node_handle_serve_call_local_uds_roundtrip(
    mock_registry: object,
    registry_socket_path: str,
    socket_path_factory: Any,
) -> None:
    server = Node(name="echo", id="echo-1", uds_addr=socket_path_factory("echo.sock"), registry_addr=registry_socket_path)
    server.handle("echo", lambda req: Response(payload=req.payload))
    server.serve_async()

    _wait_for_service(registry_socket_path, "echo")

    client = Node(name="caller", id="caller-1", registry_addr=registry_socket_path)
    client._local_node_id = "test-node"
    try:
        resp = client.call("echo", "echo", b"hello")
        assert resp.payload == b"hello"
    finally:
        client.close()
        server.close()


def test_node_server_accepts_streamed_msgpack_rpc(
    mock_registry: object,
    registry_socket_path: str,
    socket_path_factory: Any,
) -> None:
    uds_path = socket_path_factory("stream.sock")
    server = Node(name="stream", id="stream-1", uds_addr=uds_path, registry_addr=registry_socket_path)
    server.handle("echo", lambda req: Response(payload=req.payload))
    server.serve_async()

    _wait_for_service(registry_socket_path, "stream")

    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    conn.settimeout(2.0)
    conn.connect(uds_path)
    try:
        send_rpc_message(conn, {"method": "echo", "payload": b"hello", "headers": {}})
        resp = recv_rpc_message(conn)
        assert resp["payload"] == b"hello"
    finally:
        conn.close()
        server.close()


def test_node_context_manager(mock_registry: object, registry_socket_path: str, socket_path_factory: Any) -> None:
    with Node(name="ctx", id="ctx-1", uds_addr=socket_path_factory("ctx.sock"), registry_addr=registry_socket_path) as server:
        server.handle("ping", lambda req: Response(payload=b"pong"))
        server.serve_async()
        _wait_for_service(registry_socket_path, "ctx")

        with Node(name="caller", id="caller-ctx", registry_addr=registry_socket_path) as client:
            client._local_node_id = "test-node"
            resp = client.call("ctx", "ping", b"")
            assert resp.payload == b"pong"

    reg = RegistryClient(registry_socket_path)
    try:
        assert reg.lookup("ctx") == []
    finally:
        reg.close()


def test_node_multiple_handlers(
    mock_registry: object,
    registry_socket_path: str,
    socket_path_factory: Any,
) -> None:
    server = Node(name="multi", id="multi-1", uds_addr=socket_path_factory("multi.sock"), registry_addr=registry_socket_path)

    def plus_one(req: Request) -> Response:
        return Response(payload=str(int(req.payload.decode()) + 1).encode())

    def plus_two(req: Request) -> Response:
        return Response(payload=str(int(req.payload.decode()) + 2).encode())

    server.handle("one", plus_one)
    server.handle("two", plus_two)
    server.serve_async()
    _wait_for_service(registry_socket_path, "multi")

    client = Node(name="caller", id="caller-multi", registry_addr=registry_socket_path)
    client._local_node_id = "test-node"
    try:
        assert client.call("multi", "one", b"40").payload == b"41"
        assert client.call("multi", "two", b"40").payload == b"42"
    finally:
        client.close()
        server.close()


def test_call_to_nonexistent_handler_returns_error(
    mock_registry: object,
    registry_socket_path: str,
    socket_path_factory: Any,
) -> None:
    server = Node(
        name="errors",
        id="errors-1",
        uds_addr=socket_path_factory("errors.sock"),
        registry_addr=registry_socket_path,
    )
    server.handle("ok", lambda req: Response(payload=b"ok"))
    server.serve_async()
    _wait_for_service(registry_socket_path, "errors")

    client = Node(name="caller", id="caller-errors", registry_addr=registry_socket_path)
    client._local_node_id = "test-node"
    try:
        with pytest.raises(RuntimeError, match="handler not found"):
            client.call("errors", "missing", b"x")
    finally:
        client.close()
        server.close()


def test_start_serving_rolls_back_registry_registration_on_partial_startup(
    monkeypatch: Any,
    registry_socket_path: str,
    socket_path_factory: Any,
) -> None:
    class _FakeRegistry:
        def __init__(self) -> None:
            self.register_calls = 0
            self.unregister_ids: list[str] = []

        def register(
            self,
            _name: str,
            _id: str,
            *,
            endpoints: list[dict[str, str]],
            capabilities: list[str],
            ttl_ms: int,
        ) -> None:
            assert endpoints
            assert isinstance(capabilities, list)
            assert ttl_ms > 0
            self.register_calls += 1

        def unregister(self, service_id: str) -> None:
            self.unregister_ids.append(service_id)

        def close(self) -> None:
            return

    node = Node(
        name="rollback",
        id="rollback-1",
        uds_addr=socket_path_factory("rollback.sock"),
        registry_addr=registry_socket_path,
    )
    fake_registry = _FakeRegistry()
    node._registry = fake_registry  # type: ignore[assignment]

    def _fail_thread_start(self: object) -> None:
        raise RuntimeError("forced thread start failure")

    monkeypatch.setattr(node_module.threading.Thread, "start", _fail_thread_start)

    try:
        with pytest.raises(RuntimeError, match="forced thread start failure"):
            node._start_serving()
        assert fake_registry.register_calls == 1
        assert fake_registry.unregister_ids == ["rollback-1"]
        assert node._registered is False
        with node._state_lock:
            assert node._state == "new"
    finally:
        node.close()


def test_node_rejects_non_loopback_tcp_without_override(registry_socket_path: str) -> None:
    with pytest.raises(ValueError, match="Refusing non-loopback TCP listen address"):
        Node(name="tcp", id="tcp-1", tcp_addr="0.0.0.0:0", registry_addr=registry_socket_path)


def test_node_rejects_empty_host_tcp_without_override(registry_socket_path: str) -> None:
    with pytest.raises(ValueError, match="Refusing non-loopback TCP listen address"):
        Node(name="tcp", id="tcp-1-empty", tcp_addr=":0", registry_addr=registry_socket_path)


def test_node_tcp_listener_enforces_loopback(registry_socket_path: str) -> None:
    node = Node(name="tcp", id="tcp-2", tcp_addr="127.0.0.1:0", registry_addr=registry_socket_path)
    listener = node._start_tcp_listener("127.0.0.1:0")
    listener.close()

    with pytest.raises(ValueError, match="Refusing non-loopback TCP listen address"):
        node._start_tcp_listener("0.0.0.0:0")


def test_loopback_detection_uses_ip_semantics() -> None:
    assert Node._is_loopback_tcp_addr("127.0.0.2:9000")
    assert Node._is_loopback_tcp_addr("[::1]:9000")
    assert not Node._is_loopback_tcp_addr("128.0.0.1:9000")


def test_call_missing_service_cleans_round_robin_offset(registry_socket_path: str) -> None:
    class _MissingRegistry:
        def lookup(self, _: str) -> list[dict[str, object]]:
            return []

        def unregister(self, _: str) -> None:
            return

        def close(self) -> None:
            return

    node = Node(name="caller", id="caller-missing", registry_addr=registry_socket_path)
    node._registry = _MissingRegistry()  # type: ignore[assignment]
    node._rr_offsets["missing"] = 3
    try:
        with pytest.raises(RuntimeError, match="service 'missing' not found"):
            node.call("missing", "noop", b"")
        assert "missing" not in node._rr_offsets
    finally:
        node.close()


def test_call_rejects_non_loopback_tcp_without_override(registry_socket_path: str) -> None:
    class _RemoteRegistry:
        def lookup(self, _: str) -> list[dict[str, object]]:
            return [
                {
                    "id": "echo-remote-1",
                    "node": "remote-node",
                    "endpoints": [{"type": "tcp", "addr": "10.20.30.40:9000"}],
                }
            ]

        def unregister(self, _: str) -> None:
            return

        def close(self) -> None:
            return

    node = Node(name="caller", id="caller-tcp-block", registry_addr=registry_socket_path)
    node._registry = _RemoteRegistry()  # type: ignore[assignment]
    try:
        with pytest.raises(
            ConnectionError,
            match=(
                "Refusing non-loopback TCP call to 10.20.30.40:9000 without encryption. "
                "Set allow_insecure_tcp=True to override."
            ),
        ):
            node.call("echo", "echo", b"hello")
    finally:
        node.close()


def test_call_allows_non_loopback_tcp_with_override(monkeypatch: Any, registry_socket_path: str) -> None:
    class _RemoteRegistry:
        def lookup(self, _: str) -> list[dict[str, object]]:
            return [
                {
                    "id": "echo-remote-2",
                    "node": "remote-node",
                    "endpoints": [{"type": "tcp", "addr": "10.20.30.41:9001"}],
                }
            ]

        def unregister(self, _: str) -> None:
            return

        def close(self) -> None:
            return

    class _FakeConn:
        def settimeout(self, _timeout: float) -> None:
            return

        def close(self) -> None:
            return

    class _FakePool:
        def __init__(self) -> None:
            self.got_addr = ""
            self.got_use_tcp = False

        def get(self, addr: str, *, use_tcp: bool, timeout: float | None = None) -> _FakeConn:
            self.got_addr = addr
            self.got_use_tcp = use_tcp
            assert timeout is not None
            return _FakeConn()

        def put(self, _addr: str, _conn: _FakeConn, *, use_tcp: bool) -> None:
            assert use_tcp

        def close_all(self) -> None:
            return

    node = Node(
        name="caller",
        id="caller-tcp-allow",
        allow_insecure_tcp=True,
        registry_addr=registry_socket_path,
    )
    pool = _FakePool()
    node._registry = _RemoteRegistry()  # type: ignore[assignment]
    node._pool = pool  # type: ignore[assignment]
    monkeypatch.setattr(node_module, "send_message", lambda _conn, _msg: None)
    monkeypatch.setattr(node_module, "recv_message", lambda _conn: {"payload": b"ok", "headers": {}})
    try:
        resp = node.call("echo", "echo", b"hello")
        assert resp.payload == b"ok"
        assert pool.got_use_tcp is True
        assert pool.got_addr == "10.20.30.41:9001"
    finally:
        node.close()


def test_call_timeout_retries_and_raises_descriptive_error(monkeypatch: Any, registry_socket_path: str) -> None:
    class _RemoteRegistry:
        def lookup(self, _: str) -> list[dict[str, object]]:
            return [{"id": "echo-timeout-1", "node": "remote-node", "endpoints": [{"type": "tcp", "addr": "127.0.0.1:9010"}]}]

        def unregister(self, _: str) -> None:
            return

        def close(self) -> None:
            return

    class _FakeConn:
        def __init__(self) -> None:
            self.timeout_values: list[float] = []
            self.closed = False

        def settimeout(self, timeout: float) -> None:
            self.timeout_values.append(timeout)

        def close(self) -> None:
            self.closed = True

    class _RetryPool:
        def __init__(self) -> None:
            self.get_calls = 0
            self.put_calls = 0
            self.conns: list[_FakeConn] = []

        def get(self, _addr: str, *, use_tcp: bool, timeout: float | None = None) -> _FakeConn:
            assert use_tcp
            assert timeout is not None
            self.get_calls += 1
            conn = _FakeConn()
            conn.settimeout(timeout)
            self.conns.append(conn)
            return conn

        def put(self, _addr: str, _conn: _FakeConn, *, use_tcp: bool) -> None:
            assert use_tcp
            self.put_calls += 1

        def close_all(self) -> None:
            return

    node = Node(
        name="caller",
        id="caller-timeout",
        registry_addr=registry_socket_path,
        call_timeout=0.25,
        call_retries=2,
    )
    pool = _RetryPool()
    node._registry = _RemoteRegistry()  # type: ignore[assignment]
    node._pool = pool  # type: ignore[assignment]

    sleep_calls: list[float] = []
    monkeypatch.setattr(node_module.time, "sleep", lambda value: sleep_calls.append(value))
    monkeypatch.setattr(node_module, "send_message", lambda _conn, _msg: None)
    monkeypatch.setattr(node_module, "recv_message", lambda _conn: (_ for _ in ()).throw(socket.timeout("timed out")))
    try:
        with pytest.raises(
            TimeoutError,
            match=r"RPC call to echo\.echo timed out after 0.25s",
        ):
            node.call("echo", "echo", b"hello")
        assert pool.get_calls == 3
        assert pool.put_calls == 0
        assert sleep_calls == [0.1, 0.2]
        assert all(conn.closed for conn in pool.conns)
        assert all(conn.timeout_values for conn in pool.conns)
    finally:
        node.close()


def test_pick_endpoint_prefers_local_uds_and_remote_tcp(registry_socket_path: str) -> None:
    node = Node(name="caller", id="caller-pick", registry_addr=registry_socket_path)
    try:
        local_instance = {
            "node": node._local_node_id,
            "endpoints": [
                {"type": "tcp", "addr": "127.0.0.1:9000"},
                {"type": "uds", "addr": "/tmp/local.sock"},
            ],
        }
        remote_instance = {
            "node": "remote-node",
            "endpoints": [
                {"type": "uds", "addr": "/tmp/remote.sock"},
                {"type": "tcp", "addr": "127.0.0.1:9001"},
            ],
        }
        unknown_instance = {
            "endpoints": [
                {"type": "uds", "addr": "/tmp/unknown.sock"},
                {"type": "tcp", "addr": "127.0.0.1:9002"},
            ],
        }
        remote_only_uds_instance = {
            "id": "remote-only-1",
            "node": "remote-node",
            "endpoints": [{"type": "uds", "addr": "/tmp/remote-only.sock"}],
        }
        unknown_only_uds_instance = {
            "id": "unknown-only-1",
            "endpoints": [{"type": "uds", "addr": "/tmp/unknown-only.sock"}],
        }

        local_addr, local_use_tcp = node._pick_endpoint(local_instance)
        remote_addr, remote_use_tcp = node._pick_endpoint(remote_instance)
        unknown_addr, unknown_use_tcp = node._pick_endpoint(unknown_instance)
        with pytest.raises(
            ConnectionError,
            match=(
                "remote instance remote-only-1 has no TCP endpoint; refusing UDS fallback for non-local target"
            ),
        ):
            node._pick_endpoint(remote_only_uds_instance)
        with pytest.raises(
            ConnectionError,
            match=(
                "instance unknown-only-1 has unknown node identity and no TCP endpoint; "
                "refusing UDS fallback for non-local target"
            ),
        ):
            node._pick_endpoint(unknown_only_uds_instance)

        assert local_addr == "/tmp/local.sock"
        assert local_use_tcp is False
        assert remote_addr == "127.0.0.1:9001"
        assert remote_use_tcp is True
        assert unknown_addr == "127.0.0.1:9002"
        assert unknown_use_tcp is True
    finally:
        node.close()


def test_call_business_error_does_not_log_warning_or_discard_connection(
    monkeypatch: Any,
    caplog: Any,
    registry_socket_path: str,
) -> None:
    class _RemoteRegistry:
        def lookup(self, _: str) -> list[dict[str, object]]:
            return [
                {
                    "id": "echo-business-1",
                    "node": "remote-node",
                    "endpoints": [{"type": "tcp", "addr": "127.0.0.1:9011"}],
                }
            ]

        def unregister(self, _: str) -> None:
            return

        def close(self) -> None:
            return

    class _FakeConn:
        def __init__(self) -> None:
            self.closed = False

        def settimeout(self, _timeout: float) -> None:
            return

        def close(self) -> None:
            self.closed = True

    class _Pool:
        def __init__(self) -> None:
            self.conn = _FakeConn()
            self.put_calls = 0

        def get(self, _addr: str, *, use_tcp: bool, timeout: float | None = None) -> _FakeConn:
            assert use_tcp
            assert timeout is not None
            return self.conn

        def put(self, _addr: str, _conn: _FakeConn, *, use_tcp: bool) -> None:
            assert use_tcp
            self.put_calls += 1

        def close_all(self) -> None:
            return

    node = Node(name="caller", id="caller-business", registry_addr=registry_socket_path)
    pool = _Pool()
    node._registry = _RemoteRegistry()  # type: ignore[assignment]
    node._pool = pool  # type: ignore[assignment]
    monkeypatch.setattr(node_module, "send_message", lambda _conn, _msg: None)
    monkeypatch.setattr(
        node_module,
        "recv_message",
        lambda _conn: {"payload": b"", "headers": {"error": "invalid request"}},
    )
    caplog.set_level("WARNING", logger="nexus_sdk")
    try:
        with pytest.raises(RuntimeError, match="invalid request"):
            node.call("echo", "echo", b"hello")
        assert pool.put_calls == 1
        assert pool.conn.closed is False
        assert not any(
            "unexpected RPC call failure" in record.getMessage() for record in caplog.records
        )
    finally:
        node.close()


def test_node_reregisters_after_registry_state_loss(
    monkeypatch: Any,
    mock_registry: Any,
    registry_socket_path: str,
    socket_path_factory: Any,
) -> None:
    monkeypatch.setattr(node_module, "_HEARTBEAT_INTERVAL_SECONDS", 0.05)
    monkeypatch.setattr(node_module, "_HEARTBEAT_FAILURES_BEFORE_RECOVERY", 1)

    node = Node(
        name="recover",
        id="recover-1",
        uds_addr=socket_path_factory("recover.sock"),
        registry_addr=registry_socket_path,
        capabilities=["fast", "safe"],
    )
    try:
        node.serve_async()
        _wait_for_service(registry_socket_path, "recover")

        with mock_registry._lock:
            mock_registry._instances.clear()

        _wait_for_service(registry_socket_path, "recover", timeout=3.0)
    finally:
        node.close()


def test_heartbeat_recovery_waits_for_failure_threshold(registry_socket_path: str) -> None:
    class _LoopStop:
        def __init__(self, iterations: int) -> None:
            self._iterations = iterations
            self._calls = 0
            self._set = False

        def wait(self, _timeout: float) -> bool:
            if self._set:
                return True
            if self._calls >= self._iterations:
                return True
            self._calls += 1
            return False

        def set(self) -> None:
            self._set = True

    class _FailingRegistry:
        def __init__(self) -> None:
            self.register_calls = 0

        def heartbeat(self, _id: str) -> None:
            raise RuntimeError("instance not found")

        def register(
            self,
            _name: str,
            _id: str,
            *,
            endpoints: list[dict[str, str]],
            capabilities: list[str],
            ttl_ms: int,
        ) -> None:
            assert endpoints
            assert isinstance(capabilities, list)
            assert ttl_ms > 0
            self.register_calls += 1
            return

        def unregister(self, _id: str) -> None:
            return

        def close(self) -> None:
            return

    node = Node(name="bounded", id="bounded-1", registry_addr=registry_socket_path)
    fake_registry = _FailingRegistry()
    node._registry = fake_registry  # type: ignore[assignment]
    node._registered = True
    node._registered_endpoints = [{"type": "uds", "addr": "/tmp/bounded.sock"}]
    node._stop = _LoopStop(node_module._HEARTBEAT_FAILURES_BEFORE_RECOVERY)  # type: ignore[assignment]
    try:
        node._heartbeat_loop()
        assert fake_registry.register_calls == 1
    finally:
        node.close()
