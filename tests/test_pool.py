from __future__ import annotations

import os
import socket
import threading
from typing import Any

from nexus_sdk.pool import ConnectionPool


def _start_uds_listener(path: str, expected_accepts: int) -> tuple[socket.socket, threading.Thread, list[socket.socket]]:
    try:
        os.remove(path)
    except FileNotFoundError:
        pass

    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(path)
    listener.listen(16)

    accepted: list[socket.socket] = []

    def accept_loop() -> None:
        while len(accepted) < expected_accepts:
            conn, _ = listener.accept()
            accepted.append(conn)

    thread = threading.Thread(target=accept_loop, daemon=True)
    thread.start()
    return listener, thread, accepted


def test_pool_get_put_reuses_connection(socket_path_factory: Any) -> None:
    addr = socket_path_factory("pool.sock")
    listener, thread, accepted = _start_uds_listener(addr, expected_accepts=1)
    pool = ConnectionPool(max_idle=8)
    try:
        conn1 = pool.get(addr)
        thread.join(timeout=1.0)
        assert len(accepted) == 1

        pool.put(addr, conn1)
        conn2 = pool.get(addr)
        assert conn1 is conn2

        conn2.close()
    finally:
        pool.close_all()
        listener.close()
        for conn in accepted:
            try:
                conn.close()
            except OSError:
                pass


def test_pool_close_all_closes_idle_connections(socket_path_factory: Any) -> None:
    addr = socket_path_factory("pool-close.sock")
    listener, thread, accepted = _start_uds_listener(addr, expected_accepts=1)
    pool = ConnectionPool(max_idle=8)
    try:
        conn = pool.get(addr)
        thread.join(timeout=1.0)
        pool.put(addr, conn)
        pool.close_all()
        assert conn.fileno() == -1
    finally:
        listener.close()
        for conn in accepted:
            try:
                conn.close()
            except OSError:
                pass


def test_pool_max_idle_limit(socket_path_factory: Any) -> None:
    addr = socket_path_factory("pool-max-idle.sock")
    listener, thread, accepted = _start_uds_listener(addr, expected_accepts=2)
    pool = ConnectionPool(max_idle=1)
    try:
        conn1 = pool.get(addr)
        conn2 = pool.get(addr)
        thread.join(timeout=1.0)

        pool.put(addr, conn1)
        pool.put(addr, conn2)

        closed_count = sum(1 for conn in (conn1, conn2) if conn.fileno() == -1)
        assert closed_count == 1

        reused = pool.get(addr)
        assert reused.fileno() != -1
        reused.close()
    finally:
        pool.close_all()
        listener.close()
        for conn in accepted:
            try:
                conn.close()
            except OSError:
                pass


def test_pool_tcp_dial_normalizes_bracketed_ipv6(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    class _DummyConn:
        def close(self) -> None:
            return None

    def fake_create_connection(address: tuple[str, int], timeout: float | None = None) -> _DummyConn:
        captured["address"] = address
        captured["timeout"] = timeout
        return _DummyConn()

    monkeypatch.setattr(socket, "create_connection", fake_create_connection)

    conn = ConnectionPool._dial("[::1]:1234", use_tcp=True, timeout=1.5)
    assert captured["address"] == ("::1", 1234)
    assert captured["timeout"] == 1.5
    conn.close()
