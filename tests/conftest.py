from __future__ import annotations

import os
import shutil
import socket
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from nexus_sdk.transport import recv_message, send_message


@dataclass
class _Session:
    conn: socket.socket
    lock: threading.Lock = field(default_factory=threading.Lock)
    watched: set[str] = field(default_factory=set)

    def send(self, msg: dict[str, Any]) -> None:
        with self.lock:
            send_message(self.conn, msg)


class MockRegistryServer:
    def __init__(self, socket_path: str):
        self.socket_path = socket_path
        self._listener: socket.socket | None = None
        self._stop = threading.Event()
        self._accept_thread: threading.Thread | None = None

        self._lock = threading.Lock()
        self._instances: dict[str, dict[str, Any]] = {}
        self._sessions: list[_Session] = []
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        parent = os.path.dirname(self.socket_path)
        if parent:
            os.makedirs(parent, mode=0o700, exist_ok=True)
        try:
            os.remove(self.socket_path)
        except FileNotFoundError:
            pass

        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(self.socket_path)
        os.chmod(self.socket_path, 0o600)
        listener.listen(64)
        listener.settimeout(0.2)
        self._listener = listener

        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    def close(self) -> None:
        self._stop.set()

        if self._listener is not None:
            try:
                self._listener.close()
            except OSError:
                pass

        with self._lock:
            sessions = self._sessions[:]
            self._sessions = []
        for session in sessions:
            try:
                session.conn.close()
            except OSError:
                pass

        if self._accept_thread and self._accept_thread.is_alive():
            self._accept_thread.join(timeout=1.0)

        for thread in list(self._threads):
            if thread.is_alive():
                thread.join(timeout=1.0)

        try:
            os.remove(self.socket_path)
        except FileNotFoundError:
            pass

    def _accept_loop(self) -> None:
        assert self._listener is not None
        while not self._stop.is_set():
            try:
                conn, _ = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return

            session = _Session(conn=conn)
            with self._lock:
                self._sessions.append(session)

            thread = threading.Thread(target=self._serve_session, args=(session,), daemon=True)
            self._threads.append(thread)
            thread.start()

    def _serve_session(self, session: _Session) -> None:
        try:
            with session.conn:
                while not self._stop.is_set():
                    try:
                        req = recv_message(session.conn)
                    except (EOFError, OSError, ValueError):
                        return
                    self._handle_request(session, req)
        finally:
            with self._lock:
                if session in self._sessions:
                    self._sessions.remove(session)

    def _handle_request(self, session: _Session, req: dict[str, Any]) -> None:
        cmd = str(req.get("cmd") or "")
        if cmd == "register":
            name = str(req.get("name") or "")
            id_ = str(req.get("id") or "")
            if not name or not id_:
                session.send({"ok": False, "error": "register requires name and id"})
                return
            endpoints = req.get("endpoints")
            capabilities = req.get("capabilities")
            if not isinstance(endpoints, list):
                endpoints = []
            if not isinstance(capabilities, list):
                capabilities = []

            inst = {
                "name": name,
                "id": id_,
                "node": "test-node",
                "capabilities": capabilities,
                "endpoints": endpoints,
                "metadata": {},
                "ttl": int(req.get("ttl_ms", 0)) * 1_000_000,
            }
            with self._lock:
                self._instances[id_] = inst
            session.send({"ok": True})
            self._notify_watchers(name, {"event": "up", "instance": inst})
            return

        if cmd == "unregister":
            id_ = str(req.get("id") or "")
            if not id_:
                session.send({"ok": False, "error": "unregister requires id"})
                return
            with self._lock:
                inst = self._instances.pop(id_, None)
            session.send({"ok": True})
            if inst:
                self._notify_watchers(str(inst.get("name") or ""), {"event": "down", "instance": inst})
            return

        if cmd == "heartbeat":
            id_ = str(req.get("id") or "")
            if not id_:
                session.send({"ok": False, "error": "heartbeat requires id"})
                return
            with self._lock:
                exists = id_ in self._instances
            if not exists:
                session.send({"ok": False, "error": "instance not found"})
                return
            session.send({"ok": True})
            return

        if cmd == "lookup":
            name = str(req.get("name") or "")
            if not name:
                session.send({"error": "lookup requires name", "instances": []})
                return
            with self._lock:
                items = [dict(inst) for inst in self._instances.values() if inst.get("name") == name]
            session.send({"instances": items})
            return

        if cmd == "watch":
            name = str(req.get("name") or "")
            if not name:
                session.send({"ok": False, "error": "watch requires name"})
                return
            session.watched.add(name)
            session.send({"ok": True})
            return

        session.send({"ok": False, "error": f"unknown command: {cmd}"})

    def _notify_watchers(self, name: str, event: dict[str, Any]) -> None:
        with self._lock:
            sessions = [s for s in self._sessions if name in s.watched]
        for session in sessions:
            try:
                session.send(event)
            except OSError:
                with self._lock:
                    if session in self._sessions:
                        self._sessions.remove(session)


@pytest.fixture
def socket_path_factory() -> Any:
    created_dirs: list[Path] = []

    def make(filename: str = "sock") -> str:
        base = Path("/tmp") / f"nxs-{uuid.uuid4().hex[:8]}"
        base.mkdir(mode=0o700, exist_ok=True)
        created_dirs.append(base)
        return str(base / filename)

    yield make

    for directory in created_dirs:
        shutil.rmtree(directory, ignore_errors=True)


@pytest.fixture
def registry_socket_path(socket_path_factory: Any) -> str:
    return socket_path_factory("registry.sock")


@pytest.fixture
def mock_registry(registry_socket_path: str) -> MockRegistryServer:
    server = MockRegistryServer(registry_socket_path)
    server.start()
    yield server
    server.close()


def wait_until(predicate: Any, timeout: float = 2.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise TimeoutError("condition not met in time")
