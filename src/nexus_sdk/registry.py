"""Registry control-plane client over daemon Unix socket."""

from __future__ import annotations

import socket
import threading
from dataclasses import dataclass
from typing import Any, Callable

from .transport import recv_message, send_message


@dataclass
class _WatchState:
    conn: socket.socket
    stop: threading.Event
    thread: threading.Thread


class RegistryClient:
    """Connects to nexusd control socket for register/lookup/heartbeat/watch."""

    def __init__(self, socket_path: str):
        if not socket_path:
            raise ValueError("socket_path is required")
        self._socket_path = socket_path
        self._lock = threading.Lock()
        self._closed = False
        self._next_watch = 0
        self._watches: dict[int, _WatchState] = {}

    def register(
        self,
        name: str,
        id: str,
        endpoints: list[dict[str, Any]],
        capabilities: list[str],
        ttl_ms: int = 15000,
    ) -> None:
        """Register a service instance in the registry."""
        req: dict[str, Any] = {
            "cmd": "register",
            "name": name,
            "id": id,
            "endpoints": endpoints,
            "capabilities": capabilities,
        }
        if ttl_ms > 0:
            req["ttl_ms"] = ttl_ms
        resp = self._request(req)
        self._ensure_ok(resp, "register")

    def unregister(self, id: str) -> None:
        """Unregister a service instance."""
        if not id:
            return
        resp = self._request({"cmd": "unregister", "id": id})
        self._ensure_ok(resp, "unregister")

    def heartbeat(self, id: str) -> None:
        """Send heartbeat for a registered instance."""
        if not id:
            raise ValueError("id is required")
        resp = self._request({"cmd": "heartbeat", "id": id})
        self._ensure_ok(resp, "heartbeat")

    def lookup(self, name: str) -> list[dict[str, Any]]:
        """Lookup service instances by service name."""
        if not name:
            return []
        resp = self._request({"cmd": "lookup", "name": name})
        error = str(resp.get("error") or "")
        if error:
            raise RuntimeError(error)
        instances = resp.get("instances", [])
        if not isinstance(instances, list):
            raise RuntimeError("invalid lookup response: instances is not a list")
        return instances

    def watch(self, name: str, callback: Callable[[dict[str, Any]], None]) -> Callable[[], None]:
        """Watch for service changes. Returns unsubscribe function."""
        if not name:
            raise ValueError("name is required")
        if callback is None:
            raise ValueError("callback is required")

        conn = self._dial()
        try:
            send_message(conn, {"cmd": "watch", "name": name})
            ack = recv_message(conn)
            self._ensure_ok(ack, "watch")
        except Exception:
            conn.close()
            raise

        stop_event = threading.Event()

        def loop() -> None:
            try:
                while not stop_event.is_set():
                    try:
                        event = recv_message(conn)
                    except socket.timeout:
                        continue
                    except (EOFError, OSError, ValueError):
                        break
                    try:
                        callback(event)
                    except Exception:
                        continue
            finally:
                self._remove_watch(watch_id)
                try:
                    conn.close()
                except OSError:
                    pass

        conn.settimeout(1.0)
        with self._lock:
            self._ensure_open_locked()
            watch_id = self._next_watch
            self._next_watch += 1
            thread = threading.Thread(target=loop, name=f"nexus-reg-watch-{watch_id}", daemon=True)
            self._watches[watch_id] = _WatchState(conn=conn, stop=stop_event, thread=thread)
        thread.start()

        def unsubscribe() -> None:
            state = self._remove_watch(watch_id)
            if state is None:
                return
            state.stop.set()
            try:
                state.conn.close()
            except OSError:
                pass
            if state.thread.is_alive() and state.thread is not threading.current_thread():
                state.thread.join(timeout=1.0)

        return unsubscribe

    def close(self) -> None:
        """Close the registry client and all active watch streams."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            watches = list(self._watches.values())
            self._watches.clear()
        for state in watches:
            state.stop.set()
            try:
                state.conn.close()
            except OSError:
                pass
        for state in watches:
            if state.thread.is_alive() and state.thread is not threading.current_thread():
                state.thread.join(timeout=1.0)

    def _request(self, req: dict[str, Any]) -> dict[str, Any]:
        conn = self._dial()
        try:
            send_message(conn, req)
            return recv_message(conn)
        finally:
            conn.close()

    def _dial(self) -> socket.socket:
        with self._lock:
            self._ensure_open_locked()
        conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        conn.settimeout(2.0)
        conn.connect(self._socket_path)
        return conn

    def _ensure_ok(self, resp: dict[str, Any], op: str) -> None:
        ok = bool(resp.get("ok"))
        error = str(resp.get("error") or "")
        if not ok:
            raise RuntimeError(error or f"{op} failed")

    def _ensure_open_locked(self) -> None:
        if self._closed:
            raise RuntimeError("registry client is closed")

    def _remove_watch(self, watch_id: int) -> _WatchState | None:
        with self._lock:
            return self._watches.pop(watch_id, None)
