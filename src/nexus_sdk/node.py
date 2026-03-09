"""Nexus Node API for serving handlers and making RPC calls."""

from __future__ import annotations

import ipaddress
import logging
import os
import socket
import stat
import struct
import threading
import time
from dataclasses import dataclass
from typing import Callable

from .pool import ConnectionPool
from .registry import RegistryClient
from .transport import recv_rpc_message as recv_message
from .transport import send_rpc_message as send_message

DEFAULT_REGISTRY_ADDR = "/run/nexus/registry.sock"
_HEARTBEAT_INTERVAL_SECONDS = 5.0
_HEARTBEAT_FAILURES_BEFORE_RECOVERY = 3
_REGISTRATION_TTL_MS = 15000
_TCP_IDLE_TIMEOUT_SECONDS = 300.0
_TCP_LOOPBACK_HOSTS = ("localhost",)
logger = logging.getLogger("nexus_sdk")


@dataclass
class Request:
    """Inbound RPC request."""

    method: str
    payload: bytes
    headers: dict[str, str]


@dataclass
class Response:
    """Outbound RPC response."""

    payload: bytes
    headers: dict[str, str] | None = None


class _RPCBusinessError(RuntimeError):
    pass


class Node:
    """Nexus mesh participant - serves handlers and makes RPC calls."""

    def __init__(
        self,
        name: str,
        *,
        id: str | None = None,
        uds_addr: str | None = None,
        tcp_addr: str | None = None,
        allow_insecure_tcp: bool = False,
        max_inbound_conns: int = 128,
        registry_addr: str | None = None,
        capabilities: list[str] | None = None,
        call_timeout: float = 30.0,
        call_retries: int = 2,
    ):
        if not name:
            raise ValueError("name is required")
        if max_inbound_conns <= 0:
            raise ValueError("max_inbound_conns must be > 0")
        if call_timeout <= 0:
            raise ValueError("call_timeout must be > 0")
        if call_retries < 0:
            raise ValueError("call_retries must be >= 0")
        self._name = name
        self._id = id or name
        self._uds_addr = uds_addr
        self._tcp_addr = tcp_addr
        self._allow_insecure_tcp = allow_insecure_tcp
        self._local_node_id = socket.gethostname()
        self._registry = RegistryClient(registry_addr or DEFAULT_REGISTRY_ADDR)
        self._capabilities = list(capabilities or [])
        self._pool = ConnectionPool()
        self._call_timeout = call_timeout
        self._call_retries = call_retries

        self._handlers: dict[str, Callable[[Request], Response]] = {}
        self._handlers_lock = threading.Lock()

        self._state_lock = threading.Lock()
        self._state = "new"
        self._registered = False
        self._registered_endpoints: list[dict[str, str]] = []
        self._heartbeat_failures = 0

        self._stop = threading.Event()
        self._serve_thread: threading.Thread | None = None
        self._heartbeat_thread: threading.Thread | None = None
        self._listeners: list[socket.socket] = []
        self._accept_threads: list[threading.Thread] = []
        self._conn_threads: set[threading.Thread] = set()
        self._active_conns: set[socket.socket] = set()
        self._conn_lock = threading.Lock()
        self._conn_sem = threading.Semaphore(max_inbound_conns)

        self._rr_lock = threading.Lock()
        self._rr_offsets: dict[str, int] = {}

        if self._tcp_addr:
            self._validate_tcp_listen_addr(self._tcp_addr)

    def handle(self, method: str, handler: Callable[[Request], Response]) -> None:
        """Register a handler for an RPC method."""
        if not method:
            raise ValueError("method is required")
        with self._handlers_lock:
            self._handlers[method] = handler

    def serve(self, *, timeout: float | None = None) -> None:
        """Start serving (blocking). Registers with registry, starts heartbeat."""
        self._start_serving()
        started = time.monotonic()
        try:
            while not self._stop.is_set():
                if timeout is not None and (time.monotonic() - started) >= timeout:
                    break
                time.sleep(0.05)
        finally:
            self.close()

    def serve_async(self) -> None:
        """Start serving in a background thread."""
        with self._state_lock:
            if self._state in {"starting", "serving"}:
                return
            if self._state == "closed":
                raise RuntimeError("node is closed")
            self._state = "starting"
            self._serve_thread = threading.Thread(target=self.serve, name=f"nexus-node-{self._id}", daemon=True)
            self._serve_thread.start()

    def call(self, service: str, method: str, payload: bytes) -> Response:
        """Make an RPC call to a remote service."""
        if not service:
            raise ValueError("service is required")
        if not method:
            raise ValueError("method is required")

        instances = self._registry.lookup(service)
        if not instances:
            with self._rr_lock:
                self._rr_offsets.pop(service, None)
            raise RuntimeError(f"service {service!r} not found")

        instance = self._pick_instance(service, instances)
        addr, use_tcp = self._pick_endpoint(instance)
        if use_tcp and not self._allow_insecure_tcp and not self._is_loopback_tcp_addr(addr):
            raise ConnectionError(
                f"Refusing non-loopback TCP call to {addr} without encryption. "
                "Set allow_insecure_tcp=True to override."
            )

        for attempt in range(self._call_retries + 1):
            conn = self._pool.get(addr, use_tcp=use_tcp, timeout=self._call_timeout)
            reusable = True
            try:
                conn.settimeout(self._call_timeout)
                send_message(conn, {"method": method, "payload": payload, "headers": {}})
                resp = recv_message(conn)

                headers = resp.get("headers")
                if not isinstance(headers, dict):
                    headers = {}
                error = headers.get("error")
                if error:
                    raise _RPCBusinessError(str(error))
                body = resp.get("payload", b"")
                if not isinstance(body, (bytes, bytearray)):
                    raise RuntimeError("invalid response payload")
                return Response(payload=bytes(body), headers=headers)
            except _RPCBusinessError as exc:
                raise RuntimeError(str(exc))
            except socket.timeout as exc:
                reusable = False
                timeout_error = TimeoutError(
                    f"RPC call to {service}.{method} timed out after {self._call_timeout}s"
                )
                self._close_failed_conn(addr, conn)
                if attempt >= self._call_retries:
                    raise timeout_error from exc
                time.sleep(0.1 * (attempt + 1))
            except (ConnectionError, TimeoutError, OSError, EOFError, struct.error):
                reusable = False
                self._close_failed_conn(addr, conn)
                if attempt >= self._call_retries:
                    raise
                time.sleep(0.1 * (attempt + 1))
            except Exception as exc:
                reusable = False
                logger.warning(
                    "unexpected RPC call failure service=%s method=%s: %s",
                    service,
                    method,
                    exc,
                )
                self._close_failed_conn(addr, conn)
                raise
            finally:
                if reusable:
                    self._pool.put(addr, conn, use_tcp=use_tcp)

        raise RuntimeError("unreachable")

    @staticmethod
    def _close_failed_conn(addr: str, conn: socket.socket) -> None:
        try:
            conn.close()
        except OSError as exc:
            logger.debug("failed to close failed RPC connection %s: %s", addr, exc)

    def close(self) -> None:
        """Stop serving and clean up."""
        with self._state_lock:
            if self._state == "closed":
                return
            self._state = "closed"

        self._stop.set()

        for listener in self._listeners:
            try:
                listener.close()
            except OSError as exc:
                logger.debug("failed to close listener: %s", exc)

        with self._conn_lock:
            active_conns = list(self._active_conns)
        for conn in active_conns:
            try:
                conn.close()
            except OSError as exc:
                logger.debug("failed to close active connection: %s", exc)

        for thread in list(self._accept_threads):
            if thread.is_alive() and thread is not threading.current_thread():
                thread.join(timeout=1.0)

        with self._conn_lock:
            conn_threads = list(self._conn_threads)
        for thread in conn_threads:
            if thread.is_alive() and thread is not threading.current_thread():
                thread.join(timeout=1.0)

        if self._registered:
            try:
                self._registry.unregister(self._id)
            except OSError as exc:
                logger.debug("unregister failed for %s due to socket error: %s", self._id, exc)
            except Exception as exc:
                logger.warning("unregister failed for %s: %s", self._id, exc)
            self._registered = False

        if self._heartbeat_thread and self._heartbeat_thread.is_alive() and self._heartbeat_thread is not threading.current_thread():
            self._heartbeat_thread.join(timeout=1.0)

        self._pool.close_all()
        self._registry.close()

        if self._uds_addr:
            self._cleanup_socket_path(self._uds_addr, strict=False)

    def __enter__(self) -> Node:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _start_serving(self) -> None:
        with self._state_lock:
            if self._state == "closed":
                raise RuntimeError("node is closed")
            if self._state == "serving":
                raise RuntimeError("node is already serving")
            if self._state not in {"new", "starting"}:
                raise RuntimeError(f"node is in invalid state: {self._state}")
            self._state = "serving"

        listeners: list[socket.socket] = []
        endpoints: list[dict[str, str]] = []

        try:
            if self._uds_addr:
                uds_listener = self._create_uds_listener(self._uds_addr)
                listeners.append(uds_listener)
                endpoints.append({"type": "uds", "addr": self._uds_addr})
            if self._tcp_addr:
                tcp_listener = self._start_tcp_listener(self._tcp_addr)
                listeners.append(tcp_listener)
                host, port = tcp_listener.getsockname()[:2]
                endpoints.append({"type": "tcp", "addr": f"{host}:{port}"})
            if not listeners:
                raise ValueError("at least one of uds_addr or tcp_addr is required")

            self._listeners = listeners
            self._registry.register(
                self._name,
                self._id,
                endpoints=endpoints,
                capabilities=self._capabilities,
                ttl_ms=_REGISTRATION_TTL_MS,
            )
            self._registered = True
            self._registered_endpoints = [dict(endpoint) for endpoint in endpoints]
            self._heartbeat_failures = 0

            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop,
                name=f"nexus-heartbeat-{self._id}",
                daemon=True,
            )
            self._heartbeat_thread.start()

            for listener in listeners:
                thread = threading.Thread(target=self._accept_loop, args=(listener,), daemon=True)
                self._accept_threads.append(thread)
                thread.start()
        except Exception:
            for listener in listeners:
                try:
                    listener.close()
                except OSError as exc:
                    logger.debug("failed to close listener during startup rollback: %s", exc)
            if self._registered:
                try:
                    self._registry.unregister(self._id)
                except OSError as exc:
                    logger.debug("unregister failed for %s during startup rollback: %s", self._id, exc)
                except Exception as exc:
                    logger.warning("unregister failed for %s during startup rollback: %s", self._id, exc)
                self._registered = False
            self._registered_endpoints = []
            self._listeners = []
            with self._state_lock:
                if self._state != "closed":
                    self._state = "new"
            raise

    def _accept_loop(self, listener: socket.socket) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if self._stop.is_set():
                    return
                return

            acquired = False
            while not self._stop.is_set():
                acquired = self._conn_sem.acquire(timeout=1.0)
                if acquired:
                    break
            if not acquired:
                try:
                    conn.close()
                except OSError as exc:
                    logger.debug("failed to close unacquired connection: %s", exc)
                continue

            try:
                conn.settimeout(_TCP_IDLE_TIMEOUT_SECONDS)
            except OSError:
                self._conn_sem.release()
                try:
                    conn.close()
                except OSError as exc:
                    logger.debug("failed to close connection after timeout setup failure: %s", exc)
                continue

            conn_thread = threading.Thread(target=self._serve_conn, args=(conn,), daemon=True)
            with self._conn_lock:
                self._conn_threads.add(conn_thread)
            conn_thread.start()

    def _serve_conn(self, conn: socket.socket) -> None:
        with self._conn_lock:
            self._active_conns.add(conn)
        try:
            with conn:
                while not self._stop.is_set():
                    try:
                        req = recv_message(conn)
                    except socket.timeout:
                        return
                    except (EOFError, OSError, struct.error) as exc:
                        logger.debug("connection receive terminated: %s", exc)
                        return
                    except Exception as exc:
                        logger.warning("unexpected receive error: %s", exc)
                        return

                    method = str(req.get("method") or "")
                    payload = req.get("payload", b"")
                    headers = req.get("headers")
                    if not isinstance(headers, dict):
                        headers = {}

                    if not isinstance(payload, (bytes, bytearray)):
                        send_message(conn, {"method": method, "payload": b"", "headers": {"error": "invalid payload"}})
                        continue

                    with self._handlers_lock:
                        handler = self._handlers.get(method)
                    if handler is None:
                        send_message(
                            conn,
                            {
                                "method": method,
                                "payload": b"",
                                "headers": {"error": f"handler not found: {method}"},
                            },
                        )
                        continue

                    try:
                        resp = handler(Request(method=method, payload=bytes(payload), headers=headers))
                    except (ValueError, RuntimeError) as exc:
                        logger.debug("handler returned expected error for method %s: %s", method, exc)
                        send_message(conn, {"method": method, "payload": b"", "headers": {"error": str(exc)}})
                        continue
                    except Exception as exc:
                        logger.warning("unexpected handler exception for method %s: %s", method, exc)
                        send_message(conn, {"method": method, "payload": b"", "headers": {"error": str(exc)}})
                        continue

                    resp_headers = resp.headers or {}
                    send_message(
                        conn,
                        {
                            "method": method,
                            "payload": resp.payload,
                            "headers": resp_headers,
                        },
                    )
        finally:
            with self._conn_lock:
                self._active_conns.discard(conn)
                current = threading.current_thread()
                self._conn_threads.discard(current)
            self._conn_sem.release()

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(_HEARTBEAT_INTERVAL_SECONDS):
            try:
                self._registry.heartbeat(self._id)
            except Exception as exc:
                self._heartbeat_failures += 1
                if self._attempt_heartbeat_recovery(exc):
                    continue
                logger.warning("heartbeat failed for %s: %s", self._id, exc)
                continue
            self._heartbeat_failures = 0

    def _attempt_heartbeat_recovery(self, err: Exception) -> bool:
        if self._heartbeat_failures < _HEARTBEAT_FAILURES_BEFORE_RECOVERY:
            return False
        if "instance not found" not in str(err).lower():
            return False
        if not self._registered_endpoints:
            return False

        try:
            self._registry.register(
                self._name,
                self._id,
                endpoints=[dict(endpoint) for endpoint in self._registered_endpoints],
                capabilities=list(self._capabilities),
                ttl_ms=_REGISTRATION_TTL_MS,
            )
        except Exception as exc:
            logger.warning("heartbeat recovery register failed for %s: %s", self._id, exc)
            return False

        self._registered = True
        self._heartbeat_failures = 0
        logger.info("re-registered after heartbeat failure for %s", self._id)
        return True

    def _pick_instance(self, service: str, instances: list[dict[str, object]]) -> dict[str, object]:
        with self._rr_lock:
            idx = self._rr_offsets.get(service, 0) % len(instances)
            self._rr_offsets[service] = (idx + 1) % len(instances)
        return instances[idx]

    def _pick_endpoint(self, instance: dict[str, object]) -> tuple[str, bool]:
        endpoints = instance.get("endpoints")
        if not isinstance(endpoints, list):
            raise RuntimeError("instance has no endpoints")

        uds_addr: str | None = None
        tcp_addr: str | None = None
        for endpoint in endpoints:
            if not isinstance(endpoint, dict):
                continue
            kind = endpoint.get("type")
            addr = endpoint.get("addr")
            if not isinstance(addr, str) or not addr:
                continue
            if kind == "uds":
                uds_addr = addr
            if kind == "tcp":
                tcp_addr = addr

        instance_node = instance.get("node", "")
        if isinstance(instance_node, bytes):
            try:
                instance_node = instance_node.decode()
            except UnicodeDecodeError:
                instance_node = ""
        if not isinstance(instance_node, str):
            instance_node = ""
        is_local_instance = bool(instance_node) and instance_node == self._local_node_id
        is_remote_instance = bool(instance_node) and instance_node != self._local_node_id

        if is_local_instance:
            if uds_addr:
                return uds_addr, False
            if tcp_addr:
                return tcp_addr, True
        elif is_remote_instance:
            if tcp_addr:
                return tcp_addr, True
            if uds_addr:
                instance_id = instance.get("id", "")
                if isinstance(instance_id, bytes):
                    try:
                        instance_id = instance_id.decode()
                    except UnicodeDecodeError:
                        instance_id = ""
                if not isinstance(instance_id, str):
                    instance_id = ""
                raise ConnectionError(
                    f"remote instance {instance_id} has no TCP endpoint; refusing UDS fallback for non-local target"
                )
        else:
            if tcp_addr:
                return tcp_addr, True
            if uds_addr:
                instance_id = instance.get("id", "")
                if isinstance(instance_id, bytes):
                    try:
                        instance_id = instance_id.decode()
                    except UnicodeDecodeError:
                        instance_id = ""
                if not isinstance(instance_id, str):
                    instance_id = ""
                raise ConnectionError(
                    f"instance {instance_id} has unknown node identity and no TCP endpoint; "
                    "refusing UDS fallback for non-local target"
                )
        raise RuntimeError("instance has no usable endpoints")

    @staticmethod
    def _create_uds_listener(path: str) -> socket.socket:
        Node._cleanup_socket_path(path, strict=True)
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, mode=0o700, exist_ok=True)

        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(path)
        os.chmod(path, 0o600)
        listener.listen(128)
        listener.settimeout(1.0)
        return listener

    def _start_tcp_listener(self, addr: str) -> socket.socket:
        self._validate_tcp_listen_addr(addr)
        return self._create_tcp_listener(addr)

    @staticmethod
    def _parse_tcp_addr(addr: str) -> tuple[str, int]:
        if not addr:
            raise ValueError("tcp listen address is required")
        host, sep, port_text = addr.rpartition(":")
        if not sep:
            raise ValueError(f"invalid tcp listen address: {addr}")
        if host.startswith("[") and host.endswith("]"):
            host = host[1:-1]
        try:
            port = int(port_text)
        except ValueError as exc:
            raise ValueError(f"invalid tcp listen port: {addr}") from exc
        return host, port

    @classmethod
    def _is_loopback_tcp_addr(cls, addr: str) -> bool:
        try:
            host, _ = cls._parse_tcp_addr(addr)
        except ValueError:
            host = addr.split(":")[0] if ":" in addr else addr
        host = host.strip().lower()
        if host in {"", "0.0.0.0", "::"}:
            return False
        if host in _TCP_LOOPBACK_HOSTS:
            return True
        try:
            return ipaddress.ip_address(host).is_loopback
        except ValueError:
            return False

    def _validate_tcp_listen_addr(self, addr: str) -> None:
        if self._allow_insecure_tcp:
            return
        if self._is_loopback_tcp_addr(addr):
            return
        raise ValueError(
            f"Refusing non-loopback TCP listen address {addr}. "
            "Set allow_insecure_tcp=True to override."
        )

    @staticmethod
    def _create_tcp_listener(addr: str) -> socket.socket:
        host, port = Node._parse_tcp_addr(addr)
        family = socket.AF_INET6 if ":" in host else socket.AF_INET
        listener = socket.socket(family, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, port))
        listener.listen(128)
        listener.settimeout(1.0)
        return listener

    @staticmethod
    def _cleanup_socket_path(path: str, strict: bool) -> None:
        try:
            info = os.lstat(path)
        except FileNotFoundError:
            return
        except OSError as exc:
            if strict:
                raise RuntimeError(f"stat existing socket path failed: {path}: {exc}") from exc
            return

        if not stat.S_ISSOCK(info.st_mode):
            if strict:
                raise RuntimeError(f"uds path exists and is not a socket: {path}")
            return

        try:
            os.remove(path)
        except FileNotFoundError:
            return
        except OSError as exc:
            if strict:
                raise RuntimeError(f"cleanup stale socket failed: {path}: {exc}") from exc
