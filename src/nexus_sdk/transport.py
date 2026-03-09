"""Transport helpers for Nexus RPC/control messages."""

from __future__ import annotations

import socket
import struct
import threading
import weakref
from typing import Any

import msgpack
from msgpack import exceptions as msgpack_exceptions

MAX_MESSAGE_SIZE = 64 * 1024 * 1024
_RPC_READ_CHUNK_SIZE = 64 * 1024


class _RPCStreamDecoder:
    def __init__(self) -> None:
        self.unpacker = msgpack.Unpacker(
            raw=False,
            max_buffer_size=MAX_MESSAGE_SIZE,
            max_str_len=MAX_MESSAGE_SIZE,
            max_bin_len=MAX_MESSAGE_SIZE,
            max_array_len=MAX_MESSAGE_SIZE,
            max_map_len=MAX_MESSAGE_SIZE,
            max_ext_len=MAX_MESSAGE_SIZE,
        )
        self.lock = threading.Lock()


_rpc_decoders: weakref.WeakKeyDictionary[socket.socket, _RPCStreamDecoder] = weakref.WeakKeyDictionary()
_rpc_decoders_lock = threading.Lock()


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    """Receive exactly ``size`` bytes from ``sock`` or raise EOFError."""
    data = bytearray(size)
    view = memoryview(data)
    read = 0
    while read < size:
        n = sock.recv_into(view[read:])
        if n == 0:
            raise EOFError("connection closed")
        read += n
    return bytes(data)


def send_framed_message(sock: socket.socket, msg: dict[str, Any]) -> None:
    """Encode and send a length-prefixed msgpack message."""
    data = msgpack.packb(msg, use_bin_type=True)
    if len(data) > MAX_MESSAGE_SIZE:
        raise ValueError(f"message too large: {len(data)}")
    sock.sendall(struct.pack(">I", len(data)))
    sock.sendall(data)


def recv_framed_message(sock: socket.socket) -> dict[str, Any]:
    """Read a length-prefixed msgpack message."""
    header = _recv_exact(sock, 4)
    length = struct.unpack(">I", header)[0]
    if length > MAX_MESSAGE_SIZE:
        raise ValueError(f"message too large: {length}")
    data = _recv_exact(sock, length)
    obj = msgpack.unpackb(data, raw=False)
    if not isinstance(obj, dict):
        raise ValueError("decoded message is not a map")
    return obj


def _get_rpc_decoder(sock: socket.socket) -> _RPCStreamDecoder:
    with _rpc_decoders_lock:
        decoder = _rpc_decoders.get(sock)
        if decoder is None:
            decoder = _RPCStreamDecoder()
            _rpc_decoders[sock] = decoder
        return decoder


def _drop_rpc_decoder(sock: socket.socket) -> None:
    with _rpc_decoders_lock:
        _rpc_decoders.pop(sock, None)


def send_rpc_message(sock: socket.socket, msg: dict[str, Any]) -> None:
    """Encode and send one streamed msgpack RPC message (no frame header)."""
    data = msgpack.packb(msg, use_bin_type=True)
    if len(data) > MAX_MESSAGE_SIZE:
        raise ValueError(f"message too large: {len(data)}")
    sock.sendall(data)


def recv_rpc_message(sock: socket.socket) -> dict[str, Any]:
    """Read one streamed msgpack RPC message from a persistent socket."""
    decoder = _get_rpc_decoder(sock)
    with decoder.lock:
        while True:
            try:
                obj = decoder.unpacker.unpack()
            except msgpack_exceptions.OutOfData:
                chunk = sock.recv(_RPC_READ_CHUNK_SIZE)
                if not chunk:
                    _drop_rpc_decoder(sock)
                    raise EOFError("connection closed")
                decoder.unpacker.feed(chunk)
                continue
            except msgpack_exceptions.BufferFull as exc:
                _drop_rpc_decoder(sock)
                raise ValueError(f"message too large: >{MAX_MESSAGE_SIZE}") from exc
            except (
                msgpack_exceptions.ExtraData,
                msgpack_exceptions.FormatError,
                msgpack_exceptions.StackError,
                msgpack_exceptions.UnpackValueError,
                ValueError,
                TypeError,
            ) as exc:
                _drop_rpc_decoder(sock)
                raise ValueError(f"failed to decode msgpack message: {exc}") from exc
            break

    if not isinstance(obj, dict):
        _drop_rpc_decoder(sock)
        raise ValueError("decoded message is not a map")
    return obj


# Backward-compatible aliases for control-plane framing helpers.
send_message = send_framed_message
recv_message = recv_framed_message
