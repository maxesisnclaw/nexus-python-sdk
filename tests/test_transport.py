from __future__ import annotations

import os
import shutil
import socket
import struct
import subprocess
import tempfile
from pathlib import Path

import msgpack
import pytest

from nexus_sdk.transport import MAX_MESSAGE_SIZE, recv_message, recv_rpc_message, send_message, send_rpc_message


def test_send_recv_message_round_trip_socketpair() -> None:
    left, right = socket.socketpair()
    try:
        msg = {"method": "echo", "payload": b"hello", "headers": {"trace": "1"}}
        send_message(left, msg)
        got = recv_message(right)
        assert got == msg
    finally:
        left.close()
        right.close()


def test_recv_message_size_limit_enforced() -> None:
    left, right = socket.socketpair()
    try:
        left.sendall(struct.pack(">I", MAX_MESSAGE_SIZE + 1))
        with pytest.raises(ValueError, match="message too large"):
            recv_message(right)
    finally:
        left.close()
        right.close()


def test_encoding_is_4byte_big_endian_plus_msgpack() -> None:
    left, right = socket.socketpair()
    try:
        msg = {"method": "echo", "payload": b"abc", "headers": {"k": "v"}}
        send_message(left, msg)

        hdr = right.recv(4)
        assert len(hdr) == 4
        n = struct.unpack(">I", hdr)[0]
        payload = right.recv(n)
        assert len(payload) == n
        assert payload == msgpack.packb(msg, use_bin_type=True)
    finally:
        left.close()
        right.close()


def test_python_wire_encoding_matches_go_msgpack_frame() -> None:
    if shutil.which("go") is None:
        pytest.skip("go toolchain not available")

    msg = {"method": "echo", "payload": b"abc", "headers": {"trace": "1"}}
    body = msgpack.packb(msg, use_bin_type=True)
    expected = struct.pack(">I", len(body)) + body

    root = Path(__file__).resolve().parents[4]
    go_src = """
package main

import (
    "encoding/binary"
    "encoding/hex"
    "fmt"

    "github.com/vmihailenco/msgpack/v5"
)

type Message struct {
    Method  string            `msgpack:"method"`
    Payload []byte            `msgpack:"payload"`
    Headers map[string]string `msgpack:"headers,omitempty"`
}

func main() {
    msg := Message{Method: "echo", Payload: []byte("abc"), Headers: map[string]string{"trace": "1"}}
    body, err := msgpack.Marshal(msg)
    if err != nil {
        panic(err)
    }
    frame := make([]byte, 4+len(body))
    binary.BigEndian.PutUint32(frame[:4], uint32(len(body)))
    copy(frame[4:], body)
    fmt.Print(hex.EncodeToString(frame))
}
""".strip()

    with tempfile.NamedTemporaryFile("w", suffix=".go", delete=False) as fp:
        fp.write(go_src)
        temp_go = fp.name

    try:
        proc = subprocess.run(
            ["go", "run", temp_go],
            cwd=root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    finally:
        os.remove(temp_go)

    got = bytes.fromhex(proc.stdout.strip())
    assert got == expected


def test_rpc_stream_round_trip_socketpair() -> None:
    left, right = socket.socketpair()
    try:
        msg = {"method": "echo", "payload": b"hello", "headers": {"trace": "1"}}
        send_rpc_message(left, msg)
        got = recv_rpc_message(right)
        assert got == msg
    finally:
        left.close()
        right.close()


def test_rpc_stream_recv_keeps_buffered_bytes_for_reused_socket() -> None:
    left, right = socket.socketpair()
    try:
        first = {"method": "one", "payload": b"a", "headers": {}}
        second = {"method": "two", "payload": b"b", "headers": {"k": "v"}}
        body = msgpack.packb(first, use_bin_type=True) + msgpack.packb(second, use_bin_type=True)
        left.sendall(body)

        assert recv_rpc_message(right) == first
        assert recv_rpc_message(right) == second
    finally:
        left.close()
        right.close()


def test_raw_msgpack_rpc_is_not_framed_control_plane() -> None:
    left, right = socket.socketpair()
    try:
        msg = {"method": "echo", "payload": b"hello", "headers": {}}
        payload = msgpack.packb(msg, use_bin_type=True)
        assert struct.unpack(">I", payload[:4])[0] > MAX_MESSAGE_SIZE

        left.sendall(payload)
        with pytest.raises(ValueError, match="message too large"):
            recv_message(right)
    finally:
        left.close()
        right.close()
