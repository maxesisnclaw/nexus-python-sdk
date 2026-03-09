"""File descriptor passing helpers for Unix domain sockets."""

from __future__ import annotations

import os
import socket
import struct
from typing import Tuple


def send_fd(sock: socket.socket, fd: int, metadata: bytes = b"fd") -> None:
    """Send ``fd`` over ``sock`` using SCM_RIGHTS.

    Args:
        sock: Connected Unix-domain socket.
        fd: File descriptor to send.
        metadata: Small payload sent with ancillary data.

    Raises:
        ValueError: If ``fd`` is invalid.
        RuntimeError: If sending ancillary data fails.
    """
    if fd < 0:
        raise ValueError(f"invalid file descriptor: {fd}")
    payload = metadata or b"fd"
    anc = [(socket.SOL_SOCKET, socket.SCM_RIGHTS, struct.pack("i", fd))]
    try:
        sent = sock.sendmsg([payload], anc)
    except OSError as exc:
        raise RuntimeError(f"sendmsg failed: {exc}") from exc
    if sent <= 0:
        raise RuntimeError("sendmsg sent no data")


def recv_fd(sock: socket.socket) -> Tuple[int, bytes]:
    """Receive one file descriptor from ``sock``.

    Returns:
        A tuple of ``(fd, metadata)``.

    Raises:
        RuntimeError: If no descriptor is found or recvmsg fails.
    """
    fd_size = struct.calcsize("i")
    # Reserve enough ancillary buffer for multi-fd SCM_RIGHTS payloads.
    cmsg_space = socket.CMSG_SPACE(fd_size * 64)
    try:
        msg, ancdata, _flags, _addr = sock.recvmsg(65536, cmsg_space)
    except OSError as exc:
        raise RuntimeError(f"recvmsg failed: {exc}") from exc

    received_fds: list[int] = []
    for level, ctype, data in ancdata:
        if level == socket.SOL_SOCKET and ctype == socket.SCM_RIGHTS:
            for offset in range(0, len(data) - (len(data) % fd_size), fd_size):
                received_fds.append(struct.unpack("i", data[offset : offset + fd_size])[0])

    if not received_fds:
        raise RuntimeError("no fd received")

    fd = received_fds[0]
    for extra_fd in received_fds[1:]:
        try:
            os.close(extra_fd)
        except OSError:
            # Best-effort close for extra descriptors to avoid leaks.
            continue
    return fd, msg
