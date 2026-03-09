from __future__ import annotations

import os
import socket
import struct
import sys

import pytest

from nexus_sdk.fd import recv_fd

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="requires Linux SCM_RIGHTS")


def _fd_count() -> int:
    return len(os.listdir("/proc/self/fd"))


def test_recv_fd_closes_extra_file_descriptors() -> None:
    left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    sent_fds = [os.open("/dev/null", os.O_RDONLY) for _ in range(3)]
    received_fd: int | None = None
    baseline = _fd_count()

    try:
        ancillary = [
            (
                socket.SOL_SOCKET,
                socket.SCM_RIGHTS,
                struct.pack(f"{len(sent_fds)}i", *sent_fds),
            )
        ]
        sent = left.sendmsg([b"meta"], ancillary)
        assert sent > 0

        received_fd, metadata = recv_fd(right)
        assert metadata == b"meta"
        os.fstat(received_fd)
        assert _fd_count() == baseline + 1
    finally:
        left.close()
        right.close()
        for fd in sent_fds:
            try:
                os.close(fd)
            except OSError:
                pass
        if received_fd is not None:
            try:
                os.close(received_fd)
            except OSError:
                pass


def test_recv_fd_raises_when_no_fd_present() -> None:
    left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        left.sendall(b"no-fd")
        with pytest.raises(RuntimeError, match="no fd received"):
            recv_fd(right)
    finally:
        left.close()
        right.close()
