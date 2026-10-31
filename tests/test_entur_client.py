"""Tests for common/entur_client.py: a dropped connection is retried once. Run: pytest"""
import socket
import threading

import pytest
import requests

from common.entur_client import EnturClient


class FlakyServer:
    """Tiny HTTP server. The first `drop` requests are answered by closing the
    connection without a response (what Entur sometimes does); after that it
    answers normally."""

    def __init__(self, drop=1, body=b'{"data":{"vehicles":[]}}'):
        self.drop, self.body, self.requests = drop, body, 0
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(10)
        self.url = f"http://127.0.0.1:{self.sock.getsockname()[1]}/"
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            with conn:
                request = conn.recv(65536)
                if b"Content-Length" in request and request.endswith(b"\r\n\r\n"):
                    conn.recv(65536)  # POST body arrived separately
                self.requests += 1
                if self.requests <= self.drop:
                    continue  # close without answering
                conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                             b"Content-Length: %d\r\nConnection: close\r\n\r\n" % len(self.body) + self.body)

    def close(self):
        self.sock.close()


def test_get_retries_after_dropped_connection():
    srv = FlakyServer(drop=1, body=b"<Siri/>")
    try:
        assert EnturClient("test").get_siri(srv.url) == b"<Siri/>"
        assert srv.requests == 2  # first dropped, second answered
    finally:
        srv.close()


def test_post_graphql_retries_after_dropped_connection():
    srv = FlakyServer(drop=1)
    try:
        assert EnturClient("test").post_graphql(srv.url, "{ vehicles { vehicleId } }") == b'{"data":{"vehicles":[]}}'
        assert srv.requests == 2
    finally:
        srv.close()


def test_gives_up_after_one_retry():
    srv = FlakyServer(drop=5)
    try:
        with pytest.raises(requests.exceptions.ConnectionError):
            EnturClient("test").get_siri(srv.url)
        assert srv.requests == 2  # only one extra try, then the round is skipped
    finally:
        srv.close()
