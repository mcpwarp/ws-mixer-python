"""Test doubles for code built on ws_mixer: a fake clock, scripted WebSockets and a fake mcpwarp
edge. Stdlib only."""

from .fake_clock import FakeClock, FakeTimer, settle
from .fake_edge import EdgeConnection, EdgeResponse, FakeEdge, FakeEdgeUpgrade
from .fake_socket import FakeConnector, FakeWebSocket, socket_pair

__all__ = [
    "EdgeConnection",
    "EdgeResponse",
    "FakeClock",
    "FakeConnector",
    "FakeEdge",
    "FakeEdgeUpgrade",
    "FakeTimer",
    "FakeWebSocket",
    "settle",
    "socket_pair",
]
