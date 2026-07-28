"""
tests/test_ipc.py

Tests for DuplexIPCTransport.
"""

import asyncio
import pytest
import socket
from sync_state.transports.ipc import DuplexIPCTransport, FRAME_COMMAND, FRAME_COMMAND_ACK


@pytest.fixture
async def ipc_pair():
    """Create a connected pair of DuplexIPCTransport objects using a socket pair."""
    sock1, sock2 = socket.socketpair()
    loop = asyncio.get_event_loop()
    reader1, writer1 = await asyncio.open_connection(sock=sock1)
    reader2, writer2 = await asyncio.open_connection(sock=sock2)
    transport1 = DuplexIPCTransport(reader1, writer1)
    transport2 = DuplexIPCTransport(reader2, writer2)
    yield transport1, transport2
    await transport1.close()
    await transport2.close()


@pytest.mark.asyncio
async def test_send_command_and_receive(ipc_pair):
    t1, t2 = ipc_pair
    await t1.send_command("cmd-123", "robot:move", {"x": 10}, "client-abc")
    frame_type, payload = await t2.read_frame()
    assert frame_type == FRAME_COMMAND
    data = json.loads(payload)
    assert data["command_id"] == "cmd-123"
    assert data["action"] == "robot:move"
    assert data["params"]["x"] == 10
    assert data["client_id"] == "client-abc"


@pytest.mark.asyncio
async def test_send_ack_and_receive(ipc_pair):
    t1, t2 = ipc_pair
    await t1.send_ack("cmd-123", "ok", {"tick": 42}, 42)
    frame_type, payload = await t2.read_frame()
    assert frame_type == FRAME_COMMAND_ACK
    data = json.loads(payload)
    assert data["command_id"] == "cmd-123"
    assert data["status"] == "ok"
    assert data["tick_id"] == 42
    assert data["result"]["tick"] == 42


@pytest.mark.asyncio
async def test_close(ipc_pair):
    t1, t2 = ipc_pair
    await t1.close()
    # Reading from closed transport returns None
    frame_type, payload = await t2.read_frame()
    assert frame_type is None
    assert payload is None