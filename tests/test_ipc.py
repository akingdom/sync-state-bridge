import asyncio
import json
import pytest
import socket
from sync_state.core.ipc_transport import IPCTransport, read_payload, pack_header


@pytest.fixture
async def ipc_pair():
    """Create a connected pair of raw sockets for testing."""
    sock1, sock2 = socket.socketpair()
    loop = asyncio.get_event_loop()
    reader1, writer1 = await asyncio.open_connection(sock=sock1)
    reader2, writer2 = await asyncio.open_connection(sock=sock2)
    yield reader1, writer1, reader2, writer2
    writer1.close()
    writer2.close()
    await writer1.wait_closed()
    await writer2.wait_closed()


@pytest.mark.asyncio
async def test_pack_and_read_payload(ipc_pair):
    reader1, writer1, reader2, writer2 = ipc_pair

    payload = json.dumps({"type": "test", "data": "hello"}).encode()
    header = pack_header(len(payload))
    writer1.write(header + payload)
    await writer1.drain()

    frame_type, received_payload = await read_payload(reader2)
    assert frame_type == 0
    assert received_payload == payload


@pytest.mark.asyncio
async def test_ipc_transport_emit_and_on_frame():
    """Test the full IPCTransport with a mock server-client pair."""
    # We'll create a server and client IPCTransport
    server = IPCTransport(listen_on="127.0.0.1:8768")
    client = IPCTransport(connect_to="127.0.0.1:8768")

    # Start server
    loop = asyncio.get_event_loop()
    await server.start()
    await client.start()

    # Register callback on server
    received_frames = []
    server.on_frame(lambda frame: received_frames.append(frame))

    # Send a frame from client
    test_frame = {"type": "test", "id": "abc"}
    client.emit(test_frame)

    # Wait for processing
    await asyncio.sleep(0.05)

    assert len(received_frames) == 1
    assert received_frames[0]["type"] == "test"
    assert received_frames[0]["id"] == "abc"

    # Cleanup
    await server.close()
    await client.close()


@pytest.mark.asyncio
async def test_ipc_transport_close():
    server = IPCTransport(listen_on="127.0.0.1:8769")
    client = IPCTransport(connect_to="127.0.0.1:8769")
    await server.start()
    await client.start()

    # Close client
    await client.close()
    # Trying to emit should return False
    assert client.emit({"type": "x"}) is False

    await server.close()