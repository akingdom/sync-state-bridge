#!/usr/bin/env python3
"""
Self-contained test: runs a server and a client in the same asyncio loop.
No external dependencies, no process separation.
"""
import asyncio
import json
import struct
import socket

# --- Framing constants ---
MAGIC = b"SSB1"
VERSION = 0x01
FRAME_HEADER_SIZE = 12
FRAME_COMMAND = 4

def pack_header(frame_type: int, payload_len: int, flags: int = 0) -> bytes:
    return struct.pack(">4sBBHI", MAGIC, VERSION, frame_type, flags, payload_len)

def unpack_header(data: bytes):
    magic, version, frame_type, flags, length = struct.unpack(">4sBBHI", data)
    if magic != MAGIC:
        raise ValueError("Invalid magic")
    if version != VERSION:
        raise ValueError("Unsupported version")
    return frame_type, flags, length

async def read_frame(reader):
    try:
        header = await reader.readexactly(FRAME_HEADER_SIZE)
    except asyncio.IncompleteReadError:
        return None
    frame_type, flags, length = unpack_header(header)
    if length > 1024 * 1024:
        raise ValueError("Frame too large")
    payload = await reader.readexactly(length)
    return frame_type, payload

async def send_command(writer, action: str, params: dict = None):
    cmd = {"action": action, "params": params or {}}
    json_bytes = json.dumps(cmd).encode('utf-8')
    header = pack_header(FRAME_COMMAND, len(json_bytes))
    writer.write(header + json_bytes)
    await writer.drain()

# --- Server handler ---
async def handle_client(reader, writer):
    print("Server: client connected")
    try:
        while True:
            frame = await read_frame(reader)
            if frame is None:
                break
            frame_type, payload = frame
            if frame_type == FRAME_COMMAND:
                text = payload.decode('utf-8')
                print(f"Server received: {text}")
                # Echo back as a new command
                await send_command(writer, "echo", {"original": text})
            else:
                print(f"Server: unknown frame type {frame_type}")
    except Exception as e:
        print(f"Server error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()
        print("Server: client disconnected")

async def server():
    server = await asyncio.start_server(handle_client, '127.0.0.1', 8766)
    print("Server listening on 8766")
    async with server:
        await server.serve_forever()

# --- Client ---
async def client():
    # Wait a moment for server to start
    await asyncio.sleep(0.1)
    try:
        reader, writer = await asyncio.open_connection('127.0.0.1', 8766)
        print("Client connected")
        # Send a test command
        await send_command(writer, "test", {"hello": "world"})
        print("Client sent test command")
        # Wait for response
        frame = await read_frame(reader)
        if frame:
            frame_type, payload = frame
            if frame_type == FRAME_COMMAND:
                text = payload.decode('utf-8')
                print(f"Client received: {text}")
            else:
                print(f"Client: unexpected frame type {frame_type}")
        writer.close()
        await writer.wait_closed()
        print("Client done")
    except Exception as e:
        print(f"Client error: {e}")

async def main():
    # Start server and client concurrently
    server_task = asyncio.create_task(server())
    # Give server a moment to start
    await asyncio.sleep(0.2)
    client_task = asyncio.create_task(client())
    await client_task
    # Cancel server after client finishes
    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    asyncio.run(main())