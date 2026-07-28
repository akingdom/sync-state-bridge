#!/usr/bin/env python3
"""
Advanced Demo Worker – framed commands and deltas.
"""
import asyncio
import json
import math
import random
import time
import sys
import traceback
import struct
from typing import Dict, List, Any, Tuple, Optional

# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------
W, H = 1000, 800
PARTICLE_COUNT = 300
MAX_PARTICLES = 1000
ASTEROID_COUNT = 8
MAX_ASTEROIDS = 12
MIN_ASTEROIDS = 6
MAX_PLAYERS = 3
IPC_PORT = 8766
TICK = 1 / 60
GRAVITY_STRENGTH = 0.0005
BULLET_MAX_DIST = 1000
ASTEROID_MIN_RADIUS = 5
SHIP_RESPAWN_DELAY = 2.0
SHIP_RENDER_RADIUS = 20
SHIP_COLLISION_RADIUS = 10
ASTEROID_SPLIT_IMMUNITY = 2.5
ASTEROID_SPAWN_INTERVAL = 2.0
SLOW_ASTEROID_TIMEOUT = 10.0

# Frame types
FRAME_DELTA = 2
FRAME_COMMAND = 4
FRAME_HEADER_SIZE = 12
MAGIC = b"SSB1"
VERSION = 0x01

def pack_header(frame_type: int, payload_len: int, flags: int = 0) -> bytes:
    return struct.pack(">4sBBHI", MAGIC, VERSION, frame_type, flags, payload_len)

def unpack_header(data: bytes) -> Tuple[int, int, int]:
    magic, version, frame_type, flags, length = struct.unpack(">4sBBHI", data)
    if magic != MAGIC:
        raise ValueError(f"Invalid magic: {magic}")
    if version != VERSION:
        raise ValueError(f"Unsupported version: {version}")
    return frame_type, flags, length

async def read_frame(reader: asyncio.StreamReader) -> Optional[bytes]:
    try:
        header = await asyncio.wait_for(reader.readexactly(FRAME_HEADER_SIZE), timeout=5.0)
    except asyncio.IncompleteReadError:
        return None
    except asyncio.TimeoutError:
        return None
    frame_type, flags, length = unpack_header(header)
    if length > 16 * 1024 * 1024:  # 16 MB limit
        raise ValueError(f"Frame length {length} exceeds 16 MB limit")
    payload = await reader.readexactly(length)
    return header + payload

async def read_payload(reader: asyncio.StreamReader) -> Tuple[int, bytes]:
    frame = await read_frame(reader)
    if frame is None:
        return None, None
    frame_type, _, _ = unpack_header(frame[:FRAME_HEADER_SIZE])
    return frame_type, frame[FRAME_HEADER_SIZE:]

print("[Worker] Starting up...", file=sys.stderr)

# ----------------------------------------------------------------------
# Helpers and entities (same as before)
# To save space, we omit them here but you must include the full
# definitions of Asteroid, SwarmParticle, Player, Bullet, GameState.
# They are unchanged from the previous complete worker.
# ----------------------------------------------------------------------
# [INSERT ALL CLASSES HERE – copy from earlier full worker]
# ----------------------------------------------------------------------

async def connect_to_gateway(host='127.0.0.1', port=8766, max_attempts=20):
    for attempt in range(max_attempts):
        try:
            reader, writer = await asyncio.open_connection(host, port)
            print(f"[Worker] Connected to gateway (attempt {attempt+1})")
            return reader, writer
        except Exception as e:
            delay = min(2 ** attempt, 10)
            print(f"[Worker] Connection attempt {attempt+1} failed: {e}. Retrying in {delay}s...")
            await asyncio.sleep(delay)
    raise RuntimeError("Could not connect to gateway after multiple attempts")

async def send_delta(writer, payload: dict):
    json_bytes = json.dumps(payload).encode('utf-8')
    header = pack_header(FRAME_DELTA, len(json_bytes), 0)
    writer.write(header + json_bytes)
    await writer.drain()

async def main():
    print("[Worker] Initializing...", file=sys.stderr)
    try:
        reader, writer = await connect_to_gateway()
    except Exception as e:
        print(f"[Worker] Fatal: {e}", file=sys.stderr)
        traceback.print_exc()
        return

    game = GameState()
    print("[Worker] Game started.", file=sys.stderr)

    async def read_commands():
        print("[Worker] Command loop started (framed commands)")
        while True:
            try:
                frame_type, payload = await read_payload(reader)
                if frame_type is None:
                    print("[Worker] Connection closed or timeout", file=sys.stderr)
                    break
                if frame_type == FRAME_COMMAND:
                    line = payload.decode('utf-8').strip()
                    if not line:
                        continue
                    print(f"[Worker] Received command frame: {line}")
                    try:
                        cmd = json.loads(line)
                    except json.JSONDecodeError as e:
                        print(f"[Worker] Invalid JSON: {e}")
                        continue
                    action = cmd.get("action")
                    params = cmd.get("params", {})
                    print(f"[Worker] Received command: {action} {params}")
                    if action == "join":
                        pid = params.get("pid")
                        if pid is not None:
                            game.add_player(pid)
                    elif action == "move":
                        pid = params.get("pid")
                        keys = params.get("keys", {})
                        if pid is not None:
                            game.set_keys(pid, keys)
                    elif action == "fire":
                        pid = params.get("pid")
                        if pid is not None:
                            game.fire(pid)
                    elif action == "reset":
                        old_particles = game.particles
                        old_asteroids = game.asteroids
                        old_players = game.players
                        old_bullets = game.bullets
                        for e in (old_particles + old_asteroids + old_players + old_bullets):
                            e.alive = False
                        game.get_deltas()
                        game.__init__()
                        print("[Worker] Reset complete")
                    elif action == "spawn":
                        count = params.get("count", 100)
                        for _ in range(count):
                            p = SwarmParticle(game.next_particle_id)
                            game.particles.append(p)
                            game.next_particle_id += 1
                    elif action == "clear":
                        game.particles = []
                    elif action == "ping":
                        # ignore
                        pass
                else:
                    # Ignore other frame types (e.g., if we ever receive deltas back)
                    pass
            except Exception as e:
                print(f"[Worker] Unexpected error in command loop: {e}", file=sys.stderr)
                traceback.print_exc()
                continue

    asyncio.create_task(read_commands())

    try:
        while True:
            start = time.time()
            game.update()
            deltas = game.get_deltas()
            payload = {
                "type": "DELTA",
                "tick": game.tick,
                "deltas": deltas
            }
            await send_delta(writer, payload)
            elapsed = time.time() - start
            if elapsed < TICK:
                await asyncio.sleep(TICK - elapsed)
    except KeyboardInterrupt:
        print("[Worker] Shutting down.", file=sys.stderr)
    except Exception as e:
        print(f"[Worker] Unhandled exception in main loop: {e}", file=sys.stderr)
        traceback.print_exc()
    finally:
        writer.close()
        await writer.wait_closed()
        print("[Worker] Connection closed.", file=sys.stderr)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"[Worker] Fatal error: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)