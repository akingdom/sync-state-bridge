#!/usr/bin/env python3
"""
Worker: 60 Hz game simulation. Emits deltas via SyncStateBridge over IPC.
"""

import asyncio
import json
import math
import random
import time
import os
import sys
import traceback
from typing import Dict, List, Any, Optional, Tuple

from sync_state import SyncStateBridge, AsyncIPCTransport

# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------
W, H = 1000, 800
PARTICLE_COUNT = 3000
ASTEROID_COUNT = 8
MAX_PLAYERS = 3
IPC_PORT = 8766
TICK = 1 / 60

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def dist(a, b):
    return math.hypot(a[0] - b[0], a[1] - b[1])

def clamp(v, lo, hi):
    return max(lo, min(hi, v))

def random_pos():
    return (random.uniform(0, W), random.uniform(0, H))

def random_vel(max_speed=2.0):
    angle = random.uniform(0, 2 * math.pi)
    speed = random.uniform(0.5, max_speed)
    return (speed * math.cos(angle), speed * math.sin(angle))

# ----------------------------------------------------------------------
# Entities
# ----------------------------------------------------------------------
class Asteroid:
    def __init__(self):
        self.id = f"ast_{id(self)}"
        self.x, self.y = random_pos()
        self.vx, self.vy = random_vel(1.5)
        self.radius = random.uniform(20, 40)

    def update(self):
        self.x += self.vx
        self.y += self.vy
        if self.x < 0 or self.x > W: self.vx = -self.vx; self.x = clamp(self.x, 0, W)
        if self.y < 0 or self.y > H: self.vy = -self.vy; self.y = clamp(self.y, 0, H)

    def to_dict(self):
        return {"id": self.id, "x": self.x, "y": self.y, "radius": self.radius}

class SwarmParticle:
    def __init__(self, pid: int):
        self.id = f"p{pid}"
        self.x, self.y = random_pos()
        self.vx, self.vy = random_vel(1.0)
        self.color = f"hsl({random.randint(0, 360)}, 80%, 60%)"
        self.radius = 2

    def update(self, asteroids: List[Asteroid], players: List['Player'], aggression: float):
        # Avoid asteroids
        for a in asteroids:
            dx = self.x - a.x
            dy = self.y - a.y
            d = math.hypot(dx, dy)
            if d < a.radius + 30 and d > 0:
                force = 0.5 / (d + 1)
                self.vx += dx / d * force
                self.vy += dy / d * force

        # Chase players if aggressive
        if aggression > 0 and players:
            nearest = min(players, key=lambda p: dist((p.x, p.y), (self.x, self.y)))
            dx = nearest.x - self.x
            dy = nearest.y - self.y
            d = math.hypot(dx, dy)
            if d > 0:
                force = aggression * 0.02
                self.vx += dx / d * force
                self.vy += dy / d * force

        self.vx *= 0.99
        self.vy *= 0.99
        speed = math.hypot(self.vx, self.vy)
        if speed > 3:
            self.vx = self.vx / speed * 3
            self.vy = self.vy / speed * 3

        self.x += self.vx
        self.y += self.vy
        # wrap
        if self.x < 0: self.x = W
        if self.x > W: self.x = 0
        if self.y < 0: self.y = H
        if self.y > H: self.y = 0

    def to_dict(self):
        return {"id": self.id, "x": self.x, "y": self.y, "color": self.color}

class Player:
    def __init__(self, pid: int, spawn_pos: Tuple[float, float]):
        self.id = f"player_{pid}"
        self.pid = pid
        self.x, self.y = spawn_pos
        self.vx = self.vy = 0.0
        self.angle = 0.0
        self.active = False
        self.last_fire = 0
        self.color = ["#ff4444", "#44ff44", "#4444ff"][pid]

    def update(self, keys: Dict[str, bool]):
        thrust = 0
        turn = 0
        if keys.get('up', False): thrust = 1
        if keys.get('down', False): thrust = -0.5
        if keys.get('left', False): turn = -0.05
        if keys.get('right', False): turn = 0.05

        self.angle += turn
        if thrust:
            self.vx += math.cos(self.angle) * thrust * 0.2
            self.vy += math.sin(self.angle) * thrust * 0.2
        self.vx *= 0.98
        self.vy *= 0.98
        speed = math.hypot(self.vx, self.vy)
        if speed > 4:
            self.vx = self.vx / speed * 4
            self.vy = self.vy / speed * 4
        self.x += self.vx
        self.y += self.vy
        if self.x < 0: self.x = W
        if self.x > W: self.x = 0
        if self.y < 0: self.y = H
        if self.y > H: self.y = 0
        if thrust != 0 or turn != 0:
            self.active = True

    def fire(self, game_state):
        if not self.active:
            self.active = True
        now = time.time()
        if now - self.last_fire < 0.15:
            return
        self.last_fire = now
        bx = self.x + math.cos(self.angle) * 20
        by = self.y + math.sin(self.angle) * 20
        bvx = math.cos(self.angle) * 8 + self.vx
        bvy = math.sin(self.angle) * 8 + self.vy
        game_state.bullets.append(Bullet(bx, by, bvx, bvy, self.pid))

    def to_dict(self):
        return {
            "id": self.id,
            "x": self.x, "y": self.y,
            "angle": self.angle,
            "active": self.active,
            "color": self.color
        }

class Bullet:
    def __init__(self, x, y, vx, vy, owner):
        self.id = f"bullet_{id(self)}"
        self.x, self.y = x, y
        self.vx, self.vy = vx, vy
        self.owner = owner
        self.alive = True

    def update(self):
        self.x += self.vx
        self.y += self.vy
        if self.x < 0 or self.x > W or self.y < 0 or self.y > H:
            self.alive = False

    def to_dict(self):
        return {"id": self.id, "x": self.x, "y": self.y, "owner": self.owner}

# ----------------------------------------------------------------------
# Game State
# ----------------------------------------------------------------------
class GameState:
    def __init__(self):
        self.particles = [SwarmParticle(i) for i in range(PARTICLE_COUNT)]
        self.asteroids = [Asteroid() for _ in range(ASTEROID_COUNT)]
        self.players: List[Player] = []
        self.bullets: List[Bullet] = []
        self.tick = 0
        self.aggression = 0.0
        self.spawn_positions = [(200, 400), (800, 400), (500, 100)]
        self.player_keys: Dict[int, Dict[str, bool]] = {}

    def add_player(self, pid: int):
        if pid >= MAX_PLAYERS:
            return False
        if any(p.pid == pid for p in self.players):
            return True
        p = Player(pid, self.spawn_positions[pid])
        self.players.append(p)
        self.player_keys[pid] = {'up': False, 'down': False, 'left': False, 'right': False}
        return True

    def set_keys(self, pid: int, keys: Dict[str, bool]):
        if pid in self.player_keys:
            self.player_keys[pid] = keys

    def fire(self, pid: int):
        for p in self.players:
            if p.pid == pid:
                p.fire(self)
                break

    def update(self):
        self.tick += 1
        for a in self.asteroids:
            a.update()
        for p in self.players:
            p.update(self.player_keys.get(p.pid, {}))
        for b in self.bullets:
            b.update()
        self.bullets = [b for b in self.bullets if b.alive]
        # Collision: bullets kill particles
        for b in self.bullets:
            for p in self.particles:
                if dist((b.x, b.y), (p.x, p.y)) < 5:
                    p.x, p.y = random_pos()
                    p.vx, p.vy = random_vel(1.0)
                    self.aggression = min(1.0, self.aggression + 0.01)
        self.aggression = max(0, self.aggression - 0.001)

    def get_entities(self):
        return {
            "particle": [p.to_dict() for p in self.particles],
            "asteroid": [a.to_dict() for a in self.asteroids],
            "player": [p.to_dict() for p in self.players],
            "bullet": [b.to_dict() for b in self.bullets],
        }

# ----------------------------------------------------------------------
# Connection helper
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

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
async def main():
    try:
        reader, writer = await connect_to_gateway()
    except Exception as e:
        print(f"[Worker] Fatal: {e}", file=sys.stderr)
        traceback.print_exc()
        return

    bridge = SyncStateBridge()
    transport = AsyncIPCTransport(writer, capacity=2000)
    bridge.register_transport(transport)

    game = GameState()

    async def read_commands():
        while True:
            try:
                line = await reader.readline()
                if not line:
                    break
                cmd = json.loads(line.decode().strip())
                action = cmd.get("action")
                params = cmd.get("params", {})
                if action == "join":
                    pid = params.get("pid")
                    game.add_player(pid)
                elif action == "move":
                    pid = params.get("pid")
                    keys = params.get("keys", {})
                    game.set_keys(pid, keys)
                elif action == "fire":
                    game.fire(params.get("pid"))
                elif action == "reset":
                    game.__init__()
                elif action == "spawn":
                    count = params.get("count", 100)
                    for _ in range(count):
                        p = SwarmParticle(len(game.particles))
                        game.particles.append(p)
                elif action == "clear":
                    game.particles = []
            except ConnectionResetError:
                print("[Worker] Connection lost (command reader).")
                break
            except Exception as e:
                print(f"[Worker] Command error: {e}", file=sys.stderr)
                traceback.print_exc()
                break

    asyncio.create_task(read_commands())

    try:
        while True:
            start = time.time()
            game.update()

            # Emit all entities
            for p in game.particles:
                bridge.track_change(p.id, "update", p.to_dict())
            for a in game.asteroids:
                bridge.track_change(a.id, "update", a.to_dict())
            for p in game.players:
                bridge.track_change(p.id, "update", p.to_dict())
            for b in game.bullets:
                bridge.track_change(b.id, "update", b.to_dict())

            bridge.commit_tick(game.tick)

            elapsed = time.time() - start
            if elapsed < TICK:
                await asyncio.sleep(TICK - elapsed)
    except KeyboardInterrupt:
        print("[Worker] Shutting down.")
    except Exception as e:
        print(f"[Worker] Unhandled exception in main loop: {e}", file=sys.stderr)
        traceback.print_exc()
    finally:
        bridge.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"[Worker] Fatal error: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
