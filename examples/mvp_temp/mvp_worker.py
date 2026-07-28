#!/usr/bin/env python3
"""
Worker – full game with correct deletion: get_deltas() sends deletes before filtering.
"""
import asyncio
import json
import math
import random
import time
import sys
import traceback
import struct
from typing import Dict, List, Tuple, Optional

# ----------------------------------------------------------------------
# Framing constants (MVP)
# ----------------------------------------------------------------------
MAGIC = b"SSB1"
VERSION = 0x01
FRAME_HEADER_SIZE = 12
FRAME_COMMAND = 4
FRAME_DELTA = 2

def pack_header(frame_type: int, payload_len: int, flags: int = 0) -> bytes:
    return struct.pack(">4sBBHI", MAGIC, VERSION, frame_type, flags, payload_len)

def unpack_header(data: bytes):
    magic, version, frame_type, flags, length = struct.unpack(">4sBBHI", data)
    if magic != MAGIC:
        raise ValueError("Invalid magic")
    if version != VERSION:
        raise ValueError(f"Unsupported version: {version}")
    return frame_type, flags, length

async def read_frame(reader: asyncio.StreamReader):
    try:
        header = await reader.readexactly(FRAME_HEADER_SIZE)
    except asyncio.IncompleteReadError:
        return None
    frame_type, flags, length = unpack_header(header)
    if length > 16 * 1024 * 1024:
        raise ValueError("Frame too large")
    payload = await reader.readexactly(length)
    return frame_type, payload

print("[Worker] Starting up...", file=sys.stderr)

# ----------------------------------------------------------------------
# Game constants (unchanged)
# ----------------------------------------------------------------------
W, H = 1000, 800
PARTICLE_COUNT = 300
MAX_PARTICLES = 1000
ASTEROID_COUNT = 8
MAX_ASTEROIDS = 12
MIN_ASTEROIDS = 6
MAX_PLAYERS = 3
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

# ----------------------------------------------------------------------
# Helpers (unchanged)
# ----------------------------------------------------------------------
def dist(a, b):
    return math.hypot(a[0] - b[0], a[1] - b[1])

def clamp(v, lo, hi):
    return max(lo, min(hi, v))

def random_pos():
    return (random.uniform(0, W), random.uniform(0, H))

def random_vel(min_speed=0.5, max_speed=2.0):
    angle = random.uniform(0, 2 * math.pi)
    speed = random.uniform(min_speed, max_speed)
    return (speed * math.cos(angle), speed * math.sin(angle))

def area(radius):
    return math.pi * radius * radius

def radius_from_area(area):
    return math.sqrt(area / math.pi)

# ----------------------------------------------------------------------
# Entities (unchanged)
# ----------------------------------------------------------------------
class Asteroid:
    _next_id = 0
    def __init__(self, x=None, y=None, radius=None, vx=None, vy=None):
        self.id = f"ast_{Asteroid._next_id}"
        Asteroid._next_id += 1
        self.x = x if x is not None else random.uniform(0, W)
        self.y = y if y is not None else random.uniform(0, H)
        self.radius = radius if radius is not None else random.uniform(20, 40)
        self.vx = vx if vx is not None else random.uniform(-1.5, 1.5)
        self.vy = vy if vy is not None else random.uniform(-1.5, 1.5)
        self.alive = True
        self.spawn_time = time.time()
        self.type = "asteroid"

    def update(self):
        if not self.alive:
            return
        self.x += self.vx
        self.y += self.vy
        if self.x < 0: self.x = W
        if self.x > W: self.x = 0
        if self.y < 0: self.y = H
        if self.y > H: self.y = 0

    def split(self) -> List['Asteroid']:
        if self.radius < ASTEROID_MIN_RADIUS * 2:
            self.alive = False
            return []
        self.alive = False
        num_pieces = random.randint(2, 3)
        total_area = area(self.radius)
        piece_radius = radius_from_area(total_area / num_pieces)
        new_asteroids = []
        for _ in range(num_pieces):
            angle = random.uniform(0, 2 * math.pi)
            scatter_speed = random.uniform(0.5, 2.0)
            scatter_vx = math.cos(angle) * scatter_speed
            scatter_vy = math.sin(angle) * scatter_speed
            base_vx = self.vx * 0.5 + scatter_vx
            base_vy = self.vy * 0.5 + scatter_vy
            if math.hypot(base_vx, base_vy) < 0.5:
                angle2 = random.uniform(0, 2 * math.pi)
                base_vx = math.cos(angle2) * 0.5
                base_vy = math.sin(angle2) * 0.5
            new_ast = Asteroid(
                x=self.x + random.uniform(-10, 10),
                y=self.y + random.uniform(-10, 10),
                radius=piece_radius,
                vx=base_vx,
                vy=base_vy
            )
            new_asteroids.append(new_ast)
        return new_asteroids

    def is_protected(self):
        return time.time() - self.spawn_time < ASTEROID_SPLIT_IMMUNITY

    def to_dict(self):
        return {"id": self.id, "x": self.x, "y": self.y, "radius": self.radius}


class SwarmParticle:
    def __init__(self, pid: int, x=None, y=None, color=None):
        self.id = f"p{pid}"
        self.x = x if x is not None else random_pos()[0]
        self.y = y if y is not None else random_pos()[1]
        self.vx, self.vy = random_vel(0.5, 2.0)
        self.color = color if color is not None else f"hsl({random.randint(0, 360)}, 80%, 60%)"
        self.radius = 2
        self.type = "particle"

    def update(self, asteroids: List[Asteroid], players: List['Player'], aggression: float):
        for a in asteroids:
            if not a.alive:
                continue
            dx = a.x - self.x
            dy = a.y - self.y
            d = math.hypot(dx, dy)
            if d > 0:
                force = GRAVITY_STRENGTH * a.radius / (d + 10)
                self.vx += dx / d * force
                self.vy += dy / d * force
        for a in asteroids:
            if not a.alive:
                continue
            dx = self.x - a.x
            dy = self.y - a.y
            d = math.hypot(dx, dy)
            if d < a.radius + 30 and d > 0:
                force = 1.0 / (d + 1)
                self.vx += dx / d * force
                self.vy += dy / d * force
        if aggression > 0 and players:
            nearest = min(players, key=lambda p: dist((p.x, p.y), (self.x, self.y)))
            dx = nearest.x - self.x
            dy = nearest.y - self.y
            d = math.hypot(dx, dy)
            if d > 0:
                force = aggression * 0.03
                self.vx += dx / d * force
                self.vy += dy / d * force
        self.vx *= 0.99
        self.vy *= 0.99
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
        self.render_size = SHIP_RENDER_RADIUS
        self.collision_radius = SHIP_COLLISION_RADIUS
        self.alive = True
        self.respawn_timer = 0
        self.type = "player"

    def update(self, keys: Dict[str, bool]):
        if not self.alive:
            return
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
        if speed > 5:
            self.vx = self.vx / speed * 5
            self.vy = self.vy / speed * 5
        self.x += self.vx
        self.y += self.vy
        if self.x < 0: self.x = W
        if self.x > W: self.x = 0
        if self.y < 0: self.y = H
        if self.y > H: self.y = 0
        if thrust != 0 or turn != 0:
            self.active = True

    def fire(self, game_state):
        if not self.alive:
            return
        if not self.active:
            self.active = True
        now = time.time()
        if now - self.last_fire < 0.15:
            return
        self.last_fire = now
        bx = self.x + math.cos(self.angle) * (self.render_size + 10)
        by = self.y + math.sin(self.angle) * (self.render_size + 10)
        bvx = math.cos(self.angle) * 10 + self.vx
        bvy = math.sin(self.angle) * 10 + self.vy
        game_state.bullets.append(Bullet(bx, by, bvx, bvy, self.pid))

    def respawn(self, spawn_pos):
        self.x, self.y = spawn_pos
        self.vx = self.vy = 0.0
        self.angle = 0.0
        self.alive = True
        self.active = False
        self.respawn_timer = 0

    def to_dict(self):
        return {
            "id": self.id,
            "x": self.x, "y": self.y,
            "angle": self.angle,
            "active": self.active,
            "alive": self.alive,
            "color": self.color,
            "render_size": self.render_size if self.alive else 0
        }


class Bullet:
    def __init__(self, x, y, vx, vy, owner):
        self.id = f"bullet_{id(self)}"
        self.x, self.y = x, y
        self.vx, self.vy = vx, vy
        self.owner = owner
        self.alive = True
        self.radius = 4
        self.start_x = x
        self.start_y = y
        self.dist_traveled = 0
        self.type = "bullet"

    def update(self):
        if not self.alive:
            return
        self.x += self.vx
        self.y += self.vy
        self.dist_traveled += math.hypot(self.vx, self.vy)
        if (self.x < 0 or self.x > W or self.y < 0 or self.y > H or
            self.dist_traveled > BULLET_MAX_DIST):
            self.alive = False

    def to_dict(self):
        return {"id": self.id, "x": self.x, "y": self.y, "owner": self.owner}


# ----------------------------------------------------------------------
# GameState – correct deletion: get_deltas() filters after sending deletes
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
        self.next_particle_id = PARTICLE_COUNT
        self.spawn_timer = 0

    def add_player(self, pid: int):
        if pid >= MAX_PLAYERS:
            return False
        self.players = [p for p in self.players if p.pid != pid]
        spawn_pos = self.spawn_positions[pid]
        for attempt in range(10):
            safe = True
            for a in self.asteroids:
                if a.alive and dist(spawn_pos, (a.x, a.y)) < (a.radius + SHIP_COLLISION_RADIUS + 10):
                    safe = False
                    break
            if safe:
                break
            spawn_pos = (spawn_pos[0] + random.uniform(-50, 50),
                         spawn_pos[1] + random.uniform(-50, 50))
            spawn_pos = (clamp(spawn_pos[0], 20, W-20),
                         clamp(spawn_pos[1], 20, H-20))
        p = Player(pid, spawn_pos)
        self.players.append(p)
        self.player_keys[pid] = {'up': False, 'down': False, 'left': False, 'right': False}
        print(f"[Worker] Player {pid} joined")
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

        # Update all entities
        for a in self.asteroids:
            a.update()
        for p in self.particles:
            p.update(self.asteroids, self.players, self.aggression)
        for p in self.players:
            if not p.alive:
                p.respawn_timer += TICK
                if p.respawn_timer > SHIP_RESPAWN_DELAY:
                    spawn_pos = self.spawn_positions[p.pid]
                    for attempt in range(10):
                        safe = True
                        for a in self.asteroids:
                            if a.alive and dist(spawn_pos, (a.x, a.y)) < (a.radius + SHIP_COLLISION_RADIUS + 10):
                                safe = False
                                break
                        if safe:
                            break
                        spawn_pos = (spawn_pos[0] + random.uniform(-50, 50),
                                     spawn_pos[1] + random.uniform(-50, 50))
                        spawn_pos = (clamp(spawn_pos[0], 20, W-20),
                                     clamp(spawn_pos[1], 20, H-20))
                    p.respawn(spawn_pos)
                continue
            p.update(self.player_keys.get(p.pid, {}))
        for b in self.bullets:
            b.update()

        # Remove very slow asteroids (fallback)
        for a in self.asteroids:
            if a.alive and time.time() - a.spawn_time > SLOW_ASTEROID_TIMEOUT and math.hypot(a.vx, a.vy) < 0.1:
                a.alive = False

        # Helper to spawn particles
        def spawn_particles(x, y, count=2):
            for _ in range(count):
                p = SwarmParticle(
                    self.next_particle_id,
                    x=x + random.uniform(-6, 6),
                    y=y + random.uniform(-6, 6),
                    color=f"hsl({random.randint(0,360)}, 60%, 40%)"
                )
                self.particles.append(p)
                self.next_particle_id += 1

        # ---- Collision detection ----
        new_asteroids = []

        # Asteroid vs Asteroid
        for i in range(len(self.asteroids)):
            a1 = self.asteroids[i]
            if not a1.alive or a1.is_protected():
                continue
            for j in range(i + 1, len(self.asteroids)):
                a2 = self.asteroids[j]
                if not a2.alive or a2.is_protected():
                    continue
                if dist((a1.x, a1.y), (a2.x, a2.y)) < (a1.radius + a2.radius):
                    if a1.radius < ASTEROID_MIN_RADIUS * 2 and a2.radius < ASTEROID_MIN_RADIUS * 2:
                        a1.alive = False
                        a2.alive = False
                        spawn_particles(a1.x, a1.y)
                        spawn_particles(a2.x, a2.y)
                    else:
                        new_asteroids.extend(a1.split())
                        new_asteroids.extend(a2.split())

        # Asteroid vs Bullet
        for b in self.bullets:
            if not b.alive:
                continue
            for a in self.asteroids:
                if not a.alive or a.is_protected():
                    continue
                if dist((b.x, b.y), (a.x, a.y)) < (a.radius + b.radius):
                    if a.radius >= ASTEROID_MIN_RADIUS * 2:
                        new_asteroids.extend(a.split())
                    else:
                        a.alive = False
                        spawn_particles(a.x, a.y)
                    b.alive = False
                    break

        # Asteroid vs Ship
        for a in self.asteroids:
            if not a.alive or a.is_protected():
                continue
            for p in self.players:
                if not p.alive:
                    continue
                if dist((a.x, a.y), (p.x, p.y)) < (a.radius + p.collision_radius):
                    p.alive = False
                    p.respawn_timer = 0
                    print(f"[Worker] Player {p.pid} killed by asteroid")
                    break

        # Add new asteroids
        self.asteroids.extend(new_asteroids)

        # ---- DO NOT REMOVE DEAD ENTITIES HERE ----
        # They will be removed in get_deltas() after sending delete messages.

        # Cap particles
        if len(self.particles) > MAX_PARTICLES:
            excess = len(self.particles) - MAX_PARTICLES
            del self.particles[:excess]

        # Spawn new asteroids if needed
        if len(self.asteroids) < MIN_ASTEROIDS:
            self.spawn_timer += TICK
            if self.spawn_timer > ASTEROID_SPAWN_INTERVAL:
                self.spawn_timer = 0
                to_spawn = min(MAX_ASTEROIDS - len(self.asteroids), random.randint(1, 2))
                for _ in range(to_spawn):
                    side = random.choice(['top', 'bottom', 'left', 'right'])
                    if side == 'top':
                        x = random.uniform(0, W); y = 0
                        angle = random.uniform(0, math.pi)
                    elif side == 'bottom':
                        x = random.uniform(0, W); y = H
                        angle = random.uniform(math.pi, 2*math.pi)
                    elif side == 'left':
                        x = 0; y = random.uniform(0, H)
                        angle = random.uniform(-math.pi/2, math.pi/2)
                    else:
                        x = W; y = random.uniform(0, H)
                        angle = random.uniform(math.pi/2, 3*math.pi/2)
                    speed = random.uniform(0.5, 1.5)
                    vx = math.cos(angle) * speed
                    vy = math.sin(angle) * speed
                    new_ast = Asteroid(
                        x=x, y=y,
                        radius=random.uniform(30, 50),
                        vx=vx, vy=vy
                    )
                    self.asteroids.append(new_ast)

        self.aggression = max(0, self.aggression - 0.001)

    # ------------------------------------------------------------------
    # get_deltas() – sends delete for dead entities, THEN removes them
    # ------------------------------------------------------------------
    def get_deltas(self):
        deltas = []

        # Particles: always update
        for p in self.particles:
            deltas.append({"id": p.id, "op": "update", "type": p.type, "changes": p.to_dict()})

        # Asteroids: send delete if dead, update if alive
        dead_asteroids = []
        for a in self.asteroids:
            if a.alive:
                deltas.append({"id": a.id, "op": "update", "type": a.type, "changes": a.to_dict()})
            else:
                dead_asteroids.append(a.id)
                deltas.append({"id": a.id, "op": "delete", "type": a.type})
        if dead_asteroids:
            print(f"[Worker] Deleting asteroids: {dead_asteroids}")
        # Remove dead asteroids
        self.asteroids = [a for a in self.asteroids if a.alive]

        # Players: delete if dead, update if alive
        for p in self.players:
            if p.alive:
                deltas.append({"id": p.id, "op": "update", "type": p.type, "changes": p.to_dict()})
            else:
                deltas.append({"id": p.id, "op": "delete", "type": p.type})

        # Bullets: delete if dead, update if alive
        dead_bullets = []
        for b in self.bullets:
            if b.alive:
                deltas.append({"id": b.id, "op": "update", "type": b.type, "changes": b.to_dict()})
            else:
                dead_bullets.append(b.id)
                deltas.append({"id": b.id, "op": "delete", "type": b.type})
        if dead_bullets:
            print(f"[Worker] Deleting bullets: {dead_bullets}")
        self.bullets = [b for b in self.bullets if b.alive]

        return deltas


# ----------------------------------------------------------------------
# Main
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
        while True:
            try:
                frame = await read_frame(reader)
                if frame is None:
                    print("[Worker] Connection closed or read error", file=sys.stderr)
                    break
                frame_type, payload = frame
                if frame_type == FRAME_COMMAND:
                    line = payload.decode('utf-8').strip()
                    if not line:
                        continue
                    try:
                        cmd = json.loads(line)
                    except json.JSONDecodeError as e:
                        print(f"[Worker] Invalid JSON: {e}")
                        continue
                    action = cmd.get("action")
                    params = cmd.get("params", {})
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
                        for lst in (game.particles, game.asteroids, game.players, game.bullets):
                            for e in lst:
                                e.alive = False
                        # Force send deletes and reinit
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
                # ignore other frame types
            except Exception as e:
                print(f"[Worker] Command loop error: {e}", file=sys.stderr)
                traceback.print_exc()
                break

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
        print(f"[Worker] Main loop error: {e}", file=sys.stderr)
        traceback.print_exc()
    finally:
        writer.close()
        await writer.wait_closed()
        print("[Worker] Connection closed.", file=sys.stderr)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"[Worker] Fatal: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)