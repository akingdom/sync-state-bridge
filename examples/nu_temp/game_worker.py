# filename: game_worker.py
#!/usr/bin/env python3
"""
Game Worker – uses StateSync for authoritative state management.
Adapts particle/asteroid counts based on Governor recommendations.
"""

import asyncio
import json
import math
import random
import time
import sys
import traceback
import urllib.request
from typing import Dict, List, Tuple, Optional, Any

from sync_state.core import StateSync
from sync_state.core.ipc_transport import IPCTransport, read_payload, pack_header, FRAME_DELTA, FRAME_COMMAND

# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------
W, H = 1000, 800
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

current_max_particles = 300
current_max_asteroids = 20
current_max_bullets = 50

print("[GameWorker] Starting...", file=sys.stderr)

# ----------------------------------------------------------------------
# Helpers
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
# Entities
# ----------------------------------------------------------------------
class Asteroid:
    _next_id = 0
    def __init__(self, x=None, y=None, radius=None, vx=None, vy=None):
        self.id = f"ast_{Asteroid._next_id}"
        Asteroid._next_id = (Asteroid._next_id + 1) % 1_000_000
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

    def update(self, asteroids, players, aggression):
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
            dx_rep = self.x - a.x
            dy_rep = self.y - a.y
            d_rep = math.hypot(dx_rep, dy_rep)
            if d_rep < a.radius + 30 and d_rep > 0:
                force = 1.0 / (d_rep + 1)
                self.vx += dx_rep / d_rep * force
                self.vy += dy_rep / d_rep * force
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
        self.color = ["#ff4444", "#44ff44", "#4444ff"][pid % 3]
        self.render_size = SHIP_RENDER_RADIUS
        self.collision_radius = SHIP_COLLISION_RADIUS
        self.alive = True
        self.respawn_timer = 0
        self.type = "player"

    def update(self, keys):
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
    _next_id = 0
    def __init__(self, x, y, vx, vy, owner):
        self.id = f"bullet_{Bullet._next_id}"
        Bullet._next_id = (Bullet._next_id + 1) % 100_000
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
# Game State using StateSync
# ----------------------------------------------------------------------
class GameState:
    def __init__(self, state_sync: StateSync):
        self.state_sync = state_sync
        self.particles: List[SwarmParticle] = []
        self.asteroids: List[Asteroid] = []
        self.players: List[Player] = []
        self.bullets: List[Bullet] = []
        self.tick = 0
        self.aggression = 0.0
        self.spawn_positions = [(200, 400), (800, 400), (500, 100)]
        self.player_keys: Dict[int, Dict[str, bool]] = {}
        self.next_particle_id = 0
        self.spawn_timer = 0
        self._init_entities()

    def _init_entities(self):
        self.particles = [SwarmParticle(i) for i in range(current_max_particles)]
        self.asteroids = [Asteroid() for _ in range(min(current_max_asteroids, 12))]
        self.next_particle_id = current_max_particles
        self.players = []
        self.bullets = []
        self.player_keys = {}

    def apply_governor_recommendations(self, rec: Dict[str, Any]):
        global current_max_particles, current_max_asteroids
        new_max_particles = rec.get("max_particles", current_max_particles)
        new_max_asteroids = rec.get("max_asteroids", current_max_asteroids)
        current_max_particles = new_max_particles
        current_max_asteroids = new_max_asteroids

        if len(self.particles) > new_max_particles:
            self.particles = self.particles[:new_max_particles]
        elif len(self.particles) < new_max_particles and new_max_particles <= 300:
            for _ in range(len(self.particles), new_max_particles):
                self.particles.append(SwarmParticle(self.next_particle_id))
                self.next_particle_id = (self.next_particle_id + 1) % 1_000_000

        if len(self.asteroids) > new_max_asteroids:
            self.asteroids = [a for a in self.asteroids if a.alive][:new_max_asteroids]

    def get_asteroids(self): return [a.to_dict() for a in self.asteroids if a.alive]
    def get_particles(self): return [p.to_dict() for p in self.particles]
    def get_players(self): return [p.to_dict() for p in self.players if p.alive]
    def get_bullets(self): return [b.to_dict() for b in self.bullets if b.alive]

    def handle_controls(self, pid: int, keys: Dict[str, bool]):
        if pid not in self.player_keys:
            self._join_player(pid)
        self.player_keys[pid] = keys
        if keys.get('fire', False):
            for p in self.players:
                if p.pid == pid:
                    p.fire(self)

    def _join_player(self, pid: int):
        if pid >= MAX_PLAYERS:
            return
        self.players = [p for p in self.players if p.pid != pid]
        spawn_pos = self.spawn_positions[pid % len(self.spawn_positions)]
        p = Player(pid, spawn_pos)
        self.players.append(p)
        self.player_keys[pid] = {'up': False, 'down': False, 'left': False, 'right': False, 'fire': False}
        print(f"[GameWorker] Player {pid} joined", file=sys.stderr)

    def handle_reset(self):
        self._init_entities()
        self.spawn_timer = 0
        self.tick = 0
        self.aggression = 0.0
        print("[GameWorker] Game reset successfully", file=sys.stderr)

    def handle_spawn(self, count: int):
        for _ in range(count):
            p = SwarmParticle(self.next_particle_id)
            self.particles.append(p)
            self.next_particle_id = (self.next_particle_id + 1) % 1_000_000

    def handle_clear(self):
        self.particles.clear()

    def update(self):
        self.tick += 1
        for a in self.asteroids: a.update()
        for p in self.particles: p.update(self.asteroids, self.players, self.aggression)
        for p in self.players:
            if not p.alive:
                p.respawn_timer += TICK
                if p.respawn_timer > SHIP_RESPAWN_DELAY:
                    p.respawn(self.spawn_positions[p.pid % len(self.spawn_positions)])
                continue
            p.update(self.player_keys.get(p.pid, {}))
        for b in self.bullets: b.update()

        new_asteroids = []
        for i in range(len(self.asteroids)):
            a1 = self.asteroids[i]
            if not a1.alive or a1.is_protected(): continue
            for j in range(i + 1, len(self.asteroids)):
                a2 = self.asteroids[j]
                if not a2.alive or a2.is_protected(): continue
                if dist((a1.x, a1.y), (a2.x, a2.y)) < (a1.radius + a2.radius):
                    new_asteroids.extend(a1.split())
                    new_asteroids.extend(a2.split())

        for b in self.bullets:
            if not b.alive: continue
            for a in self.asteroids:
                if not a.alive or a.is_protected(): continue
                if dist((b.x, b.y), (a.x, a.y)) < (a.radius + b.radius):
                    new_asteroids.extend(a.split())
                    b.alive = False
                    break

        for a in self.asteroids:
            if not a.alive or a.is_protected(): continue
            for p in self.players:
                if not p.alive: continue
                if dist((a.x, a.y), (p.x, p.y)) < (a.radius + p.collision_radius):
                    p.alive = False
                    p.respawn_timer = 0
                    break

        self.asteroids.extend(new_asteroids)
        self.asteroids = [a for a in self.asteroids if a.alive]
        self.bullets = [b for b in self.bullets if b.alive]

        if len(self.particles) > current_max_particles:
            del self.particles[current_max_particles:]

        self.state_sync.mark_dirty("asteroid")
        self.state_sync.mark_dirty("particle")
        self.state_sync.mark_dirty("player")
        self.state_sync.mark_dirty("bullet")

# ----------------------------------------------------------------------
# Async Helper
# ----------------------------------------------------------------------
async def send_delta(writer, delta_payload: dict):
    json_bytes = json.dumps(delta_payload).encode('utf-8')
    header = pack_header(len(json_bytes), 0)
    writer.write(header + json_bytes)
    await writer.drain()

async def fetch_governor_stats(game: GameState):
    loop = asyncio.get_running_loop()
    def _poll():
        try:
            req = urllib.request.Request("http://127.0.0.1:8000/stats")
            with urllib.request.urlopen(req, timeout=1.0) as resp:
                if resp.status == 200:
                    return json.loads(resp.read().decode())
        except Exception:
            return None

    while True:
        await asyncio.sleep(2.0)
        rec = await loop.run_in_executor(None, _poll)
        if rec:
            game.apply_governor_recommendations(rec)

async def connect_to_gateway(host='127.0.0.1', port=8766, max_attempts=20):
    for attempt in range(max_attempts):
        try:
            reader, writer = await asyncio.open_connection(host, port)
            print(f"[GameWorker] Connected to gateway (attempt {attempt+1})", file=sys.stderr)
            return reader, writer
        except Exception as e:
            delay = min(2 ** attempt, 10)
            await asyncio.sleep(delay)
    raise RuntimeError("Could not connect to gateway")

async def main():
    reader, writer = await connect_to_gateway()
    state_sync = StateSync()
    game = GameState(state_sync)

    state_sync.register_snapshot_provider("asteroid", game.get_asteroids)
    state_sync.register_snapshot_provider("particle", game.get_particles)
    state_sync.register_snapshot_provider("player", game.get_players)
    state_sync.register_snapshot_provider("bullet", game.get_bullets)

    frame_queue: asyncio.Queue = asyncio.Queue()

    async def read_frames():
        while True:
            try:
                frame_type, payload = await read_payload(reader)
                if frame_type is None: break
                if frame_type == FRAME_DELTA: continue
                data = json.loads(payload.decode())
                await frame_queue.put(data)
            except Exception:
                break

    asyncio.create_task(read_frames())
    asyncio.create_task(fetch_governor_stats(game))

    try:
        while True:
            start = time.time()
            while not frame_queue.empty():
                frame = await frame_queue.get()
                ftype = frame.get("type")
                if ftype == "controls":
                    raw_id = frame.get("id", "player_0")
                    pid = int(raw_id.split("_")[-1]) if "_" in raw_id else 0
                    game.handle_controls(pid, frame.get("data", {}))
                elif ftype == "game:reset":
                    game.handle_reset()
                elif ftype == "game:spawn":
                    game.handle_spawn(frame.get("params", {}).get("count", 100))
                elif ftype == "game:clear":
                    game.handle_clear()

            game.update()
            await state_sync.commit()

            for type_name in ["asteroid", "player", "bullet", "particle"]:
                delta = state_sync.get_last_delta(type_name)
                if delta and (delta["added"] or delta["updated"] or delta["deleted"]):
                    await send_delta(writer, delta)

            elapsed = time.time() - start
            if elapsed < TICK:
                await asyncio.sleep(TICK - elapsed)
    finally:
        writer.close()
        await writer.wait_closed()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"[GameWorker] Fatal error: {e}", file=sys.stderr)
        sys.exit(1)