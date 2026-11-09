const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const VERSION = 1;
const BASE_WINDOW = 150;
const WINDOW_PER_SEC = 60;
const MAX_QUEUE_SECONDS = 30;

let queue = [];
const rooms = new Map();
let nextRoom = 1;

const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, v: VERSION, online: wss.clients.size, queue: queue.length, rooms: rooms.size }));
    return;
  }
  res.writeHead(404); res.end();
});
const wss = new WebSocketServer({ server, maxPayload: 4096 });

function send(ws, obj) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function clampInt(v, lo, hi, d) { v = parseInt(v, 10); return Number.isFinite(v) ? Math.max(lo, Math.min(hi, v)) : d; }

wss.on('connection', (ws) => {
  ws.isAlive = true; ws.room = null;
  ws.on('pong', () => { ws.isAlive = true; });
  send(ws, { t: 'hello', v: VERSION });

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    switch (m.t) {
      case 'join': join(ws, m); break;
      case 'cancel': removeFromQueue(ws); break;
      case 'ev': relay(ws, m); break;
      case 'leave': leaveRoom(ws); break;
      case 'ping': send(ws, { t: 'pong' }); break;
    }
  });
  ws.on('close', () => { removeFromQueue(ws); leaveRoom(ws); });
  ws.on('error', () => {});
});

function join(ws, m) {
  removeFromQueue(ws); leaveRoom(ws);
  queue.push({
    ws,
    name: String(m.name || 'Player').replace(/[^\w \-\.]/g, '').slice(0, 16) || 'Player',
    avatar: clampInt(m.avatar, 0, 7, 0),
    pts: clampInt(m.pts, 0, 100000, 0),
    since: Date.now(),
  });
  send(ws, { t: 'waiting' });
}

function removeFromQueue(ws) { queue = queue.filter((q) => q.ws !== ws); }

function relay(ws, m) {
  const room = ws.room; if (!room) return;
  const other = room.a === ws ? room.b : room.a;
  send(other, { t: 'ev', d: m.d });
}

function leaveRoom(ws) {
  const room = ws.room; if (!room) return;
  rooms.delete(room.id);
  const other = room.a === ws ? room.b : room.a;
  ws.room = null;
  if (other) { other.room = null; send(other, { t: 'oppleft' }); }
}

function matchLoop() {
  const now = Date.now();
  queue = queue.filter((q) => q.ws.readyState === 1 && now - q.since < MAX_QUEUE_SECONDS * 1000);
  queue.sort((x, y) => x.since - y.since);
  for (let i = 0; i < queue.length; i++) {
    const a = queue[i]; if (!a) continue;
    let best = -1, bestDiff = Infinity;
    for (let j = i + 1; j < queue.length; j++) {
      const b = queue[j]; if (!b) continue;
      const waited = (now - Math.min(a.since, b.since)) / 1000;
      const win = BASE_WINDOW + WINDOW_PER_SEC * waited;
      const diff = Math.abs(a.pts - b.pts);
      if (diff <= win && diff < bestDiff) { best = j; bestDiff = diff; }
    }
    if (best >= 0) {
      const b = queue[best];
      queue[i] = null; queue[best] = null;
      startMatch(a, b);
    }
  }
  queue = queue.filter(Boolean);
}

function startMatch(a, b) {
  const room = { id: nextRoom++, a: a.ws, b: b.ws };
  rooms.set(room.id, room);
  a.ws.room = room; b.ws.room = room;
  const seed = 1 + Math.floor(Math.random() * 2000000000);
  send(a.ws, { t: 'match', room: room.id, seed, you: 0, opp: { name: b.name, avatar: b.avatar, pts: b.pts } });
  send(b.ws, { t: 'match', room: room.id, seed, you: 1, opp: { name: a.name, avatar: a.avatar, pts: a.pts } });
}

setInterval(matchLoop, 500);
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false; try { ws.ping(); } catch (e) {}
  });
}, 25000);

server.listen(PORT, () => console.log('Sudoku Blitz server on :' + PORT));
