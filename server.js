// Tabard Clash relay: tiny WebSocket room server. Passes JSON messages between players in the same room.
const http = require('http');
const { WebSocketServer } = require('ws');

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain', 'access-control-allow-origin': '*' });
  res.end('tabard relay ok');
});
const wss = new WebSocketServer({ server, maxPayload: 16384 });
const rooms = new Map();
let counter = 0;
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };

function leave(ws) {
  const r = ws.room && rooms.get(ws.room);
  if (!r) return;
  r.delete(ws);
  if (!r.size) rooms.delete(ws.room);
  else for (const o of r) send(o, { op: 'peers', left: [ws.id] });
  ws.room = null;
}

wss.on('connection', (ws) => {
  ws.id = Math.random().toString(36).slice(2, 8) + (++counter).toString(36);
  ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', (buf) => {
    let m;
    try { m = JSON.parse(buf); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    if (m.op === 'join') {
      const name = String(m.room || '');
      if (!/^[a-z0-9][a-z0-9_.-]{0,47}$/.test(name)) return;
      leave(ws);
      let r = rooms.get(name);
      if (!r) rooms.set(name, (r = new Set()));
      if (r.size >= (name.startsWith('tcq-') ? 100 : 4)) { send(ws, { op: 'full' }); return; }
      const others = [...r].map((o) => o.id);
      for (const o of r) send(o, { op: 'peers', joined: [ws.id] });
      r.add(ws);
      ws.room = name;
      send(ws, { op: 'hi', id: ws.id, peers: others });
    } else if (m.op === 'msg' && ws.room && typeof m.t === 'string' && m.t.length < 32) {
      const r = rooms.get(ws.room);
      if (r) for (const o of r) if (o !== ws) send(o, { op: 'msg', t: m.t, d: m.d, f: ws.id });
    }
  });
  ws.on('close', () => leave(ws));
  ws.on('error', () => {});
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false;
    ws.ping();
  }
}, 25000);

server.listen(process.env.PORT || 8080, () => console.log('relay listening'));
