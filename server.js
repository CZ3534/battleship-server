const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Battleship server running');
});

const wss = new WebSocketServer({ server });

// rooms[code] = { players: [ws, ws?], settings: {} }
const rooms = {};

function generateCode() {
  return Math.random().toString(36).substring(2, 7).toUpperCase();
}

function send(ws, msg) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

function relay(sender, roomCode, msg) {
  const room = rooms[roomCode];
  if (!room) return;
  room.players.forEach(p => { if (p !== sender) send(p, msg); });
}

function broadcast(roomCode, msg) {
  const room = rooms[roomCode];
  if (!room) return;
  room.players.forEach(p => send(p, msg));
}

wss.on('connection', ws => {
  ws.roomCode = null;
  ws.playerIndex = null;

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case 'create': {
        let code;
        do { code = generateCode(); } while (rooms[code]);
        rooms[code] = { players: [ws], settings: msg.settings || { fleet: 'standard' } };
        ws.roomCode = code;
        ws.playerIndex = 0;
        send(ws, { type: 'created', code, playerIndex: 0, settings: rooms[code].settings });
        break;
      }
      case 'join': {
        const code = (msg.code || '').toUpperCase();
        const room = rooms[code];
        if (!room) { send(ws, { type: 'error', message: 'Room not found' }); return; }
        if (room.players.length >= 2 && room.players.every(p => p !== null)) {
          send(ws, { type: 'error', message: 'Room is full' }); return;
        }
        room.players.push(ws);
        ws.roomCode = code;
        ws.playerIndex = 1;
        send(ws, { type: 'joined', code, playerIndex: 1, settings: room.settings });
        send(room.players[0], { type: 'opponent_joined' });
        break;
      }

      case 'rejoin': {
        const code = (msg.code || '').toUpperCase();
        const pidx = msg.playerIndex;
        const room = rooms[code];
        if (!room) { send(ws, { type: 'error', message: 'Room expired' }); return; }
        if (room.players[pidx] !== null) { send(ws, { type: 'error', message: 'Slot taken' }); return; }
        // Clear grace period timeout
        if (room.disconnected && room.disconnected[pidx]) {
          clearTimeout(room.disconnected[pidx].timeout);
          delete room.disconnected[pidx];
        }
        // Restore player in slot
        room.players[pidx] = ws;
        ws.roomCode = code;
        ws.playerIndex = pidx;
        send(ws, { type: 'rejoined', code, playerIndex: pidx, settings: room.settings });
        room.players.forEach(p => { if (p && p !== ws) send(p, { type: 'opponent_rejoined' }); });
        break;
      }
      // All game messages relayed as-is
      case 'ping':
        send(ws, { type: 'pong' });
        break;
      case 'ready':
      case 'fire':
      case 'fire_result':
      case 'rematch_request':
      case 'rematch_accept':
        relay(ws, ws.roomCode, { ...msg, from: ws.playerIndex });
        break;
    }
  });

  ws.on('close', () => {
    const code = ws.roomCode;
    if (!code || !rooms[code]) return;
    const room = rooms[code];
    const idx = room.players.indexOf(ws);
    if (idx === -1) return;

    // Mark slot as disconnected (keep slot open for reconnection)
    room.players[idx] = null;
    room.disconnected = room.disconnected || {};
    room.disconnected[idx] = { playerIndex: ws.playerIndex, timeout: null };

    // Notify opponent
    room.players.forEach(p => { if (p) send(p, { type: 'opponent_reconnecting' }); });

    // Grace period — 30 seconds to reconnect
    room.disconnected[idx].timeout = setTimeout(() => {
      if (!rooms[code]) return;
      // Still disconnected after grace period — close room
      room.players.forEach(p => { if (p) send(p, { type: 'opponent_disconnected' }); });
      delete rooms[code];
    }, 30000);
  });
});

server.listen(PORT, () => console.log(`Battleship relay on :${PORT}`));
