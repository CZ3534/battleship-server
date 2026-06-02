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
        if (room.players.length >= 2) { send(ws, { type: 'error', message: 'Room is full' }); return; }
        room.players.push(ws);
        ws.roomCode = code;
        ws.playerIndex = 1;
        send(ws, { type: 'joined', code, playerIndex: 1, settings: room.settings });
        send(room.players[0], { type: 'opponent_joined' });
        break;
      }
      // All game messages relayed as-is
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
    rooms[code].players = rooms[code].players.filter(p => p !== ws);
    if (rooms[code].players.length === 0) delete rooms[code];
    else broadcast(code, { type: 'opponent_disconnected' });
  });
});

server.listen(PORT, () => console.log(`Battleship relay on :${PORT}`));
