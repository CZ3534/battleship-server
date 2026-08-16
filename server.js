const { WebSocketServer, WebSocket } = require('ws');
const { randomUUID } = require('crypto');
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

function ensureState(room) {
  if (!room.state) {
    room.state = {
      phase: 'placement',
      placements: [null, null],
      boards: [Array(100).fill(null), Array(100).fill(null)],
      shipHealth: [{}, {}],
      turn: 0,
      shots: [],
    };
  }
  return room.state;
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
        const hostUid = randomUUID();
        rooms[code] = {
          players: [ws],
          uids: [hostUid, null],   // uid per slot
          settings: msg.settings || { fleet: 'standard' }
        };
        ws.roomCode = code;
        ws.playerIndex = 0;
        ws.uid = hostUid;
        send(ws, { type: 'created', code, playerIndex: 0, uid: hostUid, settings: rooms[code].settings });
        break;
      }
      case 'join': {
        const code = (msg.code || '').toUpperCase();
        const room = rooms[code];
        if (!room) { send(ws, { type: 'error', message: 'Room not found' }); return; }
        // Room is full if both slots are occupied by active connections
        const slot1Active = room.players[1] !== undefined && room.players[1] !== null;
        if (slot1Active) { send(ws, { type: 'error', message: 'Room is full' }); return; }
        const guestUid = randomUUID();
        if (!room.uids) room.uids = [null, null];
        room.players.push(ws);
        room.uids[1] = guestUid;
        ws.roomCode = code;
        ws.playerIndex = 1;
        ws.uid = guestUid;
        send(ws, { type: 'joined', code, playerIndex: 1, uid: guestUid, settings: room.settings });
        room.players.forEach(p => { if (p && p !== ws) send(p, { type: 'opponent_joined' }); });
        break;
      }

      case 'rejoin': {
        const code = (msg.code || '').toUpperCase();
        const uid = msg.uid;
        const room = rooms[code];
        if (!room) { send(ws, { type: 'error', message: 'Room expired' }); return; }
        // Match UID to slot
        const pidx = room.uids ? room.uids.indexOf(uid) : -1;
        if (pidx === -1) { send(ws, { type: 'error', message: 'Invalid session' }); return; }
        if (room.players[pidx] !== null) {
          // Slot still active — UID matches so ownership is proven.
          // Force-close the old connection and let this one take over.
          const oldWs = room.players[pidx];
          room.players[pidx] = null;
          try { oldWs.terminate(); } catch(e) {}
        }
        // Clear grace period timeout
        if (room.disconnected && room.disconnected[pidx]) {
          clearTimeout(room.disconnected[pidx].timeout);
          delete room.disconnected[pidx];
        }
        // Restore player in slot
        room.players[pidx] = ws;
        ws.roomCode = code;
        ws.playerIndex = pidx;
        ws.uid = uid;
        send(ws, { type: 'rejoined', code, playerIndex: pidx, settings: room.settings });
        room.players.forEach(p => { if (p && p !== ws) send(p, { type: 'opponent_rejoined' }); });
        break;
      }
      // All game messages relayed as-is
      case 'radar_ping': {
        try {
          const room = rooms[ws.roomCode];
          if (!room) return;
          const rState = ensureState(room);
          if (rState.turn !== ws.playerIndex) {
            send(ws, { type: 'turn_correction', yourTurn: false });
            return;
          }
          const { row, col } = msg;
          if (row < 0 || row > 9 || col < 0 || col > 9) return;
          const defenderIdx = 1 - ws.playerIndex;
          const defenderBoard = rState.boards && rState.boards[defenderIdx];
          if (!defenderBoard) {
            // Board not stored yet (placement not received) — just consume turn
            room.state.turn = defenderIdx;
            send(ws, { type: 'radar_result', found: false });
            if (room.players[defenderIdx]) send(room.players[defenderIdx], { type: 'opponent_turn' });
            return;
          }
          let found = false;
          for (let dr = -1; dr <= 1 && !found; dr++) {
            for (let dc = -1; dc <= 1 && !found; dc++) {
              const nr = row + dr, nc = col + dc;
              if (nr < 0 || nr > 9 || nc < 0 || nc > 9) continue;
              if (defenderBoard[nr * 10 + nc] === 'ship') found = true;
            }
          }
          rState.turn = defenderIdx;
          send(ws, { type: 'radar_result', found });
          if (room.players[defenderIdx]) send(room.players[defenderIdx], { type: 'opponent_turn' });
        } catch(e) {
          console.error('[radar_ping error]', e.message);
        }
        break;
      }

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
