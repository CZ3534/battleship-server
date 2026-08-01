const { WebSocketServer, WebSocket } = require('ws');
const { randomUUID } = require('crypto');
const http = require('http');

const PORT = process.env.PORT || 8080;
const GRACE_MS = 5 * 60 * 1000; // 5 minutes

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Battleship server running');
});

const wss = new WebSocketServer({ server });

// rooms[code] = {
//   players: [ws|null, ws|null],
//   uids: [uid, uid],
//   settings: {},
//   state: {
//     phase: 'placement'|'battle'|'over',
//     placements: [null, null],   // each player's placedShips
//     boards: [Array(100), Array(100)],  // myBoard for each player
//     shipHealth: [{}, {}],
//     turn: 0,                    // playerIndex whose turn it is
//     shots: [],                  // [{by, row, col, hit, sunk}]
//   }
// }
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
  room.players.forEach(p => { if (p && p !== sender) send(p, msg); });
}

wss.on('connection', ws => {
  ws.roomCode = null;
  ws.playerIndex = null;
  ws.uid = null;

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      case 'create': {
        let code;
        do { code = generateCode(); } while (rooms[code]);
        const hostUid = randomUUID();
        rooms[code] = {
          players: [ws, null],
          uids: [hostUid, null],
          settings: msg.settings || { fleet: 'standard' },
          state: {
            phase: 'placement',
            placements: [null, null],
            boards: [Array(100).fill(null), Array(100).fill(null)],
            shipHealth: [{}, {}],
            turn: 0,
            shots: [],
          },
          treasure: null  // set when treasure mode starts
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
        const slot1Active = room.players[1] !== null && room.players[1] !== undefined;
        if (slot1Active) { send(ws, { type: 'error', message: 'Room is full' }); return; }
        const guestUid = randomUUID();
        room.players[1] = ws;
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
        const pidx = room.uids.indexOf(uid);
        if (pidx === -1) { send(ws, { type: 'error', message: 'Invalid session' }); return; }
        // Force-close old connection if still alive
        if (room.players[pidx] && room.players[pidx] !== ws) {
          try { room.players[pidx].terminate(); } catch(e) {}
        }
        // Clear grace period
        if (room.disconnected && room.disconnected[pidx]) {
          clearTimeout(room.disconnected[pidx].timeout);
          delete room.disconnected[pidx];
        }
        room.players[pidx] = ws;
        ws.roomCode = code;
        ws.playerIndex = pidx;
        ws.uid = uid;
        // Send full state restore so client can rebuild board without sessionStorage
        send(ws, {
          type: 'rejoined',
          code,
          playerIndex: pidx,
          settings: room.settings,
          state: room.state,
        });
        room.players.forEach(p => { if (p && p !== ws) send(p, { type: 'opponent_rejoined' }); });
        break;
      }

      case 'ready': {
        // Store placement data on server
        const room = rooms[ws.roomCode];
        if (!room) return;
        const pidx = ws.playerIndex;
        if (msg.placedShips) room.state.placements[pidx] = msg.placedShips;
        if (msg.myBoard) room.state.boards[pidx] = msg.myBoard;
        if (msg.shipHealth) room.state.shipHealth[pidx] = msg.shipHealth;
        relay(ws, ws.roomCode, { ...msg, from: pidx });
        break;
      }

      case 'fire': {
        const room = rooms[ws.roomCode];
        if (!room) return;
        // Validate it's actually this player's turn
        if (room.state.turn !== ws.playerIndex) {
          send(ws, { type: 'turn_correction', yourTurn: room.state.turn === ws.playerIndex });
          return;
        }
        relay(ws, ws.roomCode, { ...msg, from: ws.playerIndex });
        break;
      }

      case 'fire_result': {
        const room = rooms[ws.roomCode];
        if (!room) return;
        const pidx = ws.playerIndex; // defender
        const attackerIdx = 1 - pidx;
        const { row, col, hit, sunk, gameOver } = msg;
        const idx = row * 10 + col;
        // Update server board state
        if (hit) {
          room.state.boards[pidx][idx] = sunk ? 'sunk' : 'hit';
          if (sunk && room.state.shipHealth[pidx]) {
            room.state.shipHealth[pidx][sunk.id] = 0;
            sunk.cells.forEach(([r,c]) => { room.state.boards[pidx][r*10+c] = 'sunk'; });
          }
          room.state.turn = attackerIdx; // hit = attacker fires again
        } else {
          room.state.boards[pidx][idx] = 'miss';
          room.state.turn = pidx; // miss = defender's turn
        }
        room.state.shots.push({ by: attackerIdx, row, col, hit, sunk: sunk || null });
        if (gameOver) room.state.phase = 'over';
        relay(ws, ws.roomCode, { ...msg, from: pidx });
        break;
      }

      case 'reveal_ships': {
        // Winner sends their ship placements so loser can see the full map
        relay(ws, ws.roomCode, { ...msg, from: ws.playerIndex });
        break;
      }

      case 'treasure_ready': {
        const room = rooms[ws.roomCode];
        if (!room) return;
        if (!room.treasureReady) room.treasureReady = [];
        if (!room.treasureReady.includes(ws.playerIndex)) {
          room.treasureReady.push(ws.playerIndex);
        }
        if (room.treasureReady.length === 2) {
          // Both ready — place 3 treasures with minimum Chebyshev distance 4-6 between each
          const chebyshev = (r1,c1,r2,c2) => Math.max(Math.abs(r1-r2), Math.abs(c1-c2));
          const treasures = [];
          let attempts = 0;
          while (treasures.length < 3 && attempts < 10000) {
            attempts++;
            const r = Math.floor(Math.random() * 10);
            const c = Math.floor(Math.random() * 10);
            const minDist = 4 + Math.floor(Math.random() * 3); // 4, 5, or 6
            const tooClose = treasures.some(([tr, tc]) => chebyshev(r, c, tr, tc) < minDist);
            if (!tooClose) treasures.push([r, c]);
          }
          // Fallback: if can't place with random min dist, use fixed minimum of 4
          if (treasures.length < 3) {
            treasures.length = 0;
            while (treasures.length < 3) {
              const r = Math.floor(Math.random() * 10);
              const c = Math.floor(Math.random() * 10);
              if (!treasures.some(([tr, tc]) => chebyshev(r, c, tr, tc) < 4)) treasures.push([r, c]);
            }
          }
          room.treasure = {
            positions: treasures,
            grid: Array(100).fill(null), // null=undig, 'miss', 'found'
            scores: [0, 0],
            turn: 0, // playerIndex whose turn it is
            found: 0,
            digs: 0,
          };
          room.treasureReady = [];
          room.state.phase = 'treasure';
          // Send start to both players (don't send positions — they're hidden)
          room.players.forEach((p, i) => {
            send(p, { type: 'treasure_start', turn: 0, scores: [0, 0] });
          });
        }
        break;
      }

      case 'treasure_dig': {
        const room = rooms[ws.roomCode];
        if (!room || !room.treasure) return;
        const tr = room.treasure;
        // Validate turn
        if (tr.turn !== ws.playerIndex) {
          send(ws, { type: 'turn_correction', yourTurn: false });
          return;
        }
        const { row, col } = msg;
        const idx = row * 10 + col;
        if (tr.grid[idx]) return; // already dug
        tr.digs++;
        // Check if treasure
        const hit = tr.positions.some(([r, c]) => r === row && c === col);
        let treasure_id = null;
        if (hit) {
          tr.grid[idx] = 'found';
          tr.scores[ws.playerIndex]++;
          tr.found++;
          treasure_id = tr.found;
        } else {
          tr.grid[idx] = 'miss';
        }
        // Chebyshev distance to nearest UNFOUND treasure
        const unfound = tr.positions.filter(([r,c]) => tr.grid[r*10+c] !== 'found');
        let minDist = Infinity;
        unfound.forEach(([r,c]) => {
          const d = Math.max(Math.abs(r-row), Math.abs(c-col));
          if (d < minDist) minDist = d;
        });
        const hint = minDist === 0 ? 'found'
          : minDist <= 2 ? 'warmer'
          : minDist <= 5 ? 'warm'
          : 'cold';
        // Turn always flips
        tr.turn = 1 - ws.playerIndex;
        // Check win: first to 2
        const game_over = tr.scores[0] >= 2 || tr.scores[1] >= 2;
        const winner = game_over ? (tr.scores[0] >= 2 ? 0 : 1) : null;
        if (game_over) room.state.phase = 'over';
        const result = {
          type: 'treasure_dig_result',
          row, col, hit, hint, treasure_id,
          by: ws.playerIndex,
          next_turn: tr.turn,
          scores: [...tr.scores],
          game_over,
          winner,
          treasures: game_over ? tr.positions : undefined, // reveal all on game over
        };
        // Broadcast to both players
        room.players.forEach(p => send(p, result));
        break;
      }

      case 'ping':
        send(ws, { type: 'pong' });
        break;

      case 'rematch_request':
      case 'rematch_accept':
        relay(ws, ws.roomCode, { ...msg, from: ws.playerIndex });
        break;

      default:
        // Forward unknown message types (state_sync etc)
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

    room.players[idx] = null;
    room.disconnected = room.disconnected || {};

    room.players.forEach(p => { if (p) send(p, { type: 'opponent_reconnecting' }); });

    // Extended grace period — 5 minutes
    room.disconnected[idx] = {
      timeout: setTimeout(() => {
        if (!rooms[code]) return;
        room.players.forEach(p => { if (p) send(p, { type: 'opponent_disconnected' }); });
        delete rooms[code];
      }, GRACE_MS)
    };
  });
});

server.listen(PORT, () => console.log(`Battleship relay on :${PORT}`));
