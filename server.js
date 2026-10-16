// ═══════════════════════════════════════════════════════════════════════════
//  LEAGUE OF LEGENDS — ARAM: MAYHEM (LAN MULTIPLAYER SERVER)
//  Zero-Dependency Node.js HTTP + RFC-6455 WebSocket Server
//  0 Bots by Default — Supports Custom Bot Addition/Removal & Mobile Clients
// ═══════════════════════════════════════════════════════════════════════════
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const WS_MAGIC = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

const ALL_CHAMP_IDS = [
  'luxana', 'yasu', 'jinxie', 'blitz', 'zedd', 'ahria',
  'daren', 'ezre', 'morga', 'pykeo', 'veiga', 'ashela', 'kata', 'thresh', 'mf', 'malph'
];

class SimpleWebSocketClient {
  constructor(socket) {
    this.socket = socket;
    this.readyState = 1;
    this.buffer = Buffer.alloc(0);
    this.onmessage = null;
    this.onclose = null;

    socket.on('data', chunk => {
      this.buffer = Buffer.concat([this.buffer, chunk]);
      this._parseFrames();
    });
    socket.on('close', () => { this.readyState = 3; if (this.onclose) this.onclose(); });
    socket.on('error', () => { this.readyState = 3; if (this.onclose) this.onclose(); });
  }

  _parseFrames() {
    while (this.buffer.length >= 2) {
      const b0 = this.buffer[0];
      const b1 = this.buffer[1];
      const opcode = b0 & 0x0f;
      const isMasked = (b1 & 0x80) !== 0;
      let payloadLen = b1 & 0x7f;
      let offset = 2;

      if (payloadLen === 126) {
        if (this.buffer.length < 4) return;
        payloadLen = this.buffer.readUInt16BE(2);
        offset = 4;
      } else if (payloadLen === 127) {
        if (this.buffer.length < 10) return;
        payloadLen = Number(this.buffer.readBigUInt64BE(2));
        offset = 10;
      }

      let maskKey = null;
      if (isMasked) {
        if (this.buffer.length < offset + 4) return;
        maskKey = this.buffer.slice(offset, offset + 4);
        offset += 4;
      }

      if (this.buffer.length < offset + payloadLen) return;

      const payload = this.buffer.slice(offset, offset + payloadLen);
      this.buffer = this.buffer.slice(offset + payloadLen);

      if (isMasked && maskKey) {
        for (let i = 0; i < payload.length; i++) payload[i] ^= maskKey[i % 4];
      }

      if (opcode === 0x08) {
        this.close();
        return;
      } else if (opcode === 0x09) {
        this._sendFrame(0x0a, payload);
      } else if (opcode === 0x01) {
        if (this.onmessage) this.onmessage(payload.toString('utf8'));
      }
    }
  }

  send(data) {
    if (this.readyState !== 1) return;
    const buf = Buffer.from(typeof data === 'string' ? data : JSON.stringify(data), 'utf8');
    this._sendFrame(0x01, buf);
  }

  _sendFrame(opcode, payload) {
    const len = payload.length;
    let header;
    if (len < 126) {
      header = Buffer.from([0x80 | opcode, len]);
    } else if (len <= 65535) {
      header = Buffer.alloc(4);
      header[0] = 0x80 | opcode;
      header[1] = 126;
      header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.alloc(10);
      header[0] = 0x80 | opcode;
      header[1] = 127;
      header.writeBigUInt64BE(BigInt(len), 2);
    }
    try { this.socket.write(Buffer.concat([header, payload])); } catch (e) {}
  }

  close() {
    if (this.readyState !== 3) {
      this.readyState = 3;
      try { this.socket.write(Buffer.from([0x88, 0x00])); this.socket.end(); } catch (e) {}
    }
  }
}

// ═══ LOBBY & MATCH COORDINATOR (0 BOTS BY DEFAULT) ═══
const clients = new Map();
let nextPlayerId = 1;
let benchChamps = ['ahria', 'daren', 'pykeo'];
let blueBots = []; // 0 bots by default!
let redBots = [];  // 0 bots by default!
let freePickMode = true;
let aiDiff = 'hard';

function broadcast(msgObj, excludeWs = null) {
  const raw = JSON.stringify(msgObj);
  for (const [ws] of clients) {
    if (ws !== excludeWs && ws.readyState === 1) {
      ws.send(raw);
    }
  }
}

function getHostId() {
  const first = clients.values().next().value;
  return first ? first.id : 1;
}

function pickUnusedBotChamp() {
  const used = new Set([
    ...[...clients.values()].map(c => c.champId),
    ...blueBots,
    ...redBots
  ]);
  const pool = ALL_CHAMP_IDS.filter(id => !used.has(id));
  const src = pool.length > 0 ? pool : ALL_CHAMP_IDS;
  return src[Math.floor(Math.random() * src.length)];
}

function broadcastLobbyState() {
  const players = [...clients.values()].map(c => ({
    id: c.id,
    name: c.name,
    champId: c.champId,
    team: c.team,
    spellD: c.spellD,
    spellF: c.spellF
  }));
  broadcast({
    type: 'lobby_state',
    hostId: getHostId(),
    players,
    bench: benchChamps,
    blueBots,
    redBots,
    freePickMode,
    aiDiff
  });
}

function buildMatchSlots() {
  const bluePlayers = [...clients.values()].filter(c => c.team === 'blue').slice(0, 5);
  const redPlayers = [...clients.values()].filter(c => c.team === 'red').slice(0, 5);
  const slots = [];

  // Blue Team: Connected Human Players + Explicitly Added Blue Bots
  bluePlayers.forEach(p => {
    slots.push({ team: 'blue', isHuman: true, playerId: p.id, name: p.name, specId: p.champId, spellD: p.spellD, spellF: p.spellF });
  });
  blueBots.slice(0, Math.max(0, 5 - bluePlayers.length)).forEach(cid => {
    slots.push({ team: 'blue', isHuman: false, playerId: -1, name: null, specId: cid, spellD: 'snowball', spellF: 'flash' });
  });

  // Red Team: Connected Human Players + Explicitly Added Red Bots
  redPlayers.forEach(p => {
    slots.push({ team: 'red', isHuman: true, playerId: p.id, name: p.name, specId: p.champId, spellD: p.spellD, spellF: p.spellF });
  });
  redBots.slice(0, Math.max(0, 5 - redPlayers.length)).forEach(cid => {
    slots.push({ team: 'red', isHuman: false, playerId: -1, name: null, specId: cid, spellD: 'snowball', spellF: 'flash' });
  });

  return slots;
}

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(fs.readFileSync(path.join(__dirname, 'public', 'index.html'), 'utf-8'));
  } else {
    res.writeHead(404);
    res.end('Not Found');
  }
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }

  const accept = crypto.createHash('sha1').update(key + WS_MAGIC).digest('base64');
  const headers = [
    'HTTP/1.1 101 Switching Protocols',
    'Upgrade: websocket',
    'Connection: Upgrade',
    `Sec-WebSocket-Accept: ${accept}`
  ];
  socket.write(headers.join('\r\n') + '\r\n\r\n');

  const ws = new SimpleWebSocketClient(socket);
  const id = nextPlayerId++;
  const defaultChamp = ALL_CHAMP_IDS[(id - 1) % ALL_CHAMP_IDS.length];
  // Balance teams automatically as friends join (1st Blue, 2nd Red, etc.)
  const blueCount = [...clients.values()].filter(c => c.team === 'blue').length;
  const redCount = [...clients.values()].filter(c => c.team === 'red').length;
  const client = {
    id,
    name: `Summoner ${id}`,
    champId: defaultChamp,
    team: blueCount <= redCount ? 'blue' : 'red',
    spellD: 'snowball',
    spellF: 'flash'
  };
  clients.set(ws, client);

  ws.send({ type: 'welcome', playerId: id, name: client.name });
  broadcastLobbyState();
  broadcast({ type: 'chat', sender: 'System', text: `${client.name} joined (${client.team.toUpperCase()} TEAM).`, team: 'sys' });

  ws.onmessage = (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'join_lobby') {
        if (msg.name && msg.name !== 'Summoner') client.name = String(msg.name).slice(0, 16);
        if (msg.champId && ALL_CHAMP_IDS.includes(msg.champId)) client.champId = msg.champId;
        if (msg.spellD) client.spellD = msg.spellD;
        if (msg.spellF) client.spellF = msg.spellF;
        broadcastLobbyState();
      } else if (msg.type === 'set_name') {
        client.name = String(msg.name || '').slice(0, 16) || client.name;
        broadcastLobbyState();
      } else if (msg.type === 'select_champ') {
        if (ALL_CHAMP_IDS.includes(msg.champId)) {
          client.champId = msg.champId;
          broadcastLobbyState();
        }
      } else if (msg.type === 'reroll_champ') {
        const pool = ALL_CHAMP_IDS.filter(cid => cid !== client.champId && !benchChamps.includes(cid));
        if (pool.length > 0) {
          benchChamps.unshift(client.champId);
          if (benchChamps.length > 6) benchChamps.pop();
          client.champId = pool[Math.floor(Math.random() * pool.length)];
          broadcastLobbyState();
        }
      } else if (msg.type === 'swap_bench') {
        const idx = Number(msg.benchIdx);
        if (idx >= 0 && idx < benchChamps.length) {
          const prev = client.champId;
          client.champId = benchChamps[idx];
          benchChamps[idx] = prev;
          broadcastLobbyState();
        }
      } else if (msg.type === 'switch_team') {
        client.team = client.team === 'blue' ? 'red' : 'blue';
        broadcastLobbyState();
      } else if (msg.type === 'modify_bots') {
        const arr = msg.team === 'blue' ? blueBots : redBots;
        const teamHumans = [...clients.values()].filter(c => c.team === msg.team).length;
        if (msg.op === 'fill_5v5') {
        const bHum = [...clients.values()].filter(c => c.team === 'blue').length;
        const rHum = [...clients.values()].filter(c => c.team === 'red').length;
        if (blueBots.length + bHum >= 5 && redBots.length + rHum >= 5) {
          blueBots = []; redBots = [];
        } else {
          while (blueBots.length + bHum < 5) blueBots.push(pickUnusedChamp());
          while (redBots.length + rHum < 5) redBots.push(pickUnusedChamp());
        }
      } else if (msg.op === 'add') {
          if (teamHumans + arr.length < 5) arr.push(pickUnusedBotChamp());
        } else if (msg.op === 'remove') {
          if (arr.length > 0) arr.pop();
        } else if (msg.op === 'remove_idx') {
          const bIdx = Number(msg.botIdx);
          if (bIdx >= 0 && bIdx < arr.length) arr.splice(bIdx, 1);
        } else if (msg.op === 'cycle_idx') {
          const bIdx = Number(msg.botIdx);
          if (bIdx >= 0 && bIdx < arr.length) {
            const cIdx = ALL_CHAMP_IDS.indexOf(arr[bIdx]);
            arr[bIdx] = ALL_CHAMP_IDS[(cIdx + 1) % ALL_CHAMP_IDS.length];
          }
        }
        broadcastLobbyState();
      } else if (msg.type === 'set_ai_diff') {
        if (['normal', 'hard', 'mayhem'].includes(msg.aiDiff)) aiDiff = msg.aiDiff;
        broadcastLobbyState();
      } else if (msg.type === 'set_spells') {
        if (msg.spellD) client.spellD = msg.spellD;
        if (msg.spellF) client.spellF = msg.spellF;
        broadcastLobbyState();
      } else if (msg.type === 'set_free_pick') {
        freePickMode = !!msg.freePickMode;
        broadcastLobbyState();
      } else if (msg.type === 'chat') {
        const txt = String(msg.text || '').slice(0, 120).trim();
        if (txt) {
          broadcast({ type: 'chat', sender: client.name, text: txt, team: client.team });
        }
      } else if (msg.type === 'start_game') {
        const slots = buildMatchSlots();
        broadcast({ type: 'match_start', aiDiff: msg.aiDiff || aiDiff, slots });
      } else if (msg.type === 'player_action') {
        broadcast({ type: 'player_action', champIdx: msg.champIdx, action: msg.action }, ws);
      } else if (msg.type === 'world_sync') {
        if (client.id === getHostId()) {
          broadcast({ type: 'world_sync', snapshot: msg.snapshot }, ws);
        }
      }
    } catch (e) {}
  };

  ws.onclose = () => {
    clients.delete(ws);
    broadcastLobbyState();
    broadcast({ type: 'chat', sender: 'System', text: `${client.name} left the lobby.`, team: 'sys' });
  };
});

function getLocalIP() {
  const ifaces = os.networkInterfaces();
  for (const name of Object.keys(ifaces)) {
    for (const iface of ifaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) return iface.address;
    }
  }
  return '127.0.0.1';
}

server.listen(PORT, '0.0.0.0', () => {
  const ip = getLocalIP();
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  LEAGUE OF LEGENDS — ARAM: MAYHEM (PC & MOBILE LAN SERVER)');
  console.log(`  Local PC:       http://localhost:${PORT}`);
  console.log(`  LAN / Mobile:   http://${ip}:${PORT}  <-- Open on phones/PCs!`);
  console.log('═══════════════════════════════════════════════════════════════');
});
