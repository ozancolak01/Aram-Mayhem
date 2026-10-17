// ═══════════════════════════════════════════════════════════════════════════
//  LEAGUE OF LEGENDS — ARAM: MAYHEM (PRODUCTION-GRADE MULTIPLAYER SERVER)
//  Zero-Dependency Node.js HTTP + RFC-6455 WebSocket Server
//  Features:
//    - TCP_NODELAY & Keep-Alive low-latency socket tuning
//    - RFC-6455 multi-frame parser with 256KB max frame guard & backpressure safety
//    - 3-Second Heartbeat Ping/Pong RTT tracker & 15-Second Dead-Peer Watchdog
//    - Automatic Host Migration (seamless match continuation if Host drops)
//    - Late-Join / Bot-Takeover synchronization ('player_joined_match' broadcast)
//    - Slot-ownership validation & monotonic sequence numbering ('seq', 'srvTs')
//    - Live '/health' JSON telemetry endpoint
// ═══════════════════════════════════════════════════════════════════════════
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const WS_MAGIC = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_FRAME_BYTES = 262144; // 256 KB safety cap per frame
const HEARTBEAT_INTERVAL_MS = 3000;
const PEER_TIMEOUT_MS = 15000;

const ALL_CHAMP_IDS = [
  'luxana', 'yasu', 'jinxie', 'blitz', 'zedd', 'ahria',
  'daren', 'ezre', 'morga', 'pykeo', 'veiga', 'ashela', 'kata', 'thresh', 'mf', 'malph',
  'kaisa', 'jhin', 'sylas', 'leesin', 'vayne', 'sett', 'leona', 'viktor'
];

class SimpleWebSocketClient {
  constructor(socket) {
    this.socket = socket;
    this.readyState = 1;
    this.buffer = Buffer.alloc(0);
    this.onmessage = null;
    this.onclose = null;
    this.onpong = null;

    try {
      socket.setNoDelay(true);
      socket.setKeepAlive(true, 5000);
    } catch (e) {}

    socket.on('data', chunk => {
      if (this.readyState !== 1) return;
      this.buffer = Buffer.concat([this.buffer, chunk]);
      if (this.buffer.length > MAX_FRAME_BYTES * 2) {
        this.close();
        return;
      }
      this._parseFrames();
    });
    socket.on('close', () => {
      const wasOpen = this.readyState !== 3;
      this.readyState = 3;
      if (wasOpen && this.onclose) this.onclose();
    });
    socket.on('error', () => {
      const wasOpen = this.readyState !== 3;
      this.readyState = 3;
      if (wasOpen && this.onclose) this.onclose();
    });
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

      if (payloadLen > MAX_FRAME_BYTES) {
        this.close();
        return;
      }

      let maskKey = null;
      if (isMasked) {
        if (this.buffer.length < offset + 4) return;
        maskKey = this.buffer.slice(offset, offset + 4);
        offset += 4;
      }

      if (this.buffer.length < offset + payloadLen) return;

      const payload = Buffer.from(this.buffer.slice(offset, offset + payloadLen));
      this.buffer = this.buffer.slice(offset + payloadLen);

      if (isMasked && maskKey) {
        for (let i = 0; i < payload.length; i++) payload[i] ^= maskKey[i % 4];
      }

      if (opcode === 0x08) {
        this.close();
        return;
      } else if (opcode === 0x09) {
        this._sendFrame(0x0a, payload);
      } else if (opcode === 0x0a) {
        if (this.onpong) this.onpong();
      } else if (opcode === 0x01) {
        if (this.onmessage) this.onmessage(payload.toString('utf8'));
      }
    }
  }

  send(data, isVolatile = false) {
    if (this.readyState !== 1 || !this.socket || this.socket.destroyed) return;
    // Backpressure guard for high-frequency volatile state packets
    if (isVolatile && this.socket.writableLength > 65536) return;
    const buf = Buffer.from(typeof data === 'string' ? data : JSON.stringify(data), 'utf8');
    this._sendFrame(0x01, buf);
  }

  ping() {
    if (this.readyState !== 1) return;
    this._sendFrame(0x09, Buffer.alloc(0));
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
      const wasOpen = true;
      this.readyState = 3;
      try { this.socket.write(Buffer.from([0x88, 0x00])); this.socket.end(); } catch (e) {}
      if (wasOpen && this.onclose) {
        const cb = this.onclose;
        this.onclose = null;
        cb();
      }
    }
  }
}

function shuffleArray(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

// ═══ LOBBY, CHAMP SELECT & MATCH COORDINATOR ═══
const clients = new Map();
const savedSessions = new Map();
let nextPlayerId = 1;
let serverSeq = 1;
const serverStartTime = Date.now();

let roomPhase = 'lobby'; // 'lobby' | 'champ_select' | 'in_game'
let champSelectTimer = 30;
let blueBench = [];
let redBench = [];
let blueBots = ['blitz', 'jinxie'];
let redBots = ['daren', 'ahria', 'yasu'];
let freePickMode = true;
let aiDiff = 'normal';
let activeMatch = null; // { aiDiff, slots, seed, latestSnapshot, hostId }

function broadcast(msgObj, excludeWs = null, isVolatile = false) {
  const raw = JSON.stringify(msgObj);
  for (const [ws] of clients) {
    if (ws !== excludeWs && ws.readyState === 1) {
      ws.send(raw, isVolatile);
    }
  }
}

function getUniqueClients() {
  const seen = new Set();
  const list = [];
  for (const c of clients.values()) {
    const k = c.clientToken || `id_${c.id}`;
    if (seen.has(k)) continue;
    seen.add(k);
    list.push(c);
  }
  return list;
}

function getHostId() {
  const list = getUniqueClients();
  return list.length > 0 ? list[0].id : 1;
}

function pickUnusedChamp(excludeSet = new Set()) {
  const pool = ALL_CHAMP_IDS.filter(id => !excludeSet.has(id));
  const src = pool.length > 0 ? pool : ALL_CHAMP_IDS;
  return src[Math.floor(Math.random() * src.length)];
}

function pickUnusedBotChamp() {
  const used = new Set([
    ...getUniqueClients().map(c => c.champId).filter(Boolean),
    ...blueBots,
    ...redBots
  ]);
  return pickUnusedChamp(used);
}

function broadcastLobbyState() {
  const players = getUniqueClients().map(c => ({
    id: c.id,
    clientToken: c.clientToken,
    name: c.name,
    champId: c.champId,
    team: c.team,
    ready: Boolean(c.ready),
    locked: Boolean(c.locked),
    rerolls: typeof c.rerolls === 'number' ? c.rerolls : 2,
    spellD: c.spellD,
    spellF: c.spellF,
    rtt: c.rtt || 0
  }));

  const hasOpenBotSlot = Boolean(
    activeMatch && Array.isArray(activeMatch.slots) && activeMatch.slots.some(s => !s.isHuman)
  );

  const matchInfo = activeMatch ? {
    time: (activeMatch.latestSnapshot && activeMatch.latestSnapshot.time) || 0,
    blueKills: (activeMatch.latestSnapshot && activeMatch.latestSnapshot.blueKills) || 0,
    redKills: (activeMatch.latestSnapshot && activeMatch.latestSnapshot.redKills) || 0,
    hasOpenBotSlot,
    players: activeMatch.slots.filter(s => s.isHuman).map(s => s.name)
  } : null;

  broadcast({
    type: 'lobby_state',
    phase: roomPhase,
    champSelectTimer,
    hostId: getHostId(),
    players,
    blueBench,
    redBench,
    bench: blueBench,
    blueBots,
    redBots,
    freePickMode,
    aiDiff,
    matchInProgress: (roomPhase === 'in_game' && Boolean(activeMatch)),
    matchInfo
  });
}

function startChampSelectPhase() {
  roomPhase = 'champ_select';
  champSelectTimer = 30;
  blueBench = [];
  redBench = [];

  const usedChamps = new Set();
  const shuffled = shuffleArray(ALL_CHAMP_IDS);
  let ptr = 0;

  for (const c of clients.values()) {
    const assigned = shuffled[ptr % shuffled.length];
    ptr++;
    c.champId = assigned;
    usedChamps.add(assigned);
    c.locked = false;
    c.rerolls = 2;
    c.ready = false;
    if (c.clientToken && savedSessions.has(c.clientToken)) {
      savedSessions.get(c.clientToken).champId = c.champId;
    }
  }

  blueBots = blueBots.map(() => {
    const cid = pickUnusedChamp(usedChamps);
    usedChamps.add(cid);
    return cid;
  });
  redBots = redBots.map(() => {
    const cid = pickUnusedChamp(usedChamps);
    usedChamps.add(cid);
    return cid;
  });

  broadcastLobbyState();
}

function checkAllLockedShortenTimer() {
  if (roomPhase !== 'champ_select') return;
  const humans = getUniqueClients();
  if (humans.length > 0 && humans.every(c => c.locked)) {
    if (champSelectTimer > 3) {
      champSelectTimer = 3;
      broadcastLobbyState();
    }
  }
}

function buildMatchSlots() {
  const uniqueClients = getUniqueClients();
  const used = new Set(uniqueClients.map(c => c.champId).filter(Boolean));

  uniqueClients.forEach(c => {
    if (!c.champId || !ALL_CHAMP_IDS.includes(c.champId)) {
      c.champId = pickUnusedChamp(used);
      used.add(c.champId);
    }
    c.locked = true;
  });

  const bluePlayers = uniqueClients.filter(c => c.team === 'blue').slice(0, 5);
  const redPlayers = uniqueClients.filter(c => c.team === 'red').slice(0, 5);
  const slots = [];

  bluePlayers.forEach(p => {
    slots.push({
      team: 'blue', isHuman: true, playerId: p.id, clientToken: p.clientToken,
      name: p.name, specId: p.champId, spellD: p.spellD, spellF: p.spellF
    });
  });
  blueBots.slice(0, Math.max(0, 5 - bluePlayers.length)).forEach(cid => {
    slots.push({ team: 'blue', isHuman: false, playerId: -1, clientToken: null, name: null, specId: cid, spellD: 'snowball', spellF: 'flash' });
  });

  redPlayers.forEach(p => {
    slots.push({
      team: 'red', isHuman: true, playerId: p.id, clientToken: p.clientToken,
      name: p.name, specId: p.champId, spellD: p.spellD, spellF: p.spellF
    });
  });
  redBots.slice(0, Math.max(0, 5 - redPlayers.length)).forEach(cid => {
    slots.push({ team: 'red', isHuman: false, playerId: -1, clientToken: null, name: null, specId: cid, spellD: 'snowball', spellF: 'flash' });
  });

  if (!slots.some(s => s.team === 'red')) {
    slots.push({ team: 'red', isHuman: false, playerId: -1, clientToken: null, name: null, specId: pickUnusedBotChamp(), spellD: 'snowball', spellF: 'flash' });
  }
  if (!slots.some(s => s.team === 'blue')) {
    slots.push({ team: 'blue', isHuman: false, playerId: -1, clientToken: null, name: null, specId: pickUnusedBotChamp(), spellD: 'snowball', spellF: 'flash' });
  }

  return slots;
}

function launchMatchFromChampSelect() {
  roomPhase = 'in_game';
  const slots = buildMatchSlots();
  const seed = Math.floor(Math.random() * 900000000) + 100000000;
  activeMatch = { aiDiff, slots, seed, latestSnapshot: null, hostId: getHostId() };
  for (const c of clients.values()) {
    c.ready = false;
    c.locked = false;
  }
  broadcast({ type: 'match_start', aiDiff: activeMatch.aiDiff, slots, seed });
  broadcastLobbyState();
}

function handleClientDeparture(ws, client, reason = 'disconnect') {
  if (!clients.has(ws)) return;
  const oldHostId = getHostId();
  if (client.clientToken) {
    savedSessions.set(client.clientToken, {
      id: client.id, name: client.name, champId: client.champId,
      team: client.team, locked: client.locked, rerolls: client.rerolls,
      spellD: client.spellD, spellF: client.spellF, lastSeen: Date.now()
    });
  }
  clients.delete(ws);

  const stillConnectedSameToken = client.clientToken && [...clients.values()].some(c => c.clientToken === client.clientToken);

  if (!stillConnectedSameToken && activeMatch && Array.isArray(activeMatch.slots)) {
    const slotIdx = activeMatch.slots.findIndex(
      s => (s.clientToken && s.clientToken === client.clientToken) || (s.playerId && s.playerId === client.id)
    );
    if (slotIdx !== -1) {
      const sl = activeMatch.slots[slotIdx];
      sl.isHuman = false;
      sl.playerId = -1;
      sl.clientToken = null;
      sl.name = null;
      broadcast({
        type: 'player_left_match',
        slotIdx,
        playerId: client.id,
        clientToken: client.clientToken,
        name: client.name,
        reason
      });
    }
  }

  if (clients.size === 0) {
    activeMatch = null;
    roomPhase = 'lobby';
  } else {
    const newHostId = getHostId();
    if (oldHostId !== newHostId) {
      if (activeMatch) activeMatch.hostId = newHostId;
      broadcast({ type: 'host_migrated', hostId: newHostId });
    }
    if (roomPhase === 'champ_select') {
      checkAllLockedShortenTimer();
    }
  }
  broadcastLobbyState();
}

// Server-side 1-second ticker for Timed Champion Select
setInterval(() => {
  if (roomPhase === 'champ_select') {
    if (clients.size === 0) {
      roomPhase = 'lobby';
      return;
    }
    champSelectTimer--;
    if (champSelectTimer <= 0) {
      launchMatchFromChampSelect();
    } else {
      broadcast({
        type: 'champ_select_tick',
        champSelectTimer
      });
    }
  }
}, 1000);

// Production Watchdog: Ping all sockets every 3s & evict dead peers (>15s silence)
setInterval(() => {
  const now = Date.now();
  for (const [ws, client] of [...clients.entries()]) {
    if (now - (client.lastSeen || now) > PEER_TIMEOUT_MS) {
      handleClientDeparture(ws, client, 'timeout');
      ws.close();
    } else {
      ws.ping();
    }
  }
}, HEARTBEAT_INTERVAL_MS);

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(JSON.stringify({
      status: 'ok',
      uptimeSec: Math.round((Date.now() - serverStartTime) / 1000),
      phase: roomPhase,
      connectedPlayers: getUniqueClients().length,
      hostId: getHostId(),
      matchInProgress: Boolean(activeMatch)
    }));
    return;
  }

  if (req.url === '/' || req.url.startsWith('/index.html') || req.url.startsWith('/?')) {
    const candidates = [
      path.join(__dirname, 'aram_mayhem.html'),
      path.join(__dirname, 'public', 'index.html'),
      path.join(__dirname, 'index.html'),
      path.join(process.cwd(), 'aram_mayhem.html'),
      path.join(process.cwd(), 'public', 'index.html'),
      path.join(process.cwd(), 'index.html')
    ];
    const htmlPath = candidates.find(p => fs.existsSync(p));
    if (htmlPath) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(fs.readFileSync(htmlPath, 'utf-8'));
    } else {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end('<h1>ARAM: Mayhem - index.html not found</h1>');
    }
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
  const randomStartChamp = ALL_CHAMP_IDS[Math.floor(Math.random() * ALL_CHAMP_IDS.length)];
  const blueCount = getUniqueClients().filter(c => c.team === 'blue').length;
  const redCount = getUniqueClients().filter(c => c.team === 'red').length;
  const client = {
    id,
    clientToken: null,
    name: `Oyuncu ${id}`,
    champId: randomStartChamp,
    team: blueCount <= redCount ? 'blue' : 'red',
    ready: false,
    locked: false,
    rerolls: 2,
    spellD: 'snowball',
    spellF: 'flash',
    lastSeen: Date.now(),
    rtt: 0
  };
  clients.set(ws, client);

  ws.onpong = () => {
    client.lastSeen = Date.now();
  };

  ws.onmessage = (raw) => {
    client.lastSeen = Date.now();
    try {
      const msg = JSON.parse(raw);
      const isHost = (client.id === getHostId());

      if (msg.type === 'ping_hb') {
        if (typeof msg.rtt === 'number' && msg.rtt >= 0 && msg.rtt < 10000) {
          client.rtt = Math.round(msg.rtt);
        }
        ws.send({ type: 'pong_hb', ts: msg.ts || Date.now(), srvTs: Date.now(), hostId: getHostId() });
        return;
      }

      if (msg.type === 'leave_lobby') {
        handleClientDeparture(ws, client, 'left');
        ws.close();
        return;
      }

      if (msg.type === 'kick_player') {
        if (!isHost) return;
        const targetId = Number(msg.targetId);
        for (const [targetWs, targetClient] of [...clients.entries()]) {
          if (targetClient.id === targetId && targetClient.id !== client.id) {
            targetWs.send({ type: 'kicked', reason: 'Oda kurucusu tarafından çıkarıldın.' });
            handleClientDeparture(targetWs, targetClient, 'kicked');
            targetWs.close();
          }
        }
        return;
      }

      if (msg.type === 'join_lobby') {
        const token = msg.clientToken ? String(msg.clientToken).slice(0, 64) : null;
        if (token) {
          client.clientToken = token;
          for (const [otherWs, otherClient] of [...clients.entries()]) {
            if (otherWs !== ws && otherClient.clientToken === token) {
              client.id = otherClient.id;
              client.name = otherClient.name;
              client.champId = otherClient.champId;
              client.team = otherClient.team;
              client.ready = otherClient.ready;
              client.locked = otherClient.locked;
              client.rerolls = otherClient.rerolls;
              clients.delete(otherWs);
              otherWs.onclose = null;
              otherWs.close();
            }
          }
          const prevSession = savedSessions.get(token);
          if (prevSession) {
            client.id = prevSession.id;
            client.name = prevSession.name;
            client.champId = prevSession.champId || client.champId;
            client.team = prevSession.team || client.team;
            client.locked = Boolean(prevSession.locked);
            client.rerolls = typeof prevSession.rerolls === 'number' ? prevSession.rerolls : 2;
            client.spellD = prevSession.spellD || 'snowball';
            client.spellF = prevSession.spellF || 'flash';
          }
        }

        if (msg.name && msg.name !== 'Summoner' && !msg.name.startsWith('Oyuncu ')) {
          client.name = String(msg.name).slice(0, 16);
        }
        if (msg.spellD) client.spellD = msg.spellD;
        if (msg.spellF) client.spellF = msg.spellF;

        if (client.clientToken) {
          savedSessions.set(client.clientToken, {
            id: client.id, name: client.name, champId: client.champId,
            team: client.team, locked: client.locked, rerolls: client.rerolls,
            spellD: client.spellD, spellF: client.spellF, lastSeen: Date.now()
          });
        }

        ws.send({ type: 'welcome', playerId: client.id, name: client.name, hostId: getHostId() });
        broadcastLobbyState();

        // Auto-reconnect player if they belong to the currently running match
        if (roomPhase === 'in_game' && activeMatch && Array.isArray(activeMatch.slots)) {
          const slotIdx = activeMatch.slots.findIndex(
            s => (s.clientToken && s.clientToken === client.clientToken) || (s.playerId && s.playerId === client.id)
          );
          if (slotIdx !== -1) {
            const mySlot = activeMatch.slots[slotIdx];
            mySlot.isHuman = true;
            mySlot.playerId = client.id;
            mySlot.clientToken = client.clientToken;
            mySlot.name = client.name;
            ws.send({
              type: 'match_start',
              aiDiff: activeMatch.aiDiff,
              slots: activeMatch.slots,
              seed: activeMatch.seed,
              snapshot: activeMatch.latestSnapshot || null
            });
            // Notify Host & other players that this human slot is active under human control!
            broadcast({
              type: 'player_joined_match',
              slotIdx,
              playerId: client.id,
              clientToken: client.clientToken,
              name: client.name,
              specId: mySlot.specId,
              team: mySlot.team,
              spellD: mySlot.spellD,
              spellF: mySlot.spellF
            }, ws);
          }
        }
      } else if (msg.type === 'rejoin_match') {
        if (activeMatch && Array.isArray(activeMatch.slots)) {
          let slotIdx = activeMatch.slots.findIndex(
            s => (s.clientToken && s.clientToken === client.clientToken) || (s.playerId && s.playerId === client.id)
          );
          if (slotIdx === -1 && !msg.spectateOnly) {
            slotIdx = activeMatch.slots.findIndex(s => !s.isHuman && s.team === client.team);
            if (slotIdx === -1) slotIdx = activeMatch.slots.findIndex(s => !s.isHuman);
            if (slotIdx !== -1) {
              const mySlot = activeMatch.slots[slotIdx];
              mySlot.isHuman = true;
              mySlot.playerId = client.id;
              mySlot.clientToken = client.clientToken;
              mySlot.name = client.name;
            }
          }
          const mySlot = slotIdx !== -1 ? activeMatch.slots[slotIdx] : null;
          ws.send({
            type: 'match_start',
            aiDiff: activeMatch.aiDiff,
            slots: activeMatch.slots,
            seed: activeMatch.seed,
            snapshot: activeMatch.latestSnapshot || null,
            spectator: Boolean(msg.spectateOnly || !mySlot)
          });
          // CRITICAL SYNC FIX: Broadcast to Host & all clients that a human took over this slot!
          if (mySlot && !msg.spectateOnly) {
            broadcast({
              type: 'player_joined_match',
              slotIdx,
              playerId: client.id,
              clientToken: client.clientToken,
              name: client.name,
              specId: mySlot.specId,
              team: mySlot.team,
              spellD: mySlot.spellD,
              spellF: mySlot.spellF
            }, ws);
          }
          broadcastLobbyState();
        }
      } else if (msg.type === 'toggle_ready') {
        if (roomPhase === 'lobby') {
          client.ready = Boolean(msg.ready);
          broadcastLobbyState();
        }
      } else if (msg.type === 'set_name') {
        client.name = String(msg.name || '').slice(0, 16) || client.name;
        if (client.clientToken && savedSessions.has(client.clientToken)) {
          savedSessions.get(client.clientToken).name = client.name;
        }
        broadcastLobbyState();
      } else if (msg.type === 'switch_team') {
        if (roomPhase === 'lobby') {
          client.team = client.team === 'blue' ? 'red' : 'blue';
          client.ready = false;
          if (client.clientToken && savedSessions.has(client.clientToken)) {
            savedSessions.get(client.clientToken).team = client.team;
          }
          broadcastLobbyState();
        }
      } else if (msg.type === 'select_champ') {
        if (roomPhase === 'champ_select' && freePickMode && !client.locked && ALL_CHAMP_IDS.includes(msg.champId)) {
          const teamLocked = getUniqueClients().some(
            other => other.id !== client.id && other.team === client.team && other.locked && other.champId === msg.champId
          );
          if (!teamLocked) {
            client.champId = msg.champId;
            if (client.clientToken && savedSessions.has(client.clientToken)) {
              savedSessions.get(client.clientToken).champId = client.champId;
            }
            broadcastLobbyState();
          }
        }
      } else if (msg.type === 'reroll_champ') {
        if (roomPhase === 'champ_select' && !client.locked && client.rerolls > 0) {
          const teamBench = client.team === 'blue' ? blueBench : redBench;
          const teamUsed = new Set([
            ...getUniqueClients().filter(c => c.team === client.team).map(c => c.champId),
            ...teamBench
          ]);
          const pool = ALL_CHAMP_IDS.filter(cid => !teamUsed.has(cid));
          if (pool.length > 0) {
            client.rerolls--;
            if (client.champId) {
              teamBench.unshift(client.champId);
              if (teamBench.length > 6) teamBench.pop();
            }
            client.champId = pool[Math.floor(Math.random() * pool.length)];
            if (client.clientToken && savedSessions.has(client.clientToken)) {
              savedSessions.get(client.clientToken).champId = client.champId;
              savedSessions.get(client.clientToken).rerolls = client.rerolls;
            }
            broadcastLobbyState();
          }
        }
      } else if (msg.type === 'swap_bench') {
        if (roomPhase === 'champ_select' && !client.locked) {
          const teamBench = client.team === 'blue' ? blueBench : redBench;
          const idx = Number(msg.benchIdx);
          if (idx >= 0 && idx < teamBench.length) {
            const prev = client.champId;
            client.champId = teamBench[idx];
            teamBench[idx] = prev;
            if (client.clientToken && savedSessions.has(client.clientToken)) {
              savedSessions.get(client.clientToken).champId = client.champId;
            }
            broadcastLobbyState();
          }
        }
      } else if (msg.type === 'lock_champ') {
        if (roomPhase === 'champ_select') {
          if (!client.champId || !ALL_CHAMP_IDS.includes(client.champId)) {
            client.champId = pickUnusedChamp(new Set(getUniqueClients().map(c => c.champId)));
          }
          client.locked = true;
          if (client.clientToken && savedSessions.has(client.clientToken)) {
            savedSessions.get(client.clientToken).locked = true;
          }
          broadcastLobbyState();
          checkAllLockedShortenTimer();
        }
      } else if (msg.type === 'modify_bots') {
        if (!isHost || roomPhase !== 'lobby') return;
        const uniq = getUniqueClients();
        const bHum = uniq.filter(c => c.team === 'blue').length;
        const rHum = uniq.filter(c => c.team === 'red').length;
        if (msg.op === 'fill_5v5') {
          if (blueBots.length + bHum >= 5 && redBots.length + rHum >= 5) {
            blueBots = []; redBots = [];
          } else {
            while (blueBots.length + bHum < 5) blueBots.push(pickUnusedBotChamp());
            while (redBots.length + rHum < 5) redBots.push(pickUnusedBotChamp());
          }
        } else if (msg.op === 'fill_3v3') {
          blueBots = []; redBots = [];
          while (blueBots.length + bHum < 3) blueBots.push(pickUnusedBotChamp());
          while (redBots.length + rHum < 3) redBots.push(pickUnusedBotChamp());
        } else if (msg.op === 'clear_all') {
          blueBots = []; redBots = [];
        } else {
          const arr = msg.team === 'blue' ? blueBots : redBots;
          const teamHumans = msg.team === 'blue' ? bHum : rHum;
          if (msg.op === 'add') {
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
        }
        broadcastLobbyState();
      } else if (msg.type === 'set_ai_diff') {
        if (!isHost || roomPhase !== 'lobby') return;
        if (['easy', 'normal', 'hard'].includes(msg.aiDiff)) aiDiff = msg.aiDiff;
        broadcastLobbyState();
      } else if (msg.type === 'set_free_pick') {
        if (!isHost || roomPhase !== 'lobby') return;
        freePickMode = !!msg.freePickMode;
        broadcastLobbyState();
      } else if (msg.type === 'set_spells') {
        if (msg.spellD) client.spellD = msg.spellD;
        if (msg.spellF) client.spellF = msg.spellF;
        broadcastLobbyState();
      } else if (msg.type === 'chat') {
        const txt = String(msg.text || '').slice(0, 120).trim();
        if (txt) {
          broadcast({ type: 'chat', sender: client.name, text: txt, team: client.team });
        }
      } else if (msg.type === 'start_champ_select' || msg.type === 'start_game') {
        if (!isHost) {
          client.ready = !client.ready;
          broadcastLobbyState();
          return;
        }
        if (roomPhase === 'lobby') {
          if (msg.aiDiff && ['easy', 'normal', 'hard'].includes(msg.aiDiff)) aiDiff = msg.aiDiff;
          startChampSelectPhase();
        }
      } else if (msg.type === 'end_match') {
        if (isHost || getUniqueClients().length <= 1) {
          activeMatch = null;
          roomPhase = 'lobby';
          for (const c of clients.values()) {
            c.ready = false;
            c.locked = false;
          }
          broadcast({ type: 'match_ended' });
          broadcastLobbyState();
        }
      } else if (msg.type === 'player_action') {
        const cIdx = Number(msg.champIdx);
        if (activeMatch && Array.isArray(activeMatch.slots) && activeMatch.slots[cIdx]) {
          const slot = activeMatch.slots[cIdx];
          const ownsSlot = (slot.clientToken && slot.clientToken === client.clientToken) || (slot.playerId === client.id);
          const hostControllingBot = isHost && (!slot.isHuman || msg.fromBot);
          if (!ownsSlot && !hostControllingBot) return;
        }
        broadcast({
          type: 'player_action',
          champIdx: cIdx,
          fromBot: Boolean(msg.fromBot),
          action: msg.action,
          seq: serverSeq++,
          srvTs: Date.now()
        }, ws, false);
      } else if (msg.type === 'player_state') {
        const cIdx = Number(msg.champIdx);
        if (activeMatch && Array.isArray(activeMatch.slots) && activeMatch.slots[cIdx]) {
          const slot = activeMatch.slots[cIdx];
          const ownsSlot = (slot.clientToken && slot.clientToken === client.clientToken) || (slot.playerId === client.id);
          if (!ownsSlot && !isHost) return;
        }
        broadcast({
          type: 'player_state',
          champIdx: cIdx,
          state: msg.state,
          seq: serverSeq++,
          srvTs: Date.now()
        }, ws, true);
      } else if (msg.type === 'world_sync') {
        if (!isHost) return;
        if (activeMatch && msg.snapshot) {
          activeMatch.latestSnapshot = msg.snapshot;
          if (msg.snapshot.winner) {
            activeMatch = null;
            roomPhase = 'lobby';
          }
        }
        broadcast({
          type: 'world_sync',
          snapshot: msg.snapshot,
          seq: serverSeq++,
          srvTs: Date.now()
        }, ws, true);
      }
    } catch (e) {}
  };

  ws.onclose = () => {
    handleClientDeparture(ws, client, 'disconnect');
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
  console.log(`  Health Check:   http://${ip}:${PORT}/health`);
  console.log('═══════════════════════════════════════════════════════════════');
});