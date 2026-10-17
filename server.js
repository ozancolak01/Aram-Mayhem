// ═══════════════════════════════════════════════════════════════════════════
//  LEAGUE OF LEGENDS — ARAM: MAYHEM (MULTI-ROOM AUTHORITATIVE LAN SERVER)
//  Zero-Dependency Node.js HTTP + RFC-6455 WebSocket Server
//  Multi-Room Architecture:
//    - Players start in the Lobby Browser (roomId = null) — never forced into a single room.
//    - Players can Create New Rooms, Leave Rooms, Join Open Rooms, or Take Over Bots / Spectate Live Matches.
//    - Each Room has its own independent Phase ('lobby' | 'champ_select' | 'in_game'),
//      Host (tracked deterministically by hostToken & hostId), Bots, Benches, and 20Hz World Snapshot.
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

function shuffleArray(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

// ═══ MULTI-ROOM & CLIENT COORDINATOR ═══
const clients = new Map();       // ws -> client
const savedSessions = new Map(); // clientToken -> session
const rooms = new Map();         // roomId -> Room object
let nextPlayerId = 1;
let nextRoomSeq = 1;

function pickUnusedChamp(excludeSet = new Set()) {
  const pool = ALL_CHAMP_IDS.filter(id => !excludeSet.has(id));
  const src = pool.length > 0 ? pool : ALL_CHAMP_IDS;
  return src[Math.floor(Math.random() * src.length)];
}

function getUniqueAllClients() {
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

function getRoomClients(roomId) {
  if (!roomId) return [];
  const seen = new Set();
  const list = [];
  for (const c of clients.values()) {
    if (c.roomId !== roomId) continue;
    const k = c.clientToken || `id_${c.id}`;
    if (seen.has(k)) continue;
    seen.add(k);
    list.push(c);
  }
  return list;
}

function pickUnusedRoomBotChamp(room) {
  const rClients = getRoomClients(room.id);
  const used = new Set([
    ...rClients.map(c => c.champId).filter(Boolean),
    ...room.blueBots,
    ...room.redBots
  ]);
  return pickUnusedChamp(used);
}

function createRoom(hostClient, opts = {}) {
  const roomId = `room_${nextRoomSeq++}_${Math.random().toString(36).slice(2, 6)}`;
  const preset = opts.preset || '3v3';
  const room = {
    id: roomId,
    name: String(opts.name || `${hostClient ? hostClient.name : 'Sihirdar'} Lobisi`).slice(0, 28),
    hostId: hostClient ? hostClient.id : 1,
    hostToken: hostClient ? hostClient.clientToken : null,
    phase: 'lobby', // 'lobby' | 'champ_select' | 'in_game'
    champSelectTimer: 30,
    blueBench: [],
    redBench: [],
    blueBots: [],
    redBots: [],
    freePickMode: opts.freePickMode !== undefined ? Boolean(opts.freePickMode) : true,
    aiDiff: ['easy', 'normal', 'hard'].includes(opts.aiDiff) ? opts.aiDiff : 'normal',
    activeMatch: null,
    createdAt: Date.now()
  };

  if (preset === '5v5') {
    room.blueBots = ['blitz', 'jinxie', 'luxana', 'daren'];
    room.redBots = ['yasu', 'ahria', 'zedd', 'malph', 'kaisa'];
  } else if (preset === '1v1') {
    room.blueBots = [];
    room.redBots = ['yasu'];
  } else {
    room.blueBots = ['blitz', 'jinxie'];
    room.redBots = ['daren', 'ahria', 'yasu'];
  }

  rooms.set(roomId, room);
  return room;
}

function ensureRoomHost(room) {
  if (!room) return;
  const rClients = getRoomClients(room.id);
  if (rClients.length === 0) {
    rooms.delete(room.id);
    return;
  }
  const currentHostStillHere = rClients.find(
    c => (room.hostToken && c.clientToken === room.hostToken) || c.id === room.hostId
  );
  if (!currentHostStillHere) {
    const newHost = rClients[0];
    room.hostId = newHost.id;
    room.hostToken = newHost.clientToken;
    broadcastToRoom(room.id, {
      type: 'host_migrated',
      roomId: room.id,
      hostId: room.hostId,
      hostToken: room.hostToken
    });
  } else {
    room.hostId = currentHostStillHere.id;
    room.hostToken = currentHostStillHere.clientToken;
  }
}

function broadcastAll(msgObj, excludeWs = null) {
  const raw = JSON.stringify(msgObj);
  for (const [ws] of clients) {
    if (ws !== excludeWs && ws.readyState === 1) {
      ws.send(raw);
    }
  }
}

function broadcastToRoom(roomId, msgObj, excludeWs = null) {
  if (!roomId) return;
  const raw = JSON.stringify(msgObj);
  for (const [ws, c] of clients) {
    if (c.roomId === roomId && ws !== excludeWs && ws.readyState === 1) {
      ws.send(raw);
    }
  }
}

function buildRoomsListPayload() {
  const roomSummaries = [];
  for (const room of rooms.values()) {
    const rClients = getRoomClients(room.id);
    if (rClients.length === 0) {
      rooms.delete(room.id);
      continue;
    }
    ensureRoomHost(room);
    const hostClient = rClients.find(c => c.id === room.hostId) || rClients[0];
    const snap = room.activeMatch && room.activeMatch.latestSnapshot;
    const matchSlots = (room.activeMatch && Array.isArray(room.activeMatch.slots))
      ? room.activeMatch.slots.map((s, idx) => {
          const snapChamp = snap && Array.isArray(snap.champions) ? snap.champions[idx] : null;
          return {
            slotIdx: idx,
            team: s.team,
            specId: s.specId,
            isHuman: Boolean(s.isHuman),
            isPlayer: Boolean(s.isHuman),
            name: s.name || (snapChamp ? snapChamp.summonerName : null),
            summonerName: s.name || (snapChamp ? snapChamp.summonerName : null),
            level: snapChamp ? snapChamp.level : 3,
            kills: snapChamp ? snapChamp.kills : 0,
            deaths: snapChamp ? snapChamp.deaths : 0,
            assists: snapChamp ? snapChamp.assists : 0,
            gold: snapChamp ? snapChamp.gold : 1400,
            itemIds: snapChamp && Array.isArray(snapChamp.itemIds) ? snapChamp.itemIds : []
          };
        })
      : [];

    roomSummaries.push({
      id: room.id,
      name: room.name,
      hostId: room.hostId,
      hostToken: room.hostToken,
      hostName: hostClient ? hostClient.name : 'Sihirdar',
      phase: room.phase,
      humanCount: rClients.length,
      botCount: room.blueBots.length + room.redBots.length,
      freePickMode: room.freePickMode,
      aiDiff: room.aiDiff,
      matchInProgress: room.phase === 'in_game' && Boolean(room.activeMatch),
      matchInfo: room.activeMatch ? {
        time: snap ? snap.time : 0,
        blueKills: snap ? snap.blueKills : 0,
        redKills: snap ? snap.redKills : 0,
        hasOpenBotSlot: room.activeMatch.slots.some(s => !s.isHuman),
        players: room.activeMatch.slots.filter(s => s.isHuman).map(s => s.name),
        slots: matchSlots
      } : null
    });
  }

  const onlinePlayers = getUniqueAllClients().map(c => {
    const r = c.roomId ? rooms.get(c.roomId) : null;
    return {
      id: c.id,
      name: c.name,
      team: c.team,
      roomId: c.roomId || null,
      roomName: r ? r.name : null,
      phase: r ? r.phase : 'browser'
    };
  });

  return {
    type: 'rooms_list',
    rooms: roomSummaries,
    onlinePlayers
  };
}

function broadcastRoomsList() {
  broadcastAll(buildRoomsListPayload());
}

function buildRoomStatePayload(room) {
  if (!room) return null;
  ensureRoomHost(room);
  const rClients = getRoomClients(room.id);
  const players = rClients.map(c => ({
    id: c.id,
    clientToken: c.clientToken,
    name: c.name,
    champId: c.champId,
    team: c.team,
    ready: Boolean(c.ready),
    locked: Boolean(c.locked),
    rerolls: typeof c.rerolls === 'number' ? c.rerolls : 2,
    spellD: c.spellD,
    spellF: c.spellF
  }));

  const snap = room.activeMatch && room.activeMatch.latestSnapshot;
  const hasOpenBotSlot = Boolean(
    room.activeMatch && Array.isArray(room.activeMatch.slots) && room.activeMatch.slots.some(s => !s.isHuman)
  );

  const matchInfo = room.activeMatch ? {
    time: snap ? snap.time : 0,
    blueKills: snap ? snap.blueKills : 0,
    redKills: snap ? snap.redKills : 0,
    hasOpenBotSlot,
    players: room.activeMatch.slots.filter(s => s.isHuman).map(s => s.name),
    slots: room.activeMatch.slots.map((s, idx) => {
      const sc = snap && Array.isArray(snap.champions) ? snap.champions[idx] : null;
      return {
        slotIdx: idx,
        team: s.team,
        specId: s.specId,
        isHuman: Boolean(s.isHuman),
        isPlayer: Boolean(s.isHuman),
        name: s.name || (sc ? sc.summonerName : null),
        summonerName: s.name || (sc ? sc.summonerName : null),
        level: sc ? sc.level : 3,
        kills: sc ? sc.kills : 0,
        deaths: sc ? sc.deaths : 0,
        assists: sc ? sc.assists : 0,
        gold: sc ? sc.gold : 1400,
        itemIds: sc && Array.isArray(sc.itemIds) ? sc.itemIds : []
      };
    })
  } : null;

  return {
    type: 'lobby_state',
    roomId: room.id,
    roomName: room.name,
    phase: room.phase,
    champSelectTimer: room.champSelectTimer,
    hostId: room.hostId,
    hostToken: room.hostToken,
    players,
    blueBench: room.blueBench,
    redBench: room.redBench,
    bench: room.blueBench,
    blueBots: room.blueBots,
    redBots: room.redBots,
    freePickMode: room.freePickMode,
    aiDiff: room.aiDiff,
    matchInProgress: room.phase === 'in_game' && Boolean(room.activeMatch),
    matchInfo
  };
}

function broadcastRoomState(room) {
  if (!room) return;
  if (getRoomClients(room.id).length === 0) {
    rooms.delete(room.id);
    broadcastRoomsList();
    return;
  }
  const payload = buildRoomStatePayload(room);
  if (payload) broadcastToRoom(room.id, payload);
  broadcastRoomsList();
}

function removeClientFromCurrentRoom(client, ws = null) {
  const oldRoomId = client.roomId;
  if (!oldRoomId) return;
  client.roomId = null;
  client.ready = false;
  client.locked = false;
  if (client.clientToken && savedSessions.has(client.clientToken)) {
    savedSessions.get(client.clientToken).roomId = null;
    savedSessions.get(client.clientToken).locked = false;
  }

  const room = rooms.get(oldRoomId);
  if (!room) {
    broadcastRoomsList();
    return;
  }

  if (room.phase === 'in_game' && room.activeMatch && Array.isArray(room.activeMatch.slots)) {
    for (let i = 0; i < room.activeMatch.slots.length; i++) {
      const sl = room.activeMatch.slots[i];
      if ((client.clientToken && sl.clientToken === client.clientToken) || sl.playerId === client.id) {
        sl.isHuman = false;
        sl.playerId = -1;
        sl.clientToken = null;
        sl.name = null;
        if (room.activeMatch.latestSnapshot && Array.isArray(room.activeMatch.latestSnapshot.champions) && room.activeMatch.latestSnapshot.champions[i]) {
          room.activeMatch.latestSnapshot.champions[i].isPlayer = false;
          room.activeMatch.latestSnapshot.champions[i].playerId = -1;
          room.activeMatch.latestSnapshot.champions[i].clientToken = null;
        }
      }
    }
    broadcastToRoom(room.id, {
      type: 'player_left_match',
      playerId: client.id,
      clientToken: client.clientToken,
      name: client.name
    }, ws);
  }

  const remaining = getRoomClients(room.id);
  if (remaining.length === 0) {
    rooms.delete(room.id);
    broadcastRoomsList();
  } else {
    ensureRoomHost(room);
    if (room.phase === 'champ_select') {
      checkAllLockedShortenTimer(room);
    }
    broadcastRoomState(room);
  }
}

function assignClientToRoom(client, ws, room) {
  if (!room) return;
  if (client.roomId && client.roomId !== room.id) {
    removeClientFromCurrentRoom(client, ws);
  }
  const rClients = getRoomClients(room.id).filter(c => c.id !== client.id && c.clientToken !== client.clientToken);
  const blueCount = rClients.filter(c => c.team === 'blue').length;
  const redCount = rClients.filter(c => c.team === 'red').length;

  client.roomId = room.id;
  if (!client.team || (blueCount > redCount && client.team === 'blue' && blueCount >= 5)) {
    client.team = blueCount <= redCount ? 'blue' : 'red';
  }
  client.ready = false;
  client.locked = false;
  client.rerolls = 2;

  // Ensure bots don't exceed 5 per team when a human joins
  const newBlueHumans = getRoomClients(room.id).filter(c => c.team === 'blue').length;
  const newRedHumans = getRoomClients(room.id).filter(c => c.team === 'red').length;
  while (room.blueBots.length + newBlueHumans > 5 && room.blueBots.length > 0) room.blueBots.pop();
  while (room.redBots.length + newRedHumans > 5 && room.redBots.length > 0) room.redBots.pop();

  if (client.clientToken && savedSessions.has(client.clientToken)) {
    savedSessions.get(client.clientToken).roomId = room.id;
    savedSessions.get(client.clientToken).team = client.team;
  }
  ensureRoomHost(room);
  broadcastRoomState(room);
}

function startChampSelectPhase(room) {
  if (!room) return;
  room.phase = 'champ_select';
  room.champSelectTimer = 30;
  room.blueBench = [];
  room.redBench = [];

  const usedChamps = new Set();
  const shuffled = shuffleArray(ALL_CHAMP_IDS);
  let ptr = 0;

  for (const c of clients.values()) {
    if (c.roomId !== room.id) continue;
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

  room.blueBots = room.blueBots.map(() => {
    const cid = pickUnusedChamp(usedChamps);
    usedChamps.add(cid);
    return cid;
  });
  room.redBots = room.redBots.map(() => {
    const cid = pickUnusedChamp(usedChamps);
    usedChamps.add(cid);
    return cid;
  });

  broadcastRoomState(room);
}

function checkAllLockedShortenTimer(room) {
  if (!room || room.phase !== 'champ_select') return;
  const humans = getRoomClients(room.id);
  if (humans.length > 0 && humans.every(c => c.locked)) {
    if (room.champSelectTimer > 3) {
      room.champSelectTimer = 3;
      broadcastRoomState(room);
    }
  }
}

function buildMatchSlots(room) {
  const uniqueClients = getRoomClients(room.id);
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
  room.blueBots.slice(0, Math.max(0, 5 - bluePlayers.length)).forEach(cid => {
    slots.push({ team: 'blue', isHuman: false, playerId: -1, clientToken: null, name: null, specId: cid, spellD: 'snowball', spellF: 'flash' });
  });

  redPlayers.forEach(p => {
    slots.push({
      team: 'red', isHuman: true, playerId: p.id, clientToken: p.clientToken,
      name: p.name, specId: p.champId, spellD: p.spellD, spellF: p.spellF
    });
  });
  room.redBots.slice(0, Math.max(0, 5 - redPlayers.length)).forEach(cid => {
    slots.push({ team: 'red', isHuman: false, playerId: -1, clientToken: null, name: null, specId: cid, spellD: 'snowball', spellF: 'flash' });
  });

  if (!slots.some(s => s.team === 'red')) {
    slots.push({ team: 'red', isHuman: false, playerId: -1, clientToken: null, name: null, specId: pickUnusedRoomBotChamp(room), spellD: 'snowball', spellF: 'flash' });
  }
  if (!slots.some(s => s.team === 'blue')) {
    slots.push({ team: 'blue', isHuman: false, playerId: -1, clientToken: null, name: null, specId: pickUnusedRoomBotChamp(room), spellD: 'snowball', spellF: 'flash' });
  }

  return slots;
}

function launchMatchFromChampSelect(room) {
  if (!room) return;
  ensureRoomHost(room);
  room.phase = 'in_game';
  const slots = buildMatchSlots(room);
  const seed = Math.floor(Math.random() * 900000000) + 100000000;
  room.activeMatch = { aiDiff: room.aiDiff, slots, seed, latestSnapshot: null };
  for (const c of clients.values()) {
    if (c.roomId === room.id) {
      c.ready = false;
      c.locked = false;
    }
  }
  broadcastToRoom(room.id, {
    type: 'match_start',
    roomId: room.id,
    hostId: room.hostId,
    hostToken: room.hostToken,
    aiDiff: room.activeMatch.aiDiff,
    slots,
    seed
  });
  broadcastRoomState(room);
}

// Server-side 1-second ticker for Timed Champion Select across all active rooms
setInterval(() => {
  for (const room of [...rooms.values()]) {
    if (getRoomClients(room.id).length === 0) {
      rooms.delete(room.id);
      broadcastRoomsList();
      continue;
    }
    if (room.phase === 'champ_select') {
      room.champSelectTimer--;
      if (room.champSelectTimer <= 0) {
        launchMatchFromChampSelect(room);
      } else {
        broadcastToRoom(room.id, {
          type: 'champ_select_tick',
          roomId: room.id,
          champSelectTimer: room.champSelectTimer
        });
      }
    }
  }
}, 1000);

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url.startsWith('/index.html') || req.url.startsWith('/?')) {
    const candidates = [
      path.join(__dirname, 'public', 'index.html'),
      path.join(__dirname, 'aram_mayhem.html'),
      path.join(__dirname, 'index.html'),
      path.join(process.cwd(), 'public', 'index.html'),
      path.join(process.cwd(), 'aram_mayhem.html'),
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
  const client = {
    id,
    clientToken: null,
    roomId: null, // Starts in Lobby Browser! Never trapped in a single auto-lobby.
    name: `Oyuncu ${id}`,
    champId: randomStartChamp,
    team: 'blue',
    ready: false,
    locked: false,
    rerolls: 2,
    spellD: 'snowball',
    spellF: 'flash'
  };
  clients.set(ws, client);

  ws.onmessage = (raw) => {
    try {
      const msg = JSON.parse(raw);

      if (msg.type === 'ping_hb') {
        ws.send({ type: 'pong_hb', ts: msg.ts });
        return;
      }

      if (msg.type === 'join_lobby' || msg.type === 'hello') {
        const token = msg.clientToken ? String(msg.clientToken).slice(0, 64) : null;
        if (token) {
          client.clientToken = token;
          for (const [otherWs, otherClient] of [...clients.entries()]) {
            if (otherWs !== ws && otherClient.clientToken === token) {
              client.id = otherClient.id;
              client.roomId = otherClient.roomId;
              client.name = otherClient.name;
              client.champId = otherClient.champId;
              client.team = otherClient.team;
              client.ready = otherClient.ready;
              client.locked = otherClient.locked;
              client.rerolls = otherClient.rerolls;
              clients.delete(otherWs);
              otherWs.close();
            }
          }
          const prevSession = savedSessions.get(token);
          if (prevSession) {
            client.id = prevSession.id;
            client.name = prevSession.name || client.name;
            client.champId = prevSession.champId || client.champId;
            client.team = prevSession.team || client.team;
            client.spellD = prevSession.spellD || 'snowball';
            client.spellF = prevSession.spellF || 'flash';
            if (prevSession.roomId && rooms.has(prevSession.roomId)) {
              client.roomId = prevSession.roomId;
              client.locked = Boolean(prevSession.locked);
              client.rerolls = typeof prevSession.rerolls === 'number' ? prevSession.rerolls : 2;
            }
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
            team: client.team, roomId: client.roomId, locked: client.locked, rerolls: client.rerolls,
            spellD: client.spellD, spellF: client.spellF, lastSeen: Date.now()
          });
        }

        ws.send({
          type: 'welcome',
          playerId: client.id,
          clientToken: client.clientToken,
          name: client.name,
          roomId: client.roomId
        });

        if (client.roomId && rooms.has(client.roomId)) {
          const room = rooms.get(client.roomId);
          ensureRoomHost(room);
          broadcastRoomState(room);
          if (room.phase === 'in_game' && room.activeMatch && Array.isArray(room.activeMatch.slots)) {
            const inActiveMatch = room.activeMatch.slots.find(
              s => (s.clientToken && s.clientToken === client.clientToken) || (s.playerId && s.playerId === client.id)
            );
            if (inActiveMatch) {
              inActiveMatch.playerId = client.id;
              inActiveMatch.clientToken = client.clientToken;
              ws.send({
                type: 'match_start',
                roomId: room.id,
                hostId: room.hostId,
                hostToken: room.hostToken,
                aiDiff: room.activeMatch.aiDiff,
                slots: room.activeMatch.slots,
                seed: room.activeMatch.seed,
                snapshot: room.activeMatch.latestSnapshot || null
              });
            }
          }
        } else {
          client.roomId = null;
          broadcastRoomsList();
        }
        return;
      }

      if (msg.type === 'get_rooms') {
        ws.send(buildRoomsListPayload());
        return;
      }

      if (msg.type === 'create_room') {
        removeClientFromCurrentRoom(client, ws);
        const room = createRoom(client, {
          name: msg.name || `${client.name} Lobisi`,
          preset: msg.preset || '3v3',
          freePickMode: msg.freePickMode !== undefined ? msg.freePickMode : true,
          aiDiff: msg.aiDiff || 'normal'
        });
        client.team = 'blue';
        assignClientToRoom(client, ws, room);
        return;
      }

      if (msg.type === 'join_room') {
        const targetRoom = rooms.get(msg.roomId);
        if (!targetRoom) {
          ws.send(buildRoomsListPayload());
          return;
        }
        if (targetRoom.phase === 'in_game' && targetRoom.activeMatch) {
          // Joining an in-progress room routes to rejoin_match
          assignClientToRoom(client, ws, targetRoom);
          return;
        }
        assignClientToRoom(client, ws, targetRoom);
        return;
      }

      if (msg.type === 'leave_room' || msg.type === 'leave_lobby') {
        removeClientFromCurrentRoom(client, ws);
        ws.send({ type: 'left_room' });
        ws.send(buildRoomsListPayload());
        return;
      }

      if (msg.type === 'rejoin_match') {
        const targetRoomId = msg.roomId || client.roomId;
        const room = targetRoomId ? rooms.get(targetRoomId) : null;
        if (!room || !room.activeMatch || !Array.isArray(room.activeMatch.slots)) {
          ws.send(buildRoomsListPayload());
          return;
        }
        if (client.roomId !== room.id) {
          client.roomId = room.id;
          if (client.clientToken && savedSessions.has(client.clientToken)) {
            savedSessions.get(client.clientToken).roomId = room.id;
          }
          ensureRoomHost(room);
        }

        let mySlotIdx = room.activeMatch.slots.findIndex(
          s => (s.clientToken && s.clientToken === client.clientToken) || (s.playerId && s.playerId === client.id)
        );
        let tookOverBot = false;

        if (mySlotIdx === -1 && !msg.spectateOnly) {
          if (typeof msg.targetSlotIdx === 'number' && room.activeMatch.slots[msg.targetSlotIdx] && !room.activeMatch.slots[msg.targetSlotIdx].isHuman) {
            mySlotIdx = msg.targetSlotIdx;
          } else {
            mySlotIdx = room.activeMatch.slots.findIndex(s => !s.isHuman && s.team === client.team);
            if (mySlotIdx === -1) mySlotIdx = room.activeMatch.slots.findIndex(s => !s.isHuman);
          }
          if (mySlotIdx !== -1) {
            const sl = room.activeMatch.slots[mySlotIdx];
            sl.isHuman = true;
            sl.playerId = client.id;
            sl.clientToken = client.clientToken;
            sl.name = client.name;
            client.team = sl.team;
            client.champId = sl.specId;
            tookOverBot = true;
            if (room.activeMatch.latestSnapshot && Array.isArray(room.activeMatch.latestSnapshot.champions) && room.activeMatch.latestSnapshot.champions[mySlotIdx]) {
              const sc = room.activeMatch.latestSnapshot.champions[mySlotIdx];
              sc.isPlayer = true;
              sc.playerId = client.id;
              sc.clientToken = client.clientToken;
              sc.summonerName = client.name;
            }
          }
        }

        const mySlot = mySlotIdx !== -1 ? room.activeMatch.slots[mySlotIdx] : null;
        ws.send({
          type: 'match_start',
          roomId: room.id,
          hostId: room.hostId,
          hostToken: room.hostToken,
          aiDiff: room.activeMatch.aiDiff,
          slots: room.activeMatch.slots,
          seed: room.activeMatch.seed,
          snapshot: room.activeMatch.latestSnapshot || null,
          spectator: Boolean(msg.spectateOnly || !mySlot)
        });

        if (mySlot && mySlotIdx !== -1) {
          broadcastToRoom(room.id, {
            type: 'player_joined_match',
            slotIdx: mySlotIdx,
            playerId: client.id,
            clientToken: client.clientToken,
            name: client.name,
            tookOverBot
          }, ws);
        }
        broadcastRoomState(room);
        return;
      }

      if (msg.type === 'set_name') {
        client.name = String(msg.name || '').slice(0, 16) || client.name;
        if (client.clientToken && savedSessions.has(client.clientToken)) {
          savedSessions.get(client.clientToken).name = client.name;
        }
        if (client.roomId && rooms.has(client.roomId)) {
          broadcastRoomState(rooms.get(client.roomId));
        } else {
          broadcastRoomsList();
        }
        return;
      }

      // All room-specific actions require the client to be inside a valid room
      const room = client.roomId ? rooms.get(client.roomId) : null;
      if (!room) return;
      ensureRoomHost(room);
      const isHost = (room.hostToken && client.clientToken === room.hostToken) || (client.id === room.hostId);

      if (msg.type === 'toggle_ready') {
        if (room.phase === 'lobby') {
          client.ready = Boolean(msg.ready);
          broadcastRoomState(room);
        }
      } else if (msg.type === 'switch_team') {
        if (room.phase === 'lobby') {
          client.team = client.team === 'blue' ? 'red' : 'blue';
          client.ready = false;
          if (client.clientToken && savedSessions.has(client.clientToken)) {
            savedSessions.get(client.clientToken).team = client.team;
          }
          broadcastRoomState(room);
        }
      } else if (msg.type === 'kick_player') {
        if (!isHost) return;
        const targetId = Number(msg.targetId);
        for (const [tWs, tClient] of clients.entries()) {
          if (tClient.roomId === room.id && tClient.id === targetId && tClient.id !== client.id) {
            removeClientFromCurrentRoom(tClient, tWs);
            tWs.send({ type: 'kicked', reason: 'Oda kurucusu seni lobiden çıkardı.' });
            tWs.send(buildRoomsListPayload());
          }
        }
        broadcastRoomState(room);
      } else if (msg.type === 'select_champ') {
        if (room.phase === 'champ_select' && room.freePickMode && !client.locked && ALL_CHAMP_IDS.includes(msg.champId)) {
          const teamLocked = getRoomClients(room.id).some(
            other => other.id !== client.id && other.team === client.team && other.locked && other.champId === msg.champId
          );
          if (!teamLocked) {
            client.champId = msg.champId;
            if (client.clientToken && savedSessions.has(client.clientToken)) {
              savedSessions.get(client.clientToken).champId = client.champId;
            }
            broadcastRoomState(room);
          }
        }
      } else if (msg.type === 'reroll_champ') {
        if (room.phase === 'champ_select' && !client.locked && client.rerolls > 0) {
          const teamBench = client.team === 'blue' ? room.blueBench : room.redBench;
          const teamUsed = new Set([
            ...getRoomClients(room.id).filter(c => c.team === client.team).map(c => c.champId),
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
            broadcastRoomState(room);
          }
        }
      } else if (msg.type === 'swap_bench') {
        if (room.phase === 'champ_select' && !client.locked) {
          const teamBench = client.team === 'blue' ? room.blueBench : room.redBench;
          const idx = Number(msg.benchIdx);
          if (idx >= 0 && idx < teamBench.length) {
            const prev = client.champId;
            client.champId = teamBench[idx];
            teamBench[idx] = prev;
            if (client.clientToken && savedSessions.has(client.clientToken)) {
              savedSessions.get(client.clientToken).champId = client.champId;
            }
            broadcastRoomState(room);
          }
        }
      } else if (msg.type === 'lock_champ') {
        if (room.phase === 'champ_select') {
          if (!client.champId || !ALL_CHAMP_IDS.includes(client.champId)) {
            client.champId = pickUnusedChamp(new Set(getRoomClients(room.id).map(c => c.champId)));
          }
          client.locked = true;
          if (client.clientToken && savedSessions.has(client.clientToken)) {
            savedSessions.get(client.clientToken).locked = true;
          }
          broadcastRoomState(room);
          checkAllLockedShortenTimer(room);
        }
      } else if (msg.type === 'modify_bots') {
        if (!isHost || room.phase !== 'lobby') return;
        const uniq = getRoomClients(room.id);
        const bHum = uniq.filter(c => c.team === 'blue').length;
        const rHum = uniq.filter(c => c.team === 'red').length;
        if (msg.op === 'fill_5v5') {
          if (room.blueBots.length + bHum >= 5 && room.redBots.length + rHum >= 5) {
            room.blueBots = []; room.redBots = [];
          } else {
            while (room.blueBots.length + bHum < 5) room.blueBots.push(pickUnusedRoomBotChamp(room));
            while (room.redBots.length + rHum < 5) room.redBots.push(pickUnusedRoomBotChamp(room));
          }
        } else if (msg.op === 'fill_3v3') {
          room.blueBots = []; room.redBots = [];
          while (room.blueBots.length + bHum < 3) room.blueBots.push(pickUnusedRoomBotChamp(room));
          while (room.redBots.length + rHum < 3) room.redBots.push(pickUnusedRoomBotChamp(room));
        } else if (msg.op === 'clear_all') {
          room.blueBots = []; room.redBots = [];
        } else {
          const arr = msg.team === 'blue' ? room.blueBots : room.redBots;
          const teamHumans = msg.team === 'blue' ? bHum : rHum;
          if (msg.op === 'add') {
            if (teamHumans + arr.length < 5) arr.push(pickUnusedRoomBotChamp(room));
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
        broadcastRoomState(room);
      } else if (msg.type === 'set_ai_diff') {
        if (!isHost || room.phase !== 'lobby') return;
        if (['easy', 'normal', 'hard'].includes(msg.aiDiff)) room.aiDiff = msg.aiDiff;
        broadcastRoomState(room);
      } else if (msg.type === 'set_free_pick') {
        if (!isHost || room.phase !== 'lobby') return;
        room.freePickMode = !!msg.freePickMode;
        broadcastRoomState(room);
      } else if (msg.type === 'set_spells') {
        if (msg.spellD) client.spellD = msg.spellD;
        if (msg.spellF) client.spellF = msg.spellF;
        broadcastRoomState(room);
      } else if (msg.type === 'chat') {
        const txt = String(msg.text || '').slice(0, 120).trim();
        if (txt) {
          broadcastToRoom(room.id, { type: 'chat', sender: client.name, text: txt, team: client.team });
        }
      } else if (msg.type === 'start_champ_select' || msg.type === 'start_game') {
        if (!isHost) {
          client.ready = !client.ready;
          broadcastRoomState(room);
          return;
        }
        if (room.phase === 'lobby') {
          if (msg.aiDiff && ['easy', 'normal', 'hard'].includes(msg.aiDiff)) room.aiDiff = msg.aiDiff;
          startChampSelectPhase(room);
        }
      } else if (msg.type === 'end_match') {
        if (isHost || getRoomClients(room.id).length <= 1) {
          room.activeMatch = null;
          room.phase = 'lobby';
          for (const c of clients.values()) {
            if (c.roomId === room.id) {
              c.ready = false;
              c.locked = false;
            }
          }
          broadcastToRoom(room.id, { type: 'match_ended', roomId: room.id });
          broadcastRoomState(room);
        }
      } else if (msg.type === 'player_action') {
        broadcastToRoom(room.id, {
          type: 'player_action',
          champIdx: msg.champIdx,
          fromBot: Boolean(msg.fromBot),
          action: msg.action
        }, ws);
      } else if (msg.type === 'player_state') {
        broadcastToRoom(room.id, {
          type: 'player_state',
          champIdx: msg.champIdx,
          state: msg.state
        }, ws);
      } else if (msg.type === 'world_sync') {
        if (room.activeMatch && msg.snapshot) {
          room.activeMatch.latestSnapshot = msg.snapshot;
          if (msg.snapshot.winner) {
            room.activeMatch = null;
            room.phase = 'lobby';
          }
        }
        if (isHost) {
          broadcastToRoom(room.id, {
            type: 'world_sync',
            hostId: room.hostId,
            hostToken: room.hostToken,
            snapshot: msg.snapshot
          }, ws);
        }
      }
    } catch (e) {}
  };

  ws.onclose = () => {
    if (client.clientToken) {
      savedSessions.set(client.clientToken, {
        id: client.id, name: client.name, champId: client.champId,
        team: client.team, roomId: client.roomId, locked: client.locked, rerolls: client.rerolls,
        spellD: client.spellD, spellF: client.spellF, lastSeen: Date.now()
      });
    }
    const oldRoomId = client.roomId;
    clients.delete(ws);

    if (oldRoomId && rooms.has(oldRoomId)) {
      const room = rooms.get(oldRoomId);
      const stillInRoom = getRoomClients(oldRoomId);
      if (stillInRoom.length === 0) {
        rooms.delete(oldRoomId);
        broadcastRoomsList();
      } else {
        // Check if this clientToken still has another active socket in the room
        const tokenStillConnected = stillInRoom.some(c => c.clientToken && c.clientToken === client.clientToken);
        if (!tokenStillConnected && room.phase === 'in_game' && room.activeMatch && Array.isArray(room.activeMatch.slots)) {
          for (let i = 0; i < room.activeMatch.slots.length; i++) {
            const sl = room.activeMatch.slots[i];
            if ((client.clientToken && sl.clientToken === client.clientToken) || sl.playerId === client.id) {
              sl.isHuman = false;
              sl.playerId = -1;
              sl.clientToken = null;
              sl.name = null;
            }
          }
          broadcastToRoom(oldRoomId, {
            type: 'player_left_match',
            playerId: client.id,
            clientToken: client.clientToken,
            name: client.name
          });
        }
        ensureRoomHost(room);
        if (room.phase === 'champ_select') {
          checkAllLockedShortenTimer(room);
        }
        broadcastRoomState(room);
      }
    } else {
      broadcastRoomsList();
    }
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
  console.log('  LEAGUE OF LEGENDS — ARAM: MAYHEM (MULTI-ROOM LAN SERVER)');
  console.log(`  Local PC:       http://localhost:${PORT}`);
  console.log(`  LAN / Mobile:   http://${ip}:${PORT}  <-- Open on phones/PCs!`);
  console.log('═══════════════════════════════════════════════════════════════');
});
