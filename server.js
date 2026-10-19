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
const zlib = require('zlib');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const WS_MAGIC = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// ── Network tuning ──────────────────────────────────────────────────────────
const MAX_MESSAGE_BYTES   = 512 * 1024;       // largest accepted WebSocket message (a full world snapshot is ~30-60 KB)
const MAX_CLIENTS         = 500;              // hard cap on simultaneous sockets
const SYNC_BACKLOG_LIMIT  = 128 * 1024;       // if a socket has more than this queued, stale snapshots are dropped
const MAX_BACKLOG_BYTES   = 8 * 1024 * 1024;  // a socket this far behind is dead: drop it instead of leaking memory
const RATE_BURST          = 400;              // per-client message token bucket (host needs ~45 msg/s)
const RATE_PER_SEC        = 200;
const HEARTBEAT_MS        = 15000;            // server -> client WebSocket ping (keeps Fly/Render proxies from idling us out)
const DEAD_AFTER_MS       = 60000;            // no bytes (not even a pong) for this long = dead socket
const SLOT_GRACE_MS       = Number(process.env.SLOT_GRACE_MS) || 25000;            // a disconnected player keeps their champion this long before a bot takes over
const EMPTY_ROOM_GRACE = { lobby: 15000, champ_select: 30000, in_game: 120000 };

const PUBLIC_DIR = path.join(__dirname, 'public');

const ALL_CHAMP_IDS = [
  'luxana', 'yasu', 'jinxie', 'blitz', 'zedd', 'ahria',
  'daren', 'ezre', 'morga', 'pykeo', 'veiga', 'ashela', 'kata', 'thresh', 'mf', 'malph',
  'kaisa', 'jhin', 'sylas', 'leesin', 'vayne', 'sett', 'leona', 'viktor'
];

// ═══ RFC 6455 WebSocket (server side, zero dependencies) ═══
// Handles: masked-frame validation, fragmentation, ping/pong, close handshake,
// frame-size limit, per-client rate limit, send backpressure and snapshot coalescing.
class SimpleWebSocketClient {
  constructor(socket) {
    this.socket = socket;
    this.readyState = 1;               // 1 open, 2 closing, 3 closed
    this.buffer = Buffer.alloc(0);
    this.fragments = [];
    this.fragBytes = 0;
    this.fragOpcode = 0;
    this.onmessage = null;
    this.onclose = null;
    this.lastSeen = Date.now();
    this.pendingSync = null;           // newest world snapshot we could not send yet (older ones are dropped)
    this._closeFired = false;
    this._tokens = RATE_BURST;
    this._lastRefill = Date.now();
    this._starvedSince = 0;

    socket.setNoDelay(true);           // no Nagle: 20 Hz small frames must not be batched
    socket.setKeepAlive(true, 20000);

    socket.on('data', chunk => {
      this.lastSeen = Date.now();
      this.buffer = this.buffer.length ? Buffer.concat([this.buffer, chunk]) : chunk;
      try { this._parseFrames(); } catch (e) { this._fail(1002); }
    });
    socket.on('drain', () => this._flushPending());
    // http.Server sockets are half-open: a peer that vanishes with a bare TCP FIN (killed browser, dropped Wi-Fi)
    // only emits 'end', never 'close'. Without this the player lingers as a ghost until the heartbeat reaps them.
    socket.on('end', () => this.terminate());
    socket.on('close', () => this._fireClose());
    socket.on('error', () => { this._fireClose(); try { socket.destroy(); } catch (e) {} });
  }

  get backlog() { return this.socket.writableLength || 0; }

  _fireClose() {
    if (this._closeFired) return;
    this._closeFired = true;
    this.readyState = 3;
    this.pendingSync = null;
    if (this.onclose) { try { this.onclose(); } catch (e) { console.error('[ws onclose]', e); } }
  }

  _allow() {
    const now = Date.now();
    this._tokens = Math.min(RATE_BURST, this._tokens + (now - this._lastRefill) * RATE_PER_SEC / 1000);
    this._lastRefill = now;
    if (this._tokens >= 1) { this._tokens -= 1; this._starvedSince = 0; return true; }
    if (!this._starvedSince) this._starvedSince = now;
    else if (now - this._starvedSince > 5000) this._fail(1008);   // sustained flooding
    return false;
  }

  _deliver(opcode, payload) {
    if (opcode !== 0x01) return;       // binary frames are not part of our protocol
    if (!this._allow()) return;
    if (this.onmessage) this.onmessage(payload.toString('utf8'));
  }

  _parseFrames() {
    while (this.readyState === 1 && this.buffer.length >= 2) {
      const b0 = this.buffer[0];
      const b1 = this.buffer[1];
      const fin = (b0 & 0x80) !== 0;
      const opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f;
      let offset = 2;

      // No extensions are negotiated (RSV must be 0) and browsers always mask client frames.
      if ((b0 & 0x70) !== 0 || !masked) return this._fail(1002);

      if (len === 126) {
        if (this.buffer.length < 4) return;
        len = this.buffer.readUInt16BE(2);
        offset = 4;
      } else if (len === 127) {
        if (this.buffer.length < 10) return;
        const big = this.buffer.readBigUInt64BE(2);
        if (big > BigInt(MAX_MESSAGE_BYTES)) return this._fail(1009);
        len = Number(big);
        offset = 10;
      }
      if (len > MAX_MESSAGE_BYTES) return this._fail(1009);                 // reject BEFORE buffering the payload
      if (opcode >= 0x08 && (!fin || len > 125)) return this._fail(1002);   // control frames: single, small

      if (this.buffer.length < offset + 4 + len) return;                    // wait for the rest of the frame

      const mask = this.buffer.subarray(offset, offset + 4);
      offset += 4;
      const payload = Buffer.from(this.buffer.subarray(offset, offset + len));
      this.buffer = this.buffer.subarray(offset + len);
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];

      if (opcode === 0x08) {                       // close
        this.close(1000);
        return;
      } else if (opcode === 0x09) {                // ping -> pong
        this._sendFrame(0x0a, payload);
      } else if (opcode === 0x0a) {                // pong (lastSeen already refreshed)
        // nothing to do
      } else if (opcode === 0x01 || opcode === 0x02) {
        if (this.fragOpcode) return this._fail(1002);
        if (fin) {
          this._deliver(opcode, payload);
        } else {
          this.fragOpcode = opcode;
          this.fragments = [payload];
          this.fragBytes = len;
        }
      } else if (opcode === 0x00) {                // continuation
        if (!this.fragOpcode) return this._fail(1002);
        this.fragBytes += len;
        if (this.fragBytes > MAX_MESSAGE_BYTES) return this._fail(1009);
        this.fragments.push(payload);
        if (fin) {
          const full = Buffer.concat(this.fragments);
          const op = this.fragOpcode;
          this.fragOpcode = 0; this.fragments = []; this.fragBytes = 0;
          this._deliver(op, full);
        }
      } else {
        return this._fail(1002);
      }
    }
  }

  send(data) {
    if (this.readyState !== 1) return;
    const buf = Buffer.from(typeof data === 'string' ? data : JSON.stringify(data), 'utf8');
    this._sendFrame(0x01, buf);
    if (this.backlog > MAX_BACKLOG_BYTES) this.terminate();   // hopelessly slow peer: free the memory
  }

  // For high-rate, latest-wins data (world snapshots). Never lets stale frames pile up in the socket queue.
  sendLatest(raw) {
    if (this.readyState !== 1) return;
    if (this.backlog > SYNC_BACKLOG_LIMIT) {
      this.pendingSync = raw;          // overwrites any older pending snapshot
      return;
    }
    this.pendingSync = null;
    this.send(raw);
  }

  _flushPending() {
    if (this.pendingSync !== null && this.readyState === 1) {
      const raw = this.pendingSync;
      this.pendingSync = null;
      this.send(raw);
    }
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

  ping() { if (this.readyState === 1) this._sendFrame(0x09, Buffer.alloc(0)); }

  close(code = 1000) {
    if (this.readyState !== 1) return;
    this.readyState = 2;
    this.buffer = Buffer.alloc(0);
    try {
      const body = Buffer.alloc(2);
      body.writeUInt16BE(code, 0);
      this._sendFrame(0x08, body);
      this.socket.end();
    } catch (e) {}
    const t = setTimeout(() => { try { this.socket.destroy(); } catch (e) {} }, 2000);
    if (t.unref) t.unref();
  }

  terminate() {
    try { this.socket.destroy(); } catch (e) {}
    this._fireClose();
  }

  _fail(code) { this.close(code); }
}

// The host streams a full world snapshot ~20x/s. We forward the raw JSON text untouched and only parse it
// lazily (rooms list / late joiners), so the hot path costs almost nothing.
class ActiveMatch {
  constructor(aiDiff, slots, seed) {
    this.aiDiff = aiDiff;
    this.slots = slots;
    this.seed = seed;
    this._snap = null;
    this._raw = null;
    this.lastSyncAt = 0;
  }
  get latestSnapshot() {
    if (this._raw !== null) {
      try { const parsed = JSON.parse(this._raw); if (parsed && parsed.snapshot) this._snap = parsed.snapshot; } catch (e) {}
      this._raw = null;
    }
    return this._snap;
  }
  set latestSnapshot(v) { this._snap = v; this._raw = null; }
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
    if (!room.emptySince) room.emptySince = Date.now();   // swept by the ticker after a grace period
    return;
  }
  room.emptySince = null;
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
    if (rClients.length === 0) continue;   // empty rooms are hidden from the browser; the ticker deletes them after a grace period
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

    const totalSlots = (room.phase === 'in_game' && room.activeMatch && Array.isArray(room.activeMatch.slots))
      ? room.activeMatch.slots.length
      : (rClients.length + room.blueBots.length + room.redBots.length);

    roomSummaries.push({
      id: room.id,
      roomId: room.id,
      name: room.name,
      hostId: room.hostId,
      hostToken: room.hostToken,
      hostName: hostClient ? hostClient.name : 'Sihirdar',
      phase: room.phase,
      humanCount: rClients.length,
      botCount: room.blueBots.length + room.redBots.length,
      totalSlots,
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
    const st = r ? r.phase : 'browser';
    return {
      id: c.id,
      name: c.name,
      team: c.team,
      roomId: c.roomId || null,
      roomName: r ? r.name : null,
      phase: st,
      status: st
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
    if (!room.emptySince) room.emptySince = Date.now();
    broadcastRoomsList();
    return;
  }
  const payload = buildRoomStatePayload(room);
  if (payload) broadcastToRoom(room.id, payload);
  broadcastRoomsList();
}

function freeMatchSlot(room, i) {
  const sl = room.activeMatch.slots[i];
  const info = { playerId: sl.playerId, clientToken: sl.clientToken, name: sl.name };
  sl.isHuman = false;
  sl.playerId = -1;
  sl.clientToken = null;
  sl.name = null;
  delete sl.disconnectedAt;
  const snap = room.activeMatch.latestSnapshot;
  if (snap && Array.isArray(snap.champions) && snap.champions[i]) {
    snap.champions[i].isPlayer = false;
    snap.champions[i].playerId = -1;
    snap.champions[i].clientToken = null;
  }
  return info;
}

function slotBelongsTo(sl, client) {
  return Boolean((client.clientToken && sl.clientToken === client.clientToken) || (sl.playerId && sl.playerId === client.id));
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
    // Explicit leave: the bot takes over immediately (no grace period).
    for (let i = 0; i < room.activeMatch.slots.length; i++) {
      if (slotBelongsTo(room.activeMatch.slots[i], client)) freeMatchSlot(room, i);
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
    room.emptySince = Date.now();
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
  if (room.phase === 'champ_select') {
    const used = new Set([
      ...getRoomClients(room.id).filter(c => c.id !== client.id).map(c => c.champId).filter(Boolean),
      ...room.blueBench,
      ...room.redBench
    ]);
    if (!client.champId || used.has(client.champId)) {
      client.champId = pickUnusedChamp(used);
    }
  }

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
  room.activeMatch = new ActiveMatch(room.aiDiff, slots, seed);
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

// Server-side 1-second ticker: champion-select countdown, empty-room sweep, disconnect grace for players.
setInterval(() => {
  const now = Date.now();
  for (const room of [...rooms.values()]) {
    try {
      if (getRoomClients(room.id).length === 0) {
        if (!room.emptySince) room.emptySince = now;
        const grace = EMPTY_ROOM_GRACE[room.phase] || EMPTY_ROOM_GRACE.lobby;
        if (now - room.emptySince > grace) {
          rooms.delete(room.id);
          broadcastRoomsList();
        }
        continue;
      }
      room.emptySince = null;

      if (room.phase === 'in_game' && room.activeMatch && Array.isArray(room.activeMatch.slots)) {
        let released = false;
        for (let i = 0; i < room.activeMatch.slots.length; i++) {
          const sl = room.activeMatch.slots[i];
          if (sl.isHuman && sl.disconnectedAt && now - sl.disconnectedAt > SLOT_GRACE_MS) {
            const info = freeMatchSlot(room, i);
            broadcastToRoom(room.id, { type: 'player_left_match', playerId: info.playerId, clientToken: info.clientToken, name: info.name });
            released = true;
          }
        }
        if (released) broadcastRoomState(room);
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
    } catch (e) {
      console.error('[ticker]', room && room.id, e);
    }
  }
}, 1000);

// Forget sessions of players who have been gone for hours.
setInterval(() => {
  const live = new Set();
  for (const c of clients.values()) if (c.clientToken) live.add(c.clientToken);
  const cutoff = Date.now() - 6 * 3600 * 1000;
  for (const [token, sess] of savedSessions) {
    if (!live.has(token) && (sess.lastSeen || 0) < cutoff) savedSessions.delete(token);
  }
}, 10 * 60 * 1000);

// Server-side WebSocket heartbeat. Browsers answer pings at the network layer even when the tab's JS is
// throttled in the background, so this keeps proxies from idling the connection out and reaps dead sockets.
setInterval(() => {
  const now = Date.now();
  for (const ws of [...clients.keys()]) {
    if (now - ws.lastSeen > DEAD_AFTER_MS) ws.terminate();
    else ws.ping();
  }
}, HEARTBEAT_MS);

// Latest-wins fan-out of the host's world snapshot (the raw JSON string is forwarded untouched).
function handleWorldSyncRaw(ws, client, raw) {
  const room = client.roomId ? rooms.get(client.roomId) : null;
  if (!room || room.phase !== 'in_game' || !room.activeMatch) return;
  const isHost = (room.hostToken && client.clientToken === room.hostToken) || client.id === room.hostId;
  if (!isHost) return;                          // only the current host may publish world state
  const match = room.activeMatch;
  match._raw = raw;
  match.lastSyncAt = Date.now();
  const ended = raw.includes('"winner":"');
  for (const [otherWs, c] of clients) {
    if (c.roomId === room.id && otherWs !== ws) otherWs.sendLatest(raw);
  }
  if (ended) {
    room.activeMatch = null;
    room.phase = 'lobby';
    for (const c of clients.values()) {
      if (c.roomId === room.id) { c.ready = false; c.locked = false; }
    }
    broadcastRoomState(room);
  }
}

// Small, high-rate packets (positions) are simply skipped for clients that are already backed up.
function broadcastDroppable(roomId, msgObj, excludeWs = null) {
  const raw = JSON.stringify(msgObj);
  for (const [ws, c] of clients) {
    if (c.roomId === roomId && ws !== excludeWs && ws.readyState === 1 && ws.backlog < SYNC_BACKLOG_LIMIT) ws.send(raw);
  }
}

// ═══ STATIC FILES (everything inside /public) ═══
const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8', '.txt': 'text/plain; charset=utf-8',
  '.svg': 'image/svg+xml', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.gif': 'image/gif',
  '.webp': 'image/webp', '.ico': 'image/x-icon', '.mp3': 'audio/mpeg', '.ogg': 'audio/ogg', '.wav': 'audio/wav',
  '.woff': 'font/woff', '.woff2': 'font/woff2', '.ttf': 'font/ttf', '.map': 'application/json; charset=utf-8',
  '.webmanifest': 'application/manifest+json'
};
const COMPRESSIBLE = new Set(['.html', '.js', '.mjs', '.css', '.json', '.txt', '.svg', '.map', '.webmanifest']);
const staticCache = new Map();    // absolute path -> { mtimeMs, size, raw, gzip, etag, type, cache }

function sendText(res, code, text) {
  res.writeHead(code, { 'Content-Type': 'text/plain; charset=utf-8', 'X-Content-Type-Options': 'nosniff' });
  res.end(text);
}

function serveStatic(req, res) {
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    res.writeHead(405, { Allow: 'GET, HEAD' });
    res.end();
    return;
  }
  let urlPath;
  try {
    urlPath = decodeURIComponent(new URL(req.url, 'http://localhost').pathname);
  } catch (e) { return sendText(res, 400, 'Bad Request'); }
  if (urlPath.includes('\0')) return sendText(res, 400, 'Bad Request');

  if (urlPath === '/healthz') return sendText(res, 200, 'ok');
  if (urlPath === '/' || urlPath === '/aram_mayhem.html') urlPath = '/index.html';

  let abs = path.normalize(path.join(PUBLIC_DIR, urlPath));
  if (abs !== PUBLIC_DIR && !abs.startsWith(PUBLIC_DIR + path.sep)) return sendText(res, 403, 'Forbidden');

  let st;
  try {
    st = fs.statSync(abs);
    if (st.isDirectory()) { abs = path.join(abs, 'index.html'); st = fs.statSync(abs); }
  } catch (e) {
    return sendText(res, 404, 'Not Found');
  }
  if (!st.isFile()) return sendText(res, 404, 'Not Found');

  const ext = path.extname(abs).toLowerCase();
  const type = MIME[ext] || 'application/octet-stream';
  const cacheControl = ext === '.html' ? 'no-cache' : 'public, max-age=86400';
  const etag = `W/"${st.size.toString(16)}-${Math.floor(st.mtimeMs).toString(16)}"`;

  if (req.headers['if-none-match'] === etag) {
    res.writeHead(304, { ETag: etag, 'Cache-Control': cacheControl });
    res.end();
    return;
  }

  const headers = { 'Content-Type': type, ETag: etag, 'Cache-Control': cacheControl, 'X-Content-Type-Options': 'nosniff', Vary: 'Accept-Encoding' };

  if (st.size > 8 * 1024 * 1024) {                        // big media: stream it, no in-memory cache
    headers['Content-Length'] = st.size;
    res.writeHead(200, headers);
    if (req.method === 'HEAD') return res.end();
    fs.createReadStream(abs).on('error', () => res.destroy()).pipe(res);
    return;
  }

  let entry = staticCache.get(abs);
  if (!entry || entry.mtimeMs !== st.mtimeMs || entry.size !== st.size) {
    const raw = fs.readFileSync(abs);
    entry = { mtimeMs: st.mtimeMs, size: st.size, raw, gzip: COMPRESSIBLE.has(ext) ? zlib.gzipSync(raw, { level: 9 }) : null };
    staticCache.set(abs, entry);
  }
  const wantsGzip = entry.gzip && /\bgzip\b/.test(req.headers['accept-encoding'] || '');
  const body = wantsGzip ? entry.gzip : entry.raw;
  if (wantsGzip) headers['Content-Encoding'] = 'gzip';
  headers['Content-Length'] = body.length;
  res.writeHead(200, headers);
  res.end(req.method === 'HEAD' ? undefined : body);
}

const server = http.createServer((req, res) => {
  try { serveStatic(req, res); } catch (e) {
    console.error('[http]', e);
    if (!res.headersSent) sendText(res, 500, 'Internal Server Error');
    else res.destroy();
  }
});
server.keepAliveTimeout = 65000;     // longer than common proxy idle timeouts
server.headersTimeout = 70000;

server.on('upgrade', (req, socket, head) => {
  socket.on('error', () => {});
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade || '').toLowerCase() !== 'websocket' || req.headers['sec-websocket-version'] !== '13') {
    socket.destroy();
    return;
  }
  if (clients.size >= MAX_CLIENTS) {
    socket.write('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n');
    socket.destroy();
    return;
  }

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
      if (raw.startsWith('{"type":"world_sync"')) {   // hot path: forward the host snapshot without re-parsing it
        handleWorldSyncRaw(ws, client, raw);
        return;
      }
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
              delete inActiveMatch.disconnectedAt;     // came back within the grace period: keep the champion
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
        const requestedId = msg.roomId || msg.id;
        const targetRoom = requestedId ? rooms.get(requestedId) : null;
        if (!targetRoom) {
          ws.send({ type: 'left_room' });
          ws.send(buildRoomsListPayload());
          return;
        }
        assignClientToRoom(client, ws, targetRoom);
        if (targetRoom.phase === 'in_game' && targetRoom.activeMatch && Array.isArray(targetRoom.activeMatch.slots)) {
          let mySlotIdx = targetRoom.activeMatch.slots.findIndex(
            s => (s.clientToken && s.clientToken === client.clientToken) || (s.playerId && s.playerId === client.id)
          );
          let tookOverBot = false;
          if (mySlotIdx === -1) {
            mySlotIdx = targetRoom.activeMatch.slots.findIndex(s => !s.isHuman && s.team === client.team);
            if (mySlotIdx === -1) mySlotIdx = targetRoom.activeMatch.slots.findIndex(s => !s.isHuman);
            if (mySlotIdx !== -1) {
              const sl = targetRoom.activeMatch.slots[mySlotIdx];
              sl.isHuman = true;
              sl.playerId = client.id;
              sl.clientToken = client.clientToken;
              sl.name = client.name;
              client.team = sl.team;
              client.champId = sl.specId;
              tookOverBot = true;
              if (targetRoom.activeMatch.latestSnapshot && Array.isArray(targetRoom.activeMatch.latestSnapshot.champions) && targetRoom.activeMatch.latestSnapshot.champions[mySlotIdx]) {
                const sc = targetRoom.activeMatch.latestSnapshot.champions[mySlotIdx];
                sc.isPlayer = true;
                sc.playerId = client.id;
                sc.clientToken = client.clientToken;
                sc.summonerName = client.name;
              }
            }
          }
          const mySlot = mySlotIdx !== -1 ? targetRoom.activeMatch.slots[mySlotIdx] : null;
          if (mySlot) delete mySlot.disconnectedAt;
          ws.send({
            type: 'match_start',
            roomId: targetRoom.id,
            hostId: targetRoom.hostId,
            hostToken: targetRoom.hostToken,
            aiDiff: targetRoom.activeMatch.aiDiff,
            slots: targetRoom.activeMatch.slots,
            seed: targetRoom.activeMatch.seed,
            snapshot: targetRoom.activeMatch.latestSnapshot || null,
            spectator: Boolean(!mySlot)
          });
          if (mySlot && mySlotIdx !== -1) {
            broadcastToRoom(targetRoom.id, {
              type: 'player_joined_match',
              slotIdx: mySlotIdx,
              playerId: client.id,
              clientToken: client.clientToken,
              name: client.name,
              tookOverBot
            }, ws);
          }
          broadcastRoomState(targetRoom);
        }
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
        if (mySlot) delete mySlot.disconnectedAt;
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
        broadcastDroppable(room.id, {
          type: 'player_state',
          champIdx: msg.champIdx,
          state: msg.state
        }, ws);
      } else if (msg.type === 'world_sync') {
        // Fallback path (non-canonical key order). Same rules as the fast path: host only, latest wins.
        if (isHost && room.activeMatch && msg.snapshot) {
          room.activeMatch.latestSnapshot = msg.snapshot;
          room.activeMatch.lastSyncAt = Date.now();
          const out = JSON.stringify({ type: 'world_sync', hostId: room.hostId, hostToken: room.hostToken, snapshot: msg.snapshot });
          for (const [otherWs, c] of clients) {
            if (c.roomId === room.id && otherWs !== ws) otherWs.sendLatest(out);
          }
          if (msg.snapshot.winner) {
            room.activeMatch = null;
            room.phase = 'lobby';
            broadcastRoomState(room);
          }
        }
      }
    } catch (e) { console.error('[msg]', e && e.message); }
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
        // Keep the room (and its running match) alive for a grace period so a brief network blip can rejoin.
        room.emptySince = Date.now();
        if (room.phase === 'in_game' && room.activeMatch && Array.isArray(room.activeMatch.slots)) {
          for (const sl of room.activeMatch.slots) {
            if (sl.isHuman && slotBelongsTo(sl, client) && client.clientToken) sl.disconnectedAt = Date.now();
          }
        }
        broadcastRoomsList();
      } else {
        // Check if this clientToken still has another active socket in the room
        const tokenStillConnected = stillInRoom.some(c => c.clientToken && c.clientToken === client.clientToken);
        if (!tokenStillConnected && room.phase === 'in_game' && room.activeMatch && Array.isArray(room.activeMatch.slots)) {
          let freedAny = false;
          for (let i = 0; i < room.activeMatch.slots.length; i++) {
            const sl = room.activeMatch.slots[i];
            if (!slotBelongsTo(sl, client)) continue;
            if (client.clientToken) {
              sl.disconnectedAt = Date.now();      // grace period: the ticker hands it to a bot if they stay away
            } else {
              freeMatchSlot(room, i);
              freedAny = true;
            }
          }
          if (freedAny) {
            broadcastToRoom(oldRoomId, {
              type: 'player_left_match',
              playerId: client.id,
              clientToken: client.clientToken,
              name: client.name
            });
          }
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

process.on('uncaughtException', e => console.error('[uncaughtException]', e));
process.on('unhandledRejection', e => console.error('[unhandledRejection]', e));
for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, () => {
    console.log(`${sig} received, shutting down`);
    for (const ws of clients.keys()) ws.close(1001);
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 3000).unref();
  });
}

server.listen(PORT, '0.0.0.0', () => {
  const ip = getLocalIP();
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  LEAGUE OF LEGENDS — ARAM: MAYHEM (MULTI-ROOM LAN SERVER)');
  console.log(`  Local PC:       http://localhost:${PORT}`);
  console.log(`  LAN / Mobile:   http://${ip}:${PORT}  <-- Open on phones/PCs!`);
  console.log('═══════════════════════════════════════════════════════════════');
});
