// Zombee Paty — сервер комнат.
//
// Что делает: хозяин создаёт комнату и получает код (например K7Q2MX), друзья входят
// по этому коду. Сервер просто пересылает сообщения между хозяином и друзьями —
// сама игра идёт у хозяина (он «сервер» для своей комнаты).
//
// Без сторонних библиотек: нужен только Node.js 18 или новее.
// Запуск на своём компьютере:  node server.js   (порт 8080, адрес ws://localhost:8080)
// На хостинге порт берётся из переменной окружения PORT.
//
// Протокол (WebSocket):
//   текстовые сообщения — JSON-команды:
//     → {"t":"host","ver":1}            ← {"t":"hosted","code":"K7Q2MX","id":1}
//     → {"t":"join","ver":1,"code":"…"} ← {"t":"joined","code":"…","id":N}
//                                         хозяину: {"t":"peer_join","id":N}
//     → {"t":"kick","id":N}  (только хозяин)
//     → {"t":"ping"}                    ← {"t":"pong"}
//     ← {"t":"error","code":"no_room"|"full"|"version"|"bad_request"|"server_full"}
//     ← хозяину {"t":"peer_leave","id":N},  друзьям {"t":"room_closed"}
//   двоичные сообщения — игровые данные:
//     хозяин → сервер: [int32 кому][данные]  (кому: N — одному, 0 — всем, -N — всем кроме N)
//     сервер → друг:   [данные]
//     друг → сервер:   [данные]
//     сервер → хозяин: [int32 от кого][данные]

'use strict';
const http = require('http');
const crypto = require('crypto');

const PORT = Number(process.env.PORT) || 8080;
const MAX_PLAYERS = 16;              // вместе с хозяином
const MAX_ROOMS = 500;
const MAX_MESSAGE = 256 * 1024;      // самое большое сообщение, байт
const HELLO_TIMEOUT_MS = 15000;      // сколько ждать команду host/join после подключения
const HEARTBEAT_MS = 30000;          // проверка «живых» соединений
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const CODE_LEN = 6;

/** @type {Map<string, Room>} */
const rooms = new Map();
let connections = 0;

// ============================================================ комнаты
class Room {
  constructor(code, ver, host) {
    this.code = code;
    this.ver = ver;
    this.host = host;
    /** @type {Map<number, Conn>} */
    this.clients = new Map();
    this.nextId = 2;
    this.created = Date.now();
  }
  get size() { return 1 + this.clients.size; }
}

function newCode() {
  for (let attempt = 0; attempt < 50; attempt++) {
    let code = '';
    const bytes = crypto.randomBytes(CODE_LEN);
    for (let i = 0; i < CODE_LEN; i++) code += CODE_CHARS[bytes[i] % CODE_CHARS.length];
    if (!rooms.has(code)) return code;
  }
  return null;
}

function onControl(conn, msg) {
  switch (msg.t) {
    case 'ping':
      conn.sendJson({ t: 'pong' });
      return;
    case 'host': {
      if (conn.room) return conn.fail('bad_request');
      if (rooms.size >= MAX_ROOMS) return conn.fail('server_full');
      const code = newCode();
      if (!code) return conn.fail('server_full');
      const room = new Room(code, Number(msg.ver) || 0, conn);
      rooms.set(code, room);
      conn.room = room;
      conn.peerId = 1;
      conn.sendJson({ t: 'hosted', code, id: 1 });
      log(`комната ${code} создана (комнат: ${rooms.size})`);
      return;
    }
    case 'join': {
      if (conn.room) return conn.fail('bad_request');
      const code = String(msg.code || '').trim().toUpperCase();
      const room = rooms.get(code);
      if (!room) return conn.fail('no_room');
      if ((Number(msg.ver) || 0) !== room.ver) return conn.fail('version');
      if (room.size >= MAX_PLAYERS) return conn.fail('full');
      const id = room.nextId++;
      room.clients.set(id, conn);
      conn.room = room;
      conn.peerId = id;
      conn.sendJson({ t: 'joined', code, id });
      room.host.sendJson({ t: 'peer_join', id });
      log(`комната ${code}: вошёл игрок ${id} (игроков: ${room.size})`);
      return;
    }
    case 'kick': {
      const room = conn.room;
      if (!room || conn.peerId !== 1) return;
      const target = room.clients.get(Number(msg.id));
      if (target) target.close(4001, 'kicked');
      return;
    }
    default:
      conn.fail('bad_request');
  }
}

function onData(conn, data) {
  const room = conn.room;
  if (!room) return;
  if (conn.peerId === 1) {
    if (data.length < 4) return;
    const target = data.readInt32LE(0);
    const payload = data.subarray(4);
    if (target > 0) {
      const c = room.clients.get(target);
      if (c) c.sendBinary(payload);
    } else {
      const except = -target;
      for (const [id, c] of room.clients) {
        if (id !== except) c.sendBinary(payload);
      }
    }
  } else {
    const header = Buffer.allocUnsafe(4);
    header.writeInt32LE(conn.peerId, 0);
    room.host.sendBinary(Buffer.concat([header, data]));
  }
}

function onClose(conn) {
  connections--;
  const room = conn.room;
  if (!room) return;
  conn.room = null;
  if (conn.peerId === 1) {
    rooms.delete(room.code);
    for (const c of room.clients.values()) {
      c.room = null;
      c.sendJson({ t: 'room_closed' });
      c.close(4000, 'room closed');
    }
    room.clients.clear();
    log(`комната ${room.code} закрыта (комнат: ${rooms.size})`);
  } else if (room.clients.get(conn.peerId) === conn) {
    room.clients.delete(conn.peerId);
    room.host.sendJson({ t: 'peer_leave', id: conn.peerId });
    log(`комната ${room.code}: вышел игрок ${conn.peerId} (игроков: ${room.size})`);
  }
}

// ============================================================ WebSocket (RFC 6455)
// Небольшая реализация без библиотек: рукопожатие, кадры, ping/pong, закрытие.
const OP_CONT = 0x0, OP_TEXT = 0x1, OP_BIN = 0x2, OP_CLOSE = 0x8, OP_PING = 0x9, OP_PONG = 0xA;

class Conn {
  constructor(socket) {
    this.socket = socket;
    this.chunks = [];
    this.buffered = 0;
    this.fragments = null;     // части сообщения, пришедшего кусками
    this.fragOp = 0;
    this.fragLen = 0;
    this.closed = false;
    this.alive = true;
    this.room = null;
    this.peerId = 0;
    this.helloTimer = setTimeout(() => { if (!this.room) this.close(4002, 'no hello'); }, HELLO_TIMEOUT_MS);
  }

  onSocketData(chunk) {
    this.alive = true;
    this.chunks.push(chunk);
    this.buffered += chunk.length;
    if (this.buffered > MAX_MESSAGE * 2 + 64) return this.close(1009, 'too big');
    let buf = this.chunks.length === 1 ? this.chunks[0] : Buffer.concat(this.chunks, this.buffered);
    let off = 0;
    while (!this.closed) {
      const used = this.parseFrame(buf, off);
      if (used === 0) break;
      off += used;
    }
    const rest = buf.subarray(off);
    this.chunks = rest.length ? [Buffer.from(rest)] : [];
    this.buffered = rest.length;
  }

  // Разбирает один кадр. Возвращает сколько байт съедено (0 — кадр ещё не пришёл целиком).
  parseFrame(buf, off) {
    if (buf.length - off < 2) return 0;
    const b0 = buf[off], b1 = buf[off + 1];
    const fin = (b0 & 0x80) !== 0;
    const op = b0 & 0x0f;
    const masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f;
    let pos = off + 2;
    if (len === 126) {
      if (buf.length - pos < 2) return 0;
      len = buf.readUInt16BE(pos); pos += 2;
    } else if (len === 127) {
      if (buf.length - pos < 8) return 0;
      const big = buf.readBigUInt64BE(pos); pos += 8;
      if (big > BigInt(MAX_MESSAGE)) { this.close(1009, 'too big'); return 0; }
      len = Number(big);
    }
    if (!masked) { this.close(1002, 'unmasked'); return 0; }
    if (len > MAX_MESSAGE) { this.close(1009, 'too big'); return 0; }
    if (buf.length - pos < 4 + len) return 0;
    const mask = buf.subarray(pos, pos + 4); pos += 4;
    const payload = Buffer.allocUnsafe(len);
    for (let i = 0; i < len; i++) payload[i] = buf[pos + i] ^ mask[i & 3];
    pos += len;
    this.onFrame(fin, op, payload);
    return pos - off;
  }

  onFrame(fin, op, payload) {
    if (op === OP_PING) return this.sendFrame(OP_PONG, payload);
    if (op === OP_PONG) return;
    if (op === OP_CLOSE) {
      const code = payload.length >= 2 ? payload.readUInt16BE(0) : 1000;
      return this.close(code === 1005 || code === 1006 ? 1000 : code, '');
    }
    if (op === OP_TEXT || op === OP_BIN) {
      if (this.fragments) return this.close(1002, 'bad fragment');
      if (fin) return this.onMessage(op, payload);
      this.fragments = [payload]; this.fragOp = op; this.fragLen = payload.length;
      return;
    }
    if (op === OP_CONT) {
      if (!this.fragments) return this.close(1002, 'bad fragment');
      this.fragments.push(payload); this.fragLen += payload.length;
      if (this.fragLen > MAX_MESSAGE) return this.close(1009, 'too big');
      if (fin) {
        const all = Buffer.concat(this.fragments, this.fragLen);
        const fop = this.fragOp;
        this.fragments = null;
        this.onMessage(fop, all);
      }
      return;
    }
    this.close(1002, 'bad opcode');
  }

  onMessage(op, payload) {
    if (op === OP_TEXT) {
      let msg;
      try { msg = JSON.parse(payload.toString('utf8')); } catch { return this.fail('bad_request'); }
      if (!msg || typeof msg.t !== 'string') return this.fail('bad_request');
      if (msg.t === 'host' || msg.t === 'join') clearTimeout(this.helloTimer);
      onControl(this, msg);
    } else {
      onData(this, payload);
    }
  }

  sendFrame(op, payload) {
    if (this.closed || this.socket.destroyed) return;
    const len = payload.length;
    let header;
    if (len < 126) {
      header = Buffer.from([0x80 | op, len]);
    } else if (len < 65536) {
      header = Buffer.allocUnsafe(4);
      header[0] = 0x80 | op; header[1] = 126; header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.allocUnsafe(10);
      header[0] = 0x80 | op; header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2);
    }
    this.socket.write(Buffer.concat([header, payload]));
  }

  sendJson(obj) { this.sendFrame(OP_TEXT, Buffer.from(JSON.stringify(obj), 'utf8')); }
  sendBinary(buf) { this.sendFrame(OP_BIN, buf); }

  fail(code) {
    this.sendJson({ t: 'error', code });
    this.close(4003, code);
  }

  close(code = 1000, reason = '') {
    if (this.closed) return;
    const r = Buffer.from(String(reason).slice(0, 100), 'utf8');
    const p = Buffer.allocUnsafe(2 + r.length);
    p.writeUInt16BE(code, 0); r.copy(p, 2);
    this.sendFrame(OP_CLOSE, p);
    this.closed = true;
    this.socket.end();
    setTimeout(() => this.socket.destroy(), 2000).unref();
  }
}

// ============================================================ HTTP + запуск
const server = http.createServer((req, res) => {
  // Обычная страница: можно открыть адрес сервера в браузере и проверить, что он работает.
  let players = 0;
  for (const r of rooms.values()) players += r.size;
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
  res.end(`Zombee Paty server OK\nкомнат: ${rooms.size}\nигроков: ${players}\n`);
});

const live = new Set();

server.on('upgrade', (req, socket, head) => {
  const key = req.headers['sec-websocket-key'];
  if (String(req.headers.upgrade || '').toLowerCase() !== 'websocket' || !key) {
    socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
    return;
  }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\nConnection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${accept}\r\n\r\n`);
  socket.setNoDelay(true);
  const conn = new Conn(socket);
  connections++;
  live.add(conn);
  let gone = false;
  const finish = () => {
    if (gone) return;
    gone = true;
    conn.closed = true;
    clearTimeout(conn.helloTimer);
    live.delete(conn);
    onClose(conn);
  };
  socket.on('data', (d) => conn.onSocketData(d));
  socket.on('close', finish);
  socket.on('end', finish);
  socket.on('error', finish);
  if (head && head.length) conn.onSocketData(head);
});

// Пингуем всех: кто не ответил за 30 секунд — отключаем (оборвалась связь).
setInterval(() => {
  for (const conn of live) {
    if (!conn.alive) { conn.socket.destroy(); continue; }
    conn.alive = false;
    conn.sendFrame(OP_PING, Buffer.alloc(0));
  }
}, HEARTBEAT_MS).unref();

function log(text) {
  console.log(new Date().toISOString().slice(11, 19), text);
}

server.listen(PORT, () => log(`Zombee Paty server слушает порт ${PORT}`));
