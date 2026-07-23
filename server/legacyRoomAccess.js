'use strict';

const crypto = require('node:crypto');

const DEFAULT_LEGACY_ROOM_TTL_MS = 2 * 60 * 60 * 1000;
const MIN_LEGACY_ROOM_TTL_MS = 5 * 60 * 1000;
const MAX_LEGACY_ROOM_TTL_MS = 12 * 60 * 60 * 1000;
const DEFAULT_JOIN_RATE_WINDOW_MS = 60 * 1000;
const DEFAULT_SOCKET_JOIN_LIMIT = 10;
const DEFAULT_IP_JOIN_LIMIT = 60;

class LegacyRoomAccessError extends Error {
  constructor(code, status = 403) {
    super(code);
    this.name = 'LegacyRoomAccessError';
    this.code = code;
    this.status = status;
  }
}

const boundedInteger = (value, fallback, min, max) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, Math.trunc(parsed)));
};

const normalizeLegacyRoomTtlMs = (value) =>
  boundedInteger(
    value,
    DEFAULT_LEGACY_ROOM_TTL_MS,
    MIN_LEGACY_ROOM_TTL_MS,
    MAX_LEGACY_ROOM_TTL_MS
  );

const normalizeLegacyRoomCode = (value) => {
  if (typeof value !== 'string') return '';
  const normalized = value.trim().toUpperCase();
  return /^[A-Za-z0-9]{6}$/.test(normalized) ? normalized : '';
};

const legacyRoomReservationDocId = (roomCode) => {
  const room = normalizeLegacyRoomCode(roomCode);
  if (!room) return '';
  return crypto.createHash('sha256').update(room, 'utf8').digest('hex');
};

const timestampToMillis = (value) => {
  if (value instanceof Date) return value.getTime();
  if (value && typeof value.toMillis === 'function') {
    const millis = Number(value.toMillis());
    return Number.isFinite(millis) ? millis : 0;
  }
  if (value && Number.isFinite(Number(value.seconds))) {
    const seconds = Number(value.seconds);
    const nanos = Number(value.nanoseconds || value.nanos || 0);
    return seconds * 1000 + Math.floor(nanos / 1e6);
  }
  if (
    (typeof value === 'number' || typeof value === 'string') &&
    Number.isFinite(Number(value))
  ) {
    return Number(value);
  }
  return 0;
};

const normalizeReservationId = (value) => {
  if (typeof value !== 'string') return '';
  const normalized = value.trim();
  return /^[A-Za-z0-9_-]{16,128}$/.test(normalized) ? normalized : '';
};

const activeReservation = (reservation, now) => {
  if (!reservation || typeof reservation !== 'object') return null;
  const ownerUid =
    typeof reservation.ownerUid === 'string' ? reservation.ownerUid.trim() : '';
  const reservationId = normalizeReservationId(reservation.reservationId);
  const expiresAtMs = timestampToMillis(reservation.expiresAt);
  const status =
    typeof reservation.status === 'string'
      ? reservation.status.trim().toLowerCase()
      : '';
  if (
    status !== 'active' ||
    !ownerUid ||
    !reservationId ||
    !Number.isFinite(expiresAtMs) ||
    expiresAtMs <= now
  ) {
    return null;
  }
  return {
    ownerUid,
    reservationId,
    expiresAtMs,
    claimedAtMs: timestampToMillis(reservation.claimedAt),
  };
};

const decideLegacyRoomJoin = ({
  roomCode,
  requesterUid,
  requestedRole,
  isTechActive = false,
  reservation = null,
  proposedReservationId = '',
  now = Date.now(),
  ttlMs = DEFAULT_LEGACY_ROOM_TTL_MS,
} = {}) => {
  const room = normalizeLegacyRoomCode(roomCode);
  if (!room) throw new LegacyRoomAccessError('invalid-room', 400);

  const uid =
    typeof requesterUid === 'string' ? requesterUid.trim().slice(0, 256) : '';
  if (!uid) throw new LegacyRoomAccessError('missing_token', 401);

  const role =
    typeof requestedRole === 'string'
      ? requestedRole.trim().toLowerCase()
      : '';
  if (role !== 'client' && role !== 'tech') {
    throw new LegacyRoomAccessError('forbidden', 403);
  }
  if (role === 'tech' && isTechActive !== true) {
    throw new LegacyRoomAccessError('forbidden', 403);
  }

  const nowMs = Number.isFinite(Number(now)) ? Number(now) : Date.now();
  const safeTtlMs = normalizeLegacyRoomTtlMs(ttlMs);
  const current = activeReservation(reservation, nowMs);

  if (role === 'tech') {
    if (!current) {
      throw new LegacyRoomAccessError('room-unavailable', 404);
    }
    return {
      action: 'authorize',
      shouldWrite: false,
      room,
      role,
      ownerUid: current.ownerUid,
      reservationId: current.reservationId,
      claimedAtMs: current.claimedAtMs,
      expiresAtMs: current.expiresAtMs,
    };
  }

  if (current && current.ownerUid !== uid) {
    throw new LegacyRoomAccessError('room-unavailable', 403);
  }

  if (current) {
    return {
      action: 'refresh',
      shouldWrite: true,
      room,
      role,
      ownerUid: uid,
      reservationId: current.reservationId,
      claimedAtMs: current.claimedAtMs || nowMs,
      expiresAtMs: nowMs + safeTtlMs,
    };
  }

  const reservationId = normalizeReservationId(proposedReservationId);
  if (!reservationId) {
    throw new LegacyRoomAccessError('reservation-id-required', 500);
  }
  return {
    action: 'claim',
    shouldWrite: true,
    room,
    role,
    ownerUid: uid,
    reservationId,
    claimedAtMs: nowMs,
    expiresAtMs: nowMs + safeTtlMs,
  };
};

const buildLegacyRoomReservationDocument = ({
  decision,
  now = Date.now(),
  timestampFromMillis,
} = {}) => {
  if (
    !decision ||
    decision.shouldWrite !== true ||
    (decision.action !== 'claim' && decision.action !== 'refresh')
  ) {
    return null;
  }
  if (typeof timestampFromMillis !== 'function') {
    throw new LegacyRoomAccessError('timestamp_factory_required', 500);
  }
  return {
    ownerUid: decision.ownerUid,
    reservationId: decision.reservationId,
    status: 'active',
    claimedAt: decision.claimedAtMs,
    updatedAt: Number(now),
    expiresAt: timestampFromMillis(decision.expiresAtMs),
    schemaVersion: 1,
  };
};

const legacySocketRoomName = (roomCode, reservationId) => {
  const roomId = legacyRoomReservationDocId(roomCode);
  const normalizedReservationId = normalizeReservationId(reservationId);
  if (!roomId || !normalizedReservationId) return '';
  return `legacy:${roomId}:${normalizedReservationId}`;
};

const validateLegacySignalAuthorization = ({
  authorization,
  roomCode,
  authUid,
  now = Date.now(),
} = {}) => {
  if (!authorization || typeof authorization !== 'object') {
    return { ok: false, code: 'not-joined' };
  }
  const room = normalizeLegacyRoomCode(roomCode);
  const uid = typeof authUid === 'string' ? authUid.trim() : '';
  const reservationId = normalizeReservationId(authorization.reservationId);
  const expectedSocketRoom = legacySocketRoomName(room, reservationId);
  if (
    !room ||
    authorization.room !== room ||
    !uid ||
    authorization.uid !== uid ||
    (authorization.role !== 'client' && authorization.role !== 'tech') ||
    !reservationId ||
    typeof authorization.socketRoom !== 'string' ||
    authorization.socketRoom !== expectedSocketRoom
  ) {
    return { ok: false, code: 'not-joined' };
  }
  if (
    !Number.isFinite(Number(authorization.expiresAtMs)) ||
    Number(authorization.expiresAtMs) <= Number(now)
  ) {
    return { ok: false, code: 'join-expired' };
  }
  return {
    ok: true,
    role: authorization.role,
    socketRoom: authorization.socketRoom,
  };
};

const normalizeLegacySocketIp = (socket = {}) => {
  const headers = socket?.handshake?.headers || {};
  const cloudflareIp =
    typeof headers['cf-connecting-ip'] === 'string'
      ? headers['cf-connecting-ip'].trim()
      : '';
  const forwardedIp =
    typeof headers['x-forwarded-for'] === 'string'
      ? headers['x-forwarded-for']
          .split(',')
          .map((part) => part.trim())
          .filter(Boolean)
          .at(-1) || ''
      : '';
  const handshakeIp =
    typeof socket?.handshake?.address === 'string'
      ? socket.handshake.address.trim()
      : '';
  const transportIp =
    typeof socket?.conn?.remoteAddress === 'string'
      ? socket.conn.remoteAddress.trim()
      : '';
  return (cloudflareIp || forwardedIp || handshakeIp || transportIp).slice(0, 128);
};

const legacyJoinIpKey = (socket) => {
  const ip = normalizeLegacySocketIp(socket);
  if (!ip) return '';
  return crypto.createHash('sha256').update(ip, 'utf8').digest('hex');
};

class LegacyJoinRateLimiter {
  constructor({
    windowMs = DEFAULT_JOIN_RATE_WINDOW_MS,
    socketLimit = DEFAULT_SOCKET_JOIN_LIMIT,
    ipLimit = DEFAULT_IP_JOIN_LIMIT,
    maxEntries = 10_000,
  } = {}) {
    this.windowMs = boundedInteger(
      windowMs,
      DEFAULT_JOIN_RATE_WINDOW_MS,
      10_000,
      10 * 60 * 1000
    );
    this.socketLimit = boundedInteger(socketLimit, DEFAULT_SOCKET_JOIN_LIMIT, 1, 100);
    this.ipLimit = boundedInteger(ipLimit, DEFAULT_IP_JOIN_LIMIT, 1, 1000);
    this.maxEntries = boundedInteger(maxEntries, 10_000, 100, 100_000);
    this.socketWindows = new Map();
    this.ipWindows = new Map();
    this.lastPruneAt = 0;
  }

  prune(now) {
    if (
      now - this.lastPruneAt < this.windowMs &&
      this.socketWindows.size + this.ipWindows.size <= this.maxEntries
    ) {
      return;
    }
    const cutoff = now - this.windowMs;
    for (const [key, entry] of this.socketWindows) {
      if (entry.startedAt <= cutoff) this.socketWindows.delete(key);
    }
    for (const [key, entry] of this.ipWindows) {
      if (entry.startedAt <= cutoff) this.ipWindows.delete(key);
    }
    let overflow =
      this.socketWindows.size + this.ipWindows.size - this.maxEntries;
    for (const map of [this.socketWindows, this.ipWindows]) {
      while (overflow > 0 && map.size) {
        const oldestKey = map.keys().next().value;
        map.delete(oldestKey);
        overflow -= 1;
      }
    }
    this.lastPruneAt = now;
  }

  consumeWindow(map, key, limit, now) {
    if (!key) return { allowed: true, retryAfterMs: 0 };
    const existing = map.get(key);
    const entry =
      !existing || now - existing.startedAt >= this.windowMs
        ? { count: 1, startedAt: now }
        : { count: existing.count + 1, startedAt: existing.startedAt };
    map.set(key, entry);
    return {
      allowed: entry.count <= limit,
      retryAfterMs:
        entry.count <= limit
          ? 0
          : Math.max(1, entry.startedAt + this.windowMs - now),
    };
  }

  consume({ socketId = '', ipKey = '', now = Date.now() } = {}) {
    const nowMs = Number.isFinite(Number(now)) ? Number(now) : Date.now();
    this.prune(nowMs);
    const socketDecision = this.consumeWindow(
      this.socketWindows,
      String(socketId || '').slice(0, 256),
      this.socketLimit,
      nowMs
    );
    const ipDecision = this.consumeWindow(
      this.ipWindows,
      String(ipKey || '').slice(0, 256),
      this.ipLimit,
      nowMs
    );
    return {
      allowed: socketDecision.allowed && ipDecision.allowed,
      retryAfterMs: Math.max(
        socketDecision.retryAfterMs,
        ipDecision.retryAfterMs
      ),
    };
  }
}

module.exports = {
  DEFAULT_IP_JOIN_LIMIT,
  DEFAULT_JOIN_RATE_WINDOW_MS,
  DEFAULT_LEGACY_ROOM_TTL_MS,
  DEFAULT_SOCKET_JOIN_LIMIT,
  LegacyJoinRateLimiter,
  LegacyRoomAccessError,
  buildLegacyRoomReservationDocument,
  decideLegacyRoomJoin,
  legacyJoinIpKey,
  legacyRoomReservationDocId,
  legacySocketRoomName,
  normalizeLegacyRoomCode,
  normalizeLegacyRoomTtlMs,
  validateLegacySignalAuthorization,
};
