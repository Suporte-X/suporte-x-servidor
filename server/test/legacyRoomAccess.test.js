'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const {
  LegacyJoinRateLimiter,
  LegacyRoomAccessError,
  buildLegacyRoomReservationDocument,
  decideLegacyRoomJoin,
  legacyRoomReservationDocId,
  legacySocketRoomName,
  normalizeLegacyRoomCode,
  validateLegacySignalAuthorization,
} = require('../legacyRoomAccess');

const timestamp = (millis) => ({
  toMillis: () => millis,
  // Firestore Timestamp implements valueOf(); toMillis must remain authoritative.
  valueOf: () => '62135596801.234000000',
});

const activeReservation = ({
  ownerUid = 'client-owner',
  reservationId = 'reservation_1234567890',
  expiresAtMs = 20_000,
  claimedAt = 1_000,
} = {}) => ({
  ownerUid,
  reservationId,
  status: 'active',
  claimedAt,
  expiresAt: timestamp(expiresAtMs),
});

const expectAccessError = (fn, code, status) => {
  assert.throws(fn, (error) => {
    assert.equal(error instanceof LegacyRoomAccessError, true);
    assert.equal(error.code, code);
    assert.equal(error.status, status);
    return true;
  });
};

test('formato legado continua restrito a exatamente seis caracteres alfanumericos', () => {
  assert.equal(normalizeLegacyRoomCode('123456'), '123456');
  assert.equal(normalizeLegacyRoomCode(' AB12cd '), 'AB12CD');
  assert.equal(normalizeLegacyRoomCode('12345'), '');
  assert.equal(normalizeLegacyRoomCode('1234567'), '');
  assert.equal(normalizeLegacyRoomCode('12-456'), '');
  assert.equal(normalizeLegacyRoomCode('12_456'), '');
  assert.equal(normalizeLegacyRoomCode('á23456'), '');
});

test('primeiro cliente autenticado toma a sala e gera documento com TTL', () => {
  const decision = decideLegacyRoomJoin({
    roomCode: '123456',
    requesterUid: 'client-owner',
    requestedRole: 'client',
    reservation: null,
    proposedReservationId: 'reservation_1234567890',
    now: 1_000,
    ttlMs: 300_000,
  });

  assert.equal(decision.action, 'claim');
  assert.equal(decision.ownerUid, 'client-owner');
  assert.equal(decision.expiresAtMs, 301_000);
  const document = buildLegacyRoomReservationDocument({
    decision,
    now: 1_000,
    timestampFromMillis: timestamp,
  });
  assert.equal(document.status, 'active');
  assert.equal(document.ownerUid, 'client-owner');
  assert.equal(document.expiresAt.toMillis(), 301_000);
  assert.equal(document.schemaVersion, 1);
  assert.equal(Object.hasOwn(document, 'roomCode'), false);
});

test('reconexao do dono preserva a reserva e renova a expiracao', () => {
  const decision = decideLegacyRoomJoin({
    roomCode: 'ABC123',
    requesterUid: 'client-owner',
    requestedRole: 'client',
    reservation: activeReservation(),
    proposedReservationId: 'reservation_should_not_replace',
    now: 10_000,
    ttlMs: 300_000,
  });

  assert.equal(decision.action, 'refresh');
  assert.equal(decision.reservationId, 'reservation_1234567890');
  assert.equal(decision.claimedAtMs, 1_000);
  assert.equal(decision.expiresAtMs, 310_000);
});

test('outro cliente nunca assume uma reserva ainda valida', () => {
  expectAccessError(
    () =>
      decideLegacyRoomJoin({
        roomCode: '123456',
        requesterUid: 'client-intruder',
        requestedRole: 'client',
        reservation: activeReservation(),
        proposedReservationId: 'reservation_intruder_123',
        now: 10_000,
      }),
    'room-unavailable',
    403
  );
});

test('tecnico ativo entra somente depois de existir reserva valida', () => {
  expectAccessError(
    () =>
      decideLegacyRoomJoin({
        roomCode: '123456',
        requesterUid: 'tech-uid',
        requestedRole: 'tech',
        isTechActive: true,
        reservation: null,
        now: 10_000,
      }),
    'room-unavailable',
    404
  );

  expectAccessError(
    () =>
      decideLegacyRoomJoin({
        roomCode: '123456',
        requesterUid: 'tech-uid',
        requestedRole: 'tech',
        isTechActive: false,
        reservation: activeReservation(),
        now: 10_000,
      }),
    'forbidden',
    403
  );

  const authorized = decideLegacyRoomJoin({
    roomCode: '123456',
    requesterUid: 'tech-uid',
    requestedRole: 'tech',
    isTechActive: true,
    reservation: activeReservation(),
    now: 10_000,
  });
  assert.equal(authorized.action, 'authorize');
  assert.equal(authorized.shouldWrite, false);
  assert.equal(authorized.reservationId, 'reservation_1234567890');
});

test('reserva expirada pode ser retomada por novo cliente mas nao por tecnico', () => {
  const expired = activeReservation({ expiresAtMs: 9_999 });
  const reclaimed = decideLegacyRoomJoin({
    roomCode: '123456',
    requesterUid: 'new-client',
    requestedRole: 'client',
    reservation: expired,
    proposedReservationId: 'new_reservation_123456',
    now: 10_000,
    ttlMs: 300_000,
  });

  assert.equal(reclaimed.action, 'claim');
  assert.equal(reclaimed.ownerUid, 'new-client');
  assert.equal(reclaimed.reservationId, 'new_reservation_123456');

  expectAccessError(
    () =>
      decideLegacyRoomJoin({
        roomCode: '123456',
        requesterUid: 'tech-uid',
        requestedRole: 'tech',
        isTechActive: true,
        reservation: expired,
        now: 10_000,
      }),
    'room-unavailable',
    404
  );
});

test('geracao nova de reserva usa sala Socket isolada da geracao expirada', () => {
  const first = legacySocketRoomName('123456', 'reservation_aaaaaaaaaa');
  const second = legacySocketRoomName('123456', 'reservation_bbbbbbbbbb');
  assert.notEqual(first, second);
  assert.match(first, /^legacy:[a-f0-9]{64}:reservation_/);
  assert.equal(first.includes('123456'), false);
  assert.equal(
    legacyRoomReservationDocId('123456').includes('123456'),
    false
  );
});

test('signal exige a autorizacao de join do mesmo UID, sala e geracao', () => {
  const authorization = {
    room: '123456',
    role: 'client',
    uid: 'client-owner',
    reservationId: 'reservation_1234567890',
    socketRoom: legacySocketRoomName(
      '123456',
      'reservation_1234567890'
    ),
    expiresAtMs: 20_000,
  };

  assert.equal(
    validateLegacySignalAuthorization({
      authorization,
      roomCode: '123456',
      authUid: 'client-owner',
      now: 10_000,
    }).ok,
    true
  );
  assert.deepEqual(
    validateLegacySignalAuthorization({
      authorization,
      roomCode: '654321',
      authUid: 'client-owner',
      now: 10_000,
    }),
    { ok: false, code: 'not-joined' }
  );
  assert.deepEqual(
    validateLegacySignalAuthorization({
      authorization,
      roomCode: '123456',
      authUid: 'intruder',
      now: 10_000,
    }),
    { ok: false, code: 'not-joined' }
  );
  assert.deepEqual(
    validateLegacySignalAuthorization({
      authorization: {
        ...authorization,
        socketRoom: 'legacy:forged',
      },
      roomCode: '123456',
      authUid: 'client-owner',
      now: 10_000,
    }),
    { ok: false, code: 'not-joined' }
  );
  assert.deepEqual(
    validateLegacySignalAuthorization({
      authorization,
      roomCode: '123456',
      authUid: 'client-owner',
      now: 20_000,
    }),
    { ok: false, code: 'join-expired' }
  );
});

test('rate limit bloqueia enumeracao por socket e por IP e reinicia na janela seguinte', () => {
  const limiter = new LegacyJoinRateLimiter({
    windowMs: 10_000,
    socketLimit: 2,
    ipLimit: 3,
  });

  assert.equal(
    limiter.consume({ socketId: 'socket-a', ipKey: 'ip-a', now: 1_000 }).allowed,
    true
  );
  assert.equal(
    limiter.consume({ socketId: 'socket-a', ipKey: 'ip-a', now: 1_100 }).allowed,
    true
  );
  const socketBlocked = limiter.consume({
    socketId: 'socket-a',
    ipKey: 'ip-a',
    now: 1_200,
  });
  assert.equal(socketBlocked.allowed, false);
  assert.ok(socketBlocked.retryAfterMs > 0);

  const ipBlocked = limiter.consume({
    socketId: 'socket-b',
    ipKey: 'ip-a',
    now: 1_300,
  });
  assert.equal(ipBlocked.allowed, false);

  const reset = limiter.consume({
    socketId: 'socket-c',
    ipKey: 'ip-a',
    now: 11_001,
  });
  assert.equal(reset.allowed, true);
});

test('servidor reserva em transacao e retransmite apenas pela autorizacao persistida', () => {
  const source = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  const reservationStart = source.indexOf(
    'const authorizeLegacyRoomReservation = async'
  );
  const resolverStart = source.indexOf(
    'const resolveLegacyJoinAccess = async',
    reservationStart
  );
  const joinStart = source.indexOf("socket.on('join'", resolverStart);
  const signalStart = source.indexOf("socket.on('signal'", joinStart);
  const sessionJoinStart = source.indexOf(
    "socket.on('session:join'",
    signalStart
  );
  const reservationSource = source.slice(reservationStart, resolverStart);
  const joinSource = source.slice(joinStart, signalStart);
  const signalSource = source.slice(signalStart, sessionJoinStart);

  assert.ok(
    reservationStart >= 0 &&
      resolverStart > reservationStart &&
      joinStart > resolverStart &&
      signalStart > joinStart &&
      sessionJoinStart > signalStart
  );
  assert.match(reservationSource, /return db\.runTransaction|await db\.runTransaction/);
  assert.match(reservationSource, /await tx\.get\(reservationRef\)/);
  assert.match(reservationSource, /await tx\.get\(db\.collection\('techs'\)/);
  assert.match(reservationSource, /tx\.set\(reservationRef, reservationDocument\)/);
  assert.match(joinSource, /legacyJoinRateLimiter\.consume/);
  assert.match(joinSource, /socket\.join\(socketRoom\)/);
  assert.match(signalSource, /validateLegacySignalAuthorization/);
  assert.match(
    signalSource,
    /socket\.to\(signalAccess\.socketRoom\)\.emit\('signal'/
  );
  assert.equal(/socket\.to\(room\)\.emit\('signal'/.test(signalSource), false);
});
