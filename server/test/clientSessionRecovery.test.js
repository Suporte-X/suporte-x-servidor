'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const {
  ClientSessionRecoveryError,
  createClientSessionRecoveryService,
} = require('../clientSessionRecovery');
const { FakeFirestore } = require('./helpers/fakeFirebase');

test('recovers only the active session owned by the authenticated client', async () => {
  const db = new FakeFirestore({
    'support_sessions/local-own': {
      clientUid: 'uid-owner',
      realtimeSessionId: 'REALTIME1',
    },
    'sessions/REALTIME1': {
      sessionId: 'REALTIME1',
      supportSessionId: 'local-own',
      clientUid: 'uid-owner',
      clientPhone: '5565999999999',
      techName: 'Tecnico A',
      status: 'active',
      acceptedAt: 200,
    },
  });
  const service = createClientSessionRecoveryService({ db });

  const result = await service.findActiveSession({
    uid: 'uid-owner',
    localSupportSessionId: 'local-own',
  });

  assert.deepEqual(result, {
    ok: true,
    active: true,
    sessionId: 'REALTIME1',
    techName: 'Tecnico A',
    status: 'active',
  });
  assert.equal(Object.hasOwn(result, 'clientPhone'), false);
});

test('does not reveal a support session that belongs to another UID', async () => {
  const db = new FakeFirestore({
    'support_sessions/local-victim': {
      clientUid: 'uid-victim',
      realtimeSessionId: 'VICTIM1',
    },
    'sessions/VICTIM1': {
      sessionId: 'VICTIM1',
      supportSessionId: 'local-victim',
      clientUid: 'uid-victim',
      techName: 'Tecnico B',
      status: 'active',
    },
  });
  const service = createClientSessionRecoveryService({ db });

  const result = await service.findActiveSession({
    uid: 'uid-attacker',
    localSupportSessionId: 'local-victim',
  });

  assert.deepEqual(result, { ok: true, active: false });
});

test('legacy fallback is scoped by UID and local support session id', async () => {
  const db = new FakeFirestore({
    'support_sessions/local-own': {
      clientUid: 'uid-owner',
    },
    'sessions/OLDER': {
      sessionId: 'OLDER',
      supportSessionId: 'other-local',
      clientUid: 'uid-owner',
      techName: 'Wrong',
      status: 'active',
      acceptedAt: 100,
    },
    'sessions/CURRENT': {
      sessionId: 'CURRENT',
      supportSessionId: 'local-own',
      clientUid: 'uid-owner',
      techName: 'Tecnico C',
      status: 'in_progress',
      acceptedAt: 300,
    },
  });
  const service = createClientSessionRecoveryService({ db });

  const result = await service.findActiveSession({
    uid: 'uid-owner',
    localSupportSessionId: 'local-own',
  });

  assert.deepEqual(result, {
    ok: true,
    active: true,
    sessionId: 'CURRENT',
    techName: 'Tecnico C',
    status: 'in_progress',
  });
});

test('rejects missing identity and malformed local ids', async () => {
  const service = createClientSessionRecoveryService({
    db: new FakeFirestore(),
  });

  await assert.rejects(
    service.findActiveSession({
      uid: '',
      localSupportSessionId: 'local-own',
    }),
    (error) =>
      error instanceof ClientSessionRecoveryError &&
      error.code === 'invalid_token' &&
      error.status === 401
  );
  await assert.rejects(
    service.findActiveSession({
      uid: 'uid-owner',
      localSupportSessionId: '../victim',
    }),
    (error) =>
      error instanceof ClientSessionRecoveryError &&
      error.code === 'invalid_local_support_session_id' &&
      error.status === 400
  );
});

test('HTTP recovery route requires user auth and never enables Firestore list', () => {
  const serverSource = fs.readFileSync(
    path.join(__dirname, '..', 'server.js'),
    'utf8'
  );
  const routeStart = serverSource.indexOf(
    "app.get(\n  '/api/client/support-session/active'"
  );
  const routeEnd = serverSource.indexOf(
    "app.delete('/api/client/requests/:id'",
    routeStart
  );
  const routeSource = serverSource.slice(routeStart, routeEnd);
  const rulesSource = fs.readFileSync(
    path.join(__dirname, '..', '..', 'firestore.rules'),
    'utf8'
  );
  const sessionRules = rulesSource.slice(
    rulesSource.indexOf('match /sessions/{sessionId}'),
    rulesSource.indexOf('match /messages/{docId}')
  );

  assert.ok(routeStart >= 0);
  assert.match(routeSource, /requireAuth\(\['user'\]\)/);
  assert.match(routeSource, /req\.user\?\.uid/);
  assert.match(routeSource, /localSupportSessionId: req\.query\?\.localSupportSessionId/);
  assert.match(sessionRules, /allow list: if false/);
});
