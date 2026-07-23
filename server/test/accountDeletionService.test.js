'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  AccountDeletionError,
  createAccountDeletionService,
  isAccountDeletionBlocking,
} = require('../accountDeletionService');
const {
  FakeAuth,
  FakeBucket,
  FakeFirestore,
} = require('./helpers/fakeFirebase');

const NOW = 1_720_000_000_000;
const UID = 'uid-current';
const LINKED_UID = 'uid-legacy';
const CLIENT_ID = 'uid_uidcurrent';
const PHONE = '+5565999999999';

function createHarness({
  seed = createCompleteSeed(),
  verifyPnvToken = async ({ expectedPhone }) => ({ ok: true, phone: expectedPhone }),
  bucketOptions = {},
  authOptions = {},
  firestoreOptions = {},
} = {}) {
  const events = [];
  const db = new FakeFirestore(seed, { events, ...firestoreOptions });
  const bucket = new FakeBucket({ events, ...bucketOptions });
  const auth = new FakeAuth({ events, ...authOptions });
  const pnvCalls = [];
  const service = createAccountDeletionService({
    db,
    auth,
    bucket,
    clock: () => NOW,
    logger: { error() {}, warn() {} },
    verifyPnvToken: async (input) => {
      pnvCalls.push(input);
      return verifyPnvToken(input);
    },
  });
  return { auth, bucket, db, events, pnvCalls, service };
}

function createCompleteSeed() {
  return {
    [`client_app_links/${UID}`]: {
      clientUid: UID,
      clientId: CLIENT_ID,
      phone: PHONE,
      phoneVerified: true,
    },
    [`client_app_links/${LINKED_UID}`]: {
      clientUid: LINKED_UID,
      clientId: CLIENT_ID,
      phone: PHONE,
      phoneVerified: true,
    },
    [`clients/${CLIENT_ID}`]: {
      name: 'Cliente Teste',
      phone: PHONE,
      primaryEmail: 'cliente@example.com',
      credits: 2,
    },
    [`clients/${CLIENT_ID}/private/profile`]: {
      secret: 'must be recursively removed',
    },
    [`client_profiles/${CLIENT_ID}`]: {
      clientId: CLIENT_ID,
      totalSessions: 1,
    },
    [`client_verifications/${CLIENT_ID}`]: {
      clientId: CLIENT_ID,
      status: 'verified',
      verifiedPhone: PHONE,
    },
    'sessions/session-closed': {
      clientUid: UID,
      clientRecordId: CLIENT_ID,
      clientPhone: PHONE,
      status: 'closed',
    },
    'sessions/session-closed/events/event-1': {
      type: 'message',
      body: 'private',
    },
    'support_sessions/local-session': {
      clientUid: UID,
      clientId: CLIENT_ID,
      clientPhone: PHONE,
      sessionId: 'session-closed',
      status: 'completed',
    },
    'support_reports/report-1': {
      clientUid: UID,
      clientId: CLIENT_ID,
      clientPhone: PHONE,
      symptom: 'private',
    },
    'requests/request-old': {
      clientUid: UID,
      clientRecordId: CLIENT_ID,
      clientPhone: PHONE,
      state: 'removed',
    },
    'queue_notifications/request-old': {
      clientUid: UID,
      clientRecordId: CLIENT_ID,
      clientPhone: PHONE,
    },
    'support_queue_locks/lock-1': {
      clientUid: UID,
      status: 'cancelled',
    },
    'support_queue_anchors/anchor-1': {
      clientUid: UID,
      status: 'accepted',
    },
    'support_queue_outcomes/outcome-1': {
      clientUid: UID,
      status: 'accepted',
    },
    'support_tech_locks/tech-lock-1': {
      techUid: 'tech-1',
      realtimeSessionId: 'session-closed',
      status: 'active',
    },
    'legacy_webrtc_rooms/room-1': {
      ownerUid: LINKED_UID,
      status: 'active',
    },
    'client_devices/device-1': {
      clientUid: UID,
      clientId: CLIENT_ID,
      fcmToken: 'private-token',
    },
    'client_notifications/notification-1': {
      clientUid: UID,
      clientId: CLIENT_ID,
      body: 'private',
    },
    'notification_events/event-1': {
      clientUid: UID,
      clientId: CLIENT_ID,
    },
    'admin_notifications/admin-1': {
      metadata: { clientId: CLIENT_ID },
      body: 'private',
    },
    'pnv_requests/pnv-1': {
      clientUid: UID,
      clientId: CLIENT_ID,
      phone: PHONE,
    },
    'whatsapp_api_conversations/p_5565999999999': {
      phone: PHONE,
      phoneDigits: '5565999999999',
    },
    'whatsapp_api_conversations/p_5565999999999/messages/message-1': {
      text: 'private',
    },
    'credit_orders/order-1': {
      clientUid: UID,
      clientId: CLIENT_ID,
      clientName: 'Cliente Teste',
      clientPhone: PHONE,
      packageId: 'pack-1',
      status: 'paid',
      paymentMethod: 'pix',
      amountCents: 2500,
      createdAt: NOW - 10_000,
    },
    [`credit_adjustment_requests/${CLIENT_ID}_adjust-1`]: {
      clientUid: UID,
      clientId: CLIENT_ID,
      clientName: 'Cliente Teste',
      clientPhone: PHONE,
      idempotencyKey: 'adjust-1',
      requestedBy: 'tech-1',
      creditChange: {
        previousCredits: 1,
        credits: 2,
        requestedDelta: 1,
        appliedDelta: 1,
      },
      createdAt: NOW - 5_000,
    },
    'clients/unrelated-client': {
      name: 'Must remain',
    },
  };
}

test('exclui dados vinculados, subcoleções e Storage antes de remover o Auth', async () => {
  const { auth, bucket, db, events, pnvCalls, service } = createHarness();

  const result = await service.deleteAccount({
    uid: UID,
    confirmation: 'EXCLUIR CONTA',
    idempotencyKey: 'delete-request-1',
    pnvToken: 'recent-pnv-token',
    pnvPhone: '(65) 99999-9999',
  });

  assert.equal(result.ok, true);
  assert.equal(result.deleted, true);
  assert.deepEqual(bucket.deletedPrefixes, ['sessions/session-closed/']);
  assert.deepEqual(auth.deletedUids, [LINKED_UID, UID]);
  assert.deepEqual(pnvCalls, [{
    token: 'recent-pnv-token',
    expectedPhone: PHONE,
    claimedPhone: PHONE,
    uid: UID,
  }]);

  const removedPaths = [
    `clients/${CLIENT_ID}`,
    `clients/${CLIENT_ID}/private/profile`,
    `client_profiles/${CLIENT_ID}`,
    `client_verifications/${CLIENT_ID}`,
    `client_app_links/${UID}`,
    `client_app_links/${LINKED_UID}`,
    'sessions/session-closed',
    'sessions/session-closed/events/event-1',
    'support_sessions/local-session',
    'support_reports/report-1',
    'requests/request-old',
    'client_devices/device-1',
    'client_notifications/notification-1',
    'notification_events/event-1',
    'admin_notifications/admin-1',
    'pnv_requests/pnv-1',
    'support_tech_locks/tech-lock-1',
    'legacy_webrtc_rooms/room-1',
    'whatsapp_api_conversations/p_5565999999999',
    'whatsapp_api_conversations/p_5565999999999/messages/message-1',
  ];
  for (const path of removedPaths) {
    assert.equal(db.docs.has(path), false, `${path} deveria ter sido removido`);
  }
  assert.equal(db.docs.has('clients/unrelated-client'), true);

  const retainedOrder = db.docs.get('credit_orders/order-1');
  assert.equal(retainedOrder.privacyDeleted, true);
  assert.equal(retainedOrder.amountCents, 2500);
  assert.equal('clientId' in retainedOrder, false);
  assert.equal('clientName' in retainedOrder, false);
  assert.equal('clientPhone' in retainedOrder, false);

  const originalAdjustmentPath =
    `credit_adjustment_requests/${CLIENT_ID}_adjust-1`;
  assert.equal(db.docs.has(originalAdjustmentPath), false);
  const retainedAdjustments = [...db.docs.entries()].filter(([path]) =>
    path.startsWith('credit_adjustment_requests/privacy_')
  );
  assert.equal(retainedAdjustments.length, 1);
  assert.equal(retainedAdjustments[0][1].privacyDeleted, true);
  assert.equal(retainedAdjustments[0][1].creditChange.appliedDelta, 1);
  assert.match(retainedAdjustments[0][1].idempotencyHash, /^[a-f0-9]{64}$/);
  assert.equal('idempotencyKey' in retainedAdjustments[0][1], false);
  assert.equal(JSON.stringify(retainedAdjustments[0]).includes(CLIENT_ID), false);
  assert.equal(JSON.stringify(retainedAdjustments[0]).includes(PHONE), false);

  const completedOperation = [...db.docs.entries()].find(([path]) =>
    path.startsWith('account_deletion_operations/')
  );
  assert.ok(completedOperation);
  assert.equal(completedOperation[1].status, 'completed');
  assert.equal('context' in completedOperation[1], false);
  assert.equal(JSON.stringify(completedOperation[1]).includes(UID), false);
  assert.equal(JSON.stringify(completedOperation[1]).includes(PHONE), false);

  const authDeleteIndex = events.indexOf(`auth:delete:${LINKED_UID}`);
  const currentAuthDeleteIndex = events.indexOf(`auth:delete:${UID}`);
  assert.ok(authDeleteIndex > events.indexOf('storage:deleteFiles:sessions/session-closed/'));
  assert.ok(authDeleteIndex > events.indexOf(`firestore:recursiveDelete:clients/${CLIENT_ID}`));
  assert.ok(currentAuthDeleteIndex > authDeleteIndex);
  const destructiveEvents = events.filter(
    (event) =>
      event.startsWith('storage:deleteFiles:') ||
      event.startsWith('firestore:recursiveDelete:') ||
      event.startsWith('auth:delete:') ||
      (
        event.startsWith('firestore:set:') &&
        !event.startsWith('firestore:set:account_deletion_operations/')
      )
  );
  assert.equal(destructiveEvents.at(-1), `auth:delete:${UID}`);
});

test('repetição da mesma chave retorna o resultado sem executar deleção novamente', async () => {
  const { auth, bucket, events, pnvCalls, service } = createHarness();
  const input = {
    uid: UID,
    confirmation: 'EXCLUIR CONTA',
    idempotencyKey: 'same-key',
    pnvToken: 'recent-token',
    pnvPhone: PHONE,
  };

  const first = await service.deleteAccount(input);
  const eventCountAfterFirst = events.length;
  const second = await service.deleteAccount(input);

  assert.deepEqual(second, first);
  assert.equal(events.length, eventCountAfterFirst);
  assert.equal(pnvCalls.length, 1);
  assert.deepEqual(auth.deletedUids, [LINKED_UID, UID]);
  assert.deepEqual(bucket.deletedPrefixes, ['sessions/session-closed/']);
});

test('tentativas concorrentes do mesmo UID usam um único bloqueio transacional', async () => {
  const { auth, service } = createHarness({
    bucketOptions: { delayMs: 30 },
  });
  const input = {
    uid: UID,
    confirmation: 'EXCLUIR CONTA',
    pnvToken: 'recent-pnv-token',
    pnvPhone: PHONE,
  };

  const results = await Promise.allSettled([
    service.deleteAccount({ ...input, idempotencyKey: 'concurrent-a' }),
    service.deleteAccount({ ...input, idempotencyKey: 'concurrent-b' }),
  ]);

  assert.equal(results.filter((result) => result.status === 'fulfilled').length, 1);
  const rejected = results.find((result) => result.status === 'rejected');
  assert.ok(rejected);
  assert.equal(rejected.reason instanceof AccountDeletionError, true);
  assert.equal(rejected.reason.code, 'deletion_in_progress');
  assert.deepEqual(auth.deletedUids, [LINKED_UID, UID]);
});

test('telefone verificado exige prova PNV recente antes de qualquer exclusão', async () => {
  const { auth, bucket, db, service } = createHarness();

  await assert.rejects(
    service.deleteAccount({
      uid: UID,
      confirmation: 'EXCLUIR CONTA',
      idempotencyKey: 'missing-pnv',
    }),
    (error) =>
      error instanceof AccountDeletionError &&
      error.status === 403 &&
      error.code === 'pnv_verification_required'
  );

  assert.deepEqual(auth.deletedUids, []);
  assert.deepEqual(bucket.deletedPrefixes, []);
  assert.equal(db.docs.has(`clients/${CLIENT_ID}`), true);
  assert.equal(
    [...db.docs.keys()].some((path) => path.startsWith('account_deletion_operations/')),
    false
  );
});

test('confirmação literal e Idempotency-Key são obrigatórios', async () => {
  const { auth, bucket, service } = createHarness();

  await assert.rejects(
    service.deleteAccount({
      uid: UID,
      confirmation: 'excluir conta',
      idempotencyKey: 'delete-1',
      pnvToken: 'recent-token',
    }),
    (error) =>
      error instanceof AccountDeletionError &&
      error.status === 400 &&
      error.code === 'confirmation_required'
  );
  await assert.rejects(
    service.deleteAccount({
      uid: UID,
      confirmation: 'EXCLUIR CONTA',
      pnvToken: 'recent-token',
    }),
    (error) =>
      error instanceof AccountDeletionError &&
      error.status === 400 &&
      error.code === 'idempotency_key_required'
  );

  assert.deepEqual(auth.deletedUids, []);
  assert.deepEqual(bucket.deletedPrefixes, []);
});

test('suporte ativo bloqueia a exclusão com 409 e preserva os dados', async () => {
  const seed = createCompleteSeed();
  seed['sessions/session-closed'].status = 'active';
  const { auth, bucket, db, service } = createHarness({ seed });

  await assert.rejects(
    service.deleteAccount({
      uid: UID,
      confirmation: 'EXCLUIR CONTA',
      idempotencyKey: 'active-support',
      pnvToken: 'recent-token',
      pnvPhone: PHONE,
    }),
    (error) =>
      error instanceof AccountDeletionError &&
      error.status === 409 &&
      error.code === 'active_support'
  );

  assert.deepEqual(auth.deletedUids, []);
  assert.deepEqual(bucket.deletedPrefixes, []);
  assert.equal(db.docs.has('sessions/session-closed'), true);
  assert.equal(db.docs.has(`clients/${CLIENT_ID}`), true);
});

test('suporte iniciado durante a confirmação transacional interrompe a exclusão', async () => {
  let injected = false;
  const { auth, bucket, db, service } = createHarness({
    firestoreOptions: {
      failSet(path, payload, _options, firestore) {
        if (
          !injected &&
          path.startsWith('account_deletion_operations/') &&
          payload?.status === 'processing'
        ) {
          injected = true;
          firestore.docs.set('sessions/session-closed', {
            ...firestore.docs.get('sessions/session-closed'),
            status: 'active',
          });
        }
        return null;
      },
    },
  });

  await assert.rejects(
    service.deleteAccount({
      uid: UID,
      confirmation: 'EXCLUIR CONTA',
      idempotencyKey: 'support-race',
      pnvToken: 'recent-token',
      pnvPhone: PHONE,
    }),
    (error) =>
      error instanceof AccountDeletionError &&
      error.status === 409 &&
      error.code === 'active_support'
  );

  assert.equal(injected, true);
  assert.deepEqual(auth.deletedUids, []);
  assert.deepEqual(bucket.deletedPrefixes, []);
  assert.equal(db.docs.has(`clients/${CLIENT_ID}`), true);
  const operation = [...db.docs.entries()].find(([path]) =>
    path.startsWith('account_deletion_operations/')
  );
  assert.ok(operation);
  assert.equal(operation[1].status, 'failed');
});

test('bloqueio de fila reconhece operações de exclusão em andamento ou concluídas', () => {
  assert.equal(isAccountDeletionBlocking({ status: 'processing' }), true);
  assert.equal(isAccountDeletionBlocking({ status: 'COMPLETED' }), true);
  assert.equal(isAccountDeletionBlocking({ status: 'failed' }), false);
  assert.equal(isAccountDeletionBlocking(null), false);
});

test('reserva de fila lê o bloqueio de exclusão dentro da própria transação', () => {
  const source = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  const start = source.indexOf('const reserveSupportQueueRequest = async');
  const end = source.indexOf('const cancelSupportQueueRequest = async', start);
  const reservationSource = source.slice(start, end);

  assert.ok(start >= 0);
  assert.ok(end > start);
  assert.match(reservationSource, /collection\('account_deletion_operations'\)/);
  assert.match(reservationSource, /await tx\.get\(deletionOperationRef\)/);
  assert.match(reservationSource, /isAccountDeletionBlocking\(deletionOperation\)/);
});

test('falha no Storage interrompe o fluxo antes do Auth e deixa operação retomável', async () => {
  const { auth, db, service } = createHarness({
    bucketOptions: { failPrefixes: ['sessions/session-closed/'] },
  });

  await assert.rejects(
    service.deleteAccount({
      uid: UID,
      confirmation: 'EXCLUIR CONTA',
      idempotencyKey: 'storage-failure',
      pnvToken: 'recent-token',
      pnvPhone: PHONE,
    }),
    /storage failure/
  );

  assert.deepEqual(auth.deletedUids, []);
  assert.equal(db.docs.has('sessions/session-closed'), true);
  const operation = [...db.docs.entries()].find(([path]) =>
    path.startsWith('account_deletion_operations/')
  );
  assert.ok(operation);
  assert.equal(operation[1].status, 'failed');
  assert.deepEqual(operation[1].context.sessionIds, ['session-closed']);
});
