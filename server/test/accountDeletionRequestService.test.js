'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  AccountDeletionRequestError,
  createAccountDeletionRequestService,
  requestIdForUid,
} = require('../accountDeletionRequestService');
const { FakeFirestore } = require('./helpers/fakeFirebase');

const NOW = 1_720_000_000_000;
const DAY = 24 * 60 * 60 * 1000;
const UID = 'uid-user';
const REQUEST_ID = requestIdForUid(UID);

function createHarness({
  seedDocs = {},
  now = NOW,
} = {}) {
  let currentTime = now;
  const notifications = [];
  const deletions = [];
  const db = new FakeFirestore(seedDocs);
  const service = createAccountDeletionRequestService({
    db,
    clock: () => currentTime,
    notify: async (event) => {
      notifications.push(event);
      return { status: 'sent', providerMessageId: `mail-${notifications.length}` };
    },
    accountDeletionService: {
      async deleteAccountAfterGracePeriod(input) {
        deletions.push(input);
        return { ok: true, deleted: true, deletedAt: currentTime };
      },
    },
    logger: { error() {}, warn() {} },
  });
  return {
    db,
    service,
    notifications,
    deletions,
    setNow(value) {
      currentTime = value;
    },
  };
}

test('solicitação autenticada aguarda 40 dias e é idempotente enquanto pendente', async () => {
  const harness = createHarness({
    seedDocs: {
      [`client_app_links/${UID}`]: { clientUid: UID, clientId: 'client-1' },
      'clients/client-1': {
        name: 'Cliente Teste',
        email: 'cliente@example.com',
        phone: '+5565999999999',
      },
    },
  });

  const first = await harness.service.requestDeletion({
    uid: UID,
    reason: 'Não preciso mais do atendimento',
    confirmation: 'EXCLUIR CONTA',
  });
  const second = await harness.service.requestDeletion({
    uid: UID,
    reason: 'Outro motivo não deve substituir o pedido pendente',
    confirmation: 'EXCLUIR CONTA',
  });

  assert.equal(first.status, 'pending');
  assert.equal(first.eligibleAt, NOW + 40 * DAY);
  assert.deepEqual(second, first);
  assert.equal(harness.notifications.length, 1);
  const stored = harness.db.docs.get(`account_deletion_requests/${REQUEST_ID}`);
  assert.equal(stored.reason, 'Não preciso mais do atendimento');
  assert.equal(stored.clientEmail, 'cliente@example.com');
});

test('somente atividade material cancela pedido e abrir o app não é atividade válida', async () => {
  const harness = createHarness();
  await harness.service.requestDeletion({
    uid: UID,
    reason: 'Não preciso mais do atendimento',
    confirmation: 'EXCLUIR CONTA',
  });

  await assert.rejects(
    harness.service.cancelForActivity({ uid: UID, activity: 'app_open' }),
    (error) =>
      error instanceof AccountDeletionRequestError &&
      error.code === 'invalid_activity'
  );
  const cancelled = await harness.service.cancelForActivity({
    uid: UID,
    activity: 'support_request',
  });

  assert.equal(cancelled.cancelled, true);
  assert.equal(
    harness.db.docs.get(`account_deletion_requests/${REQUEST_ID}`).status,
    'cancelled'
  );
  assert.equal(harness.notifications.at(-1).type, 'cancelled');
});

test('processamento não exclui antes do prazo e conclui quando o 40º dia vence', async () => {
  const harness = createHarness();
  await harness.service.requestDeletion({
    uid: UID,
    reason: 'Não preciso mais do atendimento',
    confirmation: 'EXCLUIR CONTA',
  });

  const early = await harness.service.processDueRequests();
  assert.deepEqual(early, []);
  assert.deepEqual(harness.deletions, []);

  harness.setNow(NOW + 40 * DAY);
  const due = await harness.service.processDueRequests();

  assert.equal(due.length, 1);
  assert.equal(due[0].status, 'completed');
  assert.deepEqual(harness.deletions, [{ uid: UID, requestId: REQUEST_ID }]);
  assert.equal(
    harness.db.docs.get(`account_deletion_requests/${REQUEST_ID}`).status,
    'completed'
  );
  assert.equal(harness.notifications.at(-1).type, 'completed');
});

test('atividade persistida após o pedido cancela antes da exclusão automática', async () => {
  const harness = createHarness({
    seedDocs: {
      [`client_app_links/${UID}`]: { clientUid: UID, clientId: 'client-1' },
    },
  });
  await harness.service.requestDeletion({
    uid: UID,
    reason: 'Não preciso mais do atendimento',
    confirmation: 'EXCLUIR CONTA',
  });
  harness.db.docs.set('credit_orders/order-1', {
    clientId: 'client-1',
    createdAt: NOW + DAY,
  });
  harness.setNow(NOW + 40 * DAY);

  const result = await harness.service.processDueRequests();

  assert.equal(result.length, 1);
  assert.equal(result[0].status, 'cancelled');
  assert.deepEqual(harness.deletions, []);
  assert.equal(
    harness.db.docs.get(`account_deletion_requests/${REQUEST_ID}`)
      .cancellationActivity,
    'credit_purchase'
  );
});
