'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const request = require('supertest');

const {
  GENERIC_DELETION_REQUEST_RESPONSE,
  createPrivacyContactProtector,
  createPrivacyRouter,
} = require('../privacyRouter');
const {
  FakeAuth,
  FakeBucket,
  FakeFirestore,
} = require('./helpers/fakeFirebase');

const NOW = 1_720_000_000_000;

function createHarness({
  accountDeletionService = null,
  verifyTurnstile = async () => ({ success: true }),
  protectContact = async (contact) => `protected:${contact}`,
  rateLimit = {},
  seedDocs = {},
} = {}) {
  const events = [];
  if (
    typeof protectContact === 'function' &&
    typeof protectContact.hash !== 'function'
  ) {
    protectContact.hash = async () => 'a'.repeat(64);
  }
  const db = new FakeFirestore(seedDocs, { events });
  const auth = new FakeAuth({
    events,
    tokens: {
      'valid-user-token': { uid: 'uid-user', role: 'user' },
      'valid-default-role-token': { uid: 'uid-default-role' },
      'valid-tech-token': { uid: 'uid-tech', role: 'tech' },
    },
  });
  const bucket = new FakeBucket({ events });
  const deletionCalls = [];
  const service =
    accountDeletionService ||
    {
      async deleteAccount(input) {
        deletionCalls.push(input);
        return {
          ok: true,
          deleted: true,
          deletedAt: NOW,
          deletedCounts: {},
          retained: [],
        };
      },
      async deleteAccountAfterGracePeriod(input) {
        deletionCalls.push(input);
        return {
          ok: true,
          deleted: true,
          deletedAt: NOW,
          deletedCounts: {},
          retained: [],
        };
      },
    };
  const app = express();
  app.use('/api', createPrivacyRouter({
    db,
    auth,
    bucket,
    accountDeletionService: service,
    verifyTurnstile,
    protectContact,
    rateLimit,
    clock: () => NOW,
    logger: { error() {}, warn() {} },
  }));
  return { app, auth, db, deletionCalls, events };
}

test('rota autenticada registra solicitação de 40 dias usando somente o UID validado', async () => {
  const { app, auth, db, deletionCalls } = createHarness();

  const response = await request(app)
    .post('/api/client/account/deletion-request')
    .set('Authorization', 'Bearer valid-user-token')
    .send({
      confirmation: 'EXCLUIR CONTA',
      reason: 'Não preciso mais do serviço',
      clientId: 'victim-client-id',
      uid: 'victim-uid',
    });

  assert.equal(response.status, 202);
  assert.equal(response.body.status, 'pending');
  assert.equal(response.body.gracePeriodDays, 40);
  assert.equal(response.body.eligibleAt, NOW + 40 * 24 * 60 * 60 * 1000);
  assert.deepEqual(deletionCalls, []);
  const stored = [...db.docs.entries()].find(([path]) =>
    path.startsWith('account_deletion_requests/')
  );
  assert.equal(stored[1].uid, 'uid-user');
  assert.equal(stored[1].reason, 'Não preciso mais do serviço');
  assert.deepEqual(auth.verifiedTokens, [{
    token: 'valid-user-token',
    checkRevoked: true,
  }]);
});

test('rota autenticada rejeita ausência de token e papel técnico', async () => {
  const { app, deletionCalls } = createHarness();

  const missing = await request(app)
    .post('/api/client/account/deletion-request')
    .send({
      confirmation: 'EXCLUIR CONTA',
      reason: 'Não preciso mais do serviço',
    });
  const tech = await request(app)
    .post('/api/client/account/deletion-request')
    .set('Authorization', 'Bearer valid-tech-token')
    .send({
      confirmation: 'EXCLUIR CONTA',
      reason: 'Não preciso mais do serviço',
    });

  assert.equal(missing.status, 401);
  assert.equal(missing.body.error, 'missing_token');
  assert.equal(tech.status, 403);
  assert.equal(tech.body.error, 'insufficient_role');
  assert.deepEqual(deletionCalls, []);
});

test('rota legada preserva exclusão imediata para o Android já distribuído', async () => {
  const { app, deletionCalls } = createHarness();

  const response = await request(app)
    .post('/api/client/account/delete')
    .set('Authorization', 'Bearer valid-user-token')
    .set('Idempotency-Key', 'legacy-delete-request')
    .send({
      confirmation: 'EXCLUIR CONTA',
      pnvPhone: '+5565999999999',
      pnvToken: 'legacy-pnv-token',
    });

  assert.equal(response.status, 200);
  assert.equal(response.body.deleted, true);
  assert.deepEqual(deletionCalls, [{
    uid: 'uid-user',
    confirmation: 'EXCLUIR CONTA',
    idempotencyKey: 'legacy-delete-request',
    pnvToken: 'legacy-pnv-token',
    pnvPhone: '+5565999999999',
  }]);
});

test('rota autenticada exige motivo e confirmação explícita', async () => {
  const { app } = createHarness();

  const missingReason = await request(app)
    .post('/api/client/account/deletion-request')
    .set('Authorization', 'Bearer valid-default-role-token')
    .send({ confirmation: 'EXCLUIR CONTA' });
  const missingConfirmation = await request(app)
    .post('/api/client/account/deletion-request')
    .set('Authorization', 'Bearer valid-default-role-token')
    .send({ reason: 'Não preciso mais do serviço' });

  assert.equal(missingReason.status, 400);
  assert.deepEqual(missingReason.body, { error: 'reason_required' });
  assert.equal(missingConfirmation.status, 400);
  assert.deepEqual(missingConfirmation.body, { error: 'confirmation_required' });
});

test('atividade material autenticada cancela solicitação pendente', async () => {
  const { app, db } = createHarness();
  const created = await request(app)
    .post('/api/client/account/deletion-request')
    .set('Authorization', 'Bearer valid-user-token')
    .send({
      confirmation: 'EXCLUIR CONTA',
      reason: 'Não preciso mais do serviço',
    });
  const cancelled = await request(app)
    .post('/api/client/account/deletion-request/activity')
    .set('Authorization', 'Bearer valid-user-token')
    .send({ activity: 'credit_purchase' });

  assert.equal(created.status, 202);
  assert.equal(cancelled.status, 200);
  assert.deepEqual(cancelled.body, {
    ok: true,
    cancelled: true,
    status: 'cancelled',
  });
  const stored = [...db.docs.entries()].find(([path]) =>
    path.startsWith('account_deletion_requests/')
  );
  assert.equal(stored[1].status, 'cancelled');
  assert.equal(stored[1].cancellationActivity, 'credit_purchase');
});

test('pedido público válido verifica Turnstile, protege contato e grava TTL', async () => {
  const turnstileCalls = [];
  const protectionCalls = [];
  const { app, db } = createHarness({
    verifyTurnstile: async (input) => {
      turnstileCalls.push(input);
      return { success: true };
    },
    protectContact: async (contact, context) => {
      protectionCalls.push({ contact, context });
      return `sealed:${contact}`;
    },
  });

  const response = await request(app)
    .post('/api/privacy/deletion-requests')
    .send({
      contactType: 'email',
      contact: ' Cliente@Example.com ',
      reason: 'Não utilizo mais o serviço',
      turnstileToken: 'turnstile-ok',
    });

  assert.equal(response.status, 202);
  assert.deepEqual(response.body, GENERIC_DELETION_REQUEST_RESPONSE);
  assert.equal(turnstileCalls.length, 1);
  assert.equal(turnstileCalls[0].token, 'turnstile-ok');
  assert.equal(protectionCalls.length, 1);
  assert.equal(protectionCalls[0].contact, 'cliente@example.com');

  const stored = [...db.docs.entries()].filter(([path]) =>
    path.startsWith('privacy_deletion_requests/')
  );
  assert.equal(stored.length, 1);
  assert.equal(stored[0][1].contact, 'sealed:cliente@example.com');
  assert.equal(stored[0][1].contactType, 'email');
  assert.equal(stored[0][1].status, 'received');
  assert.ok(stored[0][1].expiresAt instanceof Date);
  assert.equal(
    stored[0][1].expiresAt.getTime(),
    NOW + 30 * 24 * 60 * 60 * 1000
  );
  assert.match(stored[0][1].contactHash, /^[a-f0-9]{64}$/);
});

test('cifrador de contato usa AES-GCM autenticado e exige o contexto do pedido', async () => {
  const protectContact = createPrivacyContactProtector(
    Buffer.alloc(32, 7).toString('base64')
  );
  const context = {
    type: 'email',
    requestId: 'request-1',
  };

  const sealed = await protectContact('cliente@example.com', context);

  assert.match(sealed, /^v1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/);
  assert.equal(sealed.includes('cliente@example.com'), false);
  assert.equal(await protectContact.open(sealed, context), 'cliente@example.com');
  const contactHash = await protectContact.hash('cliente@example.com', {
    type: 'email',
  });
  assert.match(contactHash, /^[a-f0-9]{64}$/);
  assert.equal(contactHash.includes('cliente@example.com'), false);
  assert.equal(
    contactHash,
    await protectContact.hash('cliente@example.com', { type: 'email' })
  );
  assert.notEqual(
    contactHash,
    await protectContact.hash('cliente@example.com', { type: 'phone' })
  );
  await assert.rejects(
    protectContact.open(sealed, { ...context, requestId: 'request-2' })
  );
});

test('pedido público não grava contato quando a proteção não está configurada', async () => {
  const { app, db } = createHarness({
    protectContact: null,
  });

  const response = await request(app)
    .post('/api/privacy/deletion-requests')
    .send({
      contactType: 'email',
      contact: 'cliente@example.com',
      reason: 'Não utilizo mais o serviço',
      turnstileToken: 'turnstile-ok',
    });

  assert.equal(response.status, 503);
  assert.equal(response.body.error, 'temporarily_unavailable');
  assert.equal(
    [...db.docs.keys()].some((path) => path.startsWith('privacy_deletion_requests/')),
    false
  );
});

test('Turnstile inválido responde 202 genérico sem persistir contato', async () => {
  const { app, db } = createHarness({
    verifyTurnstile: async () => ({ success: false }),
  });

  const response = await request(app)
    .post('/api/privacy/deletion-requests')
    .send({
      contactType: 'phone',
      contact: '(65) 99999-9999',
      turnstileToken: 'turnstile-invalid',
    });

  assert.equal(response.status, 202);
  assert.deepEqual(response.body, GENERIC_DELETION_REQUEST_RESPONSE);
  assert.equal(
    [...db.docs.keys()].some((path) => path.startsWith('privacy_deletion_requests/')),
    false
  );
});

test('rate limit em memória bloqueia excesso por IP com Retry-After', async () => {
  const { app } = createHarness({
    verifyTurnstile: async () => false,
    rateLimit: { limit: 1, windowMs: 60_000 },
  });
  const payload = {
    contact: 'client@example.com',
    turnstileToken: 'invalid',
  };

  const first = await request(app).post('/api/privacy/deletion-requests').send(payload);
  const second = await request(app).post('/api/privacy/deletion-requests').send(payload);

  assert.equal(first.status, 202);
  assert.equal(second.status, 429);
  assert.equal(second.body.error, 'too_many_requests');
  assert.equal(second.headers['retry-after'], '60');
});
