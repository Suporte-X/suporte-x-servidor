'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const request = require('supertest');

const {
  AccountDeletionError,
} = require('../accountDeletionService');
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
} = {}) {
  const events = [];
  if (
    typeof protectContact === 'function' &&
    typeof protectContact.hash !== 'function'
  ) {
    protectContact.hash = async () => 'a'.repeat(64);
  }
  const db = new FakeFirestore({}, { events });
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

test('rota autenticada usa somente o UID validado e exige Idempotency-Key', async () => {
  const { app, auth, deletionCalls } = createHarness();

  const response = await request(app)
    .post('/api/client/account/delete')
    .set('Authorization', 'Bearer valid-user-token')
    .set('Idempotency-Key', 'delete-1')
    .send({
      confirmation: 'EXCLUIR CONTA',
      clientId: 'victim-client-id',
      uid: 'victim-uid',
      pnvToken: 'pnv-token',
      pnvPhone: '+5565999999999',
    });

  assert.equal(response.status, 200);
  assert.equal(response.body.deleted, true);
  assert.deepEqual(deletionCalls, [{
    uid: 'uid-user',
    confirmation: 'EXCLUIR CONTA',
    idempotencyKey: 'delete-1',
    pnvToken: 'pnv-token',
    pnvPhone: '+5565999999999',
  }]);
  assert.deepEqual(auth.verifiedTokens, [{
    token: 'valid-user-token',
    checkRevoked: true,
  }]);
});

test('rota autenticada rejeita ausência de token e papel técnico', async () => {
  const { app, deletionCalls } = createHarness();

  const missing = await request(app)
    .post('/api/client/account/delete')
    .set('Idempotency-Key', 'delete-1')
    .send({ confirmation: 'EXCLUIR CONTA' });
  const tech = await request(app)
    .post('/api/client/account/delete')
    .set('Authorization', 'Bearer valid-tech-token')
    .set('Idempotency-Key', 'delete-1')
    .send({ confirmation: 'EXCLUIR CONTA' });

  assert.equal(missing.status, 401);
  assert.equal(missing.body.error, 'missing_token');
  assert.equal(tech.status, 403);
  assert.equal(tech.body.error, 'insufficient_role');
  assert.deepEqual(deletionCalls, []);
});

test('rota autenticada converte bloqueio de suporte ativo em 409 estável', async () => {
  const { app } = createHarness({
    accountDeletionService: {
      async deleteAccount() {
        throw new AccountDeletionError(409, 'active_support');
      },
    },
  });

  const response = await request(app)
    .post('/api/client/account/delete')
    .set('Authorization', 'Bearer valid-default-role-token')
    .set('Idempotency-Key', 'delete-1')
    .send({ confirmation: 'EXCLUIR CONTA' });

  assert.equal(response.status, 409);
  assert.deepEqual(response.body, { error: 'active_support' });
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
