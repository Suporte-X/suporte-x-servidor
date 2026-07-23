'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  createPrivacyContactProtector,
} = require('../privacyRouter');

const REQUEST_ID = '4d6da30d-5f6f-4bb8-96cf-b472119afc70';
const NOW = 1_720_000_000_000;

async function loadSubject() {
  return import('../scripts/privacyDeletionRequests.mjs');
}

test('listagem é o modo padrão e revelação exige pedido e operador explícitos', async () => {
  const { parseArgs } = await loadSubject();

  assert.deepEqual(parseArgs([]), {
    help: false,
    requestId: '',
    revealContact: false,
    setStatus: '',
    actor: '',
    status: '',
    limit: 50,
  });
  assert.throws(
    () => parseArgs(['--request-id', REQUEST_ID, '--reveal-contact']),
    /actor_required/
  );
  assert.throws(
    () => parseArgs(['--reveal-contact', '--actor', 'operador-1']),
    /request_id_required/
  );
});

test('metadados operacionais nunca incluem contato cifrado nem índice', async () => {
  const { sanitizeRequestMetadata } = await loadSubject();
  const metadata = sanitizeRequestMetadata(REQUEST_ID, {
    requestId: REQUEST_ID,
    status: 'received',
    contactType: 'email',
    contact: 'ciphertext-must-not-leak',
    contactHash: 'lookup-must-not-leak',
    createdAt: NOW,
    expiresAt: new Date(NOW + 1_000),
  });

  assert.equal('contact' in metadata, false);
  assert.equal('contactHash' in metadata, false);
  assert.equal(metadata.contactType, 'email');
  assert.equal(metadata.createdAt, new Date(NOW).toISOString());
});

test('somente o operador que assumiu o pedido consegue abrir o contato', async () => {
  const {
    operatorHash,
    revealContactForOperator,
  } = await loadSubject();
  const protector = createPrivacyContactProtector(
    Buffer.alloc(32, 11).toString('base64')
  );
  const actor = 'operador-1';
  const protectedValue = await protector('opaque-test-contact', {
    type: 'email',
    requestId: REQUEST_ID,
  });
  const processingByHash = await operatorHash(protector, actor);
  const data = {
    requestId: REQUEST_ID,
    status: 'processing',
    processingByHash,
    contactType: 'email',
    contact: protectedValue,
  };

  assert.equal(
    await revealContactForOperator({
      protector,
      requestId: REQUEST_ID,
      data,
      actor,
    }),
    'opaque-test-contact'
  );
  await assert.rejects(
    revealContactForOperator({
      protector,
      requestId: REQUEST_ID,
      data,
      actor: 'operador-2',
    }),
    /request_claimed_by_another_operator/
  );
});

test('conclusão ou rejeição elimina contato e índice do documento', async () => {
  const {
    buildStatusTransition,
  } = await loadSubject();
  const deleteField = Object.freeze({ delete: true });
  const currentData = {
    status: 'processing',
    processingByHash: 'operator-hash',
  };

  const completed = buildStatusTransition({
    currentData,
    nextStatus: 'completed',
    actorHash: 'operator-hash',
    now: NOW,
    deleteField,
  });

  assert.equal(completed.status, 'completed');
  assert.equal(completed.contact, deleteField);
  assert.equal(completed.contactHash, deleteField);
  assert.equal(completed.contactPurgedAt, NOW);
  assert.throws(
    () =>
      buildStatusTransition({
        currentData: { status: 'completed' },
        nextStatus: 'processing',
        actorHash: 'operator-hash',
        now: NOW,
        deleteField,
      }),
    /invalid_status_transition/
  );
  assert.throws(
    () =>
      buildStatusTransition({
        currentData: { status: 'processing' },
        nextStatus: 'completed',
        actorHash: 'operator-hash',
        now: NOW,
        deleteField,
      }),
    /request_claimed_by_another_operator/
  );
});
