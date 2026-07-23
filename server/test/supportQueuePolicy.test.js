'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const {
  SupportQueuePolicyError,
  buildClientBillingUpdates,
  decideTechQueueRemoval,
  decideTechSessionClaim,
  decideTechSupportAvailability,
  decideQueueCancellation,
  decideQueueReservation,
  evaluateAuthoritativeBilling,
} = require('../supportQueuePolicy');

const expectPolicyError = (fn, code) => {
  assert.throws(fn, (error) => {
    assert.equal(error instanceof SupportQueuePolicyError, true);
    assert.equal(error.code, code);
    return true;
  });
};

test('duas tentativas com a mesma âncora reutilizam exatamente o mesmo request', () => {
  const decision = decideQueueReservation({
    authUid: 'uid-a',
    localSupportSessionId: 'local-a',
    supportSession: { clientUid: 'uid-a', status: 'queued' },
    anchor: { requestId: 'REQ001', status: 'queued' },
    anchorRequest: {
      requestId: 'REQ001',
      clientUid: 'uid-a',
      localSupportSessionId: 'local-a',
      state: 'queued',
    },
    uidLock: {
      requestId: 'REQ001',
      localSupportSessionId: 'local-a',
    },
    lockRequest: {
      requestId: 'REQ001',
      clientUid: 'uid-a',
      localSupportSessionId: 'local-a',
      state: 'queued',
    },
    generatedRequestId: 'OTHER1',
  });

  assert.deepEqual(decision, {
    action: 'reuse',
    requestId: 'REQ001',
    localSupportSessionId: 'local-a',
    reused: true,
  });
});

test('duas âncoras concorrentes do mesmo UID não criam duas filas', () => {
  expectPolicyError(
    () =>
      decideQueueReservation({
        authUid: 'uid-a',
        localSupportSessionId: 'local-b',
        supportSession: { clientUid: 'uid-a', status: 'queued' },
        uidLock: {
          requestId: 'REQ001',
          localSupportSessionId: 'local-a',
        },
        lockRequest: {
          requestId: 'REQ001',
          clientUid: 'uid-a',
          localSupportSessionId: 'local-a',
          state: 'queued',
        },
        generatedRequestId: 'REQ002',
      }),
    'active_support_request_exists'
  );
});

test('lock órfão pode ser substituído por uma nova reserva', () => {
  const decision = decideQueueReservation({
    authUid: 'uid-a',
    localSupportSessionId: 'local-b',
    supportSession: { clientUid: 'uid-a', status: 'queued' },
    uidLock: {
      requestId: 'STALE1',
      localSupportSessionId: 'local-a',
    },
    lockRequest: null,
    generatedRequestId: 'REQ002',
  });

  assert.equal(decision.action, 'create');
  assert.equal(decision.requestId, 'REQ002');
});

test('sessão local de outro UID é rejeitada', () => {
  expectPolicyError(
    () =>
      decideQueueReservation({
        authUid: 'uid-attacker',
        localSupportSessionId: 'local-victim',
        supportSession: { clientUid: 'uid-victim', status: 'queued' },
        generatedRequestId: 'REQ001',
      }),
    'forbidden'
  );
});

test('cancelamento repetido é idempotente e continua verificando o dono', () => {
  const repeated = decideQueueCancellation({
    authUid: 'uid-a',
    requestedRequestId: 'REQ001',
    requestedLocalSupportSessionId: 'local-a',
    supportSession: {
      clientUid: 'uid-a',
      queueRequestId: 'REQ001',
      queueStatus: 'cancelled',
    },
    outcome: {
      requestId: 'REQ001',
      clientUid: 'uid-a',
      localSupportSessionId: 'local-a',
      status: 'cancelled',
    },
  });
  assert.equal(repeated.action, 'already_cancelled');
  assert.equal(repeated.removed, false);

  expectPolicyError(
    () =>
      decideQueueCancellation({
        authUid: 'uid-attacker',
        requestedRequestId: 'REQ001',
        request: {
          requestId: 'REQ001',
          clientUid: 'uid-victim',
          localSupportSessionId: 'local-a',
          state: 'queued',
        },
      }),
    'forbidden'
  );
});

test('elegibilidade autoritativa cobra uma vez e bloqueia saldo insuficiente', () => {
  const free = evaluateAuthoritativeBilling({
    client: { credits: 0, freeFirstSupportUsed: false },
    requestId: 'FREE01',
  });
  assert.equal(free.isFreeFirstSupport, true);
  assert.equal(free.creditsConsumed, 0);
  assert.equal(free.creditsAfter, 0);

  const paid = evaluateAuthoritativeBilling({
    client: { credits: 2, freeFirstSupportUsed: true },
    requestId: 'PAID01',
  });
  assert.equal(paid.isFreeFirstSupport, false);
  assert.equal(paid.creditsConsumed, 1);
  assert.equal(paid.creditsAfter, 1);

  expectPolicyError(
    () =>
      evaluateAuthoritativeBilling({
        client: { credits: 0, freeFirstSupportUsed: true },
        requestId: 'NOCR01',
      }),
    'credit_required'
  );
});

test('mutação de cobrança atualiza cliente e perfil de forma determinística', () => {
  const billing = evaluateAuthoritativeBilling({
    client: { credits: 3, freeFirstSupportUsed: true },
  });
  const mutation = buildClientBillingUpdates({
    client: {
      credits: 3,
      supportsUsed: 4,
      freeFirstSupportUsed: true,
      profileCompleted: true,
    },
    profile: {
      totalSessions: 4,
      totalPaidSessions: 3,
      totalFreeSessions: 1,
      totalCreditsPurchased: 5,
      totalCreditsUsed: 3,
    },
    billing,
    now: 123,
    deriveStatus: ({ credits }) => (credits > 0 ? 'with_credit' : 'without_credit'),
  });

  assert.deepEqual(mutation.client, {
    credits: 2,
    supportsUsed: 5,
    freeFirstSupportUsed: true,
    profileCompleted: true,
    status: 'with_credit',
    updatedAt: 123,
    lastSessionAt: 123,
    lastSeenAt: 123,
  });
  assert.deepEqual(mutation.profile, {
    totalSessions: 5,
    totalPaidSessions: 4,
    totalFreeSessions: 1,
    totalCreditsPurchased: 5,
    totalCreditsUsed: 4,
    lastSupportAt: 123,
    updatedAt: 123,
  });
});

test('aceite usa somente a transação autoritativa antes dos efeitos externos', () => {
  const serverSource = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  const transactionStart = serverSource.indexOf(
    'const acceptSupportQueueRequestTransaction = async'
  );
  const routeStart = serverSource.indexOf(
    "app.post('/api/requests/:id/accept'"
  );
  const routeEnd = serverSource.indexOf(
    'const buildManualDeclineChatMessage',
    routeStart
  );
  const transactionSource = serverSource.slice(transactionStart, routeStart);
  const routeSource = serverSource.slice(routeStart, routeEnd);

  assert.ok(transactionStart >= 0);
  assert.ok(routeStart > transactionStart);
  assert.ok(routeEnd > routeStart);
  assert.equal(serverSource.includes('applyClientConsumptionOnAccept'), false);
  assert.match(transactionSource, /return db\.runTransaction\(async \(tx\) =>/);
  assert.match(transactionSource, /tx\.set\(realtimeSessionRef, sessionData\)/);
  assert.match(transactionSource, /tx\.delete\(requestRef\)/);
  assert.match(transactionSource, /billingAppliedAt: now/);
  assert.match(routeSource, /await acceptSupportQueueRequestTransaction\(\{/);
  assert.ok(
    routeSource.indexOf('await acceptSupportQueueRequestTransaction({') <
      routeSource.indexOf('await persistQueueNotification({')
  );
});

test('technician lock rejects a concurrent active session and accepts a stale lock', () => {
  expectPolicyError(
    () =>
      decideTechSupportAvailability({
        requestedSessionId: 'SESSION-B',
        lockedSessionId: 'SESSION-A',
        lockedSession: { status: 'active' },
      }),
    'active_session_exists'
  );

  const staleLock = decideTechSupportAvailability({
    requestedSessionId: 'SESSION-B',
    lockedSessionId: 'SESSION-A',
    lockedSession: { status: 'closed' },
  });
  assert.deepEqual(staleLock, {
    allowed: true,
    requestedSessionId: 'SESSION-B',
  });
});

test('claim técnico é canônico, idempotente e rejeita sessão terminal ou de outro técnico', () => {
  assert.deepEqual(
    decideTechSessionClaim({
      session: { status: 'open' },
      techUid: 'tech-a',
    }),
    {
      action: 'claim',
      status: 'active',
      techUid: 'tech-a',
    }
  );
  assert.equal(
    decideTechSessionClaim({
      session: {
        status: 'active',
        techUid: 'tech-a',
        tech: { techUid: 'tech-a' },
      },
      techUid: 'tech-a',
    }).action,
    'reuse'
  );
  expectPolicyError(
    () =>
      decideTechSessionClaim({
        session: { status: 'active', techUid: 'tech-b' },
        techUid: 'tech-a',
      }),
    'already_claimed'
  );
  expectPolicyError(
    () =>
      decideTechSessionClaim({
        session: { status: 'closed' },
        techUid: 'tech-a',
      }),
    'session_not_claimable'
  );
});

test('remoção técnica é idempotente e perde corretamente a corrida para um aceite', () => {
  assert.deepEqual(
    decideTechQueueRemoval({
      request: { requestId: 'REQ001', state: 'queued' },
    }),
    {
      action: 'remove',
      removed: true,
    }
  );
  assert.deepEqual(
    decideTechQueueRemoval({
      request: null,
      outcome: { requestId: 'REQ001', status: 'removed' },
    }),
    {
      action: 'already_removed',
      removed: false,
    }
  );
  expectPolicyError(
    () =>
      decideTechQueueRemoval({
        request: null,
        outcome: {
          requestId: 'REQ001',
          status: 'accepted',
          realtimeSessionId: 'SESSION1',
        },
      }),
    'request_not_queued'
  );
  expectPolicyError(
    () =>
      decideTechQueueRemoval({
        request: { requestId: 'REQ001', state: 'queued' },
        outcome: {
          requestId: 'REQ001',
          status: 'accepted',
          realtimeSessionId: 'SESSION1',
        },
      }),
    'request_not_queued'
  );
});

test('accept and manual decline serialize on the same server-only technician lock', () => {
  const serverSource = fs.readFileSync(
    path.join(__dirname, '..', 'server.js'),
    'utf8'
  );
  const acceptStart = serverSource.indexOf(
    'const acceptSupportQueueRequestTransaction = async'
  );
  const acceptEnd = serverSource.indexOf(
    "app.post('/api/requests/:id/accept'",
    acceptStart
  );
  const declineStart = serverSource.indexOf(
    'const declineSupportQueueRequestTransaction = async'
  );
  const declineEnd = serverSource.indexOf(
    'const closeManualDeclinedSession',
    declineStart
  );
  const declineRouteStart = serverSource.indexOf(
    "app.post('/api/requests/:id/decline-with-refund'"
  );
  const declineRouteEnd = serverSource.indexOf(
    "app.get(\n  '/api/client/support-session/active'",
    declineRouteStart
  );
  const acceptSource = serverSource.slice(acceptStart, acceptEnd);
  const declineSource = serverSource.slice(declineStart, declineEnd);
  const declineRouteSource = serverSource.slice(
    declineRouteStart,
    declineRouteEnd
  );

  assert.match(acceptSource, /readTechSupportLockInTransaction\(\{/);
  assert.match(acceptSource, /writeTechSupportLockInTransaction\(\{/);
  assert.match(declineSource, /readTechSupportLockInTransaction\(\{/);
  assert.match(declineSource, /writeTechSupportLockInTransaction\(\{/);
  assert.match(
    declineSource,
    /tx\.set\(realtimeSessionRef, persistedSessionData\)/
  );
  assert.match(declineSource, /tx\.delete\(requestRef\)/);
  assert.match(declineSource, /status: 'accepted'/);
  assert.match(
    declineRouteSource,
    /await declineSupportQueueRequestTransaction\(\{/
  );
  assert.equal(declineRouteSource.includes('await requestRef.delete()'), false);
});

test('manual decline pending is reconciled after a server restart', () => {
  const serverSource = fs.readFileSync(
    path.join(__dirname, '..', 'server.js'),
    'utf8'
  );
  const reconcilerStart = serverSource.indexOf(
    'const reconcilePendingManualDeclines = async'
  );
  const reconcilerEnd = serverSource.indexOf(
    "app.post('/api/requests/:id/decline-with-refund'",
    reconcilerStart
  );
  const reconcilerSource = serverSource.slice(
    reconcilerStart,
    reconcilerEnd
  );

  assert.ok(reconcilerStart >= 0);
  assert.match(
    reconcilerSource,
    /\.where\('outcome', '==', 'unavailable_refund_pending'\)/
  );
  assert.match(reconcilerSource, /scheduleManualDeclineClose\(\{/);
  assert.match(reconcilerSource, /setInterval\(\(\) => \{/);
  assert.match(serverSource, /startManualDeclineReconciler\(\);/);
});

test('client HTTP cancellation shares the authoritative cancellation transaction', () => {
  const serverSource = fs.readFileSync(
    path.join(__dirname, '..', 'server.js'),
    'utf8'
  );
  const routeStart = serverSource.indexOf(
    "app.delete('/api/client/requests/:id'"
  );
  const routeEnd = serverSource.indexOf(
    "app.delete('/api/requests/:id'",
    routeStart
  );
  const routeSource = serverSource.slice(routeStart, routeEnd);

  assert.match(routeSource, /await cancelSupportQueueRequest\(\{/);
  assert.match(routeSource, /verifiedPhone: tokenPhone/);
  assert.equal(routeSource.includes('await requestRef.delete()'), false);
});

test('claim e remoção técnica usam transações autoritativas antes de efeitos externos', () => {
  const serverSource = fs.readFileSync(
    path.join(__dirname, '..', 'server.js'),
    'utf8'
  );
  const claimRouteStart = serverSource.indexOf(
    "app.post('/api/sessions/:id/claim'"
  );
  const claimRouteEnd = serverSource.indexOf(
    'const ACTIVE_REALTIME_SESSION_STATES',
    claimRouteStart
  );
  const claimTransactionStart = serverSource.indexOf(
    'const claimSupportSessionTransaction = async'
  );
  const claimTransactionEnd = serverSource.indexOf(
    'const acceptSupportQueueRequestTransaction = async',
    claimTransactionStart
  );
  const removalTransactionStart = serverSource.indexOf(
    'const removeSupportQueueRequestByTechTransaction = async'
  );
  const removalRouteStart = serverSource.indexOf(
    "app.delete('/api/requests/:id'",
    removalTransactionStart
  );
  const removalRouteEnd = serverSource.indexOf(
    'const getFirestoreCollectionCount',
    removalRouteStart
  );
  const claimRoute = serverSource.slice(claimRouteStart, claimRouteEnd);
  const claimTransaction = serverSource.slice(
    claimTransactionStart,
    claimTransactionEnd
  );
  const removalTransaction = serverSource.slice(
    removalTransactionStart,
    removalRouteStart
  );
  const removalRoute = serverSource.slice(removalRouteStart, removalRouteEnd);

  assert.match(claimRoute, /await claimSupportSessionTransaction\(\{/);
  assert.match(claimTransaction, /return db\.runTransaction\(async \(tx\) =>/);
  assert.match(claimTransaction, /readTechSupportLockInTransaction\(\{/);
  assert.match(claimTransaction, /writeTechSupportLockInTransaction\(\{/);
  assert.match(claimTransaction, /techUid: normalizedTechUid/);
  assert.match(claimTransaction, /status: claim\.status/);

  assert.match(
    removalTransaction,
    /return db\.runTransaction\(async \(tx\) =>/
  );
  assert.match(removalTransaction, /tx\.delete\(requestRef\)/);
  assert.match(removalTransaction, /status: 'removed'/);
  assert.match(
    removalRoute,
    /await removeSupportQueueRequestByTechTransaction\(\{/
  );
  assert.ok(
    removalRoute.indexOf('await removeSupportQueueRequestByTechTransaction({') <
      removalRoute.indexOf('await persistQueueNotification({')
  );
  assert.match(removalRoute, /result\.action !== 'remove'/);
  assert.equal(removalRoute.includes('await requestRef.delete()'), false);
});
