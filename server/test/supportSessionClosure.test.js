'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const {
  SupportSessionClosureError,
  buildSupportSessionClosure,
} = require('../supportSessionClosure');

const expectClosureError = (fn, code, status) => {
  assert.throws(fn, (error) => {
    assert.equal(error instanceof SupportSessionClosureError, true);
    assert.equal(error.code, code);
    assert.equal(error.status, status);
    return true;
  });
};

const realtimeSession = {
  sessionId: 'REAL01',
  supportSessionId: 'LOCAL01',
  clientUid: 'client-uid',
  techUid: 'tech-uid',
  techName: 'Tecnico autorizado',
};

test('tecnico atribuido finaliza a sessao sem alterar cobranca', () => {
  const expiresAt = { toMillis: () => 999_999 };
  const result = buildSupportSessionClosure({
    realtimeSessionId: 'REAL01',
    realtimeSession,
    supportSession: {
      clientUid: 'client-uid',
      status: 'in_progress',
      creditsConsumed: 1,
      isFreeFirstSupport: false,
      billingAppliedAt: 111,
    },
    actorUid: 'tech-uid',
    actorRole: 'tech',
    authorizedTech: true,
    summary: {
      problemSummary: '  aparelho sem rede  ',
      solutionSummary: '  rede reconfigurada  ',
      internalNotes: '  validado pelo tecnico  ',
      creditsConsumed: 0,
      billingAppliedAt: 999,
      techId: 'attacker',
    },
    now: 500,
    expiresAt,
  });

  assert.equal(result.shouldWrite, true);
  assert.deepEqual(result.patch, {
    status: 'completed',
    queueStatus: 'completed',
    endedAt: 500,
    sessionId: 'REAL01',
    realtimeSessionId: 'REAL01',
    expiresAt,
    techId: 'tech-uid',
    techName: 'Tecnico autorizado',
    problemSummary: 'aparelho sem rede',
    solutionSummary: 'rede reconfigurada',
    internalNotes: 'validado pelo tecnico',
    updatedAt: 500,
  });
  assert.equal(Object.hasOwn(result.patch, 'creditsConsumed'), false);
  assert.equal(Object.hasOwn(result.patch, 'isFreeFirstSupport'), false);
  assert.equal(Object.hasOwn(result.patch, 'billingAppliedAt'), false);
});

test('cliente pode encerrar somente a propria sessao e nao controla resumo ou tecnico', () => {
  const result = buildSupportSessionClosure({
    realtimeSessionId: 'REAL01',
    realtimeSession,
    supportSession: {
      clientUid: 'client-uid',
      status: 'in_progress',
    },
    actorUid: 'client-uid',
    actorRole: 'client',
    summary: {
      problemSummary: 'forjado',
      solutionSummary: 'forjado',
      internalNotes: 'forjado',
      techId: 'attacker',
    },
    now: 700,
    expiresAt: { toMillis: () => 888_888 },
  });

  assert.equal(result.patch.techId, 'tech-uid');
  assert.equal(result.patch.techName, 'Tecnico autorizado');
  assert.equal(Object.hasOwn(result.patch, 'problemSummary'), false);
  assert.equal(Object.hasOwn(result.patch, 'solutionSummary'), false);
  assert.equal(Object.hasOwn(result.patch, 'internalNotes'), false);
});

test('ator nao atribuido ou sem autorizacao nao encerra sessao', () => {
  expectClosureError(
    () =>
      buildSupportSessionClosure({
        realtimeSessionId: 'REAL01',
        realtimeSession,
        supportSession: { clientUid: 'client-uid', status: 'in_progress' },
        actorUid: 'outro-cliente',
        actorRole: 'client',
      }),
    'forbidden',
    403
  );

  expectClosureError(
    () =>
      buildSupportSessionClosure({
        realtimeSessionId: 'REAL01',
        realtimeSession,
        supportSession: { clientUid: 'client-uid', status: 'in_progress' },
        actorUid: 'tech-uid',
        actorRole: 'tech',
        authorizedTech: false,
      }),
    'forbidden',
    403
  );

  expectClosureError(
    () =>
      buildSupportSessionClosure({
        realtimeSessionId: 'REAL01',
        realtimeSession,
        supportSession: { clientUid: 'client-uid', status: 'in_progress' },
        actorUid: 'outro-tech',
        actorRole: 'tech',
        authorizedTech: true,
      }),
    'forbidden',
    403
  );
});

test('encerramento repetido e idempotente', () => {
  const expiresAt = { toMillis: () => 900_000 };
  const result = buildSupportSessionClosure({
    realtimeSessionId: 'REAL01',
    realtimeSession,
    supportSession: {
      clientUid: 'client-uid',
      techId: 'tech-uid',
      techName: 'Tecnico autorizado',
      status: 'completed',
      queueStatus: 'completed',
      endedAt: 500,
      sessionId: 'REAL01',
      realtimeSessionId: 'REAL01',
      expiresAt,
    },
    actorUid: 'client-uid',
    actorRole: 'client',
    now: 800,
    expiresAt: { toMillis: () => 999_999 },
  });

  assert.equal(result.alreadyFinalized, true);
  assert.equal(result.shouldWrite, false);
  assert.deepEqual(result.patch, {});
});

test('cancelamento terminal nunca e convertido em atendimento concluido', () => {
  const result = buildSupportSessionClosure({
    realtimeSessionId: 'REAL01',
    realtimeSession,
    supportSession: {
      clientUid: 'client-uid',
      status: 'cancelled',
    },
    actorUid: 'client-uid',
    actorRole: 'client',
    now: 900,
  });

  assert.equal(result.finalStatus, 'cancelled');
  assert.equal(result.shouldWrite, false);
  assert.deepEqual(result.patch, {});
});

test('associacoes divergentes entre documentos sao rejeitadas', () => {
  expectClosureError(
    () =>
      buildSupportSessionClosure({
        realtimeSessionId: 'REAL01',
        realtimeSession,
        supportSession: {
          clientUid: 'outra-pessoa',
          status: 'in_progress',
        },
        actorUid: 'client-uid',
        actorRole: 'client',
      }),
    'support_session_owner_mismatch',
    409
  );

  expectClosureError(
    () =>
      buildSupportSessionClosure({
        realtimeSessionId: 'REAL01',
        realtimeSession,
        supportSession: {
          clientUid: 'client-uid',
          sessionId: 'OUTRA',
          status: 'in_progress',
        },
        actorUid: 'tech-uid',
        actorRole: 'tech',
        authorizedTech: true,
      }),
    'support_session_realtime_mismatch',
    409
  );

  expectClosureError(
    () =>
      buildSupportSessionClosure({
        realtimeSessionId: 'REAL01',
        realtimeSession,
        supportSession: {
          clientUid: 'client-uid',
          techId: 'outro-tech',
          status: 'in_progress',
        },
        actorUid: 'tech-uid',
        actorRole: 'tech',
        authorizedTech: true,
      }),
    'support_session_tech_mismatch',
    409
  );
});

test('todos os caminhos de encerramento chamam a finalizacao autoritativa', () => {
  const serverSource = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  const socketStart = serverSource.indexOf("socket.on('session:command'");
  const socketEnd = serverSource.indexOf("socket.on('session:telemetry'", socketStart);
  const manualStart = serverSource.indexOf('const closeManualDeclinedSession = async');
  const manualEnd = serverSource.indexOf('const scheduleManualDeclineClose', manualStart);
  const routeStart = serverSource.indexOf("app.post('/api/sessions/:id/close'");
  const routeEnd = serverSource.indexOf(
    "app.post('/api/sessions/:id/customer-feedback'",
    routeStart
  );

  assert.ok(socketStart >= 0 && socketEnd > socketStart);
  assert.ok(manualStart >= 0 && manualEnd > manualStart);
  assert.ok(routeStart >= 0 && routeEnd > routeStart);
  assert.match(
    serverSource.slice(socketStart, socketEnd),
    /await finalizeSupportSessionFromRealtime\(\{/
  );
  assert.match(
    serverSource.slice(manualStart, manualEnd),
    /await finalizeSupportSessionFromRealtime\(\{/
  );
  assert.match(
    serverSource.slice(routeStart, routeEnd),
    /await finalizeSupportSessionFromRealtime\(\{/
  );
  assert.match(
    serverSource.slice(routeStart, routeEnd),
    /closerUid !== getSessionTechUid\(session\)/
  );
});

test('regras permitem ao cliente apenas criar fila segura e cancelar a propria fila', () => {
  const rulesSource = fs.readFileSync(
    path.join(__dirname, '..', '..', 'firestore.rules'),
    'utf8'
  );
  const blockStart = rulesSource.indexOf('match /support_sessions/{supportSessionId}');
  const blockEnd = rulesSource.indexOf('match /support_reports/{reportId}', blockStart);
  const supportRules = rulesSource.slice(blockStart, blockEnd);

  assert.ok(blockStart >= 0 && blockEnd > blockStart);
  assert.match(supportRules, /request\.resource\.data\.status == 'queued'/);
  assert.match(supportRules, /request\.resource\.data\.isFreeFirstSupport == false/);
  assert.match(supportRules, /request\.resource\.data\.creditsConsumed == 0/);
  assert.match(supportRules, /request\.resource\.data\.get\('techId', null\) == null/);
  assert.match(supportRules, /request\.resource\.data\.get\('internalNotes', null\) == null/);
  assert.match(supportRules, /resource\.data\.status == 'queued'/);
  assert.match(supportRules, /request\.resource\.data\.status == 'cancelled'/);
  assert.match(
    supportRules,
    /affectedKeys\(\)\.hasOnly\(\[\s*'status',\s*'updatedAt',\s*'expiresAt'/
  );
  assert.equal(/allow create: if canAccessAsTech/.test(supportRules), false);
  assert.equal(/allow update: if canAccessAsTech/.test(supportRules), false);
});
