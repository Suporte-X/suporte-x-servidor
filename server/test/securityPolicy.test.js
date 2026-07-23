'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const {
  ACTIVE_TECH_SOCKET_ROOM,
  buildClientIdentityLookupPlan,
  clientSocketRoom,
  isExplicitlyEnabled,
  mayReplaceClientUidLink,
  selectClientPhoneForIdentity,
  sessionRoleSocketRoom,
  timingSafeStringEqual,
} = require('../securityPolicy');

test('comparação de segredos tem resultado estável para valores e tamanhos diferentes', () => {
  assert.equal(timingSafeStringEqual('segredo-forte', 'segredo-forte'), true);
  assert.equal(timingSafeStringEqual('segredo-forte', 'segredo-fraco'), false);
  assert.equal(timingSafeStringEqual('curto', 'um-valor-bem-maior'), false);
  assert.equal(isExplicitlyEnabled('true'), true);
  assert.equal(isExplicitlyEnabled(' TRUE '), true);
  assert.equal(isExplicitlyEnabled('1'), false);
  assert.equal(isExplicitlyEnabled('false'), false);
});

test('UID novo não herda cadastro por telefone ou âncora sem prova verificada', () => {
  const plan = buildClientIdentityLookupPlan({
    linkedClientId: null,
    linkedDeviceClientId: 'client_victim_by_device',
    verificationClientIds: ['client_victim_by_phone'],
    uidDocId: 'uid_attacker',
    phoneDocId: 'phone_5565999999999',
    hasVerifiedIdentityProof: false,
  });

  assert.deepEqual(plan.candidateIds, ['uid_attacker']);
  assert.equal(plan.fallbackClientId, 'uid_attacker');
  assert.equal(plan.allowPhoneLookup, false);
  assert.equal(plan.allowDeviceLookup, false);
  assert.equal(plan.allowDeviceLinkWrite, false);
});

test('UID já vinculado continua reconhecendo o mesmo cliente', () => {
  const plan = buildClientIdentityLookupPlan({
    linkedClientId: 'client_existing',
    linkedDeviceClientId: 'client_other',
    verificationClientIds: ['client_other'],
    uidDocId: 'uid_same_user',
    phoneDocId: 'phone_other',
    hasVerifiedIdentityProof: false,
  });

  assert.deepEqual(plan.candidateIds, ['client_existing']);
  assert.equal(plan.fallbackClientId, 'client_existing');
  assert.equal(mayReplaceClientUidLink({ hasVerifiedIdentityProof: false }), false);
  assert.equal(mayReplaceClientUidLink({ hasVerifiedIdentityProof: true }), true);
  assert.equal(
    selectClientPhoneForIdentity({
      existingPhone: '556511111111',
      claimedPhone: '556599999999',
      hasVerifiedIdentityProof: false,
    }),
    '556511111111'
  );
  assert.equal(
    selectClientPhoneForIdentity({
      existingPhone: '556511111111',
      claimedPhone: '556599999999',
      hasVerifiedIdentityProof: true,
    }),
    '556599999999'
  );
});

test('prova PNV libera resolução por telefone verificado e vínculo de dispositivo', () => {
  const plan = buildClientIdentityLookupPlan({
    linkedClientId: 'uid_provisional_client',
    linkedDeviceClientId: 'client_verified_device',
    verificationClientIds: ['client_verified_phone'],
    uidDocId: 'uid_new_install',
    phoneDocId: 'phone_5565988888888',
    hasVerifiedIdentityProof: true,
  });

  assert.deepEqual(plan.candidateIds, [
    'client_verified_phone',
    'phone_5565988888888',
    'uid_provisional_client',
    'uid_new_install',
  ]);
  assert.equal(plan.fallbackClientId, 'client_verified_phone');
  assert.equal(plan.allowPhoneLookup, true);
  assert.equal(plan.allowDeviceLookup, false);
  assert.equal(plan.allowDeviceLinkWrite, true);
});

test('âncora só pode localizar cadastro em contexto técnico explicitamente autorizado', () => {
  const plan = buildClientIdentityLookupPlan({
    linkedDeviceClientId: 'client_from_device',
    uidDocId: null,
    phoneDocId: null,
    hasVerifiedIdentityProof: false,
    allowDeviceIdentityLookup: true,
  });

  assert.deepEqual(plan.candidateIds, ['client_from_device']);
  assert.equal(plan.fallbackClientId, 'client_from_device');
  assert.equal(plan.allowDeviceLookup, true);
  assert.equal(plan.allowDeviceLinkWrite, false);
});

test('salas privadas separam técnicos ativos, usuário autenticado e papel da sessão', () => {
  const userRoom = clientSocketRoom('firebase-user-123');

  assert.equal(ACTIVE_TECH_SOCKET_ROOM, 'auth:tech:active');
  assert.match(userRoom, /^auth:user:/);
  assert.equal(userRoom.includes('firebase-user-123'), false);
  assert.equal(sessionRoleSocketRoom('ABC-123', 'tech'), 's:ABC-123:tech');
  assert.equal(sessionRoleSocketRoom('ABC-123', 'client'), 's:ABC-123:client');
  assert.equal(sessionRoleSocketRoom('ABC-123', 'unknown'), null);
});

test('servidor não usa broadcast global para estado de sessão ou fila', () => {
  const serverSource = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

  assert.equal(/io\.emit\(\s*['"]session:updated['"]/.test(serverSource), false);
  assert.equal(/io\.emit\(\s*['"]queue:updated['"]/.test(serverSource), false);
  assert.match(
    serverSource,
    /io\.to\(ACTIVE_TECH_SOCKET_ROOM\)\.emit\('session:updated', session\)/
  );
  assert.match(
    serverSource,
    /resolveSocketIdentityAccess\(decoded, \{ requireActiveTech: requiresActiveTech \}\)/
  );
  assert.match(serverSource, /socketsForUser\.disconnectSockets\(true\)/);
  assert.match(
    serverSource,
    /hasVerifiedIdentityProof:\s*Boolean\(tokenPhone\)/
  );
  assert.match(serverSource, /baseLinkPayload\.identityAssurance = 'uid_only'/);
  assert.match(serverSource, /finalClientId = await db\.runTransaction/);
  assert.match(
    serverSource,
    /identityAssurance:\s*'firebase_pnv'/
  );
});

test('regras Firestore só confiam no link cujo clientUid pertence ao usuário', () => {
  const rulesSource = fs.readFileSync(
    path.join(__dirname, '..', '..', 'firestore.rules'),
    'utf8'
  );

  assert.match(
    rulesSource,
    /get\(clientAppLinkPath\(request\.auth\.uid\)\)\.data\.clientUid == request\.auth\.uid/
  );
  assert.match(
    rulesSource,
    /resource\.data\.clientUid == request\.auth\.uid/
  );
});

test('dispositivo pode migrar do cliente provisório para vínculo próprio verificado', () => {
  const rulesSource = fs.readFileSync(
    path.join(__dirname, '..', '..', 'firestore.rules'),
    'utf8'
  );
  const deviceRules = rulesSource.slice(
    rulesSource.indexOf('match /client_devices/{deviceId}'),
    rulesSource.indexOf('match /client_notifications/{notificationId}')
  );

  assert.match(
    deviceRules,
    /request\.resource\.data\.clientUid == resource\.data\.clientUid/
  );
  assert.match(
    deviceRules,
    /request\.resource\.data\.deviceAnchor == resource\.data\.deviceAnchor/
  );
  assert.match(
    deviceRules,
    /request\.resource\.data\.platform == resource\.data\.platform/
  );
  assert.match(
    deviceRules,
    /ownClientId\(request\.resource\.data\.clientId\)/
  );
  assert.equal(
    deviceRules.includes(
      'request.resource.data.clientId == resource.data.clientId'
    ),
    false
  );
});

test('Storage bloqueia upload direto e obriga validação pelo backend', () => {
  const rulesSource = fs.readFileSync(
    path.join(__dirname, '..', '..', 'storage.rules'),
    'utf8'
  );
  const sessionAudioRules = rulesSource.slice(
    rulesSource.indexOf('match /sessions/{sessionId}/audio/{fileName}'),
    rulesSource.indexOf('match /sessions/{sessionId}/{allPaths=**}')
  );

  assert.match(sessionAudioRules, /allow read: if isSessionMember\(sessionId\)/);
  assert.match(sessionAudioRules, /allow write: if false/);
  assert.equal(sessionAudioRules.includes('application/octet-stream'), false);
});

test('Firestore tech access requires a tech claim and an explicitly active record', () => {
  const rulesSource = fs.readFileSync(
    path.join(__dirname, '..', '..', 'firestore.rules'),
    'utf8'
  );
  const activeTechRules = rulesSource.slice(
    rulesSource.indexOf('function isActiveTechUser(uid)'),
    rulesSource.indexOf('function ownUidClientId()')
  );

  assert.match(activeTechRules, /request\.auth\.uid == uid/);
  assert.match(activeTechRules, /request\.auth\.token\.role == 'tech'/);
  assert.match(
    activeTechRules,
    /get\(techDocPath\(uid\)\)\.data\.active == true/
  );
  assert.equal(activeTechRules.includes('.data.active != false'), false);
  assert.match(
    activeTechRules,
    /isActiveTechUser\(request\.auth\.uid\)[\s\S]*request\.auth\.token\.supervisor == true/
  );
});

test('Firestore queue coordination collections stay server-only', () => {
  const rulesSource = fs.readFileSync(
    path.join(__dirname, '..', '..', 'firestore.rules'),
    'utf8'
  );

  for (const collection of [
    'support_queue_locks',
    'support_queue_anchors',
    'support_queue_outcomes',
    'support_tech_locks',
  ]) {
    const start = rulesSource.indexOf(`match /${collection}/`);
    assert.ok(start >= 0, `missing rules for ${collection}`);
    const block = rulesSource.slice(start, start + 160);
    assert.match(block, /allow read, write: if false/);
  }
});

test('bootstrap do supervisor exige habilitação explícita, e-mail por ambiente e comparação segura', () => {
  const serverSource = fs.readFileSync(
    path.join(__dirname, '..', 'server.js'),
    'utf8'
  );
  const route = serverSource.slice(
    serverSource.indexOf("app.post('/api/admin/bootstrap-supervisor'"),
    serverSource.indexOf("app.get('/api/admin/list-techs'")
  );

  assert.match(route, /SUPERVISOR_BOOTSTRAP_ENABLED/);
  assert.match(route, /SUPERVISOR_BOOTSTRAP_EMAIL/);
  assert.match(route, /timingSafeStringEqual\(providedSecret, secret\)/);
  assert.match(route, /req\.user\?\.email_verified !== true/);
  assert.match(route, /userRecord\.emailVerified !== true/);
  assert.equal(route.includes('isacxaviersoares@gmail.com'), false);
  assert.equal(route.includes('providedSecret !== secret'), false);
});

test('webhook Meta falha fechado em produção quando o segredo não está configurado', () => {
  const serverSource = fs.readFileSync(
    path.join(__dirname, '..', 'server.js'),
    'utf8'
  );
  const verifier = serverSource.slice(
    serverSource.indexOf('const verifyMetaWebhookSignature'),
    serverSource.indexOf('const resolveWhatsAppWebhookMessageText')
  );
  const route = serverSource.slice(
    serverSource.indexOf("app.post('/api/whatsapp-api/webhook'"),
    serverSource.indexOf("app.get('/api/whatsapp-api/conversations'")
  );

  assert.match(verifier, /ok: requireSecret !== true/);
  assert.match(verifier, /reason: 'secret_missing'/);
  assert.match(
    serverSource,
    /timingSafeStringEqual\(token, expectedToken\)/
  );
  assert.match(route, /ALLOW_UNSIGNED_META_WEBHOOK/);
  assert.match(route, /requireSecret: !allowUnsignedWebhook/);
  assert.match(route, /status\(503\)\.json\(\{ error: 'webhook_unavailable' \}\)/);
});

test('health profundo exige segredo em produção e usa contagem agregada', () => {
  const serverSource = fs.readFileSync(
    path.join(__dirname, '..', 'server.js'),
    'utf8'
  );
  const route = serverSource.slice(
    serverSource.indexOf("app.get('/health'"),
    serverSource.indexOf("app.post('/api/tech/profile-name'")
  );

  assert.match(route, /HEALTH_DEEP_SECRET/);
  assert.match(route, /ALLOW_UNAUTHENTICATED_DEEP_HEALTH/);
  assert.match(route, /req\.get\('x-health-secret'\)/);
  assert.match(route, /timingSafeStringEqual\(providedSecret, expectedSecret\)/);
  assert.match(route, /getFirestoreCollectionCount\(requestsCollection\)/);
  assert.match(route, /getFirestoreCollectionCount\(sessionsCollection\)/);
  assert.equal(route.includes('requestsCollection.get()'), false);
  assert.equal(route.includes('sessionsCollection.get()'), false);
});

test('erro público do Turnstile não devolve detalhe interno da exceção', () => {
  const serverSource = fs.readFileSync(
    path.join(__dirname, '..', 'server.js'),
    'utf8'
  );
  const route = serverSource.slice(
    serverSource.indexOf("app.post('/api/auth/turnstile/verify'"),
    serverSource.indexOf("app.get('/api/auth/me'")
  );

  assert.equal(route.includes('detail,'), false);
  assert.equal(route.includes("error?.message"), false);
});

test('verificação PNV devolve somente estado e telefone, sem JWT ou erro interno', () => {
  const serverSource = fs.readFileSync(
    path.join(__dirname, '..', 'server.js'),
    'utf8'
  );
  const verifier = serverSource.slice(
    serverSource.indexOf('const verifyFirebasePnvToken'),
    serverSource.indexOf('const mapPhoneVerificationError')
  );

  assert.match(verifier, /return \{ ok: true, phone: verifiedPhone \}/);
  assert.equal(verifier.includes('phone: verifiedPhone, payload'), false);
  assert.equal(verifier.includes('detail:'), false);
});

test('perfil autenticado não registra UID ou papel em log de rotina', () => {
  const serverSource = fs.readFileSync(
    path.join(__dirname, '..', 'server.js'),
    'utf8'
  );
  const route = serverSource.slice(
    serverSource.indexOf("app.get('/api/auth/me'"),
    serverSource.indexOf("app.post('/api/admin/bootstrap-supervisor'")
  );

  assert.equal(route.includes("console.log('[auth/me] uid:'"), false);
  assert.equal(route.includes("console.log('[auth/me] role claim:'"), false);
});

test('upload do catálogo valida a assinatura real antes de gravar no Storage', () => {
  const serverSource = fs.readFileSync(
    path.join(__dirname, '..', 'server.js'),
    'utf8'
  );
  const route = serverSource.slice(
    serverSource.indexOf("'/api/device-images/upload'"),
    serverSource.indexOf("app.get('/api/clients'")
  );

  assert.match(route, /validateFileSignature\(file, 'image', extension\)/);
  assert.ok(
    route.indexOf("validateFileSignature(file, 'image', extension)") <
      route.indexOf('uploadBucket.file(storagePath).save(file.buffer')
  );
  assert.match(route, /invalid_file_signature/);
});
