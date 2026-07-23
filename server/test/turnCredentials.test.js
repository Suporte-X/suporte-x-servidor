'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const {
  MAX_TTL_SECONDS,
  MIN_TTL_SECONDS,
  TurnCredentialError,
  TurnCredentialsService,
  evaluateSessionIceAccess,
  parseCloudflareIceServers,
  parseTurnEnvironment,
} = require('../turnCredentials');

const cloudflarePayload = () => ({
  iceServers: [
    {
      urls: [
        'stun:stun.cloudflare.com:3478',
        'https://not-an-ice-server.invalid',
      ],
    },
    {
      urls: [
        'turn:turn.cloudflare.com:3478?transport=udp',
        'turns:turn.cloudflare.com:443?transport=tcp',
      ],
      username: 'short-lived-user',
      credential: 'short-lived-credential',
    },
  ],
});

const successResponse = () => ({
  ok: true,
  status: 201,
  json: async () => cloudflarePayload(),
});

const serviceConfig = (overrides = {}) => ({
  configured: true,
  keyId: 'turn-key-id',
  apiToken: 'server-only-api-token',
  ttlSeconds: 300,
  timeoutMs: 100,
  cacheMs: 30_000,
  ...overrides,
});

test('configuração limita TTL, timeout e cache e exige as duas chaves', () => {
  const missing = parseTurnEnvironment({
    CLOUDFLARE_TURN_KEY_ID: 'key-only',
  });
  assert.equal(missing.configured, false);
  assert.equal(missing.ttlSeconds >= MIN_TTL_SECONDS, true);

  const configured = parseTurnEnvironment({
    CLOUDFLARE_TURN_KEY_ID: 'valid_key-123',
    CLOUDFLARE_TURN_KEY_API_TOKEN: 'api-token',
    CLOUDFLARE_TURN_TTL_SECONDS: '999999',
    CLOUDFLARE_TURN_TIMEOUT_MS: '1',
    CLOUDFLARE_TURN_CACHE_SECONDS: '999',
  });
  assert.equal(configured.configured, true);
  assert.equal(configured.ttlSeconds, MAX_TTL_SECONDS);
  assert.equal(configured.timeoutMs, 500);
  assert.equal(configured.cacheMs, 60_000);
});

test('resposta Cloudflare é filtrada e exige TURN com credencial', () => {
  const parsed = parseCloudflareIceServers(cloudflarePayload());
  assert.deepEqual(parsed, [
    {
      urls: ['stun:stun.cloudflare.com:3478'],
    },
    {
      urls: [
        'turn:turn.cloudflare.com:3478?transport=udp',
        'turns:turn.cloudflare.com:443?transport=tcp',
      ],
      username: 'short-lived-user',
      credential: 'short-lived-credential',
    },
  ]);

  assert.throws(
    () =>
      parseCloudflareIceServers({
        iceServers: [{ urls: ['turn:turn.cloudflare.com:3478'] }],
      }),
    (error) =>
      error instanceof TurnCredentialError &&
      error.code === 'turn_invalid_response'
  );
});

test('credencial fica em cache curto, isolada de mutação e nunca passa da expiração', async () => {
  let now = 1_000_000;
  let fetchCount = 0;
  const service = new TurnCredentialsService({
    config: serviceConfig({ ttlSeconds: 2, cacheMs: 10_000 }),
    now: () => now,
    fetchImpl: async (_url, request) => {
      fetchCount += 1;
      assert.equal(request.method, 'POST');
      assert.deepEqual(JSON.parse(request.body), { ttl: 2 });
      return successResponse();
    },
  });

  const first = await service.getIceConfig({ cacheKey: 'session:user' });
  assert.equal(first.source, 'cloudflare_turn');
  assert.equal(first.expiresAt, 1_002_000);
  assert.equal(fetchCount, 1);

  first.iceServers[1].credential = 'mutated-by-caller';
  now += 500;
  const cached = await service.getIceConfig({ cacheKey: 'session:user' });
  assert.equal(cached.source, 'cloudflare_turn_cache');
  assert.equal(cached.iceServers[1].credential, 'short-lived-credential');
  assert.equal(fetchCount, 1);

  now = 1_002_001;
  const refreshed = await service.getIceConfig({ cacheKey: 'session:user' });
  assert.equal(refreshed.source, 'cloudflare_turn');
  assert.equal(refreshed.expiresAt, 1_004_001);
  assert.equal(fetchCount, 2);
});

test('requisições simultâneas da mesma sessão compartilham uma geração', async () => {
  let releaseFetch;
  let fetchCount = 0;
  const pendingResponse = new Promise((resolve) => {
    releaseFetch = resolve;
  });
  const service = new TurnCredentialsService({
    config: serviceConfig(),
    now: () => 2_000_000,
    fetchImpl: async () => {
      fetchCount += 1;
      return pendingResponse;
    },
  });

  const first = service.getIceConfig({ cacheKey: 'same-session:user' });
  const second = service.getIceConfig({ cacheKey: 'same-session:user' });
  await Promise.resolve();
  assert.equal(fetchCount, 1);
  releaseFetch(successResponse());

  const [firstResult, secondResult] = await Promise.all([first, second]);
  assert.equal(firstResult.source, 'cloudflare_turn');
  assert.equal(secondResult.source, 'cloudflare_turn');
  assert.equal(fetchCount, 1);
});

test('timeout e falha HTTP retornam STUN e diagnóstico sem segredo', async () => {
  const timeoutDiagnostics = [];
  const timeoutService = new TurnCredentialsService({
    config: serviceConfig({ timeoutMs: 10 }),
    setTimer: (callback) => {
      queueMicrotask(callback);
      return 1;
    },
    clearTimer: () => {},
    onDiagnostic: (diagnostic) => timeoutDiagnostics.push(diagnostic),
    fetchImpl: async (_url, { signal }) =>
      new Promise((_resolve, reject) => {
        const abort = () => {
          const error = new Error('aborted');
          error.name = 'AbortError';
          reject(error);
        };
        if (signal.aborted) abort();
        else signal.addEventListener('abort', abort, { once: true });
      }),
  });
  const timeoutFallback = await timeoutService.getIceConfig({
    cacheKey: 'timeout',
  });
  assert.equal(timeoutFallback.source, 'stun_fallback_timeout');
  assert.deepEqual(timeoutFallback.iceServers, [
    { urls: ['stun:stun.cloudflare.com:3478'] },
  ]);
  assert.deepEqual(timeoutDiagnostics, [
    { code: 'turn_timeout', upstreamStatus: null },
  ]);

  const httpDiagnostics = [];
  const httpFailureService = new TurnCredentialsService({
    config: serviceConfig(),
    onDiagnostic: (diagnostic) => httpDiagnostics.push(diagnostic),
    fetchImpl: async () => ({ ok: false, status: 429 }),
  });
  const httpFallback = await httpFailureService.getIceConfig({
    cacheKey: 'http-failure',
  });
  assert.equal(httpFallback.source, 'stun_fallback_upstream');
  assert.deepEqual(httpDiagnostics, [
    { code: 'turn_upstream_http_error', upstreamStatus: 429 },
  ]);
  assert.equal(JSON.stringify(httpDiagnostics).includes('server-only-api-token'), false);
});

test('ambiente ausente retorna STUN sem consultar a Cloudflare', async () => {
  let fetchCount = 0;
  const diagnostics = [];
  const service = new TurnCredentialsService({
    env: {},
    fetchImpl: async () => {
      fetchCount += 1;
      return successResponse();
    },
    onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
    now: () => 5_000,
  });

  const fallback = await service.getIceConfig({ cacheKey: 'unconfigured' });
  assert.equal(fallback.source, 'stun_fallback_unconfigured');
  assert.equal(fallback.expiresAt, 125_000);
  assert.equal(fetchCount, 0);
  assert.deepEqual(diagnostics, [
    { code: 'turn_unconfigured', upstreamStatus: null },
  ]);
});

test('somente cliente ou técnico ativo da sessão recebe configuração ICE', () => {
  const sessionData = {
    status: 'active',
    clientUid: 'client-uid',
    techUid: 'tech-uid',
  };

  assert.deepEqual(
    evaluateSessionIceAccess({
      authUid: 'client-uid',
      userRole: 'user',
      sessionData,
    }),
    { ok: true, role: 'client' }
  );
  assert.deepEqual(
    evaluateSessionIceAccess({
      authUid: 'tech-uid',
      userRole: 'tech',
      sessionData,
      isTechActive: true,
    }),
    { ok: true, role: 'tech' }
  );
  assert.deepEqual(
    evaluateSessionIceAccess({
      authUid: 'tech-uid',
      userRole: 'tech',
      sessionData,
      isTechActive: false,
    }),
    { ok: false, status: 403, error: 'tech_inactive' }
  );
  assert.deepEqual(
    evaluateSessionIceAccess({
      authUid: 'intruder',
      userRole: 'user',
      sessionData,
    }),
    { ok: false, status: 403, error: 'forbidden' }
  );
  assert.deepEqual(
    evaluateSessionIceAccess({
      authUid: 'client-uid',
      userRole: 'user',
      sessionData: { ...sessionData, status: 'closed' },
    }),
    { ok: false, status: 409, error: 'session_not_active' }
  );
});

test('rota ICE exige autenticação e valida membership antes do provedor', () => {
  const serverSource = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  const routeStart = serverSource.indexOf(
    "app.get('/api/webrtc/ice-config', requireAuth()"
  );
  const routeEnd = serverSource.indexOf(
    'const normalizeLegacyRoom',
    routeStart
  );
  const routeSource = serverSource.slice(routeStart, routeEnd);

  assert.ok(routeStart >= 0);
  assert.ok(routeEnd > routeStart);
  assert.match(routeSource, /await getSessionSnapshot\(sessionId\)/);
  assert.match(routeSource, /evaluateSessionIceAccess\(\{/);
  assert.match(routeSource, /await isActiveTechUid\(authUid\)/);
  assert.ok(
    routeSource.indexOf('evaluateSessionIceAccess({') <
      routeSource.indexOf('turnCredentialsService.getIceConfig({')
  );
  assert.equal(routeSource.includes('CLOUDFLARE_TURN_KEY_API_TOKEN'), false);
});
