'use strict';

const CLOUDFLARE_TURN_CREDENTIALS_BASE_URL =
  'https://rtc.live.cloudflare.com/v1/turn/keys';
const DEFAULT_TTL_SECONDS = 3600;
const MIN_TTL_SECONDS = 300;
const MAX_TTL_SECONDS = 86400;
const DEFAULT_TIMEOUT_MS = 4000;
const MIN_TIMEOUT_MS = 500;
const MAX_TIMEOUT_MS = 10000;
const DEFAULT_CACHE_SECONDS = 30;
const MAX_CACHE_SECONDS = 60;
const MAX_CACHE_ENTRIES = 500;
const FALLBACK_TTL_MS = 2 * 60 * 1000;
const TERMINAL_SESSION_STATES = new Set([
  'cancelled',
  'canceled',
  'closed',
  'completed',
  'ended',
  'expired',
  'rejected',
]);

class TurnCredentialError extends Error {
  constructor(code, { upstreamStatus = null } = {}) {
    super(code);
    this.name = 'TurnCredentialError';
    this.code = code;
    this.upstreamStatus =
      Number.isInteger(upstreamStatus) && upstreamStatus > 0
        ? upstreamStatus
        : null;
  }
}

const compactString = (value, maxLength = 4096) => {
  if (typeof value !== 'string') return '';
  return value.trim().slice(0, maxLength);
};

const boundedInteger = (value, fallback, minimum, maximum) => {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(minimum, Math.min(maximum, parsed));
};

const parseTurnEnvironment = (env = {}) => {
  const keyId = compactString(env.CLOUDFLARE_TURN_KEY_ID, 128);
  const apiToken = compactString(env.CLOUDFLARE_TURN_KEY_API_TOKEN, 8192);
  const keyIdValid = /^[A-Za-z0-9_-]{1,128}$/.test(keyId);
  const ttlSeconds = boundedInteger(
    env.CLOUDFLARE_TURN_TTL_SECONDS,
    DEFAULT_TTL_SECONDS,
    MIN_TTL_SECONDS,
    MAX_TTL_SECONDS
  );
  const timeoutMs = boundedInteger(
    env.CLOUDFLARE_TURN_TIMEOUT_MS,
    DEFAULT_TIMEOUT_MS,
    MIN_TIMEOUT_MS,
    MAX_TIMEOUT_MS
  );
  const requestedCacheSeconds = boundedInteger(
    env.CLOUDFLARE_TURN_CACHE_SECONDS,
    DEFAULT_CACHE_SECONDS,
    1,
    MAX_CACHE_SECONDS
  );
  const maximumSafeCacheSeconds = Math.max(1, ttlSeconds - 60);
  const cacheMs =
    Math.min(requestedCacheSeconds, maximumSafeCacheSeconds) * 1000;

  return {
    configured: Boolean(keyIdValid && apiToken),
    keyId,
    apiToken,
    ttlSeconds,
    timeoutMs,
    cacheMs,
  };
};

const fallbackIceServers = () => [
  {
    urls: ['stun:stun.cloudflare.com:3478'],
  },
];

const cloneIceServers = (iceServers = []) =>
  iceServers.map((server) => ({
    urls: Array.isArray(server.urls) ? [...server.urls] : server.urls,
    ...(server.username ? { username: server.username } : {}),
    ...(server.credential ? { credential: server.credential } : {}),
  }));

const cloneIceConfig = (config, source = config.source) => ({
  iceServers: cloneIceServers(config.iceServers),
  expiresAt: config.expiresAt,
  source,
});

const createStunFallback = ({ now = Date.now(), source }) => ({
  iceServers: fallbackIceServers(),
  expiresAt: now + FALLBACK_TTL_MS,
  source,
});

const normalizeIceUrls = (value) => {
  const candidates = Array.isArray(value) ? value : [value];
  return candidates
    .map((url) => compactString(url, 512))
    .filter((url) => /^(stun|turn|turns):/i.test(url))
    .filter((url, index, urls) => url && urls.indexOf(url) === index)
    .slice(0, 12);
};

const parseCloudflareIceServers = (payload) => {
  if (!payload || typeof payload !== 'object' || !Array.isArray(payload.iceServers)) {
    throw new TurnCredentialError('turn_invalid_response');
  }

  let hasCredentialedTurnServer = false;
  const iceServers = payload.iceServers
    .map((rawServer) => {
      if (!rawServer || typeof rawServer !== 'object') return null;
      const urls = normalizeIceUrls(rawServer.urls);
      if (!urls.length) return null;

      const username = compactString(rawServer.username, 4096);
      const credential = compactString(rawServer.credential, 4096);
      const turnUrls = urls.filter((url) => /^turns?:/i.test(url));
      const stunUrls = urls.filter((url) => /^stun:/i.test(url));

      if (turnUrls.length && username && credential) {
        hasCredentialedTurnServer = true;
        return { urls, username, credential };
      }
      if (stunUrls.length) return { urls: stunUrls };
      return null;
    })
    .filter(Boolean)
    .slice(0, 8);

  if (!iceServers.length || !hasCredentialedTurnServer) {
    throw new TurnCredentialError('turn_invalid_response');
  }
  return iceServers;
};

const evaluateSessionIceAccess = ({
  authUid = '',
  userRole = '',
  sessionData = null,
  isTechActive = false,
} = {}) => {
  const uid = compactString(authUid, 256);
  if (!uid) return { ok: false, status: 401, error: 'invalid_token' };
  if (!sessionData || typeof sessionData !== 'object') {
    return { ok: false, status: 404, error: 'session_not_found' };
  }

  const clientUid = compactString(sessionData.clientUid, 256);
  const techUid = compactString(
    sessionData.techUid ||
      sessionData.tech?.techUid ||
      sessionData.tech?.uid ||
      '',
    256
  );

  let authorizedRole = null;
  if (techUid && uid === techUid) {
    if (compactString(userRole, 64).toLowerCase() !== 'tech') {
      return { ok: false, status: 403, error: 'insufficient_role' };
    }
    if (isTechActive !== true) {
      return { ok: false, status: 403, error: 'tech_inactive' };
    }
    authorizedRole = 'tech';
  } else if (clientUid && uid === clientUid) {
    authorizedRole = 'client';
  } else {
    return { ok: false, status: 403, error: 'forbidden' };
  }

  const sessionStatus = compactString(sessionData.status, 64).toLowerCase();
  if (TERMINAL_SESSION_STATES.has(sessionStatus)) {
    return { ok: false, status: 409, error: 'session_not_active' };
  }
  return { ok: true, role: authorizedRole };
};

class TurnCredentialsService {
  constructor({
    env = process.env,
    config = null,
    fetchImpl = globalThis.fetch,
    now = Date.now,
    setTimer = setTimeout,
    clearTimer = clearTimeout,
    onDiagnostic = null,
  } = {}) {
    this.config = config || parseTurnEnvironment(env);
    this.fetchImpl = fetchImpl;
    this.now = now;
    this.setTimer = setTimer;
    this.clearTimer = clearTimer;
    this.onDiagnostic =
      typeof onDiagnostic === 'function' ? onDiagnostic : () => {};
    this.cache = new Map();
    this.inFlight = new Map();
    this.reportedUnconfigured = false;
  }

  diagnose(code, upstreamStatus = null) {
    try {
      this.onDiagnostic({
        code: compactString(code, 64) || 'turn_unknown_error',
        upstreamStatus:
          Number.isInteger(upstreamStatus) && upstreamStatus > 0
            ? upstreamStatus
            : null,
      });
    } catch (_error) {
      // Diagnostics must never interfere with the STUN fallback.
    }
  }

  getCached(cacheKey, now) {
    const cached = this.cache.get(cacheKey);
    if (!cached) return null;
    if (now >= cached.cacheUntil || now >= cached.config.expiresAt) {
      this.cache.delete(cacheKey);
      return null;
    }
    return cloneIceConfig(cached.config, 'cloudflare_turn_cache');
  }

  pruneCache(now) {
    for (const [cacheKey, cached] of this.cache.entries()) {
      if (now >= cached.cacheUntil || now >= cached.config.expiresAt) {
        this.cache.delete(cacheKey);
      }
    }
    while (this.cache.size >= MAX_CACHE_ENTRIES) {
      const oldestKey = this.cache.keys().next().value;
      if (!oldestKey) break;
      this.cache.delete(oldestKey);
    }
  }

  async generateCloudflareConfig(cacheKey) {
    const startedAt = this.now();
    const ttlMs = Math.max(1000, Number(this.config.ttlSeconds) * 1000);
    const timeoutMs = Math.max(1, Number(this.config.timeoutMs) || DEFAULT_TIMEOUT_MS);
    const cacheMs = Math.max(0, Number(this.config.cacheMs) || 0);
    const controller = new AbortController();
    let timedOut = false;
    const timeoutId = this.setTimer(() => {
      timedOut = true;
      controller.abort();
    }, timeoutMs);

    let response;
    let payload;
    try {
      const endpoint =
        `${CLOUDFLARE_TURN_CREDENTIALS_BASE_URL}/` +
        `${encodeURIComponent(this.config.keyId)}/credentials/generate-ice-servers`;
      response = await this.fetchImpl(endpoint, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${this.config.apiToken}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ ttl: this.config.ttlSeconds }),
        signal: controller.signal,
      });
      if (!response || response.ok !== true) {
        throw new TurnCredentialError('turn_upstream_http_error', {
          upstreamStatus: Number(response?.status) || null,
        });
      }
      try {
        payload = await response.json();
      } catch (error) {
        if (timedOut || error?.name === 'AbortError') throw error;
        throw new TurnCredentialError('turn_invalid_response');
      }
    } catch (error) {
      if (error instanceof TurnCredentialError) throw error;
      if (timedOut || error?.name === 'AbortError') {
        throw new TurnCredentialError('turn_timeout');
      }
      throw new TurnCredentialError('turn_upstream_unavailable');
    } finally {
      this.clearTimer(timeoutId);
    }

    const iceServers = parseCloudflareIceServers(payload);
    const expiresAt = startedAt + ttlMs;
    const completedAt = this.now();
    if (completedAt >= expiresAt) {
      throw new TurnCredentialError('turn_credential_expired');
    }

    const credentialSafetyMs = Math.min(
      60 * 1000,
      Math.max(1000, Math.floor(ttlMs * 0.1))
    );
    const cacheUntil = Math.min(
      completedAt + cacheMs,
      expiresAt - credentialSafetyMs
    );
    const result = {
      iceServers,
      expiresAt,
      source: 'cloudflare_turn',
    };
    if (cacheUntil > completedAt) {
      this.pruneCache(completedAt);
      this.cache.delete(cacheKey);
      this.cache.set(cacheKey, {
        config: cloneIceConfig(result),
        cacheUntil,
      });
    }
    return cloneIceConfig(result);
  }

  async getIceConfig({ cacheKey = 'default' } = {}) {
    const normalizedCacheKey = compactString(cacheKey, 512) || 'default';
    const now = this.now();
    if (
      !this.config?.configured ||
      typeof this.fetchImpl !== 'function'
    ) {
      if (!this.reportedUnconfigured) {
        this.reportedUnconfigured = true;
        this.diagnose('turn_unconfigured');
      }
      return createStunFallback({
        now,
        source: 'stun_fallback_unconfigured',
      });
    }

    this.pruneCache(now);
    const cached = this.getCached(normalizedCacheKey, now);
    if (cached) return cached;

    const existingRequest = this.inFlight.get(normalizedCacheKey);
    if (existingRequest) {
      return cloneIceConfig(await existingRequest);
    }

    const request = this.generateCloudflareConfig(normalizedCacheKey);
    this.inFlight.set(normalizedCacheKey, request);
    try {
      return cloneIceConfig(await request);
    } catch (error) {
      const code =
        error instanceof TurnCredentialError
          ? error.code
          : 'turn_upstream_unavailable';
      const upstreamStatus =
        error instanceof TurnCredentialError ? error.upstreamStatus : null;
      this.diagnose(code, upstreamStatus);

      const source =
        code === 'turn_timeout'
          ? 'stun_fallback_timeout'
          : code === 'turn_invalid_response' || code === 'turn_credential_expired'
            ? 'stun_fallback_invalid_response'
            : 'stun_fallback_upstream';
      return createStunFallback({ now: this.now(), source });
    } finally {
      if (this.inFlight.get(normalizedCacheKey) === request) {
        this.inFlight.delete(normalizedCacheKey);
      }
    }
  }
}

module.exports = {
  MAX_TTL_SECONDS,
  MIN_TTL_SECONDS,
  TurnCredentialError,
  TurnCredentialsService,
  createStunFallback,
  evaluateSessionIceAccess,
  parseCloudflareIceServers,
  parseTurnEnvironment,
};
