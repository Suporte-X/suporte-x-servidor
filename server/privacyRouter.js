const crypto = require('node:crypto');
const express = require('express');

const {
  AccountDeletionError,
  createAccountDeletionService,
  defaultNormalizePhone,
} = require('./accountDeletionService');

const DEFAULT_PUBLIC_REQUEST_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const GENERIC_DELETION_REQUEST_RESPONSE = Object.freeze({
  ok: true,
  status: 'received',
  message:
    'Se os dados informados corresponderem a uma conta, a Suporte X continuará a verificação pelos canais oficiais.',
});

function createPrivacyRouter({
  db,
  auth,
  bucket,
  accountDeletionService = null,
  verifyPnvToken = null,
  verifyTurnstile = null,
  normalizePhone = defaultNormalizePhone,
  clock = () => Date.now(),
  logger = console,
  publicRequestCollection = 'privacy_deletion_requests',
  publicRequestTtlMs = DEFAULT_PUBLIC_REQUEST_TTL_MS,
  rateLimiter = null,
  rateLimit = {},
  protectContact = null,
} = {}) {
  if (!db || typeof db.collection !== 'function') {
    throw new Error('createPrivacyRouter requires Firestore db');
  }
  if (!auth || typeof auth.verifyIdToken !== 'function') {
    throw new Error('createPrivacyRouter requires auth.verifyIdToken');
  }

  const service =
    accountDeletionService ||
    createAccountDeletionService({
      db,
      auth,
      bucket,
      verifyPnvToken,
      normalizePhone,
      clock,
      logger,
    });
  if (!service || typeof service.deleteAccount !== 'function') {
    throw new Error('createPrivacyRouter requires accountDeletionService.deleteAccount');
  }

  const limiter =
    rateLimiter ||
    createMemoryRateLimiter({
      clock,
      ...rateLimit,
    });
  const log = normalizeLogger(logger);
  const router = express.Router();
  router.use(express.json({ limit: '32kb' }));

  router.post('/client/account/delete', async (req, res) => {
    const token = extractBearerToken(req.headers.authorization);
    if (!token) {
      return res.status(401).json({ error: 'missing_token' });
    }

    let decoded;
    try {
      decoded = await auth.verifyIdToken(token, true);
    } catch (error) {
      log.warn('Account deletion authentication failed', {
        code: safeErrorCode(error),
      });
      return res.status(401).json({ error: 'invalid_token' });
    }

    const role = normalizeRole(decoded?.role);
    if (role !== 'user') {
      return res.status(403).json({ error: 'insufficient_role' });
    }

    try {
      const result = await service.deleteAccount({
        uid: decoded.uid,
        confirmation: req.body?.confirmation,
        idempotencyKey: req.get('Idempotency-Key'),
        pnvToken: req.body?.pnvToken,
        pnvPhone: req.body?.pnvPhone,
      });
      return res.status(200).json(result);
    } catch (error) {
      if (error instanceof AccountDeletionError) {
        return res.status(error.status).json({ error: error.code });
      }
      log.error('Authenticated account deletion failed', error);
      return res.status(500).json({ error: 'account_deletion_failed' });
    }
  });

  router.post('/privacy/deletion-requests', async (req, res) => {
    const clientKey = resolveClientRateLimitKey(req);
    const limitResult = limiter.consume(clientKey);
    if (!limitResult.allowed) {
      res.setHeader('Retry-After', String(Math.max(1, Math.ceil(limitResult.retryAfterMs / 1000))));
      return res.status(429).json({
        error: 'too_many_requests',
        message: 'Tente novamente mais tarde.',
      });
    }

    const turnstileToken = normalizeShortText(
      req.body?.turnstileToken ||
        req.body?.captchaToken ||
        req.body?.['cf-turnstile-response'],
      4096
    );
    const turnstileOk = await verifyTurnstileSafely({
      verifier: verifyTurnstile,
      token: turnstileToken,
      remoteIp: req.ip,
      request: req,
      logger: log,
    });
    const contact = normalizeDeletionContact(req.body, normalizePhone);

    // CAPTCHA and contact validation deliberately have the same public response
    // as a stored request, preventing account/contact enumeration.
    if (!turnstileOk || !contact) {
      return res.status(202).json(GENERIC_DELETION_REQUEST_RESPONSE);
    }

    const requestId = crypto.randomUUID();
    const now = clock();
    let protectedContact;
    let contactHash;
    try {
      if (
        typeof protectContact !== 'function' ||
        typeof protectContact.hash !== 'function'
      ) {
        throw new Error('privacy_contact_protection_not_configured');
      }
      protectedContact = await protectContact(contact.value, {
        type: contact.type,
        requestId,
      });
      contactHash = await protectContact.hash(contact.value, {
        type: contact.type,
      });
      if (
        typeof protectedContact !== 'string' ||
        !protectedContact.trim() ||
        protectedContact === contact.value ||
        typeof contactHash !== 'string' ||
        !/^[a-f0-9]{64}$/.test(contactHash)
      ) {
        throw new Error('privacy_contact_protection_invalid');
      }
    } catch (error) {
      log.error('Failed to protect privacy request contact', error);
      return res.status(503).json({ error: 'temporarily_unavailable' });
    }

    try {
      await db.collection(publicRequestCollection).doc(requestId).set({
        requestId,
        requestType: 'account_and_data_deletion',
        source: 'public_web',
        status: 'received',
        contactType: contact.type,
        contact: protectedContact,
        contactHash,
        createdAt: now,
        updatedAt: now,
        expiresAt: new Date(now + publicRequestTtlMs),
      });
    } catch (error) {
      log.error('Failed to store public privacy deletion request', error);
      return res.status(503).json({ error: 'temporarily_unavailable' });
    }

    return res.status(202).json(GENERIC_DELETION_REQUEST_RESPONSE);
  });

  return router;
}

function createPrivacyContactProtector(rawKey) {
  const masterKey = parsePrivacyContactEncryptionKey(rawKey);
  const encryptionKey = Buffer.from(
    crypto.hkdfSync(
      'sha256',
      masterKey,
      Buffer.alloc(0),
      Buffer.from('suportex/privacy-contact/encryption/v1', 'utf8'),
      32
    )
  );
  const lookupKey = Buffer.from(
    crypto.hkdfSync(
      'sha256',
      masterKey,
      Buffer.alloc(0),
      Buffer.from('suportex/privacy-contact/lookup/v1', 'utf8'),
      32
    )
  );

  const protect = async (contact, { type = '', requestId = '' } = {}) => {
    const value = normalizeShortText(contact, 320);
    const normalizedType = normalizeShortText(type, 24).toLowerCase();
    const normalizedRequestId = normalizeShortText(requestId, 128);
    if (!value || !normalizedType || !normalizedRequestId) {
      throw new Error('privacy_contact_encryption_context_required');
    }

    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv('aes-256-gcm', encryptionKey, iv);
    cipher.setAAD(buildPrivacyContactAad(normalizedType, normalizedRequestId));
    const encrypted = Buffer.concat([
      cipher.update(value, 'utf8'),
      cipher.final(),
    ]);
    const tag = cipher.getAuthTag();
    return [
      'v1',
      iv.toString('base64url'),
      tag.toString('base64url'),
      encrypted.toString('base64url'),
    ].join('.');
  };

  protect.open = async (sealedContact, { type = '', requestId = '' } = {}) => {
    const parts = String(sealedContact || '').split('.');
    if (parts.length !== 4 || parts[0] !== 'v1') {
      throw new Error('privacy_contact_ciphertext_invalid');
    }
    const normalizedType = normalizeShortText(type, 24).toLowerCase();
    const normalizedRequestId = normalizeShortText(requestId, 128);
    if (!normalizedType || !normalizedRequestId) {
      throw new Error('privacy_contact_encryption_context_required');
    }

    const iv = Buffer.from(parts[1], 'base64url');
    const tag = Buffer.from(parts[2], 'base64url');
    const encrypted = Buffer.from(parts[3], 'base64url');
    if (iv.length !== 12 || tag.length !== 16 || encrypted.length === 0) {
      throw new Error('privacy_contact_ciphertext_invalid');
    }

    const decipher = crypto.createDecipheriv('aes-256-gcm', encryptionKey, iv);
    decipher.setAAD(buildPrivacyContactAad(normalizedType, normalizedRequestId));
    decipher.setAuthTag(tag);
    return Buffer.concat([
      decipher.update(encrypted),
      decipher.final(),
    ]).toString('utf8');
  };

  protect.hash = async (contact, { type = '' } = {}) => {
    const value = normalizeShortText(contact, 320);
    const normalizedType = normalizeShortText(type, 24).toLowerCase();
    if (!value || !normalizedType) {
      throw new Error('privacy_contact_hash_context_required');
    }
    return crypto
      .createHmac('sha256', lookupKey)
      .update(`privacy-contact-hash-v1\n${normalizedType}\n${value}`, 'utf8')
      .digest('hex');
  };

  return protect;
}

function parsePrivacyContactEncryptionKey(rawKey) {
  const value = String(rawKey || '').trim();
  let key;
  if (/^[a-fA-F0-9]{64}$/.test(value)) {
    key = Buffer.from(value, 'hex');
  } else if (/^[A-Za-z0-9+/_-]+={0,2}$/.test(value)) {
    key = Buffer.from(value, 'base64url');
  } else {
    key = Buffer.alloc(0);
  }
  if (key.length !== 32) {
    throw new Error(
      'PRIVACY_CONTACT_ENCRYPTION_KEY must contain exactly 32 bytes in base64 or hex'
    );
  }
  return key;
}

function buildPrivacyContactAad(type, requestId) {
  return Buffer.from(`privacy-contact-v1\n${type}\n${requestId}`, 'utf8');
}

function createMemoryRateLimiter({
  limit = 5,
  windowMs = 15 * 60 * 1000,
  clock = () => Date.now(),
  maxEntries = 10_000,
} = {}) {
  const safeLimit = Math.max(1, Number(limit) || 5);
  const safeWindowMs = Math.max(1_000, Number(windowMs) || 15 * 60 * 1000);
  const entries = new Map();

  function consume(rawKey) {
    const now = clock();
    const key = normalizeShortText(rawKey, 256) || 'unknown';
    let entry = entries.get(key);
    if (!entry || entry.resetAt <= now) {
      entry = { count: 0, resetAt: now + safeWindowMs };
    }
    entry.count += 1;
    entries.set(key, entry);

    if (entries.size > maxEntries) {
      for (const [candidateKey, candidate] of entries.entries()) {
        if (candidate.resetAt <= now || entries.size > maxEntries) {
          entries.delete(candidateKey);
        }
        if (entries.size <= maxEntries) break;
      }
    }

    return {
      allowed: entry.count <= safeLimit,
      remaining: Math.max(0, safeLimit - entry.count),
      retryAfterMs: Math.max(0, entry.resetAt - now),
    };
  }

  return {
    consume,
    reset() {
      entries.clear();
    },
  };
}

async function verifyTurnstileSafely({ verifier, token, remoteIp, request, logger }) {
  if (typeof verifier !== 'function' || !token) return false;
  try {
    const result = await verifier({
      token,
      remoteIp,
      request,
    });
    return result === true || result?.ok === true || result?.success === true;
  } catch (error) {
    logger.warn('Turnstile verification failed for privacy request', {
      code: safeErrorCode(error),
    });
    return false;
  }
}

function normalizeDeletionContact(body, normalizePhone) {
  const rawType = normalizeShortText(
    body?.contactType || body?.channel || body?.method,
    24
  ).toLowerCase();
  const rawContact = normalizeShortText(
    body?.contact || body?.email || body?.phone,
    320
  );
  if (!rawContact) return null;

  const inferredType = rawContact.includes('@') ? 'email' : 'phone';
  const type = rawType === 'email' || rawType === 'phone' ? rawType : inferredType;
  if (type === 'email') {
    const email = rawContact.trim().toLowerCase();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) || email.length > 254) return null;
    return { type, value: email };
  }

  const phone = normalizePhone(rawContact);
  return phone ? { type: 'phone', value: phone } : null;
}

function resolveClientRateLimitKey(req) {
  const ip = normalizeShortText(req.ip || req.socket?.remoteAddress || '', 128);
  return `ip:${ip || 'unknown'}`;
}

function extractBearerToken(header) {
  if (typeof header !== 'string') return '';
  const match = header.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : '';
}

function normalizeRole(value) {
  if (typeof value !== 'string') return 'user';
  return value.trim().toLowerCase() || 'user';
}

function normalizeShortText(value, maxLength) {
  if (typeof value !== 'string' && typeof value !== 'number') return '';
  return String(value).trim().slice(0, maxLength);
}

function normalizeLogger(logger) {
  return {
    error: typeof logger?.error === 'function' ? logger.error.bind(logger) : () => {},
    warn: typeof logger?.warn === 'function' ? logger.warn.bind(logger) : () => {},
  };
}

function safeErrorCode(error) {
  const raw = String(error?.code || error?.name || 'unknown_error')
    .trim()
    .toLowerCase();
  return /^[a-z0-9_:/.-]{1,96}$/.test(raw) ? raw : 'unknown_error';
}

module.exports = {
  DEFAULT_PUBLIC_REQUEST_TTL_MS,
  GENERIC_DELETION_REQUEST_RESPONSE,
  createPrivacyContactProtector,
  createMemoryRateLimiter,
  createPrivacyRouter,
  extractBearerToken,
  normalizeDeletionContact,
};
