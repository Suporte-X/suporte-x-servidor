'use strict';

const crypto = require('node:crypto');

const CLIENT_MANUAL_VERIFICATION_HASH_VERSION = 2;

const normalizeString = (value) => String(value ?? '').trim();

const normalizeVerificationEmail = (value) => {
  const normalized = normalizeString(value).toLowerCase();
  if (!normalized || normalized.length > 254) return null;
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalized) ? normalized : null;
};

const hashClientManualVerificationCode = ({
  clientId = '',
  phone = '',
  email = '',
  code = '',
  salt = '',
  hashVersion = CLIENT_MANUAL_VERIFICATION_HASH_VERSION,
} = {}) => {
  const normalizedVersion = Number(hashVersion) === 1 ? 1 : CLIENT_MANUAL_VERIFICATION_HASH_VERSION;
  const parts = [
    normalizeString(clientId),
    normalizeString(phone),
  ];
  if (normalizedVersion >= 2) {
    parts.push(normalizeVerificationEmail(email) || '');
  }
  parts.push(normalizeString(code), normalizeString(salt));
  return crypto.createHash('sha256').update(parts.join(':')).digest('hex');
};

const validateClientManualVerificationCode = ({
  clientId = '',
  phone = '',
  email = '',
  code = '',
  manualCode = null,
  now = Date.now(),
} = {}) => {
  if (!manualCode || typeof manualCode !== 'object') {
    return { ok: false, status: 400, error: 'verification_code_missing' };
  }
  const expiresAt = Number(manualCode.expiresAt || 0);
  if (!Number.isFinite(expiresAt) || expiresAt < now) {
    return { ok: false, status: 400, error: 'verification_code_expired' };
  }
  const attempts = Math.max(0, Number.parseInt(manualCode.attempts, 10) || 0);
  const maxAttempts = Math.max(1, Number.parseInt(manualCode.maxAttempts, 10) || 1);
  if (attempts >= maxAttempts) {
    return { ok: false, status: 429, error: 'verification_code_attempts_exceeded' };
  }

  const expectedPhone = normalizeString(manualCode.phone) || null;
  const receivedPhone = normalizeString(phone) || null;
  const expectedEmail = normalizeVerificationEmail(manualCode.email);
  const receivedEmail = normalizeVerificationEmail(email);
  if (!expectedPhone && !expectedEmail) {
    return { ok: false, status: 400, error: 'verification_channel_missing' };
  }
  if (expectedPhone && receivedPhone && expectedPhone !== receivedPhone) {
    return { ok: false, status: 409, error: 'verification_phone_mismatch' };
  }
  if (expectedEmail && receivedEmail && expectedEmail !== receivedEmail) {
    return { ok: false, status: 409, error: 'verification_email_mismatch' };
  }

  const verifiedPhone = expectedPhone || receivedPhone;
  const verifiedEmail = expectedEmail || receivedEmail;
  const hashVersion = Number(manualCode.hashVersion) >= CLIENT_MANUAL_VERIFICATION_HASH_VERSION
    ? CLIENT_MANUAL_VERIFICATION_HASH_VERSION
    : 1;
  const expectedHash = normalizeString(manualCode.codeHash);
  const salt = normalizeString(manualCode.salt);
  const receivedHash = hashClientManualVerificationCode({
    clientId,
    phone: verifiedPhone || '',
    email: verifiedEmail || '',
    code,
    salt,
    hashVersion,
  });
  if (!expectedHash || !salt || receivedHash !== expectedHash) {
    return { ok: false, status: 400, error: 'invalid_verification_code' };
  }

  return {
    ok: true,
    verifiedPhone: verifiedPhone || null,
    verifiedEmail: verifiedEmail || null,
    verificationChannel: verifiedPhone ? 'phone' : 'email',
  };
};

module.exports = {
  CLIENT_MANUAL_VERIFICATION_HASH_VERSION,
  hashClientManualVerificationCode,
  normalizeVerificationEmail,
  validateClientManualVerificationCode,
};
