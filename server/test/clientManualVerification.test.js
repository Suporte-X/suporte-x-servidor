'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {
  CLIENT_MANUAL_VERIFICATION_HASH_VERSION,
  hashClientManualVerificationCode,
  normalizeVerificationEmail,
  validateClientManualVerificationCode,
} = require('../clientManualVerification');

const CLIENT_ID = 'client-email-only';
const EMAIL = 'cliente@example.com';
const CODE = '482901';
const SALT = 'salt-for-test';
const NOW = 1_800_000_000_000;

const buildManualCode = ({ phone = null, email = EMAIL, hashVersion = CLIENT_MANUAL_VERIFICATION_HASH_VERSION } = {}) => ({
  codeHash: hashClientManualVerificationCode({
    clientId: CLIENT_ID,
    phone: phone || '',
    email: email || '',
    code: CODE,
    salt: SALT,
    hashVersion,
  }),
  salt: SALT,
  phone,
  email,
  hashVersion,
  expiresAt: NOW + 60_000,
  attempts: 0,
  maxAttempts: 5,
});

test('normaliza somente e-mail apto para receber verificação', () => {
  assert.equal(normalizeVerificationEmail(' Cliente@Example.COM '), EMAIL);
  assert.equal(normalizeVerificationEmail('cliente-sem-dominio'), null);
  assert.equal(normalizeVerificationEmail(''), null);
});

test('confirma cliente usando somente o código enviado ao e-mail', () => {
  const result = validateClientManualVerificationCode({
    clientId: CLIENT_ID,
    email: EMAIL,
    code: CODE,
    manualCode: buildManualCode(),
    now: NOW,
  });

  assert.deepEqual(result, {
    ok: true,
    verifiedPhone: null,
    verifiedEmail: EMAIL,
    verificationChannel: 'email',
  });
});

test('não permite trocar o e-mail vinculado ao código pendente', () => {
  const result = validateClientManualVerificationCode({
    clientId: CLIENT_ID,
    email: 'outro@example.com',
    code: CODE,
    manualCode: buildManualCode(),
    now: NOW,
  });

  assert.deepEqual(result, {
    ok: false,
    status: 409,
    error: 'verification_email_mismatch',
  });
});

test('preserva validação de códigos legados vinculados ao telefone', () => {
  const phone = '+5565999999999';
  const manualCode = buildManualCode({
    phone,
    email: null,
    hashVersion: 1,
  });
  delete manualCode.hashVersion;

  const result = validateClientManualVerificationCode({
    clientId: CLIENT_ID,
    phone,
    code: CODE,
    manualCode,
    now: NOW,
  });

  assert.equal(result.ok, true);
  assert.equal(result.verifiedPhone, phone);
  assert.equal(result.verificationChannel, 'phone');
});

test('rota de confirmação não exige telefone quando há código manual por e-mail', () => {
  const source = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  const route = source.slice(
    source.indexOf("app.post('/api/client-context/verification/confirm-manual'"),
    source.indexOf("app.post('/api/client-context/verification/mark-mismatch'")
  );

  assert.match(route, /const verifiedEmail = normalizeVerificationEmail/);
  assert.match(route, /\(!verificationIdToken && !verificationCode\)/);
  assert.match(route, /\(verificationIdToken && !verifiedPhone\)/);
  assert.match(route, /verificationMethod = 'manual_email_code'/);
  assert.match(route, /verifiedEmail: verificationChannel === 'email' \? confirmedEmail : null/);
});
