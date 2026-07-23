'use strict';

const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const express = require('express');
const request = require('supertest');
const {
  SEND_INLINE_SCRIPT_HASHES,
  buildContentSecurityPolicy,
  createWebSecurityHeadersMiddleware,
  isAuthorizedTechProfilePhotoUrl,
} = require('../securityPolicy');

const publicDir = path.join(__dirname, '..', '..', 'web', 'public');
const readPublicFile = (fileName) =>
  fs.readFileSync(path.join(publicDir, fileName), 'utf8');

test('retorno do login técnico permanece restrito à mesma origem', () => {
  const source = readPublicFile('tech-login.js');

  assert.match(source, /const parsed = new URL\(raw, window\.location\.origin\)/);
  assert.match(source, /parsed\.origin !== window\.location\.origin/);
  assert.match(source, /const nextPath = resolveSafeNextPath\(params\.get\('next'\)\)/);
  assert.equal(
    source.includes("const nextPath = params.get('next') || '/central.html'"),
    false
  );
});

test('anexos e imagens do painel rejeitam esquemas executáveis', () => {
  const source = readPublicFile('central.js');
  const urlGuard = source.slice(
    source.indexOf('const parseHttpUrl ='),
    source.indexOf('const buildAvatarMarkup =')
  );

  assert.match(urlGuard, /\['https:', 'http:'\]\.includes\(parsed\.protocol\)/);
  assert.match(urlGuard, /parsed\.origin === window\.location\.origin/);
  assert.match(urlGuard, /isAllowedFirebaseStorageUrl\(parsed\)/);
  assert.match(urlGuard, /hostname\.endsWith\('\.googleusercontent\.com'\)/);
  assert.match(urlGuard, /hostname\.endsWith\('\.fbcdn\.net'\)/);
  assert.equal(urlGuard.includes("parsed.protocol === 'javascript:'"), false);
  assert.match(source, /link\.href = normalizedFileUrl/);
  assert.equal(source.includes('link.href = fileUrl'), false);
  assert.match(source, /const normalized = safeResourceUrl\(value\)/);
});

test('service worker não pré-cacheia o código mutável do painel', () => {
  const source = readPublicFile('service-worker.js');
  const precache = source.slice(
    source.indexOf('const PRECACHE_URLS'),
    source.indexOf('const shouldBypassCache')
  );

  assert.equal(precache.includes("'/central.js'"), false);
  assert.equal(precache.includes("'/common.js'"), false);
  assert.equal(precache.includes("'/central.css'"), false);
  assert.match(source, /\\\.\(\?:html\|js\|css\)\$/);
});

test('tela legada usa o cliente Socket.IO servido pela própria aplicação', () => {
  const source = readPublicFile('send.html');

  assert.match(source, /src="\/socket\.io\/socket\.io\.js"/);
  assert.equal(source.includes('https://cdn.socket.io/'), false);
});

test('foto técnica só aceita o upload autorizado no bucket e caminho do próprio UID', () => {
  const storagePath = 'chat/avatars/tech-1/123-avatar.webp';
  const photoUrl =
    'https://firebasestorage.googleapis.com/v0/b/suporte-x-19ae8.firebasestorage.app/o/' +
    `${encodeURIComponent(storagePath)}?alt=media&token=token-seguro`;
  const base = {
    photoUrl,
    authorizedPhotoUrl: photoUrl,
    storagePath,
    uid: 'tech-1',
    bucketName: 'suporte-x-19ae8.firebasestorage.app',
  };

  assert.equal(isAuthorizedTechProfilePhotoUrl(base), true);
  assert.equal(
    isAuthorizedTechProfilePhotoUrl({
      ...base,
      photoUrl: 'https://example.test/rastreamento.png',
    }),
    false
  );
  assert.equal(
    isAuthorizedTechProfilePhotoUrl({
      ...base,
      storagePath: 'chat/avatars/outro-uid/avatar.webp',
    }),
    false
  );
  assert.equal(
    isAuthorizedTechProfilePhotoUrl({
      ...base,
      authorizedPhotoUrl: `${photoUrl}&versao=outra`,
    }),
    false
  );
});

test('rota de foto técnica exige a URL previamente autorizada pelo upload', () => {
  const source = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  const route = source.slice(
    source.indexOf("app.post('/api/tech/profile-photo'"),
    source.indexOf("app.post('/api/auth/turnstile/verify'")
  );

  assert.match(route, /isAuthorizedTechProfilePhotoUrl/);
  assert.match(route, /techDoc\.customPhotoURL/);
  assert.match(route, /techDoc\.avatarPath/);
  assert.match(route, /error: 'invalid_photo_url'/);
});

test('CSP permite somente origens necessárias e hashes dos scripts inline atuais', () => {
  const policy = buildContentSecurityPolicy({ isProduction: true });
  const sendHtml = readPublicFile('send.html');
  const inlineHashes = [...sendHtml.matchAll(/<script>([\s\S]*?)<\/script>/g)]
    .map((match) =>
      `'sha256-${crypto.createHash('sha256').update(match[1], 'utf8').digest('base64')}'`
    );

  assert.deepEqual(inlineHashes, [...SEND_INLINE_SCRIPT_HASHES]);
  assert.match(policy, /default-src 'self'/);
  assert.match(policy, /object-src 'none'/);
  assert.match(policy, /frame-ancestors 'none'/);
  assert.match(policy, /script-src [^;]*https:\/\/www\.gstatic\.com/);
  assert.match(policy, /script-src [^;]*https:\/\/challenges\.cloudflare\.com/);
  assert.match(policy, /connect-src [^;]*https:\/\/\*\.googleapis\.com/);
  assert.match(policy, /img-src [^;]*https:\/\/firebasestorage\.googleapis\.com/);
  assert.equal(/script-src [^;]*'unsafe-inline'/.test(policy), false);
  assert.match(policy, /upgrade-insecure-requests/);
});

test('middleware entrega CSP e impede cache de rotas privadas', async () => {
  const app = express();
  app.use(createWebSecurityHeadersMiddleware({ isProduction: true }));
  app.get('/central.html', (_req, res) => res.send('ok'));
  app.get('/api/private', (_req, res) => res.json({ ok: true }));

  const page = await request(app).get('/central.html');
  assert.equal(page.status, 200);
  assert.match(page.headers['content-security-policy'], /frame-ancestors 'none'/);

  const api = await request(app).get('/api/private');
  assert.equal(api.status, 200);
  assert.equal(api.headers['cache-control'], 'private, no-store, max-age=0');
  assert.equal(api.headers.pragma, 'no-cache');
  assert.match(api.headers['content-security-policy'], /default-src 'self'/);
});
