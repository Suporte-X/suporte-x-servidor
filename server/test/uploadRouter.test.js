const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const request = require('supertest');

const { createUploadRouter, LIMITS } = require('../uploadRouter');

class FakeFirestore {
  constructor(seedDocs = {}) {
    this.docs = new Map(Object.entries(seedDocs));
  }

  collection(name) {
    return new FakeCollectionRef(this.docs, [name]);
  }
}

class FakeCollectionRef {
  constructor(store, segments) {
    this.store = store;
    this.segments = segments;
  }

  doc(id) {
    return new FakeDocRef(this.store, [...this.segments, id]);
  }
}

class FakeDocRef {
  constructor(store, segments) {
    this.store = store;
    this.segments = segments;
  }

  get key() {
    return this.segments.join('/');
  }

  async get() {
    const value = this.store.get(this.key);
    return {
      exists: value !== undefined,
      data: () => (value === undefined ? undefined : deepClone(value)),
    };
  }

  async set(payload, options = {}) {
    if (options && options.merge === true) {
      const current = this.store.get(this.key);
      const currentObj = current && typeof current === 'object' ? current : {};
      this.store.set(this.key, { ...deepClone(currentObj), ...deepClone(payload) });
      return;
    }
    this.store.set(this.key, deepClone(payload));
  }

  collection(name) {
    return new FakeCollectionRef(this.store, [...this.segments, name]);
  }
}

class FakeBucket {
  constructor({ saveError = null } = {}) {
    this.name = 'fake-bucket';
    this.saved = new Map();
    this.saveError = saveError;
  }

  file(objectPath) {
    return {
      save: async (buffer, options = {}) => {
        if (this.saveError) throw this.saveError;
        this.saved.set(objectPath, {
          size: buffer.length,
          metadata: deepClone(options.metadata || {}),
        });
      },
    };
  }
}

function deepClone(value) {
  return JSON.parse(JSON.stringify(value));
}

function sampleMediaBuffer(filename = '') {
  const extension = String(filename).split('.').pop()?.toLowerCase();
  const suffix = Buffer.from('test-payload');
  const signatures = {
    jpg: Buffer.from([0xff, 0xd8, 0xff, 0xe0]),
    jpeg: Buffer.from([0xff, 0xd8, 0xff, 0xe0]),
    png: Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    gif: Buffer.from('GIF89a', 'ascii'),
    webp: Buffer.from('RIFF0000WEBP', 'ascii'),
    bmp: Buffer.from('BM', 'ascii'),
    heic: Buffer.concat([Buffer.from([0, 0, 0, 24]), Buffer.from('ftypheic', 'ascii')]),
    heif: Buffer.concat([Buffer.from([0, 0, 0, 24]), Buffer.from('ftypmif1', 'ascii')]),
    webm: Buffer.from([0x1a, 0x45, 0xdf, 0xa3]),
    m4a: Buffer.concat([Buffer.from([0, 0, 0, 24]), Buffer.from('ftypM4A ', 'ascii')]),
    aac: Buffer.from([0xff, 0xf1]),
    mp3: Buffer.from('ID3', 'ascii'),
    ogg: Buffer.from('OggS', 'ascii'),
    wav: Buffer.from('RIFF0000WAVE', 'ascii'),
  };
  return Buffer.concat([signatures[extension] || suffix, suffix]);
}

function buildHarness({
  bucket = new FakeBucket(),
  logger = { error: () => {} },
} = {}) {
  const db = new FakeFirestore({
    'sessions/s-tech-client': {
      clientUid: 'client-uid-1',
      techUid: 'tech-uid-1',
      status: 'active',
    },
    'techs/tech-uid-1': {
      active: true,
      name: 'Tecnico de teste',
    },
  });
  const auth = {
    async verifyIdToken(token) {
      if (token === 'token-client') return { uid: 'client-uid-1' };
      if (token === 'token-tech') return { uid: 'tech-uid-1', role: 'tech' };
      if (token === 'token-tech-without-claim') return { uid: 'tech-uid-1' };
      if (token === 'token-outsider') return { uid: 'outsider-uid-1' };
      throw new Error('invalid token');
    },
  };

  const app = express();
  app.use('/api/upload', createUploadRouter({
    auth,
    db,
    bucket,
    clock: () => 1710000000000,
    logger,
  }));

  return { app, db, bucket };
}

async function postFile(app, route, {
  token,
  sessionId,
  messageId = 'msg-1',
  filename = 'sample.bin',
  contentType = 'application/octet-stream',
  buffer = null,
} = {}) {
  let req = request(app)
    .post(`/api/upload/${route}`)
    .field('messageId', messageId)
    .attach('file', buffer || sampleMediaBuffer(filename), { filename, contentType });

  if (sessionId) {
    req = req.field('sessionId', sessionId);
  }
  if (token) {
    req = req.set('Authorization', `Bearer ${token}`);
  }
  return req;
}

test('fluxo web->app imagem (tech) deve subir com sucesso', async () => {
  const { app, db } = buildHarness();
  const res = await postFile(app, 'session-attachment', {
    token: 'token-tech',
    sessionId: 's-tech-client',
    filename: 'painel.png',
    contentType: 'image/png',
  });

  assert.equal(res.status, 200);
  assert.equal(res.body.ok, true);
  assert.equal(res.body.upload.uploadedByRole, 'tech');
  assert.match(res.body.upload.path, /^sessions\/s-tech-client\/attachments\//);

  const uploads = [...db.docs.keys()].filter((key) => key.startsWith('sessions/s-tech-client/uploads/'));
  assert.equal(uploads.length, 1);
});

test('fluxo app->web imagem (client) deve subir com sucesso', async () => {
  const { app } = buildHarness();
  const res = await postFile(app, 'session-attachment', {
    token: 'token-client',
    sessionId: 's-tech-client',
    filename: 'app-camera.jpg',
    contentType: 'image/jpeg',
  });

  assert.equal(res.status, 200);
  assert.equal(res.body.upload.uploadedByRole, 'client');
  assert.equal(res.body.upload.contentType, 'image/jpeg');
});

test('fluxo web->app audio (tech webm) deve subir com sucesso', async () => {
  const { app } = buildHarness();
  const res = await postFile(app, 'session-audio', {
    token: 'token-tech',
    sessionId: 's-tech-client',
    filename: 'gravacao.webm',
    contentType: 'video/webm',
  });

  assert.equal(res.status, 200);
  assert.equal(res.body.upload.kind, 'audio');
  assert.equal(res.body.upload.contentType, 'video/webm');
});

test('fluxo app->web audio (client m4a) deve subir com sucesso', async () => {
  const { app } = buildHarness();
  const res = await postFile(app, 'session-audio', {
    token: 'token-client',
    sessionId: 's-tech-client',
    filename: 'gravacao.m4a',
    contentType: 'audio/mp4',
  });

  assert.equal(res.status, 200);
  assert.equal(res.body.upload.kind, 'audio');
  assert.equal(res.body.upload.contentType, 'audio/mp4');
});

test('upload sem token retorna 401', async () => {
  const { app } = buildHarness();
  const res = await postFile(app, 'session-attachment', {
    sessionId: 's-tech-client',
    filename: 'no-token.png',
    contentType: 'image/png',
  });

  assert.equal(res.status, 401);
  assert.equal(res.body.error, 'missing_auth_token');
});

test('upload fora da sessao retorna 403', async () => {
  const { app } = buildHarness();
  const res = await postFile(app, 'session-attachment', {
    token: 'token-outsider',
    sessionId: 's-tech-client',
    filename: 'outsider.png',
    contentType: 'image/png',
  });

  assert.equal(res.status, 403);
  assert.equal(res.body.error, 'not_session_member');
});

test('mime invalido retorna 400', async () => {
  const { app } = buildHarness();
  const res = await postFile(app, 'session-attachment', {
    token: 'token-tech',
    sessionId: 's-tech-client',
    filename: 'arquivo.pdf',
    contentType: 'application/pdf',
  });

  assert.equal(res.status, 400);
  assert.equal(res.body.error, 'invalid_mime_type');
});

test('imagem SVG ativa e bloqueada mesmo com MIME de imagem', async () => {
  const { app } = buildHarness();
  const res = await postFile(app, 'session-attachment', {
    token: 'token-tech',
    sessionId: 's-tech-client',
    filename: 'conteudo.svg',
    contentType: 'image/svg+xml',
    buffer: Buffer.from('<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>'),
  });

  assert.equal(res.status, 400);
  assert.equal(res.body.error, 'invalid_mime_type');
});

test('conteudo ativo disfarçado de PNG e bloqueado pela assinatura real', async () => {
  const { app, bucket } = buildHarness();
  const res = await postFile(app, 'session-attachment', {
    token: 'token-tech',
    sessionId: 's-tech-client',
    filename: 'conteudo.png',
    contentType: 'image/png',
    buffer: Buffer.from('<html><script>alert(1)</script></html>'),
  });

  assert.equal(res.status, 400);
  assert.equal(res.body.error, 'invalid_file_signature');
  assert.equal(bucket.saved.size, 0);
});

test('octet-stream valido e normalizado pelo conteúdo e extensão', async () => {
  const { app } = buildHarness();
  const res = await postFile(app, 'session-attachment', {
    token: 'token-client',
    sessionId: 's-tech-client',
    filename: 'captura.png',
    contentType: 'application/octet-stream',
  });

  assert.equal(res.status, 200);
  assert.equal(res.body.upload.contentType, 'image/png');
});

test('playlist declarada como audio e bloqueada', async () => {
  const { app } = buildHarness();
  const res = await postFile(app, 'session-audio', {
    token: 'token-tech',
    sessionId: 's-tech-client',
    filename: 'playlist.m3u8',
    contentType: 'audio/x-mpegurl',
    buffer: Buffer.from('#EXTM3U\nhttps://example.invalid/audio.mp3'),
  });

  assert.equal(res.status, 400);
  assert.equal(res.body.error, 'invalid_mime_type');
});

test('arquivo acima do limite retorna 400', async () => {
  const { app } = buildHarness();
  const tooBig = Buffer.alloc(LIMITS.attachmentBytes + 1, 0x01);
  const res = await postFile(app, 'session-attachment', {
    token: 'token-tech',
    sessionId: 's-tech-client',
    filename: 'large.png',
    contentType: 'image/png',
    buffer: tooBig,
  });

  assert.equal(res.status, 400);
  assert.equal(res.body.error, 'file_too_large');
});

test('sessao inexistente retorna 404', async () => {
  const { app } = buildHarness();
  const res = await postFile(app, 'session-attachment', {
    token: 'token-tech',
    sessionId: 'missing-session',
    filename: 'missing.png',
    contentType: 'image/png',
  });

  assert.equal(res.status, 404);
  assert.equal(res.body.error, 'session_not_found');
});

test('sessao encerrada nao aceita novos uploads', async () => {
  const { app, db } = buildHarness();
  db.docs.set('sessions/s-tech-client', {
    clientUid: 'client-uid-1',
    techUid: 'tech-uid-1',
    status: 'closed',
  });
  const res = await postFile(app, 'session-attachment', {
    token: 'token-client',
    sessionId: 's-tech-client',
    filename: 'late.png',
    contentType: 'image/png',
  });

  assert.equal(res.status, 409);
  assert.equal(res.body.error, 'session_not_active');
});

test('upload de avatar usa endpoint backend e grava em techs/{uid}', async () => {
  const { app, db } = buildHarness();
  const res = await postFile(app, 'avatar', {
    token: 'token-tech',
    filename: 'avatar.webp',
    contentType: 'image/webp',
  });

  assert.equal(res.status, 200);
  assert.equal(res.body.upload.kind, 'avatar');
  const techDoc = db.docs.get('techs/tech-uid-1');
  assert.ok(techDoc);
  assert.match(String(techDoc.customPhotoURL || ''), /^https:\/\/firebasestorage\.googleapis\.com\//);
});

test('cliente anonimo nao pode criar perfil tecnico por upload de avatar', async () => {
  const { app, db } = buildHarness();
  const res = await postFile(app, 'avatar', {
    token: 'token-client',
    filename: 'avatar.webp',
    contentType: 'image/webp',
    buffer: Buffer.from('fake-webp-content'),
  });

  assert.equal(res.status, 403);
  assert.equal(res.body.error, 'insufficient_role');
  assert.equal(db.docs.has('techs/client-uid-1'), false);
});

test('documento técnico ativo não substitui claim ausente no upload de avatar', async () => {
  const { app } = buildHarness();
  const res = await postFile(app, 'avatar', {
    token: 'token-tech-without-claim',
    filename: 'avatar.webp',
    contentType: 'image/webp',
  });

  assert.equal(res.status, 403);
  assert.equal(res.body.error, 'insufficient_role');
});

test('falha interna de upload nao expoe detalhes do provedor ao cliente ou ao log', async () => {
  const logEntries = [];
  const { app } = buildHarness({
    bucket: new FakeBucket({
      saveError: new Error('super-secret-storage-detail'),
    }),
    logger: {
      error: (...args) => logEntries.push(args),
    },
  });

  const res = await postFile(app, 'session-attachment', {
    token: 'token-client',
    sessionId: 's-tech-client',
    filename: 'evidencia.png',
    contentType: 'image/png',
  });

  assert.equal(res.status, 500);
  assert.deepEqual(res.body, {
    error: 'upload_failed',
    message: 'Falha interna no upload.',
  });
  assert.equal(JSON.stringify(logEntries).includes('super-secret-storage-detail'), false);
});
