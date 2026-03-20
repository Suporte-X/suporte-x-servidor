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
  constructor() {
    this.name = 'fake-bucket';
    this.saved = new Map();
  }

  file(objectPath) {
    return {
      save: async (buffer, options = {}) => {
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

function buildHarness() {
  const db = new FakeFirestore({
    'sessions/s-tech-client': {
      clientUid: 'client-uid-1',
      techUid: 'tech-uid-1',
      status: 'active',
    },
  });
  const bucket = new FakeBucket();
  const auth = {
    async verifyIdToken(token) {
      if (token === 'token-client') return { uid: 'client-uid-1' };
      if (token === 'token-tech') return { uid: 'tech-uid-1' };
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
    logger: { error: () => {} },
  }));

  return { app, db, bucket };
}

async function postFile(app, route, {
  token,
  sessionId,
  messageId = 'msg-1',
  filename = 'sample.bin',
  contentType = 'application/octet-stream',
  buffer = Buffer.from('hello-world'),
} = {}) {
  let req = request(app)
    .post(`/api/upload/${route}`)
    .field('messageId', messageId)
    .attach('file', buffer, { filename, contentType });

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
    buffer: Buffer.from('image-content-tech'),
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
    buffer: Buffer.from('image-content-client'),
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
    buffer: Buffer.from('audio-webm-from-web'),
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
    buffer: Buffer.from('audio-m4a-from-app'),
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

test('upload de avatar usa endpoint backend e grava em techs/{uid}', async () => {
  const { app, db } = buildHarness();
  const res = await postFile(app, 'avatar', {
    token: 'token-tech',
    filename: 'avatar.webp',
    contentType: 'image/webp',
    buffer: Buffer.from('avatar-webp'),
  });

  assert.equal(res.status, 200);
  assert.equal(res.body.upload.kind, 'avatar');
  const techDoc = db.docs.get('techs/tech-uid-1');
  assert.ok(techDoc);
  assert.match(String(techDoc.customPhotoURL || ''), /^https:\/\/firebasestorage\.googleapis\.com\//);
});
