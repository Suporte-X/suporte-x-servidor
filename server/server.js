const path = require('path');
const express = require('express');
const http = require('http');
const cors = require('cors');
const { Server } = require('socket.io');
const { customAlphabet } = require('nanoid');
const { db, firebaseProjectId } = require('./firebase');
const admin = require('firebase-admin');
const { requireAuth, normalizeRole } = require('./auth');

const ensureString = (value, fallback = '') => {
  if (typeof value === 'string') return value.slice(0, 256);
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value).slice(0, 256);
  }
  return fallback;
};

const parseJsonObject = (value) => {
  if (!value) return null;
  try {
    const parsed = JSON.parse(value);
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return parsed;
    }
  } catch (err) {
    console.warn('Failed to parse JSON config', err);
  }
  return null;
};

const normalizeSessionId = (value) => {
  if (typeof value !== 'string') return '';
  return value.trim().slice(0, 64);
};

const respondAck = (ack, payload) => {
  if (typeof ack === 'function') {
    ack(payload);
  }
};

let firebaseClientConfigCache = undefined;
const DEFAULT_CENTRAL_FIREBASE_CONFIG = {
  apiKey: 'AIzaSyAooFHhk6ewqKPkXVX48CCWVVoV0eOUesI',
  authDomain: 'suporte-x-19ae8.firebaseapp.com',
  projectId: 'suporte-x-19ae8',
  storageBucket: 'suporte-x-19ae8.firebasestorage.app',
  messagingSenderId: '603259295557',
  appId: '1:603259295557:web:00ca6e9fe02ff5fbe0902c',
  measurementId: 'G-KF1CQYGZVF',
};

const resolveFirebaseClientConfig = () => {
  if (firebaseClientConfigCache !== undefined) {
    return firebaseClientConfigCache;
  }

  const jsonSources = [
    ensureString(process.env.CENTRAL_FIREBASE_CONFIG || '', ''),
    ensureString(process.env.FIREBASE_CLIENT_CONFIG || '', ''),
  ].filter(Boolean);

  const base64Sources = [
    ensureString(
      process.env.CENTRAL_FIREBASE_CONFIG_BASE64 || process.env.CENTRAL_FIREBASE_CONFIG_B64 || '',
      ''
    ),
    ensureString(
      process.env.FIREBASE_CLIENT_CONFIG_BASE64 || process.env.FIREBASE_CLIENT_CONFIG_B64 || '',
      ''
    ),
  ].filter(Boolean);

  base64Sources.forEach((encoded) => {
    try {
      const decoded = Buffer.from(encoded, 'base64').toString('utf8');
      jsonSources.push(decoded);
    } catch (err) {
      console.warn('Failed to decode base64 Firebase client config', err);
    }
  });

  for (const source of jsonSources) {
    const parsed = parseJsonObject(source);
    if (parsed) {
      firebaseClientConfigCache = parsed;
      return firebaseClientConfigCache;
    }
  }

  const fieldMap = {
    apiKey: ['CENTRAL_FIREBASE_API_KEY', 'FIREBASE_API_KEY'],
    authDomain: ['CENTRAL_FIREBASE_AUTH_DOMAIN'],
    projectId: ['CENTRAL_FIREBASE_PROJECT_ID'],
    storageBucket: ['CENTRAL_FIREBASE_STORAGE_BUCKET'],
    messagingSenderId: ['CENTRAL_FIREBASE_MESSAGING_SENDER_ID'],
    appId: ['CENTRAL_FIREBASE_APP_ID'],
    measurementId: ['CENTRAL_FIREBASE_MEASUREMENT_ID'],
    databaseURL: ['CENTRAL_FIREBASE_DATABASE_URL'],
  };

  const config = {};
  Object.entries(fieldMap).forEach(([field, envKeys]) => {
    for (const envKey of envKeys) {
      const value = ensureString(process.env[envKey] || '', '');
      if (value) {
        config[field] = value;
        break;
      }
    }
  });

  if (!config.projectId && firebaseProjectId) {
    config.projectId = firebaseProjectId;
  }

  firebaseClientConfigCache = Object.keys(config).length ? config : DEFAULT_CENTRAL_FIREBASE_CONFIG;
  return firebaseClientConfigCache;
};

const runFirestoreHealthProbe = async () => {
  if (!db) {
    console.warn('Skipping Firestore health probe because Firestore is not configured.');
    return;
  }

  try {
    await db.collection('meta').limit(1).get();
    console.log('Firestore OK');
  } catch (err) {
    console.error('Firestore health probe failed', err);
  }
};

runFirestoreHealthProbe();

const getSessionsCollection = () => {
  if (!db) return null;
  try {
    return db.collection('sessions');
  } catch (err) {
    console.error('Failed to access sessions collection', err);
    return null;
  }
};

const getRequestsCollection = () => {
  if (!db) return null;
  try {
    return db.collection('requests');
  } catch (err) {
    console.error('Failed to access requests collection', err);
    return null;
  }
};

const isFirestoreReady = () => Boolean(getSessionsCollection() && getRequestsCollection());

// ===== Básico
const app = express();
const server = http.createServer(app);
const isProduction = process.env.NODE_ENV === 'production';
const productionOrigins = ['https://suportex.app', 'https://www.suportex.app'];
const corsOptions = isProduction
  ? { origin: productionOrigins, credentials: true }
  : { origin: true, credentials: true };
const io = new Server(server, {
  cors: isProduction
    ? {
        origin: productionOrigins,
        methods: ['GET', 'POST'],
        credentials: true,
      }
    : { origin: '*', methods: ['GET', 'POST'], credentials: true },
  allowEIO3: true, // compat com socket.io-client 2.x (Android)
  pingInterval: 25000,
  pingTimeout: 20000,
});
const PORT = process.env.PORT || 3000;
const WEB_STATIC_PATH = path.resolve(__dirname, '../web/public');

app.use(cors(corsOptions));

const CANONICAL_HOST = 'suportex.app';
app.use((req, res, next) => {
  if (!isProduction) return next();
  const host = req.headers.host;
  if (!host) return next();
  const isLocal = host.startsWith('localhost') || host.startsWith('127.0.0.1');
  if (isLocal || host === CANONICAL_HOST) return next();

  const target = `https://${CANONICAL_HOST}${req.originalUrl}`;
  return res.redirect(301, target);
});

// ===== Anti-cache seletivo (HTML/JS/CSS)
app.use(express.json());
app.use(express.static(WEB_STATIC_PATH, {
  setHeaders: (res, filePath) => {
    const lower = filePath.toLowerCase();
    if (lower.endsWith('.html') || lower.endsWith('.js') || lower.endsWith('.css')) {
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
      res.setHeader('Pragma', 'no-cache');
      res.setHeader('Expires', '0');
      res.removeHeader('ETag');
      res.removeHeader('Last-Modified');
    }
  }
}));

app.get('/central-config.js', (_req, res) => {
  const config = resolveFirebaseClientConfig();
  res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');

  const serialized = config ? JSON.stringify(config) : 'null';
  const safeSerialized = serialized.replace(/</g, '\\u003C');
  const script = `(() => {
    const target = (window.__CENTRAL_CONFIG__ = window.__CENTRAL_CONFIG__ || {});
    if (!target.firebase) {
      target.firebase = ${safeSerialized};
    }
    if (!target.firebase) {
      console.warn('Firebase client config not configured for central.');
    }
  })();`;

  res.send(script);
});

app.get('/healthz', async (_req, res) => {
  if (!db) {
    return res.status(503).json({ ok: false, error: 'firestore_unavailable' });
  }

  try {
    await db.collection('meta').limit(1).get();
    res.json({ ok: true });
  } catch (err) {
    console.error('Firestore health check failed', err);
    res.status(503).json({ ok: false, error: 'firestore_unavailable' });
  }
});

// ===== Estado
const nanoid = customAlphabet('ABCDEFGHJKLMNPQRSTUVWXYZ23456789', 6);

// ====== SOCKETS
const connectionIndex = new Map();


io.use(async (socket, next) => {
  try {
    const auth = socket.handshake?.auth || {};
    const token = ensureString(auth.token || '', '').trim();
    const requiresAuth = auth.requireAuth === true || auth.panel === 'tech';

    if (!token) {
      if (requiresAuth) return next(new Error('missing_token'));
      return next();
    }

    const decoded = await admin.auth().verifyIdToken(token);
    socket.user = decoded;
    return next();
  } catch (_error) {
    return next(new Error('invalid_token'));
  }
});


const getRequestById = async (requestId) => {
  const requestsCollection = getRequestsCollection();
  if (!requestsCollection) return null;
  const snapshot = await requestsCollection.doc(requestId).get();
  if (!snapshot.exists) return null;
  return { requestId: snapshot.id, ...snapshot.data() };
};

const getSessionSnapshot = async (sessionId) => {
  const sessionsCollection = getSessionsCollection();
  if (!sessionsCollection) return null;
  const normalized = normalizeSessionId(sessionId);
  if (!normalized) return null;
  const snapshot = await sessionsCollection.doc(normalized).get();
  if (!snapshot.exists) return null;
  return snapshot;
};

const fetchMessages = async (sessionRef, limit = 50) => {
  if (!sessionRef) return [];
  const snapshot = await sessionRef.collection('messages').orderBy('ts', 'desc').limit(limit).get();
  const messages = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
  messages.sort((a, b) => (a.ts || 0) - (b.ts || 0));
  return messages;
};

const fetchEvents = async (sessionRef, limit = 100) => {
  if (!sessionRef) return [];
  const snapshot = await sessionRef.collection('events').orderBy('ts', 'desc').limit(limit).get();
  const events = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
  events.sort((a, b) => (a.ts || 0) - (b.ts || 0));
  return events;
};

const buildSessionState = async (sessionId, { includeLogs = true, snapshot: providedSnapshot = null } = {}) => {
  const snapshot = providedSnapshot || (await getSessionSnapshot(sessionId));
  if (!snapshot) return null;

  const data = snapshot.data() || {};
  const base = {
    sessionId: snapshot.id,
    requestId: data.requestId || null,
    techId: data.techId || null,
    techUid: data.techUid || null,
    techName: data.techName || null,
    clientId: data.clientId || null,
    clientUid: data.clientUid || null,
    clientName: data.clientName || null,
    brand: data.brand || null,
    model: data.model || null,
    osVersion: data.osVersion || null,
    plan: data.plan || null,
    issue: data.issue || null,
    requestedAt: data.requestedAt || null,
    acceptedAt: data.acceptedAt || null,
    waitTimeMs: data.waitTimeMs || null,
    status: data.status || 'active',
    closedAt: data.closedAt || null,
    handleTimeMs: data.handleTimeMs || null,
    firstContactResolution:
      typeof data.firstContactResolution === 'boolean' ? data.firstContactResolution : null,
    npsScore: typeof data.npsScore === 'number' ? data.npsScore : null,
    outcome: data.outcome || null,
    symptom: data.symptom || null,
    solution: data.solution || null,
    notes: data.notes || null,
    telemetry: typeof data.telemetry === 'object' && data.telemetry !== null ? data.telemetry : {},
    extra: typeof data.extra === 'object' && data.extra !== null ? { ...data.extra } : {},
  };

  if (includeLogs) {
    const [messages, events] = await Promise.all([
      fetchMessages(snapshot.ref),
      fetchEvents(snapshot.ref),
    ]);
    const commandLog = events.filter((event) => event.kind === 'command');

    base.chatLog = messages;
    base.commandLog = commandLog;
    base.events = events;

    base.extra = {
      ...base.extra,
      chatLog: messages,
      commandLog,
      telemetry: base.telemetry,
    };

    if (messages.length) {
      base.extra.lastMessageAt = messages[messages.length - 1].ts || null;
    }
    if (commandLog.length) {
      base.extra.lastCommand = commandLog[commandLog.length - 1] || null;
    }
  } else {
    base.chatLog = [];
    base.commandLog = [];
  }

  if (typeof base.telemetry === 'object' && base.telemetry !== null) {
    const telemetry = base.telemetry;
    if (typeof telemetry.network !== 'undefined') base.extra.network = telemetry.network;
    if (typeof telemetry.health !== 'undefined') base.extra.health = telemetry.health;
    if (typeof telemetry.permissions !== 'undefined') base.extra.permissions = telemetry.permissions;
    if (typeof telemetry.alerts !== 'undefined') base.extra.alerts = telemetry.alerts;
  }

  return base;
};

const emitSessionUpdated = async (sessionId, options = {}) => {
  try {
    const session = await buildSessionState(sessionId, options);
    if (session) {
      io.emit('session:updated', session);
    }
  } catch (err) {
    console.error('Failed to emit session update', err);
  }
};

const normalizeEventType = (type) => {
  switch (type) {
    case 'remote_disable':
      return 'remote_revoke';
    case 'remote_enable':
      return 'remote_grant';
    case 'session_end':
      return 'end';
    default:
      return type;
  }
};

const toMillis = (value, fallback = null) => {
  if (value === undefined || value === null) return fallback;
  const num = Number(value);
  if (Number.isFinite(num)) return num;
  return fallback;
};


const buildTechAccessPayload = async (decoded) => {
  const uid = ensureString(decoded?.uid || '', '');
  if (!uid) {
    return { ok: false, status: 401, error: 'invalid_token' };
  }

  const techRef = db.collection('techs').doc(uid);
  const techSnap = await techRef.get();
  const techDoc = techSnap.exists ? techSnap.data() || {} : null;
  const roleClaim = normalizeRole(decoded?.role);
  const isActiveTech = Boolean(techDoc && techDoc.active === true);

  if (roleClaim !== 'tech' || !isActiveTech) {
    return {
      ok: false,
      status: 403,
      error: 'not_tech',
      payload: {
        uid,
        email: ensureString(decoded?.email || '', '') || null,
        name: ensureString(decoded?.name || '', '') || null,
        photoURL: ensureString(decoded?.picture || '', '') || null,
        roleClaim,
        techDoc,
      },
    };
  }

  return {
    ok: true,
    payload: {
      uid,
      email: ensureString(decoded?.email || '', '') || null,
      name: ensureString(decoded?.name || '', '') || null,
      photoURL: ensureString(decoded?.picture || '', '') || null,
      roleClaim,
      techDoc,
    },
  };
};

const requireTechAccess = async (req, res, next) => {
  try {
    if (!db) {
      return res.status(503).json({ error: 'firestore_unavailable' });
    }

    const access = await buildTechAccessPayload(req.user || {});
    if (!access.ok) {
      return res.status(access.status || 403).json({ error: access.error || 'not_tech' });
    }

    req.techAccess = access.payload;
    return next();
  } catch (error) {
    console.error('Failed to validate technical access', error);
    return res.status(500).json({ error: 'server_error' });
  }
};

io.on('connection', (socket) => {
  connectionIndex.set(socket.id, { socketId: socket.id, userType: 'unknown', sessionId: null });

  // 1) CLIENTE cria um pedido de suporte (fila real)
  // payload: { clientName?, brand?, model? }
  socket.on('support:request', async (payload = {}) => {
    const requestsCollection = getRequestsCollection();
    if (!requestsCollection) {
      console.error('Firestore not configured. Cannot enqueue support request.');
      socket.emit('support:error', { error: 'firestore_unavailable' });
      return;
    }

    const requestId = nanoid().toUpperCase();
    const now = Date.now();
    const requestData = {
      requestId,
      clientId: socket.id,
      clientUid: ensureString(payload.clientUid || payload.uid || '', '') || null,
      clientName: ensureString(payload.clientName, 'Cliente'),
      brand: ensureString(payload.brand || payload?.device?.brand || '', '') || null,
      model: ensureString(payload.model || payload?.device?.model || '', '') || null,
      osVersion: ensureString(payload?.device?.osVersion || payload.osVersion || '', '') || null,
      plan: ensureString(payload.plan || '', '') || null,
      issue: ensureString(payload.issue || '', '') || null,
      extra: typeof payload.extra === 'object' && payload.extra !== null ? payload.extra : {},
      createdAt: now,
      state: 'queued',
    };

    try {
      await requestsCollection.doc(requestId).set(requestData);
      socket.emit('support:enqueued', { requestId });
      io.emit('queue:updated', { requestId, state: 'queued' });
    } catch (err) {
      console.error('Failed to persist support request', err);
      socket.emit('support:error', { error: 'request_failed' });
    }
  });

  // Mantém sua sinalização atual por sala (sessionId)
  socket.on('join', (payload) => {
    const room = typeof payload === 'string' ? payload : payload?.room;
    const role = typeof payload === 'object' ? payload?.role : undefined;
    if (!room) return;

    socket.join(room);
    socket.data.room = room;
    socket.to(room).emit('peer-joined', { role });
  });

  // Sinalização legada para send.html (room-based)
  socket.on('signal', (payload = {}) => {
    const room = ensureString(payload.room || '', '').trim();
    if (!room) return;
    if (!payload.data) return;
    socket.to(room).emit('signal', payload.data);
  });

  socket.on('session:join', async (payload = {}, ack) => {
    const sessionId = normalizeSessionId(payload.sessionId);
    if (!sessionId) {
      return respondAck(ack, { ok: false, err: 'no-session' });
    }

    const snapshot = await getSessionSnapshot(sessionId);
    if (!snapshot) {
      return respondAck(ack, { ok: false, err: 'session-not-found' });
    }

    const userTypeRaw = ensureString(payload.userType || payload.role || '', '').toLowerCase();
    const userType = userTypeRaw === 'tech' || userTypeRaw === 'client' ? userTypeRaw : 'unknown';
    if (userType === 'tech' && !socket.user) {
      return respondAck(ack, { ok: false, err: 'missing_token' });
    }
    const room = `s:${sessionId}`;
    socket.join(room);
    if (!socket.data.sessionRoles) socket.data.sessionRoles = {};
    socket.data.sessionRoles[sessionId] = userType;
    socket.data.sessionId = sessionId;
    socket.data.userType = userType;
    connectionIndex.set(socket.id, { socketId: socket.id, userType, sessionId });
    respondAck(ack, { ok: true });
  });

  socket.on('session:chat:send', async (msg = {}, ack) => {
    const sessionId = normalizeSessionId(msg.sessionId);
    const text = ensureString(msg.text || '', '').trim();
    const from = ensureString(msg.from || '', '');
    const typeRaw = ensureString(msg.type || '', '').trim().toLowerCase();
    const type = typeRaw || (msg.audioUrl ? 'audio' : msg.imageUrl ? 'image' : msg.fileUrl ? 'file' : 'text');
    const audioUrl = ensureString(msg.audioUrl || '', '').trim();
    const imageUrl = ensureString(msg.imageUrl || '', '').trim();
    const fileUrl = ensureString(msg.fileUrl || '', '').trim();
    const hasRenderableContent = Boolean(text || audioUrl || imageUrl || fileUrl);
    if (!sessionId || !hasRenderableContent) {
      return respondAck(ack, { ok: false, err: 'bad-payload' });
    }

    const snapshot = await getSessionSnapshot(sessionId);
    if (!snapshot) {
      return respondAck(ack, { ok: false, err: 'session-not-found' });
    }

    const room = `s:${sessionId}`;
    const providedId = ensureString(msg.id || '', '');
    const ts = typeof msg.ts === 'number' ? msg.ts : Date.now();
    const messageId = providedId || Date.now().toString(36);
    const out = {
      id: messageId,
      sessionId,
      from: from || 'unknown',
      type,
      text,
      audioUrl,
      imageUrl,
      fileUrl,
      status: ensureString(msg.status || '', '').trim() || 'sent',
      ts,
    };

    try {
      await snapshot.ref.collection('messages').doc(messageId).set(out);
      await snapshot.ref.set(
        {
          lastMessageAt: ts,
          updatedAt: ts,
          'extra.lastMessageAt': ts,
        },
        { merge: true }
      );
    } catch (err) {
      console.error('Failed to store chat message', err);
      return respondAck(ack, { ok: false, err: 'store-failed' });
    }

    socket.to(room).emit('session:chat:new', out);
    await emitSessionUpdated(sessionId);

    respondAck(ack, { ok: true, id: out.id });
  });

  socket.on('session:command', async (cmd = {}, ack) => {
    const sessionId = normalizeSessionId(cmd.sessionId);
    const rawType = ensureString(cmd.type || '', '').trim();
    if (!sessionId || !rawType) {
      return respondAck(ack, { ok: false, err: 'bad-payload' });
    }

    const snapshot = await getSessionSnapshot(sessionId);
    if (!snapshot) {
      return respondAck(ack, { ok: false, err: 'session-not-found' });
    }

    const session = snapshot.data() || {};
    const byRole = socket.data?.sessionRoles?.[sessionId];
    const ts = Date.now();
    const normalizedType = normalizeEventType(rawType);
    const by = ensureString(cmd.by || byRole || socket.id, '');
    const eventId = ensureString(cmd.id || '', '') || `${ts.toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    const enriched = {
      id: eventId,
      sessionId,
      type: normalizedType,
      rawType,
      data: cmd.data || null,
      by,
      ts,
      kind: 'command',
    };

    const room = `s:${sessionId}`;
    const socketPayload = {
      ...enriched,
      type: rawType,
      normalizedType,
    };
    socket.to(room).emit('session:command', socketPayload);

    const nextTelemetry =
      typeof session.telemetry === 'object' && session.telemetry !== null ? { ...session.telemetry } : {};

    const setFlag = (flag, value) => {
      nextTelemetry[flag] = value;
    };

    const updates = {
      updatedAt: ts,
      lastCommandAt: ts,
      'extra.lastCommand': enriched,
    };

    switch (normalizedType) {
      case 'share_start':
        setFlag('shareActive', true);
        break;
      case 'share_stop':
        setFlag('shareActive', false);
        break;
      case 'remote_grant':
        setFlag('remoteActive', true);
        break;
      case 'remote_revoke':
        setFlag('remoteActive', false);
        break;
      case 'call_start':
        setFlag('callActive', true);
        break;
      case 'call_end':
        setFlag('callActive', false);
        break;
      case 'end': {
        updates.status = 'closed';
        updates.closedAt = ts;
        updates.handleTimeMs = ts - (session.acceptedAt || session.createdAt || ts);
        updates.outcome = session.outcome || 'peer_ended';
        setFlag('shareActive', false);
        setFlag('callActive', false);
        setFlag('remoteActive', false);
        io.to(room).emit('session:ended', { sessionId, reason: 'peer_ended' });
        io.socketsLeave(room);
        break;
      }
      default:
        break;
    }

    nextTelemetry.updatedAt = ts;

    const telemetryUpdates = Object.keys(nextTelemetry).length
      ? {
          telemetry: nextTelemetry,
          'extra.telemetry': nextTelemetry,
        }
      : {};

    if (typeof nextTelemetry.network !== 'undefined') updates['extra.network'] = nextTelemetry.network;
    if (typeof nextTelemetry.health !== 'undefined') updates['extra.health'] = nextTelemetry.health;
    if (typeof nextTelemetry.permissions !== 'undefined')
      updates['extra.permissions'] = nextTelemetry.permissions;
    if (typeof nextTelemetry.alerts !== 'undefined') updates['extra.alerts'] = nextTelemetry.alerts;

    try {
      await snapshot.ref.collection('events').doc(eventId).set(enriched);
      await snapshot.ref.set(
        {
          ...updates,
          ...telemetryUpdates,
        },
        { merge: true }
      );
    } catch (err) {
      console.error('Failed to persist command event', err);
      return respondAck(ack, { ok: false, err: 'store-failed' });
    }

    await emitSessionUpdated(sessionId);

    respondAck(ack, { ok: true });
  });

  socket.on('session:telemetry', async (payload = {}, ack) => {
    const sessionId = normalizeSessionId(payload.sessionId);
    if (!sessionId) {
      return respondAck(ack, { ok: false, err: 'bad-payload' });
    }

    const snapshot = await getSessionSnapshot(sessionId);
    if (!snapshot) {
      return respondAck(ack, { ok: false, err: 'session-not-found' });
    }

    const data = typeof payload.data === 'object' && payload.data !== null ? payload.data : {};
    const ts = Date.now();
    const from = ensureString(payload.from || '', '');
    const status = {
      sessionId,
      from,
      data,
      ts,
    };

    const mergedTelemetry = {
      ...(snapshot.data()?.telemetry || {}),
      ...data,
      updatedAt: ts,
    };

    const eventId = `${ts.toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    const telemetryEvent = {
      id: eventId,
      sessionId,
      kind: 'telemetry',
      type: 'telemetry',
      data,
      by: from || 'unknown',
      ts,
    };

    const updates = {
      telemetry: mergedTelemetry,
      'extra.telemetry': mergedTelemetry,
      updatedAt: ts,
    };
    if (typeof data.network !== 'undefined') updates['extra.network'] = ensureString(data.network, '');
    if (typeof data.health !== 'undefined') updates['extra.health'] = ensureString(data.health, '');
    if (typeof data.permissions !== 'undefined') updates['extra.permissions'] = ensureString(data.permissions, '');
    if (typeof data.alerts !== 'undefined') updates['extra.alerts'] = ensureString(data.alerts, '');

    try {
      await snapshot.ref.collection('events').doc(eventId).set(telemetryEvent);
      await snapshot.ref.set(updates, { merge: true });
    } catch (err) {
      console.error('Failed to persist telemetry event', err);
      return respondAck(ack, { ok: false, err: 'store-failed' });
    }

    io.to(`s:${sessionId}`).emit('session:status', status);
    await emitSessionUpdated(sessionId);

    respondAck(ack, { ok: true });
  });

  const relaySignal = (eventName) => {
    socket.on(eventName, (payload = {}) => {
      const sessionId = normalizeSessionId(payload.sessionId);
      if (!sessionId) return;
      const room = `s:${sessionId}`;
      socket.to(room).emit(eventName, {
        sessionId,
        ...(payload.sdp ? { sdp: payload.sdp } : {}),
        ...(payload.candidate ? { candidate: payload.candidate } : {}),
      });
    });
  };

  ['signal:offer', 'signal:answer', 'signal:candidate'].forEach(relaySignal);

  socket.on('disconnect', async () => {
    connectionIndex.delete(socket.id);
    const requestsCollection = getRequestsCollection();
    if (requestsCollection && db) {
      try {
        const snapshot = await requestsCollection.where('clientId', '==', socket.id).get();
        const batch = db.batch();
        let hasDeletes = false;
        snapshot.docs.forEach((doc) => {
          const data = doc.data() || {};
          if (data.state === 'queued') {
            batch.delete(doc.ref);
            io.emit('queue:updated', { requestId: doc.id, state: 'removed' });
            hasDeletes = true;
          }
        });
        if (hasDeletes) {
          await batch.commit();
        }
      } catch (err) {
        console.error('Failed to cleanup queued requests on disconnect', err);
      }
    } else {
      console.warn('Firestore not configured. Skipping queued request cleanup on disconnect.');
    }
    if (socket.data?.room) {
      socket.to(socket.data.room).emit('peer-left');
    }
  });
});

// ====== HTTP API (usada pelo central.html)
app.get('/api/requests', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const requestsRef = getRequestsCollection();
  if (!requestsRef) {
    console.error('Firestore not configured. Cannot list requests.');
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const status = ensureString(req.query.status || '', '').toLowerCase();

  try {
    let snapshot;
    if (status) {
      snapshot = await requestsRef.where('state', '==', status).get();
    } else {
      snapshot = await requestsRef.get();
    }
    const list = snapshot.docs.map((doc) => {
      const data = doc.data() || {};
      return {
        requestId: doc.id,
        clientName: data.clientName || 'Cliente',
        brand: data.brand || null,
        model: data.model || null,
        createdAt: data.createdAt || null,
        state: data.state || 'queued',
      };
    });
    list.sort((a, b) => (a.createdAt || 0) - (b.createdAt || 0));
    res.json(list);
  } catch (err) {
    console.error('Failed to fetch requests', err);
    if (status === 'queued') {
      return res.status(503).json({ error: 'firestore_unavailable' });
    }
    res.status(500).json({ error: 'firestore_error' });
  }
});

// Aceitar um request -> cria sessionId, notifica cliente

app.post('/api/sessions/:id/claim', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const sessionId = normalizeSessionId(req.params.id);
  if (!sessionId) {
    return res.status(400).json({ error: 'invalid_session_id' });
  }

  const sessionsCollection = getSessionsCollection();
  if (!sessionsCollection || !db) {
    console.error('Firestore not configured. Cannot claim session.');
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  try {
    const uid = ensureString(req.user?.uid || '', '');
    if (!uid) {
      return res.status(401).json({ error: 'invalid_token' });
    }

    const techData = req.techAccess?.techDoc || {};
    const sessionRef = sessionsCollection.doc(sessionId);
    const techName = ensureString(techData.name || techData.displayName || req.user?.name || 'Técnico', 'Técnico') || 'Técnico';

    await db.runTransaction(async (tx) => {
      const sessionSnap = await tx.get(sessionRef);
      if (!sessionSnap.exists) {
        throw new Error('session_not_found');
      }

      const sessionData = sessionSnap.data() || {};
      const existingTech = sessionData.tech;
      if (existingTech && typeof existingTech === 'object' && ensureString(existingTech.techUid || existingTech.uid || '', '')) {
        throw new Error('already_claimed');
      }

      const techEmail = ensureString(techData.email || req.user?.email || '', '') || null;
      const techPhotoURL =
        ensureString(techData.photoURL || techData.photoUrl || req.user?.picture || '', '') || null;

      tx.update(sessionRef, {
        tech: {
          techUid: uid,
          techId: uid,
          uid,
          id: uid,
          name: techName,
          techName,
          email: techEmail,
          techPhotoURL,
          photoURL: techPhotoURL,
        },
        techUid: uid,
        techId: uid,
        techName,
        techEmail,
        techPhotoURL,
        updatedAt: Date.now(),
        status: sessionData.status || 'open',
      });
    });

    return res.json({ ok: true, sessionId });
  } catch (err) {
    const message = ensureString(err && err.message ? err.message : err, 'server_error');
    if (message.includes('already_claimed')) return res.status(409).json({ error: 'already_claimed' });
    if (message.includes('session_not_found')) return res.status(404).json({ error: 'session_not_found' });
    if (message.includes('auth/id-token-expired')) return res.status(401).json({ error: 'token_expired' });
    if (message.includes('auth/argument-error') || message.includes('auth/invalid')) {
      return res.status(401).json({ error: 'invalid_token' });
    }
    console.error('Failed to claim session', err);
    return res.status(500).json({ error: 'server_error', detail: message });
  }
});

app.post('/api/requests/:id/accept', async (req, res) => {
  const id = req.params.id;
  const requestsCollection = getRequestsCollection();
  const sessionsCollection = getSessionsCollection();
  if (!requestsCollection || !sessionsCollection) {
    console.error('Firestore not configured. Cannot accept request.');
    return res.status(503).json({ error: 'firestore_unavailable' });
  }
  try {
    const requestRef = requestsCollection.doc(id);
    const snapshot = await requestRef.get();
    if (!snapshot.exists) {
      return res.status(404).json({ error: 'request_not_found_or_already_taken' });
    }

    const request = snapshot.data() || {};
    if (request.state && request.state !== 'queued') {
      return res.status(404).json({ error: 'request_not_found_or_already_taken' });
    }

    const sessionId = nanoid().toUpperCase();
    const now = Date.now();
    const techName = req.body && req.body.techName ? ensureString(req.body.techName, 'Técnico') : 'Técnico';
    const techId = req.body && req.body.techId ? ensureString(req.body.techId, '') || null : null;
    const techUid = req.body && req.body.techUid ? ensureString(req.body.techUid, '') || null : techId;
    const baseExtra = typeof request.extra === 'object' && request.extra !== null ? { ...request.extra } : {};
    const baseTelemetry =
      typeof baseExtra.telemetry === 'object' && baseExtra.telemetry !== null ? { ...baseExtra.telemetry } : {};
    const sessionData = {
      sessionId,
      requestId: id,
      clientId: request.clientId || null,
      clientUid: request.clientUid || null,
      techName,
      techId,
      techUid,
      clientName: request.clientName || 'Cliente',
      brand: request.brand || null,
      model: request.model || null,
      osVersion: request.osVersion || null,
      plan: request.plan || null,
      issue: request.issue || null,
      requestedAt: request.createdAt || now,
      acceptedAt: now,
      waitTimeMs: now - (request.createdAt || now),
      status: 'active',
      createdAt: now,
      updatedAt: now,
      telemetry: baseTelemetry,
      extra: { ...baseExtra, telemetry: baseTelemetry },
    };

    await sessionsCollection.doc(sessionId).set(sessionData);
    await requestRef.delete();

    if (request.clientId) {
      try {
        io.to(request.clientId).emit('support:accepted', { sessionId, techName });
      } catch (err) {
        console.error('Failed to emit acceptance to client', err);
      }
    }

    io.emit('queue:updated', { requestId: id, state: 'accepted', sessionId });
    await emitSessionUpdated(sessionId);

    return res.json({ sessionId });
  } catch (err) {
    console.error('Failed to accept request', err);
    return res.status(500).json({ error: 'firestore_error' });
  }
});

// Recusar/remover um request (apaga da fila e, se quiser, avisa o cliente)
app.delete('/api/requests/:id', async (req, res) => {
  const id = req.params.id;
  const requestsCollection = getRequestsCollection();
  if (!requestsCollection) {
    console.error('Firestore not configured. Cannot remove request.');
    return res.status(503).json({ error: 'firestore_unavailable' });
  }
  try {
    const requestRef = requestsCollection.doc(id);
    const snapshot = await requestRef.get();
    if (!snapshot.exists) {
      return res.status(204).end();
    }
    const data = snapshot.data() || {};
    await requestRef.delete();
    if (data.clientId) {
      try {
        io.to(data.clientId).emit('support:rejected', { requestId: id });
      } catch (err) {
        console.error('Failed to emit rejection to client', err);
      }
    }
    io.emit('queue:updated', { requestId: id, state: 'removed' });
    return res.status(204).end();
  } catch (err) {
    console.error('Failed to remove request', err);
    return res.status(500).json({ error: 'firestore_error' });
  }
});

// Debug/saúde
app.get('/health', async (_req, res) => {
  if (!isFirestoreReady()) {
    return res.status(503).json({ ok: false, error: 'firestore_unavailable' });
  }
  try {
    const requestsCollection = getRequestsCollection();
    const sessionsCollection = getSessionsCollection();
    if (!requestsCollection || !sessionsCollection) {
      return res.status(503).json({ ok: false, error: 'firestore_unavailable' });
    }
    const [requestsSnap, sessionsSnap] = await Promise.all([
      requestsCollection.get(),
      sessionsCollection.get(),
    ]);
    res.json({ ok: true, requests: requestsSnap.size, sessions: sessionsSnap.size, now: Date.now() });
  } catch (err) {
    console.error('Failed to compute health status', err);
    res.status(500).json({ ok: false, error: 'firestore_error' });
  }
});


app.get('/api/auth/me', requireAuth(), async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  try {
    const access = await buildTechAccessPayload(req.user || {});
    if (!access.ok) {
      return res.status(access.status || 403).json({ error: access.error || 'not_tech' });
    }
    return res.json(access.payload);
  } catch (error) {
    console.error('Failed to get auth profile', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.get('/api/sessions', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const sessionsCollection = getSessionsCollection();
  if (!sessionsCollection) {
    console.error('Firestore not configured. Cannot list sessions.');
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  try {
    const uid = ensureString(req.user?.uid || '', '');
    if (!uid) {
      return res.status(401).json({ error: 'invalid_token' });
    }

    const mineOnly = ensureString(req.query.mine || '', '').trim() === '1';

    const assignedQuery = sessionsCollection
      .where('tech.techUid', '==', uid)
      .orderBy('updatedAt', 'desc')
      .limit(100);

    const queueQuery = sessionsCollection
      .where('status', '==', 'queued')
      .orderBy('createdAt', 'desc')
      .limit(100);

    const assignedSnapshotPromise = assignedQuery.get();
    const queueSnapshotPromise = mineOnly ? Promise.resolve({ docs: [] }) : queueQuery.get();
    const [assignedSnapshot, queueSnapshot] = await Promise.all([
      assignedSnapshotPromise,
      queueSnapshotPromise,
    ]);

    const allDocs = [...(queueSnapshot.docs || []), ...assignedSnapshot.docs];
    const uniqueById = new Map();
    allDocs.forEach((doc) => {
      uniqueById.set(doc.id, doc);
    });

    const sessions = await Promise.all(
      Array.from(uniqueById.values()).map((doc) => buildSessionState(doc.id, { snapshot: doc }))
    );

    return res.json({ sessions: sessions.filter(Boolean) });
  } catch (err) {
    console.error('Failed to fetch sessions', err);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/sessions/:id/close', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const id = req.params.id;
  if (!getSessionsCollection()) {
    console.error('Firestore not configured. Cannot close session.');
    return res.status(503).json({ error: 'firestore_unavailable' });
  }
  try {
    const snapshot = await getSessionSnapshot(id);
    if (!snapshot) {
      return res.status(404).json({ error: 'session_not_found' });
    }

    const session = snapshot.data() || {};
    if (session.status === 'closed') {
      return res.status(409).json({ error: 'session_already_closed' });
    }

    const payload = req.body || {};
    const closedAt = Date.now();
    const updates = {
      status: 'closed',
      closedAt,
      outcome: ensureString(payload.outcome || session.outcome || 'resolved', 'resolved'),
      symptom: ensureString(payload.symptom || session.symptom || '', '') || null,
      solution: ensureString(payload.solution || session.solution || '', '') || null,
      handleTimeMs: closedAt - (session.acceptedAt || session.createdAt || closedAt),
      updatedAt: closedAt,
    };

    if (payload.notes && typeof payload.notes === 'string') {
      updates.notes = ensureString(payload.notes, '');
    }
    if (typeof payload.npsScore !== 'undefined') {
      const nps = Number(payload.npsScore);
      if (!Number.isNaN(nps)) {
        updates.npsScore = Math.max(0, Math.min(10, Math.round(nps)));
      }
    }
    if (typeof payload.firstContactResolution !== 'undefined') {
      updates.firstContactResolution = Boolean(payload.firstContactResolution);
    }

    await snapshot.ref.set(updates, { merge: true });
    await emitSessionUpdated(id);

    return res.json({ ok: true });
  } catch (err) {
    console.error('Failed to close session', err);
    return res.status(500).json({ error: 'firestore_error' });
  }
});

app.get('/api/metrics', async (req, res) => {
  const sessionsCollection = getSessionsCollection();
  const requestsCollection = getRequestsCollection();
  if (!sessionsCollection || !requestsCollection) {
    console.error('Firestore not configured. Cannot compute metrics.');
    return res.status(503).json({ error: 'firestore_unavailable' });
  }
  try {
    const techFilterRaw = ensureString(req.query.tech || req.query.techId || '', '');
    const techFilter = techFilterRaw ? techFilterRaw.toLowerCase() : '';
    const startFilter = toMillis(req.query.start, null);
    const endFilter = toMillis(req.query.end, null);

    const now = new Date();
    const defaultStart = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
    const rangeStart = startFilter !== null ? startFilter : defaultStart;
    let query = sessionsCollection.where('acceptedAt', '>=', rangeStart);
    if (endFilter !== null) {
      query = query.where('acceptedAt', '<=', endFilter);
    }
    query = query.orderBy('acceptedAt', 'desc');

    const snapshot = await query.get();
    let sessions = snapshot.docs.map((doc) => ({ id: doc.id, ...(doc.data() || {}) }));
    if (techFilter) {
      sessions = sessions.filter((session) => {
        const techName = ensureString(session.techName || '', '').toLowerCase();
        const techId = ensureString(session.techId || '', '').toLowerCase();
        const techUidValue = ensureString(session.techUid || '', '').toLowerCase();
        return techName === techFilter || techId === techFilter || techUidValue === techFilter;
      });
    }

    if (endFilter !== null) {
      sessions = sessions.filter((session) => (session.acceptedAt || 0) <= endFilter);
    }

    const todaysSessions = sessions;
    const closedSessions = todaysSessions.filter((s) => s.status === 'closed');
    const activeSessions = todaysSessions.filter((s) => s.status === 'active');

    const waitTimes = todaysSessions
      .map((s) => s.waitTimeMs)
      .filter((ms) => typeof ms === 'number' && ms >= 0);
    const averageWaitMs = waitTimes.length ? waitTimes.reduce((a, b) => a + b, 0) / waitTimes.length : null;

    const handleTimes = closedSessions
      .map((s) => s.handleTimeMs)
      .filter((ms) => typeof ms === 'number' && ms >= 0);
    const averageHandleMs = handleTimes.length ? handleTimes.reduce((a, b) => a + b, 0) / handleTimes.length : null;

    const fcrValues = closedSessions
      .filter((s) => typeof s.firstContactResolution === 'boolean')
      .map((s) => (s.firstContactResolution ? 1 : 0));
    const fcrPercentage = fcrValues.length
      ? Math.round((fcrValues.reduce((a, b) => a + b, 0) / fcrValues.length) * 100)
      : null;

    const npsScores = closedSessions
      .map((s) => (typeof s.npsScore === 'number' ? s.npsScore : null))
      .filter((n) => n !== null && !Number.isNaN(n));
    let nps = null;
    if (npsScores.length) {
      const promoters = npsScores.filter((score) => score >= 9).length;
      const detractors = npsScores.filter((score) => score <= 6).length;
      nps = Math.round(((promoters - detractors) / npsScores.length) * 100);
    }

    const queueSnapshot = await requestsCollection.where('state', '==', 'queued').get();

    res.json({
      attendancesToday: todaysSessions.length,
      activeSessions: activeSessions.length,
      averageWaitMs,
      averageHandleMs,
      fcrPercentage,
      nps,
      queueSize: queueSnapshot.size,
      lastUpdated: Date.now(),
    });
  } catch (err) {
    console.error('Failed to compute metrics', err);
    res.status(500).json({ error: 'firestore_error' });
  }
});

// Start
server.listen(PORT, () => {
  console.log(`Suporte X signaling server running on :${PORT}`);
});
