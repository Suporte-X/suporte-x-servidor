const path = require('path');
const express = require('express');
const http = require('http');
const cors = require('cors');
const { Server } = require('socket.io');
const { customAlphabet } = require('nanoid');
const { db, firebaseProjectId } = require('./firebase');
const admin = require('firebase-admin');
const { requireAuth, normalizeRole } = require('./auth');
const { createUploadRouter } = require('./uploadRouter');

const ensureString = (value, fallback = '') => {
  if (typeof value === 'string') return value.slice(0, 256);
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value).slice(0, 256);
  }
  return fallback;
};

const ensureFullString = (value, fallback = '') => {
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  return fallback;
};

const ensureLongString = (value, fallback = '', maxLength = 4096) => {
  const normalized = ensureFullString(value, fallback);
  if (typeof normalized !== 'string') return fallback;
  return normalized.slice(0, maxLength);
};

const ensureBoolean = (value, fallback = false) => {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (normalized === 'true') return true;
    if (normalized === 'false') return false;
  }
  return fallback;
};

const ensureArray = (value) => (Array.isArray(value) ? value : []);

const buildProfileHistoryEntry = ({ field, from = null, to = null, source = 'self' }) => ({
  id: customAlphabet('abcdefghijklmnopqrstuvwxyz0123456789', 14)(),
  field: ensureString(field || '', '').slice(0, 32) || 'profile',
  from: from == null ? null : ensureString(from, ''),
  to: to == null ? null : ensureString(to, ''),
  source: ensureString(source || 'self', '').slice(0, 32) || 'self',
  // Firestore does not allow FieldValue.serverTimestamp() inside arrayUnion payloads.
  // profileHistory entries are array items, so use a concrete timestamp value.
  createdAt: admin.firestore.Timestamp.now(),
});

const mapFirestoreWriteError = (error) => {
  const message = ensureString(error?.message || '', '');
  if (message.includes('FieldValue.serverTimestamp() cannot be used inside of an array')) {
    return {
      status: 400,
      error: 'invalid_history_timestamp',
      message: 'Não foi possível salvar o histórico: timestamp inválido em item de lista.',
    };
  }
  return {
    status: 500,
    error: 'server_error',
    message: 'Erro interno ao salvar alterações de perfil.',
  };
};

const safeGetDocs = async (query, contextLabel) => {
  try {
    const snapshot = await query.get();
    return snapshot.docs || [];
  } catch (error) {
    console.error(`Failed to fetch ${contextLabel}`, error);
    return [];
  }
};

const mapAdminError = (error) => {
  const code = ensureString(error?.code || '', '');
  const message = ensureString(error?.message || '', '');

  const isAuthPermissionError =
    code === 'auth/insufficient-permission' ||
    code === 'auth/invalid-credential' ||
    message.toLowerCase().includes('insufficient permission') ||
    message.toLowerCase().includes('permission iam') ||
    message.toLowerCase().includes('firebaseauth');

  if (isAuthPermissionError) {
    return {
      status: 503,
      error: 'firebase_admin_permission_denied',
      message:
        'Painel administrativo sem permissão no Firebase Auth. Verifique se a conta de serviço do backend possui as roles Firebase Authentication Admin e Service Account Token Creator.',
    };
  }

  if (code === 'auth/email-already-exists') {
    return { status: 409, error: 'email_already_exists', message: 'Este email já está cadastrado.' };
  }
  if (code === 'auth/invalid-password') {
    return { status: 400, error: 'invalid_password', message: 'Senha temporária inválida (mínimo de 6 caracteres).' };
  }
  if (code === 'auth/invalid-email') {
    return { status: 400, error: 'invalid_email', message: 'Email inválido.' };
  }
  if (code === 'auth/user-not-found') {
    return {
      status: 404,
      error: 'tech_user_not_found',
      message: 'O usuário deste técnico não foi encontrado no Firebase Authentication.',
    };
  }
  return { status: 500, error: 'server_error', message: 'Erro interno ao processar solicitação.' };
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


const extractSocketToken = (socket) => {
  const auth = socket?.handshake?.auth || {};
  const authToken = ensureFullString(auth.token || '', '').trim();
  if (authToken) return authToken;

  const authHeader = ensureFullString(socket?.handshake?.headers?.authorization || '', '');
  if (authHeader.toLowerCase().startsWith('bearer ')) {
    const token = authHeader.slice(7).trim();
    if (token) return token;
  }

  const headerToken = ensureFullString(socket?.handshake?.headers?.['x-id-token'] || '', '').trim();
  if (headerToken) return headerToken;

  return '';
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
const STORAGE_BUCKET_NAME =
  ensureString(process.env.FIREBASE_STORAGE_BUCKET || process.env.CENTRAL_FIREBASE_STORAGE_BUCKET || '', '') ||
  'suporte-x-19ae8.firebasestorage.app';

app.use(cors(corsOptions));
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  if (isProduction) {
    res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
  }
  next();
});

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
let uploadBucket = null;
try {
  uploadBucket = admin.storage().bucket(STORAGE_BUCKET_NAME);
} catch (error) {
  console.error('Falha ao inicializar bucket para uploads seguros', error);
}

if (uploadBucket) {
  app.use('/api/upload', createUploadRouter({
    auth: admin.auth(),
    db,
    bucket: uploadBucket,
    logger: console,
  }));
} else {
  app.use('/api/upload', (_req, res) => {
    res.status(503).json({ error: 'storage_unavailable' });
  });
}

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
    const token = extractSocketToken(socket);
    const requiresAuth = auth.requireAuth === true || auth.panel === 'tech';

    if (!token) {
      if (requiresAuth) return next(new Error('missing_token'));
      return next();
    }

    const decoded = await admin.auth().verifyIdToken(token);
    socket.user = decoded;
    return next();
  } catch (error) {
    console.error('Socket auth failed', error);
    const code = ensureFullString(error?.code || '', 'unknown_error').trim() || 'unknown_error';
    return next(new Error(`invalid_token:${code}`));
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

const getSessionTechUid = (sessionData = {}) =>
  ensureString(
    sessionData.techUid ||
      sessionData?.tech?.techUid ||
      sessionData?.tech?.uid ||
      '',
    ''
  ).trim();

const getSessionClientUid = (sessionData = {}) =>
  ensureString(sessionData.clientUid || '', '').trim();

const isActiveTechUid = async (uid) => {
  if (!db) return false;
  const normalizedUid = ensureString(uid || '', '').trim();
  if (!normalizedUid) return false;
  const techSnap = await db.collection('techs').doc(normalizedUid).get();
  return techSnap.exists && techSnap.data()?.active === true;
};

const resolveSocketAuthFromPayload = async (socket, payload = {}) => {
  if (socket?.user?.uid) {
    return socket.user;
  }

  const payloadToken = ensureString(payload.idToken || payload.token || '', '').trim();
  if (!payloadToken) return null;

  const decoded = await admin.auth().verifyIdToken(payloadToken);
  socket.user = decoded;
  return decoded;
};

const validateSocketSessionAccess = async (socket, sessionId, expectedRole = 'any') => {
  const snapshot = await getSessionSnapshot(sessionId);
  if (!snapshot) {
    return { ok: false, code: 'session-not-found' };
  }

  const sessionData = snapshot.data() || {};
  const sessionTechUid = getSessionTechUid(sessionData);
  const sessionClientUid = getSessionClientUid(sessionData);
  const authUid = ensureString(socket?.user?.uid || '', '').trim();

  const techAllowed =
    authUid &&
    sessionTechUid &&
    authUid === sessionTechUid &&
    normalizeRole(socket?.user?.role) === 'tech' &&
    (await isActiveTechUid(authUid));

  const clientAllowedByUid =
    authUid &&
    sessionClientUid &&
    authUid === sessionClientUid;

  const clientAllowedByLegacySocketId =
    ensureString(sessionData.clientId || '', '').trim() === ensureString(socket?.id || '', '').trim();

  // Legacy fallback is only allowed for sessions that still do not have clientUid bound.
  // Once clientUid exists, socket-id-only access is considered insecure.
  const allowLegacyClientSocketFallback = !sessionClientUid && clientAllowedByLegacySocketId;
  const clientAllowed = clientAllowedByUid || allowLegacyClientSocketFallback;

  if (expectedRole === 'tech' && !techAllowed) {
    return { ok: false, code: 'forbidden' };
  }
  if (expectedRole === 'client' && !clientAllowed) {
    return { ok: false, code: 'forbidden' };
  }
  if (expectedRole === 'any' && !techAllowed && !clientAllowed) {
    return { ok: false, code: 'forbidden' };
  }

  return {
    ok: true,
    snapshot,
    sessionData,
    role: techAllowed ? 'tech' : 'client',
  };
};

const normalizeLegacyRoom = (payload = {}) => {
  if (typeof payload === 'string') return normalizeSessionId(payload);
  if (!payload || typeof payload !== 'object') return '';
  return normalizeSessionId(payload.room || payload.sessionId || payload.code || '');
};

const normalizeLegacyRole = (value, fallback = 'client') => {
  const raw = ensureString(value || '', '').trim().toLowerCase();
  if (raw === 'tech' || raw === 'viewer' || raw === 'supervisor') return 'tech';
  if (raw === 'client' || raw === 'sender') return 'client';
  if (fallback === 'tech' || fallback === 'client') return fallback;
  return '';
};

const resolveLegacyJoinAccess = async (socket, payload = {}) => {
  const room = normalizeLegacyRoom(payload);
  if (!room) return { ok: false, code: 'no-room' };

  let decoded = socket?.user || null;
  try {
    decoded = await resolveSocketAuthFromPayload(socket, typeof payload === 'object' ? payload : {});
  } catch (err) {
    console.error('Failed to resolve auth for legacy join', err);
    return { ok: false, code: 'invalid_token' };
  }

  if (!decoded?.uid) {
    return { ok: false, code: 'missing_token' };
  }

  const requestedRole = normalizeLegacyRole(typeof payload === 'object' ? payload.role : '', 'client');
  if (requestedRole === 'tech') {
    const isTechRole = normalizeRole(decoded?.role) === 'tech';
    const isTechActive = isTechRole ? await isActiveTechUid(decoded.uid) : false;
    if (!isTechActive) {
      return { ok: false, code: 'forbidden' };
    }
  }

  return { ok: true, room, role: requestedRole };
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

  const techSnap = await db.collection('techs').doc(uid).get();
  console.log('[auth/me] tech doc exists:', techSnap.exists);
  console.log('[auth/me] tech doc data:', techSnap.data());
  const techDoc = techSnap.exists ? techSnap.data() || {} : null;
  const roleClaim = normalizeRole(decoded?.role);
  const isActiveTech = Boolean(techDoc && techDoc.active === true);
  const supervisor = decoded?.supervisor === true;

  if (roleClaim !== 'tech') {
    return {
      ok: false,
      status: 403,
      error: 'insufficient_role',
    };
  }

  if (!isActiveTech) {
    return {
      ok: false,
      status: 403,
      error: 'tech_inactive',
    };
  }

  return {
    ok: true,
    payload: {
      uid,
      email: ensureString(decoded?.email || '', '') || null,
      name: ensureString(decoded?.name || '', '') || null,
      photoURL:
        ensureString(techDoc?.customPhotoURL || techDoc?.photoURL || techDoc?.photoUrl || decoded?.picture || '', '') ||
        null,
      roleClaim,
      supervisor,
      profileHistory: ensureArray(techDoc?.profileHistory),
      techDoc,
    },
  };
};

const requireTechAccess = async (req, res, next) => {
  try {
    if (!db) {
      return res.status(503).json({ error: 'firestore_unavailable' });
    }

    if (!req.user || typeof req.user !== 'object') {
      return res.status(401).json({ error: 'invalid_token' });
    }

    const access = await buildTechAccessPayload(req.user);
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


const requireSupervisor = async (req, res, next) => {
  try {
    if (!db) {
      return res.status(503).json({ error: 'firestore_unavailable' });
    }

    if (!req.user || typeof req.user !== 'object') {
      return res.status(401).json({ error: 'invalid_token' });
    }

    const access = await buildTechAccessPayload(req.user);
    if (!access.ok) {
      return res.status(access.status || 403).json({ error: access.error || 'not_tech' });
    }

    const isSupervisor = req.user?.supervisor === true;
    if (!isSupervisor) {
      return res.status(403).json({ error: 'supervisor_required' });
    }

    if (!req.techAccess) {
      req.techAccess = access.payload;
    }
    return next();
  } catch (error) {
    console.error('Failed to validate supervisor access', error);
    return res.status(500).json({ error: 'server_error' });
  }
};

const upsertTechDoc = async ({ uid, email = null, name = null, active = true, role = 'tech' }) => {
  if (!db || !uid) return;
  await db.collection('techs').doc(uid).set(
    {
      uid,
      email,
      name,
      active: active === true,
      role,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
};

const listTechs = async () => {
  if (!db) return [];
  const snapshot = await db.collection('techs').orderBy('createdAt', 'desc').get();
  const techs = await Promise.all(snapshot.docs.map(async (techDoc) => {
    const data = techDoc.data() || {};
    let userRecord = null;
    try {
      userRecord = await admin.auth().getUser(techDoc.id);
    } catch (error) {
      console.warn('Failed to load auth user for tech list', techDoc.id, error?.code || error?.message || error);
    }

    const normalizedEmail =
      ensureString(data.email || data.profile?.email || '', '').trim().toLowerCase() ||
      ensureString(userRecord?.email || '', '').trim().toLowerCase() ||
      null;

    return {
      uid: techDoc.id,
      name: ensureString(data.name || '', '').trim() || null,
      email: normalizedEmail,
      photoURL:
        ensureString(data.customPhotoURL || data.photoURL || data.photoUrl || userRecord?.photoURL || '', '') || null,
      role: normalizeRole(data.role || 'tech'),
      supervisor: data.supervisor === true || normalizeRole(data.role || 'tech') === 'supervisor',
      active: data.active === true,
      createdAt: data.createdAt || null,
      updatedAt: data.updatedAt || null,
    };
  }));
  return techs;
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

    let decodedClient = null;
    try {
      decodedClient = await resolveSocketAuthFromPayload(socket, payload);
    } catch (err) {
      console.error('Failed to resolve client auth for support:request', err);
      socket.emit('support:error', { error: 'invalid_token' });
      return;
    }

    if (!decodedClient?.uid) {
      socket.emit('support:error', { error: 'missing_token' });
      return;
    }

    const requestId = nanoid().toUpperCase();
    const now = Date.now();
    const requestData = {
      requestId,
      clientId: socket.id,
      clientUid: ensureString(decodedClient.uid || payload.clientUid || payload.uid || '', '') || null,
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
  socket.on('join', async (payload = {}, ack) => {
    const access = await resolveLegacyJoinAccess(socket, payload);
    if (!access.ok) {
      return respondAck(ack, { ok: false, err: access.code || 'forbidden' });
    }

    const { room, role } = access;
    socket.join(room);
    socket.data.room = room;
    if (!socket.data.legacyRooms) socket.data.legacyRooms = {};
    socket.data.legacyRooms[room] = role;
    socket.to(room).emit('peer-joined', { role });
    return respondAck(ack, { ok: true, role });
  });

  // Sinalização legada para send.html (room-based)
  socket.on('signal', async (payload = {}, ack) => {
    const room = normalizeLegacyRoom(payload);
    if (!room) {
      return respondAck(ack, { ok: false, err: 'no-room' });
    }
    if (!payload.data) {
      return respondAck(ack, { ok: false, err: 'bad-payload' });
    }

    const joinedRole = normalizeLegacyRole(socket.data?.legacyRooms?.[room] || '', '');
    if (!joinedRole) {
      return respondAck(ack, { ok: false, err: 'not-joined' });
    }

    if (!socket?.user?.uid) {
      try {
        await resolveSocketAuthFromPayload(socket, payload);
      } catch (err) {
        console.error('Failed to resolve auth for legacy signal', err);
        return respondAck(ack, { ok: false, err: 'invalid_token' });
      }
    }
    if (!socket?.user?.uid) {
      return respondAck(ack, { ok: false, err: 'missing_token' });
    }

    socket.to(room).emit('signal', payload.data);
    return respondAck(ack, { ok: true });
  });

  socket.on('session:join', async (payload = {}, ack) => {
    const sessionId = normalizeSessionId(payload.sessionId);
    if (!sessionId) {
      return respondAck(ack, { ok: false, err: 'no-session' });
    }
    const userTypeRaw = ensureString(payload.userType || payload.role || '', '').toLowerCase();
    const requestedRole = userTypeRaw === 'tech' ? 'tech' : 'client';

    try {
      if (requestedRole === 'client') {
        await resolveSocketAuthFromPayload(socket, payload);
      }
    } catch (err) {
      console.error('Failed to resolve auth for session:join', err);
      return respondAck(ack, { ok: false, err: 'invalid_token' });
    }

    const access = await validateSocketSessionAccess(socket, sessionId, requestedRole);
    if (!access.ok) {
      return respondAck(ack, { ok: false, err: access.code || 'forbidden' });
    }

    const room = `s:${sessionId}`;
    socket.join(room);
    if (!socket.data.sessionRoles) socket.data.sessionRoles = {};
    socket.data.sessionRoles[sessionId] = access.role;
    socket.data.sessionId = sessionId;
    socket.data.userType = access.role;
    connectionIndex.set(socket.id, { socketId: socket.id, userType: access.role, sessionId });
    respondAck(ack, { ok: true, role: access.role });
  });

  socket.on('session:chat:send', async (msg = {}, ack) => {
    const sessionId = normalizeSessionId(msg.sessionId);
    const text = ensureString(msg.text || '', '').trim();
    const from = ensureString(msg.from || '', '');
    const typeRaw = ensureString(msg.type || '', '').trim().toLowerCase();
    const type = typeRaw || (msg.audioUrl ? 'audio' : msg.imageUrl ? 'image' : msg.fileUrl ? 'file' : 'text');
    // Media URLs can easily exceed 256 chars because of Firebase download tokens.
    const audioUrl = ensureLongString(msg.audioUrl || '', '', 4096).trim();
    const imageUrl = ensureLongString(msg.imageUrl || '', '', 4096).trim();
    const fileUrl = ensureLongString(msg.fileUrl || '', '', 4096).trim();
    const fileName = ensureLongString(msg.fileName || '', '', 256).trim();
    const contentType = ensureLongString(msg.contentType || msg.mimeType || '', '', 128).trim().toLowerCase();
    const sizeRaw = msg.size ?? msg.fileSize;
    const fileSize = typeof sizeRaw === 'number' && Number.isFinite(sizeRaw) && sizeRaw > 0 ? sizeRaw : null;
    const hasRenderableContent = Boolean(text || audioUrl || imageUrl || fileUrl);
    if (!sessionId || !hasRenderableContent) {
      return respondAck(ack, { ok: false, err: 'bad-payload' });
    }

    const declaredRole = ensureString(socket.data?.sessionRoles?.[sessionId] || '', '').toLowerCase();
    if (declaredRole !== 'tech' && declaredRole !== 'client') {
      return respondAck(ack, { ok: false, err: 'not-joined' });
    }

    const access = await validateSocketSessionAccess(socket, sessionId, declaredRole);
    if (!access.ok) {
      return respondAck(ack, { ok: false, err: access.code || 'forbidden' });
    }

    const snapshot = access.snapshot;
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
      ...(fileName ? { fileName } : {}),
      ...(contentType ? { contentType, mimeType: contentType } : {}),
      ...(fileSize != null ? { size: fileSize, fileSize } : {}),
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

    const declaredRole = ensureString(socket.data?.sessionRoles?.[sessionId] || '', '').toLowerCase();
    if (declaredRole !== 'tech' && declaredRole !== 'client') {
      return respondAck(ack, { ok: false, err: 'not-joined' });
    }

    const access = await validateSocketSessionAccess(socket, sessionId, declaredRole);
    if (!access.ok) {
      return respondAck(ack, { ok: false, err: access.code || 'forbidden' });
    }

    const snapshot = access.snapshot;
    const session = access.sessionData || {};
    const byRole = access.role;
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

    const declaredRole = ensureString(socket.data?.sessionRoles?.[sessionId] || '', '').toLowerCase();
    if (declaredRole !== 'tech' && declaredRole !== 'client') {
      return respondAck(ack, { ok: false, err: 'not-joined' });
    }

    const access = await validateSocketSessionAccess(socket, sessionId, declaredRole);
    if (!access.ok) {
      return respondAck(ack, { ok: false, err: access.code || 'forbidden' });
    }

    const snapshot = access.snapshot;

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
    socket.on(eventName, async (payload = {}) => {
      const sessionId = normalizeSessionId(payload.sessionId);
      if (!sessionId) return;
      const declaredRole = ensureString(socket.data?.sessionRoles?.[sessionId] || '', '').toLowerCase();
      if (declaredRole !== 'tech' && declaredRole !== 'client') return;
      const access = await validateSocketSessionAccess(socket, sessionId, declaredRole);
      if (!access.ok) return;
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

app.post('/api/requests/:id/accept', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const id = req.params.id;
  const requestsCollection = getRequestsCollection();
  const sessionsCollection = getSessionsCollection();
  if (!requestsCollection || !sessionsCollection) {
    console.error('Firestore not configured. Cannot accept request.');
    return res.status(503).json({ error: 'firestore_unavailable' });
  }
  try {
    const uid = ensureString(req.user?.uid || '', '');
    if (!uid) {
      return res.status(401).json({ error: 'invalid_token' });
    }

    const techData = req.techAccess?.techDoc || {};
    const requestRef = requestsCollection.doc(id);
    const snapshot = await requestRef.get();
    if (!snapshot.exists) {
      return res.status(404).json({ error: 'request_not_found_or_already_taken' });
    }

    const request = snapshot.data() || {};
    if (request.state && request.state !== 'queued') {
      return res.status(404).json({ error: 'request_not_found_or_already_taken' });
    }

    const [assignedRootDocs, assignedLegacyDocs] = await Promise.all([
      safeGetDocs(sessionsCollection.where('techUid', '==', uid).limit(30), 'active-session-check (techUid)'),
      safeGetDocs(sessionsCollection.where('tech.techUid', '==', uid).limit(30), 'active-session-check (legacy tech.techUid)'),
    ]);

    const existingActiveDoc = [...assignedRootDocs, ...assignedLegacyDocs].find((doc) => {
      const data = doc.data() || {};
      return ensureString(data.status || '', '').toLowerCase() === 'active';
    });

    if (existingActiveDoc) {
      const activeData = existingActiveDoc.data() || {};
      const activeSessionId =
        ensureString(activeData.sessionId || '', '').trim() || ensureString(existingActiveDoc.id || '', '').trim() || null;
      return res.status(409).json({ error: 'active_session_exists', sessionId: activeSessionId });
    }

    const sessionId = nanoid().toUpperCase();
    const now = Date.now();
    const techName = req.body && req.body.techName ? ensureString(req.body.techName, 'Técnico') : 'Técnico';
    const techId = req.body && req.body.techId ? ensureString(req.body.techId, '') || null : null;
    const techUid = req.body && req.body.techUid ? ensureString(req.body.techUid, '') || null : techId;
    const normalizedTechName =
      ensureString(techData.name || techData.displayName || req.user?.name || req.body?.techName || techName || 'Tecnico', 'Tecnico') ||
      'Tecnico';
    const normalizedTechUid = uid;
    const normalizedTechId = uid;
    const normalizedTechEmail = ensureString(techData.email || req.user?.email || req.body?.techEmail || '', '') || null;
    const normalizedTechPhotoURL =
      ensureString(techData.photoURL || techData.photoUrl || req.user?.picture || req.body?.techPhotoURL || '', '') || null;
    const baseExtra = typeof request.extra === 'object' && request.extra !== null ? { ...request.extra } : {};
    const baseTelemetry =
      typeof baseExtra.telemetry === 'object' && baseExtra.telemetry !== null ? { ...baseExtra.telemetry } : {};
    const sessionData = {
      sessionId,
      requestId: id,
      clientId: request.clientId || null,
      clientUid: request.clientUid || null,
      techName: normalizedTechName,
      techId: normalizedTechId,
      techUid: normalizedTechUid,
      techEmail: normalizedTechEmail,
      techPhotoURL: normalizedTechPhotoURL,
      tech: {
        techUid: normalizedTechUid,
        techId: normalizedTechId,
        uid: normalizedTechUid,
        id: normalizedTechId,
        name: normalizedTechName,
        techName: normalizedTechName,
        email: normalizedTechEmail,
        techPhotoURL: normalizedTechPhotoURL,
        photoURL: normalizedTechPhotoURL,
      },
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
        io.to(request.clientId).emit('support:accepted', { sessionId, techName: normalizedTechName });
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
app.delete('/api/requests/:id', requireAuth(['tech']), requireTechAccess, async (req, res) => {
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



app.post('/api/tech/profile-name', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  if (!req.user || typeof req.user !== 'object') {
    return res.status(401).json({ error: 'invalid_token' });
  }

  const uid = ensureString(req.user.uid || '', '');
  const name = ensureString(req.body?.name || '', '');
  if (!uid || !name) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  try {
    const previousName = ensureString(req.techAccess?.techDoc?.name || '', '') || null;
    const historyEntry =
      previousName !== name
        ? buildProfileHistoryEntry({ field: 'name', from: previousName, to: name, source: 'self' })
        : null;

    await db.collection('techs').doc(uid).set(
      {
        name,
        ...(historyEntry
          ? {
              profileHistory: admin.firestore.FieldValue.arrayUnion(historyEntry),
            }
          : {}),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    try {
      await admin.auth().updateUser(uid, { displayName: name });
    } catch (authError) {
      console.warn('Failed to update auth displayName; keeping Firestore profile update', authError);
    }

    return res.json({ ok: true, uid, name });
  } catch (error) {
    console.error('Failed to update tech profile name', error);
    const mappedError = mapFirestoreWriteError(error);
    return res.status(mappedError.status).json({ error: mappedError.error, message: mappedError.message });
  }
});

app.post('/api/tech/profile-photo', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const uid = ensureString(req.user?.uid || '', '');
  const photoURL = ensureString(req.body?.photoURL || '', '');
  if (!uid || !photoURL) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  try {
    const previousPhoto =
      ensureString(req.techAccess?.techDoc?.customPhotoURL || req.techAccess?.techDoc?.photoURL || '', '') || null;
    const historyEntry = buildProfileHistoryEntry({
      field: 'photo',
      from: previousPhoto,
      to: photoURL,
      source: 'self',
    });

    await db.collection('techs').doc(uid).set(
      {
        customPhotoURL: photoURL,
        profileHistory: admin.firestore.FieldValue.arrayUnion(historyEntry),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    return res.json({ ok: true, uid, photoURL });
  } catch (error) {
    console.error('Failed to update tech profile photo', error);
    const mappedError = mapFirestoreWriteError(error);
    return res.status(mappedError.status).json({ error: mappedError.error, message: mappedError.message });
  }
});

app.get('/api/auth/me', requireAuth(), async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  try {
    const decoded = req.user || {};
    console.log('[auth/me] uid:', decoded.uid);
    console.log('[auth/me] role claim:', decoded.role);
    console.log('[auth/me] checking Firestore techs collection...');
    const access = await buildTechAccessPayload(decoded);
    if (!access.ok) {
      if (access.error === 'tech_inactive') {
        return res.status(403).json({ error: 'tech_inactive', message: 'Conta desativada' });
      }
      if (access.error === 'insufficient_role') {
        return res.status(403).json({ error: 'insufficient_role', message: 'Sem permissão' });
      }
      return res.status(access.status || 403).json({ error: access.error || 'not_tech' });
    }
    return res.json({ ...access.payload, supervisor: access.payload.supervisor === true });
  } catch (error) {
    console.error('Failed to get auth profile', error);
    return res.status(500).json({ error: 'server_error' });
  }
});


app.post('/api/admin/bootstrap-supervisor', requireAuth(), async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const secret = ensureString(process.env.SUPERVISOR_BOOTSTRAP_SECRET || '', '');
  const expectedEmail = 'isacxaviersoares@gmail.com';
  const email = ensureString(req.user?.email || '', '').toLowerCase();
  const providedSecret = ensureString(req.body?.secret || '', '');

  if (!secret || providedSecret !== secret) {
    return res.status(403).json({ error: 'invalid_bootstrap_secret' });
  }

  if (email !== expectedEmail) {
    return res.status(403).json({ error: 'supervisor_email_mismatch' });
  }

  try {
    const uid = ensureString(req.user?.uid || '', '');
    if (!uid) {
      return res.status(401).json({ error: 'invalid_token' });
    }

    const userRecord = await admin.auth().getUser(uid);
    const claims = userRecord.customClaims || {};
    if (claims.supervisor === true) {
      return res.json({ ok: true, supervisor: true, alreadyBootstrapped: true });
    }

    await admin.auth().setCustomUserClaims(uid, {
      ...claims,
      role: 'tech',
      supervisor: true,
    });

    await upsertTechDoc({
      uid,
      email: userRecord.email || email,
      name: userRecord.displayName || ensureString(req.user?.name || '', '') || 'Supervisor',
      active: true,
      role: 'tech',
    });

    return res.json({ ok: true, supervisor: true });
  } catch (error) {
    console.error('Failed to bootstrap supervisor', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.get('/api/admin/list-techs', requireAuth(['tech']), requireSupervisor, async (_req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  try {
    const techs = await listTechs();
    const requestUserUid = ensureString(_req.user?.uid || '', '');
    const requestUserEmail = ensureString(_req.user?.email || '', '').trim().toLowerCase() || null;
    const techsWithRequestFallback = techs.map((tech) => {
      if (tech.uid === requestUserUid && !tech.email && requestUserEmail) {
        return { ...tech, email: requestUserEmail };
      }
      return tech;
    });
    return res.json({ techs: techsWithRequestFallback });
  } catch (error) {
    console.error('Failed to list techs', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/admin/create-tech', requireAuth(['tech']), requireSupervisor, async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const email = ensureString(req.body?.email || '', '').toLowerCase();
  const passwordTemp = ensureString(req.body?.passwordTemp || '', '');
  const name = ensureString(req.body?.name || '', '');

  if (!email || !passwordTemp || passwordTemp.length < 6 || !name) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  try {
    const created = await admin.auth().createUser({
      email,
      password: passwordTemp,
      displayName: name,
    });

    await admin.auth().setCustomUserClaims(created.uid, { role: 'tech' });

    await upsertTechDoc({
      uid: created.uid,
      email,
      name,
      active: true,
      role: 'tech',
    });

    return res.status(201).json({ uid: created.uid, email: created.email || email });
  } catch (error) {
    console.error('Failed to create tech', error);
    const mappedError = mapAdminError(error);
    return res.status(mappedError.status).json({ error: mappedError.error, message: mappedError.message });
  }
});

app.post('/api/admin/set-tech-active', requireAuth(['tech']), requireSupervisor, async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const uid = ensureString(req.body?.uid || '', '');
  const active = ensureBoolean(req.body?.active, true);

  if (!uid) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  try {
    const userRecord = await admin.auth().getUser(uid);
    const claims = userRecord.customClaims || {};

    await db.collection('techs').doc(uid).set(
      {
        active,
        role: 'tech',
        email: userRecord.email || null,
        name: userRecord.displayName || null,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    await admin.auth().setCustomUserClaims(uid, {
      ...claims,
      role: 'tech',
      disabled: !active,
    });

    return res.json({ ok: true, uid, active });
  } catch (error) {
    console.error('Failed to set tech active', error);
    return res.status(500).json({ error: 'server_error' });
  }
});


app.post('/api/admin/update-tech', requireAuth(['tech']), requireSupervisor, async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const uid = ensureString(req.body?.uid || '', '');
  const name = ensureString(req.body?.name || '', '');
  const email = ensureString(req.body?.email || '', '').toLowerCase();
  const active = ensureBoolean(req.body?.active, true);
  const role = normalizeRole(req.body?.role || 'tech') === 'supervisor' ? 'supervisor' : 'tech';

  if (!uid || !name) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  try {
    const currentUser = await admin.auth().getUser(uid);
    const resolvedEmail = email || ensureString(currentUser.email || '', '').toLowerCase();
    if (!resolvedEmail) {
      return res.status(400).json({ error: 'invalid_payload', message: 'Email é obrigatório para salvar.' });
    }

    const updatePayload = { displayName: name };
    if (resolvedEmail !== ensureString(currentUser.email || '', '').toLowerCase()) {
      updatePayload.email = resolvedEmail;
    }
    await admin.auth().updateUser(uid, updatePayload);

    const userRecord = await admin.auth().getUser(uid);
    const currentClaims = userRecord.customClaims || {};
    await admin.auth().setCustomUserClaims(uid, {
      ...currentClaims,
      role: 'tech',
      supervisor: role === 'supervisor',
      disabled: !active,
    });

    await db.collection('techs').doc(uid).set(
      {
        uid,
        name,
        email: resolvedEmail,
        active,
        role,
        supervisor: role === 'supervisor',
        photoURL: ensureString(userRecord.photoURL || '', '') || null,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    return res.json({ ok: true, uid });
  } catch (error) {
    console.error('Failed to update tech profile', error);
    const mappedError = mapAdminError(error);
    return res.status(mappedError.status).json({ error: mappedError.error, message: mappedError.message });
  }
});

app.post('/api/tech/reset-my-password', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const uid = ensureString(req.user?.uid || '', '');
  const newPasswordTemp = ensureString(req.body?.newPasswordTemp || '', '');

  if (!uid || !newPasswordTemp || newPasswordTemp.length < 6) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  try {
    await admin.auth().updateUser(uid, { password: newPasswordTemp });
    return res.json({ ok: true, uid });
  } catch (error) {
    console.error('Failed to reset my password', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/admin/reset-tech-password', requireAuth(['tech']), requireSupervisor, async (req, res) => {
  const uid = ensureString(req.body?.uid || '', '');
  const newPasswordTemp = ensureString(req.body?.newPasswordTemp || '', '');

  if (!uid || !newPasswordTemp || newPasswordTemp.length < 6) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  try {
    await admin.auth().updateUser(uid, { password: newPasswordTemp });
    return res.json({ ok: true, uid });
  } catch (error) {
    console.error('Failed to reset tech password', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.delete('/api/admin/delete-tech', requireAuth(['tech']), requireSupervisor, async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const uid = ensureString(req.body?.uid || '', '');

  if (!uid) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  try {
    await admin.auth().deleteUser(uid);
    await db.collection('techs').doc(uid).delete();
    return res.json({ ok: true, uid });
  } catch (error) {
    console.error('Failed to delete tech', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.get('/api/sessions', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const sessionsCollection = getSessionsCollection();
  if (!sessionsCollection) {
    console.error('Firestore not configured. Cannot list sessions.');
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  if (!req.user || typeof req.user !== 'object') {
    return res.status(401).json({ error: 'invalid_token' });
  }

  try {
    const uid = ensureString(req.user.uid || '', '');
    if (!uid) {
      return res.status(401).json({ error: 'invalid_token' });
    }

    const mineOnly = ensureString(req.query.mine || '', '').trim() === '1';

    const assignedRootQuery = sessionsCollection.where('techUid', '==', uid).limit(150);
    const assignedLegacyQuery = sessionsCollection.where('tech.techUid', '==', uid).limit(150);
    const queueQuery = sessionsCollection.where('status', '==', 'queued').limit(150);

    const [assignedRootDocs, assignedLegacyDocs, queueDocs] = await Promise.all([
      safeGetDocs(assignedRootQuery, 'assigned sessions (techUid)'),
      safeGetDocs(assignedLegacyQuery, 'assigned sessions (legacy tech.techUid)'),
      mineOnly ? Promise.resolve([]) : safeGetDocs(queueQuery, 'queued sessions'),
    ]);

    const allDocs = [...queueDocs, ...assignedRootDocs, ...assignedLegacyDocs];
    const uniqueById = new Map();
    allDocs.forEach((doc) => {
      uniqueById.set(doc.id, doc);
    });

    const sessions = await Promise.all(
      Array.from(uniqueById.values()).map((doc) => buildSessionState(doc.id, { snapshot: doc }))
    );
    const sortedSessions = sessions
      .filter(Boolean)
      .sort(
        (a, b) =>
          Number(b.updatedAt || b.acceptedAt || b.createdAt || 0) -
          Number(a.updatedAt || a.acceptedAt || a.createdAt || 0)
      );

    return res.json({ sessions: sortedSessions });
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

app.get('/api/metrics', requireAuth(['tech']), requireTechAccess, async (req, res) => {
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
  console.log('ADMIN PROJECT ID:', admin.app().options.projectId);
  console.log('ENV FIREBASE_PROJECT_ID:', process.env.FIREBASE_PROJECT_ID);
  console.log('ENV GOOGLE_CLOUD_PROJECT:', process.env.GOOGLE_CLOUD_PROJECT);
});
