const path = require('path');
const express = require('express');
const http = require('http');
const https = require('https');
const cors = require('cors');
const multer = require('multer');
const PDFDocument = require('pdfkit');
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

const ensureInteger = (value, fallback = 0) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.trunc(parsed);
};

const DEFAULT_PHONE_COUNTRY_CODE = '55';

const normalizePhone = (value) => {
  const raw = ensureFullString(value || '', '').trim();
  if (!raw) return null;
  const digitsOnly = raw.replace(/\D/g, '');
  if (digitsOnly.length < 10) return null;
  if (raw.startsWith('+')) return `+${digitsOnly}`;
  if (raw.startsWith('00') && digitsOnly.length > 2) return `+${digitsOnly.slice(2)}`;
  if (
    (digitsOnly.length === 10 || digitsOnly.length === 11) &&
    !digitsOnly.startsWith(DEFAULT_PHONE_COUNTRY_CODE)
  ) {
    return `+${DEFAULT_PHONE_COUNTRY_CODE}${digitsOnly}`;
  }
  if (
    (digitsOnly.length === 12 || digitsOnly.length === 13) &&
    digitsOnly.startsWith(DEFAULT_PHONE_COUNTRY_CODE)
  ) {
    return `+${digitsOnly}`;
  }
  return `+${digitsOnly}`;
};

const normalizeDeviceIdentityPart = (value) =>
  ensureFullString(value || '', '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();

const buildDeviceImageCatalogKey = ({ brand = '', model = '' } = {}) => {
  const normalizedBrand = normalizeDeviceIdentityPart(brand).replace(/\s+/g, '-').slice(0, 80);
  const normalizedModel = normalizeDeviceIdentityPart(model).replace(/\s+/g, '-').slice(0, 120);
  if (!normalizedBrand || !normalizedModel) return '';
  return `${normalizedBrand}__${normalizedModel}`;
};

const detectDeviceImageExtension = (mimeType = '', originalName = '') => {
  const normalizedMime = ensureString(mimeType || '', '').toLowerCase().split(';')[0];
  const mimeMap = {
    'image/jpeg': 'jpg',
    'image/jpg': 'jpg',
    'image/png': 'png',
    'image/webp': 'webp',
    'image/gif': 'gif',
    'image/heic': 'heic',
    'image/heif': 'heif',
    'image/bmp': 'bmp',
  };
  if (mimeMap[normalizedMime]) return mimeMap[normalizedMime];

  const ext = path.extname(ensureString(originalName || '', '')).replace('.', '').toLowerCase();
  if (!ext) return '';
  return ext;
};

const clientDocIdFromPhone = (phone) => {
  const normalized = normalizePhone(phone);
  if (!normalized) return null;
  return `phone_${normalized.replace(/\D/g, '')}`;
};

const clientDocIdFromUid = (clientUid) => {
  const normalizedUid = ensureString(clientUid || '', '')
    .trim()
    .replace(/[^a-zA-Z0-9]/g, '');
  if (!normalizedUid) return null;
  return `uid_${normalizedUid}`;
};

const normalizeDeviceAnchor = (value) => {
  const normalized = ensureString(value || '', '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');
  return normalized || null;
};

const linkDocIdFromDeviceAnchor = (deviceAnchor) => {
  const normalizedAnchor = normalizeDeviceAnchor(deviceAnchor);
  if (!normalizedAnchor) return null;
  return `device_${normalizedAnchor}`;
};

const deriveClientStatus = ({ credits = 0, freeFirstSupportUsed = false } = {}) => {
  if (!freeFirstSupportUsed) return 'first_support_pending';
  if ((Number(credits) || 0) > 0) return 'with_credit';
  return 'without_credit';
};

const isClientProfileCompleted = (client = {}) => {
  if (typeof client?.profileCompleted === 'boolean') return client.profileCompleted;
  const hasName = Boolean(ensureString(client?.name || '', '').trim());
  return hasName;
};

const scoreClientSnapshot = (snapshot) => {
  if (!snapshot?.data) return 0;
  const data = snapshot.data;
  const supportsUsed = Math.max(0, ensureInteger(data.supportsUsed, 0));
  const freeFirstSupportUsed = ensureBoolean(data.freeFirstSupportUsed, false);
  const profileCompleted = isClientProfileCompleted(data);
  const updatedAt = Math.max(
    0,
    ensureInteger(data.updatedAt || data.lastSeenAt || data.lastSessionAt || data.createdAt, 0)
  );
  return (
    (freeFirstSupportUsed ? 1000 : 0) +
    supportsUsed * 100 +
    (profileCompleted ? 10 : 0) +
    updatedAt / 1_000_000_000_000
  );
};

const buildClientEligibility = (client = null) => {
  const freeFirstSupportUsed = ensureBoolean(client?.freeFirstSupportUsed, false);
  const credits = Math.max(0, ensureInteger(client?.credits, 0));
  const isFreeFirstSupport = !freeFirstSupportUsed;
  const creditsConsumed = isFreeFirstSupport ? 0 : 1;
  const canRequest = isFreeFirstSupport || credits >= creditsConsumed;
  return {
    canRequest,
    reason: canRequest ? null : 'credit_required',
    isFreeFirstSupport,
    creditsConsumed,
    credits,
  };
};

const normalizeTelemetryData = (value) => {
  const source = value && typeof value === 'object' ? { ...value } : {};
  if (typeof source.network === 'undefined' && typeof source.net !== 'undefined') {
    source.network = source.net;
  }
  if (typeof source.batteryLevel === 'undefined' && typeof source.battery === 'number') {
    source.batteryLevel = source.battery;
  }
  if (typeof source.temperatureC !== 'undefined') {
    const temp = Number(source.temperatureC);
    source.temperatureC = Number.isFinite(temp) ? Number(temp.toFixed(1)) : source.temperatureC;
  }
  if (typeof source.storageFreeBytes !== 'undefined') {
    const valueNum = Number(source.storageFreeBytes);
    source.storageFreeBytes = Number.isFinite(valueNum) ? Math.max(0, Math.trunc(valueNum)) : source.storageFreeBytes;
  }
  if (typeof source.storageTotalBytes !== 'undefined') {
    const valueNum = Number(source.storageTotalBytes);
    source.storageTotalBytes = Number.isFinite(valueNum) ? Math.max(0, Math.trunc(valueNum)) : source.storageTotalBytes;
  }
  return source;
};

const sanitizeSupportProfile = (value) => {
  if (!value || typeof value !== 'object') return {};
  const source = value;
  const localSupportSessionId = ensureString(
    source.localSupportSessionId || source.supportSessionId || source.localSessionId || '',
    ''
  )
    .trim()
    .slice(0, 128);
  const output = {
    isNewClient: ensureBoolean(source.isNewClient, false),
    isFreeFirstSupport: ensureBoolean(source.isFreeFirstSupport, false),
    creditsToConsume: Math.max(0, ensureInteger(source.creditsToConsume, 0)),
    disableQuickIdentificationModal: ensureBoolean(source.disableQuickIdentificationModal, false),
    technicianDrivenRegistrationEnabled: ensureBoolean(source.technicianDrivenRegistrationEnabled, false),
    pnvPostRegistrationFlow: ensureBoolean(source.pnvPostRegistrationFlow, false),
  };
  if (localSupportSessionId) {
    output.localSupportSessionId = localSupportSessionId;
  }
  return output;
};

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
  const lowerMessage = message.toLowerCase();
  if (message.includes('FieldValue.serverTimestamp() cannot be used inside of an array')) {
    return {
      status: 400,
      error: 'invalid_history_timestamp',
      message: 'Não foi possível salvar o histórico: timestamp inválido em item de lista.',
    };
  }
  if (lowerMessage.includes('cannot use arrayunion') || lowerMessage.includes('non-array field')) {
    return {
      status: 400,
      error: 'invalid_history_format',
      message: 'Histórico do cliente em formato inválido. Corrija o campo e tente novamente.',
    };
  }
  if (lowerMessage.includes('permission denied') || lowerMessage.includes('insufficient permissions')) {
    return {
      status: 503,
      error: 'firestore_permission_denied',
      message: 'Sem permissao para gravar no Firestore no ambiente atual.',
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

const DEFAULT_TECH_LOGIN_RECAPTCHA_SITE_KEY = '6LciOHosAAAAAOc2JgTXnnt2x2XsPyXCBI6stIGm';
const DEFAULT_TECH_LOGIN_RECAPTCHA_ALLOWED_HOSTNAMES = ['suportex.app', 'www.suportex.app', 'localhost', '127.0.0.1'];
const RECAPTCHA_ENTERPRISE_BASE_URL = 'https://recaptchaenterprise.googleapis.com/v1';
const RECAPTCHA_VERIFY_TIMEOUT_MS = 8000;

const resolveTechLoginRecaptchaConfig = () => {
  const siteKey = ensureString(
    process.env.TECH_LOGIN_RECAPTCHA_SITE_KEY ||
      process.env.RECAPTCHA_TECH_LOGIN_SITE_KEY ||
      process.env.RECAPTCHA_SITE_KEY ||
      DEFAULT_TECH_LOGIN_RECAPTCHA_SITE_KEY,
    ''
  ).trim();
  const rawEnabled = ensureString(
    process.env.TECH_LOGIN_RECAPTCHA_ENABLED || process.env.RECAPTCHA_TECH_LOGIN_ENABLED || '',
    ''
  )
    .trim()
    .toLowerCase();
  const enabledByDefault = Boolean(siteKey);
  const enabled =
    rawEnabled === ''
      ? enabledByDefault
      : rawEnabled === '1' || rawEnabled === 'true' || rawEnabled === 'yes' || rawEnabled === 'on';

  const projectId = ensureString(
    process.env.RECAPTCHA_ENTERPRISE_PROJECT_ID ||
      process.env.GOOGLE_CLOUD_PROJECT ||
      process.env.GCP_PROJECT_ID ||
      process.env.FIREBASE_PROJECT_ID ||
      firebaseProjectId ||
      '',
    ''
  ).trim();

  const rawMinScore = Number(
    ensureString(
      process.env.TECH_LOGIN_RECAPTCHA_MIN_SCORE || process.env.RECAPTCHA_TECH_LOGIN_MIN_SCORE || '0',
      '0'
    )
  );
  const minScore = Number.isFinite(rawMinScore) ? Math.max(0, Math.min(1, rawMinScore)) : 0;

  const rawHostnames = ensureString(
    process.env.TECH_LOGIN_RECAPTCHA_ALLOWED_HOSTNAMES || process.env.RECAPTCHA_TECH_LOGIN_ALLOWED_HOSTNAMES || '',
    ''
  )
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);
  const allowedHostnames = rawHostnames.length
    ? Array.from(new Set(rawHostnames))
    : [...DEFAULT_TECH_LOGIN_RECAPTCHA_ALLOWED_HOSTNAMES];

  return {
    enabled: enabled && Boolean(siteKey),
    siteKey,
    projectId,
    minScore,
    allowedHostnames,
  };
};

const getTechLoginRecaptchaPublicConfig = () => {
  const config = resolveTechLoginRecaptchaConfig();
  if (!config.enabled) {
    return {
      enabled: false,
      provider: 'recaptcha_enterprise',
      siteKey: '',
    };
  }

  return {
    enabled: true,
    provider: 'recaptcha_enterprise',
    siteKey: config.siteKey,
  };
};

const resolveRequestIpAddress = (req) => {
  const forwardedFor = ensureFullString(req.headers?.['x-forwarded-for'] || '', '')
    .split(',')
    .map((value) => value.trim())
    .find(Boolean);
  return ensureString(forwardedFor || req.ip || '', '').trim();
};

const resolveFirebaseAdminAccessToken = async () => {
  if (!admin.apps || admin.apps.length === 0) {
    throw new Error('firebase_admin_not_initialized');
  }

  const credential = admin.app().options?.credential;
  if (!credential || typeof credential.getAccessToken !== 'function') {
    throw new Error('firebase_admin_credential_missing');
  }

  const tokenResponse = await credential.getAccessToken();
  if (typeof tokenResponse === 'string') {
    const directToken = ensureFullString(tokenResponse || '', '').trim();
    if (directToken) return directToken;
  }

  const accessToken = ensureFullString(tokenResponse?.access_token || tokenResponse?.accessToken || '', '').trim();
  if (!accessToken) {
    throw new Error('firebase_admin_access_token_missing');
  }
  return accessToken;
};

const postJson = ({ url, headers = {}, body = null, timeoutMs = 8000 }) =>
  new Promise((resolve, reject) => {
    let parsedUrl;
    try {
      parsedUrl = new URL(url);
    } catch (error) {
      reject(error);
      return;
    }

    const req = https.request(
      {
        protocol: parsedUrl.protocol,
        hostname: parsedUrl.hostname,
        port: parsedUrl.port || 443,
        path: `${parsedUrl.pathname}${parsedUrl.search}`,
        method: 'POST',
        headers,
      },
      (res) => {
        let raw = '';
        res.setEncoding('utf8');
        res.on('data', (chunk) => {
          raw += chunk;
        });
        res.on('end', () => {
          resolve({
            ok: res.statusCode >= 200 && res.statusCode < 300,
            status: res.statusCode || 0,
            statusText: ensureString(res.statusMessage || '', ''),
            text: raw,
          });
        });
      }
    );

    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error('recaptcha_request_timeout'));
    });

    req.on('error', (error) => reject(error));

    if (body) {
      req.write(body);
    }
    req.end();
  });

const postJsonWithRuntimeFallback = async ({ url, headers = {}, body = null, timeoutMs = 8000 }) => {
  if (typeof fetch === 'function') {
    const timeoutController = new AbortController();
    const timeoutId = setTimeout(() => timeoutController.abort(), timeoutMs);
    try {
      const response = await fetch(url, {
        method: 'POST',
        headers,
        body,
        signal: timeoutController.signal,
      });
      const text = await response.text();
      return {
        ok: response.ok,
        status: response.status,
        statusText: ensureString(response.statusText || '', ''),
        text,
      };
    } finally {
      clearTimeout(timeoutId);
    }
  }

  return postJson({ url, headers, body, timeoutMs });
};

const mapRecaptchaRuntimeError = (error) => {
  const rawMessage = ensureFullString(error?.message || '', '');
  const message = rawMessage.toLowerCase();

  if (!message) {
    return {
      error: 'captcha_verification_failed',
      message: 'N\u00E3o foi poss\u00EDvel validar o reCAPTCHA agora. Tente novamente em instantes.',
      hint: null,
    };
  }

  if (message.includes('recaptcha_request_timeout') || message.includes('timed out') || message.includes('timeout')) {
    return {
      error: 'captcha_timeout',
      message: 'A valida\u00E7\u00E3o anti-bot excedeu o tempo limite. Tente novamente.',
      hint: 'timeout',
    };
  }

  if (message.includes('permission') || message.includes('insufficient permission') || message.includes('permission denied')) {
    return {
      error: 'captcha_permission_denied',
      message: 'Sem permiss\u00E3o para validar reCAPTCHA Enterprise com a credencial atual do servidor.',
      hint: 'grant_recaptcha_enterprise_agent_role',
    };
  }

  if (
    message.includes('api has not been used') ||
    message.includes('api is not enabled') ||
    message.includes('service disabled') ||
    message.includes('accessnotconfigured')
  ) {
    return {
      error: 'captcha_api_disabled',
      message: 'A API reCAPTCHA Enterprise n\u00E3o est\u00E1 habilitada para o projeto configurado no servidor.',
      hint: 'enable_recaptcha_enterprise_api',
    };
  }

  if (message.includes('project') && message.includes('not found')) {
    return {
      error: 'captcha_project_not_found',
      message: 'Projeto de reCAPTCHA Enterprise n\u00E3o encontrado para a configura\u00E7\u00E3o atual.',
      hint: 'check_project_id',
    };
  }

  if (message.includes('site key') && message.includes('mismatch')) {
    return {
      error: 'captcha_sitekey_mismatch',
      message: 'A site key informada n\u00E3o corresponde ao projeto do reCAPTCHA Enterprise.',
      hint: 'check_site_key_project_binding',
    };
  }

  if (message.includes('fetch is not defined')) {
    return {
      error: 'captcha_runtime_missing_fetch',
      message: 'Runtime do servidor sem suporte a fetch para valida\u00E7\u00E3o anti-bot.',
      hint: 'runtime_upgrade_or_https_fallback',
    };
  }

  return {
    error: 'captcha_verification_failed',
    message: 'N\u00E3o foi poss\u00EDvel validar o reCAPTCHA agora. Tente novamente em instantes.',
    hint: null,
  };
};

const verifyTechLoginRecaptchaToken = async ({ token = '', userAgent = '', remoteIpAddress = '', isProduction = false }) => {
  const config = resolveTechLoginRecaptchaConfig();
  if (!config.enabled) {
    return { ok: false, status: 503, error: 'captcha_unavailable' };
  }
  if (!config.projectId) {
    return { ok: false, status: 503, error: 'captcha_project_missing' };
  }

  const accessToken = await resolveFirebaseAdminAccessToken();
  const endpoint = `${RECAPTCHA_ENTERPRISE_BASE_URL}/projects/${encodeURIComponent(config.projectId)}/assessments`;

  const requestBody = {
    event: {
      token,
      siteKey: config.siteKey,
    },
  };
  if (remoteIpAddress) requestBody.event.userIpAddress = remoteIpAddress;
  if (userAgent) requestBody.event.userAgent = userAgent;

  const rawBody = JSON.stringify(requestBody);
  let response;
  let payload = {};
  response = await postJsonWithRuntimeFallback({
    url: endpoint,
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(rawBody, 'utf8'),
    },
    body: rawBody,
    timeoutMs: RECAPTCHA_VERIFY_TIMEOUT_MS,
  });
  try {
    payload = JSON.parse(ensureFullString(response.text || '', '{}') || '{}');
  } catch (_error) {
    payload = {};
  }

  if (!response.ok) {
    const responseMessage = ensureString(payload?.error?.message || '', '').trim();
    const statusText = ensureString(response.statusText || '', '').trim();
    const fallbackMessage = `recaptcha_enterprise_http_${response.status}`;
    throw new Error(responseMessage || statusText || fallbackMessage);
  }

  const tokenProperties = payload?.tokenProperties || {};
  if (tokenProperties.valid !== true) {
    return {
      ok: false,
      status: 403,
      error: 'captcha_invalid',
      invalidReason: ensureString(tokenProperties.invalidReason || '', '').trim() || 'invalid',
    };
  }

  const hostname = ensureString(tokenProperties.hostname || '', '').trim().toLowerCase();
  if (isProduction && hostname && config.allowedHostnames.length && !config.allowedHostnames.includes(hostname)) {
    return {
      ok: false,
      status: 403,
      error: 'captcha_hostname_mismatch',
      hostname,
    };
  }

  const riskScoreValue = Number(payload?.riskAnalysis?.score);
  const riskScore = Number.isFinite(riskScoreValue) ? riskScoreValue : null;
  if (config.minScore > 0 && Number.isFinite(riskScore) && riskScore < config.minScore) {
    return {
      ok: false,
      status: 403,
      error: 'captcha_low_score',
      score: riskScore,
      minScore: config.minScore,
    };
  }

  return { ok: true, score: riskScore };
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

const getClientsCollection = () => {
  if (!db) return null;
  try {
    return db.collection('clients');
  } catch (err) {
    console.error('Failed to access clients collection', err);
    return null;
  }
};

const getClientProfilesCollection = () => {
  if (!db) return null;
  try {
    return db.collection('client_profiles');
  } catch (err) {
    console.error('Failed to access client_profiles collection', err);
    return null;
  }
};

const getClientAppLinksCollection = () => {
  if (!db) return null;
  try {
    return db.collection('client_app_links');
  } catch (err) {
    console.error('Failed to access client_app_links collection', err);
    return null;
  }
};

const getClientVerificationsCollection = () => {
  if (!db) return null;
  try {
    return db.collection('client_verifications');
  } catch (err) {
    console.error('Failed to access client_verifications collection', err);
    return null;
  }
};

const getPnvRequestsCollection = () => {
  if (!db) return null;
  try {
    return db.collection('pnv_requests');
  } catch (err) {
    console.error('Failed to access pnv_requests collection', err);
    return null;
  }
};

const getSupportSessionsCollection = () => {
  if (!db) return null;
  try {
    return db.collection('support_sessions');
  } catch (err) {
    console.error('Failed to access support_sessions collection', err);
    return null;
  }
};

const getDeviceImagesCollection = () => {
  if (!db) return null;
  try {
    return db.collection('device_images');
  } catch (err) {
    console.error('Failed to access device_images collection', err);
    return null;
  }
};

const toClientSummary = (id, data = {}) => ({
  id,
  name: ensureString(data.name || '', '').trim() || null,
  phone: normalizePhone(data.phone) || null,
  primaryEmail: ensureString(data.primaryEmail || '', '').trim() || null,
  notes: ensureLongString(data.notes || '', '', 4000) || null,
  credits: Math.max(0, ensureInteger(data.credits, 0)),
  supportsUsed: Math.max(0, ensureInteger(data.supportsUsed, 0)),
  freeFirstSupportUsed: ensureBoolean(data.freeFirstSupportUsed, false),
  profileCompleted: ensureBoolean(data.profileCompleted, false),
  status:
    ensureString(data.status || '', '').trim() ||
    deriveClientStatus({
      credits: data.credits,
      freeFirstSupportUsed: data.freeFirstSupportUsed,
    }),
  createdAt: data.createdAt || null,
  updatedAt: data.updatedAt || null,
  createdByTechUid: ensureString(data.createdByTechUid || '', '').trim() || null,
  createdByTechName: ensureString(data.createdByTechName || '', '').trim() || null,
  createdByTechEmail: ensureString(data.createdByTechEmail || '', '').trim().toLowerCase() || null,
});

const resolveClientContext = async ({
  clientRecordId = '',
  clientUid = '',
  phone = '',
  deviceAnchor = '',
} = {}) => {
  const clientsCollection = getClientsCollection();
  const profilesCollection = getClientProfilesCollection();
  const linksCollection = getClientAppLinksCollection();
  const verificationsCollection = getClientVerificationsCollection();
  if (!clientsCollection) {
    return {
      client: null,
      profile: null,
      verification: null,
      normalizedPhone: normalizePhone(phone),
      resolvedClientId: null,
      resolvedBy: null,
    };
  }

  const normalizedPhone = normalizePhone(phone);
  const requestedClientId = ensureString(clientRecordId || '', '').trim();
  const normalizedClientUid = ensureString(clientUid || '', '').trim();
  const normalizedDeviceAnchor = normalizeDeviceAnchor(deviceAnchor);
  let resolvedBy = null;
  let clientDoc = null;

  const tryLoadClient = async (id, source) => {
    if (!id || clientDoc) return;
    try {
      const snap = await clientsCollection.doc(id).get();
      if (snap.exists) {
        clientDoc = snap;
        resolvedBy = source;
      }
    } catch (error) {
      console.error(`Failed to fetch client by ${source}`, error);
    }
  };

  await tryLoadClient(requestedClientId, 'clientId');

  if (!clientDoc) {
    const preferredClientId = await resolvePreferredClientRecordId({
      normalizedPhone,
      normalizedClientUid,
      normalizedDeviceAnchor,
      clientsCollection,
      linksCollection,
      verificationsCollection,
    });
    await tryLoadClient(preferredClientId || '', 'preferred');
  }

  let client = null;
  let profile = null;
  let verification = null;

  if (clientDoc?.exists) {
    client = toClientSummary(clientDoc.id, clientDoc.data() || {});

    if (profilesCollection) {
      try {
        const profileSnap = await profilesCollection.doc(clientDoc.id).get();
        if (profileSnap.exists) {
          const data = profileSnap.data() || {};
          profile = {
            clientId: clientDoc.id,
            totalSessions: Math.max(0, ensureInteger(data.totalSessions, 0)),
            totalPaidSessions: Math.max(0, ensureInteger(data.totalPaidSessions, 0)),
            totalFreeSessions: Math.max(0, ensureInteger(data.totalFreeSessions, 0)),
            totalCreditsPurchased: Math.max(0, ensureInteger(data.totalCreditsPurchased, 0)),
            totalCreditsUsed: Math.max(0, ensureInteger(data.totalCreditsUsed, 0)),
            lastSupportAt: data.lastSupportAt || null,
            updatedAt: data.updatedAt || null,
          };
        }
      } catch (error) {
        console.error('Failed to fetch client profile', error);
      }
    }

    if (verificationsCollection) {
      try {
        const verificationSnap = await verificationsCollection.doc(clientDoc.id).get();
        if (verificationSnap.exists) {
          const data = verificationSnap.data() || {};
          verification = {
            clientId: clientDoc.id,
            status: ensureString(data.status || '', '').trim().toLowerCase() || 'pending',
            primaryPhone: normalizePhone(data.primaryPhone) || null,
            verifiedPhone: normalizePhone(data.verifiedPhone) || null,
            mismatchReason: ensureLongString(data.mismatchReason || '', '', 1000) || null,
            source: ensureString(data.source || '', '').trim() || null,
            lastTriggerAt: data.lastTriggerAt || null,
            lastVerificationAt: data.lastVerificationAt || null,
            updatedAt: data.updatedAt || null,
          };
        }
      } catch (error) {
        console.error('Failed to fetch client verification', error);
      }
    }
  }

  return {
    client,
    profile,
    verification,
    normalizedPhone,
    resolvedClientId: client?.id || null,
    resolvedBy,
  };
};

const resolvePreferredClientRecordId = async ({
  normalizedPhone = null,
  normalizedClientUid = null,
  normalizedDeviceAnchor = null,
  clientsCollection = null,
  linksCollection = null,
  verificationsCollection = null,
} = {}) => {
  if (!clientsCollection) return null;
  const phoneDocId = normalizedPhone ? clientDocIdFromPhone(normalizedPhone) : null;
  const uidDocId = normalizedClientUid ? clientDocIdFromUid(normalizedClientUid) : null;
  const deviceLinkDocId = normalizedDeviceAnchor ? linkDocIdFromDeviceAnchor(normalizedDeviceAnchor) : null;
  let linkedClientId = null;
  let linkedDeviceClientId = null;
  const verificationClientIds = [];

  const loadClientSnapshot = async (candidateId) => {
    const normalizedCandidate = ensureString(candidateId || '', '').trim();
    if (!normalizedCandidate) return null;
    try {
      const snap = await clientsCollection.doc(normalizedCandidate).get();
      if (!snap.exists) return null;
      return {
        id: normalizedCandidate,
        data: snap.data() || {},
      };
    } catch (error) {
      console.error('Failed to resolve preferred client record', error);
      return null;
    }
  };

  if (normalizedClientUid && linksCollection) {
    try {
      const linkSnap = await linksCollection.doc(normalizedClientUid).get();
      if (linkSnap.exists) {
        linkedClientId = ensureString(linkSnap.data()?.clientId || '', '').trim() || null;
      }
    } catch (error) {
      console.error('Failed to resolve linked client record by uid', error);
    }
  }

  if (deviceLinkDocId && linksCollection) {
    try {
      const deviceLinkSnap = await linksCollection.doc(deviceLinkDocId).get();
      if (deviceLinkSnap.exists) {
        linkedDeviceClientId = ensureString(deviceLinkSnap.data()?.clientId || '', '').trim() || null;
      }
    } catch (error) {
      console.error('Failed to resolve linked client record by device anchor', error);
    }
  }

  if (normalizedPhone && verificationsCollection) {
    const appendVerificationCandidates = (docs = []) => {
      docs.forEach((item) => {
        const clientId = ensureString(item.data()?.clientId || '', '').trim();
        if (clientId) verificationClientIds.push(clientId);
      });
    };
    try {
      const verifiedMatches = await verificationsCollection.where('verifiedPhone', '==', normalizedPhone).limit(20).get();
      appendVerificationCandidates(verifiedMatches.docs || []);
    } catch (error) {
      console.error('Failed to resolve preferred client by verifiedPhone', error);
    }
    try {
      const primaryMatches = await verificationsCollection.where('primaryPhone', '==', normalizedPhone).limit(20).get();
      appendVerificationCandidates(primaryMatches.docs || []);
    } catch (error) {
      console.error('Failed to resolve preferred client by primaryPhone', error);
    }
  }

  const candidateIds = [linkedClientId, linkedDeviceClientId, ...verificationClientIds, uidDocId, phoneDocId]
    .map((value) => ensureString(value || '', '').trim())
    .filter(Boolean)
    .filter((value, index, array) => array.indexOf(value) === index);

  const snapshots = [];
  for (const candidateId of candidateIds) {
    const snapshot = await loadClientSnapshot(candidateId);
    if (snapshot) snapshots.push(snapshot);
  }
  if (snapshots.length) {
    snapshots.sort((a, b) => scoreClientSnapshot(b) - scoreClientSnapshot(a));
    return snapshots[0].id;
  }
  if (linkedClientId) return linkedClientId;
  if (linkedDeviceClientId) return linkedDeviceClientId;
  if (verificationClientIds.length) return verificationClientIds[0];

  return phoneDocId || uidDocId || null;
};

const ensureClientIdentityFromPhone = async ({
  normalizedPhone = null,
  clientUid = '',
  deviceAnchor = '',
  clientName = '',
  source = 'support_request',
} = {}) => {
  if (!db) return null;
  const clientsCollection = getClientsCollection();
  if (!clientsCollection) return null;
  const profilesCollection = getClientProfilesCollection();
  const linksCollection = getClientAppLinksCollection();
  const phone = normalizePhone(normalizedPhone);
  const normalizedClientUid = ensureString(clientUid || '', '').trim() || null;
  const normalizedDeviceAnchor = normalizeDeviceAnchor(deviceAnchor);
  const clientId = await resolvePreferredClientRecordId({
    normalizedPhone: phone,
    normalizedClientUid,
    normalizedDeviceAnchor,
    clientsCollection,
    linksCollection,
    verificationsCollection: getClientVerificationsCollection(),
  });
  if (!clientId) return null;

  const normalizedClientName = ensureString(clientName || '', '').trim() || null;
  const now = Date.now();

  await db.runTransaction(async (tx) => {
    const clientRef = clientsCollection.doc(clientId);
    const clientSnap = await tx.get(clientRef);
    const oldData = clientSnap.exists ? clientSnap.data() || {} : {};

    const credits = Math.max(0, ensureInteger(oldData.credits, 0));
    const supportsUsed = Math.max(0, ensureInteger(oldData.supportsUsed, 0));
    const freeFirstSupportUsed = ensureBoolean(oldData.freeFirstSupportUsed, false);
    const profileCompleted = ensureBoolean(oldData.profileCompleted, false);
    const status = deriveClientStatus({ credits, freeFirstSupportUsed });

    tx.set(
      clientRef,
      {
        phone: phone || normalizePhone(oldData.phone || '') || null,
        name: oldData.name || normalizedClientName || null,
        primaryEmail: oldData.primaryEmail || null,
        notes: oldData.notes || null,
        credits,
        supportsUsed,
        freeFirstSupportUsed,
        profileCompleted,
        status,
        createdAt: oldData.createdAt || now,
        updatedAt: now,
        source: oldData.source || source,
        lastSeenAt: now,
      },
      { merge: true }
    );

    if (!clientSnap.exists && profilesCollection) {
      const profileRef = profilesCollection.doc(clientId);
      tx.set(
        profileRef,
        {
          clientId,
          totalSessions: 0,
          totalPaidSessions: 0,
          totalFreeSessions: 0,
          totalCreditsPurchased: 0,
          totalCreditsUsed: 0,
          lastSupportAt: null,
          createdAt: now,
          updatedAt: now,
        },
        { merge: true }
      );
    }
  });

  if (linksCollection && (normalizedClientUid || normalizedDeviceAnchor)) {
    const baseLinkPayload = {
      clientUid: normalizedClientUid,
      clientId,
      phone: phone || null,
      deviceAnchor: normalizedDeviceAnchor,
      createdAt: now,
      updatedAt: now,
    };
    if (normalizedClientUid) {
      await linksCollection.doc(normalizedClientUid).set(baseLinkPayload, { merge: true });
    }
    const deviceLinkDocId = normalizedDeviceAnchor ? linkDocIdFromDeviceAnchor(normalizedDeviceAnchor) : null;
    if (deviceLinkDocId) {
      await linksCollection.doc(deviceLinkDocId).set(
        {
          ...baseLinkPayload,
          linkType: 'device',
        },
        { merge: true }
      );
    }
  }

  return resolveClientContext({
    clientRecordId: clientId,
    clientUid: normalizedClientUid,
    phone,
    deviceAnchor: normalizedDeviceAnchor,
  });
};

const resolveClientLinkInfoByClientId = async (clientId = '') => {
  const normalizedClientId = ensureString(clientId || '', '').trim().slice(0, 128);
  const clientsCollection = getClientsCollection();
  const linksCollection = getClientAppLinksCollection();
  if (!normalizedClientId || !clientsCollection) {
    return {
      clientUid: null,
      phone: null,
    };
  }

  let phone = null;
  let clientUid = null;

  try {
    const snap = await clientsCollection.doc(normalizedClientId).get();
    if (snap.exists) {
      phone = normalizePhone(snap.data()?.phone || '') || null;
    }
  } catch (error) {
    console.error('Failed to resolve client phone by clientId', error);
  }

  if (linksCollection) {
    try {
      const linkDocs = await safeGetDocs(
        linksCollection.where('clientId', '==', normalizedClientId).limit(1),
        'client_app_links by clientId'
      );
      const firstLink = linkDocs[0];
      if (firstLink) {
        const linkData = firstLink.data() || {};
        clientUid =
          ensureString(linkData.clientUid || firstLink.id || '', '').trim() ||
          null;
      }
    } catch (error) {
      console.error('Failed to resolve client link by clientId', error);
    }
  }

  return {
    clientUid,
    phone,
  };
};

const applyClientConsumptionOnAccept = async ({
  clientId = '',
  isFreeFirstSupport = false,
  creditsConsumed = 0,
  now = Date.now(),
} = {}) => {
  if (!db) return;
  const normalizedClientId = ensureString(clientId || '', '').trim();
  if (!normalizedClientId) return;
  const clientsCollection = getClientsCollection();
  if (!clientsCollection) return;
  const profilesCollection = getClientProfilesCollection();

  await db.runTransaction(async (tx) => {
    const clientRef = clientsCollection.doc(normalizedClientId);
    const profileRef = profilesCollection ? profilesCollection.doc(normalizedClientId) : null;
    const clientSnap = await tx.get(clientRef);
    const profileSnap = profileRef ? await tx.get(profileRef) : null;
    if (!clientSnap.exists) return;
    const oldData = clientSnap.data() || {};
    const oldCredits = Math.max(0, ensureInteger(oldData.credits, 0));
    const oldSupportsUsed = Math.max(0, ensureInteger(oldData.supportsUsed, 0));
    const oldFreeUsed = ensureBoolean(oldData.freeFirstSupportUsed, false);
    const profileCompleted = ensureBoolean(oldData.profileCompleted, false);
    const profileData = profileSnap?.exists ? profileSnap.data() || {} : {};

    const safeCreditsConsumed = Math.max(0, ensureInteger(creditsConsumed, 0));
    const nextFreeUsed = oldFreeUsed || ensureBoolean(isFreeFirstSupport, false);
    const nextCredits = isFreeFirstSupport
      ? oldCredits
      : Math.max(0, oldCredits - safeCreditsConsumed);
    const nextSupportsUsed = oldSupportsUsed + 1;
    const nextStatus = deriveClientStatus({
      credits: nextCredits,
      freeFirstSupportUsed: nextFreeUsed,
    });

    tx.set(
      clientRef,
      {
        credits: nextCredits,
        supportsUsed: nextSupportsUsed,
        freeFirstSupportUsed: nextFreeUsed,
        profileCompleted,
        status: nextStatus,
        updatedAt: now,
        lastSessionAt: now,
        lastSeenAt: now,
      },
      { merge: true }
    );

    if (!profileRef) return;
    const totalSessions = Math.max(0, ensureInteger(profileData.totalSessions, 0)) + 1;
    const totalPaidSessions =
      Math.max(0, ensureInteger(profileData.totalPaidSessions, 0)) + (isFreeFirstSupport ? 0 : 1);
    const totalFreeSessions =
      Math.max(0, ensureInteger(profileData.totalFreeSessions, 0)) + (isFreeFirstSupport ? 1 : 0);
    const totalCreditsPurchased = Math.max(0, ensureInteger(profileData.totalCreditsPurchased, 0));
    const totalCreditsUsed = Math.max(0, ensureInteger(profileData.totalCreditsUsed, 0)) + safeCreditsConsumed;

    tx.set(
      profileRef,
      {
        clientId: normalizedClientId,
        totalSessions,
        totalPaidSessions,
        totalFreeSessions,
        totalCreditsPurchased,
        totalCreditsUsed,
        lastSupportAt: now,
        updatedAt: now,
      },
      { merge: true }
    );
  });
};

const toPublicSessionSummary = (id, data = {}) => ({
  sessionId: id,
  requestId: ensureString(data.requestId || '', '').trim() || null,
  status: ensureString(data.status || '', '').trim() || null,
  clientName: ensureString(data.clientName || '', '').trim() || null,
  clientUid: ensureString(data.clientUid || '', '').trim() || null,
  clientPhone: normalizePhone(data.clientPhone) || null,
  clientRecordId: ensureString(data.clientRecordId || '', '').trim() || null,
  createdAt: data.createdAt || null,
  requestedAt: data.requestedAt || null,
  acceptedAt: data.acceptedAt || null,
  closedAt: data.closedAt || null,
  supportSessionId: ensureString(data.supportSessionId || '', '').trim() || null,
  requiresTechnicianRegistration: ensureBoolean(data.requiresTechnicianRegistration, false),
});

const buildClientContextPayload = async ({
  sessionId = '',
  requestId = '',
  clientRecordId = '',
  clientUid = '',
  phone = '',
  deviceAnchor = '',
} = {}) => {
  const sessionsCollection = getSessionsCollection();
  const requestsCollection = getRequestsCollection();
  const supportSessionsCollection = getSupportSessionsCollection();

  let sessionData = null;
  let requestData = null;
  const normalizedSessionId = normalizeSessionId(sessionId);
  const normalizedRequestId = ensureString(requestId || '', '').trim().slice(0, 64);

  if (normalizedSessionId && sessionsCollection) {
    try {
      const snap = await sessionsCollection.doc(normalizedSessionId).get();
      if (snap.exists) {
        sessionData = { id: snap.id, ...(snap.data() || {}) };
      }
    } catch (error) {
      console.error('Failed to fetch session for client context', error);
    }
  }

  if (normalizedRequestId && requestsCollection) {
    try {
      const snap = await requestsCollection.doc(normalizedRequestId).get();
      if (snap.exists) {
        requestData = { id: snap.id, ...(snap.data() || {}) };
      }
    } catch (error) {
      console.error('Failed to fetch request for client context', error);
    }
  }

  const resolvedDeviceAnchor = normalizeDeviceAnchor(
    deviceAnchor ||
      sessionData?.deviceAnchor ||
      sessionData?.device?.anchor ||
      requestData?.deviceAnchor ||
      requestData?.device?.anchor ||
      ''
  );

  const resolvedClientContext = await resolveClientContext({
    clientRecordId:
      ensureString(
        clientRecordId ||
          sessionData?.clientRecordId ||
          requestData?.clientRecordId ||
          '',
        ''
      ).trim(),
    clientUid:
      ensureString(
        clientUid ||
          sessionData?.clientUid ||
          requestData?.clientUid ||
          '',
        ''
      ).trim(),
    phone:
      normalizePhone(phone) ||
      normalizePhone(sessionData?.clientPhone) ||
      normalizePhone(requestData?.clientPhone) ||
      null,
    deviceAnchor: resolvedDeviceAnchor,
  });

  let recentSupportSessions = [];
  if (resolvedClientContext.client?.id) {
    const clientId = resolvedClientContext.client.id;
    const recentFromLiveSessions = [];

    if (sessionsCollection) {
      try {
        const liveSessionDocs = await safeGetDocs(
          sessionsCollection.where('clientRecordId', '==', clientId).limit(60),
          'live sessions by client'
        );
        recentFromLiveSessions.push(
          ...liveSessionDocs.map((doc) => {
            const session = { id: doc.id, ...(doc.data() || {}) };
            return {
              id: session.id,
              status: ensureString(session.status || '', '').trim() || null,
              startedAt: session.acceptedAt || session.startedAt || session.createdAt || null,
              endedAt: session.closedAt || session.endedAt || null,
              problemSummary: ensureLongString(session.problemSummary || session.issue || session.symptom || '', '', 1000) || null,
              solutionSummary: ensureLongString(session.solutionSummary || session.solution || '', '', 1000) || null,
              symptom: ensureLongString(session.symptom || session.issue || '', '', 1000) || null,
              solution: ensureLongString(session.solution || '', '', 1000) || null,
              outcome: ensureString(session.outcome || '', '').trim() || null,
              internalNotes: ensureLongString(session.internalNotes || session.notes || '', '', 1000) || null,
              techName: ensureString(session.techName || '', '').trim() || null,
              creditsConsumed: Math.max(0, ensureInteger(session.creditsConsumed, 0)),
              isFreeFirstSupport: ensureBoolean(session.isFreeFirstSupport, false),
              updatedAt:
                session.updatedAt ||
                session.closedAt ||
                session.acceptedAt ||
                session.createdAt ||
                null,
            };
          })
        );
      } catch (error) {
        console.error('Failed to fetch live sessions for client context', error);
      }
    }

    if (recentFromLiveSessions.length) {
      recentSupportSessions = recentFromLiveSessions
        .sort(
          (a, b) =>
            parseReportTimestamp(b.updatedAt || b.endedAt || b.startedAt || 0, 0) -
            parseReportTimestamp(a.updatedAt || a.endedAt || a.startedAt || 0, 0)
        )
        .slice(0, 12)
        .map(({ updatedAt, ...session }) => session);
    } else if (supportSessionsCollection) {
      try {
        const supportDocs = await safeGetDocs(
          supportSessionsCollection.where('clientId', '==', clientId).limit(40),
          'support sessions by client'
        );
        recentSupportSessions = supportDocs
          .map((doc) => ({ id: doc.id, ...(doc.data() || {}) }))
          .sort(
            (a, b) =>
              parseReportTimestamp(b.updatedAt || b.startedAt || b.createdAt || 0, 0) -
              parseReportTimestamp(a.updatedAt || a.startedAt || a.createdAt || 0, 0)
          )
          .slice(0, 12)
          .map((session) => ({
            id: session.id,
            status: ensureString(session.status || '', '').trim() || null,
            startedAt: session.startedAt || null,
            endedAt: session.endedAt || null,
            problemSummary: ensureLongString(session.problemSummary || session.issue || session.symptom || '', '', 1000) || null,
            solutionSummary: ensureLongString(session.solutionSummary || session.solution || '', '', 1000) || null,
            symptom: ensureLongString(session.symptom || session.issue || '', '', 1000) || null,
            solution: ensureLongString(session.solution || '', '', 1000) || null,
            outcome: ensureString(session.outcome || '', '').trim() || null,
            internalNotes: ensureLongString(session.internalNotes || session.notes || '', '', 1000) || null,
            techName: ensureString(session.techName || '', '').trim() || null,
            creditsConsumed: Math.max(0, ensureInteger(session.creditsConsumed, 0)),
            isFreeFirstSupport: ensureBoolean(session.isFreeFirstSupport, false),
          }));
      } catch (error) {
        console.error('Failed to fetch support sessions fallback for client context', error);
      }
    }
  }

  const verificationStatus = ensureString(resolvedClientContext.verification?.status || '', '').trim().toLowerCase();
  const verificationTone =
    verificationStatus === 'verified'
      ? 'ok'
      : verificationStatus === 'pending'
        ? 'warn'
        : verificationStatus
          ? 'danger'
          : 'warn';

  return {
    anchor: {
      requestId: requestData?.id || null,
      sessionId: sessionData?.id || null,
      supportSessionId:
        ensureString(
          sessionData?.supportSessionId ||
            requestData?.localSupportSessionId ||
            requestData?.supportProfile?.localSupportSessionId ||
            '',
          ''
        ).trim() || null,
      clientUid:
        ensureString(
          sessionData?.clientUid ||
            requestData?.clientUid ||
            '',
          ''
        ).trim() || null,
      clientPhone:
        normalizePhone(
          sessionData?.clientPhone ||
            requestData?.clientPhone ||
            phone
        ) || null,
      deviceAnchor: resolvedDeviceAnchor,
      requiresTechnicianRegistration:
        ensureBoolean(sessionData?.requiresTechnicianRegistration, false) ||
        ensureBoolean(requestData?.requiresTechnicianRegistration, false) ||
        !isClientProfileCompleted(resolvedClientContext.client),
      status:
        ensureString(sessionData?.status || requestData?.state || '', '').trim() ||
        null,
    },
    request: requestData
      ? {
          requestId: requestData.id,
          state: ensureString(requestData.state || '', '').trim() || null,
          createdAt: requestData.createdAt || null,
          clientName: ensureString(requestData.clientName || '', '').trim() || null,
          brand: ensureString(requestData.brand || '', '').trim() || null,
          model: ensureString(requestData.model || '', '').trim() || null,
          osVersion: ensureString(requestData.osVersion || '', '').trim() || null,
        }
      : null,
    session: sessionData ? toPublicSessionSummary(sessionData.id, sessionData) : null,
    client: resolvedClientContext.client,
    profile: resolvedClientContext.profile,
    verification: resolvedClientContext.verification,
    verificationTone,
    needsRegistration:
      !resolvedClientContext.client || !isClientProfileCompleted(resolvedClientContext.client),
    recentSupportSessions,
  };
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
const DEVICE_IMAGE_MAX_BYTES = 5 * 1024 * 1024;
const DEVICE_IMAGE_ALLOWED_EXTENSIONS = new Set(['jpg', 'jpeg', 'png', 'webp', 'gif', 'bmp', 'heic', 'heif']);

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

const deviceImageUpload = multer({
  storage: multer.memoryStorage(),
  limits: {
    files: 1,
    fileSize: DEVICE_IMAGE_MAX_BYTES,
  },
});

const parseDeviceImageUpload = (req, res, next) => {
  deviceImageUpload.single('file')(req, res, (error) => {
    if (!error) return next();
    if (error.code === 'LIMIT_FILE_SIZE') {
      return res.status(400).json({ error: 'file_too_large', message: 'A imagem excede o limite de 5MB.' });
    }
    return res.status(400).json({ error: 'invalid_multipart_payload' });
  });
};

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

app.get(['/credit-panel.html', '/tech-panel', '/tech-panel/', '/tech-panel/index.html'], (_req, res) => {
  return res.redirect(302, '/central.html');
});

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
  const firebaseConfig = resolveFirebaseClientConfig();
  const techLoginRecaptcha = getTechLoginRecaptchaPublicConfig();
  res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');

  const firebaseSerialized = (firebaseConfig ? JSON.stringify(firebaseConfig) : 'null').replace(/</g, '\\u003C');
  const recaptchaSerialized = JSON.stringify(techLoginRecaptcha || {}).replace(/</g, '\\u003C');
  const script = `(() => {
    const target = (window.__CENTRAL_CONFIG__ = window.__CENTRAL_CONFIG__ || {});
    if (!target.firebase) {
      target.firebase = ${firebaseSerialized};
    }
    target.techLoginRecaptcha = ${recaptchaSerialized};
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
  const normalizedTelemetry = normalizeTelemetryData(
    typeof data.telemetry === 'object' && data.telemetry !== null ? data.telemetry : {}
  );
  const base = {
    sessionId: snapshot.id,
    requestId: data.requestId || null,
    techId: data.techId || null,
    techUid: data.techUid || null,
    techName: data.techName || null,
    clientId: data.clientId || null,
    clientSocketId: data.clientSocketId || data.clientId || null,
    clientRecordId: data.clientRecordId || null,
    clientUid: data.clientUid || null,
    clientPhone: normalizePhone(data.clientPhone) || null,
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
    technicianSatisfactionScore:
      typeof data.technicianSatisfactionScore === 'number'
        ? data.technicianSatisfactionScore
        : typeof data.npsScore === 'number'
          ? data.npsScore
          : null,
    customerSatisfactionScore:
      typeof data.customerSatisfactionScore === 'number' ? data.customerSatisfactionScore : null,
    firstContactResolution:
      typeof data.firstContactResolution === 'boolean' ? data.firstContactResolution : null,
    npsScore: typeof data.npsScore === 'number' ? data.npsScore : null,
    outcome: data.outcome || null,
    symptom: data.symptom || null,
    solution: data.solution || null,
    notes: data.notes || null,
    supportSessionId: data.supportSessionId || null,
    requiresTechnicianRegistration: ensureBoolean(data.requiresTechnicianRegistration, false),
    isFreeFirstSupport: ensureBoolean(data.isFreeFirstSupport, false),
    creditsConsumed: Math.max(0, ensureInteger(data.creditsConsumed, 0)),
    telemetry: normalizedTelemetry,
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
    if (typeof telemetry.net !== 'undefined' && typeof base.extra.network === 'undefined') {
      base.extra.network = telemetry.net;
    }
    if (typeof telemetry.health !== 'undefined') base.extra.health = telemetry.health;
    if (typeof telemetry.permissions !== 'undefined') base.extra.permissions = telemetry.permissions;
    if (typeof telemetry.alerts !== 'undefined') base.extra.alerts = telemetry.alerts;
    if (typeof telemetry.batteryLevel !== 'undefined') base.extra.batteryLevel = telemetry.batteryLevel;
    if (typeof telemetry.batteryCharging !== 'undefined') base.extra.batteryCharging = telemetry.batteryCharging;
    if (typeof telemetry.temperatureC !== 'undefined') base.extra.temperatureC = telemetry.temperatureC;
    if (typeof telemetry.storageFreeBytes !== 'undefined') base.extra.storageFreeBytes = telemetry.storageFreeBytes;
    if (typeof telemetry.storageTotalBytes !== 'undefined') base.extra.storageTotalBytes = telemetry.storageTotalBytes;
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
    const normalizedClientUid =
      ensureString(decodedClient.uid || payload.clientUid || payload.uid || '', '').trim() || null;
    const tokenPhone = normalizePhone(decodedClient.phone_number || '');
    const normalizedPhone =
      tokenPhone ||
      normalizePhone(payload.clientPhone || payload.phone || payload?.client?.phone || '') ||
      null;
    const normalizedDeviceAnchor =
      normalizeDeviceAnchor(
        payload.deviceAnchor ||
          payload.device?.anchor ||
          payload.supportProfile?.deviceAnchor ||
          payload.extra?.device?.anchor ||
          ''
      ) || null;
    if (!normalizedClientUid && !normalizedPhone && !normalizedDeviceAnchor) {
      socket.emit('support:error', { error: 'client_identity_required' });
      return;
    }

    let resolvedClientContext = null;
    try {
      resolvedClientContext = await ensureClientIdentityFromPhone({
        normalizedPhone,
        clientUid: normalizedClientUid,
        deviceAnchor: normalizedDeviceAnchor,
        clientName: ensureString(payload.clientName || '', '').trim(),
        source: 'support_request',
      });
    } catch (error) {
      console.error('Failed to ensure client identity from phone', error);
      socket.emit('support:error', { error: 'client_resolution_failed' });
      return;
    }

    const resolvedClient = resolvedClientContext?.client || null;
    if (!resolvedClient?.id) {
      socket.emit('support:error', { error: 'client_resolution_failed' });
      return;
    }
    const resolvedClientPhone = normalizePhone(resolvedClient.phone || '') || normalizedPhone || null;

    const eligibility = buildClientEligibility(resolvedClient);
    if (!eligibility.canRequest) {
      socket.emit('support:error', {
        error: eligibility.reason || 'support_blocked',
        message: 'Necessario adquirir creditos para novo atendimento.',
        freeFirstSupportUsed: ensureBoolean(resolvedClient.freeFirstSupportUsed, false),
        credits: eligibility.credits,
      });
      return;
    }

    const supportProfile = sanitizeSupportProfile(payload.supportProfile);
    const profileCompleted = isClientProfileCompleted(resolvedClient);
    const requiresTechnicianRegistration =
      ensureBoolean(payload.requiresTechnicianRegistration, false) ||
      !profileCompleted ||
      ensureBoolean(supportProfile.isNewClient, false);
    const resolvedSupportProfile = {
      ...supportProfile,
      isNewClient: requiresTechnicianRegistration,
      isFreeFirstSupport: eligibility.isFreeFirstSupport,
      creditsToConsume: eligibility.creditsConsumed,
      deviceAnchor: normalizedDeviceAnchor,
    };
    const localSupportSessionId = ensureString(
      resolvedSupportProfile.localSupportSessionId || payload.localSupportSessionId || '',
      ''
    )
      .trim()
      .slice(0, 128);
    const requestData = {
      requestId,
      clientSocketId: socket.id,
      clientId: socket.id,
      clientUid: normalizedClientUid,
      clientRecordId: resolvedClient.id,
      clientPhone: resolvedClientPhone,
      deviceAnchor: normalizedDeviceAnchor,
      clientName: resolvedClient.name || ensureString(payload.clientName, 'Cliente em atendimento'),
      brand: ensureString(payload.brand || payload?.device?.brand || '', '') || null,
      model: ensureString(payload.model || payload?.device?.model || '', '') || null,
      osVersion: ensureString(payload?.device?.osVersion || payload.osVersion || '', '') || null,
      device:
        payload?.device && typeof payload.device === 'object'
          ? { ...payload.device, anchor: normalizedDeviceAnchor }
          : normalizedDeviceAnchor
            ? { anchor: normalizedDeviceAnchor }
            : null,
      plan: ensureString(payload.plan || '', '') || null,
      issue: ensureString(payload.issue || '', '') || null,
      supportProfile: resolvedSupportProfile,
      localSupportSessionId: localSupportSessionId || null,
      profileCompleted,
      requiresTechnicianRegistration,
      isFreeFirstSupport: eligibility.isFreeFirstSupport,
      creditsConsumed: eligibility.creditsConsumed,
      extra:
        typeof payload.extra === 'object' && payload.extra !== null
          ? { ...payload.extra, supportProfile: resolvedSupportProfile }
          : { supportProfile: resolvedSupportProfile },
      createdAt: now,
      updatedAt: now,
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
  socket.on('support:cancel', async (payload = {}, ack) => {
    const requestsCollection = getRequestsCollection();
    if (!requestsCollection) {
      respondAck(ack, { ok: false, err: 'firestore_unavailable' });
      return;
    }

    const requestId = ensureString(payload.requestId || '', '').trim().slice(0, 64).toUpperCase();
    if (!requestId) {
      respondAck(ack, { ok: false, err: 'invalid_request_id' });
      return;
    }

    let decodedClient = socket.user || null;
    try {
      if (!decodedClient?.uid) {
        decodedClient = await resolveSocketAuthFromPayload(socket, payload);
      }
    } catch (err) {
      console.error('Failed to resolve client auth for support:cancel', err);
      respondAck(ack, { ok: false, err: 'invalid_token' });
      return;
    }

    const authUid = ensureString(decodedClient?.uid || '', '').trim();
    if (!authUid) {
      respondAck(ack, { ok: false, err: 'missing_token' });
      return;
    }
    const tokenPhone = normalizePhone(decodedClient?.phone_number || payload.clientPhone || '');

    try {
      const requestRef = requestsCollection.doc(requestId);
      const requestSnap = await requestRef.get();
      if (!requestSnap.exists) {
        respondAck(ack, { ok: true, removed: false, requestId });
        return;
      }

      const requestData = requestSnap.data() || {};
      const requestState = ensureString(requestData.state || 'queued', '').trim().toLowerCase() || 'queued';
      if (requestState !== 'queued') {
        respondAck(ack, { ok: false, err: 'request_not_queued' });
        return;
      }

      const ownerUid = ensureString(requestData.clientUid || '', '').trim();
      const requestPhone = normalizePhone(requestData.clientPhone || '');
      const ownerMatchesByUid = Boolean(ownerUid && ownerUid === authUid);
      const ownerMatchesByPhone = Boolean(!ownerUid && tokenPhone && requestPhone && tokenPhone === requestPhone);
      if (!ownerMatchesByUid && !ownerMatchesByPhone) {
        respondAck(ack, { ok: false, err: 'forbidden' });
        return;
      }

      await requestRef.delete();

      const targetSocketId = ensureString(requestData.clientSocketId || requestData.clientId || '', '').trim();
      if (targetSocketId) {
        try {
          io.to(targetSocketId).emit('support:rejected', { requestId, reason: 'client_cancelled' });
        } catch (emitError) {
          console.error('Failed to emit client cancellation to socket', emitError);
        }
      }

      io.emit('queue:updated', { requestId, state: 'removed' });
      respondAck(ack, { ok: true, removed: true, requestId });
    } catch (err) {
      console.error('Failed to cancel support request', err);
      respondAck(ack, { ok: false, err: 'server_error' });
    }
  });

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

    const data = normalizeTelemetryData(
      typeof payload.data === 'object' && payload.data !== null ? payload.data : {}
    );
    const ts = Date.now();
    const from = ensureString(payload.from || '', '');
    const status = {
      sessionId,
      from,
      data,
      ts,
    };

    const mergedTelemetry = normalizeTelemetryData({
      ...(snapshot.data()?.telemetry || {}),
      ...data,
      updatedAt: ts,
    });

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
    if (typeof data.network !== 'undefined') updates['extra.network'] = data.network;
    if (typeof data.health !== 'undefined') updates['extra.health'] = data.health;
    if (typeof data.permissions !== 'undefined') updates['extra.permissions'] = data.permissions;
    if (typeof data.alerts !== 'undefined') updates['extra.alerts'] = data.alerts;
    if (typeof data.batteryLevel !== 'undefined') updates['extra.batteryLevel'] = data.batteryLevel;
    if (typeof data.batteryCharging !== 'undefined') updates['extra.batteryCharging'] = data.batteryCharging;
    if (typeof data.temperatureC !== 'undefined') updates['extra.temperatureC'] = data.temperatureC;
    if (typeof data.storageFreeBytes !== 'undefined') updates['extra.storageFreeBytes'] = data.storageFreeBytes;
    if (typeof data.storageTotalBytes !== 'undefined') updates['extra.storageTotalBytes'] = data.storageTotalBytes;

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
    const list = await Promise.all(
      snapshot.docs.map(async (doc) => {
        const data = doc.data() || {};
        const clientContext = await resolveClientContext({
          clientRecordId: ensureString(data.clientRecordId || '', '').trim(),
          clientUid: ensureString(data.clientUid || '', '').trim(),
          phone: normalizePhone(data.clientPhone) || null,
          deviceAnchor:
            normalizeDeviceAnchor(
              data.deviceAnchor ||
                data.device?.anchor ||
                data.extra?.device?.anchor ||
                data.supportProfile?.deviceAnchor ||
                ''
            ) || null,
        });
        return {
          requestId: doc.id,
          clientName: clientContext.client?.name || data.clientName || 'Cliente',
          clientPhone: normalizePhone(data.clientPhone) || clientContext.client?.phone || null,
          clientUid: ensureString(data.clientUid || '', '').trim() || null,
          clientRecordId: clientContext.client?.id || ensureString(data.clientRecordId || '', '').trim() || null,
          clientRegistered: Boolean(clientContext.client),
          profileCompleted: isClientProfileCompleted(clientContext.client),
          requiresTechnicianRegistration:
            ensureBoolean(data.requiresTechnicianRegistration, false) ||
            !clientContext.client ||
            !isClientProfileCompleted(clientContext.client),
          verificationStatus: clientContext.verification?.status || null,
          credits: clientContext.client?.credits ?? 0,
          supportsUsed: clientContext.client?.supportsUsed ?? 0,
          freeFirstSupportUsed: clientContext.client?.freeFirstSupportUsed ?? false,
          brand: data.brand || null,
          model: data.model || null,
          osVersion: data.osVersion || null,
          localSupportSessionId:
            ensureString(
              data.localSupportSessionId || data.supportProfile?.localSupportSessionId || '',
              ''
            ).trim() || null,
          supportProfile: sanitizeSupportProfile(data.supportProfile || data.extra?.supportProfile || {}),
          createdAt: data.createdAt || null,
          state: data.state || 'queued',
        };
      })
    );
    list.sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0));
    res.json(list);
  } catch (err) {
    console.error('Failed to fetch requests', err);
    if (status === 'queued') {
      return res.status(503).json({ error: 'firestore_unavailable' });
    }
    res.status(500).json({ error: 'firestore_error' });
  }
});

app.get('/api/client-context', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const sessionId = normalizeSessionId(req.query.sessionId || '');
  const requestId = ensureString(req.query.requestId || '', '').trim().slice(0, 64);
  const clientRecordId =
    ensureString(req.query.clientRecordId || req.query.clientId || '', '').trim().slice(0, 128);
  const clientUid = ensureString(req.query.clientUid || '', '').trim().slice(0, 256);
  const phone = normalizePhone(req.query.phone || '');
  const deviceAnchor = normalizeDeviceAnchor(req.query.deviceAnchor || '');

  if (!sessionId && !requestId && !clientRecordId && !clientUid && !phone && !deviceAnchor) {
    return res.status(400).json({ error: 'invalid_query' });
  }

  try {
    const payload = await buildClientContextPayload({
      sessionId,
      requestId,
      clientRecordId,
      clientUid,
      phone,
      deviceAnchor,
    });
    return res.json(payload);
  } catch (error) {
    console.error('Failed to load client context', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.get('/api/device-images/resolve', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const deviceImagesCollection = getDeviceImagesCollection();
  if (!deviceImagesCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const brand = ensureString(req.query.brand || '', '').trim();
  const model = ensureString(req.query.model || '', '').trim();
  const key = buildDeviceImageCatalogKey({ brand, model });
  if (!key) {
    return res.status(400).json({ error: 'invalid_query' });
  }

  try {
    const snapshot = await deviceImagesCollection.doc(key).get();
    if (!snapshot.exists) {
      return res.json({ found: false, key });
    }

    const data = snapshot.data() || {};
    return res.json({
      found: true,
      key,
      item: {
        key,
        brand: ensureString(data.brand || '', '').trim() || brand,
        model: ensureString(data.model || '', '').trim() || model,
        imageUrl: ensureString(data.imageUrl || '', '').trim() || null,
        storagePath: ensureString(data.storagePath || '', '').trim() || null,
        updatedAt: data.updatedAt || null,
        createdAt: data.createdAt || null,
      },
    });
  } catch (error) {
    console.error('Failed to resolve device image', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post(
  '/api/device-images/upload',
  requireAuth(['tech']),
  requireTechAccess,
  parseDeviceImageUpload,
  async (req, res) => {
    if (!uploadBucket) {
      return res.status(503).json({ error: 'storage_unavailable' });
    }

    const deviceImagesCollection = getDeviceImagesCollection();
    if (!deviceImagesCollection) {
      return res.status(503).json({ error: 'firestore_unavailable' });
    }

    const brand = ensureString(req.body?.brand || '', '').trim();
    const model = ensureString(req.body?.model || '', '').trim();
    const key = buildDeviceImageCatalogKey({ brand, model });
    if (!key) {
      return res.status(400).json({ error: 'invalid_payload', message: 'Marca e modelo sao obrigatorios.' });
    }

    const file = req.file;
    if (!file || !file.buffer || !Number.isFinite(file.size) || file.size <= 0) {
      return res.status(400).json({ error: 'file_required', message: 'Envie uma imagem valida.' });
    }

    const normalizedMimeType = ensureString(file.mimetype || '', '').toLowerCase().split(';')[0];
    const extension = detectDeviceImageExtension(normalizedMimeType, file.originalname);
    if (!normalizedMimeType.startsWith('image/') || !DEVICE_IMAGE_ALLOWED_EXTENSIONS.has(extension)) {
      return res.status(400).json({ error: 'invalid_file_type', message: 'Apenas imagens sao permitidas.' });
    }

    const now = Date.now();
    const uploadId = customAlphabet('abcdefghijklmnopqrstuvwxyz0123456789', 14)();
    const storagePath = `catalog/device-images/${key}/${now}-${uploadId}.${extension}`;
    const downloadToken = customAlphabet('abcdefghijklmnopqrstuvwxyz0123456789', 32)();
    const techUid = ensureString(req.user?.uid || '', '').trim() || null;
    const techName =
      ensureString(req.techAccess?.techDoc?.name || req.user?.name || 'Tecnico', 'Tecnico').trim() || 'Tecnico';

    try {
      await uploadBucket.file(storagePath).save(file.buffer, {
        resumable: false,
        metadata: {
          contentType: normalizedMimeType,
          cacheControl: 'public, max-age=31536000, immutable',
          metadata: {
            firebaseStorageDownloadTokens: downloadToken,
            kind: 'device_image_catalog',
            deviceKey: key,
            uploadedByUid: techUid || '',
          },
        },
      });

      const encodedPath = encodeURIComponent(storagePath);
      const imageUrl = `https://firebasestorage.googleapis.com/v0/b/${uploadBucket.name}/o/${encodedPath}?alt=media&token=${downloadToken}`;
      const existingSnapshot = await deviceImagesCollection.doc(key).get();
      const existingCreatedAt = existingSnapshot.exists ? existingSnapshot.data()?.createdAt || null : null;

      await deviceImagesCollection.doc(key).set(
        {
          key,
          brand,
          model,
          brandNormalized: normalizeDeviceIdentityPart(brand),
          modelNormalized: normalizeDeviceIdentityPart(model),
          imageUrl,
          storagePath,
          contentType: normalizedMimeType,
          size: file.size,
          createdAt: existingCreatedAt || now,
          updatedAt: now,
          updatedByTechUid: techUid,
          updatedByTechName: techName,
        },
        { merge: true }
      );

      return res.json({
        ok: true,
        item: {
          key,
          brand,
          model,
          imageUrl,
          storagePath,
          updatedAt: now,
          createdAt: existingCreatedAt || now,
        },
      });
    } catch (error) {
      console.error('Failed to upload device image', error);
      return res.status(500).json({ error: 'server_error' });
    }
  }
);

app.get('/api/clients', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const clientsCollection = getClientsCollection();
  const profilesCollection = getClientProfilesCollection();
  const verificationsCollection = getClientVerificationsCollection();
  if (!clientsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const q = ensureString(req.query.q || '', '').trim().toLowerCase();
  const limitParam = Math.max(1, Math.min(300, ensureInteger(req.query.limit, 120)));

  try {
    const docs = await safeGetDocs(clientsCollection.limit(500), 'clients list');
    const summaries = docs.map((doc) => toClientSummary(doc.id, doc.data() || {}));
    const filtered = summaries
      .filter((client) => {
        if (!q) return true;
        const haystack = [
          ensureString(client.id || '', '').toLowerCase(),
          ensureString(client.name || '', '').toLowerCase(),
          ensureString(client.phone || '', '').toLowerCase(),
          ensureString(client.primaryEmail || '', '').toLowerCase(),
        ].join(' ');
        return haystack.includes(q);
      })
      .sort((a, b) => Number(b.updatedAt || b.createdAt || 0) - Number(a.updatedAt || a.createdAt || 0))
      .slice(0, limitParam);

    const enriched = await Promise.all(
      filtered.map(async (client) => {
        let profile = null;
        let verification = null;
        if (profilesCollection) {
          try {
            const snap = await profilesCollection.doc(client.id).get();
            if (snap.exists) {
              const data = snap.data() || {};
              profile = {
                totalSessions: Math.max(0, ensureInteger(data.totalSessions, 0)),
                totalPaidSessions: Math.max(0, ensureInteger(data.totalPaidSessions, 0)),
                totalFreeSessions: Math.max(0, ensureInteger(data.totalFreeSessions, 0)),
                totalCreditsPurchased: Math.max(0, ensureInteger(data.totalCreditsPurchased, 0)),
                totalCreditsUsed: Math.max(0, ensureInteger(data.totalCreditsUsed, 0)),
                lastSupportAt: data.lastSupportAt || null,
              };
            }
          } catch (error) {
            console.error('Failed to load client profile in list', error);
          }
        }
        if (verificationsCollection) {
          try {
            const snap = await verificationsCollection.doc(client.id).get();
            if (snap.exists) {
              const data = snap.data() || {};
              verification = {
                status: ensureString(data.status || '', '').trim().toLowerCase() || 'pending',
                primaryPhone: normalizePhone(data.primaryPhone) || null,
                verifiedPhone: normalizePhone(data.verifiedPhone) || null,
                updatedAt: data.updatedAt || null,
              };
            }
          } catch (error) {
            console.error('Failed to load client verification in list', error);
          }
        }

        return {
          ...client,
          profileCompleted: isClientProfileCompleted(client),
          verificationStatus: verification?.status || 'pending',
          verification,
          profile,
        };
      })
    );

    return res.json({
      items: enriched,
      total: enriched.length,
      query: q || null,
    });
  } catch (error) {
    console.error('Failed to list clients', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/client-context/register', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const clientsCollection = getClientsCollection();
  const profilesCollection = getClientProfilesCollection();
  const linksCollection = getClientAppLinksCollection();
  const verificationsCollection = getClientVerificationsCollection();
  const pnvRequestsCollection = getPnvRequestsCollection();
  const requestsCollection = getRequestsCollection();
  const sessionsCollection = getSessionsCollection();
  const supportSessionsCollection = getSupportSessionsCollection();

  if (!clientsCollection || !profilesCollection || !requestsCollection || !sessionsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const sessionId = normalizeSessionId(req.body?.sessionId || '');
  const requestId = ensureString(req.body?.requestId || '', '').trim().slice(0, 64);
  const explicitClientRecordId =
    ensureString(req.body?.clientRecordId || req.body?.clientId || '', '').trim().slice(0, 128) || null;
  const explicitClientUid = ensureString(req.body?.clientUid || '', '').trim().slice(0, 256) || null;
  const explicitDeviceAnchor =
    normalizeDeviceAnchor(
      req.body?.deviceAnchor ||
        req.body?.device?.anchor ||
        req.body?.anchor?.deviceAnchor ||
        ''
    ) || null;
  const name = ensureString(req.body?.name || '', '').trim().slice(0, 120);
  const normalizedPhone = normalizePhone(req.body?.phone || req.body?.clientPhone || '');
  const emailRaw = ensureString(req.body?.email || req.body?.primaryEmail || '', '').trim().toLowerCase();
  const primaryEmail = emailRaw || null;
  const notesRaw = ensureLongString(req.body?.notes || '', '', 4000).trim();
  const notes = notesRaw || null;

  const resolveClientUidFromDoc = (value = {}) =>
    ensureString(value?.clientUid || value?.client?.clientUid || '', '').trim() || null;
  const resolveClientSocketIdFromDoc = (value = {}) =>
    ensureString(
      value?.clientSocketId || value?.clientId || value?.client?.clientSocketId || value?.client?.clientId || '',
      ''
    ).trim() || null;

  if (!name || !normalizedPhone) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  const techUid = ensureString(req.user?.uid || '', '').trim();
  const techName =
    ensureString(req.techAccess?.techDoc?.name || req.user?.name || 'Técnico', 'Técnico').trim() || 'Técnico';
  const techEmail =
    ensureString(req.techAccess?.email || req.user?.email || '', '').trim().toLowerCase() || null;

  if (!techUid) {
    return res.status(401).json({ error: 'invalid_token' });
  }

  const now = Date.now();
  let seedRequestData = null;
  let seedSessionData = null;
  if (requestId && requestsCollection) {
    try {
      const requestSnap = await requestsCollection.doc(requestId).get();
      if (requestSnap.exists) seedRequestData = requestSnap.data() || {};
    } catch (error) {
      console.error('Failed to preload request before client registration', error);
    }
  }
  if (sessionId && sessionsCollection) {
    try {
      const sessionSnap = await sessionsCollection.doc(sessionId).get();
      if (sessionSnap.exists) seedSessionData = sessionSnap.data() || {};
    } catch (error) {
      console.error('Failed to preload session before client registration', error);
    }
  }
  let resolvedSeedContext = null;
  let resolvedDeviceAnchor = explicitDeviceAnchor || null;
  try {
    const seededDeviceAnchor =
      normalizeDeviceAnchor(
        explicitDeviceAnchor ||
          seedSessionData?.deviceAnchor ||
          seedSessionData?.device?.anchor ||
          seedRequestData?.deviceAnchor ||
          seedRequestData?.device?.anchor ||
          ''
      ) || null;
    resolvedDeviceAnchor = seededDeviceAnchor;
    resolvedSeedContext = await resolveClientContext({
      clientRecordId:
        ensureString(
          explicitClientRecordId ||
            seedSessionData?.clientRecordId ||
            seedRequestData?.clientRecordId ||
            '',
          ''
        ).trim(),
      clientUid:
        ensureString(
          explicitClientUid ||
            seedSessionData?.clientUid ||
            seedRequestData?.clientUid ||
            '',
          ''
        ).trim(),
      phone: normalizedPhone,
      deviceAnchor: seededDeviceAnchor,
    });
  } catch (error) {
    console.error('Failed to resolve seeded client context before registration', error);
  }
  const fallbackClientId = clientDocIdFromPhone(normalizedPhone);
  const clientId = resolvedSeedContext?.client?.id || fallbackClientId;
  if (!clientId) {
    return res.status(400).json({ error: 'invalid_phone' });
  }

  let linkedClientUid = explicitClientUid;
  let supportSessionId = null;
  let linkedClientSocketId = null;

  try {
    await db.runTransaction(async (tx) => {
      const clientRef = clientsCollection.doc(clientId);
      const profileRef = profilesCollection.doc(clientId);
      const requestRef = requestId ? requestsCollection.doc(requestId) : null;
      const sessionRef = sessionId ? sessionsCollection.doc(sessionId) : null;
      const clientSnap = await tx.get(clientRef);
      const requestSnap = requestRef ? await tx.get(requestRef) : null;
      const sessionSnap = sessionRef ? await tx.get(sessionRef) : null;
      const oldData = clientSnap.exists ? clientSnap.data() || {} : {};
      const requestData = requestSnap?.exists ? requestSnap.data() || {} : null;
      const sessionData = sessionSnap?.exists ? sessionSnap.data() || {} : null;

      const credits = Math.max(0, ensureInteger(oldData.credits, 0));
      const supportsUsed = Math.max(0, ensureInteger(oldData.supportsUsed, 0));
      const freeFirstSupportUsed = ensureBoolean(oldData.freeFirstSupportUsed, false);
      const existingNotes = ensureLongString(oldData.notes || '', '', 4000).trim();
      const mergedNotes = notes || existingNotes || null;
      const existingRegistrationHistory = ensureArray(oldData.registrationHistory)
        .filter((entry) => entry && typeof entry === 'object')
        .slice(-49);

      if (requestData) {
        linkedClientUid = resolveClientUidFromDoc(requestData) || linkedClientUid;
        resolvedDeviceAnchor =
          normalizeDeviceAnchor(requestData.deviceAnchor || requestData.device?.anchor || resolvedDeviceAnchor || '') ||
          resolvedDeviceAnchor;
        supportSessionId =
          ensureString(
            requestData.localSupportSessionId ||
              requestData.supportProfile?.localSupportSessionId ||
              supportSessionId ||
              '',
            ''
          ).trim() || supportSessionId;
        linkedClientSocketId = resolveClientSocketIdFromDoc(requestData) || linkedClientSocketId;
      }

      if (sessionData) {
        linkedClientUid = resolveClientUidFromDoc(sessionData) || linkedClientUid;
        resolvedDeviceAnchor =
          normalizeDeviceAnchor(sessionData.deviceAnchor || sessionData.device?.anchor || resolvedDeviceAnchor || '') ||
          resolvedDeviceAnchor;
        supportSessionId =
          ensureString(sessionData.supportSessionId || supportSessionId || '', '').trim() || supportSessionId;
        linkedClientSocketId = resolveClientSocketIdFromDoc(sessionData) || linkedClientSocketId;
      }

      const registrationEntry = {
        id: customAlphabet('abcdefghijklmnopqrstuvwxyz0123456789', 14)(),
        action: clientSnap.exists ? 'update' : 'create',
        techUid,
        techName,
        techEmail,
        requestId: requestId || null,
        sessionId: sessionId || null,
        createdAt: admin.firestore.Timestamp.now(),
      };

      tx.set(
        clientRef,
        {
          phone: normalizedPhone,
          name,
          primaryEmail: primaryEmail || oldData.primaryEmail || null,
          notes: mergedNotes,
          credits,
          supportsUsed,
          freeFirstSupportUsed,
          deviceAnchor: resolvedDeviceAnchor || oldData.deviceAnchor || null,
          profileCompleted: true,
          status: deriveClientStatus({ credits, freeFirstSupportUsed }),
          createdAt: oldData.createdAt || now,
          updatedAt: now,
          createdByTechUid: oldData.createdByTechUid || techUid,
          createdByTechName: oldData.createdByTechName || techName,
          createdByTechEmail: oldData.createdByTechEmail || techEmail,
          lastUpdatedByTechUid: techUid,
          lastUpdatedByTechName: techName,
          lastUpdatedByTechEmail: techEmail,
          registrationHistory: [...existingRegistrationHistory, registrationEntry],
        },
        { merge: true }
      );

      if (!clientSnap.exists) {
        tx.set(
          profileRef,
          {
            clientId,
            totalSessions: 0,
            totalPaidSessions: 0,
            totalFreeSessions: 0,
            totalCreditsPurchased: 0,
            totalCreditsUsed: 0,
            lastSupportAt: null,
            createdAt: now,
            updatedAt: now,
          },
          { merge: true }
        );
      }

      if (requestRef && requestData) {
        tx.set(
          requestRef,
          {
            clientRecordId: clientId,
            clientName: name,
            clientPhone: normalizedPhone,
            deviceAnchor: resolvedDeviceAnchor || null,
            requiresTechnicianRegistration: false,
            updatedAt: now,
          },
          { merge: true }
        );
      }

      if (sessionRef && sessionData) {
        tx.set(
          sessionRef,
          {
            clientRecordId: clientId,
            clientName: name,
            clientPhone: normalizedPhone,
            deviceAnchor: resolvedDeviceAnchor || null,
            requiresTechnicianRegistration: false,
            updatedAt: now,
          },
          { merge: true }
        );
      }

      if (supportSessionId && supportSessionsCollection) {
        const supportRef = supportSessionsCollection.doc(supportSessionId);
        tx.set(
          supportRef,
          {
            clientId,
            clientName: name,
            clientPhone: normalizedPhone,
            deviceAnchor: resolvedDeviceAnchor || null,
            requiresTechnicianRegistration: false,
            isFreeFirstSupport: !freeFirstSupportUsed,
            creditsConsumed: !freeFirstSupportUsed ? 0 : 1,
            updatedAt: now,
          },
          { merge: true }
        );
      }
    });
  } catch (error) {
    console.error('Failed to register client in context', error);
    const mappedError = mapFirestoreWriteError(error);
    return res
      .status(mappedError.status)
      .json({ error: mappedError.error, message: mappedError.message, detail: ensureString(error?.message || '', 'server_error') });
  }

  if ((linkedClientUid || resolvedDeviceAnchor) && linksCollection) {
    try {
      const linkPayload = {
        clientUid: linkedClientUid || null,
        clientId,
        phone: normalizedPhone,
        deviceAnchor: resolvedDeviceAnchor || null,
        supportSessionId: supportSessionId || null,
        createdAt: now,
        updatedAt: now,
      };
      if (linkedClientUid) {
        await linksCollection.doc(linkedClientUid).set(linkPayload, { merge: true });
      }
      const deviceLinkDocId = resolvedDeviceAnchor ? linkDocIdFromDeviceAnchor(resolvedDeviceAnchor) : null;
      if (deviceLinkDocId) {
        await linksCollection.doc(deviceLinkDocId).set(
          {
            ...linkPayload,
            linkType: 'device',
          },
          { merge: true }
        );
      }
    } catch (error) {
      console.error('Failed to upsert client_app_link', error);
    }
  }

  let verificationTrigger = { status: 'ok', message: 'Verificação iniciada com sucesso.' };
  try {
    if (verificationsCollection) {
      const verificationRef = verificationsCollection.doc(clientId);
      const existingVerificationSnap = await verificationRef.get();
      const existingVerification = existingVerificationSnap.exists ? existingVerificationSnap.data() || {} : {};
      const existingStatus = ensureString(existingVerification.status || '', '').trim().toLowerCase();
      const verificationPayload = {
        clientId,
        primaryPhone: normalizedPhone,
        status: existingStatus || 'pending',
        source: 'technician_registration',
        lastTriggerAt: now,
        updatedAt: now,
      };
      if (existingStatus) {
        verificationPayload.verifiedPhone = normalizePhone(existingVerification.verifiedPhone || '') || null;
        verificationPayload.mismatchReason = ensureLongString(existingVerification.mismatchReason || '', '', 1000) || null;
        verificationPayload.lastVerificationAt = existingVerification.lastVerificationAt || null;
      }
      await verificationRef.set(verificationPayload, { merge: true });
    }
    if (pnvRequestsCollection) {
      await pnvRequestsCollection.add({
        clientId,
        clientUid: linkedClientUid || null,
        deviceAnchor: resolvedDeviceAnchor || null,
        phone: normalizedPhone,
        supportSessionId: supportSessionId || null,
        manualFallback: false,
        status: 'pending',
        source: 'technician_registration',
        requestId: requestId || null,
        sessionId: sessionId || null,
        createdAt: now,
        updatedAt: now,
      });
    }
  } catch (error) {
    console.error('Failed to trigger client verification', error);
    verificationTrigger = {
      status: 'error',
      message: 'Cliente salvo, mas houve falha ao disparar verificação automática.',
    };
  }

  const verificationEventPayload = {
    clientId,
    clientUid: linkedClientUid || null,
    deviceAnchor: resolvedDeviceAnchor || null,
    phone: normalizedPhone,
    supportSessionId: supportSessionId || null,
    source: 'technician_registration',
    triggeredAt: now,
  };

  if (linkedClientSocketId) {
    io.to(linkedClientSocketId).emit('client:verification:trigger', verificationEventPayload);
  }
  if (sessionId) {
    io.to(`s:${sessionId}`).emit('client:verification:trigger', verificationEventPayload);
  }

  try {
    const context = await buildClientContextPayload({
      sessionId,
      requestId,
      clientRecordId: clientId,
      clientUid: linkedClientUid,
      phone: normalizedPhone,
      deviceAnchor: resolvedDeviceAnchor,
    });
    return res.json({
      ok: true,
      ...context,
      verificationTrigger,
    });
  } catch (error) {
    console.error('Failed to refresh context after registration', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/client-context/credits', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }
  const clientsCollection = getClientsCollection();
  if (!clientsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const clientId = ensureString(req.body?.clientId || '', '').trim().slice(0, 128);
  const delta = ensureInteger(req.body?.delta, 0);
  if (!clientId || delta === 0) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  try {
    await db.runTransaction(async (tx) => {
      const clientRef = clientsCollection.doc(clientId);
      const snap = await tx.get(clientRef);
      if (!snap.exists) throw new Error('client_not_found');
      const data = snap.data() || {};
      const credits = Math.max(0, ensureInteger(data.credits, 0) + delta);
      const freeFirstSupportUsed = ensureBoolean(data.freeFirstSupportUsed, false);
      tx.set(
        clientRef,
        {
          credits,
          status: deriveClientStatus({ credits, freeFirstSupportUsed }),
          updatedAt: Date.now(),
          lastUpdatedByTechUid: ensureString(req.user?.uid || '', '').trim() || null,
        },
        { merge: true }
      );
    });
  } catch (error) {
    if (ensureString(error?.message || '', '').includes('client_not_found')) {
      return res.status(404).json({ error: 'client_not_found' });
    }
    console.error('Failed to update client credits', error);
    return res.status(500).json({ error: 'server_error' });
  }

  try {
    const context = await buildClientContextPayload({ clientRecordId: clientId });
    return res.json({ ok: true, ...context });
  } catch (error) {
    console.error('Failed to load context after credit update', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/client-context/note', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }
  const clientsCollection = getClientsCollection();
  if (!clientsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const clientId = ensureString(req.body?.clientId || '', '').trim().slice(0, 128);
  const note = ensureLongString(req.body?.note || '', '', 1000).trim();
  if (!clientId || !note) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  try {
    await db.runTransaction(async (tx) => {
      const clientRef = clientsCollection.doc(clientId);
      const snap = await tx.get(clientRef);
      if (!snap.exists) throw new Error('client_not_found');
      const oldNotes = ensureLongString(snap.data()?.notes || '', '', 4000).trim();
      const entry = `- ${note}`;
      const merged = oldNotes ? `${oldNotes}\n${entry}` : entry;
      tx.set(
        clientRef,
        {
          notes: merged.slice(0, 4000),
          updatedAt: Date.now(),
          lastUpdatedByTechUid: ensureString(req.user?.uid || '', '').trim() || null,
        },
        { merge: true }
      );
    });
  } catch (error) {
    if (ensureString(error?.message || '', '').includes('client_not_found')) {
      return res.status(404).json({ error: 'client_not_found' });
    }
    console.error('Failed to append client note', error);
    return res.status(500).json({ error: 'server_error' });
  }

  try {
    const context = await buildClientContextPayload({ clientRecordId: clientId });
    return res.json({ ok: true, ...context });
  } catch (error) {
    console.error('Failed to load context after note update', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/client-context/delete', requireAuth(['tech']), requireSupervisor, async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }
  const clientsCollection = getClientsCollection();
  const profilesCollection = getClientProfilesCollection();
  const verificationsCollection = getClientVerificationsCollection();
  const linksCollection = getClientAppLinksCollection();
  if (!clientsCollection || !profilesCollection || !verificationsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const clientId = ensureString(req.body?.clientId || '', '').trim().slice(0, 128);
  const sessionId = normalizeSessionId(req.body?.sessionId || '');
  const requestId = ensureString(req.body?.requestId || '', '').trim().slice(0, 64);
  if (!clientId) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  let deletedPhone = null;
  try {
    await db.runTransaction(async (tx) => {
      const clientRef = clientsCollection.doc(clientId);
      const profileRef = profilesCollection.doc(clientId);
      const verificationRef = verificationsCollection.doc(clientId);
      const clientSnap = await tx.get(clientRef);
      if (!clientSnap.exists) {
        throw new Error('client_not_found');
      }
      const clientData = clientSnap.data() || {};
      deletedPhone = normalizePhone(clientData.phone || '') || null;
      tx.delete(clientRef);
      tx.delete(profileRef);
      tx.delete(verificationRef);
    });
  } catch (error) {
    if (ensureString(error?.message || '', '').includes('client_not_found')) {
      return res.status(404).json({ error: 'client_not_found' });
    }
    console.error('Failed to delete client context', error);
    return res.status(500).json({ error: 'server_error' });
  }

  if (linksCollection) {
    try {
      while (true) {
        const linksDocs = await safeGetDocs(
          linksCollection.where('clientId', '==', clientId).limit(80),
          'client_app_links by clientId'
        );
        if (!linksDocs.length) break;
        const batch = db.batch();
        linksDocs.forEach((linkDoc) => batch.delete(linkDoc.ref));
        await batch.commit();
        if (linksDocs.length < 80) break;
      }
    } catch (error) {
      console.error('Failed to cleanup client_app_links after client deletion', error);
    }
  }

  try {
    const context = await buildClientContextPayload({
      sessionId,
      requestId,
      phone: deletedPhone || normalizePhone(req.body?.phone || '') || null,
    });
    return res.json({
      ok: true,
      deletedClientId: clientId,
      deletedPhone,
      ...context,
    });
  } catch (error) {
    console.error('Failed to rebuild context after client deletion', error);
    return res.json({
      ok: true,
      deletedClientId: clientId,
      deletedPhone,
    });
  }
});

app.post('/api/client-context/verification/request-manual', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const verificationsCollection = getClientVerificationsCollection();
  const pnvRequestsCollection = getPnvRequestsCollection();
  if (!verificationsCollection || !pnvRequestsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const clientId = ensureString(req.body?.clientId || '', '').trim().slice(0, 128);
  const reason =
    ensureLongString(req.body?.reason || '', '', 1000).trim() ||
    'manual_requested_by_technician';
  if (!clientId) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  const now = Date.now();
  const linkInfo = await resolveClientLinkInfoByClientId(clientId);

  try {
    await verificationsCollection.doc(clientId).set(
      {
        clientId,
        primaryPhone: linkInfo.phone || null,
        verifiedPhone: null,
        status: 'manual_required',
        mismatchReason: reason,
        lastVerificationAt: now,
        updatedAt: now,
      },
      { merge: true }
    );

    await pnvRequestsCollection.add({
      clientUid: linkInfo.clientUid || null,
      clientId,
      phone: linkInfo.phone || null,
      status: 'manual_pending',
      manualFallback: true,
      reason,
      source: 'tech_panel',
      createdAt: now,
      updatedAt: now,
    });
  } catch (error) {
    console.error('Failed to request manual verification', error);
    return res.status(500).json({ error: 'server_error' });
  }

  try {
    const context = await buildClientContextPayload({
      clientRecordId: clientId,
      clientUid: linkInfo.clientUid,
      phone: linkInfo.phone,
    });
    return res.json({ ok: true, ...context });
  } catch (error) {
    console.error('Failed to load context after manual request', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/client-context/verification/confirm-manual', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const clientsCollection = getClientsCollection();
  const verificationsCollection = getClientVerificationsCollection();
  const pnvRequestsCollection = getPnvRequestsCollection();
  if (!clientsCollection || !verificationsCollection || !pnvRequestsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const clientId = ensureString(req.body?.clientId || '', '').trim().slice(0, 128);
  const verifiedPhone = normalizePhone(req.body?.verifiedPhone || req.body?.phone || '');
  const verificationIdToken = ensureLongString(req.body?.verificationIdToken || '', '', 4096).trim();
  if (!clientId || !verifiedPhone || !verificationIdToken) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  let decodedVerificationToken = null;
  try {
    decodedVerificationToken = await admin.auth().verifyIdToken(verificationIdToken, true);
  } catch (error) {
    console.error('Invalid SMS verification token on manual confirmation', error);
    return res.status(400).json({ error: 'invalid_phone_verification_token' });
  }

  const tokenVerifiedPhone = normalizePhone(decodedVerificationToken?.phone_number || '');
  if (!tokenVerifiedPhone) {
    return res.status(400).json({ error: 'verification_phone_missing' });
  }
  if (tokenVerifiedPhone !== verifiedPhone) {
    return res.status(409).json({ error: 'verification_phone_mismatch' });
  }

  const now = Date.now();
  const linkInfo = await resolveClientLinkInfoByClientId(clientId);

  try {
    await clientsCollection.doc(clientId).set(
      {
        phone: verifiedPhone,
        profileCompleted: true,
        updatedAt: now,
        lastUpdatedByTechUid: ensureString(req.user?.uid || '', '').trim() || null,
      },
      { merge: true }
    );

    await verificationsCollection.doc(clientId).set(
      {
        clientId,
        primaryPhone: verifiedPhone,
        verifiedPhone,
        status: 'verified',
        mismatchReason: null,
        lastVerificationAt: now,
        updatedAt: now,
      },
      { merge: true }
    );

    await pnvRequestsCollection.add({
      clientUid: linkInfo.clientUid || null,
      clientId,
      phone: verifiedPhone,
      status: 'processed',
      manualFallback: true,
      reason: 'sms_verified_by_technician',
      source: 'tech_panel',
      createdAt: now,
      updatedAt: now,
      processedAt: now,
      verificationUid: ensureString(decodedVerificationToken?.uid || '', '').trim() || null,
    });
  } catch (error) {
    console.error('Failed to confirm manual verification', error);
    return res.status(500).json({ error: 'server_error' });
  }

  try {
    const context = await buildClientContextPayload({
      clientRecordId: clientId,
      clientUid: linkInfo.clientUid,
      phone: verifiedPhone,
    });
    return res.json({ ok: true, ...context });
  } catch (error) {
    console.error('Failed to load context after manual confirmation', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/client-context/verification/mark-mismatch', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const verificationsCollection = getClientVerificationsCollection();
  const pnvRequestsCollection = getPnvRequestsCollection();
  if (!verificationsCollection || !pnvRequestsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const clientId = ensureString(req.body?.clientId || '', '').trim().slice(0, 128);
  const reason =
    ensureLongString(req.body?.reason || '', '', 1000).trim() ||
    'phone_divergent_manual';
  if (!clientId) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  const now = Date.now();
  const linkInfo = await resolveClientLinkInfoByClientId(clientId);

  try {
    await verificationsCollection.doc(clientId).set(
      {
        clientId,
        primaryPhone: linkInfo.phone || null,
        verifiedPhone: null,
        status: 'mismatch',
        mismatchReason: reason,
        lastVerificationAt: now,
        updatedAt: now,
      },
      { merge: true }
    );

    await pnvRequestsCollection.add({
      clientUid: linkInfo.clientUid || null,
      clientId,
      phone: linkInfo.phone || null,
      status: 'manual_pending',
      manualFallback: true,
      reason,
      source: 'tech_panel',
      createdAt: now,
      updatedAt: now,
    });
  } catch (error) {
    console.error('Failed to mark verification mismatch', error);
    return res.status(500).json({ error: 'server_error' });
  }

  try {
    const context = await buildClientContextPayload({
      clientRecordId: clientId,
      clientUid: linkInfo.clientUid,
      phone: linkInfo.phone,
    });
    return res.json({ ok: true, ...context });
  } catch (error) {
    console.error('Failed to load context after mismatch mark', error);
    return res.status(500).json({ error: 'server_error' });
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
      ensureString(techData.name || techData.displayName || req.user?.name || req.body?.techName || techName || 'Técnico', 'Técnico') ||
      'Técnico';
    const normalizedTechUid = uid;
    const normalizedTechId = uid;
    const normalizedTechEmail = ensureString(techData.email || req.user?.email || req.body?.techEmail || '', '') || null;
    const normalizedTechPhotoURL =
      ensureString(techData.photoURL || techData.photoUrl || req.user?.picture || req.body?.techPhotoURL || '', '') || null;
    const supportProfile = sanitizeSupportProfile(request.supportProfile || request.extra?.supportProfile || {});
    const normalizedClientPhone = normalizePhone(request.clientPhone || req.body?.clientPhone || '');
    const requestDeviceAnchor =
      normalizeDeviceAnchor(
        request.deviceAnchor ||
          request.device?.anchor ||
          request.extra?.device?.anchor ||
          supportProfile.deviceAnchor ||
          ''
      ) || null;
    let resolvedClient = null;
    if (normalizedClientPhone || requestDeviceAnchor || ensureString(request.clientUid || '', '').trim()) {
      resolvedClient = await ensureClientIdentityFromPhone({
        normalizedPhone: normalizedClientPhone,
        clientUid: ensureString(request.clientUid || '', '').trim(),
        deviceAnchor: requestDeviceAnchor,
        clientName: ensureString(request.clientName || '', '').trim(),
        source: 'support_accept',
      });
    } else {
      resolvedClient = await resolveClientContext({
        clientRecordId: ensureString(request.clientRecordId || '', '').trim(),
        clientUid: ensureString(request.clientUid || '', '').trim(),
        phone: normalizedClientPhone,
        deviceAnchor: requestDeviceAnchor,
      });
    }

    const resolvedClientEntity = resolvedClient?.client || null;
    const clientRecordId = resolvedClientEntity?.id || ensureString(request.clientRecordId || '', '').trim() || null;
    const clientPhone = normalizedClientPhone || normalizePhone(resolvedClientEntity?.phone || '') || null;
    if (!clientRecordId) {
      return res.status(409).json({ error: 'client_not_registered' });
    }

    const profileCompleted = isClientProfileCompleted(resolvedClientEntity);
    const eligibility = buildClientEligibility(resolvedClientEntity);
    if (!eligibility.canRequest) {
      return res.status(409).json({
        error: eligibility.reason || 'credit_required',
        message: 'Necessario adquirir creditos para novo atendimento.',
      });
    }

    const requiresTechnicianRegistration =
      ensureBoolean(request.requiresTechnicianRegistration, false) ||
      !profileCompleted;
    const supportSessionId =
      ensureString(
        request.localSupportSessionId || supportProfile.localSupportSessionId || '',
        ''
      ).trim() || null;
    const isFreeFirstSupport = eligibility.isFreeFirstSupport;
    const creditsConsumed = eligibility.creditsConsumed;
    const resolvedSupportProfile = {
      ...supportProfile,
      isNewClient: requiresTechnicianRegistration,
      isFreeFirstSupport,
      creditsToConsume: creditsConsumed,
    };
    const baseExtra = typeof request.extra === 'object' && request.extra !== null ? { ...request.extra } : {};
    const baseTelemetry = normalizeTelemetryData(
      typeof baseExtra.telemetry === 'object' && baseExtra.telemetry !== null ? { ...baseExtra.telemetry } : {}
    );
    const sessionData = {
      sessionId,
      requestId: id,
      clientId: request.clientId || null,
      clientSocketId: request.clientSocketId || request.clientId || null,
      clientRecordId,
      clientUid: request.clientUid || null,
      deviceAnchor: requestDeviceAnchor,
      clientPhone,
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
      clientName: resolvedClientEntity?.name || request.clientName || 'Cliente',
      brand: request.brand || null,
      model: request.model || null,
      osVersion: request.osVersion || null,
      plan: request.plan || null,
      issue: request.issue || null,
      supportSessionId,
      supportProfile: resolvedSupportProfile,
      profileCompleted,
      requiresTechnicianRegistration,
      isFreeFirstSupport,
      creditsConsumed,
      requestedAt: request.createdAt || now,
      acceptedAt: now,
      waitTimeMs: now - (request.createdAt || now),
      status: 'active',
      createdAt: now,
      updatedAt: now,
      telemetry: baseTelemetry,
      extra: {
        ...baseExtra,
        supportProfile: resolvedSupportProfile,
        telemetry: baseTelemetry,
        device: requestDeviceAnchor
          ? {
              ...(baseExtra.device && typeof baseExtra.device === 'object' ? baseExtra.device : {}),
              anchor: requestDeviceAnchor,
            }
          : baseExtra.device,
      },
    };

    await applyClientConsumptionOnAccept({
      clientId: clientRecordId,
      isFreeFirstSupport,
      creditsConsumed,
      now,
    });

    await sessionsCollection.doc(sessionId).set(sessionData);
    await requestRef.delete();

    const targetSocketId = request.clientSocketId || request.clientId;
    if (targetSocketId) {
      try {
        io.to(targetSocketId).emit('support:accepted', { sessionId, techName: normalizedTechName });
      } catch (err) {
        console.error('Failed to emit acceptance to client', err);
      }
    }

    io.emit('queue:updated', { requestId: id, state: 'accepted', sessionId });
    await emitSessionUpdated(sessionId);

    return res.json({ sessionId });
  } catch (err) {
    console.error('Failed to accept request', err);
    return res.status(500).json({ error: 'firestore_error', detail: ensureString(err?.message || '', 'firestore_error') });
  }
});

app.delete('/api/client/requests/:id', requireAuth(), async (req, res) => {
  const requestId = ensureString(req.params.id || '', '').trim().slice(0, 64).toUpperCase();
  if (!requestId) {
    return res.status(400).json({ error: 'invalid_request_id' });
  }
  const requestsCollection = getRequestsCollection();
  if (!requestsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const authUid = ensureString(req.user?.uid || '', '').trim();
  if (!authUid) {
    return res.status(401).json({ error: 'invalid_token' });
  }
  const tokenPhone = normalizePhone(req.user?.phone_number || '');

  try {
    const requestRef = requestsCollection.doc(requestId);
    const requestSnap = await requestRef.get();
    if (!requestSnap.exists) {
      return res.status(204).end();
    }

    const requestData = requestSnap.data() || {};
    const requestState = ensureString(requestData.state || 'queued', '').trim().toLowerCase() || 'queued';
    if (requestState !== 'queued') {
      return res.status(409).json({ error: 'request_not_queued' });
    }

    const ownerUid = ensureString(requestData.clientUid || '', '').trim();
    const requestPhone = normalizePhone(requestData.clientPhone || '');
    const ownerMatchesByUid = Boolean(ownerUid && ownerUid === authUid);
    const ownerMatchesByPhone = Boolean(!ownerUid && tokenPhone && requestPhone && tokenPhone === requestPhone);
    if (!ownerMatchesByUid && !ownerMatchesByPhone) {
      return res.status(403).json({ error: 'forbidden' });
    }

    await requestRef.delete();

    const targetSocketId = ensureString(requestData.clientSocketId || requestData.clientId || '', '').trim();
    if (targetSocketId) {
      try {
        io.to(targetSocketId).emit('support:rejected', { requestId, reason: 'client_cancelled' });
      } catch (err) {
        console.error('Failed to emit rejection after client cancel', err);
      }
    }
    io.emit('queue:updated', { requestId, state: 'removed' });
    return res.status(204).end();
  } catch (err) {
    console.error('Failed to cancel request from client endpoint', err);
    return res.status(500).json({ error: 'firestore_error', detail: ensureString(err?.message || '', 'firestore_error') });
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
    const targetSocketId = data.clientSocketId || data.clientId;
    if (targetSocketId) {
      try {
        io.to(targetSocketId).emit('support:rejected', { requestId: id });
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

app.post('/api/auth/recaptcha/verify', async (req, res) => {
  const config = resolveTechLoginRecaptchaConfig();
  if (!config.enabled) {
    return res.status(503).json({
      error: 'captcha_unavailable',
      message: 'A prote\u00E7\u00E3o anti-bot do login t\u00E9cnico est\u00E1 temporariamente indispon\u00EDvel.',
    });
  }

  const token = ensureLongString(req.body?.token || '', '', 8192).trim();
  if (!token) {
    return res.status(400).json({
      error: 'captcha_required',
      message: 'Confirme o reCAPTCHA antes de continuar.',
    });
  }

  try {
    const verification = await verifyTechLoginRecaptchaToken({
      token,
      userAgent: ensureFullString(req.headers?.['user-agent'] || '', ''),
      remoteIpAddress: resolveRequestIpAddress(req),
      isProduction,
    });

    if (!verification.ok) {
      if (verification.error === 'captcha_low_score') {
        return res.status(403).json({
          error: 'captcha_low_score',
          message: 'A verificacao anti-bot recusou esta tentativa. Tente novamente.',
          score: verification.score ?? null,
          minScore: verification.minScore ?? null,
        });
      }
      if (verification.error === 'captcha_hostname_mismatch') {
        return res.status(403).json({
          error: 'captcha_hostname_mismatch',
          message: 'Host inv\u00E1lido para esta chave de reCAPTCHA.',
          hostname: verification.hostname || null,
        });
      }
      if (verification.error === 'captcha_project_missing') {
        return res.status(503).json({
          error: 'captcha_project_missing',
          message: 'Projeto do reCAPTCHA Enterprise n\u00E3o configurado no servidor.',
        });
      }
      return res.status(verification.status || 403).json({
        error: verification.error || 'captcha_invalid',
        message: 'Falha na validacao anti-bot. Tente novamente.',
        reason: verification.invalidReason || null,
      });
    }

    return res.json({ ok: true, score: verification.score ?? null });
  } catch (error) {
    console.error('Failed to verify reCAPTCHA token for tech login', error);
    const mappedError = mapRecaptchaRuntimeError(error);
    const detail = ensureString(error?.message || '', '').slice(0, 220) || null;
    return res.status(503).json({
      error: mappedError.error,
      message: mappedError.message,
      hint: mappedError.hint,
      detail,
    });
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

const normalizeIdentifier = (value) => ensureString(value || '', '').trim().toLowerCase();

const parseReportTimestamp = (value, fallback = null) => {
  if (value === undefined || value === null || value === '') return fallback;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (value instanceof Date) {
    const ts = value.getTime();
    return Number.isFinite(ts) ? ts : fallback;
  }
  if (typeof value === 'string') {
    const asNumber = Number(value);
    if (Number.isFinite(asNumber)) return asNumber;
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? fallback : parsed;
  }
  if (typeof value === 'object') {
    if (typeof value.toMillis === 'function') {
      const ts = value.toMillis();
      return Number.isFinite(ts) ? ts : fallback;
    }
    if (typeof value.toDate === 'function') {
      const date = value.toDate();
      if (date instanceof Date) {
        const ts = date.getTime();
        return Number.isFinite(ts) ? ts : fallback;
      }
    }
    if (typeof value.seconds === 'number') {
      const nanos = typeof value.nanoseconds === 'number' ? value.nanoseconds : 0;
      return value.seconds * 1000 + Math.floor(nanos / 1e6);
    }
    if (typeof value._seconds === 'number') {
      const nanos = typeof value._nanoseconds === 'number' ? value._nanoseconds : 0;
      return value._seconds * 1000 + Math.floor(nanos / 1e6);
    }
  }
  return fallback;
};

const clampRoundedScore = (rawValue, min, max) => {
  if (rawValue === undefined || rawValue === null || rawValue === '') return null;
  const num = Number(rawValue);
  if (!Number.isFinite(num)) return null;
  return Math.max(min, Math.min(max, Math.round(num)));
};

const SUPPORT_REPORT_DIVIDER = '\u2501'.repeat(23);
const SUPPORT_REPORT_TIMEZONE = ensureString(process.env.SUPPORT_REPORT_TIMEZONE || 'America/Cuiaba', '').trim() || 'America/Cuiaba';

const normalizeEmail = (value) => ensureString(value || '', '').trim().toLowerCase() || null;

const formatDateTimePtBr = (timestamp, fallback = '—') => {
  const millis = parseReportTimestamp(timestamp, null);
  if (millis === null) return fallback;
  try {
    return new Date(millis).toLocaleString('pt-BR', { timeZone: SUPPORT_REPORT_TIMEZONE });
  } catch (_error) {
    return new Date(millis).toLocaleString('pt-BR');
  }
};

const formatPhoneForDisplay = (phone) => {
  const normalized = normalizePhone(phone);
  if (!normalized) return '—';
  const digits = normalized.replace(/\D/g, '');
  if (digits.length === 13) {
    return `+${digits.slice(0, 2)} (${digits.slice(2, 4)}) ${digits.slice(4, 9)}-${digits.slice(9)}`;
  }
  if (digits.length === 12) {
    return `+${digits.slice(0, 2)} (${digits.slice(2, 4)}) ${digits.slice(4, 8)}-${digits.slice(8)}`;
  }
  return normalized;
};

const normalizeOutcomeLabel = (value) => {
  const normalized = ensureString(value || '', '').trim().toLowerCase();
  if (normalized === 'resolved') return 'Resolvido';
  if (normalized === 'partial') return 'Parcial';
  if (normalized === 'transferred') return 'Transferido';
  if (normalized === 'cancelled') return 'Cancelado';
  return normalized ? normalized : 'Resolvido';
};

const compactReportField = (value, fallback = '—') => {
  const normalized = ensureLongString(value || '', '', 2000)
    .replace(/\s+/g, ' ')
    .trim();
  return normalized || fallback;
};

const parseSolutionItems = (solution = '') => {
  const normalized = ensureLongString(solution || '', '', 4000)
    .replace(/\r/g, '\n')
    .split('\n')
    .map((line) => line.replace(/^[\s•\-]+/, '').trim())
    .filter(Boolean);
  if (normalized.length) return normalized.slice(0, 8);

  const fallback = ensureLongString(solution || '', '', 4000)
    .split(/[;]+/)
    .map((line) => line.replace(/^[\s•\-]+/, '').trim())
    .filter(Boolean);
  return fallback.slice(0, 8);
};

const resolveSupportReportCredits = ({ sessionData = {}, clientSummary = null } = {}) => {
  const creditsConsumed = Math.max(0, ensureInteger(sessionData.creditsConsumed, 0));
  const creditsAfter =
    clientSummary && Number.isFinite(Number(clientSummary.credits)) ? Math.max(0, Number(clientSummary.credits)) : null;
  const creditsBefore = creditsAfter != null ? creditsAfter + creditsConsumed : null;
  const consumedLabel = sessionData.isFreeFirstSupport ? '0 (primeiro suporte gratuito)' : String(creditsConsumed);
  return {
    creditsBefore,
    creditsConsumedLabel: consumedLabel,
    creditsAfter,
  };
};

const buildClientSupportReportPayload = ({ sessionId = '', sessionData = {}, clientSummary = null } = {}) => {
  const closedAt = parseReportTimestamp(sessionData.closedAt || sessionData.updatedAt || Date.now(), Date.now());
  const resolvedClientName =
    compactReportField(sessionData.clientName || clientSummary?.name || 'Cliente', 'Cliente');
  const resolvedTechName = compactReportField(sessionData.techName || 'T\u00E9cnico', 'T\u00E9cnico');
  const resolvedPhone = normalizePhone(sessionData.clientPhone || clientSummary?.phone || '') || null;
  const resolvedEmail = normalizeEmail(clientSummary?.primaryEmail || sessionData.clientEmail || '');
  const symptom = compactReportField(sessionData.symptom || '', 'Nao informado');
  const solution = compactReportField(sessionData.solution || '', 'Nao informado');
  const outcomeLabel = normalizeOutcomeLabel(sessionData.outcome || 'resolved');
  const solutionItems = parseSolutionItems(sessionData.solution || solution);
  const credits = resolveSupportReportCredits({ sessionData, clientSummary });

  return {
    sessionId: ensureString(sessionId || sessionData.sessionId || '', '').trim() || null,
    clientName: resolvedClientName,
    clientPhone: resolvedPhone,
    clientPhoneDisplay: formatPhoneForDisplay(resolvedPhone),
    clientEmail: resolvedEmail,
    closedAt,
    closedAtDisplay: formatDateTimePtBr(closedAt, '—'),
    techName: resolvedTechName,
    outcomeLabel,
    symptom,
    solution,
    solutionItems: solutionItems.length ? solutionItems : [solution],
    creditsBeforeDisplay: credits.creditsBefore == null ? '—' : String(credits.creditsBefore),
    creditsConsumedDisplay: credits.creditsConsumedLabel,
    creditsAfterDisplay: credits.creditsAfter == null ? '—' : String(credits.creditsAfter),
  };
};

const buildClientSupportReportText = (report) => {
  const header = '\uD83D\uDD27 *SUPORTE X \u2013 RELAT\u00D3RIO DE ATENDIMENTO*';
  const intro = `Ol\u00E1, *${report.clientName}*! \uD83D\uDC4B`;
  const body = [
    header,
    intro,
    'Seu atendimento foi conclu\u00EDdo com sucesso. Segue o resumo do que foi realizado no seu dispositivo:',
    SUPPORT_REPORT_DIVIDER,
    '\uD83D\uDCCB *DADOS DO CLIENTE*',
    `Nome: ${report.clientName}`,
    `Telefone: ${report.clientPhoneDisplay}`,
    `Data do atendimento: ${report.closedAtDisplay}`,
    `T\u00E9cnico respons\u00E1vel: ${report.techName}`,
    SUPPORT_REPORT_DIVIDER,
    '\u2699\uFE0F *O QUE FOI IDENTIFICADO*',
    `${report.symptom}`,
    `Resultado: ${report.outcomeLabel}`,
    SUPPORT_REPORT_DIVIDER,
    '\uD83D\uDEE0\uFE0F *O QUE FOI FEITO*',
    ...report.solutionItems.map((item) => `\u2022 ${item}`),
    SUPPORT_REPORT_DIVIDER,
    '\uD83D\uDCB3 *CR\u00C9DITOS*',
    `Antes: ${report.creditsBeforeDisplay}`,
    `Consumido: ${report.creditsConsumedDisplay}`,
    `Depois: ${report.creditsAfterDisplay}`,
    SUPPORT_REPORT_DIVIDER,
    '\uD83D\uDCDE *SUPORTE*',
    'Caso precise novamente, \u00E9 s\u00F3 abrir o aplicativo Suporte X e solicitar um novo atendimento.',
    SUPPORT_REPORT_DIVIDER,
    '\uD83D\uDE4F _Obrigado por confiar na Suporte X_',
    '\uD83D\uDE80 _Simplificando o digital!_',
  ];
  return body.join('\n');
};

const buildClientSupportWhatsAppSummaryText = (report) => {
  const safeCredit = (value) => {
    const normalized = ensureString(value || '', '').trim();
    if (!normalized || normalized === '—') return 'x';
    return normalized;
  };
  const summary = [
    '\uD83D\uDCCB *DADOS DO CLIENTE*',
    `Nome: ${report.clientName}`,
    `Telefone: ${report.clientPhoneDisplay}`,
    `Data do atendimento: ${report.closedAtDisplay}`,
    `T\u00E9cnico respons\u00E1vel: ${report.techName}`,
    SUPPORT_REPORT_DIVIDER,
    '\u2699\uFE0F *O QUE FOI IDENTIFICADO*',
    `${report.symptom}`,
    `Resultado: ${report.outcomeLabel}`,
    SUPPORT_REPORT_DIVIDER,
    '\uD83D\uDEE0\uFE0F *O QUE FOI FEITO*',
    ...report.solutionItems.map((item) => `* ${item}`),
    SUPPORT_REPORT_DIVIDER,
    '\uD83D\uDCB3 *CR\u00C9DITOS*',
    `Antes: ${safeCredit(report.creditsBeforeDisplay)}`,
    `Consumido: ${safeCredit(report.creditsConsumedDisplay)}`,
    `Depois: ${safeCredit(report.creditsAfterDisplay)}`,
  ].join('\n');
  return ensureLongString(summary, '', 1024);
};

const escapeHtmlForEmail = (value = '') =>
  ensureFullString(value || '', '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

const buildClientSupportReportEmailHtml = (report, textVersion) => {
  const safeText = escapeHtmlForEmail(textVersion).replace(/\n/g, '<br/>');
  const title = `Relat\u00F3rio de atendimento - Sess\u00E3o ${escapeHtmlForEmail(report.sessionId || '\u2014')}`;
  return [
    '<!doctype html>',
    '<html lang="pt-BR">',
    '<body style="font-family:Arial,Helvetica,sans-serif;background:#f6f8fb;padding:16px;color:#0f172a;">',
    '<div style="max-width:720px;margin:0 auto;background:#ffffff;border:1px solid #dbe4ff;border-radius:10px;padding:16px;">',
    `<h2 style="margin-top:0;margin-bottom:8px;">${title}</h2>`,
    '<p style="margin-top:0;color:#334155;">Resumo enviado automaticamente ao cliente no encerramento do atendimento.</p>',
    `<div style="line-height:1.5;font-size:14px;white-space:normal;">${safeText}</div>`,
    '</div>',
    '</body>',
    '</html>',
  ].join('');
};

const resolveSupportReportChannelConfig = () => {
  const whatsappToken = ensureLongString(process.env.WHATSAPP_ACCESS_TOKEN || '', '', 4096).trim();
  const whatsappPhoneNumberId = ensureString(process.env.WHATSAPP_PHONE_NUMBER_ID || '', '').trim();
  const whatsappApiVersion = ensureString(process.env.WHATSAPP_API_VERSION || 'v21.0', '').trim() || 'v21.0';
  const whatsappTemplateName =
    ensureString(process.env.WHATSAPP_TEMPLATE_NAME || 'relatorio_suporte_x', '').trim() || 'relatorio_suporte_x';
  const whatsappTemplateLanguage = ensureString(process.env.WHATSAPP_TEMPLATE_LANGUAGE || 'pt_BR', '').trim() || 'pt_BR';
  const emailApiKey = ensureLongString(
    process.env.RESEND_API_KEY || process.env.SUPPORT_REPORT_EMAIL_API_KEY || '',
    '',
    4096
  ).trim();
  const emailFrom =
    ensureString(
      process.env.SUPPORT_REPORT_EMAIL_FROM ||
        process.env.RESEND_FROM_EMAIL ||
        'Suporte X <no-reply@xavierassessoriadigital.com.br>',
      ''
    ).trim() || 'Suporte X <no-reply@xavierassessoriadigital.com.br>';

  return {
    whatsapp: {
      enabled: Boolean(whatsappToken && whatsappPhoneNumberId),
      token: whatsappToken,
      phoneNumberId: whatsappPhoneNumberId,
      apiVersion: whatsappApiVersion,
      templateName: whatsappTemplateName,
      templateLanguage: whatsappTemplateLanguage,
      forceRecipient: normalizePhone(process.env.SUPPORT_REPORT_WHATSAPP_FORCE_TO || ''),
    },
    email: {
      enabled: Boolean(emailApiKey),
      apiKey: emailApiKey,
      from: emailFrom,
      replyTo: normalizeEmail(process.env.SUPPORT_REPORT_EMAIL_REPLY_TO || ''),
    },
  };
};

const sendSupportReportViaWhatsApp = async ({
  toPhone = null,
  text = '',
  clientName = 'Cliente',
  summaryText = '',
  config = null,
} = {}) => {
  if (!config?.enabled) {
    return { channel: 'whatsapp', status: 'skipped', reason: 'not_configured' };
  }

  const target = config.forceRecipient || normalizePhone(toPhone || '');
  if (!target) {
    return { channel: 'whatsapp', status: 'skipped', reason: 'missing_recipient' };
  }

  const endpoint = `https://graph.facebook.com/${encodeURIComponent(config.apiVersion)}/${encodeURIComponent(
    config.phoneNumberId
  )}/messages`;
  const templateName = ensureString(config.templateName || '', '').trim();
  const templateLanguage = ensureString(config.templateLanguage || 'pt_BR', '').trim() || 'pt_BR';
  const fallbackText = ensureLongString(text || '', '', 3900);
  const safeClientName = ensureString(clientName || 'Cliente', '').trim() || 'Cliente';
  const safeSummary = ensureLongString(summaryText || '', '', 1024);
  const bodyPayload = templateName
    ? {
        messaging_product: 'whatsapp',
        recipient_type: 'individual',
        to: target.replace(/\D/g, ''),
        type: 'template',
        template: {
          name: templateName,
          language: { code: templateLanguage },
          components: [
            {
              type: 'body',
              parameters: [
                { type: 'text', text: safeClientName },
                { type: 'text', text: safeSummary },
              ],
            },
          ],
        },
      }
    : {
        messaging_product: 'whatsapp',
        recipient_type: 'individual',
        to: target.replace(/\D/g, ''),
        type: 'text',
        text: {
          preview_url: false,
          body: fallbackText,
        },
      };
  const body = JSON.stringify(bodyPayload);

  try {
    const response = await postJsonWithRuntimeFallback({
      url: endpoint,
      headers: {
        Authorization: `Bearer ${config.token}`,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body, 'utf8'),
      },
      body,
      timeoutMs: 12000,
    });
    const payload = JSON.parse(ensureFullString(response.text || '{}', '{}') || '{}');
    if (!response.ok) {
      const reason =
        ensureString(payload?.error?.message || '', '').trim() ||
        ensureString(response.statusText || '', '').trim() ||
        'provider_error';
      return { channel: 'whatsapp', status: 'error', reason, statusCode: response.status || 500 };
    }
    const providerMessageId = ensureString(payload?.messages?.[0]?.id || '', '').trim() || null;
    return { channel: 'whatsapp', status: 'sent', recipient: target, providerMessageId };
  } catch (error) {
    return { channel: 'whatsapp', status: 'error', reason: ensureString(error?.message || '', 'provider_error') };
  }
};

const sendSupportReportViaEmail = async ({ toEmail = null, subject = '', text = '', html = '', config = null } = {}) => {
  if (!config?.enabled) {
    return { channel: 'email', status: 'skipped', reason: 'not_configured' };
  }
  const recipient = normalizeEmail(toEmail || '');
  if (!recipient) {
    return { channel: 'email', status: 'skipped', reason: 'missing_recipient' };
  }

  const payload = {
    from: config.from,
    to: [recipient],
    subject: ensureString(subject || 'Relat\u00F3rio de atendimento - Suporte X', ''),
    text: ensureLongString(text || '', '', 12000),
    html: ensureLongString(html || '', '', 24000),
  };
  if (config.replyTo) {
    payload.reply_to = config.replyTo;
  }
  const rawBody = JSON.stringify(payload);

  try {
    const response = await postJsonWithRuntimeFallback({
      url: 'https://api.resend.com/emails',
      headers: {
        Authorization: `Bearer ${config.apiKey}`,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(rawBody, 'utf8'),
      },
      body: rawBody,
      timeoutMs: 12000,
    });
    const responsePayload = JSON.parse(ensureFullString(response.text || '{}', '{}') || '{}');
    if (!response.ok) {
      const reason =
        ensureString(responsePayload?.message || '', '').trim() ||
        ensureString(responsePayload?.error || '', '').trim() ||
        ensureString(response.statusText || '', '').trim() ||
        'provider_error';
      return { channel: 'email', status: 'error', reason, statusCode: response.status || 500 };
    }
    return {
      channel: 'email',
      status: 'sent',
      recipient,
      providerMessageId: ensureString(responsePayload?.id || '', '').trim() || null,
    };
  } catch (error) {
    return { channel: 'email', status: 'error', reason: ensureString(error?.message || '', 'provider_error') };
  }
};

const resolveClientSummaryForSession = async (sessionData = {}) => {
  try {
    const context = await resolveClientContext({
      clientRecordId: ensureString(sessionData.clientRecordId || '', '').trim(),
      clientUid: ensureString(sessionData.clientUid || '', '').trim(),
      phone: ensureString(sessionData.clientPhone || '', '').trim(),
      deviceAnchor: ensureString(sessionData.deviceAnchor || sessionData.extra?.device?.anchor || '', '').trim(),
    });
    return context?.client || null;
  } catch (error) {
    console.error('Failed to resolve client summary for report dispatch', error);
    return null;
  }
};

const hasClientFacingReportData = (source = {}) => {
  const symptom = ensureString(source.symptom || '', '').trim();
  const solution = ensureString(source.solution || '', '').trim();
  return Boolean(symptom || solution);
};

const normalizeSupportReportChannel = (value) => {
  const normalized = ensureString(value || '', '').trim().toLowerCase();
  if (!normalized || normalized === 'both' || normalized === 'all') return 'both';
  if (normalized === 'email' || normalized === 'e-mail' || normalized === 'mail') return 'email';
  if (normalized === 'whatsapp' || normalized === 'wa') return 'whatsapp';
  return '';
};

const resolveSupportReportChannels = (requested = null) => {
  const valid = new Set(['whatsapp', 'email']);
  const fromArray = Array.isArray(requested) ? requested : [requested];
  const normalized = fromArray
    .map((item) => normalizeSupportReportChannel(item))
    .filter(Boolean)
    .flatMap((item) => (item === 'both' ? ['whatsapp', 'email'] : [item]))
    .filter((item) => valid.has(item));
  const unique = Array.from(new Set(normalized));
  return unique.length ? unique : ['whatsapp', 'email'];
};

const shouldDispatchClientSupportReport = ({ sessionData = {}, payload = {} } = {}) => {
  const merged = {
    ...sessionData,
    symptom:
      typeof payload.symptom !== 'undefined'
        ? ensureString(payload.symptom || '', '').trim()
        : ensureString(sessionData.symptom || '', '').trim(),
    solution:
      typeof payload.solution !== 'undefined'
        ? ensureString(payload.solution || '', '').trim()
        : ensureString(sessionData.solution || '', '').trim(),
  };
  if (ensureString(merged.status || '', '').toLowerCase() !== 'closed') {
    return { shouldSend: false, reason: 'session_not_closed' };
  }
  if (!hasClientFacingReportData(merged)) {
    return { shouldSend: false, reason: 'missing_report_data' };
  }
  const forceResend = ensureBoolean(payload.forceResendClientReport, false);
  if (merged?.clientReport?.sentAt && !forceResend) {
    return { shouldSend: false, reason: 'already_sent' };
  }
  return { shouldSend: true, reason: null };
};

const dispatchClientSupportReportForSession = async ({
  sessionRef = null,
  sessionId = '',
  sessionData = {},
  payload = {},
  channelsOverride = null,
} = {}) => {
  const decision = shouldDispatchClientSupportReport({ sessionData, payload });
  if (!decision.shouldSend) {
    return { sent: false, status: 'skipped', reason: decision.reason, channels: [] };
  }

  const clientSummary = await resolveClientSummaryForSession(sessionData);
  const report = buildClientSupportReportPayload({
    sessionId: sessionId || sessionData.sessionId || '',
    sessionData,
    clientSummary,
  });
  const text = buildClientSupportReportText(report);
  const whatsappSummaryText = buildClientSupportWhatsAppSummaryText(report);
  const html = buildClientSupportReportEmailHtml(report, text);
  const subject = `Suporte X - Relat\u00F3rio de atendimento (${report.sessionId || 'sess\u00E3o'})`;
  const config = resolveSupportReportChannelConfig();
  const channelsToSend = resolveSupportReportChannels(channelsOverride);
  const sendTasks = [];
  if (channelsToSend.includes('whatsapp')) {
    sendTasks.push(
      sendSupportReportViaWhatsApp({
        toPhone: report.clientPhone,
        text: whatsappSummaryText,
        summaryText: whatsappSummaryText,
        clientName: report.clientName,
        config: config.whatsapp,
      })
    );
  }
  if (channelsToSend.includes('email')) {
    sendTasks.push(sendSupportReportViaEmail({ toEmail: report.clientEmail, subject, text, html, config: config.email }));
  }
  const channels = await Promise.all(sendTasks);
  const sent = channels.some((channel) => channel.status === 'sent');
  const dispatchedAt = Date.now();
  const dispatchResult = {
    sent,
    status: sent ? 'sent' : 'error',
    reason: sent ? null : 'all_channels_failed',
    channels,
    dispatchedAt,
    recipient: {
      phone: report.clientPhone,
      email: report.clientEmail,
    },
  };

  if (sessionRef) {
    try {
      const previous = sessionData.clientReport && typeof sessionData.clientReport === 'object' ? sessionData.clientReport : {};
      await sessionRef.set(
        {
          clientReport: {
            ...previous,
            version: 1,
            updatedAt: dispatchedAt,
            sentAt: sent ? dispatchedAt : previous.sentAt || null,
            channels,
            recipient: dispatchResult.recipient,
            title: 'SUPORTE X - RELAT\u00D3RIO DE ATENDIMENTO',
            text: ensureLongString(text, '', 16000),
            outcome: report.outcomeLabel,
            symptom: report.symptom,
            solution: report.solution,
            sessionId: report.sessionId,
          },
        },
        { merge: true }
      );
    } catch (error) {
      console.error('Failed to persist client report dispatch metadata', error);
    }
  }

  return dispatchResult;
};

const buildSupportReportPdfBuffer = (report) =>
  new Promise((resolve, reject) => {
    const doc = new PDFDocument({ size: 'A4', margin: 36, info: { Title: 'Relat\u00F3rio de Atendimento - Suporte X' } });
    const chunks = [];
    doc.on('data', (chunk) => chunks.push(chunk));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', (error) => reject(error));

    const writeLine = (text = '', options = {}) => {
      doc.font(options.bold ? 'Helvetica-Bold' : 'Helvetica').fontSize(options.size || 11).text(text, {
        width: 520,
        lineGap: 2,
      });
    };

    writeLine('SUPORTE X - RELAT\u00D3RIO DE ATENDIMENTO', { bold: true, size: 14 });
    doc.moveDown(0.6);
    writeLine(`Sess\u00E3o: ${report.sessionId || '\u2014'}`, { bold: true });
    writeLine(`Cliente: ${report.clientName}`);
    writeLine(`Telefone: ${report.clientPhoneDisplay}`);
    writeLine(`Data do atendimento: ${report.closedAtDisplay}`);
    writeLine(`T\u00E9cnico respons\u00E1vel: ${report.techName}`);
    doc.moveDown(0.6);
    writeLine('O QUE FOI IDENTIFICADO', { bold: true });
    writeLine(report.symptom);
    writeLine(`Resultado: ${report.outcomeLabel}`);
    doc.moveDown(0.5);
    writeLine('O QUE FOI FEITO', { bold: true });
    report.solutionItems.forEach((item) => writeLine(`- ${item}`));
    doc.moveDown(0.5);
    writeLine('CR\u00C9DITOS', { bold: true });
    writeLine(`Antes: ${report.creditsBeforeDisplay}`);
    writeLine(`Consumido: ${report.creditsConsumedDisplay}`);
    writeLine(`Depois: ${report.creditsAfterDisplay}`);
    doc.moveDown(0.8);
    writeLine('Obrigado por confiar na Suporte X.', { bold: true });
    writeLine('Simplificando o digital.');
    doc.end();
  });

const resolveSessionTechIdentifiers = (session) =>
  [
    session?.techUid,
    session?.tech?.techUid,
    session?.tech?.uid,
    session?.techId,
    session?.tech?.techId,
    session?.tech?.id,
  ]
    .map((value) => normalizeIdentifier(value))
    .filter(Boolean);

const normalizeReportSessionDoc = (doc) => {
  if (!doc) return null;
  const data = doc.data() || {};
  const technicianSatisfactionScore = (() => {
    const direct = clampRoundedScore(data.technicianSatisfactionScore, 0, 10);
    if (direct !== null) return direct;
    return clampRoundedScore(data.npsScore, 0, 10);
  })();
  const customerSatisfactionScore = clampRoundedScore(data.customerSatisfactionScore, 0, 5);
  const requestedAt = parseReportTimestamp(data.requestedAt, null);
  const acceptedAt = parseReportTimestamp(data.acceptedAt, null);
  const closedAt = parseReportTimestamp(data.closedAt, null);
  const updatedAt = parseReportTimestamp(data.updatedAt, null);
  const techUid =
    ensureString(data.techUid || data.tech?.techUid || data.tech?.uid || data.techId || data.tech?.id || '', '').trim() ||
    null;
  const techName = ensureString(data.techName || data.tech?.name || data.tech?.techName || '', '').trim() || null;
  const session = {
    sessionId: doc.id,
    requestId: ensureString(data.requestId || '', '').trim() || null,
    status: ensureString(data.status || 'active', '').trim() || 'active',
    techUid,
    techName,
    techEmail: ensureString(data.techEmail || data.tech?.email || '', '').trim() || null,
    clientName: ensureString(data.clientName || data.client?.name || '', '').trim() || 'Cliente',
    clientPhone: normalizePhone(data.clientPhone || data.client?.phone || '') || null,
    requestedAt,
    acceptedAt,
    closedAt,
    updatedAt,
    waitTimeMs: Number.isFinite(Number(data.waitTimeMs)) ? Number(data.waitTimeMs) : null,
    handleTimeMs: Number.isFinite(Number(data.handleTimeMs)) ? Number(data.handleTimeMs) : null,
    outcome: ensureString(data.outcome || '', '').trim() || null,
    symptom: ensureString(data.symptom || '', '').trim() || null,
    solution: ensureString(data.solution || '', '').trim() || null,
    firstContactResolution:
      typeof data.firstContactResolution === 'boolean' ? data.firstContactResolution : null,
    technicianSatisfactionScore,
    customerSatisfactionScore,
    npsScore: technicianSatisfactionScore,
    techIdentifiers: resolveSessionTechIdentifiers(data),
  };
  return session;
};

const canAccessReportSession = (sessionData, accessPayload) => {
  if (!sessionData || !accessPayload) return false;
  if (accessPayload.supervisor === true) return true;
  const requesterUid = normalizeIdentifier(accessPayload.uid || '');
  if (!requesterUid) return false;
  const identifiers = resolveSessionTechIdentifiers(sessionData);
  return identifiers.includes(requesterUid);
};

app.get('/api/reports/sessions', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const sessionsCollection = getSessionsCollection();
  if (!sessionsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  try {
    const supervisor = req.techAccess?.supervisor === true;
    const requesterUid = normalizeIdentifier(req.techAccess?.uid || '');
    const requestedTechUid = normalizeIdentifier(req.query.techUid || req.query.tech || '');
    const statusFilter = normalizeIdentifier(req.query.status || 'closed');
    const limitRaw = Number(req.query.limit);
    const queryLimit = Number.isFinite(limitRaw)
      ? Math.max(1, Math.min(Math.round(limitRaw), 1000))
      : 500;
    const startAt = parseReportTimestamp(req.query.start, null);
    const endAt = parseReportTimestamp(req.query.end, null);

    const docs = await safeGetDocs(
      sessionsCollection.orderBy('updatedAt', 'desc').limit(queryLimit),
      'report sessions list'
    );

    const normalized = docs
      .map((doc) => normalizeReportSessionDoc(doc))
      .filter(Boolean)
      .filter((session) => {
        if (!session) return false;
        const identifiers = Array.isArray(session.techIdentifiers) ? session.techIdentifiers : [];
        if (!supervisor && (!requesterUid || !identifiers.includes(requesterUid))) {
          return false;
        }
        if (requestedTechUid && !identifiers.includes(requestedTechUid)) {
          return false;
        }
        if (statusFilter && statusFilter !== 'all' && normalizeIdentifier(session.status) !== statusFilter) {
          return false;
        }
        const basis = session.closedAt || session.acceptedAt || session.requestedAt || session.updatedAt || 0;
        if (startAt !== null && basis < startAt) return false;
        if (endAt !== null && basis > endAt) return false;
        return true;
      })
      .sort((a, b) => {
        const left = b.updatedAt || b.closedAt || b.acceptedAt || b.requestedAt || 0;
        const right = a.updatedAt || a.closedAt || a.acceptedAt || a.requestedAt || 0;
        return left - right;
      });

    const sessions = normalized.map((session) => {
      const { techIdentifiers, ...rest } = session;
      return rest;
    });

    return res.json({
      sessions,
      meta: {
        count: sessions.length,
        status: statusFilter || 'all',
        start: startAt,
        end: endAt,
        techUid: requestedTechUid || null,
        supervisor,
      },
    });
  } catch (error) {
    console.error('Failed to fetch report sessions', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.get('/api/reports/sessions/:id', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const sessionsCollection = getSessionsCollection();
  if (!sessionsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const sessionId = normalizeSessionId(req.params.id || '');
  if (!sessionId) {
    return res.status(400).json({ error: 'invalid_session_id' });
  }

  try {
    const snapshot = await sessionsCollection.doc(sessionId).get();
    if (!snapshot.exists) {
      return res.status(404).json({ error: 'session_not_found' });
    }

    const sessionData = snapshot.data() || {};
    if (!canAccessReportSession(sessionData, req.techAccess || {})) {
      return res.status(403).json({ error: 'forbidden' });
    }

    const fullSession = await buildSessionState(sessionId, { snapshot, includeLogs: true });
    return res.json({ session: fullSession });
  } catch (error) {
    console.error('Failed to fetch report session detail', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/reports/sessions/:id/send-client-report', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const sessionsCollection = getSessionsCollection();
  if (!sessionsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const sessionId = normalizeSessionId(req.params.id || '');
  if (!sessionId) {
    return res.status(400).json({ error: 'invalid_session_id' });
  }

  const requestedChannel = normalizeSupportReportChannel(req.body?.channel || 'both');
  if (!requestedChannel) {
    return res.status(400).json({ error: 'invalid_channel', message: 'Use channel: email, whatsapp ou both.' });
  }
  const channelsToSend = resolveSupportReportChannels(requestedChannel);

  try {
    const snapshot = await sessionsCollection.doc(sessionId).get();
    if (!snapshot.exists) {
      return res.status(404).json({ error: 'session_not_found' });
    }
    const sessionData = snapshot.data() || {};
    if (!canAccessReportSession(sessionData, req.techAccess || {})) {
      return res.status(403).json({ error: 'forbidden' });
    }

    const reportDispatch = await dispatchClientSupportReportForSession({
      sessionRef: snapshot.ref,
      sessionId,
      sessionData: {
        ...sessionData,
        sessionId,
      },
      payload: {
        forceResendClientReport: true,
      },
      channelsOverride: channelsToSend,
    });
    await emitSessionUpdated(sessionId);
    return res.json({ ok: true, reportDispatch, channel: requestedChannel });
  } catch (error) {
    console.error('Failed to send client report manually', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.get('/api/reports/sessions/:id/pdf', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const sessionsCollection = getSessionsCollection();
  if (!sessionsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const sessionId = normalizeSessionId(req.params.id || '');
  if (!sessionId) {
    return res.status(400).json({ error: 'invalid_session_id' });
  }

  try {
    const snapshot = await sessionsCollection.doc(sessionId).get();
    if (!snapshot.exists) {
      return res.status(404).json({ error: 'session_not_found' });
    }

    const sessionData = snapshot.data() || {};
    if (!canAccessReportSession(sessionData, req.techAccess || {})) {
      return res.status(403).json({ error: 'forbidden' });
    }

    const fullSession = await buildSessionState(sessionId, { snapshot, includeLogs: false });
    if (!fullSession) {
      return res.status(404).json({ error: 'session_not_found' });
    }

    const clientSummary = await resolveClientSummaryForSession(fullSession);
    const reportPayload = buildClientSupportReportPayload({
      sessionId,
      sessionData: fullSession,
      clientSummary,
    });
    const pdfBuffer = await buildSupportReportPdfBuffer(reportPayload);
    const fileSessionId = ensureString(sessionId, '').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 40) || 'sessao';
    const filename = `relatorio-atendimento-${fileSessionId}.pdf`;

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Length', String(pdfBuffer.length));
    return res.status(200).send(pdfBuffer);
  } catch (error) {
    console.error('Failed to generate report session pdf', error);
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
    const payload = req.body || {};
    const nowTs = Date.now();

    const reportUpdates = {
      outcome: ensureString(payload.outcome || session.outcome || 'resolved', 'resolved'),
      symptom: ensureString(payload.symptom || session.symptom || '', '') || null,
      solution: ensureString(payload.solution || session.solution || '', '') || null,
      updatedAt: nowTs,
    };

    if (payload.notes && typeof payload.notes === 'string') {
      reportUpdates.notes = ensureString(payload.notes, '');
    }

    const technicianScoreRaw =
      typeof payload.technicianSatisfactionScore !== 'undefined' ? payload.technicianSatisfactionScore : payload.npsScore;
    if (typeof technicianScoreRaw !== 'undefined') {
      const technicianScore = Number(technicianScoreRaw);
      if (!Number.isNaN(technicianScore)) {
        const clamped = Math.max(0, Math.min(10, Math.round(technicianScore)));
        reportUpdates.technicianSatisfactionScore = clamped;
        reportUpdates.npsScore = clamped;
      }
    }

    if (typeof payload.customerSatisfactionScore !== 'undefined') {
      const customerScore = Number(payload.customerSatisfactionScore);
      if (!Number.isNaN(customerScore)) {
        reportUpdates.customerSatisfactionScore = Math.max(0, Math.min(5, Math.round(customerScore)));
      }
    }

    if (typeof payload.firstContactResolution !== 'undefined') {
      reportUpdates.firstContactResolution = Boolean(payload.firstContactResolution);
    }

    if (session.status === 'closed') {
      await snapshot.ref.set(reportUpdates, { merge: true });
      await emitSessionUpdated(id);
      const reportDispatch = await dispatchClientSupportReportForSession({
        sessionRef: snapshot.ref,
        sessionId: id,
        sessionData: {
          ...session,
          ...reportUpdates,
          sessionId: id,
          status: 'closed',
          closedAt: session.closedAt || nowTs,
        },
        payload,
      });
      return res.json({ ok: true, alreadyClosed: true, reportDispatch });
    }

    const closedAt = Date.now();
    const room = `s:${id}`;
    const closerUid = ensureString(req.techAccess?.uid || req.user?.uid || session.techUid || '', '');
    const reason = ensureString(payload.reason || '', '').trim() || 'tech_ended';
    const eventId = `${closedAt.toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    const commandEvent = {
      id: eventId,
      sessionId: id,
      type: 'end',
      rawType: 'session_end',
      data: null,
      by: closerUid || 'tech',
      reason,
      ts: closedAt,
      kind: 'command',
    };

    const nextTelemetry =
      typeof session.telemetry === 'object' && session.telemetry !== null ? { ...session.telemetry } : {};
    nextTelemetry.shareActive = false;
    nextTelemetry.callActive = false;
    nextTelemetry.remoteActive = false;
    nextTelemetry.updatedAt = closedAt;

    const updates = {
      status: 'closed',
      closedAt,
      outcome: reportUpdates.outcome,
      symptom: reportUpdates.symptom,
      solution: reportUpdates.solution,
      handleTimeMs: closedAt - (session.acceptedAt || session.createdAt || closedAt),
      updatedAt: closedAt,
      lastCommandAt: closedAt,
      telemetry: nextTelemetry,
      'extra.telemetry': nextTelemetry,
      'extra.lastCommand': commandEvent,
    };

    if (typeof reportUpdates.notes !== 'undefined') updates.notes = reportUpdates.notes;
    if (typeof reportUpdates.technicianSatisfactionScore !== 'undefined') {
      updates.technicianSatisfactionScore = reportUpdates.technicianSatisfactionScore;
      updates.npsScore = reportUpdates.npsScore;
    }
    if (typeof reportUpdates.customerSatisfactionScore !== 'undefined') {
      updates.customerSatisfactionScore = reportUpdates.customerSatisfactionScore;
    }
    if (typeof reportUpdates.firstContactResolution !== 'undefined') {
      updates.firstContactResolution = reportUpdates.firstContactResolution;
    }

    if (typeof nextTelemetry.network !== 'undefined') updates['extra.network'] = nextTelemetry.network;
    if (typeof nextTelemetry.health !== 'undefined') updates['extra.health'] = nextTelemetry.health;
    if (typeof nextTelemetry.permissions !== 'undefined') updates['extra.permissions'] = nextTelemetry.permissions;
    if (typeof nextTelemetry.alerts !== 'undefined') updates['extra.alerts'] = nextTelemetry.alerts;

    await snapshot.ref.collection('events').doc(eventId).set(commandEvent);
    await snapshot.ref.set(updates, { merge: true });
    io.to(room).emit('session:command', {
      ...commandEvent,
      type: 'session_end',
      normalizedType: 'end',
    });
    io.to(room).emit('session:ended', { sessionId: id, reason });
    io.socketsLeave(room);
    await emitSessionUpdated(id);

    const reportDispatch = await dispatchClientSupportReportForSession({
      sessionRef: snapshot.ref,
      sessionId: id,
      sessionData: {
        ...session,
        ...updates,
        sessionId: id,
      },
      payload,
    });

    return res.json({ ok: true, reportDispatch });
  } catch (err) {
    console.error('Failed to close session', err);
    return res.status(500).json({ error: 'firestore_error' });
  }
});

app.post('/api/sessions/:id/customer-feedback', requireAuth(), async (req, res) => {
  const id = req.params.id;
  if (!getSessionsCollection()) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  try {
    const snapshot = await getSessionSnapshot(id);
    if (!snapshot) {
      return res.status(404).json({ error: 'session_not_found' });
    }

    const session = snapshot.data() || {};
    const authUid = ensureString(req.user?.uid || '', '').trim();
    const sessionClientUid = getSessionClientUid(session);
    if (!authUid || !sessionClientUid || authUid !== sessionClientUid) {
      return res.status(403).json({ error: 'forbidden' });
    }

    const rawScore = req.body?.customerSatisfactionScore;
    const scoreNum = Number(rawScore);
    if (!Number.isFinite(scoreNum)) {
      return res.status(400).json({ error: 'invalid_customer_satisfaction' });
    }
    const customerSatisfactionScore = Math.max(0, Math.min(5, Math.round(scoreNum)));
    const updatedAt = Date.now();

    await snapshot.ref.set(
      {
        customerSatisfactionScore,
        updatedAt,
      },
      { merge: true }
    );
    await emitSessionUpdated(id);

    return res.json({ ok: true, sessionId: id, customerSatisfactionScore });
  } catch (err) {
    console.error('Failed to save customer feedback', err);
    return res.status(500).json({ error: 'firestore_error' });
  }
});

app.get('/api/client/queue-stats', requireAuth(), async (req, res) => {
  const sessionsCollection = getSessionsCollection();
  const requestsCollection = getRequestsCollection();
  if (!sessionsCollection || !requestsCollection) {
    console.error('Firestore not configured. Cannot compute client queue stats.');
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  try {
    const now = new Date();
    const rangeStart = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
    const snapshot = await sessionsCollection
      .where('acceptedAt', '>=', rangeStart)
      .orderBy('acceptedAt', 'desc')
      .get();

    const sessions = snapshot.docs.map((doc) => ({ id: doc.id, ...(doc.data() || {}) }));
    const waitTimes = sessions
      .map((s) => s.waitTimeMs)
      .filter((ms) => typeof ms === 'number' && ms >= 0);
    const averageWaitMs = waitTimes.length ? waitTimes.reduce((a, b) => a + b, 0) / waitTimes.length : null;

    const queueSnapshot = await requestsCollection.where('state', '==', 'queued').get();
    return res.json({
      averageWaitMs,
      queueSize: queueSnapshot.size,
      sampleSize: waitTimes.length,
      targetSampleSize: waitTimes.length,
      lastUpdated: Date.now(),
    });
  } catch (err) {
    console.error('Failed to compute client queue stats', err);
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

    const technicianScores = closedSessions
      .map((s) => {
        if (typeof s.technicianSatisfactionScore === 'number') return s.technicianSatisfactionScore;
        if (typeof s.npsScore === 'number') return s.npsScore;
        return null;
      })
      .filter((n) => n !== null && !Number.isNaN(n));
    const technicianSatisfactionAverage = technicianScores.length
      ? technicianScores.reduce((a, b) => a + b, 0) / technicianScores.length
      : null;

    const customerScores = closedSessions
      .map((s) => (typeof s.customerSatisfactionScore === 'number' ? s.customerSatisfactionScore : null))
      .filter((n) => n !== null && !Number.isNaN(n));
    const customerSatisfactionAverage = customerScores.length
      ? customerScores.reduce((a, b) => a + b, 0) / customerScores.length
      : null;
    const lastClosedSession = closedSessions
      .slice()
      .sort(
        (a, b) =>
          parseReportTimestamp(b.closedAt || b.updatedAt || b.acceptedAt || b.createdAt || 0, 0) -
          parseReportTimestamp(a.closedAt || a.updatedAt || a.acceptedAt || a.createdAt || 0, 0)
      )[0] || null;
    const lastClosedAt = lastClosedSession
      ? parseReportTimestamp(
          lastClosedSession.closedAt ||
            lastClosedSession.updatedAt ||
            lastClosedSession.acceptedAt ||
            lastClosedSession.createdAt,
          null
        )
      : null;
    const lastClosedCustomerSatisfactionScore =
      lastClosedSession && typeof lastClosedSession.customerSatisfactionScore === 'number'
        ? lastClosedSession.customerSatisfactionScore
        : null;

    const queueSnapshot = await requestsCollection.where('state', '==', 'queued').get();

    res.json({
      attendancesToday: todaysSessions.length,
      activeSessions: activeSessions.length,
      averageWaitMs,
      averageHandleMs,
      technicianSatisfactionAverage,
      customerSatisfactionAverage,
      lastClosedAt,
      lastClosedCustomerSatisfactionScore,
      fcrPercentage: technicianSatisfactionAverage,
      nps: customerSatisfactionAverage,
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
