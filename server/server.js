const path = require('path');
const crypto = require('crypto');
const express = require('express');
const http = require('http');
const https = require('https');
const cors = require('cors');
const multer = require('multer');
const PDFDocument = require('pdfkit');
const { Server } = require('socket.io');
const { JwtVerifier } = require('aws-jwt-verify');
const { db, firebaseProjectId } = require('./firebase');
const admin = require('firebase-admin');
const { requireAuth, normalizeRole } = require('./auth');
const {
  createUploadRouter,
  validateFileSignature,
} = require('./uploadRouter');
const {
  createPrivacyContactProtector,
  createPrivacyRouter,
} = require('./privacyRouter');
const {
  isAccountDeletionBlocking,
} = require('./accountDeletionService');
const {
  ClientSessionRecoveryError,
  createClientSessionRecoveryService,
} = require('./clientSessionRecovery');
const {
  ACTIVE_TECH_SOCKET_ROOM,
  buildClientIdentityLookupPlan,
  clientSocketRoom,
  createWebSecurityHeadersMiddleware,
  isExplicitlyEnabled,
  isAuthorizedTechProfilePhotoUrl,
  mayReplaceClientUidLink,
  selectClientPhoneForIdentity,
  sessionRoleSocketRoom,
  timingSafeStringEqual,
} = require('./securityPolicy');
const {
  SupportQueuePolicyError,
  buildClientBillingUpdates,
  decideQueueCancellation,
  decideQueueReservation,
  decideTechQueueRemoval,
  decideTechSessionClaim,
  decideTechSupportAvailability,
  evaluateAuthoritativeBilling,
} = require('./supportQueuePolicy');
const {
  SupportSessionClosureError,
  buildSupportSessionClosure,
  localSupportSessionIdFromRealtime,
} = require('./supportSessionClosure');
const {
  TurnCredentialsService,
  evaluateSessionIceAccess,
} = require('./turnCredentials');
const {
  LegacyJoinRateLimiter,
  LegacyRoomAccessError,
  buildLegacyRoomReservationDocument,
  decideLegacyRoomJoin,
  legacyJoinIpKey,
  legacyRoomReservationDocId,
  legacySocketRoomName,
  normalizeLegacyRoomCode,
  normalizeLegacyRoomTtlMs,
  validateLegacySignalAuthorization,
} = require('./legacyRoomAccess');

const LOWERCASE_ID_ALPHABET = 'abcdefghijklmnopqrstuvwxyz0123456789';
const SESSION_ID_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const secureRandomId = (alphabet, length) => {
  if (typeof alphabet !== 'string' || alphabet.length < 2) {
    throw new TypeError('secureRandomId requires an alphabet');
  }
  const safeLength = Math.max(1, Math.min(256, Number(length) || 1));
  let value = '';
  for (let index = 0; index < safeLength; index += 1) {
    value += alphabet[crypto.randomInt(0, alphabet.length)];
  }
  return value;
};
const randomLowercaseId = (length) => secureRandomId(LOWERCASE_ID_ALPHABET, length);
const randomSessionId = () => secureRandomId(SESSION_ID_ALPHABET, 6);

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
const MANUAL_DECLINE_REFUND_MESSAGE =
  'Olá! No momento não estamos disponíveis para realizar este atendimento. Tente novamente mais tarde. O crédito deste acionamento será devolvido. Agradecemos a compreensão.';
const MANUAL_DECLINE_END_REASON =
  'Atendimento encerrado. O crédito deste acionamento foi mantido ou devolvido para você.';
const MANUAL_DECLINE_CLOSE_DELAY_MS = 30_000;
const DEFAULT_FIREBASE_PROJECT_NUMBER_BY_ID = {
  'suporte-x-19ae8': '603259295557',
};
const FPNV_JWKS_URI = 'https://fpnv.googleapis.com/v1beta/jwks';
const DEFAULT_NOTIFICATION_TTL_DAYS = 30;
const NOTIFICATION_TYPES = new Set([
  'APP_UPDATE',
  'APP_UPDATE_REQUIRED',
  'CREDIT_ADDED',
  'CREDIT_AVAILABLE',
  'LOW_CREDITS',
  'NO_CREDITS',
  'FIRST_FREE_AVAILABLE',
  'FIRST_FREE_USED',
  'SESSION_INTERRUPTED',
  'SECURITY_NOTICE',
  'INACTIVE_7_DAYS',
  'INACTIVE_30_DAYS',
  'REVIEW_APP',
  'SHARE_APP',
  'VERIFY_PHONE',
  'COMPLETE_PROFILE',
  'MANUAL_NOTICE',
  'GENERAL_INFO',
]);
const NOTIFICATION_ACTION_TYPES = new Set([
  'OPEN_NOTIFICATIONS',
  'REQUEST_SUPPORT',
  'OPEN_CREDITS',
  'OPEN_PLAY_STORE',
  'OPEN_SHARE_SHEET',
  'OPEN_SECURITY_SETTINGS',
  'OPEN_PERMISSIONS',
  'MARK_AS_READ',
  'DISMISS',
  'NONE',
]);
const NOTIFICATION_PRIORITIES = new Set(['critical', 'high', 'normal', 'low']);
const NOTIFICATION_STATUS = new Set(['unread', 'read', 'dismissed', 'expired', 'failed', 'sent']);
const NOTIFICATION_RULE_SEED = [
  {
    ruleId: 'rule_app_update',
    name: 'Atualizacao disponivel',
    description: 'Notifica clientes com versao instalada menor que a recomendada.',
    type: 'APP_UPDATE',
    enabled: true,
    priority: 'normal',
    conditions: { recommendedVersionCode: 10 },
    notificationTemplate: {
      title: 'Atualizacao disponivel',
      body: 'Atualize o Suporte X para receber correcoes e melhorias.',
      actionLabel: 'Atualizar agora',
      actionType: 'OPEN_PLAY_STORE',
      iconType: 'update',
    },
    delivery: { inApp: true, push: true },
    cooldown: { days: 7 },
    maxOccurrences: 3,
    expiresAfterDays: 30,
  },
  {
    ruleId: 'rule_app_update_required',
    name: 'Atualizacao obrigatoria',
    description: 'Notifica clientes abaixo da versao minima exigida.',
    type: 'APP_UPDATE_REQUIRED',
    enabled: true,
    priority: 'critical',
    conditions: { minimumVersionCode: 10 },
    notificationTemplate: {
      title: 'Atualizacao necessaria',
      body: 'Para continuar usando o Suporte X com seguranca, instale a versao mais recente.',
      actionLabel: 'Atualizar agora',
      actionType: 'OPEN_PLAY_STORE',
      iconType: 'update',
    },
    delivery: { inApp: true, push: true },
    cooldown: { days: 1 },
    maxOccurrences: 30,
    expiresAfterDays: 30,
  },
  {
    ruleId: 'rule_low_credits',
    name: 'Creditos baixos',
    description: 'Notifica clientes com 1 credito restante.',
    type: 'LOW_CREDITS',
    enabled: true,
    priority: 'normal',
    conditions: { creditsLessOrEqual: 1, creditsGreaterOrEqual: 1 },
    notificationTemplate: {
      title: 'Seus creditos estao acabando',
      body: 'Adicione creditos para continuar solicitando suporte quando precisar.',
      actionLabel: 'Adicionar credito',
      actionType: 'OPEN_CREDITS',
      iconType: 'warning',
    },
    delivery: { inApp: true, push: true },
    cooldown: { days: 7 },
    maxOccurrences: 3,
    expiresAfterDays: 30,
  },
  {
    ruleId: 'rule_no_credits',
    name: 'Creditos zerados',
    description: 'Notifica clientes que estao sem creditos.',
    type: 'NO_CREDITS',
    enabled: true,
    priority: 'normal',
    conditions: { creditsEquals: 0, freeFirstSupportUsed: true },
    notificationTemplate: {
      title: 'Voce esta sem creditos',
      body: 'Adicione creditos para solicitar novos atendimentos.',
      actionLabel: 'Adicionar credito',
      actionType: 'OPEN_CREDITS',
      iconType: 'warning',
    },
    delivery: { inApp: true, push: true },
    cooldown: { days: 7 },
    maxOccurrences: 3,
    expiresAfterDays: 30,
  },
  {
    ruleId: 'rule_first_free_available',
    name: 'Primeiro atendimento gratis disponivel',
    description: 'Notifica clientes que ainda nao usaram o primeiro atendimento gratis.',
    type: 'FIRST_FREE_AVAILABLE',
    enabled: true,
    priority: 'normal',
    conditions: { freeFirstSupportUsed: false },
    notificationTemplate: {
      title: 'Primeiro atendimento gratis disponivel',
      body: 'Voce ainda possui um atendimento gratuito para testar o Suporte X.',
      actionLabel: 'Solicitar suporte',
      actionType: 'REQUEST_SUPPORT',
      iconType: 'gift',
    },
    delivery: { inApp: true, push: true },
    cooldown: { days: 14 },
    maxOccurrences: 2,
    expiresAfterDays: 30,
  },
  {
    ruleId: 'rule_review_app',
    name: 'Avalie o app',
    description: 'Solicita avaliacao voluntaria apos atendimentos concluidos.',
    type: 'REVIEW_APP',
    enabled: true,
    priority: 'low',
    conditions: { minimumCompletedSessions: 3 },
    notificationTemplate: {
      title: 'Avalie o Suporte X',
      body: 'Sua opiniao ajuda a melhorar o aplicativo.',
      actionLabel: 'Avaliar agora',
      actionType: 'OPEN_PLAY_STORE',
      iconType: 'star',
    },
    delivery: { inApp: true, push: true },
    cooldown: { days: 30 },
    maxOccurrences: 1,
    expiresAfterDays: 30,
  },
];
let cachedFpnvVerifier = null;
let cachedFpnvVerifierKey = null;

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

const getFirebaseProjectNumber = () =>
  ensureString(
    process.env.FIREBASE_PROJECT_NUMBER ||
      process.env.FPNV_PROJECT_NUMBER ||
      DEFAULT_FIREBASE_PROJECT_NUMBER_BY_ID[firebaseProjectId] ||
      '',
    ''
  ).trim();

const getFpnvVerifier = () => {
  const projectNumber = getFirebaseProjectNumber();
  if (!projectNumber || !firebaseProjectId) return null;
  const verifierKey = `${projectNumber}:${firebaseProjectId}`;
  if (cachedFpnvVerifier && cachedFpnvVerifierKey === verifierKey) return cachedFpnvVerifier;
  const issuer = `https://fpnv.googleapis.com/projects/${projectNumber}`;
  cachedFpnvVerifier = JwtVerifier.create({
    issuer,
    audience: [
      `https://fpnv.googleapis.com/projects/${projectNumber}`,
      `https://fpnv.googleapis.com/projects/${firebaseProjectId}`,
    ],
    jwksUri: FPNV_JWKS_URI,
  });
  cachedFpnvVerifierKey = verifierKey;
  return cachedFpnvVerifier;
};

const verifyFirebasePnvToken = async ({ token = '', expectedPhone = null } = {}) => {
  const safeToken = ensureLongString(token || '', '', 8192).trim();
  if (!safeToken) {
    return { ok: false, error: 'missing_pnv_token' };
  }
  const verifier = getFpnvVerifier();
  if (!verifier) {
    return { ok: false, error: 'pnv_verifier_not_configured' };
  }
  try {
    const payload = await verifier.verify(safeToken);
    const verifiedPhone = normalizePhone(payload?.sub || '');
    if (!verifiedPhone) {
      return { ok: false, error: 'pnv_phone_missing' };
    }
    if (expectedPhone && verifiedPhone !== expectedPhone) {
      return { ok: false, error: 'pnv_phone_mismatch' };
    }
    return { ok: true, phone: verifiedPhone };
  } catch (_error) {
    return { ok: false, error: 'invalid_pnv_token' };
  }
};

const mapPhoneVerificationError = (errorCode = '') => {
  const normalized = ensureString(errorCode || '', '').trim();
  if (normalized === 'invalid_phone_verification_token') {
    return { status: 400, error: normalized, message: 'Token de verificacao por SMS invalido.' };
  }
  if (normalized === 'verification_phone_missing') {
    return { status: 400, error: normalized, message: 'O token de verificacao nao contem telefone.' };
  }
  if (normalized === 'verification_phone_mismatch') {
    return { status: 409, error: normalized, message: 'O telefone verificado por SMS diverge do telefone informado.' };
  }
  return { status: 400, error: 'invalid_payload', message: 'Falha ao validar telefone por SMS.' };
};

const verifySmsPhoneToken = async ({ verificationIdToken = '', expectedPhone = null } = {}) => {
  const safeToken = ensureLongString(verificationIdToken || '', '', 4096).trim();
  if (!safeToken) {
    return { ok: false, error: 'invalid_phone_verification_token' };
  }

  let decoded = null;
  try {
    decoded = await admin.auth().verifyIdToken(safeToken, true);
  } catch (_error) {
    return { ok: false, error: 'invalid_phone_verification_token' };
  }

  const tokenPhone = normalizePhone(decoded?.phone_number || '');
  if (!tokenPhone) {
    return { ok: false, error: 'verification_phone_missing' };
  }
  if (expectedPhone && tokenPhone !== expectedPhone) {
    return { ok: false, error: 'verification_phone_mismatch' };
  }

  return {
    ok: true,
    phone: tokenPhone,
    verificationUid: ensureString(decoded?.uid || '', '').trim() || null,
  };
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

const clientDocIdFromContext = ({ sessionId = '', requestId = '', clientUid = '', deviceAnchor = '' } = {}) => {
  const uidDocId = clientDocIdFromUid(clientUid);
  if (uidDocId) return uidDocId;
  const normalizedSessionId = normalizeSessionId(sessionId).replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 120);
  if (normalizedSessionId) return `session_${normalizedSessionId}`;
  const normalizedRequestId = ensureString(requestId || '', '')
    .trim()
    .replace(/[^a-zA-Z0-9_-]/g, '')
    .slice(0, 120);
  if (normalizedRequestId) return `request_${normalizedRequestId}`;
  const normalizedAnchor = normalizeDeviceAnchor(deviceAnchor);
  if (normalizedAnchor) return `device_${normalizedAnchor}`;
  return null;
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

const stableQueueDocId = (scope, ...values) => {
  const normalizedScope = ensureString(scope || '', '').trim();
  const normalizedValues = values.map((value) => ensureFullString(value || '', '').trim());
  if (!normalizedScope || normalizedValues.some((value) => !value)) return null;
  return crypto
    .createHash('sha256')
    .update([normalizedScope, ...normalizedValues].join('\n'), 'utf8')
    .digest('hex');
};

const queueLockDocIdFromUid = (clientUid) =>
  stableQueueDocId('support_queue_uid', clientUid);

const queueAnchorDocId = (clientUid, localSupportSessionId) =>
  stableQueueDocId('support_queue_anchor', clientUid, localSupportSessionId);

const techSupportLockDocIdFromUid = (techUid) =>
  stableQueueDocId('support_tech_uid', techUid);

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
  if (typeof source.shareActive !== 'boolean' && typeof source.sharing === 'boolean') {
    source.shareActive = source.sharing;
  }
  if (typeof source.remoteActive !== 'boolean' && typeof source.remoteEnabled === 'boolean') {
    source.remoteActive = source.remoteEnabled;
  }
  if (typeof source.callActive !== 'boolean') {
    const hasCalling = typeof source.calling === 'boolean';
    const hasCallConnected = typeof source.callConnected === 'boolean';
    if (hasCalling && hasCallConnected) {
      source.callActive = source.calling || source.callConnected;
    } else if (hasCalling) {
      source.callActive = source.calling;
    } else if (hasCallConnected) {
      source.callActive = source.callConnected;
    }
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
  id: randomLowercaseId(14),
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

const DEFAULT_TECH_LOGIN_TURNSTILE_ALLOWED_HOSTNAMES = ['suportex.app', 'www.suportex.app', 'localhost', '127.0.0.1'];
const TECH_LOGIN_TURNSTILE_ACTION = 'tech_login';
const PRIVACY_DELETION_TURNSTILE_ACTION = 'privacy_deletion_request';
const TURNSTILE_SITEVERIFY_URL = 'https://challenges.cloudflare.com/turnstile/v0/siteverify';
const TURNSTILE_VERIFY_TIMEOUT_MS = 8000;
const resolveTechLoginTurnstileConfig = () => {
  const siteKey = ensureString(
    process.env.TECH_LOGIN_TURNSTILE_SITE_KEY || process.env.TURNSTILE_TECH_LOGIN_SITE_KEY || '',
    ''
  ).trim();
  const secretKey = ensureFullString(
    process.env.TECH_LOGIN_TURNSTILE_SECRET_KEY || process.env.TURNSTILE_TECH_LOGIN_SECRET_KEY || '',
    ''
  ).trim();
  const rawEnabled = ensureString(
    process.env.TECH_LOGIN_TURNSTILE_ENABLED || process.env.TURNSTILE_TECH_LOGIN_ENABLED || '',
    ''
  )
    .trim()
    .toLowerCase();
  const enabledByDefault = Boolean(siteKey && secretKey);
  const enabled =
    rawEnabled === ''
      ? enabledByDefault
      : rawEnabled === '1' || rawEnabled === 'true' || rawEnabled === 'yes' || rawEnabled === 'on';

  const rawHostnames = ensureString(
    process.env.TECH_LOGIN_TURNSTILE_ALLOWED_HOSTNAMES || process.env.TURNSTILE_TECH_LOGIN_ALLOWED_HOSTNAMES || '',
    ''
  )
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);
  const allowedHostnames = rawHostnames.length
    ? Array.from(new Set(rawHostnames))
    : [...DEFAULT_TECH_LOGIN_TURNSTILE_ALLOWED_HOSTNAMES];

  return {
    enabled: enabled && Boolean(siteKey && secretKey),
    siteKey,
    secretKey,
    allowedHostnames,
  };
};

const getTechLoginTurnstilePublicConfig = () => {
  const config = resolveTechLoginTurnstileConfig();
  if (!config.enabled) {
    return {
      enabled: false,
      provider: 'cloudflare_turnstile',
      siteKey: '',
    };
  }

  return {
    enabled: true,
    provider: 'cloudflare_turnstile',
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
      req.destroy(new Error('turnstile_request_timeout'));
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

const mapTurnstileRuntimeError = (error) => {
  const rawMessage = ensureFullString(error?.message || '', '');
  const message = rawMessage.toLowerCase();

  if (!message) {
    return {
      error: 'captcha_verification_failed',
      message: 'N\u00E3o foi poss\u00EDvel validar a prote\u00E7\u00E3o anti-bot agora. Tente novamente em instantes.',
      hint: null,
    };
  }

  if (message.includes('turnstile_request_timeout') || message.includes('timed out') || message.includes('timeout')) {
    return {
      error: 'captcha_timeout',
      message: 'A valida\u00E7\u00E3o anti-bot excedeu o tempo limite. Tente novamente.',
      hint: 'timeout',
    };
  }

  if (message.includes('secret') || message.includes('permission') || message.includes('invalid-input-secret')) {
    return {
      error: 'captcha_secret_invalid',
      message: 'Chave secreta do Cloudflare Turnstile inv\u00E1lida ou ausente no servidor.',
      hint: 'check_turnstile_secret_key',
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
    message: 'N\u00E3o foi poss\u00EDvel validar a prote\u00E7\u00E3o anti-bot agora. Tente novamente em instantes.',
    hint: null,
  };
};

const verifyTechLoginTurnstileToken = async ({
  token = '',
  remoteIpAddress = '',
  isProduction = false,
  expectedAction = TECH_LOGIN_TURNSTILE_ACTION,
}) => {
  const config = resolveTechLoginTurnstileConfig();
  if (!config.enabled) {
    return { ok: false, status: 503, error: 'captcha_unavailable' };
  }
  if (!config.secretKey) {
    return { ok: false, status: 503, error: 'captcha_secret_missing' };
  }

  const requestBody = {
    secret: config.secretKey,
    response: token,
  };
  if (remoteIpAddress) requestBody.remoteip = remoteIpAddress;

  const rawBody = JSON.stringify(requestBody);
  let payload = {};
  const response = await postJsonWithRuntimeFallback({
    url: TURNSTILE_SITEVERIFY_URL,
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(rawBody, 'utf8'),
    },
    body: rawBody,
    timeoutMs: TURNSTILE_VERIFY_TIMEOUT_MS,
  });
  try {
    payload = JSON.parse(ensureFullString(response.text || '', '{}') || '{}');
  } catch (_error) {
    payload = {};
  }

  if (!response.ok) {
    const responseMessage = ensureString(payload?.error || payload?.message || '', '').trim();
    const statusText = ensureString(response.statusText || '', '').trim();
    const fallbackMessage = `turnstile_http_${response.status}`;
    throw new Error(responseMessage || statusText || fallbackMessage);
  }

  if (payload?.success !== true) {
    const errorCodes = ensureArray(payload?.['error-codes'])
      .map((value) => ensureString(value || '', '').trim())
      .filter(Boolean);
    if (errorCodes.some((code) => code === 'missing-input-secret' || code === 'invalid-input-secret')) {
      return {
        ok: false,
        status: 503,
        error: 'captcha_secret_invalid',
        invalidReason: errorCodes.join(',') || 'secret_invalid',
      };
    }
    return {
      ok: false,
      status: 403,
      error: 'captcha_invalid',
      invalidReason: errorCodes.join(',') || 'invalid',
    };
  }

  const hostname = ensureString(payload?.hostname || '', '').trim().toLowerCase();
  if (
    isProduction &&
    config.allowedHostnames.length &&
    (!hostname || !config.allowedHostnames.includes(hostname))
  ) {
    return {
      ok: false,
      status: 403,
      error: 'captcha_hostname_mismatch',
      hostname,
    };
  }

  const action = ensureString(payload?.action || '', '').trim();
  if (expectedAction && action !== expectedAction) {
    return {
      ok: false,
      status: 403,
      error: 'captcha_action_mismatch',
      action,
    };
  }

  return { ok: true, hostname: hostname || null, action: action || null };
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

const getQueueNotificationsCollection = () => {
  if (!db) return null;
  try {
    return db.collection('queue_notifications');
  } catch (err) {
    console.error('Failed to access queue_notifications collection', err);
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

const persistQueueNotification = async ({ requestId = '', requestData = {}, state = 'queued', sessionId = null, techUid = null, techName = null, reason = null } = {}) => {
  const normalizedRequestId = ensureString(requestId || requestData.requestId || '', '').trim().slice(0, 128);
  if (!normalizedRequestId) return;
  const collection = getQueueNotificationsCollection();
  if (!collection) return;
  const now = Date.now();
  const createdAt = parseReportTimestamp(requestData.createdAt || requestData.requestedAt || null, now) || now;
  const payload = {
    requestId: normalizedRequestId,
    state: ensureString(state || requestData.state || 'queued', 'queued'),
    clientName: ensureString(requestData.clientName || 'Cliente', 'Cliente'),
    clientPhone: normalizePhone(requestData.clientPhone || '') || null,
    clientUid: ensureString(requestData.clientUid || '', '').trim() || null,
    clientRecordId: ensureString(requestData.clientRecordId || '', '').trim() || null,
    brand: ensureString(requestData.brand || '', '').trim() || null,
    model: ensureString(requestData.model || '', '').trim() || null,
    osVersion: ensureString(requestData.osVersion || '', '').trim() || null,
    issue: ensureString(requestData.issue || '', '').trim() || null,
    plan: ensureString(requestData.plan || '', '').trim() || null,
    createdAt,
    updatedAt: now,
  };
  if (sessionId) payload.sessionId = ensureString(sessionId || '', '').trim() || null;
  if (techUid) payload.techUid = ensureString(techUid || '', '').trim() || null;
  if (techName) payload.techName = ensureString(techName || '', '').trim() || null;
  if (reason) payload.reason = ensureString(reason || '', '').trim() || null;
  try {
    await collection.doc(normalizedRequestId).set(payload, { merge: true });
  } catch (error) {
    console.error('Failed to persist queue notification', error);
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

const getSupportQueueLocksCollection = () => {
  if (!db) return null;
  try {
    return db.collection('support_queue_locks');
  } catch (err) {
    console.error('Failed to access support_queue_locks collection', err);
    return null;
  }
};

const getSupportQueueAnchorsCollection = () => {
  if (!db) return null;
  try {
    return db.collection('support_queue_anchors');
  } catch (err) {
    console.error('Failed to access support_queue_anchors collection', err);
    return null;
  }
};

const getSupportQueueOutcomesCollection = () => {
  if (!db) return null;
  try {
    return db.collection('support_queue_outcomes');
  } catch (err) {
    console.error('Failed to access support_queue_outcomes collection', err);
    return null;
  }
};

const getSupportTechLocksCollection = () => {
  if (!db) return null;
  try {
    return db.collection('support_tech_locks');
  } catch (err) {
    console.error('Failed to access support_tech_locks collection', err);
    return null;
  }
};

const getLegacyWebrtcRoomsCollection = () => {
  if (!db) return null;
  try {
    return db.collection('legacy_webrtc_rooms');
  } catch (err) {
    console.error('Failed to access legacy_webrtc_rooms collection', err);
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

const getClientDevicesCollection = () => {
  if (!db) return null;
  try {
    return db.collection('client_devices');
  } catch (err) {
    console.error('Failed to access client_devices collection', err);
    return null;
  }
};

const getClientNotificationsCollection = () => {
  if (!db) return null;
  try {
    return db.collection('client_notifications');
  } catch (err) {
    console.error('Failed to access client_notifications collection', err);
    return null;
  }
};

const getNotificationCampaignsCollection = () => {
  if (!db) return null;
  try {
    return db.collection('notification_campaigns');
  } catch (err) {
    console.error('Failed to access notification_campaigns collection', err);
    return null;
  }
};

const getNotificationRulesCollection = () => {
  if (!db) return null;
  try {
    return db.collection('notification_rules');
  } catch (err) {
    console.error('Failed to access notification_rules collection', err);
    return null;
  }
};

const getNotificationEventsCollection = () => {
  if (!db) return null;
  try {
    return db.collection('notification_events');
  } catch (err) {
    console.error('Failed to access notification_events collection', err);
    return null;
  }
};

const getAdminNotificationsCollection = () => {
  if (!db) return null;
  try {
    return db.collection('admin_notifications');
  } catch (err) {
    console.error('Failed to access admin_notifications collection', err);
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

const notificationHash = (value = '') =>
  crypto.createHash('sha256').update(ensureFullString(value || '', '')).digest('hex').slice(0, 40);

const normalizeNotificationType = (value = '', fallback = 'MANUAL_NOTICE') => {
  const normalized = ensureString(value || '', '').trim().toUpperCase();
  if (NOTIFICATION_TYPES.has(normalized)) return normalized;
  const label = ensureFullString(value || '', '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();
  if (label.includes('atualizacao obrig') || label.includes('necessaria')) return 'APP_UPDATE_REQUIRED';
  if (label.includes('atualizacao')) return 'APP_UPDATE';
  if (label.includes('credito adicionado')) return 'CREDIT_ADDED';
  if (label.includes('credito disponivel')) return 'CREDIT_AVAILABLE';
  if (label.includes('credito') && (label.includes('baixo') || label.includes('acabando'))) return 'LOW_CREDITS';
  if (label.includes('sem credito') || label.includes('zerado')) return 'NO_CREDITS';
  if (label.includes('primeiro')) return 'FIRST_FREE_AVAILABLE';
  if (label.includes('sessao interrompida')) return 'SESSION_INTERRUPTED';
  if (label.includes('seguranca')) return 'SECURITY_NOTICE';
  if (label.includes('inativo') && label.includes('30')) return 'INACTIVE_30_DAYS';
  if (label.includes('inativo')) return 'INACTIVE_7_DAYS';
  if (label.includes('avalie') || label.includes('avaliar')) return 'REVIEW_APP';
  if (label.includes('compartilh')) return 'SHARE_APP';
  return NOTIFICATION_TYPES.has(fallback) ? fallback : 'MANUAL_NOTICE';
};

const normalizeNotificationActionType = (value = '', fallback = 'NONE') => {
  const normalized = ensureString(value || '', '').trim().toUpperCase();
  if (NOTIFICATION_ACTION_TYPES.has(normalized)) return normalized;
  const label = ensureFullString(value || '', '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();
  if (label.includes('solicitar')) return 'REQUEST_SUPPORT';
  if (label.includes('credito') || label.includes('resgatar') || label.includes('adicionar')) return 'OPEN_CREDITS';
  if (label.includes('atualizar') || label.includes('avaliar')) return 'OPEN_PLAY_STORE';
  if (label.includes('compartilh')) return 'OPEN_SHARE_SHEET';
  if (label.includes('permiss')) return 'OPEN_PERMISSIONS';
  if (label.includes('seguranca')) return 'OPEN_SECURITY_SETTINGS';
  if (label.includes('entendi') || label.includes('lida')) return 'MARK_AS_READ';
  return NOTIFICATION_ACTION_TYPES.has(fallback) ? fallback : 'NONE';
};

const iconTypeForNotification = (type = '', iconType = '') => {
  const normalizedIcon = ensureString(iconType || '', '').trim().toLowerCase();
  if (normalizedIcon) return normalizedIcon.slice(0, 32);
  switch (normalizeNotificationType(type)) {
    case 'APP_UPDATE':
    case 'APP_UPDATE_REQUIRED':
      return 'update';
    case 'CREDIT_ADDED':
    case 'CREDIT_AVAILABLE':
    case 'FIRST_FREE_AVAILABLE':
      return 'gift';
    case 'LOW_CREDITS':
    case 'NO_CREDITS':
    case 'SESSION_INTERRUPTED':
      return 'warning';
    case 'SECURITY_NOTICE':
      return 'security';
    case 'REVIEW_APP':
      return 'star';
    case 'SHARE_APP':
      return 'share';
    default:
      return 'info';
  }
};

const normalizeNotificationPriority = (priority = '', type = '') => {
  const normalized = ensureString(priority || '', '').trim().toLowerCase();
  if (NOTIFICATION_PRIORITIES.has(normalized)) return normalized;
  if (type === 'APP_UPDATE_REQUIRED' || type === 'SECURITY_NOTICE') return 'critical';
  if (type === 'SESSION_INTERRUPTED') return 'high';
  if (type === 'REVIEW_APP' || type === 'SHARE_APP' || type.startsWith('INACTIVE_')) return 'low';
  return 'normal';
};

const normalizeNotificationStatus = (value = '', fallback = 'unread') => {
  const normalized = ensureString(value || '', '').trim().toLowerCase();
  if (NOTIFICATION_STATUS.has(normalized)) return normalized;
  return fallback;
};

const normalizeDelivery = (delivery = {}) => ({
  inApp: delivery?.inApp !== false,
  push: delivery?.push === true,
});

const toNotificationMillis = (value, fallback = null) => {
  const parsed = parseReportTimestamp(value, null);
  if (Number.isFinite(Number(parsed))) return Number(parsed);
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

const actionLabelForType = (type = '', actionType = 'NONE', explicit = '') => {
  const label = ensureString(explicit || '', '').trim();
  if (label) return label.slice(0, 48);
  switch (actionType) {
    case 'REQUEST_SUPPORT':
      return 'Solicitar suporte';
    case 'OPEN_CREDITS':
      return type === 'CREDIT_AVAILABLE' ? 'Resgatar' : 'Adicionar credito';
    case 'OPEN_PLAY_STORE':
      return type === 'REVIEW_APP' ? 'Avaliar agora' : 'Atualizar agora';
    case 'OPEN_SHARE_SHEET':
      return 'Compartilhar';
    case 'MARK_AS_READ':
      return 'Entendi';
    default:
      return '';
  }
};

const buildNotificationPayload = ({
  client = {},
  clientUid = null,
  title = '',
  body = '',
  type = 'MANUAL_NOTICE',
  iconType = '',
  priority = '',
  actionLabel = '',
  actionType = 'NONE',
  actionPayload = {},
  delivery = {},
  source = 'manual',
  createdBy = null,
  campaignId = null,
  ruleId = null,
  expiresAfterDays = DEFAULT_NOTIFICATION_TTL_DAYS,
  expiresAt = null,
  dedupeKey = '',
} = {}) => {
  const normalizedType = normalizeNotificationType(type);
  const normalizedActionType = normalizeNotificationActionType(actionType);
  const normalizedDelivery = normalizeDelivery(delivery);
  const now = Date.now();
  const safeTitle = ensureString(title || '', '').trim().slice(0, 96) || 'Notificacao';
  const safeBody = ensureLongString(body || '', '', 600).trim();
  const clientId = ensureString(client?.id || client?.clientId || '', '').trim() || null;
  const normalizedClientUid = ensureString(clientUid || client?.clientUid || '', '').trim() || null;
  const resolvedDedupeKey =
    ensureString(dedupeKey || '', '').trim() ||
    [
      normalizedClientUid || clientId || 'unknown-client',
      normalizedType,
      campaignId || '',
      ruleId || '',
      notificationHash(`${safeTitle}:${safeBody}:${normalizedActionType}`),
    ].join(':');
  const ttlDays = Math.max(1, Math.min(365, ensureInteger(expiresAfterDays, DEFAULT_NOTIFICATION_TTL_DAYS)));
  const resolvedExpiresAt = toNotificationMillis(expiresAt, null) || now + ttlDays * 24 * 60 * 60 * 1000;
  const docId = `notif_${notificationHash(resolvedDedupeKey)}`;
  return {
    id: docId,
    clientId,
    clientUid: normalizedClientUid,
    title: safeTitle,
    body: safeBody,
    type: normalizedType,
    iconType: iconTypeForNotification(normalizedType, iconType),
    priority: normalizeNotificationPriority(priority, normalizedType),
    status: 'unread',
    read: false,
    dismissed: false,
    actionLabel: actionLabelForType(normalizedType, normalizedActionType, actionLabel) || null,
    actionType: normalizedActionType,
    actionPayload: actionPayload && typeof actionPayload === 'object' ? actionPayload : {},
    delivery: normalizedDelivery,
    source: ensureString(source || 'manual', 'manual').trim().slice(0, 48) || 'manual',
    createdAt: now,
    updatedAt: now,
    readAt: null,
    dismissedAt: null,
    expiresAt: resolvedExpiresAt,
    createdBy: createdBy || null,
    campaignId: campaignId || null,
    ruleId: ruleId || null,
    dedupeKey: resolvedDedupeKey,
  };
};

const toClientNotificationSummary = (doc) => {
  const data = doc.data ? doc.data() || {} : doc || {};
  const status = normalizeNotificationStatus(data.status || (data.read ? 'read' : 'unread'));
  return {
    id: ensureString(data.id || doc.id || '', '').trim() || doc.id,
    clientId: ensureString(data.clientId || '', '').trim() || null,
    clientUid: ensureString(data.clientUid || '', '').trim() || null,
    title: ensureString(data.title || 'Notificacao', 'Notificacao'),
    body: ensureLongString(data.body || '', '', 600),
    type: normalizeNotificationType(data.type || 'MANUAL_NOTICE'),
    iconType: ensureString(data.iconType || '', '').trim() || 'info',
    priority: normalizeNotificationPriority(data.priority || '', data.type || ''),
    status,
    read: data.read === true || status === 'read',
    dismissed: data.dismissed === true || status === 'dismissed',
    actionLabel: ensureString(data.actionLabel || '', '').trim() || null,
    actionType: normalizeNotificationActionType(data.actionType || 'NONE'),
    delivery: normalizeDelivery(data.delivery || {}),
    source: ensureString(data.source || '', '').trim() || null,
    createdAt: toNotificationMillis(data.createdAt, null),
    updatedAt: toNotificationMillis(data.updatedAt, null),
    readAt: toNotificationMillis(data.readAt, null),
    dismissedAt: toNotificationMillis(data.dismissedAt, null),
    expiresAt: toNotificationMillis(data.expiresAt, null),
    createdBy: data.createdBy || null,
    campaignId: ensureString(data.campaignId || '', '').trim() || null,
    ruleId: ensureString(data.ruleId || '', '').trim() || null,
  };
};

const recordNotificationEvent = async (eventType, payload = {}) => {
  const collection = getNotificationEventsCollection();
  if (!collection) return null;
  const now = Date.now();
  const safeType = ensureString(eventType || '', '').trim().toUpperCase() || 'NOTIFICATION_EVENT';
  const event = {
    eventType: safeType,
    notificationId: ensureString(payload.notificationId || '', '').trim() || null,
    clientId: ensureString(payload.clientId || '', '').trim() || null,
    clientUid: ensureString(payload.clientUid || '', '').trim() || null,
    campaignId: ensureString(payload.campaignId || '', '').trim() || null,
    ruleId: ensureString(payload.ruleId || '', '').trim() || null,
    actorUid: ensureString(payload.actorUid || '', '').trim() || null,
    actorName: ensureString(payload.actorName || '', '').trim() || null,
    status: ensureString(payload.status || '', '').trim() || null,
    error: ensureLongString(payload.error || '', '', 1000).trim() || null,
    metadata: payload.metadata && typeof payload.metadata === 'object' ? payload.metadata : {},
    createdAt: now,
  };
  try {
    const doc = await collection.add(event);
    return { id: doc.id, ...event };
  } catch (error) {
    console.error('Failed to record notification event', error);
    return null;
  }
};

const createAdminNotification = async ({ title = '', body = '', type = 'GENERAL_INFO', iconType = 'bell', actorUid = null, metadata = {} } = {}) => {
  const collection = getAdminNotificationsCollection();
  if (!collection) return null;
  const now = Date.now();
  const payload = {
    title: ensureString(title || '', '').trim().slice(0, 96) || 'Notificacao administrativa',
    body: ensureLongString(body || '', '', 500).trim(),
    type: normalizeNotificationType(type, 'GENERAL_INFO'),
    iconType: ensureString(iconType || 'bell', 'bell').trim().slice(0, 32) || 'bell',
    read: false,
    status: 'unread',
    actorUid: ensureString(actorUid || '', '').trim() || null,
    metadata: metadata && typeof metadata === 'object' ? metadata : {},
    createdAt: now,
    updatedAt: now,
  };
  try {
    const doc = await collection.add(payload);
    return { id: doc.id, ...payload };
  } catch (error) {
    console.error('Failed to create admin notification', error);
    return null;
  }
};

const resolveClientUidForClientId = async (clientId = '') => {
  const normalizedClientId = ensureString(clientId || '', '').trim();
  if (!normalizedClientId) return null;
  const linksCollection = getClientAppLinksCollection();
  if (!linksCollection) return null;
  try {
    const snapshot = await linksCollection.where('clientId', '==', normalizedClientId).limit(5).get();
    const docs = snapshot.docs
      .map((doc) => ({ id: doc.id, data: doc.data() || {} }))
      .sort((a, b) => Number(b.data.updatedAt || 0) - Number(a.data.updatedAt || 0));
    const link = docs.find((item) => ensureString(item.data.clientUid || item.id || '', '').trim());
    return ensureString(link?.data?.clientUid || link?.id || '', '').trim() || null;
  } catch (error) {
    console.error('Failed to resolve clientUid for notification', error);
    return null;
  }
};

const loadClientNotificationTokens = async ({ clientId = '', clientUid = '' } = {}) => {
  const devicesCollection = getClientDevicesCollection();
  if (!devicesCollection) return [];
  const tokens = new Map();
  const queries = [];
  const normalizedClientId = ensureString(clientId || '', '').trim();
  const normalizedClientUid = ensureString(clientUid || '', '').trim();
  if (normalizedClientId) queries.push(devicesCollection.where('clientId', '==', normalizedClientId).limit(20).get());
  if (normalizedClientUid) queries.push(devicesCollection.where('clientUid', '==', normalizedClientUid).limit(20).get());
  try {
    const snapshots = await Promise.all(queries);
    snapshots.forEach((snapshot) => {
      snapshot.docs.forEach((doc) => {
        const data = doc.data() || {};
        if (data.active === false) return;
        const token = ensureString(data.fcmToken || data.token || '', '').trim();
        if (!token) return;
        tokens.set(token, {
          token,
          deviceId: doc.id,
          clientId: ensureString(data.clientId || normalizedClientId || '', '').trim() || null,
          clientUid: ensureString(data.clientUid || normalizedClientUid || '', '').trim() || null,
        });
      });
    });
  } catch (error) {
    console.error('Failed to load FCM tokens for notification', error);
  }
  return Array.from(tokens.values()).slice(0, 20);
};

const sendClientNotificationPush = async (notification = {}) => {
  if (!notification?.delivery?.push) {
    return { status: 'skipped', reason: 'push_not_requested', sent: 0, failed: 0 };
  }
  const targets = await loadClientNotificationTokens({
    clientId: notification.clientId,
    clientUid: notification.clientUid,
  });
  if (!targets.length) {
    return { status: 'skipped', reason: 'no_active_token', sent: 0, failed: 0 };
  }
  const messaging = admin.messaging();
  let sent = 0;
  let failed = 0;
  const errors = [];
  for (const target of targets) {
    try {
      await messaging.send({
        token: target.token,
        data: {
          type: 'client_notification',
          notificationId: ensureString(notification.id || '', '').trim(),
          notificationType: normalizeNotificationType(notification.type || ''),
          title: ensureString(notification.title || '', ''),
          body: ensureLongString(notification.body || '', '', 600),
          actionType: normalizeNotificationActionType(notification.actionType || 'NONE'),
          actionLabel: ensureString(notification.actionLabel || '', ''),
          clientId: ensureString(notification.clientId || '', ''),
        },
        android: {
          priority: 'high',
        },
      });
      sent += 1;
    } catch (error) {
      failed += 1;
      const reason = ensureString(error?.code || error?.message || 'push_failed', 'push_failed');
      errors.push(reason);
      if (String(reason).includes('registration-token-not-registered')) {
        const devicesCollection = getClientDevicesCollection();
        await devicesCollection?.doc(target.deviceId).set(
          {
            active: false,
            disabledAt: Date.now(),
            disabledReason: 'registration_token_not_registered',
          },
          { merge: true }
        );
      }
    }
  }
  return {
    status: sent > 0 ? 'sent' : 'error',
    reason: sent > 0 ? null : errors[0] || 'push_failed',
    sent,
    failed,
    errors: errors.slice(0, 5),
  };
};

const createClientNotification = async (input = {}) => {
  const notificationsCollection = getClientNotificationsCollection();
  if (!notificationsCollection) {
    return { ok: false, error: 'firestore_unavailable' };
  }
  const client = input.client || {};
  const clientId = ensureString(client.id || client.clientId || input.clientId || '', '').trim() || null;
  const clientUid =
    ensureString(input.clientUid || client.clientUid || '', '').trim() ||
    (clientId ? await resolveClientUidForClientId(clientId) : null);
  if (!clientId && !clientUid) {
    return { ok: false, error: 'client_target_required' };
  }
  const payload = buildNotificationPayload({
    ...input,
    client: { ...client, id: clientId },
    clientUid,
  });
  if (!payload.delivery.inApp) {
    payload.status = 'sent';
    payload.read = true;
  }

  let created = false;
  try {
    const docRef = notificationsCollection.doc(payload.id);
    await db.runTransaction(async (tx) => {
      const snap = await tx.get(docRef);
      if (snap.exists) {
        const current = snap.data() || {};
        const active =
          current.dismissed !== true &&
          normalizeNotificationStatus(current.status || 'unread') !== 'expired' &&
          (!current.expiresAt || toNotificationMillis(current.expiresAt, Date.now() + 1) > Date.now());
        if (active) return;
      }
      tx.set(docRef, payload, { merge: true });
      created = true;
    });
  } catch (error) {
    console.error('Failed to create client notification', error);
    return { ok: false, error: 'server_error' };
  }

  if (!created) {
    return { ok: true, created: false, duplicate: true, notification: payload };
  }

  const pushResult = await sendClientNotificationPush(payload);
  await notificationsCollection.doc(payload.id).set(
    {
      delivery: {
        ...payload.delivery,
        pushStatus: pushResult.status,
        pushSent: pushResult.sent || 0,
        pushFailed: pushResult.failed || 0,
        pushReason: pushResult.reason || null,
      },
      updatedAt: Date.now(),
    },
    { merge: true }
  );
  await recordNotificationEvent('NOTIFICATION_CREATED', {
    notificationId: payload.id,
    clientId: payload.clientId,
    clientUid: payload.clientUid,
    campaignId: payload.campaignId,
    ruleId: payload.ruleId,
    actorUid: payload.createdBy?.uid || payload.createdBy || null,
    status: 'created',
  });
  if (pushResult.status === 'sent') {
    await recordNotificationEvent('NOTIFICATION_PUSH_SENT', {
      notificationId: payload.id,
      clientId: payload.clientId,
      clientUid: payload.clientUid,
      campaignId: payload.campaignId,
      ruleId: payload.ruleId,
      status: 'sent',
      metadata: { sent: pushResult.sent, failed: pushResult.failed },
    });
  } else if (payload.delivery.push) {
    await recordNotificationEvent('NOTIFICATION_PUSH_FAILED', {
      notificationId: payload.id,
      clientId: payload.clientId,
      clientUid: payload.clientUid,
      campaignId: payload.campaignId,
      ruleId: payload.ruleId,
      status: pushResult.status,
      error: pushResult.reason,
    });
  }
  return { ok: true, created: true, notification: payload, push: pushResult };
};

const ensureDefaultNotificationRules = async () => {
  const rulesCollection = getNotificationRulesCollection();
  if (!rulesCollection) return [];
  const now = Date.now();
  const rows = [];
  for (const seed of NOTIFICATION_RULE_SEED) {
    const ref = rulesCollection.doc(seed.ruleId);
    try {
      const snap = await ref.get();
      if (!snap.exists) {
        await ref.set({
          ...seed,
          createdAt: now,
          updatedAt: now,
          seeded: true,
        });
        rows.push({ ...seed, createdAt: now, updatedAt: now });
      } else {
        rows.push({ ruleId: snap.id, ...(snap.data() || {}) });
      }
    } catch (error) {
      console.error('Failed to ensure notification rule', error);
    }
  }
  return rows;
};

const toNotificationRuleSummary = (docOrData = {}) => {
  const data = typeof docOrData.data === 'function' ? docOrData.data() || {} : docOrData || {};
  const ruleId = ensureString(data.ruleId || docOrData.id || '', '').trim();
  const type = normalizeNotificationType(data.type || ruleId || 'GENERAL_INFO');
  const template = data.notificationTemplate && typeof data.notificationTemplate === 'object' ? data.notificationTemplate : {};
  const conditions = data.conditions && typeof data.conditions === 'object' ? data.conditions : {};
  const cooldown = data.cooldown && typeof data.cooldown === 'object' ? data.cooldown : {};
  const delivery = normalizeDelivery(data.delivery || {});
  return {
    ruleId,
    name: ensureString(data.name || ruleId || 'Regra automatica', 'Regra automatica'),
    description: ensureLongString(data.description || '', '', 500),
    type,
    enabled: data.enabled !== false,
    priority: normalizeNotificationPriority(data.priority || '', type),
    conditions,
    notificationTemplate: {
      title: ensureString(template.title || data.title || '', '').trim() || type,
      body: ensureLongString(template.body || data.body || '', '', 600).trim(),
      actionLabel: ensureString(template.actionLabel || '', '').trim() || null,
      actionType: normalizeNotificationActionType(template.actionType || 'NONE'),
      iconType: iconTypeForNotification(type, template.iconType || data.iconType || ''),
    },
    delivery,
    cooldown: { days: Math.max(0, ensureInteger(cooldown.days, 0)) },
    maxOccurrences: Math.max(1, ensureInteger(data.maxOccurrences, 1)),
    expiresAfterDays: Math.max(1, ensureInteger(data.expiresAfterDays, DEFAULT_NOTIFICATION_TTL_DAYS)),
    createdAt: toNotificationMillis(data.createdAt, null),
    updatedAt: toNotificationMillis(data.updatedAt, null),
    lastRunAt: toNotificationMillis(data.lastRunAt, null),
  };
};

const listNotificationRules = async () => {
  const rulesCollection = getNotificationRulesCollection();
  if (!rulesCollection) return [];
  await ensureDefaultNotificationRules();
  try {
    const snapshot = await rulesCollection.get();
    return snapshot.docs
      .map(toNotificationRuleSummary)
      .sort((a, b) => String(a.name || '').localeCompare(String(b.name || ''), 'pt-BR'));
  } catch (error) {
    console.error('Failed to list notification rules', error);
    return [];
  }
};

const loadClientDevicesByClientId = async () => {
  const devicesCollection = getClientDevicesCollection();
  if (!devicesCollection) return new Map();
  const byClientId = new Map();
  try {
    const snapshot = await devicesCollection.limit(1000).get();
    snapshot.docs.forEach((doc) => {
      const data = doc.data() || {};
      const clientId = ensureString(data.clientId || '', '').trim();
      if (!clientId) return;
      const list = byClientId.get(clientId) || [];
      list.push({ id: doc.id, ...data });
      byClientId.set(clientId, list);
    });
  } catch (error) {
    console.error('Failed to load client devices', error);
  }
  return byClientId;
};

const loadVerificationsByClientId = async () => {
  const verificationsCollection = getClientVerificationsCollection();
  if (!verificationsCollection) return new Map();
  const byClientId = new Map();
  try {
    const snapshot = await verificationsCollection.limit(1000).get();
    snapshot.docs.forEach((doc) => {
      byClientId.set(doc.id, { id: doc.id, ...(doc.data() || {}) });
    });
  } catch (error) {
    console.error('Failed to load client verifications', error);
  }
  return byClientId;
};

const loadProfilesByClientId = async () => {
  const profilesCollection = getClientProfilesCollection();
  if (!profilesCollection) return new Map();
  const byClientId = new Map();
  try {
    const snapshot = await profilesCollection.limit(1000).get();
    snapshot.docs.forEach((doc) => {
      byClientId.set(doc.id, { id: doc.id, ...(doc.data() || {}) });
    });
  } catch (error) {
    console.error('Failed to load client profiles', error);
  }
  return byClientId;
};

const getLatestClientDevice = (devices = []) =>
  ensureArray(devices)
    .slice()
    .sort((a, b) => Number(b.lastSeenAt || b.updatedAt || 0) - Number(a.lastSeenAt || a.updatedAt || 0))[0] || null;

const getCurrentVersionRuleConfig = (rules = []) => {
  const recommended = rules.find((rule) => rule.type === 'APP_UPDATE')?.conditions || {};
  const minimum = rules.find((rule) => rule.type === 'APP_UPDATE_REQUIRED')?.conditions || {};
  return {
    recommendedVersionCode: Math.max(0, ensureInteger(recommended.recommendedVersionCode, 0)),
    minimumVersionCode: Math.max(0, ensureInteger(minimum.minimumVersionCode, 0)),
  };
};

const buildClientAudienceSnapshot = async () => {
  const clientsCollection = getClientsCollection();
  if (!clientsCollection) return [];
  const [devicesByClientId, verificationsByClientId, profilesByClientId, rules] = await Promise.all([
    loadClientDevicesByClientId(),
    loadVerificationsByClientId(),
    loadProfilesByClientId(),
    listNotificationRules(),
  ]);
  const versionConfig = getCurrentVersionRuleConfig(rules);
  try {
    const snapshot = await clientsCollection.limit(1000).get();
    return snapshot.docs.map((doc) => {
      const client = toClientSummary(doc.id, doc.data() || {});
      const devices = devicesByClientId.get(doc.id) || [];
      const latestDevice = getLatestClientDevice(devices);
      const verification = verificationsByClientId.get(doc.id) || null;
      const profile = profilesByClientId.get(doc.id) || null;
      const lastSeenAt = Number(latestDevice?.lastSeenAt || client.updatedAt || client.createdAt || 0) || null;
      const appVersionCode = Math.max(0, ensureInteger(latestDevice?.appVersionCode, 0));
      return {
        client,
        devices,
        latestDevice,
        verification,
        profile,
        lastSeenAt,
        appVersionCode,
        appOutdated: versionConfig.recommendedVersionCode > 0 && appVersionCode > 0 && appVersionCode < versionConfig.recommendedVersionCode,
        appUpdateRequired: versionConfig.minimumVersionCode > 0 && appVersionCode > 0 && appVersionCode < versionConfig.minimumVersionCode,
        verified: ensureString(verification?.status || '', '').toLowerCase() === 'verified',
      };
    });
  } catch (error) {
    console.error('Failed to build client audience snapshot', error);
    return [];
  }
};

const clientMatchesAudienceFilter = (item = {}, filter = '') => {
  const normalized = ensureString(filter || '', '').trim();
  const client = item.client || {};
  const credits = Math.max(0, ensureInteger(client.credits, 0));
  const now = Date.now();
  const lastSeenAt = Number(item.lastSeenAt || 0);
  const daysInactive = lastSeenAt > 0 ? (now - lastSeenAt) / (24 * 60 * 60 * 1000) : null;
  switch (normalized) {
    case 'allClients':
      return true;
    case 'inactive7':
      return daysInactive != null && daysInactive >= 7;
    case 'inactive30':
      return daysInactive != null && daysInactive >= 30;
    case 'outdated':
      return item.appOutdated === true;
    case 'zeroCredits':
      return credits === 0 && client.freeFirstSupportUsed === true;
    case 'lowCredits':
      return credits > 0 && credits <= 1;
    case 'freeFirst':
      return client.freeFirstSupportUsed === false;
    case 'interrupted':
      return false;
    case 'verified':
      return item.verified === true;
    case 'unverified':
      return item.verified !== true;
    default:
      return false;
  }
};

const resolveAudience = async (filters = []) => {
  const selectedFilters = ensureArray(filters)
    .map((filter) => ensureString(filter || '', '').trim())
    .filter(Boolean);
  const clients = await buildClientAudienceSnapshot();
  if (!selectedFilters.length) return { clients: [], totalClients: clients.length, filters: selectedFilters };
  const selected = clients.filter((item) => selectedFilters.some((filter) => clientMatchesAudienceFilter(item, filter)));
  return { clients: selected, totalClients: clients.length, filters: selectedFilters };
};

const buildAudienceFilterSummaries = async () => {
  const clients = await buildClientAudienceSnapshot();
  const filters = [
    { id: 'allClients', label: 'Todos os clientes' },
    { id: 'inactive7', label: 'Sem acessar ha 7 dias' },
    { id: 'inactive30', label: 'Sem acessar ha 30 dias' },
    { id: 'outdated', label: 'App desatualizado' },
    { id: 'zeroCredits', label: 'Creditos = 0' },
    { id: 'lowCredits', label: 'Creditos baixos' },
    { id: 'freeFirst', label: 'Primeiro gratis disponivel' },
    { id: 'interrupted', label: 'Sessao interrompida' },
    { id: 'verified', label: 'Verificados' },
    { id: 'unverified', label: 'Nao verificados' },
  ];
  return filters.map((filter) => ({
    ...filter,
    count: clients.filter((item) => clientMatchesAudienceFilter(item, filter.id)).length,
  }));
};

const toCampaignSummary = (doc) => {
  const data = doc.data() || {};
  const stats = data.stats && typeof data.stats === 'object' ? data.stats : {};
  const delivery = normalizeDelivery(data.delivery || {});
  return {
    campaignId: ensureString(data.campaignId || doc.id || '', '').trim() || doc.id,
    title: ensureString(data.title || 'Campanha', 'Campanha'),
    body: ensureLongString(data.body || '', '', 600),
    type: normalizeNotificationType(data.type || 'MANUAL_NOTICE'),
    iconType: ensureString(data.iconType || '', '').trim() || 'bell',
    actionLabel: ensureString(data.actionLabel || '', '').trim() || null,
    actionType: normalizeNotificationActionType(data.actionType || 'NONE'),
    targetFilters: data.targetFilters && typeof data.targetFilters === 'object' ? data.targetFilters : {},
    estimatedAudience: Math.max(0, ensureInteger(data.estimatedAudience, 0)),
    delivery,
    status: ensureString(data.status || 'sent', 'sent'),
    schedule: data.schedule && typeof data.schedule === 'object' ? data.schedule : { sendNow: true, scheduledAt: null },
    createdBy: data.createdBy || null,
    createdAt: toNotificationMillis(data.createdAt, null),
    sentAt: toNotificationMillis(data.sentAt, null),
    stats: {
      created: Math.max(0, ensureInteger(stats.created, 0)),
      pushed: Math.max(0, ensureInteger(stats.pushed, 0)),
      read: Math.max(0, ensureInteger(stats.read, 0)),
      dismissed: Math.max(0, ensureInteger(stats.dismissed, 0)),
      failed: Math.max(0, ensureInteger(stats.failed, 0)),
    },
  };
};

const listNotificationCampaigns = async () => {
  const campaignsCollection = getNotificationCampaignsCollection();
  if (!campaignsCollection) return [];
  try {
    const snapshot = await campaignsCollection.orderBy('createdAt', 'desc').limit(80).get();
    return snapshot.docs.map(toCampaignSummary);
  } catch (error) {
    console.error('Failed to list notification campaigns', error);
    return [];
  }
};

const clientMatchesNotificationRule = (item = {}, rule = {}) => {
  const client = item.client || {};
  const profile = item.profile || {};
  const conditions = rule.conditions || {};
  const credits = Math.max(0, ensureInteger(client.credits, 0));
  switch (rule.type) {
    case 'APP_UPDATE':
      return item.appOutdated === true;
    case 'APP_UPDATE_REQUIRED':
      return item.appUpdateRequired === true;
    case 'LOW_CREDITS':
      return credits <= Math.max(0, ensureInteger(conditions.creditsLessOrEqual, 1)) &&
        credits >= Math.max(0, ensureInteger(conditions.creditsGreaterOrEqual, 1));
    case 'NO_CREDITS':
      return credits === Math.max(0, ensureInteger(conditions.creditsEquals, 0)) &&
        (conditions.freeFirstSupportUsed == null || client.freeFirstSupportUsed === ensureBoolean(conditions.freeFirstSupportUsed, true));
    case 'FIRST_FREE_AVAILABLE':
      return client.freeFirstSupportUsed === false;
    case 'REVIEW_APP':
      return Math.max(0, ensureInteger(profile.totalSessions || client.supportsUsed, 0)) >=
        Math.max(1, ensureInteger(conditions.minimumCompletedSessions, 3));
    case 'INACTIVE_7_DAYS':
      return clientMatchesAudienceFilter(item, 'inactive7');
    case 'INACTIVE_30_DAYS':
      return clientMatchesAudienceFilter(item, 'inactive30');
    default:
      return false;
  }
};

const executeNotificationRules = async ({ actorUid = null, actorName = null } = {}) => {
  const rulesCollection = getNotificationRulesCollection();
  if (!rulesCollection) return { ok: false, error: 'firestore_unavailable' };
  const [rules, audience] = await Promise.all([listNotificationRules(), buildClientAudienceSnapshot()]);
  const enabledRules = rules.filter((rule) => rule.enabled === true);
  const results = [];
  for (const rule of enabledRules) {
    let matched = 0;
    let created = 0;
    let duplicates = 0;
    let pushed = 0;
    let failed = 0;
    for (const item of audience) {
      if (!clientMatchesNotificationRule(item, rule)) continue;
      matched += 1;
      const template = rule.notificationTemplate || {};
      const result = await createClientNotification({
        client: item.client,
        clientUid: item.latestDevice?.clientUid || null,
        title: template.title,
        body: template.body,
        type: rule.type,
        iconType: template.iconType,
        priority: rule.priority,
        actionLabel: template.actionLabel,
        actionType: template.actionType,
        delivery: rule.delivery,
        source: 'automatic_rule',
        ruleId: rule.ruleId,
        createdBy: actorUid ? { uid: actorUid, name: actorName || null } : { uid: 'system', name: 'Sistema' },
        expiresAfterDays: rule.expiresAfterDays,
        dedupeKey: `rule:${rule.ruleId}:${item.client.id}`,
      });
      if (result.created) created += 1;
      if (result.duplicate) duplicates += 1;
      pushed += Math.max(0, ensureInteger(result.push?.sent, 0));
      failed += Math.max(0, ensureInteger(result.push?.failed, 0));
    }
    await rulesCollection.doc(rule.ruleId).set(
      {
        lastRunAt: Date.now(),
        lastRunStats: { matched, created, duplicates, pushed, failed },
        updatedAt: Date.now(),
      },
      { merge: true }
    );
    await recordNotificationEvent('RULE_EXECUTED', {
      ruleId: rule.ruleId,
      actorUid,
      actorName,
      status: 'executed',
      metadata: { matched, created, duplicates, pushed, failed },
    });
    results.push({ ruleId: rule.ruleId, matched, created, duplicates, pushed, failed });
  }
  await createAdminNotification({
    title: 'Regras automaticas executadas',
    body: `${enabledRules.length} regra(s) avaliadas.`,
    type: 'GENERAL_INFO',
    iconType: 'bell',
    actorUid,
    metadata: { results },
  });
  return { ok: true, rules: results };
};

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
      // This resolver is used by authenticated technical/context flows.
      // Untrusted support requests use ensureClientIdentityFromPhone with
      // hasVerifiedIdentityProof=false and cannot reach phone/device matches.
      hasVerifiedIdentityProof: true,
      allowDeviceIdentityLookup: true,
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
  hasVerifiedIdentityProof = false,
  allowDeviceIdentityLookup = false,
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

  if (allowDeviceIdentityLookup && deviceLinkDocId && linksCollection) {
    try {
      const deviceLinkSnap = await linksCollection.doc(deviceLinkDocId).get();
      if (deviceLinkSnap.exists) {
        linkedDeviceClientId = ensureString(deviceLinkSnap.data()?.clientId || '', '').trim() || null;
      }
    } catch (error) {
      console.error('Failed to resolve linked client record by device anchor', error);
    }
  }

  if (hasVerifiedIdentityProof && normalizedPhone && verificationsCollection) {
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

  const lookupPlan = buildClientIdentityLookupPlan({
    linkedClientId,
    linkedDeviceClientId,
    verificationClientIds,
    uidDocId,
    phoneDocId,
    hasVerifiedIdentityProof,
    allowDeviceIdentityLookup,
  });

  const snapshots = [];
  for (const candidateId of lookupPlan.candidateIds) {
    const snapshot = await loadClientSnapshot(candidateId);
    if (snapshot) snapshots.push(snapshot);
  }
  if (snapshots.length) {
    snapshots.sort((a, b) => scoreClientSnapshot(b) - scoreClientSnapshot(a));
    return snapshots[0].id;
  }
  return lookupPlan.fallbackClientId;
};

const ensureClientIdentityFromPhone = async ({
  normalizedPhone = null,
  clientUid = '',
  deviceAnchor = '',
  clientName = '',
  source = 'support_request',
  hasVerifiedIdentityProof = false,
  identityAssurance = 'uid_bound',
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
    hasVerifiedIdentityProof,
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
    const existingPhone = normalizePhone(oldData.phone || '') || null;
    const persistedPhone = selectClientPhoneForIdentity({
      existingPhone,
      claimedPhone: phone,
      hasVerifiedIdentityProof,
    });

    tx.set(
      clientRef,
      {
        phone: persistedPhone,
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

  let finalClientId = clientId;
  if (linksCollection && (normalizedClientUid || normalizedDeviceAnchor)) {
    const baseLinkPayload = {
      clientUid: normalizedClientUid,
      clientId,
      phone: phone || null,
      deviceAnchor: normalizedDeviceAnchor,
      createdAt: now,
      updatedAt: now,
    };
    if (hasVerifiedIdentityProof) {
      baseLinkPayload.identityAssurance =
        ensureString(identityAssurance || '', '').trim() || 'verified_phone';
      baseLinkPayload.phoneVerified = true;
      baseLinkPayload.phoneVerifiedAt = now;
    } else {
      baseLinkPayload.identityAssurance = 'uid_only';
      baseLinkPayload.phoneVerified = false;
    }
    if (normalizedClientUid) {
      const uidLinkRef = linksCollection.doc(normalizedClientUid);
      if (mayReplaceClientUidLink({ hasVerifiedIdentityProof })) {
        await uidLinkRef.set(baseLinkPayload, { merge: true });
      } else {
        // An unverified request may create its own UID link, but it may never
        // replace a link established by PNV or by an authorized technician.
        finalClientId = await db.runTransaction(async (tx) => {
          const linkSnap = await tx.get(uidLinkRef);
          const existingClientId = linkSnap.exists
            ? ensureString(linkSnap.data()?.clientId || '', '').trim()
            : '';
          if (existingClientId) {
            tx.set(
              uidLinkRef,
              {
                clientUid: normalizedClientUid,
                updatedAt: now,
              },
              { merge: true }
            );
            return existingClientId;
          }

          tx.set(uidLinkRef, baseLinkPayload, { merge: true });
          return clientId;
        });
      }
    }
    const deviceLinkDocId =
      hasVerifiedIdentityProof && normalizedDeviceAnchor
        ? linkDocIdFromDeviceAnchor(normalizedDeviceAnchor)
        : null;
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
    clientRecordId: finalClientId,
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
const clientSessionRecoveryService = db
  ? createClientSessionRecoveryService({ db })
  : null;
const turnCredentialsService = new TurnCredentialsService({
  onDiagnostic: ({ code, upstreamStatus }) => {
    console.warn('TURN credential fallback active', {
      code,
      ...(upstreamStatus ? { upstreamStatus } : {}),
    });
  },
});
const isProduction = process.env.NODE_ENV === 'production';
if (isProduction) {
  app.set('trust proxy', 1);
}
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
  maxHttpBufferSize: 256 * 1024,
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
const QUEUE_ALERT_FIRST_THRESHOLD_MINUTES = Math.max(5, ensureInteger(process.env.QUEUE_ALERT_FIRST_THRESHOLD_MINUTES, 5));
const QUEUE_ALERT_STEP_MINUTES = Math.max(5, ensureInteger(process.env.QUEUE_ALERT_STEP_MINUTES, 5));
const QUEUE_ALERT_SWEEP_INTERVAL_MS = Math.max(30_000, ensureInteger(process.env.QUEUE_ALERT_SWEEP_INTERVAL_MS, 60_000));
let queueAlertSweepTimer = null;
let queueAlertSweepInProgress = false;
let queueAlertLastMissingPhoneLogAt = 0;

app.use(cors(corsOptions));
app.use(createWebSecurityHeadersMiddleware({ isProduction }));
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader(
    'Permissions-Policy',
    'geolocation=(), camera=(), microphone=(self), display-capture=(self), payment=(), usb=(), serial=(), bluetooth=()'
  );
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
app.use(
  express.json({
    verify: (req, _res, buf) => {
      if (req.originalUrl && req.originalUrl.startsWith('/api/whatsapp-api/webhook')) {
        req.rawBody = Buffer.from(buf || '');
      }
    },
  })
);
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

if (uploadBucket) {
  const privacyContactEncryptionKey = String(
    process.env.PRIVACY_CONTACT_ENCRYPTION_KEY || ''
  ).trim();
  app.use('/api', createPrivacyRouter({
    auth: admin.auth(),
    db,
    bucket: uploadBucket,
    normalizePhone,
    verifyPnvToken: verifyFirebasePnvToken,
    verifyTurnstile: ({ token, remoteIp }) =>
      verifyTechLoginTurnstileToken({
        token,
        remoteIpAddress: remoteIp,
        isProduction,
        expectedAction: PRIVACY_DELETION_TURNSTILE_ACTION,
      }),
    protectContact: privacyContactEncryptionKey
      ? createPrivacyContactProtector(privacyContactEncryptionKey)
      : null,
    logger: console,
  }));
} else {
  app.post(['/api/client/account/delete', '/api/privacy/deletion-requests'], (_req, res) => {
    res.status(503).json({ error: 'storage_unavailable' });
  });
}

app.get(['/credit-panel.html', '/tech-panel', '/tech-panel/', '/tech-panel/index.html'], (_req, res) => {
  return res.redirect(302, '/central.html');
});

const sendNoCacheStaticHtml = (fileName) => (_req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
  return res.sendFile(path.join(WEB_STATIC_PATH, fileName));
};

app.get(['/privacidade', '/privacidade/'], sendNoCacheStaticHtml('privacidade.html'));
app.get(['/excluir-conta', '/excluir-conta/'], sendNoCacheStaticHtml('excluir-conta.html'));

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
  const techLoginTurnstile = getTechLoginTurnstilePublicConfig();
  const privacyTurnstile = {
    ...techLoginTurnstile,
    action: PRIVACY_DELETION_TURNSTILE_ACTION,
  };
  res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');

  const firebaseSerialized = (firebaseConfig ? JSON.stringify(firebaseConfig) : 'null').replace(/</g, '\\u003C');
  const turnstileSerialized = JSON.stringify(techLoginTurnstile || {}).replace(/</g, '\\u003C');
  const privacyTurnstileSerialized = JSON.stringify(privacyTurnstile).replace(/</g, '\\u003C');
  const script = `(() => {
    const target = (window.__CENTRAL_CONFIG__ = window.__CENTRAL_CONFIG__ || {});
    if (!target.firebase) {
      target.firebase = ${firebaseSerialized};
    }
    target.techLoginTurnstile = ${turnstileSerialized};
    target.privacyTurnstile = ${privacyTurnstileSerialized};
    if (!target.firebase) {
      console.warn('Firebase client config not configured for central.');
    }
  })();`;

  res.send(script);
});

app.get('/healthz', async (_req, res) => {
  return res.json({
    ok: true,
    service: 'up',
    firestoreConfigured: Boolean(db),
    uptimeSec: Math.floor(process.uptime()),
    now: Date.now(),
  });
});

// ===== Estado
const generateSessionId = randomSessionId;

// ====== SOCKETS
const connectionIndex = new Map();
const LEGACY_WEBRTC_ROOM_TTL_MS = normalizeLegacyRoomTtlMs(
  process.env.LEGACY_WEBRTC_ROOM_TTL_MS
);
const legacyJoinRateLimiter = new LegacyJoinRateLimiter({
  windowMs: process.env.LEGACY_JOIN_RATE_WINDOW_MS,
  socketLimit: process.env.LEGACY_JOIN_SOCKET_MAX_ATTEMPTS,
  ipLimit: process.env.LEGACY_JOIN_IP_MAX_ATTEMPTS,
});
const supportRequestRateLimiter = new LegacyJoinRateLimiter({
  windowMs: 60 * 1000,
  socketLimit: 4,
  ipLimit: 30,
});

const emitQueueUpdated = ({
  requestId,
  state,
  sessionId = null,
  techName = null,
  clientUid = null,
  targetSocketId = null,
  notifyClient = false,
} = {}) => {
  const payload = {
    requestId: ensureString(requestId || '', '').trim() || null,
    state: ensureString(state || '', '').trim() || null,
    ...(sessionId ? { sessionId: ensureString(sessionId || '', '').trim() } : {}),
    ...(techName ? { techName: ensureString(techName || '', '').trim() } : {}),
  };

  io.to(ACTIVE_TECH_SOCKET_ROOM).emit('queue:updated', payload);
  if (!notifyClient) return;

  const userRoom = clientSocketRoom(clientUid);
  if (userRoom) {
    io.to(userRoom).emit('queue:updated', payload);
    return;
  }

  const normalizedSocketId = ensureString(targetSocketId || '', '').trim();
  if (normalizedSocketId) io.to(normalizedSocketId).emit('queue:updated', payload);
};

const getRequestById = async (requestId) => {
  const requestsCollection = getRequestsCollection();
  if (!requestsCollection) return null;
  const snapshot = await requestsCollection.doc(requestId).get();
  if (!snapshot.exists) return null;
  return { requestId: snapshot.id, ...snapshot.data() };
};

const rebindQueuedRequestsToClientSocket = async (socket) => {
  const requestsCollection = getRequestsCollection();
  if (!requestsCollection || !db) return;
  const clientUid = ensureString(socket?.user?.uid || '', '').trim();
  const clientPhone = normalizePhone(socket?.user?.phone_number || '');
  if (!clientUid && !clientPhone) return;

  const snapshots = [];
  if (clientUid) {
    snapshots.push(await requestsCollection.where('clientUid', '==', clientUid).get());
  } else if (clientPhone) {
    snapshots.push(await requestsCollection.where('clientPhone', '==', clientPhone).get());
  }

  const docsById = new Map();
  snapshots.forEach((snapshot) => {
    snapshot.docs.forEach((docSnap) => {
      docsById.set(docSnap.id, docSnap);
    });
  });
  if (!docsById.size) return;

  const now = Date.now();
  const batch = db.batch();
  let updates = 0;
  docsById.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const state = ensureString(data.state || '', '').trim().toLowerCase();
    if (state !== 'queued') return;

    const currentSocketId = ensureString(data.clientSocketId || data.clientId || '', '').trim();
    if (currentSocketId === socket.id) return;

    batch.set(
      docSnap.ref,
      {
        clientSocketId: socket.id,
        clientId: socket.id,
        updatedAt: now,
      },
      { merge: true }
    );
    updates += 1;
  });

  if (!updates) return;
  await batch.commit();
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

const finalizeSupportSessionFromRealtime = async ({
  realtimeSessionId,
  realtimeSession,
  actorUid,
  actorRole,
  authorizedTech = false,
  summary = {},
  now = Date.now(),
} = {}) => {
  const normalizedRealtimeSessionId = normalizeSessionId(realtimeSessionId);
  if (!/^[A-Za-z0-9_-]{1,64}$/.test(normalizedRealtimeSessionId)) {
    throw new SupportSessionClosureError('invalid_session_id', 400);
  }
  const supportSessionId = localSupportSessionIdFromRealtime(realtimeSession);
  if (!supportSessionId) {
    return {
      ok: true,
      skipped: true,
      reason: 'support_session_id_missing',
      supportSessionId: null,
    };
  }
  if (supportSessionId.includes('/')) {
    throw new SupportSessionClosureError('invalid_support_session_id', 400);
  }

  const supportSessionsCollection = getSupportSessionsCollection();
  if (!db || !supportSessionsCollection) {
    throw new SupportSessionClosureError('firestore_unavailable', 503);
  }
  const supportSessionRef = supportSessionsCollection.doc(supportSessionId);
  const expiresAt = admin.firestore.Timestamp.fromMillis(
    Number(now) + 30 * 24 * 60 * 60 * 1000
  );

  return db.runTransaction(async (tx) => {
    const supportSessionSnap = await tx.get(supportSessionRef);
    if (!supportSessionSnap.exists) {
      throw new SupportSessionClosureError('support_session_not_found', 404);
    }

    const decision = buildSupportSessionClosure({
      realtimeSessionId: normalizedRealtimeSessionId,
      realtimeSession,
      supportSession: supportSessionSnap.data() || {},
      actorUid,
      actorRole,
      authorizedTech,
      summary,
      now,
      expiresAt,
    });
    if (decision.shouldWrite) {
      tx.set(supportSessionRef, decision.patch, { merge: true });
    }
    return {
      ok: true,
      skipped: false,
      supportSessionId,
      alreadyFinalized: decision.alreadyFinalized,
      status: decision.finalStatus,
      updated: decision.shouldWrite,
    };
  });
};

const resolveSocketIdentityAccess = async (decoded, { requireActiveTech = false } = {}) => {
  const uid = ensureString(decoded?.uid || '', '').trim();
  if (!uid) return { ok: false, code: 'invalid_token' };

  const isTechRole = normalizeRole(decoded?.role) === 'tech';
  if (requireActiveTech && !isTechRole) {
    return { ok: false, code: 'insufficient_role' };
  }

  const isActiveTech = isTechRole ? await isActiveTechUid(uid) : false;
  if (requireActiveTech && !isActiveTech) {
    return { ok: false, code: 'tech_inactive' };
  }

  return { ok: true, uid, isActiveTech };
};

const bindAuthorizedSocketRooms = (socket, access = {}) => {
  const uid = ensureString(access.uid || socket?.user?.uid || '', '').trim();
  const userRoom = clientSocketRoom(uid);
  if (userRoom) socket.join(userRoom);
  if (access.isActiveTech === true) socket.join(ACTIVE_TECH_SOCKET_ROOM);

  socket.data.authUid = uid || null;
  socket.data.isActiveTech = access.isActiveTech === true;
};

const syncActiveTechSocketRoom = (uid, active) => {
  const userRoom = clientSocketRoom(uid);
  if (!userRoom) return;
  const socketsForUser = io.in(userRoom);
  if (active === true) {
    socketsForUser.socketsJoin(ACTIVE_TECH_SOCKET_ROOM);
  } else {
    // Revocation must be immediate: leaving only the technical broadcast room
    // would still let a deactivated technician receive traffic from a session
    // room that was joined previously.
    socketsForUser.disconnectSockets(true);
  }
};

io.use(async (socket, next) => {
  try {
    const auth = socket.handshake?.auth || {};
    const token = extractSocketToken(socket);
    const requiresAuth = auth.requireAuth === true || auth.panel === 'tech';
    const requiresActiveTech = auth.panel === 'tech';

    if (!token) {
      if (requiresAuth) return next(new Error('missing_token'));
      return next();
    }

    const decoded = await admin.auth().verifyIdToken(token);
    const access = await resolveSocketIdentityAccess(decoded, { requireActiveTech: requiresActiveTech });
    if (!access.ok) return next(new Error(access.code || 'forbidden'));

    socket.user = decoded;
    socket.data.authUid = access.uid;
    socket.data.isActiveTech = access.isActiveTech;
    return next();
  } catch (error) {
    console.error('Socket auth failed', error);
    const code = ensureFullString(error?.code || '', 'unknown_error').trim() || 'unknown_error';
    return next(new Error(`invalid_token:${code}`));
  }
});

const resolveSocketAuthFromPayload = async (socket, payload = {}) => {
  if (socket?.user?.uid) {
    const access = await resolveSocketIdentityAccess(socket.user);
    if (access.ok) bindAuthorizedSocketRooms(socket, access);
    return socket.user;
  }

  const payloadToken = ensureString(payload.idToken || payload.token || '', '').trim();
  if (!payloadToken) return null;

  const decoded = await admin.auth().verifyIdToken(payloadToken);
  const access = await resolveSocketIdentityAccess(decoded);
  if (!access.ok) return null;
  socket.user = decoded;
  bindAuthorizedSocketRooms(socket, access);
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

app.get('/api/webrtc/ice-config', requireAuth(), async (req, res) => {
  const sessionId = normalizeSessionId(req.query?.sessionId || '');
  if (!/^[A-Za-z0-9_-]{1,64}$/.test(sessionId)) {
    return res.status(400).json({ error: 'invalid_session_id' });
  }
  if (!getSessionsCollection()) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  try {
    const snapshot = await getSessionSnapshot(sessionId);
    if (!snapshot) {
      return res.status(404).json({ error: 'session_not_found' });
    }

    const sessionData = snapshot.data() || {};
    const authUid = ensureString(req.user?.uid || '', '').trim();
    const assignedTechUid = getSessionTechUid(sessionData);
    const isAssignedTech = Boolean(
      authUid && assignedTechUid && authUid === assignedTechUid
    );
    const isTechActive = isAssignedTech
      ? await isActiveTechUid(authUid)
      : false;
    const access = evaluateSessionIceAccess({
      authUid,
      userRole: req.userRole || req.user?.role || '',
      sessionData,
      isTechActive,
    });
    if (!access.ok) {
      return res.status(access.status).json({ error: access.error });
    }

    const iceConfig = await turnCredentialsService.getIceConfig({
      cacheKey: `${sessionId}:${authUid}`,
    });
    res.set('Cache-Control', 'private, no-store, max-age=0');
    res.set('Pragma', 'no-cache');
    return res.status(200).json(iceConfig);
  } catch (error) {
    console.error('Failed to resolve authorized ICE configuration', {
      code: ensureString(error?.code || 'ice_config_error', 'ice_config_error'),
    });
    return res.status(500).json({ error: 'ice_config_error' });
  }
});

const normalizeLegacyRoom = (payload = {}) => {
  if (typeof payload === 'string') return normalizeLegacyRoomCode(payload);
  if (!payload || typeof payload !== 'object') return '';
  return normalizeLegacyRoomCode(
    payload.room || payload.sessionId || payload.code || ''
  );
};

const normalizeLegacyRole = (value, fallback = 'client') => {
  const raw = ensureString(value || '', '').trim().toLowerCase();
  if (raw === 'tech' || raw === 'viewer' || raw === 'supervisor') return 'tech';
  if (raw === 'client' || raw === 'sender') return 'client';
  if (fallback === 'tech' || fallback === 'client') return fallback;
  return '';
};

const isAllowedSessionMediaUrl = (rawUrl, sessionId) => {
  const value = ensureLongString(rawUrl || '', '', 4096).trim();
  if (!value) return true;
  try {
    const parsed = new URL(value);
    if (parsed.protocol !== 'https:' || parsed.hostname !== 'firebasestorage.googleapis.com') {
      return false;
    }
    const pathMatch = parsed.pathname.match(/^\/v0\/b\/([^/]+)\/o\/(.+)$/);
    if (!pathMatch) return false;
    const bucketName = decodeURIComponent(pathMatch[1]);
    const objectPath = decodeURIComponent(pathMatch[2]);
    if (bucketName !== STORAGE_BUCKET_NAME) return false;
    if (!objectPath.startsWith(`sessions/${sessionId}/`)) return false;
    if (parsed.searchParams.get('alt') !== 'media') return false;
    return Boolean(parsed.searchParams.get('token'));
  } catch (_error) {
    return false;
  }
};

const authorizeLegacyRoomReservation = async ({
  room,
  requesterUid,
  requestedRole,
} = {}) => {
  const roomsCollection = getLegacyWebrtcRoomsCollection();
  const normalizedRoom = normalizeLegacyRoomCode(room);
  const reservationDocId = legacyRoomReservationDocId(normalizedRoom);
  const normalizedUid = ensureString(requesterUid || '', '').trim();
  if (!db || !roomsCollection) {
    throw new LegacyRoomAccessError('firestore_unavailable', 503);
  }
  if (!normalizedRoom || !reservationDocId) {
    throw new LegacyRoomAccessError('invalid-room', 400);
  }
  if (!normalizedUid) {
    throw new LegacyRoomAccessError('missing_token', 401);
  }

  const reservationRef = roomsCollection.doc(reservationDocId);
  const proposedReservationId = crypto.randomBytes(16).toString('hex');
  const decision = await db.runTransaction(async (tx) => {
    const reservationSnap = await tx.get(reservationRef);
    let isTechActive = false;
    if (requestedRole === 'tech') {
      const techSnap = await tx.get(db.collection('techs').doc(normalizedUid));
      isTechActive =
        techSnap.exists && techSnap.data()?.active === true;
    }
    const now = Date.now();
    const joinDecision = decideLegacyRoomJoin({
      roomCode: normalizedRoom,
      requesterUid: normalizedUid,
      requestedRole,
      isTechActive,
      reservation: reservationSnap.exists
        ? reservationSnap.data() || {}
        : null,
      proposedReservationId,
      now,
      ttlMs: LEGACY_WEBRTC_ROOM_TTL_MS,
    });
    const reservationDocument = buildLegacyRoomReservationDocument({
      decision: joinDecision,
      now,
      timestampFromMillis: (millis) =>
        admin.firestore.Timestamp.fromMillis(millis),
    });
    if (reservationDocument) {
      tx.set(reservationRef, reservationDocument);
    }
    return joinDecision;
  });

  const socketRoom = legacySocketRoomName(
    normalizedRoom,
    decision.reservationId
  );
  if (!socketRoom) {
    throw new LegacyRoomAccessError('invalid-reservation', 500);
  }
  return {
    ...decision,
    socketRoom,
  };
};

const resolveLegacyJoinAccess = async (socket, payload = {}) => {
  const room = normalizeLegacyRoom(payload);
  if (!room) return { ok: false, code: 'invalid-room' };

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
    if (!isTechRole) {
      return { ok: false, code: 'forbidden' };
    }
  }

  try {
    const reservation = await authorizeLegacyRoomReservation({
      room,
      requesterUid: decoded.uid,
      requestedRole,
    });
    return {
      ok: true,
      room,
      role: requestedRole,
      uid: decoded.uid,
      reservationId: reservation.reservationId,
      expiresAtMs: reservation.expiresAtMs,
      socketRoom: reservation.socketRoom,
    };
  } catch (error) {
    if (error instanceof LegacyRoomAccessError) {
      return {
        ok: false,
        code: error.code,
        status: error.status,
      };
    }
    console.error('Failed to authorize legacy room join', {
      code: ensureString(error?.code || 'reservation_failed', 'reservation_failed'),
    });
    return { ok: false, code: 'reservation-failed' };
  }
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
      // Full session state contains customer data, telemetry and logs. It is
      // intentionally restricted to authenticated, active technical users.
      io.to(ACTIVE_TECH_SOCKET_ROOM).emit('session:updated', session);
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

const upsertTechDoc = async ({
  uid,
  email = null,
  name = null,
  active = true,
  role = 'tech',
  phone = null,
  phoneVerified = false,
  phoneVerifiedAt = null,
  phoneVerificationUid = null,
  phoneVerificationMethod = null,
} = {}) => {
  if (!db || !uid) return;
  const normalizedPhone = normalizePhone(phone || '');
  const phoneIsVerified = normalizedPhone ? phoneVerified === true : false;
  const now = Date.now();
  const resolvedPhoneVerifiedAt = phoneIsVerified
    ? (Number.isFinite(Number(phoneVerifiedAt)) ? Number(phoneVerifiedAt) : now)
    : null;
  const resolvedPhoneVerificationUid = phoneIsVerified
    ? (ensureString(phoneVerificationUid || '', '').trim() || null)
    : null;
  const resolvedPhoneVerificationMethod = phoneIsVerified
    ? (ensureString(phoneVerificationMethod || '', '').trim() || 'sms')
    : null;
  await db.collection('techs').doc(uid).set(
    {
      uid,
      email,
      name,
      active: active === true,
      role,
      phone: normalizedPhone || null,
      whatsappPhone: normalizedPhone || null,
      phoneNumber: normalizedPhone || null,
      phoneVerified: phoneIsVerified,
      phoneVerifiedAt: resolvedPhoneVerifiedAt,
      phoneVerificationUid: resolvedPhoneVerificationUid,
      phoneVerificationMethod: resolvedPhoneVerificationMethod,
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
    const normalizedPhone =
      normalizePhone(data.phone || data.whatsappPhone || data.phoneNumber || data.contactPhone || userRecord?.phoneNumber || '') ||
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
      phone: normalizedPhone,
      phoneVerified: data.phoneVerified === true,
      phoneVerifiedAt: Number.isFinite(Number(data.phoneVerifiedAt)) ? Number(data.phoneVerifiedAt) : null,
      phoneVerificationMethod: ensureString(data.phoneVerificationMethod || '', '').trim() || null,
      createdAt: data.createdAt || null,
      updatedAt: data.updatedAt || null,
    };
  }));
  return techs;
};

const ALLOWED_SESSION_COMMAND_TYPES = new Set([
  'share_start',
  'share_stop',
  'remote_enable',
  'remote_disable',
  'remote_revoke',
  'call_start',
  'call_end',
  'session_end',
  'end',
]);

const sanitizeSessionCommandData = (value, normalizedType, actorRole) => {
  if (
    normalizedType !== 'end' ||
    actorRole !== 'tech' ||
    !value ||
    typeof value !== 'object' ||
    Array.isArray(value)
  ) {
    return null;
  }
  const fields = [
    'problemSummary',
    'symptom',
    'solutionSummary',
    'solution',
    'internalNotes',
    'notes',
  ];
  const sanitized = {};
  for (const field of fields) {
    const text = ensureLongString(value[field] || '', '', 1000).trim();
    if (text) sanitized[field] = text;
  }
  return Object.keys(sanitized).length ? sanitized : null;
};

const toSupportQueueErrorPayload = (error, correlation = {}) => {
  const isPolicyError = error instanceof SupportQueuePolicyError;
  const code = isPolicyError
    ? error.code
    : ensureString(error?.code || error?.message || '', '').trim() || 'server_error';
  const details = isPolicyError && error.details && typeof error.details === 'object'
    ? error.details
    : {};
  return {
    ok: false,
    error: code,
    err: code,
    ...details,
    ...Object.fromEntries(
      Object.entries(correlation).filter(([, value]) => value !== null && value !== undefined && value !== '')
    ),
  };
};

const emitSupportQueueError = (socket, ack, error, correlation = {}) => {
  const payload = toSupportQueueErrorPayload(error, correlation);
  respondAck(ack, payload);
  socket.emit('support:error', payload);
  return payload;
};

const findLegacyQueuedRequestForUid = async (requestsCollection, clientUid) => {
  if (!requestsCollection || !clientUid) return null;
  const docs = await safeGetDocs(
    requestsCollection.where('clientUid', '==', clientUid).limit(20),
    'queued request compatibility lookup'
  );
  const queued = docs
    .filter((doc) => ensureString(doc.data()?.state || 'queued', '').trim().toLowerCase() === 'queued')
    .sort((a, b) => {
      const left = Number(a.data()?.createdAt || a.data()?.updatedAt || 0);
      const right = Number(b.data()?.createdAt || b.data()?.updatedAt || 0);
      return left - right;
    });
  if (!queued.length) return null;
  return {
    requestId: queued[0].id,
    data: queued[0].data() || {},
  };
};

const reserveSupportQueueRequest = async ({
  authUid,
  localSupportSessionId,
  generatedRequestId,
  requestData,
} = {}) => {
  const requestsCollection = getRequestsCollection();
  const supportSessionsCollection = getSupportSessionsCollection();
  const locksCollection = getSupportQueueLocksCollection();
  const anchorsCollection = getSupportQueueAnchorsCollection();
  if (!db || !requestsCollection || !supportSessionsCollection || !locksCollection || !anchorsCollection) {
    throw new SupportQueuePolicyError('firestore_unavailable', 503);
  }

  const normalizedUid = ensureString(authUid || '', '').trim();
  const normalizedLocalId = ensureString(localSupportSessionId || '', '').trim().slice(0, 128);
  const normalizedGeneratedRequestId =
    ensureString(generatedRequestId || '', '').trim().slice(0, 64).toUpperCase();
  const lockId = queueLockDocIdFromUid(normalizedUid);
  const anchorId = queueAnchorDocId(normalizedUid, normalizedLocalId);
  if (!normalizedUid || !normalizedLocalId || !normalizedGeneratedRequestId || !lockId || !anchorId) {
    throw new SupportQueuePolicyError('invalid_payload', 400);
  }

  const legacyQueued = await findLegacyQueuedRequestForUid(requestsCollection, normalizedUid);
  const now = Date.now();
  const supportSessionRef = supportSessionsCollection.doc(normalizedLocalId);
  const lockRef = locksCollection.doc(lockId);
  const anchorRef = anchorsCollection.doc(anchorId);
  const deletionOperationRef = db
    .collection('account_deletion_operations')
    .doc(crypto.createHash('sha256').update(normalizedUid, 'utf8').digest('hex'));

  return db.runTransaction(async (tx) => {
    const supportSessionSnap = await tx.get(supportSessionRef);
    const anchorSnap = await tx.get(anchorRef);
    const lockSnap = await tx.get(lockRef);
    const deletionOperationSnap = await tx.get(deletionOperationRef);
    const supportSession = supportSessionSnap.exists ? supportSessionSnap.data() || {} : null;
    const anchor = anchorSnap.exists ? anchorSnap.data() || {} : null;
    const persistedLock = lockSnap.exists ? lockSnap.data() || {} : null;
    const deletionOperation = deletionOperationSnap.exists
      ? deletionOperationSnap.data() || {}
      : null;
    if (isAccountDeletionBlocking(deletionOperation)) {
      throw new SupportQueuePolicyError('account_deletion_in_progress', 409);
    }

    const candidateRequestIds = [
      ensureString(anchor?.requestId || '', '').trim().toUpperCase(),
      ensureString(persistedLock?.requestId || '', '').trim().toUpperCase(),
      ensureString(legacyQueued?.requestId || '', '').trim().toUpperCase(),
      normalizedGeneratedRequestId,
    ]
      .filter(Boolean)
      .filter((value, index, values) => values.indexOf(value) === index);

    const requestSnapshots = new Map();
    for (const candidateId of candidateRequestIds) {
      requestSnapshots.set(candidateId, await tx.get(requestsCollection.doc(candidateId)));
    }
    const requestDataById = (requestId) => {
      const normalizedRequestId = ensureString(requestId || '', '').trim().toUpperCase();
      const snapshot = requestSnapshots.get(normalizedRequestId);
      return snapshot?.exists ? { requestId: snapshot.id, ...(snapshot.data() || {}) } : null;
    };

    const persistedLockRequest = requestDataById(persistedLock?.requestId);
    const legacyLockRequest = requestDataById(legacyQueued?.requestId);
    const persistedLockIsActive =
      persistedLockRequest &&
      ensureString(persistedLockRequest.state || 'queued', '').trim().toLowerCase() === 'queued' &&
      ensureString(persistedLockRequest.clientUid || '', '').trim() === normalizedUid;
    const effectiveLock =
      persistedLockIsActive || !legacyLockRequest
        ? persistedLock
        : {
            requestId: legacyLockRequest.requestId,
            localSupportSessionId: legacyLockRequest.localSupportSessionId || null,
          };
    const effectiveLockRequest = persistedLockIsActive ? persistedLockRequest : legacyLockRequest;

    const decision = decideQueueReservation({
      authUid: normalizedUid,
      localSupportSessionId: normalizedLocalId,
      supportSession,
      anchor,
      anchorRequest: requestDataById(anchor?.requestId),
      uidLock: effectiveLock,
      lockRequest: effectiveLockRequest,
      generatedRequestId: normalizedGeneratedRequestId,
    });

    if (decision.action === 'already_accepted') {
      return {
        ...decision,
        status: 'accepted',
        requestData: null,
      };
    }

    const requestId = decision.requestId;
    const requestRef = requestsCollection.doc(requestId);
    const existingRequest = requestDataById(requestId);
    if (decision.action === 'create' && existingRequest) {
      throw new SupportQueuePolicyError('request_id_collision', 409);
    }

    const requestPatch =
      decision.action === 'reuse'
        ? {
            clientSocketId: requestData.clientSocketId,
            clientId: requestData.clientId,
            updatedAt: now,
          }
        : {
            ...requestData,
            requestId,
            localSupportSessionId: normalizedLocalId,
            createdAt: requestData.createdAt || now,
            updatedAt: now,
            state: 'queued',
          };
    if (decision.action === 'reuse') {
      tx.set(requestRef, requestPatch, { merge: true });
    } else {
      tx.set(requestRef, requestPatch);
    }

    const lockPayload = {
      clientUid: normalizedUid,
      requestId,
      localSupportSessionId: normalizedLocalId,
      status: 'queued',
      createdAt: persistedLock?.createdAt || now,
      updatedAt: now,
    };
    const anchorPayload = {
      clientUid: normalizedUid,
      requestId,
      localSupportSessionId: normalizedLocalId,
      status: 'queued',
      createdAt: anchor?.createdAt || now,
      updatedAt: now,
    };
    tx.set(lockRef, lockPayload, { merge: true });
    tx.set(anchorRef, anchorPayload, { merge: true });
    tx.set(
      supportSessionRef,
      {
        queueRequestId: requestId,
        queueStatus: 'queued',
        status: 'queued',
        clientId: requestData.clientRecordId || supportSession?.clientId || null,
        clientPhone: requestData.clientPhone || supportSession?.clientPhone || null,
        queueUpdatedAt: now,
        updatedAt: now,
      },
      { merge: true }
    );

    return {
      action: decision.action,
      requestId,
      localSupportSessionId: normalizedLocalId,
      reused: decision.reused,
      status: 'queued',
      requestData:
        decision.action === 'reuse'
          ? { ...(existingRequest || {}), ...requestPatch, requestId }
          : requestPatch,
    };
  });
};

const cancelSupportQueueRequest = async ({
  authUid,
  requestId = '',
  localSupportSessionId = '',
  verifiedPhone = '',
  now = Date.now(),
} = {}) => {
  const requestsCollection = getRequestsCollection();
  const supportSessionsCollection = getSupportSessionsCollection();
  const locksCollection = getSupportQueueLocksCollection();
  const anchorsCollection = getSupportQueueAnchorsCollection();
  const outcomesCollection = getSupportQueueOutcomesCollection();
  if (
    !db ||
    !requestsCollection ||
    !supportSessionsCollection ||
    !locksCollection ||
    !anchorsCollection ||
    !outcomesCollection
  ) {
    throw new SupportQueuePolicyError('firestore_unavailable', 503);
  }

  const normalizedUid = ensureString(authUid || '', '').trim();
  const normalizedVerifiedPhone = normalizePhone(verifiedPhone || '');
  const requestedRequestId = ensureString(requestId || '', '').trim().slice(0, 64).toUpperCase();
  const requestedLocalId = ensureString(localSupportSessionId || '', '').trim().slice(0, 128);
  if (!normalizedUid || (!requestedRequestId && !requestedLocalId)) {
    throw new SupportQueuePolicyError('invalid_payload', 400);
  }

  const lockId = queueLockDocIdFromUid(normalizedUid);
  if (!lockId) throw new SupportQueuePolicyError('invalid_payload', 400);
  const lockRef = locksCollection.doc(lockId);

  return db.runTransaction(async (tx) => {
    let resolvedRequestId = requestedRequestId;
    let resolvedLocalId = requestedLocalId;
    let requestSnap = resolvedRequestId
      ? await tx.get(requestsCollection.doc(resolvedRequestId))
      : null;
    let outcomeSnap = resolvedRequestId
      ? await tx.get(outcomesCollection.doc(resolvedRequestId))
      : null;
    let request = requestSnap?.exists ? { requestId: requestSnap.id, ...(requestSnap.data() || {}) } : null;
    let outcome = outcomeSnap?.exists ? { requestId: outcomeSnap.id, ...(outcomeSnap.data() || {}) } : null;

    if (!resolvedLocalId) {
      resolvedLocalId =
        ensureString(request?.localSupportSessionId || outcome?.localSupportSessionId || '', '')
          .trim()
          .slice(0, 128);
    }

    let supportSessionSnap = resolvedLocalId
      ? await tx.get(supportSessionsCollection.doc(resolvedLocalId))
      : null;
    let supportSession = supportSessionSnap?.exists ? supportSessionSnap.data() || {} : null;

    const lockSnap = await tx.get(lockRef);
    const lock = lockSnap.exists ? lockSnap.data() || {} : null;
    if (!resolvedRequestId) {
      const supportRequestId =
        ensureString(supportSession?.queueRequestId || '', '').trim().slice(0, 64).toUpperCase();
      const lockMatchesLocal =
        resolvedLocalId &&
        ensureString(lock?.localSupportSessionId || '', '').trim() === resolvedLocalId;
      resolvedRequestId =
        supportRequestId ||
        (lockMatchesLocal
          ? ensureString(lock?.requestId || '', '').trim().slice(0, 64).toUpperCase()
          : '');
      if (resolvedRequestId) {
        requestSnap = await tx.get(requestsCollection.doc(resolvedRequestId));
        outcomeSnap = await tx.get(outcomesCollection.doc(resolvedRequestId));
        request = requestSnap.exists ? { requestId: requestSnap.id, ...(requestSnap.data() || {}) } : null;
        outcome = outcomeSnap.exists ? { requestId: outcomeSnap.id, ...(outcomeSnap.data() || {}) } : null;
      }
    }

    if (!resolvedLocalId) {
      resolvedLocalId =
        ensureString(request?.localSupportSessionId || outcome?.localSupportSessionId || '', '')
          .trim()
          .slice(0, 128);
      if (resolvedLocalId) {
        supportSessionSnap = await tx.get(supportSessionsCollection.doc(resolvedLocalId));
        supportSession = supportSessionSnap.exists ? supportSessionSnap.data() || {} : null;
      }
    }

    const anchorId = resolvedLocalId ? queueAnchorDocId(normalizedUid, resolvedLocalId) : null;
    const anchorRef = anchorId ? anchorsCollection.doc(anchorId) : null;
    const anchorSnap = anchorRef ? await tx.get(anchorRef) : null;
    const anchor = anchorSnap?.exists ? anchorSnap.data() || {} : null;

    const requestOwnedByVerifiedLegacyPhone =
      request &&
      !ensureString(request.clientUid || '', '').trim() &&
      normalizedVerifiedPhone &&
      normalizePhone(request.clientPhone || '') === normalizedVerifiedPhone
        ? { ...request, clientUid: normalizedUid }
        : request;
    const decision = decideQueueCancellation({
      authUid: normalizedUid,
      requestedRequestId: requestedRequestId,
      requestedLocalSupportSessionId: requestedLocalId,
      request: requestOwnedByVerifiedLegacyPhone,
      supportSession,
      outcome,
    });
    if (decision.action === 'already_cancelled') {
      return {
        ...decision,
        requestData: requestOwnedByVerifiedLegacyPhone,
      };
    }

    const finalRequestId = decision.requestId;
    const finalLocalId = decision.localSupportSessionId;
    if (requestSnap?.exists) tx.delete(requestSnap.ref);
    if (
      lockSnap.exists &&
      (
        ensureString(lock?.requestId || '', '').trim().toUpperCase() === finalRequestId ||
        ensureString(lock?.localSupportSessionId || '', '').trim() === finalLocalId
      )
    ) {
      tx.delete(lockRef);
    }
    if (
      anchorRef &&
      anchorSnap?.exists &&
      (
        !finalRequestId ||
        ensureString(anchor?.requestId || '', '').trim().toUpperCase() === finalRequestId
      )
    ) {
      tx.delete(anchorRef);
    }

    if (finalLocalId) {
      tx.set(
        supportSessionsCollection.doc(finalLocalId),
        {
          queueRequestId: finalRequestId || supportSession?.queueRequestId || null,
          queueStatus: 'cancelled',
          status: 'cancelled',
          queueCancelledAt: now,
          updatedAt: now,
          expiresAt: admin.firestore.Timestamp.fromMillis(
            now + 30 * 24 * 60 * 60 * 1000
          ),
        },
        { merge: true }
      );
    }
    if (finalRequestId) {
      tx.set(
        outcomesCollection.doc(finalRequestId),
        {
          requestId: finalRequestId,
          clientUid: normalizedUid,
          localSupportSessionId: finalLocalId || null,
          status: 'cancelled',
          cancelledAt: now,
          updatedAt: now,
        },
        { merge: true }
      );
    }

    return {
      ...decision,
      requestData: requestOwnedByVerifiedLegacyPhone,
    };
  });
};

io.on('connection', (socket) => {
  bindAuthorizedSocketRooms(socket, {
    uid: socket.data?.authUid || socket.user?.uid || null,
    isActiveTech: socket.data?.isActiveTech === true,
  });
  connectionIndex.set(socket.id, { socketId: socket.id, userType: 'unknown', sessionId: null });
  void rebindQueuedRequestsToClientSocket(socket).catch((error) => {
    console.error('Failed to rebind queued requests to reconnecting client socket', error);
  });

  // 1) CLIENTE cria um pedido de suporte (fila real)
  // payload: { clientName?, brand?, model? }
  socket.on('support:request', async (payload = {}, ack) => {
    const rateDecision = supportRequestRateLimiter.consume({
      socketId: socket.id,
      ipKey: legacyJoinIpKey(socket),
    });
    if (!rateDecision.allowed) {
      emitSupportQueueError(
        socket,
        ack,
        new SupportQueuePolicyError('rate_limited', 429),
        {
          localSupportSessionId: ensureString(
            payload?.localSupportSessionId ||
              payload?.supportProfile?.localSupportSessionId ||
              '',
            ''
          ).trim(),
        }
      );
      return;
    }
    const requestsCollection = getRequestsCollection();
    if (!requestsCollection) {
      console.error('Firestore not configured. Cannot enqueue support request.');
      emitSupportQueueError(
        socket,
        ack,
        new SupportQueuePolicyError('firestore_unavailable', 503)
      );
      return;
    }

    let decodedClient = null;
    try {
      decodedClient = await resolveSocketAuthFromPayload(socket, payload);
    } catch (err) {
      console.error('Failed to resolve client auth for support:request', err);
      emitSupportQueueError(socket, ack, new SupportQueuePolicyError('invalid_token', 401));
      return;
    }

    if (!decodedClient?.uid) {
      emitSupportQueueError(socket, ack, new SupportQueuePolicyError('missing_token', 401));
      return;
    }

    const now = Date.now();
    const normalizedClientUid =
      ensureString(decodedClient.uid || payload.clientUid || payload.uid || '', '').trim() || null;
    const supportProfile = sanitizeSupportProfile(payload.supportProfile);
    const localSupportSessionId = ensureString(
      supportProfile.localSupportSessionId || payload.localSupportSessionId || '',
      ''
    )
      .trim()
      .slice(0, 128);
    if (!localSupportSessionId) {
      emitSupportQueueError(
        socket,
        ack,
        new SupportQueuePolicyError('local_support_session_required', 400)
      );
      return;
    }

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
      emitSupportQueueError(
        socket,
        ack,
        new SupportQueuePolicyError('client_identity_required', 400),
        { localSupportSessionId }
      );
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
        hasVerifiedIdentityProof: Boolean(tokenPhone),
        identityAssurance: tokenPhone ? 'firebase_auth_phone' : 'uid_bound',
      });
    } catch (error) {
      console.error('Failed to ensure client identity from phone', error);
      emitSupportQueueError(
        socket,
        ack,
        new SupportQueuePolicyError('client_resolution_failed', 500),
        { localSupportSessionId }
      );
      return;
    }

    const resolvedClient = resolvedClientContext?.client || null;
    if (!resolvedClient?.id) {
      emitSupportQueueError(
        socket,
        ack,
        new SupportQueuePolicyError('client_resolution_failed', 500),
        { localSupportSessionId }
      );
      return;
    }
    const resolvedClientPhone = normalizePhone(resolvedClient.phone || '') || normalizedPhone || null;

    const eligibility = buildClientEligibility(resolvedClient);
    if (!eligibility.canRequest) {
      emitSupportQueueError(
        socket,
        ack,
        new SupportQueuePolicyError(eligibility.reason || 'support_blocked', 409, {
          message: 'Necessario adquirir creditos para novo atendimento.',
          freeFirstSupportUsed: ensureBoolean(resolvedClient.freeFirstSupportUsed, false),
          credits: eligibility.credits,
        }),
        { localSupportSessionId }
      );
      return;
    }

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
    const requestData = {
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
      let reservation = null;
      let lastCollision = null;
      for (let attempt = 0; attempt < 3 && !reservation; attempt += 1) {
        const generatedRequestId = generateSessionId();
        try {
          reservation = await reserveSupportQueueRequest({
            authUid: normalizedClientUid,
            localSupportSessionId,
            generatedRequestId,
            requestData,
          });
        } catch (error) {
          if (error instanceof SupportQueuePolicyError && error.code === 'request_id_collision') {
            lastCollision = error;
            continue;
          }
          throw error;
        }
      }
      if (!reservation) throw lastCollision || new SupportQueuePolicyError('request_failed', 500);

      const successPayload = {
        ok: true,
        requestId: reservation.requestId,
        reused: reservation.reused === true,
        localSupportSessionId: reservation.localSupportSessionId,
        status: reservation.status || 'queued',
        ...(reservation.realtimeSessionId
          ? { sessionId: reservation.realtimeSessionId }
          : {}),
      };

      respondAck(ack, successPayload);
      socket.emit('support:enqueued', successPayload);

      if (reservation.status === 'queued' && reservation.requestData) {
        emitQueueUpdated({ requestId: reservation.requestId, state: 'queued' });
        try {
          await persistQueueNotification({
            requestId: reservation.requestId,
            requestData: reservation.requestData,
            state: 'queued',
          });
        } catch (error) {
          console.error('Failed to persist post-commit queue notification', error);
        }
      }
    } catch (err) {
      console.error('Failed to persist support request', err);
      emitSupportQueueError(socket, ack, err, { localSupportSessionId });
    }
  });

  // Mantém sua sinalização atual por sala (sessionId)
  socket.on('support:cancel', async (payload = {}, ack) => {
    const requestId = ensureString(payload.requestId || '', '').trim().slice(0, 64).toUpperCase();
    const localSupportSessionId = ensureString(
      payload.localSupportSessionId || payload.supportSessionId || '',
      ''
    )
      .trim()
      .slice(0, 128);
    if (!requestId && !localSupportSessionId) {
      emitSupportQueueError(
        socket,
        ack,
        new SupportQueuePolicyError('invalid_payload', 400)
      );
      return;
    }

    let decodedClient = socket.user || null;
    try {
      if (!decodedClient?.uid) {
        decodedClient = await resolveSocketAuthFromPayload(socket, payload);
      }
    } catch (err) {
      console.error('Failed to resolve client auth for support:cancel', err);
      emitSupportQueueError(
        socket,
        ack,
        new SupportQueuePolicyError('invalid_token', 401),
        { requestId, localSupportSessionId }
      );
      return;
    }

    const authUid = ensureString(decodedClient?.uid || '', '').trim();
    if (!authUid) {
      emitSupportQueueError(
        socket,
        ack,
        new SupportQueuePolicyError('missing_token', 401),
        { requestId, localSupportSessionId }
      );
      return;
    }

    try {
      const result = await cancelSupportQueueRequest({
        authUid,
        requestId,
        localSupportSessionId,
        verifiedPhone: decodedClient?.phone_number,
      });
      const finalRequestId = result.requestId || requestId || null;
      const finalLocalId = result.localSupportSessionId || localSupportSessionId || null;
      const eventPayload = {
        requestId: finalRequestId,
        localSupportSessionId: finalLocalId,
        reason: 'client_cancelled',
      };

      respondAck(ack, {
        ok: true,
        removed: result.removed === true,
        reused: result.action === 'already_cancelled',
        requestId: finalRequestId,
        localSupportSessionId: finalLocalId,
      });

      if (result.requestData && finalRequestId) {
        try {
          await persistQueueNotification({
            requestId: finalRequestId,
            requestData: result.requestData,
            state: 'removed',
            reason: 'client_cancelled',
          });
        } catch (error) {
          console.error('Failed to persist post-commit cancellation notification', error);
        }
      }

      const targetSocketId = ensureString(
        result.requestData?.clientSocketId || result.requestData?.clientId || '',
        ''
      ).trim();
      if (targetSocketId) io.to(targetSocketId).emit('support:rejected', eventPayload);
      socket.emit('support:rejected', eventPayload);
      if (finalRequestId) emitQueueUpdated({ requestId: finalRequestId, state: 'removed' });
    } catch (err) {
      console.error('Failed to cancel support request', err);
      emitSupportQueueError(socket, ack, err, { requestId, localSupportSessionId });
    }
  });

  socket.on('join', async (payload = {}, ack) => {
    const joinAttempt = legacyJoinRateLimiter.consume({
      socketId: socket.id,
      ipKey: legacyJoinIpKey(socket),
    });
    if (!joinAttempt.allowed) {
      return respondAck(ack, {
        ok: false,
        err: 'rate_limited',
        retryAfterMs: joinAttempt.retryAfterMs,
      });
    }

    const access = await resolveLegacyJoinAccess(socket, payload);
    if (!access.ok) {
      return respondAck(ack, { ok: false, err: access.code || 'forbidden' });
    }

    const { room, role, uid, reservationId, expiresAtMs, socketRoom } = access;
    const previousJoin = socket.data?.legacyJoin || null;
    const isSameAuthorization =
      previousJoin &&
      previousJoin.socketRoom === socketRoom &&
      previousJoin.uid === uid &&
      previousJoin.role === role;
    if (
      previousJoin?.socketRoom &&
      previousJoin.socketRoom !== socketRoom
    ) {
      socket.to(previousJoin.socketRoom).emit('peer-left');
      socket.leave(previousJoin.socketRoom);
    }
    socket.join(socketRoom);
    socket.data.legacyJoin = {
      room,
      role,
      uid,
      reservationId,
      expiresAtMs,
      socketRoom,
      authorizedAt: Date.now(),
    };
    if (!isSameAuthorization) {
      socket.to(socketRoom).emit('peer-joined', { role });
    }
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

    const signalAccess = validateLegacySignalAuthorization({
      authorization: socket.data?.legacyJoin,
      roomCode: room,
      authUid: socket?.user?.uid || '',
    });
    if (
      !signalAccess.ok ||
      !socket.rooms.has(signalAccess.socketRoom)
    ) {
      if (signalAccess.code === 'join-expired') {
        const expiredSocketRoom = socket.data?.legacyJoin?.socketRoom;
        if (expiredSocketRoom) socket.leave(expiredSocketRoom);
        socket.data.legacyJoin = null;
      }
      return respondAck(ack, {
        ok: false,
        err: signalAccess.code || 'not-joined',
      });
    }

    socket.to(signalAccess.socketRoom).emit('signal', payload.data);
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
    const roleRoom = sessionRoleSocketRoom(sessionId, access.role);
    if (roleRoom) socket.join(roleRoom);
    if (!socket.data.sessionRoles) socket.data.sessionRoles = {};
    socket.data.sessionRoles[sessionId] = access.role;
    socket.data.sessionId = sessionId;
    socket.data.userType = access.role;
    connectionIndex.set(socket.id, { socketId: socket.id, userType: access.role, sessionId });
    respondAck(ack, { ok: true, role: access.role });
  });

  socket.on('session:chat:send', async (msg = {}, ack) => {
    const sessionId = normalizeSessionId(msg.sessionId);
    const text = ensureLongString(msg.text || '', '', 2000).trim();
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
    if (
      !sessionId ||
      !hasRenderableContent ||
      !['text', 'audio', 'image', 'file'].includes(type) ||
      !isAllowedSessionMediaUrl(audioUrl, sessionId) ||
      !isAllowedSessionMediaUrl(imageUrl, sessionId) ||
      !isAllowedSessionMediaUrl(fileUrl, sessionId)
    ) {
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
    const actorUid = ensureString(socket.user?.uid || '', '').trim();
    if (!actorUid) {
      return respondAck(ack, { ok: false, err: 'missing_token' });
    }
    const providedId = ensureString(msg.id || '', '').trim();
    const messageId = /^[A-Za-z0-9._:-]{1,128}$/.test(providedId)
      ? providedId
      : randomLowercaseId(20);
    const ts = Date.now();
    const senderName =
      access.role === 'tech'
        ? ensureString(access.sessionData?.techName || access.sessionData?.tech?.name || '', '').trim()
        : ensureString(access.sessionData?.clientName || access.sessionData?.client?.name || '', '').trim();
    const out = {
      id: messageId,
      sessionId,
      from: access.role,
      senderUid: actorUid,
      ...(senderName ? { fromName: senderName } : {}),
      type,
      text,
      audioUrl,
      imageUrl,
      fileUrl,
      ...(fileName ? { fileName } : {}),
      ...(contentType ? { contentType, mimeType: contentType } : {}),
      ...(fileSize != null ? { size: fileSize, fileSize } : {}),
      status: 'sent',
      ts,
      createdAt: ts,
    };

    const messageRef = snapshot.ref.collection('messages').doc(messageId);
    let created = false;
    try {
      await messageRef.create(out);
      created = true;
      await snapshot.ref.set(
        {
          lastMessageAt: ts,
          updatedAt: ts,
          'extra.lastMessageAt': ts,
        },
        { merge: true }
      );
    } catch (err) {
      const code = ensureString(err?.code || '', '').toLowerCase();
      if (code === '6' || code.includes('already-exists') || code.includes('already_exists')) {
        const existing = await messageRef.get().catch(() => null);
        const existingData = existing?.exists ? existing.data() || {} : {};
        const sameActor =
          ensureString(existingData.senderUid || '', '').trim() === actorUid ||
          (
            !existingData.senderUid &&
            ensureString(existingData.from || '', '').trim().toLowerCase() === access.role
          );
        if (sameActor) {
          return respondAck(ack, { ok: true, id: messageId, reused: true });
        }
        return respondAck(ack, { ok: false, err: 'message-id-conflict' });
      }
      console.error('Failed to store chat message', err);
      return respondAck(ack, { ok: false, err: 'store-failed' });
    }

    if (created) {
      socket.to(room).emit('session:chat:new', out);
      await emitSessionUpdated(sessionId);
    }

    respondAck(ack, { ok: true, id: out.id });
  });

  socket.on('session:command', async (cmd = {}, ack) => {
    const sessionId = normalizeSessionId(cmd.sessionId);
    const rawType = ensureString(cmd.type || '', '').trim();
    if (!sessionId || !ALLOWED_SESSION_COMMAND_TYPES.has(rawType)) {
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
    const actorUid = ensureString(socket.user?.uid || '', '').trim();
    const by = actorUid || byRole || socket.id;
    const rawCommandData = sanitizeSessionCommandData(
      cmd.data,
      normalizedType,
      byRole
    );
    const requestedEventId = ensureString(cmd.id || '', '').trim();
    const eventId = /^[A-Za-z0-9._:-]{1,128}$/.test(requestedEventId)
      ? requestedEventId
      : `${ts.toString(36)}-${randomLowercaseId(8)}`;
    const enriched = {
      id: eventId,
      sessionId,
      type: normalizedType,
      rawType,
      data:
        normalizedType === 'end' && byRole === 'client'
          ? null
          : rawCommandData,
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
    if (normalizedType !== 'end') {
      socket.to(room).emit('session:command', socketPayload);
    }

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

    const technicalClosureSummary = {};
    if (normalizedType === 'end' && byRole === 'tech') {
      const problemSummary =
        rawCommandData?.problemSummary ??
        rawCommandData?.symptom ??
        session.symptom;
      const solutionSummary =
        rawCommandData?.solutionSummary ??
        rawCommandData?.solution ??
        session.solution;
      const internalNotes =
        rawCommandData?.internalNotes ??
        rawCommandData?.notes ??
        session.notes;
      if (typeof problemSummary !== 'undefined') {
        technicalClosureSummary.problemSummary = problemSummary;
      }
      if (typeof solutionSummary !== 'undefined') {
        technicalClosureSummary.solutionSummary = solutionSummary;
      }
      if (typeof internalNotes !== 'undefined') {
        technicalClosureSummary.internalNotes = internalNotes;
      }
    }

    try {
      await snapshot.ref.collection('events').doc(eventId).set(enriched);
      await snapshot.ref.set(
        {
          ...updates,
          ...telemetryUpdates,
        },
        { merge: true }
      );
      if (normalizedType === 'end') {
        await finalizeSupportSessionFromRealtime({
          realtimeSessionId: sessionId,
          realtimeSession: {
            ...session,
            ...updates,
            ...telemetryUpdates,
            sessionId,
          },
          actorUid,
          actorRole: byRole,
          authorizedTech: byRole === 'tech',
          summary: technicalClosureSummary,
          now: ts,
        });
      }
    } catch (err) {
      console.error('Failed to persist command event', err);
      return respondAck(ack, {
        ok: false,
        err:
          err instanceof SupportSessionClosureError
            ? err.code
            : 'store-failed',
      });
    }

    if (normalizedType === 'end') {
      socket.to(room).emit('session:command', socketPayload);
      io.to(room).emit('session:ended', { sessionId, reason: 'peer_ended' });
      io.socketsLeave(room);
      ['tech', 'client'].forEach((role) => {
        const roleRoom = sessionRoleSocketRoom(sessionId, role);
        if (roleRoom) io.socketsLeave(roleRoom);
      });
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
    const actorUid = ensureString(socket.user?.uid || '', '').trim();
    const from = access.role;
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
      by: actorUid || from,
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
    if (socket.data?.legacyJoin?.socketRoom) {
      socket.to(socket.data.legacyJoin.socketRoom).emit('peer-left');
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

app.get('/api/notifications/queue', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const collection = getQueueNotificationsCollection();
  if (!collection) {
    console.error('Firestore not configured. Cannot list queue notifications.');
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const limit = Math.max(1, Math.min(200, ensureInteger(req.query.limit, 80)));
  try {
    const snapshot = await collection.orderBy('createdAt', 'desc').limit(limit).get();
    const notifications = snapshot.docs
      .map((doc) => {
        const data = doc.data() || {};
        return {
          id: doc.id,
          requestId: ensureString(data.requestId || doc.id, '').trim() || doc.id,
          state: ensureString(data.state || 'queued', 'queued'),
          reason: ensureString(data.reason || '', '').trim() || null,
          sessionId: ensureString(data.sessionId || '', '').trim() || null,
          techUid: ensureString(data.techUid || '', '').trim() || null,
          techName: ensureString(data.techName || '', '').trim() || null,
          clientName: ensureString(data.clientName || 'Cliente', 'Cliente'),
          clientPhone: normalizePhone(data.clientPhone || '') || null,
          clientUid: ensureString(data.clientUid || '', '').trim() || null,
          clientRecordId: ensureString(data.clientRecordId || '', '').trim() || null,
          brand: ensureString(data.brand || '', '').trim() || null,
          model: ensureString(data.model || '', '').trim() || null,
          osVersion: ensureString(data.osVersion || '', '').trim() || null,
          issue: ensureString(data.issue || '', '').trim() || null,
          plan: ensureString(data.plan || '', '').trim() || null,
          createdAt: parseReportTimestamp(data.createdAt || null, null),
          updatedAt: parseReportTimestamp(data.updatedAt || null, null),
        };
      })
      .filter((item) => Number.isFinite(Number(item.createdAt)));
    return res.json({ notifications });
  } catch (err) {
    console.error('Failed to fetch queue notifications', err);
    return res.status(500).json({ error: 'firestore_error' });
  }
});

app.get('/api/notifications/admin', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const collection = getAdminNotificationsCollection();
  if (!collection) return res.status(503).json({ error: 'firestore_unavailable' });
  const limitParam = Math.max(1, Math.min(120, ensureInteger(req.query.limit, 60)));
  try {
    const snapshot = await collection.orderBy('createdAt', 'desc').limit(limitParam).get();
    const notifications = snapshot.docs.map((doc) => {
      const data = doc.data() || {};
      return {
        id: doc.id,
        title: ensureString(data.title || 'Notificacao administrativa', 'Notificacao administrativa'),
        body: ensureLongString(data.body || '', '', 500),
        type: normalizeNotificationType(data.type || 'GENERAL_INFO', 'GENERAL_INFO'),
        iconType: ensureString(data.iconType || 'bell', 'bell'),
        read: data.read === true,
        status: normalizeNotificationStatus(data.status || (data.read ? 'read' : 'unread')),
        createdAt: toNotificationMillis(data.createdAt, null),
        updatedAt: toNotificationMillis(data.updatedAt, null),
      };
    });
    return res.json({ notifications });
  } catch (error) {
    console.error('Failed to list admin notifications', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/notifications/admin/read-all', requireAuth(['tech']), requireTechAccess, async (_req, res) => {
  const collection = getAdminNotificationsCollection();
  if (!collection) return res.status(503).json({ error: 'firestore_unavailable' });
  try {
    const snapshot = await collection.where('read', '==', false).limit(100).get();
    const batch = db.batch();
    snapshot.docs.forEach((doc) => {
      batch.set(doc.ref, { read: true, status: 'read', readAt: Date.now(), updatedAt: Date.now() }, { merge: true });
    });
    await batch.commit();
    return res.json({ ok: true, updated: snapshot.size });
  } catch (error) {
    console.error('Failed to mark admin notifications as read', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.get('/api/notifications/center', requireAuth(['tech']), requireTechAccess, async (_req, res) => {
  const notificationsCollection = getClientNotificationsCollection();
  const eventsCollection = getNotificationEventsCollection();
  if (!notificationsCollection || !eventsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }
  try {
    const [campaigns, rules, audienceFilters, individualSnapshot, eventsSnapshot] = await Promise.all([
      listNotificationCampaigns(),
      listNotificationRules(),
      buildAudienceFilterSummaries(),
      notificationsCollection.orderBy('createdAt', 'desc').limit(120).get(),
      eventsCollection.orderBy('createdAt', 'desc').limit(120).get(),
    ]);
    const individual = individualSnapshot.docs.map(toClientNotificationSummary);
    const history = eventsSnapshot.docs.map((doc) => {
      const data = doc.data() || {};
      return {
        id: doc.id,
        eventType: ensureString(data.eventType || '', '').trim() || 'NOTIFICATION_EVENT',
        notificationId: ensureString(data.notificationId || '', '').trim() || null,
        clientId: ensureString(data.clientId || '', '').trim() || null,
        campaignId: ensureString(data.campaignId || '', '').trim() || null,
        ruleId: ensureString(data.ruleId || '', '').trim() || null,
        actorName: ensureString(data.actorName || '', '').trim() || null,
        status: ensureString(data.status || '', '').trim() || null,
        error: ensureLongString(data.error || '', '', 1000).trim() || null,
        createdAt: toNotificationMillis(data.createdAt, null),
      };
    });
    return res.json({
      campaigns,
      rules,
      individual,
      history,
      audienceFilters,
      options: {
        types: Array.from(NOTIFICATION_TYPES),
        actions: Array.from(NOTIFICATION_ACTION_TYPES),
        priorities: Array.from(NOTIFICATION_PRIORITIES),
      },
    });
  } catch (error) {
    console.error('Failed to load notification center', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/notifications/client/send', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const clientId = ensureString(req.body?.clientId || req.body?.clientRecordId || '', '').trim().slice(0, 128);
  const clientUid = ensureString(req.body?.clientUid || '', '').trim().slice(0, 256);
  const phone = normalizePhone(req.body?.phone || req.body?.clientPhone || '');
  const title = ensureString(req.body?.title || '', '').trim();
  const body = ensureLongString(req.body?.body || req.body?.message || '', '', 600).trim();
  const type = normalizeNotificationType(req.body?.type || 'MANUAL_NOTICE');
  const actionType = normalizeNotificationActionType(req.body?.actionType || req.body?.cta || 'NONE');
  const delivery = normalizeDelivery(req.body?.delivery || {});
  const idempotencyKey = ensureString(req.body?.idempotencyKey || req.get('Idempotency-Key') || '', '').trim();
  if (!body || !title) {
    return res.status(400).json({ error: 'invalid_payload', message: 'Titulo e mensagem sao obrigatorios.' });
  }
  if (!delivery.inApp && !delivery.push) {
    return res.status(400).json({ error: 'invalid_delivery', message: 'Selecione pelo menos um canal.' });
  }
  try {
    const context = await resolveClientContext({ clientRecordId: clientId, clientUid, phone });
    if (!context.client?.id && !clientUid) {
      return res.status(404).json({ error: 'client_not_found' });
    }
    const actor = {
      uid: ensureString(req.user?.uid || '', '').trim() || null,
      name: ensureString(req.techAccess?.techDoc?.name || req.user?.name || 'Tecnico', 'Tecnico').trim() || 'Tecnico',
    };
    const result = await createClientNotification({
      client: context.client || { id: clientId },
      clientUid,
      title,
      body,
      type,
      iconType: req.body?.iconType,
      priority: req.body?.priority,
      actionLabel: req.body?.actionLabel || req.body?.cta,
      actionType,
      actionPayload: req.body?.actionPayload || {},
      delivery,
      source: 'manual',
      createdBy: actor,
      expiresAfterDays: ensureInteger(req.body?.expiresAfterDays, DEFAULT_NOTIFICATION_TTL_DAYS),
      dedupeKey: idempotencyKey
        ? `manual:${idempotencyKey}`
        : `manual:${context.client?.id || clientUid}:${Date.now()}:${randomLowercaseId(8)}`,
    });
    if (!result.ok) return res.status(400).json({ error: result.error || 'notification_failed' });
    await createAdminNotification({
      title: 'Notificacao enviada',
      body: `${actor.name} enviou "${title}" para ${context.client?.name || 'cliente'}.`,
      type,
      iconType: iconTypeForNotification(type),
      actorUid: actor.uid,
      metadata: { notificationId: result.notification?.id, clientId: context.client?.id || clientId },
    });
    return res.json({ ok: true, ...result });
  } catch (error) {
    console.error('Failed to send client notification', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/notifications/campaigns', requireAuth(['tech']), requireSupervisor, async (req, res) => {
  const campaignsCollection = getNotificationCampaignsCollection();
  if (!campaignsCollection) return res.status(503).json({ error: 'firestore_unavailable' });
  const title = ensureString(req.body?.title || '', '').trim();
  const body = ensureLongString(req.body?.body || req.body?.message || '', '', 600).trim();
  const type = normalizeNotificationType(req.body?.type || 'MANUAL_NOTICE');
  const actionType = normalizeNotificationActionType(req.body?.actionType || req.body?.cta || 'NONE');
  const targetFilters = ensureArray(req.body?.targetFilters || req.body?.filters).map((item) => ensureString(item || '', '').trim()).filter(Boolean);
  const delivery = normalizeDelivery(req.body?.delivery || {});
  const confirmedLargeAudience = req.body?.confirmedLargeAudience === true;
  if (!title || !body || !targetFilters.length) {
    return res.status(400).json({ error: 'invalid_payload' });
  }
  if (!delivery.inApp && !delivery.push) {
    return res.status(400).json({ error: 'invalid_delivery' });
  }
  try {
    const audience = await resolveAudience(targetFilters);
    if (audience.clients.length > 25 && !confirmedLargeAudience) {
      return res.status(409).json({
        error: 'large_audience_confirmation_required',
        estimatedAudience: audience.clients.length,
      });
    }
    const campaignId = `camp_${Date.now()}_${randomLowercaseId(8)}`;
    const actor = {
      uid: ensureString(req.user?.uid || '', '').trim() || null,
      name: ensureString(req.techAccess?.techDoc?.name || req.user?.name || 'Supervisor', 'Supervisor').trim() || 'Supervisor',
    };
    const campaign = {
      campaignId,
      title,
      body,
      type,
      iconType: iconTypeForNotification(type, req.body?.iconType),
      actionLabel: actionLabelForType(type, actionType, req.body?.actionLabel || req.body?.cta || ''),
      actionType,
      targetFilters: { selected: targetFilters },
      estimatedAudience: audience.clients.length,
      delivery,
      status: 'sent',
      schedule: { sendNow: true, scheduledAt: null },
      createdBy: actor,
      createdAt: Date.now(),
      sentAt: Date.now(),
      stats: { created: 0, pushed: 0, read: 0, dismissed: 0, failed: 0 },
    };
    await campaignsCollection.doc(campaignId).set(campaign);
    const stats = { created: 0, pushed: 0, read: 0, dismissed: 0, failed: 0, duplicates: 0 };
    for (const item of audience.clients) {
      const result = await createClientNotification({
        client: item.client,
        clientUid: item.latestDevice?.clientUid || null,
        title,
        body,
        type,
        iconType: campaign.iconType,
        priority: req.body?.priority,
        actionLabel: campaign.actionLabel,
        actionType,
        actionPayload: req.body?.actionPayload || {},
        delivery,
        source: 'campaign',
        campaignId,
        createdBy: actor,
        expiresAfterDays: ensureInteger(req.body?.expiresAfterDays, DEFAULT_NOTIFICATION_TTL_DAYS),
        dedupeKey: `campaign:${campaignId}:${item.client.id}`,
      });
      if (result.created) stats.created += 1;
      if (result.duplicate) stats.duplicates += 1;
      stats.pushed += Math.max(0, ensureInteger(result.push?.sent, 0));
      stats.failed += Math.max(0, ensureInteger(result.push?.failed, 0));
    }
    await campaignsCollection.doc(campaignId).set({ stats, updatedAt: Date.now() }, { merge: true });
    await recordNotificationEvent('CAMPAIGN_SENT', {
      campaignId,
      actorUid: actor.uid,
      actorName: actor.name,
      status: 'sent',
      metadata: { targetFilters, estimatedAudience: audience.clients.length, stats },
    });
    await createAdminNotification({
      title: 'Campanha enviada',
      body: `${title}: ${stats.created} notificacao(oes) criadas.`,
      type,
      iconType: campaign.iconType,
      actorUid: actor.uid,
      metadata: { campaignId, stats },
    });
    return res.json({ ok: true, campaign: { ...campaign, stats }, stats });
  } catch (error) {
    console.error('Failed to create notification campaign', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.put('/api/notifications/rules/:ruleId', requireAuth(['tech']), requireSupervisor, async (req, res) => {
  const rulesCollection = getNotificationRulesCollection();
  if (!rulesCollection) return res.status(503).json({ error: 'firestore_unavailable' });
  const ruleId = ensureString(req.params.ruleId || '', '').trim().slice(0, 128);
  if (!ruleId) return res.status(400).json({ error: 'invalid_rule' });
  const allowedConditionKeys = new Set([
    'recommendedVersionCode',
    'minimumVersionCode',
    'creditsLessOrEqual',
    'creditsGreaterOrEqual',
    'creditsEquals',
    'freeFirstSupportUsed',
    'minimumCompletedSessions',
  ]);
  const rawConditions = req.body?.conditions && typeof req.body.conditions === 'object' ? req.body.conditions : {};
  const conditions = {};
  Object.entries(rawConditions).forEach(([key, value]) => {
    if (!allowedConditionKeys.has(key)) return;
    if (typeof value === 'boolean') conditions[key] = value;
    else conditions[key] = Math.max(0, ensureInteger(value, 0));
  });
  try {
    const snap = await rulesCollection.doc(ruleId).get();
    if (!snap.exists) return res.status(404).json({ error: 'rule_not_found' });
    const current = toNotificationRuleSummary({ id: snap.id, ...(snap.data() || {}) });
    const nextType = normalizeNotificationType(req.body?.type || current.type);
    const template = req.body?.notificationTemplate && typeof req.body.notificationTemplate === 'object'
      ? req.body.notificationTemplate
      : {};
    const update = {
      enabled: typeof req.body?.enabled === 'boolean' ? req.body.enabled : current.enabled,
      priority: normalizeNotificationPriority(req.body?.priority || current.priority, nextType),
      conditions: Object.keys(conditions).length ? conditions : current.conditions,
      notificationTemplate: {
        title: ensureString(template.title || current.notificationTemplate.title || '', '').trim().slice(0, 96),
        body: ensureLongString(template.body || current.notificationTemplate.body || '', '', 600).trim(),
        actionLabel: ensureString(template.actionLabel || current.notificationTemplate.actionLabel || '', '').trim() || null,
        actionType: normalizeNotificationActionType(template.actionType || current.notificationTemplate.actionType || 'NONE'),
        iconType: iconTypeForNotification(nextType, template.iconType || current.notificationTemplate.iconType || ''),
      },
      delivery: normalizeDelivery(req.body?.delivery || current.delivery),
      cooldown: { days: Math.max(0, ensureInteger(req.body?.cooldown?.days ?? current.cooldown.days, current.cooldown.days)) },
      maxOccurrences: Math.max(1, ensureInteger(req.body?.maxOccurrences ?? current.maxOccurrences, current.maxOccurrences)),
      expiresAfterDays: Math.max(1, ensureInteger(req.body?.expiresAfterDays ?? current.expiresAfterDays, current.expiresAfterDays)),
      updatedAt: Date.now(),
      updatedBy: ensureString(req.user?.uid || '', '').trim() || null,
    };
    await rulesCollection.doc(ruleId).set(update, { merge: true });
    await recordNotificationEvent(update.enabled ? 'RULE_UPDATED' : 'RULE_DISABLED', {
      ruleId,
      actorUid: ensureString(req.user?.uid || '', '').trim() || null,
      actorName: ensureString(req.techAccess?.techDoc?.name || req.user?.name || '', '').trim() || null,
      status: 'updated',
    });
    return res.json({ ok: true, rule: toNotificationRuleSummary({ id: ruleId, ...current, ...update }) });
  } catch (error) {
    console.error('Failed to update notification rule', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/notifications/rules/run', requireAuth(['tech']), requireSupervisor, async (req, res) => {
  try {
    const result = await executeNotificationRules({
      actorUid: ensureString(req.user?.uid || '', '').trim() || null,
      actorName: ensureString(req.techAccess?.techDoc?.name || req.user?.name || '', '').trim() || null,
    });
    if (!result.ok) return res.status(503).json({ error: result.error || 'rule_engine_failed' });
    return res.json(result);
  } catch (error) {
    console.error('Failed to run notification rules', error);
    return res.status(500).json({ error: 'server_error' });
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
    try {
      validateFileSignature(file, 'image', extension);
    } catch (error) {
      return res.status(Number(error?.status) || 400).json({
        error:
          ensureString(error?.code || '', '').trim() ||
          'invalid_file_signature',
        message: 'O conteúdo do arquivo não corresponde a uma imagem permitida.',
      });
    }

    const now = Date.now();
    const uploadId = randomLowercaseId(14);
    const storagePath = `catalog/device-images/${key}/${now}-${uploadId}.${extension}`;
    const downloadToken = randomLowercaseId(32);
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

  if (!name) {
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
  const fallbackClientId = normalizedPhone
    ? clientDocIdFromPhone(normalizedPhone)
    : clientDocIdFromContext({
        sessionId,
        requestId,
        clientUid: explicitClientUid,
        deviceAnchor: resolvedDeviceAnchor,
      });
  const clientId = resolvedSeedContext?.client?.id || fallbackClientId;
  if (!clientId) {
    return res.status(400).json({ error: 'invalid_client_context' });
  }
  const existingResolvedPhone = normalizePhone(resolvedSeedContext?.client?.phone || '') || null;
  const existingResolvedEmail =
    ensureString(resolvedSeedContext?.client?.primaryEmail || '', '').trim().toLowerCase() || null;
  const effectiveClientPhone = normalizedPhone || existingResolvedPhone || null;
  const effectivePrimaryEmail = primaryEmail || existingResolvedEmail || null;
  const profileWillBeCompleted = Boolean(name && effectiveClientPhone && effectivePrimaryEmail);

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
      const effectivePhone = normalizedPhone || normalizePhone(oldData.phone) || null;
      const effectiveEmail = primaryEmail || ensureString(oldData.primaryEmail || '', '').trim().toLowerCase() || null;
      const profileCompleted = Boolean(name && effectivePhone && effectiveEmail);
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
        id: randomLowercaseId(14),
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
          phone: effectivePhone,
          name,
          primaryEmail: effectiveEmail,
          notes: mergedNotes,
          credits,
          supportsUsed,
          freeFirstSupportUsed,
          deviceAnchor: resolvedDeviceAnchor || oldData.deviceAnchor || null,
          profileCompleted,
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
        const requestPatch = {
          clientRecordId: clientId,
          clientName: name,
          deviceAnchor: resolvedDeviceAnchor || null,
          requiresTechnicianRegistration: !profileCompleted,
          updatedAt: now,
        };
        if (effectivePhone) requestPatch.clientPhone = effectivePhone;
        tx.set(requestRef, requestPatch, { merge: true });
      }

      if (sessionRef && sessionData) {
        const sessionPatch = {
          clientRecordId: clientId,
          clientName: name,
          deviceAnchor: resolvedDeviceAnchor || null,
          requiresTechnicianRegistration: !profileCompleted,
          updatedAt: now,
        };
        if (effectivePhone) sessionPatch.clientPhone = effectivePhone;
        tx.set(sessionRef, sessionPatch, { merge: true });
      }

      if (supportSessionId && supportSessionsCollection) {
        const supportRef = supportSessionsCollection.doc(supportSessionId);
        const supportPatch = {
          clientId,
          clientName: name,
          deviceAnchor: resolvedDeviceAnchor || null,
          requiresTechnicianRegistration: !profileCompleted,
          isFreeFirstSupport: !freeFirstSupportUsed,
          creditsConsumed: !freeFirstSupportUsed ? 0 : 1,
          updatedAt: now,
        };
        if (effectivePhone) supportPatch.clientPhone = effectivePhone;
        tx.set(supportRef, supportPatch, { merge: true });
      }
    });
  } catch (error) {
    console.error('Failed to register client in context', error);
    const mappedError = mapFirestoreWriteError(error);
    return res
      .status(mappedError.status)
      .json({ error: mappedError.error, message: mappedError.message });
  }

  if ((linkedClientUid || resolvedDeviceAnchor) && linksCollection) {
    try {
      const linkPayload = {
        clientUid: linkedClientUid || null,
        clientId,
        phone: effectiveClientPhone,
        deviceAnchor: resolvedDeviceAnchor || null,
        supportSessionId: supportSessionId || null,
        linkSource: 'tech_registered',
        linkedByTechUid: techUid,
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

  let verificationTrigger = profileWillBeCompleted
    ? { status: 'ok', message: 'Verificação iniciada com sucesso.' }
    : {
        status: 'ok',
        message: 'Identificação parcial salva. Complete telefone e e-mail para concluir o cadastro.',
      };
  try {
    if (!profileWillBeCompleted) {
      const context = await buildClientContextPayload({
        sessionId,
        requestId,
        clientRecordId: clientId,
        clientUid: linkedClientUid,
        phone: effectiveClientPhone,
        deviceAnchor: resolvedDeviceAnchor,
      });
      return res.json({
        ok: true,
        ...context,
        verificationTrigger,
      });
    }
    if (verificationsCollection) {
      const verificationRef = verificationsCollection.doc(clientId);
      const existingVerificationSnap = await verificationRef.get();
      const existingVerification = existingVerificationSnap.exists ? existingVerificationSnap.data() || {} : {};
      const existingStatus = ensureString(existingVerification.status || '', '').trim().toLowerCase();
      const verificationPayload = {
        clientId,
        primaryPhone: effectiveClientPhone,
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
        phone: effectiveClientPhone,
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
    phone: effectiveClientPhone,
    sessionId: sessionId || null,
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
      phone: effectiveClientPhone,
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

app.post('/api/client-context/verification/pnv-result', requireAuth(['user']), async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const clientsCollection = getClientsCollection();
  const verificationsCollection = getClientVerificationsCollection();
  const pnvRequestsCollection = getPnvRequestsCollection();
  const sessionsCollection = getSessionsCollection();
  const supportSessionsCollection = getSupportSessionsCollection();
  if (!clientsCollection || !verificationsCollection || !pnvRequestsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const authClientUid = ensureString(req.user?.uid || '', '').trim();
  const requestedClientId = ensureString(req.body?.clientId || '', '').trim().slice(0, 128) || null;
  const requestedPhone = normalizePhone(req.body?.phone || '');
  const pnvToken = ensureLongString(req.body?.token || req.body?.pnvToken || '', '', 8192).trim();
  const deviceAnchor = normalizeDeviceAnchor(req.body?.deviceAnchor || req.body?.device?.anchor || '') || null;
  const sessionId = normalizeSessionId(req.body?.sessionId || '');
  const supportSessionId =
    ensureString(req.body?.supportSessionId || req.body?.localSupportSessionId || '', '').trim().slice(0, 128) || null;

  if (!authClientUid || !pnvToken) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  const verified = await verifyFirebasePnvToken({
    token: pnvToken,
    expectedPhone: requestedPhone || null,
  });
  if (!verified.ok) {
    const status = verified.error === 'pnv_phone_mismatch' ? 409 : 400;
    return res.status(status).json({ error: verified.error || 'invalid_pnv_token' });
  }

  const verifiedPhone = verified.phone;
  const resolvedClient = await ensureClientIdentityFromPhone({
    normalizedPhone: verifiedPhone,
    clientUid: authClientUid,
    deviceAnchor,
    source: 'android_pnv_sdk',
    hasVerifiedIdentityProof: true,
    identityAssurance: 'firebase_pnv',
  });
  const clientId = resolvedClient?.client?.id || requestedClientId || clientDocIdFromPhone(verifiedPhone);
  if (!clientId) {
    return res.status(400).json({ error: 'invalid_phone' });
  }

  const now = Date.now();
  try {
    await verificationsCollection.doc(clientId).set(
      {
        clientId,
        primaryPhone: verifiedPhone,
        verifiedPhone,
        status: 'verified',
        verificationMethod: 'firebase_pnv',
        mismatchReason: null,
        lastVerificationAt: now,
        updatedAt: now,
      },
      { merge: true }
    );

    await pnvRequestsCollection.add({
      clientId,
      clientUid: authClientUid,
      deviceAnchor,
      phone: verifiedPhone,
      supportSessionId,
      sessionId: sessionId || null,
      manualFallback: false,
      status: 'processed',
      source: 'android_pnv_server_verified',
      tokenPresent: true,
      processedAt: now,
      createdAt: now,
      updatedAt: now,
    });

    const sessionUpdate = {
      clientId,
      clientRecordId: clientId,
      clientPhone: verifiedPhone,
      phoneVerified: true,
      phoneVerificationMethod: 'firebase_pnv',
      phoneVerifiedAt: now,
      updatedAt: now,
    };
    const writes = [];
    if (sessionId && sessionsCollection) {
      writes.push(sessionsCollection.doc(sessionId).set(sessionUpdate, { merge: true }));
    }
    if (supportSessionId && supportSessionsCollection) {
      writes.push(supportSessionsCollection.doc(supportSessionId).set(sessionUpdate, { merge: true }));
    }
    if (writes.length) await Promise.all(writes);
  } catch (error) {
    console.error('Failed to persist PNV verification result', error);
    return res.status(500).json({ error: 'server_error' });
  }

  const eventPayload = {
    clientId,
    clientUid: authClientUid,
    deviceAnchor,
    phone: verifiedPhone,
    status: 'verified',
    verificationMethod: 'firebase_pnv',
    supportSessionId,
    sessionId: sessionId || null,
    updatedAt: now,
  };
  if (sessionId) io.to(`s:${sessionId}`).emit('client:verification:updated', eventPayload);
  if (supportSessionId) io.to(`s:${supportSessionId}`).emit('client:verification:updated', eventPayload);

  try {
    const context = await buildClientContextPayload({
      sessionId,
      clientRecordId: clientId,
      clientUid: authClientUid,
      phone: verifiedPhone,
      deviceAnchor,
    });
    return res.json({
      ok: true,
      ...context,
      verification: context.verification || {
        clientId,
        primaryPhone: verifiedPhone,
        verifiedPhone,
        status: 'verified',
      },
    });
  } catch (error) {
    console.error('Failed to refresh context after PNV verification', error);
    return res.json({
      ok: true,
      clientId,
      phone: verifiedPhone,
      verification: {
        clientId,
        primaryPhone: verifiedPhone,
        verifiedPhone,
        status: 'verified',
      },
    });
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
  const idempotencyKey = ensureString(req.body?.idempotencyKey || req.get('Idempotency-Key') || '', '')
    .trim()
    .replace(/[^a-zA-Z0-9._:-]/g, '')
    .slice(0, 120);
  if (!clientId || delta === 0) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  let creditChange = null;
  let idempotentReplay = false;
  try {
    await db.runTransaction(async (tx) => {
      const clientRef = clientsCollection.doc(clientId);
      const idempotencyRef = idempotencyKey
        ? db.collection('credit_adjustment_requests').doc(`${clientId}_${idempotencyKey}`)
        : null;
      if (idempotencyRef) {
        const idempotencySnap = await tx.get(idempotencyRef);
        if (idempotencySnap.exists) {
          const existing = idempotencySnap.data() || {};
          creditChange = existing.creditChange || null;
          idempotentReplay = true;
          return;
        }
      }
      const snap = await tx.get(clientRef);
      if (!snap.exists) throw new Error('client_not_found');
      const data = snap.data() || {};
      const previousCredits = Math.max(0, ensureInteger(data.credits, 0));
      const credits = Math.max(0, previousCredits + delta);
      const appliedDelta = credits - previousCredits;
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
      creditChange = {
        clientId,
        previousCredits,
        credits,
        requestedDelta: delta,
        appliedDelta,
        clientName: ensureString(data.name || '', '').trim() || 'Cliente',
        clientPhone: normalizePhone(data.phone || '') || null,
        clientEmail: normalizeEmail(data.primaryEmail || data.email || ''),
      };
      if (idempotencyRef) {
        tx.set(idempotencyRef, {
          clientId,
          idempotencyKey,
          creditChange,
          requestedBy: ensureString(req.user?.uid || '', '').trim() || null,
          createdAt: Date.now(),
        });
      }
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
    let creditDispatch = null;
    let creditNotification = null;
    if (creditChange && creditChange.appliedDelta > 0 && !idempotentReplay) {
      creditDispatch = await dispatchClientCreditAddedNotification({
        client: context?.client || {},
        creditChange,
      });
      creditNotification = await createClientNotification({
        client: context?.client || { id: clientId },
        clientUid: context?.anchor?.clientUid || null,
        title: 'Credito adicionado',
        body: `Foram adicionados ${creditChange.appliedDelta} credito${creditChange.appliedDelta === 1 ? '' : 's'} a sua conta.`,
        type: 'CREDIT_ADDED',
        iconType: 'gift',
        priority: 'normal',
        actionLabel: 'Ver creditos',
        actionType: 'OPEN_CREDITS',
        actionPayload: {
          creditAmount: creditChange.appliedDelta,
          balance: creditChange.credits,
        },
        delivery: { inApp: true, push: true },
        source: 'credit_adjustment',
        createdBy: {
          uid: ensureString(req.user?.uid || '', '').trim() || null,
          name: ensureString(req.techAccess?.techDoc?.name || req.user?.name || 'Tecnico', 'Tecnico').trim() || 'Tecnico',
        },
        dedupeKey:
          ensureString(req.body?.idempotencyKey || req.get('Idempotency-Key') || '', '').trim()
            ? `credit:${ensureString(req.body?.idempotencyKey || req.get('Idempotency-Key') || '', '').trim()}`
            : `credit:${clientId}:${Date.now()}:${randomLowercaseId(8)}`,
      });
      await createAdminNotification({
        title: 'Credito enviado ao cliente',
        body: `${creditChange.appliedDelta} credito${creditChange.appliedDelta === 1 ? '' : 's'} adicionado${creditChange.appliedDelta === 1 ? '' : 's'} para ${creditChange.clientName || 'cliente'}.`,
        type: 'CREDIT_ADDED',
        iconType: 'gift',
        actorUid: ensureString(req.user?.uid || '', '').trim() || null,
        metadata: {
          clientId,
          notificationId: creditNotification?.notification?.id || null,
          appliedDelta: creditChange.appliedDelta,
        },
      });
      await recordNotificationEvent('CREDIT_AND_NOTIFICATION_SENT', {
        notificationId: creditNotification?.notification?.id || null,
        clientId,
        actorUid: ensureString(req.user?.uid || '', '').trim() || null,
        actorName: ensureString(req.techAccess?.techDoc?.name || req.user?.name || '', '').trim() || null,
        status: creditNotification?.ok ? 'sent' : 'notification_failed',
      });
    }
    return res.json({ ok: true, idempotentReplay, ...context, creditDispatch, creditNotification });
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

app.post('/api/client-context/verification/send-code', requireAuth(['tech']), requireTechAccess, async (req, res) => {
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
  const requestedPhone = normalizePhone(req.body?.phone || '');
  const requestedEmail = normalizeEmail(req.body?.email || '');
  if (!clientId) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  const now = Date.now();
  const linkInfo = await resolveClientLinkInfoByClientId(clientId);
  const clientSnap = await clientsCollection.doc(clientId).get();
  if (!clientSnap.exists) {
    return res.status(404).json({ error: 'client_not_found' });
  }
  const clientData = clientSnap.data() || {};
  const targetPhone = requestedPhone || normalizePhone(clientData.phone || '') || linkInfo.phone || null;
  const targetEmail = requestedEmail || normalizeEmail(clientData.primaryEmail || clientData.email || '') || null;
  if (!targetPhone && !targetEmail) {
    return res.status(400).json({ error: 'missing_verification_channel' });
  }

  const code = String(crypto.randomInt(100000, 1000000));
  const salt = crypto.randomBytes(16).toString('hex');
  const expiresAt = now + CLIENT_MANUAL_VERIFICATION_CODE_TTL_MS;
  const codeHash = hashClientManualVerificationCode({ clientId, phone: targetPhone || '', code, salt });

  const dispatch = await dispatchClientManualVerificationCode({
    client: { id: clientId, ...clientData, phone: targetPhone, primaryEmail: targetEmail },
    code,
  });

  try {
    await verificationsCollection.doc(clientId).set(
      {
        clientId,
        primaryPhone: targetPhone || null,
        verifiedPhone: null,
        status: 'manual_required',
        mismatchReason: 'manual_code_sent',
        manualCode: {
          codeHash,
          salt,
          phone: targetPhone || null,
          email: targetEmail || null,
          expiresAt,
          createdAt: now,
          attempts: 0,
          maxAttempts: CLIENT_MANUAL_VERIFICATION_MAX_ATTEMPTS,
        },
        lastCodeSentAt: now,
        updatedAt: now,
      },
      { merge: true }
    );

    await pnvRequestsCollection.add({
      clientUid: linkInfo.clientUid || null,
      clientId,
      phone: targetPhone || null,
      email: targetEmail || null,
      status: 'manual_code_sent',
      manualFallback: true,
      reason: 'manual_code_sent',
      source: 'tech_panel',
      createdAt: now,
      updatedAt: now,
      expiresAt,
    });
  } catch (error) {
    console.error('Failed to persist manual verification code', error);
    return res.status(500).json({ error: 'server_error' });
  }

  try {
    const context = await buildClientContextPayload({
      clientRecordId: clientId,
      clientUid: linkInfo.clientUid,
      phone: targetPhone,
    });
    return res.json({ ok: true, ...context, manualVerificationDispatch: dispatch, expiresAt });
  } catch (error) {
    console.error('Failed to load context after manual verification code send', error);
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
  const verificationCode = ensureString(req.body?.verificationCode || req.body?.code || '', '')
    .replace(/\D/g, '')
    .slice(0, 10);
  if (!clientId || !verifiedPhone || (!verificationIdToken && !verificationCode)) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  let decodedVerificationToken = null;
  let verificationMethod = 'sms';
  if (verificationIdToken) {
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
  } else {
    verificationMethod = 'manual_code';
    const verificationRef = verificationsCollection.doc(clientId);
    const verificationSnap = await verificationRef.get();
    const manualCode = verificationSnap.exists ? verificationSnap.data()?.manualCode || null : null;
    const validation = validateClientManualVerificationCode({
      clientId,
      phone: verifiedPhone,
      code: verificationCode,
      manualCode,
    });
    if (!validation.ok) {
      const attempts = Math.max(0, ensureInteger(manualCode?.attempts, 0)) + 1;
      try {
        await verificationRef.set(
          {
            manualCode: {
              ...(manualCode || {}),
              attempts,
              lastAttemptAt: Date.now(),
            },
            updatedAt: Date.now(),
          },
          { merge: true }
        );
      } catch (_error) {
        // A falha de auditoria nao deve esconder o erro real de validacao do codigo.
      }
      return res.status(validation.status).json({ error: validation.error });
    }
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
        verificationMethod,
        manualCode: null,
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
      reason: verificationMethod === 'manual_code' ? 'manual_code_verified_by_technician' : 'sms_verified_by_technician',
      source: 'tech_panel',
      createdAt: now,
      updatedAt: now,
      processedAt: now,
      verificationUid: ensureString(decodedVerificationToken?.uid || '', '').trim() || null,
      verificationMethod,
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
  if (!/^[A-Za-z0-9_-]{1,64}$/.test(sessionId)) {
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
    const existingActiveSession = await findActiveRealtimeSessionForTech({
      sessionsCollection,
      techUid: uid,
    });
    if (
      existingActiveSession &&
      normalizeSessionId(existingActiveSession.id || '') !== sessionId
    ) {
      return res.status(409).json({
        error: 'active_session_exists',
        sessionId: existingActiveSession.id,
      });
    }

    const techName =
      ensureString(
        techData.name || techData.displayName || req.user?.name || 'Técnico',
        'Técnico'
      ) || 'Técnico';
    const claim = await claimSupportSessionTransaction({
      sessionId,
      tech: {
        uid,
        name: techName,
        email:
          ensureString(techData.email || req.user?.email || '', '') || null,
        photoURL:
          ensureString(
            techData.photoURL ||
              techData.photoUrl ||
              req.user?.picture ||
              '',
            ''
          ) || null,
      },
    });

    return res.json({
      ok: true,
      sessionId,
      alreadyClaimed: claim.action === 'reuse',
    });
  } catch (err) {
    if (err instanceof SupportQueuePolicyError) {
      return res.status(err.status || 409).json({
        error: err.code,
        ...(err.details || {}),
      });
    }
    console.error('Failed to claim session', err);
    return res.status(500).json({ error: 'server_error' });
  }
});

const ACTIVE_REALTIME_SESSION_STATES = new Set(['active', 'accepted', 'in_progress']);

const isRealtimeSessionActive = (session = {}) =>
  ACTIVE_REALTIME_SESSION_STATES.has(
    ensureString(session.status || '', '').trim().toLowerCase()
  );

const findActiveRealtimeSessionForTech = async ({
  sessionsCollection,
  techUid,
} = {}) => {
  const normalizedTechUid = ensureString(techUid || '', '').trim();
  if (!sessionsCollection || !normalizedTechUid) return null;

  const queries = [];
  for (const status of ACTIVE_REALTIME_SESSION_STATES) {
    queries.push(
      sessionsCollection
        .where('techUid', '==', normalizedTechUid)
        .where('status', '==', status)
        .limit(1)
        .get()
    );
    queries.push(
      sessionsCollection
        .where('tech.techUid', '==', normalizedTechUid)
        .where('status', '==', status)
        .limit(1)
        .get()
    );
  }

  const snapshots = await Promise.all(queries);
  const activeDoc = snapshots
    .flatMap((snapshot) => snapshot.docs || [])
    .find((doc) => isRealtimeSessionActive(doc.data() || {}));
  if (!activeDoc) return null;

  const data = activeDoc.data() || {};
  return {
    id:
      normalizeSessionId(data.sessionId || '') ||
      normalizeSessionId(activeDoc.id || '') ||
      null,
    data,
  };
};

const readTechSupportLockInTransaction = async ({
  tx,
  sessionsCollection,
  techLocksCollection,
  techUid,
  requestedSessionId,
} = {}) => {
  const normalizedTechUid = ensureString(techUid || '', '').trim();
  const normalizedRequestedSessionId = normalizeSessionId(requestedSessionId);
  const lockId = techSupportLockDocIdFromUid(normalizedTechUid);
  if (
    !tx ||
    !sessionsCollection ||
    !techLocksCollection ||
    !normalizedTechUid ||
    !normalizedRequestedSessionId ||
    !lockId
  ) {
    throw new SupportQueuePolicyError('invalid_payload', 400);
  }

  const lockRef = techLocksCollection.doc(lockId);
  const lockSnap = await tx.get(lockRef);
  const lock = lockSnap.exists ? lockSnap.data() || {} : null;
  const lockedSessionId = normalizeSessionId(
    lock?.realtimeSessionId || lock?.sessionId || ''
  );

  if (lockedSessionId && lockedSessionId !== normalizedRequestedSessionId) {
    const lockedSessionSnap = await tx.get(sessionsCollection.doc(lockedSessionId));
    decideTechSupportAvailability({
      requestedSessionId: normalizedRequestedSessionId,
      lockedSessionId,
      lockedSession: lockedSessionSnap.exists
        ? lockedSessionSnap.data() || {}
        : null,
    });
  }

  return {
    lockRef,
    createdAt: lock?.createdAt || null,
  };
};

const writeTechSupportLockInTransaction = ({
  tx,
  lockRef,
  techUid,
  requestId,
  sessionId,
  createdAt,
  now,
  action,
} = {}) => {
  tx.set(
    lockRef,
    {
      techUid,
      requestId,
      realtimeSessionId: sessionId,
      status: 'active',
      action,
      createdAt: createdAt || now,
      updatedAt: now,
    },
    { merge: true }
  );
};

const claimSupportSessionTransaction = async ({
  sessionId,
  tech,
  now = Date.now(),
} = {}) => {
  const sessionsCollection = getSessionsCollection();
  const techLocksCollection = getSupportTechLocksCollection();
  const normalizedSessionId = normalizeSessionId(sessionId);
  const normalizedTechUid = ensureString(tech?.uid || '', '').trim();
  if (
    !db ||
    !sessionsCollection ||
    !techLocksCollection
  ) {
    throw new SupportQueuePolicyError('firestore_unavailable', 503);
  }
  if (
    !/^[A-Za-z0-9_-]{1,64}$/.test(normalizedSessionId) ||
    !normalizedTechUid
  ) {
    throw new SupportQueuePolicyError('invalid_payload', 400);
  }

  const sessionRef = sessionsCollection.doc(normalizedSessionId);
  return db.runTransaction(async (tx) => {
    const sessionSnap = await tx.get(sessionRef);
    if (!sessionSnap.exists) {
      throw new SupportQueuePolicyError('session_not_found', 404);
    }

    const sessionData = sessionSnap.data() || {};
    const claim = decideTechSessionClaim({
      session: sessionData,
      techUid: normalizedTechUid,
    });
    const techSupportLock = await readTechSupportLockInTransaction({
      tx,
      sessionsCollection,
      techLocksCollection,
      techUid: normalizedTechUid,
      requestedSessionId: normalizedSessionId,
    });
    const techName =
      ensureString(tech?.name || 'Técnico', 'Técnico') || 'Técnico';
    const techEmail = ensureString(tech?.email || '', '') || null;
    const techPhotoURL = ensureString(tech?.photoURL || '', '') || null;

    tx.update(sessionRef, {
      tech: {
        techUid: normalizedTechUid,
        techId: normalizedTechUid,
        uid: normalizedTechUid,
        id: normalizedTechUid,
        name: techName,
        techName,
        email: techEmail,
        techPhotoURL,
        photoURL: techPhotoURL,
      },
      techUid: normalizedTechUid,
      techId: normalizedTechUid,
      techName,
      techEmail,
      techPhotoURL,
      claimedAt: sessionData.claimedAt || now,
      updatedAt: now,
      status: claim.status,
    });
    writeTechSupportLockInTransaction({
      tx,
      lockRef: techSupportLock.lockRef,
      techUid: normalizedTechUid,
      requestId: ensureString(sessionData.requestId || '', '').trim() || null,
      sessionId: normalizedSessionId,
      createdAt: techSupportLock.createdAt,
      now,
      action: 'claimed',
    });

    return {
      ...claim,
      sessionId: normalizedSessionId,
    };
  });
};

const acceptSupportQueueRequestTransaction = async ({
  requestId,
  sessionId,
  tech,
  fallbackClientRecordId = null,
  now = Date.now(),
} = {}) => {
  const requestsCollection = getRequestsCollection();
  const sessionsCollection = getSessionsCollection();
  const clientsCollection = getClientsCollection();
  const profilesCollection = getClientProfilesCollection();
  const supportSessionsCollection = getSupportSessionsCollection();
  const locksCollection = getSupportQueueLocksCollection();
  const anchorsCollection = getSupportQueueAnchorsCollection();
  const outcomesCollection = getSupportQueueOutcomesCollection();
  const techLocksCollection = getSupportTechLocksCollection();
  if (
    !db ||
    !requestsCollection ||
    !sessionsCollection ||
    !clientsCollection ||
    !profilesCollection ||
    !supportSessionsCollection ||
    !locksCollection ||
    !anchorsCollection ||
    !outcomesCollection ||
    !techLocksCollection
  ) {
    throw new SupportQueuePolicyError('firestore_unavailable', 503);
  }

  const normalizedRequestId = ensureString(requestId || '', '').trim().slice(0, 64).toUpperCase();
  const normalizedSessionId = normalizeSessionId(sessionId);
  if (!normalizedRequestId || !normalizedSessionId) {
    throw new SupportQueuePolicyError('invalid_payload', 400);
  }

  const requestRef = requestsCollection.doc(normalizedRequestId);
  const realtimeSessionRef = sessionsCollection.doc(normalizedSessionId);
  const outcomeRef = outcomesCollection.doc(normalizedRequestId);

  return db.runTransaction(async (tx) => {
    const requestSnap = await tx.get(requestRef);
    const outcomeSnap = await tx.get(outcomeRef);
    if (!requestSnap.exists) {
      const outcome = outcomeSnap.exists ? outcomeSnap.data() || {} : null;
      if (
        outcome &&
        ensureString(outcome.status || '', '').trim().toLowerCase() === 'accepted'
      ) {
        return {
          alreadyAccepted: true,
          sessionId:
            normalizeSessionId(outcome.realtimeSessionId || outcome.sessionId || '') ||
            null,
          requestId: normalizedRequestId,
        };
      }
      throw new SupportQueuePolicyError('request_not_found_or_already_taken', 404);
    }

    const request = { requestId: requestSnap.id, ...(requestSnap.data() || {}) };
    if (ensureString(request.state || 'queued', '').trim().toLowerCase() !== 'queued') {
      throw new SupportQueuePolicyError('request_not_found_or_already_taken', 404);
    }

    const clientUid = ensureString(request.clientUid || '', '').trim();
    const localSupportSessionId = ensureString(
      request.localSupportSessionId || request.supportProfile?.localSupportSessionId || '',
      ''
    )
      .trim()
      .slice(0, 128);
    const clientRecordId =
      ensureString(request.clientRecordId || fallbackClientRecordId || '', '').trim().slice(0, 128);
    if (!clientRecordId) {
      throw new SupportQueuePolicyError('client_not_registered', 409);
    }

    const clientRef = clientsCollection.doc(clientRecordId);
    const profileRef = profilesCollection.doc(clientRecordId);
    const supportSessionRef = localSupportSessionId
      ? supportSessionsCollection.doc(localSupportSessionId)
      : null;
    const lockId = clientUid ? queueLockDocIdFromUid(clientUid) : null;
    const anchorId =
      clientUid && localSupportSessionId
        ? queueAnchorDocId(clientUid, localSupportSessionId)
        : null;
    const lockRef = lockId ? locksCollection.doc(lockId) : null;
    const anchorRef = anchorId ? anchorsCollection.doc(anchorId) : null;

    const realtimeSessionSnap = await tx.get(realtimeSessionRef);
    const clientSnap = await tx.get(clientRef);
    const profileSnap = await tx.get(profileRef);
    const supportSessionSnap = supportSessionRef ? await tx.get(supportSessionRef) : null;
    const lockSnap = lockRef ? await tx.get(lockRef) : null;
    const anchorSnap = anchorRef ? await tx.get(anchorRef) : null;
    const techSupportLock = await readTechSupportLockInTransaction({
      tx,
      sessionsCollection,
      techLocksCollection,
      techUid: tech.uid,
      requestedSessionId: normalizedSessionId,
    });
    if (realtimeSessionSnap.exists) {
      throw new SupportQueuePolicyError('session_id_collision', 409);
    }
    if (!clientSnap.exists) {
      throw new SupportQueuePolicyError('client_not_registered', 409);
    }

    const clientData = clientSnap.data() || {};
    const profileData = profileSnap.exists ? profileSnap.data() || {} : {};
    const supportSessionData = supportSessionSnap?.exists
      ? supportSessionSnap.data() || {}
      : null;
    if (localSupportSessionId) {
      if (!supportSessionData) {
        throw new SupportQueuePolicyError('local_support_session_not_found', 409, {
          localSupportSessionId,
        });
      }
      const supportOwnerUid = ensureString(supportSessionData.clientUid || '', '').trim();
      if (!clientUid || supportOwnerUid !== clientUid) {
        throw new SupportQueuePolicyError('forbidden', 403, {
          localSupportSessionId,
        });
      }
      const queuedRequestId = ensureString(
        supportSessionData.queueRequestId || '',
        ''
      )
        .trim()
        .toUpperCase();
      if (queuedRequestId && queuedRequestId !== normalizedRequestId) {
        throw new SupportQueuePolicyError('request_mismatch', 409, {
          localSupportSessionId,
        });
      }
    }

    const billing = evaluateAuthoritativeBilling({
      client: clientData,
      requestId: normalizedRequestId,
    });
    const billingUpdates = buildClientBillingUpdates({
      client: clientData,
      profile: profileData,
      billing,
      now,
      deriveStatus: deriveClientStatus,
    });
    const supportProfile = sanitizeSupportProfile(
      request.supportProfile || request.extra?.supportProfile || {}
    );
    const profileCompleted = isClientProfileCompleted({
      id: clientRecordId,
      ...clientData,
    });
    const requiresTechnicianRegistration =
      ensureBoolean(request.requiresTechnicianRegistration, false) ||
      !profileCompleted;
    const resolvedSupportProfile = {
      ...supportProfile,
      isNewClient: requiresTechnicianRegistration,
      isFreeFirstSupport: billing.isFreeFirstSupport,
      creditsToConsume: billing.creditsConsumed,
    };
    const baseExtra =
      typeof request.extra === 'object' && request.extra !== null
        ? { ...request.extra }
        : {};
    const baseTelemetry = normalizeTelemetryData(
      typeof baseExtra.telemetry === 'object' && baseExtra.telemetry !== null
        ? { ...baseExtra.telemetry }
        : {}
    );
    const requestDeviceAnchor =
      normalizeDeviceAnchor(
        request.deviceAnchor ||
          request.device?.anchor ||
          request.extra?.device?.anchor ||
          supportProfile.deviceAnchor ||
          ''
      ) || null;
    const clientPhone =
      normalizePhone(clientData.phone || '') ||
      normalizePhone(request.clientPhone || '') ||
      null;
    const sessionData = {
      sessionId: normalizedSessionId,
      requestId: normalizedRequestId,
      clientId: request.clientId || null,
      clientSocketId: request.clientSocketId || request.clientId || null,
      clientRecordId,
      clientUid: clientUid || null,
      deviceAnchor: requestDeviceAnchor,
      clientPhone,
      techName: tech.name,
      techId: tech.uid,
      techUid: tech.uid,
      techEmail: tech.email,
      techPhotoURL: tech.photoURL,
      tech: {
        techUid: tech.uid,
        techId: tech.uid,
        uid: tech.uid,
        id: tech.uid,
        name: tech.name,
        techName: tech.name,
        email: tech.email,
        techPhotoURL: tech.photoURL,
        photoURL: tech.photoURL,
      },
      clientName:
        ensureString(clientData.name || request.clientName || 'Cliente', 'Cliente') ||
        'Cliente',
      brand: request.brand || null,
      model: request.model || null,
      osVersion: request.osVersion || null,
      plan: request.plan || null,
      issue: request.issue || null,
      supportSessionId: localSupportSessionId || null,
      supportProfile: resolvedSupportProfile,
      profileCompleted,
      requiresTechnicianRegistration,
      isFreeFirstSupport: billing.isFreeFirstSupport,
      creditsConsumed: billing.creditsConsumed,
      requestedAt: request.createdAt || now,
      acceptedAt: now,
      waitTimeMs: Math.max(0, now - Number(request.createdAt || now)),
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
              ...(baseExtra.device && typeof baseExtra.device === 'object'
                ? baseExtra.device
                : {}),
              anchor: requestDeviceAnchor,
            }
          : baseExtra.device,
      },
    };

    tx.set(clientRef, billingUpdates.client, { merge: true });
    tx.set(
      profileRef,
      {
        clientId: clientRecordId,
        ...billingUpdates.profile,
      },
      { merge: true }
    );
    tx.set(realtimeSessionRef, sessionData);
    tx.delete(requestRef);
    writeTechSupportLockInTransaction({
      tx,
      lockRef: techSupportLock.lockRef,
      techUid: tech.uid,
      requestId: normalizedRequestId,
      sessionId: normalizedSessionId,
      createdAt: techSupportLock.createdAt,
      now,
      action: 'accepted',
    });

    if (
      lockRef &&
      lockSnap?.exists &&
      ensureString(lockSnap.data()?.requestId || '', '').trim().toUpperCase() ===
        normalizedRequestId
    ) {
      tx.delete(lockRef);
    }
    if (anchorRef) {
      tx.set(
        anchorRef,
        {
          clientUid,
          requestId: normalizedRequestId,
          localSupportSessionId,
          status: 'accepted',
          realtimeSessionId: normalizedSessionId,
          acceptedAt: now,
          updatedAt: now,
        },
        { merge: true }
      );
    }
    if (supportSessionRef && supportSessionSnap?.exists) {
      tx.set(
        supportSessionRef,
        {
          queueRequestId: normalizedRequestId,
          queueStatus: 'accepted',
          status: 'in_progress',
          realtimeSessionId: normalizedSessionId,
          sessionId: normalizedSessionId,
          clientId: clientRecordId,
          clientPhone,
          techId: tech.uid,
          techName: tech.name,
          acceptedAt: now,
          queueAcceptedAt: now,
          billingAppliedAt: now,
          billingRequestId: normalizedRequestId,
          billingSource: 'server_accept_v1',
          isFreeFirstSupport: billing.isFreeFirstSupport,
          creditsConsumed: billing.creditsConsumed,
          updatedAt: now,
        },
        { merge: true }
      );
    }
    tx.set(
      outcomeRef,
      {
        requestId: normalizedRequestId,
        clientUid: clientUid || null,
        localSupportSessionId: localSupportSessionId || null,
        status: 'accepted',
        realtimeSessionId: normalizedSessionId,
        techUid: tech.uid,
        acceptedAt: now,
        updatedAt: now,
      },
      { merge: true }
    );

    return {
      alreadyAccepted: false,
      requestId: normalizedRequestId,
      sessionId: normalizedSessionId,
      requestData: request,
      sessionData,
      billing,
    };
  });
};

app.post('/api/requests/:id/accept', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const id = ensureString(req.params.id || '', '').trim().slice(0, 64).toUpperCase();
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

    const existingActiveSession = await findActiveRealtimeSessionForTech({
      sessionsCollection,
      techUid: uid,
    });
    if (existingActiveSession) {
      return res.status(409).json({
        error: 'active_session_exists',
        sessionId: existingActiveSession.id,
      });
    }

    const initialSessionId = generateSessionId();
    const normalizedTechName =
      ensureString(
        techData.name || techData.displayName || req.user?.name || req.body?.techName || 'Técnico',
        'Técnico'
      ) ||
      'Técnico';
    const normalizedTechUid = uid;
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
    if (!clientRecordId) {
      return res.status(409).json({ error: 'client_not_registered' });
    }

    let acceptance = null;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      try {
        acceptance = await acceptSupportQueueRequestTransaction({
          requestId: id,
          sessionId: attempt === 0 ? initialSessionId : generateSessionId(),
          tech: {
            uid: normalizedTechUid,
            name: normalizedTechName,
            email: normalizedTechEmail,
            photoURL: normalizedTechPhotoURL,
          },
          fallbackClientRecordId: clientRecordId,
        });
        break;
      } catch (error) {
        if (
          error instanceof SupportQueuePolicyError &&
          error.code === 'session_id_collision' &&
          attempt < 2
        ) {
          continue;
        }
        throw error;
      }
    }
    if (!acceptance) {
      throw new SupportQueuePolicyError('session_id_collision', 409);
    }
    if (acceptance.alreadyAccepted) {
      return res.status(409).json({
        error: 'request_not_found_or_already_taken',
        requestId: acceptance.requestId,
        sessionId: acceptance.sessionId || null,
      });
    }

    const acceptedSessionId = acceptance.sessionId;
    const acceptedRequest = acceptance.requestData || request;
    try {
      await persistQueueNotification({
        requestId: id,
        requestData: acceptedRequest,
        state: 'accepted',
        sessionId: acceptedSessionId,
        techUid: normalizedTechUid,
        techName: normalizedTechName,
      });
    } catch (error) {
      console.error('Failed to persist post-commit acceptance notification', error);
    }

    const targetSocketId = acceptedRequest.clientSocketId || acceptedRequest.clientId;
    if (targetSocketId) {
      try {
        io.to(targetSocketId).emit('support:accepted', {
          sessionId: acceptedSessionId,
          techName: normalizedTechName,
        });
      } catch (err) {
        console.error('Failed to emit acceptance to client', err);
      }
    }

    emitQueueUpdated({
      requestId: id,
      state: 'accepted',
      sessionId: acceptedSessionId,
      techName: normalizedTechName,
      clientUid: acceptedRequest.clientUid || null,
      targetSocketId,
      notifyClient: true,
    });
    try {
      await emitSessionUpdated(acceptedSessionId);
    } catch (error) {
      console.error('Failed to emit post-commit session update', error);
    }

    return res.json({
      sessionId: acceptedSessionId,
      requestId: acceptance.requestId,
      billingApplied: true,
      isFreeFirstSupport: acceptance.billing.isFreeFirstSupport,
      creditsConsumed: acceptance.billing.creditsConsumed,
    });
  } catch (err) {
    console.error('Failed to accept request', err);
    if (err instanceof SupportQueuePolicyError) {
      return res.status(err.status || 409).json({
        error: err.code,
        ...(err.details || {}),
      });
    }
    return res.status(500).json({ error: 'firestore_error' });
  }
});

const buildManualDeclineChatMessage = ({ sessionId, message, techName, ts }) => {
  const messageId = `${ts.toString(36)}-decline-refund`;
  return {
    id: messageId,
    sessionId,
    from: 'tech',
    author: techName || 'Suporte X',
    type: 'text',
    text: message,
    status: 'sent',
    ts,
  };
};

const emitManualDeclineChatMessage = (sessionId, chatMessage) => {
  io.to(`s:${sessionId}`).emit('session:chat:new', chatMessage);
};

const declineSupportQueueRequestTransaction = async ({
  requestId,
  sessionId,
  techUid,
  clientRecordId,
  localSupportSessionId,
  sessionData,
  chatMessage,
  creditsRefunded,
  now,
} = {}) => {
  const requestsCollection = getRequestsCollection();
  const sessionsCollection = getSessionsCollection();
  const clientsCollection = getClientsCollection();
  const supportSessionsCollection = getSupportSessionsCollection();
  const locksCollection = getSupportQueueLocksCollection();
  const anchorsCollection = getSupportQueueAnchorsCollection();
  const outcomesCollection = getSupportQueueOutcomesCollection();
  const techLocksCollection = getSupportTechLocksCollection();
  if (
    !db ||
    !requestsCollection ||
    !sessionsCollection ||
    !clientsCollection ||
    !supportSessionsCollection ||
    !locksCollection ||
    !anchorsCollection ||
    !outcomesCollection ||
    !techLocksCollection
  ) {
    throw new SupportQueuePolicyError('firestore_unavailable', 503);
  }

  const normalizedRequestId =
    ensureString(requestId || '', '').trim().slice(0, 64).toUpperCase();
  const normalizedSessionId = normalizeSessionId(sessionId);
  const normalizedTechUid = ensureString(techUid || '', '').trim();
  const normalizedClientRecordId =
    ensureString(clientRecordId || '', '').trim().slice(0, 128);
  const normalizedLocalId =
    ensureString(localSupportSessionId || '', '').trim().slice(0, 128);
  if (
    !normalizedRequestId ||
    !normalizedSessionId ||
    !normalizedTechUid ||
    !normalizedClientRecordId ||
    !sessionData ||
    !chatMessage
  ) {
    throw new SupportQueuePolicyError('invalid_payload', 400);
  }

  const requestRef = requestsCollection.doc(normalizedRequestId);
  const realtimeSessionRef = sessionsCollection.doc(normalizedSessionId);
  const clientRef = clientsCollection.doc(normalizedClientRecordId);
  const supportSessionRef = normalizedLocalId
    ? supportSessionsCollection.doc(normalizedLocalId)
    : null;
  const outcomeRef = outcomesCollection.doc(normalizedRequestId);

  return db.runTransaction(async (tx) => {
    const requestSnap = await tx.get(requestRef);
    const outcomeSnap = await tx.get(outcomeRef);
    if (!requestSnap.exists) {
      const outcome = outcomeSnap.exists ? outcomeSnap.data() || {} : null;
      throw new SupportQueuePolicyError('request_not_found_or_already_taken', 404, {
        sessionId:
          normalizeSessionId(
            outcome?.realtimeSessionId || outcome?.sessionId || ''
          ) || null,
      });
    }

    const request = { requestId: requestSnap.id, ...(requestSnap.data() || {}) };
    if (
      ensureString(request.state || 'queued', '').trim().toLowerCase() !==
      'queued'
    ) {
      throw new SupportQueuePolicyError('request_not_found_or_already_taken', 404);
    }

    const clientUid = ensureString(request.clientUid || '', '').trim();
    const effectiveLocalId =
      ensureString(
        request.localSupportSessionId ||
          request.supportProfile?.localSupportSessionId ||
          normalizedLocalId ||
          '',
        ''
      )
        .trim()
        .slice(0, 128) || null;
    if (normalizedLocalId && effectiveLocalId !== normalizedLocalId) {
      throw new SupportQueuePolicyError('request_mismatch', 409);
    }

    const effectiveSupportSessionRef = effectiveLocalId
      ? supportSessionsCollection.doc(effectiveLocalId)
      : supportSessionRef;
    const clientLockId = clientUid ? queueLockDocIdFromUid(clientUid) : null;
    const clientAnchorId =
      clientUid && effectiveLocalId
        ? queueAnchorDocId(clientUid, effectiveLocalId)
        : null;
    const clientLockRef = clientLockId
      ? locksCollection.doc(clientLockId)
      : null;
    const clientAnchorRef = clientAnchorId
      ? anchorsCollection.doc(clientAnchorId)
      : null;

    const realtimeSessionSnap = await tx.get(realtimeSessionRef);
    const clientSnap = await tx.get(clientRef);
    const supportSessionSnap = effectiveSupportSessionRef
      ? await tx.get(effectiveSupportSessionRef)
      : null;
    const clientLockSnap = clientLockRef
      ? await tx.get(clientLockRef)
      : null;
    const clientAnchorSnap = clientAnchorRef
      ? await tx.get(clientAnchorRef)
      : null;
    const techSupportLock = await readTechSupportLockInTransaction({
      tx,
      sessionsCollection,
      techLocksCollection,
      techUid: normalizedTechUid,
      requestedSessionId: normalizedSessionId,
    });

    if (realtimeSessionSnap.exists) {
      throw new SupportQueuePolicyError('session_id_collision', 409);
    }
    if (!clientSnap.exists) {
      throw new SupportQueuePolicyError('client_not_registered', 409);
    }
    if (effectiveSupportSessionRef) {
      if (!supportSessionSnap?.exists) {
        throw new SupportQueuePolicyError(
          'local_support_session_not_found',
          409
        );
      }
      const supportOwnerUid = ensureString(
        supportSessionSnap.data()?.clientUid || '',
        ''
      ).trim();
      if (!clientUid || supportOwnerUid !== clientUid) {
        throw new SupportQueuePolicyError('forbidden', 403);
      }
    }

    const persistedSessionData = {
      ...sessionData,
      lastMessageAt: chatMessage.ts,
      updatedAt: chatMessage.ts,
      extra: {
        ...(sessionData.extra &&
        typeof sessionData.extra === 'object' &&
        !Array.isArray(sessionData.extra)
          ? sessionData.extra
          : {}),
        lastMessageAt: chatMessage.ts,
      },
    };

    tx.set(realtimeSessionRef, persistedSessionData);
    tx.set(
      realtimeSessionRef.collection('messages').doc(chatMessage.id),
      chatMessage
    );
    tx.delete(requestRef);
    writeTechSupportLockInTransaction({
      tx,
      lockRef: techSupportLock.lockRef,
      techUid: normalizedTechUid,
      requestId: normalizedRequestId,
      sessionId: normalizedSessionId,
      createdAt: techSupportLock.createdAt,
      now,
      action: 'manual_decline_refund',
    });

    if (
      clientLockRef &&
      clientLockSnap?.exists &&
      ensureString(clientLockSnap.data()?.requestId || '', '')
        .trim()
        .toUpperCase() === normalizedRequestId
    ) {
      tx.delete(clientLockRef);
    }
    if (clientAnchorRef) {
      tx.set(
        clientAnchorRef,
        {
          clientUid,
          requestId: normalizedRequestId,
          localSupportSessionId: effectiveLocalId,
          status: 'accepted',
          realtimeSessionId: normalizedSessionId,
          acceptedAt: now,
          updatedAt: now,
          outcome: 'manual_decline_refund',
        },
        { merge: true }
      );
    }
    if (effectiveSupportSessionRef) {
      tx.set(
        effectiveSupportSessionRef,
        {
          queueRequestId: normalizedRequestId,
          queueStatus: 'accepted',
          sessionId: normalizedSessionId,
          realtimeSessionId: normalizedSessionId,
          status: 'in_progress',
          clientId: normalizedClientRecordId,
          clientPhone: sessionData.clientPhone || null,
          techId: normalizedTechUid,
          techName: sessionData.techName || null,
          acceptedAt: now,
          queueAcceptedAt: now,
          creditsConsumed: 0,
          creditsRefunded: Math.max(0, ensureInteger(creditsRefunded, 0)),
          isFreeFirstSupport: false,
          billingAppliedAt: now,
          manualDeclineRefund: true,
          updatedAt: now,
        },
        { merge: true }
      );
    }
    tx.set(
      outcomeRef,
      {
        requestId: normalizedRequestId,
        clientUid: clientUid || null,
        localSupportSessionId: effectiveLocalId,
        status: 'accepted',
        realtimeSessionId: normalizedSessionId,
        techUid: normalizedTechUid,
        reason: 'manual_decline_refund',
        acceptedAt: now,
        updatedAt: now,
      },
      { merge: true }
    );

    return {
      requestId: normalizedRequestId,
      sessionId: normalizedSessionId,
      requestData: request,
      sessionData: persistedSessionData,
      chatMessage,
    };
  });
};

const closeManualDeclinedSession = async ({ sessionId, reason = MANUAL_DECLINE_END_REASON } = {}) => {
  const normalizedSessionId = normalizeSessionId(sessionId);
  if (!normalizedSessionId) return { ok: false, error: 'invalid_session_id' };

  const snapshot = await getSessionSnapshot(normalizedSessionId);
  if (!snapshot) return { ok: false, error: 'session_not_found' };

  const session = snapshot.data() || {};
  const manualTechUid = getSessionTechUid(session);
  const manualClosureSummary = {
    problemSummary: 'Atendimento recusado manualmente por indisponibilidade.',
    solutionSummary: reason,
    internalNotes:
      'Atendimento encerrado por ação manual do técnico com crédito mantido/devolvido.',
  };
  if (ensureString(session.status || '', '').toLowerCase() === 'closed') {
    const supportFinalization = await finalizeSupportSessionFromRealtime({
      realtimeSessionId: normalizedSessionId,
      realtimeSession: {
        ...session,
        sessionId: normalizedSessionId,
      },
      actorUid: manualTechUid,
      actorRole: 'server',
      authorizedTech: true,
      summary: manualClosureSummary,
      now: Number(session.closedAt || Date.now()),
    });
    return { ok: true, alreadyClosed: true, supportFinalization };
  }

  const closedAt = Date.now();
  const room = `s:${normalizedSessionId}`;
  const eventId = `${closedAt.toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const commandEvent = {
    id: eventId,
    sessionId: normalizedSessionId,
    type: 'end',
    rawType: 'session_end',
    data: null,
    by: ensureString(session.techUid || session.tech?.techUid || '', '').trim() || 'tech',
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
    outcome: 'unavailable_refunded',
    symptom: 'Atendimento recusado manualmente por indisponibilidade.',
    solution: reason,
    notes: 'Atendimento encerrado por ação manual do técnico com crédito mantido/devolvido.',
    handleTimeMs: closedAt - (session.acceptedAt || session.createdAt || closedAt),
    updatedAt: closedAt,
    lastCommandAt: closedAt,
    telemetry: nextTelemetry,
    'extra.telemetry': nextTelemetry,
    'extra.lastCommand': commandEvent,
    'manualDeclineRefund.closedAt': closedAt,
  };

  await snapshot.ref.collection('events').doc(eventId).set(commandEvent);
  await snapshot.ref.set(updates, { merge: true });
  const supportFinalization = await finalizeSupportSessionFromRealtime({
    realtimeSessionId: normalizedSessionId,
    realtimeSession: {
      ...session,
      ...updates,
      sessionId: normalizedSessionId,
    },
    actorUid: manualTechUid,
    actorRole: 'server',
    authorizedTech: true,
    summary: manualClosureSummary,
    now: closedAt,
  });
  io.to(room).emit('session:command', {
    ...commandEvent,
    type: 'session_end',
    normalizedType: 'end',
  });
  io.to(room).emit('session:ended', { sessionId: normalizedSessionId, reason });
  io.socketsLeave(room);
  ['tech', 'client'].forEach((role) => {
    const roleRoom = sessionRoleSocketRoom(normalizedSessionId, role);
    if (roleRoom) io.socketsLeave(roleRoom);
  });
  await emitSessionUpdated(normalizedSessionId);
  return { ok: true, supportFinalization };
};

const manualDeclineCloseTimers = new Map();
let manualDeclineReconcileTimer = null;

const scheduleManualDeclineClose = ({ sessionId, delayMs }) => {
  const normalizedSessionId = normalizeSessionId(sessionId);
  if (!normalizedSessionId) return false;

  const effectiveDelayMs = Math.max(
    0,
    ensureInteger(delayMs, MANUAL_DECLINE_CLOSE_DELAY_MS)
  );
  const dueAt = Date.now() + effectiveDelayMs;
  const existing = manualDeclineCloseTimers.get(normalizedSessionId);
  if (existing?.dueAt <= dueAt) return true;
  if (existing?.timer) clearTimeout(existing.timer);

  const timer = setTimeout(() => {
    manualDeclineCloseTimers.delete(normalizedSessionId);
    closeManualDeclinedSession({ sessionId: normalizedSessionId }).catch((error) => {
      console.error('[manual-decline-refund] Failed to close session', normalizedSessionId, error);
    });
  }, effectiveDelayMs);
  manualDeclineCloseTimers.set(normalizedSessionId, { timer, dueAt });
  if (typeof timer.unref === 'function') timer.unref();
  return true;
};

const reconcilePendingManualDeclines = async () => {
  const sessionsCollection = getSessionsCollection();
  if (!sessionsCollection) return { scheduled: 0, unavailable: true };

  try {
    const snapshot = await sessionsCollection
      .where('outcome', '==', 'unavailable_refund_pending')
      .get();
    let scheduled = 0;
    const now = Date.now();
    for (const doc of snapshot.docs) {
      const session = doc.data() || {};
      if (ensureString(session.status || '', '').toLowerCase() === 'closed') {
        continue;
      }
      const manualDecline =
        typeof session.manualDeclineRefund === 'object' &&
        session.manualDeclineRefund !== null
          ? session.manualDeclineRefund
          : {};
      const requestedAt = Math.max(
        0,
        ensureInteger(
          manualDecline.requestedAt ?? session.acceptedAt ?? session.createdAt,
          now
        )
      );
      const closeDelayMs = Math.max(
        5_000,
        Math.min(
          120_000,
          ensureInteger(
            manualDecline.closeDelayMs,
            MANUAL_DECLINE_CLOSE_DELAY_MS
          )
        )
      );
      if (
        scheduleManualDeclineClose({
          sessionId: doc.id,
          delayMs: Math.max(0, requestedAt + closeDelayMs - now),
        })
      ) {
        scheduled += 1;
      }
    }
    return { scheduled, unavailable: false };
  } catch (error) {
    console.error(
      '[manual-decline-refund] Failed to reconcile pending sessions',
      error
    );
    return { scheduled: 0, unavailable: false, failed: true };
  }
};

const startManualDeclineReconciler = () => {
  if (manualDeclineReconcileTimer) return;
  void reconcilePendingManualDeclines();
  manualDeclineReconcileTimer = setInterval(() => {
    void reconcilePendingManualDeclines();
  }, 60_000);
  if (typeof manualDeclineReconcileTimer.unref === 'function') {
    manualDeclineReconcileTimer.unref();
  }
};

app.post('/api/requests/:id/decline-with-refund', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const id = ensureString(req.params.id || '', '').trim().slice(0, 64).toUpperCase();
  const requestsCollection = getRequestsCollection();
  const sessionsCollection = getSessionsCollection();
  if (!requestsCollection || !sessionsCollection) {
    console.error('Firestore not configured. Cannot decline request with refund.');
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  try {
    const uid = ensureString(req.user?.uid || '', '').trim();
    if (!uid) {
      return res.status(401).json({ error: 'invalid_token' });
    }

    const existingActiveSession = await findActiveRealtimeSessionForTech({
      sessionsCollection,
      techUid: uid,
    });
    if (existingActiveSession) {
      return res.status(409).json({
        error: 'active_session_exists',
        sessionId: existingActiveSession.id,
      });
    }

    const requestRef = requestsCollection.doc(id);
    const snapshot = await requestRef.get();
    if (!snapshot.exists) {
      return res.status(404).json({ error: 'request_not_found_or_already_taken' });
    }

    const request = snapshot.data() || {};
    if (ensureString(request.state || 'queued', '').toLowerCase() !== 'queued') {
      return res.status(409).json({ error: 'request_not_queued' });
    }

    const techData = req.techAccess?.techDoc || {};
    const normalizedTechName =
      ensureString(techData.name || techData.displayName || req.user?.name || req.body?.techName || 'Técnico', 'Técnico') ||
      'Técnico';
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
        source: 'manual_decline_refund',
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
    if (!clientRecordId) {
      return res.status(409).json({ error: 'client_not_registered' });
    }

    const now = Date.now();
    const sessionId = generateSessionId();
    const localSupportSessionId =
      ensureString(request.localSupportSessionId || supportProfile.localSupportSessionId || '', '').trim() || null;
    const requestedCreditsToConsume = Math.max(
      0,
      ensureInteger(
        request.creditsConsumed ??
          request.creditsToConsume ??
          supportProfile.creditsToConsume ??
          request.extra?.supportProfile?.creditsToConsume,
        0
      )
    );
    const isQueuedFreeFirstSupport =
      ensureBoolean(request.isFreeFirstSupport, false) || ensureBoolean(supportProfile.isFreeFirstSupport, false);
    const creditsRefunded = isQueuedFreeFirstSupport ? 0 : requestedCreditsToConsume;
    const closeDelayMs = Math.max(
      5_000,
      Math.min(120_000, ensureInteger(req.body?.closeDelayMs, MANUAL_DECLINE_CLOSE_DELAY_MS))
    );
    const message =
      ensureLongString(req.body?.message || '', '', 1200).trim() ||
      MANUAL_DECLINE_REFUND_MESSAGE;
    const baseExtra = typeof request.extra === 'object' && request.extra !== null ? { ...request.extra } : {};
    const baseTelemetry = normalizeTelemetryData(
      typeof baseExtra.telemetry === 'object' && baseExtra.telemetry !== null ? { ...baseExtra.telemetry } : {}
    );
    const resolvedSupportProfile = {
      ...supportProfile,
      isFreeFirstSupport: false,
      creditsToConsume: 0,
      originalCreditsToConsume: requestedCreditsToConsume,
      manualDeclineRefund: true,
    };

    const sessionData = {
      sessionId,
      requestId: id,
      clientId: request.clientId || null,
      clientSocketId: request.clientSocketId || request.clientId || null,
      clientRecordId,
      clientUid: request.clientUid || null,
      deviceAnchor: requestDeviceAnchor,
      clientPhone: normalizedClientPhone || normalizePhone(resolvedClientEntity?.phone || '') || null,
      techName: normalizedTechName,
      techId: uid,
      techUid: uid,
      techEmail: normalizedTechEmail,
      techPhotoURL: normalizedTechPhotoURL,
      tech: {
        techUid: uid,
        techId: uid,
        uid,
        id: uid,
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
      supportSessionId: localSupportSessionId,
      supportProfile: resolvedSupportProfile,
      profileCompleted: isClientProfileCompleted(resolvedClientEntity),
      requiresTechnicianRegistration: ensureBoolean(request.requiresTechnicianRegistration, false),
      isFreeFirstSupport: false,
      creditsConsumed: 0,
      creditsRefunded,
      requestedAt: request.createdAt || now,
      acceptedAt: now,
      waitTimeMs: now - (request.createdAt || now),
      status: 'active',
      outcome: 'unavailable_refund_pending',
      createdAt: now,
      updatedAt: now,
      telemetry: baseTelemetry,
      manualDeclineRefund: {
        requestedByTechUid: uid,
        requestedAt: now,
        closeDelayMs,
        message,
        creditsRefunded,
        creditAction: creditsRefunded > 0 ? 'not_charged_as_refund' : 'free_support_preserved',
      },
      extra: {
        ...baseExtra,
        supportProfile: resolvedSupportProfile,
        telemetry: baseTelemetry,
        manualDeclineRefund: true,
      },
    };

    const chatMessage = buildManualDeclineChatMessage({
      sessionId,
      message,
      techName: normalizedTechName,
      ts: now + 1,
    });
    const decline = await declineSupportQueueRequestTransaction({
      requestId: id,
      sessionId,
      techUid: uid,
      clientRecordId,
      localSupportSessionId,
      sessionData,
      chatMessage,
      creditsRefunded,
      now,
    });
    const declinedRequest = decline.requestData || request;
    const persistedSessionData = decline.sessionData || sessionData;

    emitManualDeclineChatMessage(sessionId, chatMessage);
    try {
      await persistQueueNotification({
        requestId: id,
        requestData: declinedRequest,
        state: 'accepted',
        sessionId,
        techUid: uid,
        techName: normalizedTechName,
        reason: 'manual_decline_refund',
      });
    } catch (error) {
      console.error(
        'Failed to persist post-commit manual decline notification',
        error
      );
    }

    const targetSocketId =
      declinedRequest.clientSocketId || declinedRequest.clientId;
    if (targetSocketId) {
      try {
        io.to(targetSocketId).emit('support:accepted', { sessionId, techName: normalizedTechName });
      } catch (err) {
        console.error('Failed to emit manual decline acceptance to client', err);
      }
    }

    emitQueueUpdated({
      requestId: id,
      state: 'accepted',
      sessionId,
      techName: normalizedTechName,
      clientUid: declinedRequest.clientUid || null,
      targetSocketId,
      notifyClient: true,
    });
    try {
      await emitSessionUpdated(sessionId);
    } catch (error) {
      console.error(
        'Failed to emit post-commit manual decline session update',
        error
      );
    }
    scheduleManualDeclineClose({ sessionId, delayMs: closeDelayMs });

    return res.json({
      ok: true,
      sessionId,
      closeDelayMs,
      creditsRefunded,
      creditAction: persistedSessionData.manualDeclineRefund.creditAction,
      messageId: chatMessage.id,
    });
  } catch (err) {
    console.error('Failed to decline request with refund', err);
    if (err instanceof SupportQueuePolicyError) {
      return res.status(err.status || 409).json({
        error: err.code,
        ...(err.details || {}),
      });
    }
    return res.status(500).json({ error: 'firestore_error' });
  }
});

app.get(
  '/api/client/support-session/active',
  requireAuth(['user']),
  async (req, res) => {
    if (!clientSessionRecoveryService) {
      return res.status(503).json({ error: 'firestore_unavailable' });
    }

    try {
      const result = await clientSessionRecoveryService.findActiveSession({
        uid: req.user?.uid,
        localSupportSessionId: req.query?.localSupportSessionId,
      });
      res.set('Cache-Control', 'private, no-store, max-age=0');
      res.set('Pragma', 'no-cache');
      return res.json(result);
    } catch (error) {
      if (error instanceof ClientSessionRecoveryError) {
        return res.status(error.status).json({ error: error.code });
      }
      console.error('Failed to recover active client support session', error);
      return res.status(500).json({ error: 'firestore_error' });
    }
  }
);

app.delete('/api/client/requests/:id', requireAuth(), async (req, res) => {
  const requestId = ensureString(req.params.id || '', '').trim().slice(0, 64).toUpperCase();
  if (!requestId) {
    return res.status(400).json({ error: 'invalid_request_id' });
  }
  const authUid = ensureString(req.user?.uid || '', '').trim();
  if (!authUid) {
    return res.status(401).json({ error: 'invalid_token' });
  }
  const tokenPhone = normalizePhone(req.user?.phone_number || '');

  try {
    const result = await cancelSupportQueueRequest({
      authUid,
      requestId,
      verifiedPhone: tokenPhone,
    });
    const requestData = result.requestData || null;
    if (result.action === 'cancel' && requestData) {
      try {
        await persistQueueNotification({
          requestId,
          requestData,
          state: 'removed',
          reason: 'client_cancelled',
        });
      } catch (error) {
        console.error(
          'Failed to persist post-commit client cancellation notification',
          error
        );
      }
    }

    const targetSocketId = ensureString(
      requestData?.clientSocketId || requestData?.clientId || '',
      ''
    ).trim();
    if (targetSocketId) {
      try {
        io.to(targetSocketId).emit('support:rejected', { requestId, reason: 'client_cancelled' });
      } catch (err) {
        console.error('Failed to emit rejection after client cancel', err);
      }
    }
    emitQueueUpdated({ requestId, state: 'removed' });
    return res.status(204).end();
  } catch (err) {
    console.error('Failed to cancel request from client endpoint', err);
    if (err instanceof SupportQueuePolicyError) {
      if (err.code === 'support_request_not_found') {
        return res.status(204).end();
      }
      return res.status(err.status || 409).json({
        error: err.code,
        ...(err.details || {}),
      });
    }
    return res.status(500).json({ error: 'firestore_error' });
  }
});

const removeSupportQueueRequestByTechTransaction = async ({
  requestId,
  techUid,
  now = Date.now(),
} = {}) => {
  const requestsCollection = getRequestsCollection();
  const supportSessionsCollection = getSupportSessionsCollection();
  const locksCollection = getSupportQueueLocksCollection();
  const anchorsCollection = getSupportQueueAnchorsCollection();
  const outcomesCollection = getSupportQueueOutcomesCollection();
  if (
    !db ||
    !requestsCollection ||
    !supportSessionsCollection ||
    !locksCollection ||
    !anchorsCollection ||
    !outcomesCollection
  ) {
    throw new SupportQueuePolicyError('firestore_unavailable', 503);
  }

  const normalizedRequestId =
    ensureString(requestId || '', '').trim().slice(0, 64).toUpperCase();
  const normalizedTechUid = ensureString(techUid || '', '').trim();
  if (
    !/^[A-Za-z0-9_-]{1,64}$/.test(normalizedRequestId) ||
    !normalizedTechUid
  ) {
    throw new SupportQueuePolicyError('invalid_payload', 400);
  }

  const requestRef = requestsCollection.doc(normalizedRequestId);
  const outcomeRef = outcomesCollection.doc(normalizedRequestId);
  return db.runTransaction(async (tx) => {
    const requestSnap = await tx.get(requestRef);
    const outcomeSnap = await tx.get(outcomeRef);
    const request = requestSnap.exists
      ? { requestId: requestSnap.id, ...(requestSnap.data() || {}) }
      : null;
    const outcome = outcomeSnap.exists
      ? { requestId: outcomeSnap.id, ...(outcomeSnap.data() || {}) }
      : null;
    const decision = decideTechQueueRemoval({ request, outcome });
    if (decision.action === 'already_removed') {
      return {
        ...decision,
        requestId: normalizedRequestId,
        requestData: null,
      };
    }

    const clientUid = ensureString(request.clientUid || '', '').trim();
    const localSupportSessionId =
      ensureString(
        request.localSupportSessionId ||
          request.supportProfile?.localSupportSessionId ||
          '',
        ''
      )
        .trim()
        .slice(0, 128) || null;
    if (
      localSupportSessionId &&
      !/^[A-Za-z0-9._:-]{1,128}$/.test(localSupportSessionId)
    ) {
      throw new SupportQueuePolicyError('invalid_local_support_session_id', 409);
    }

    const supportSessionRef = localSupportSessionId
      ? supportSessionsCollection.doc(localSupportSessionId)
      : null;
    const lockId = clientUid ? queueLockDocIdFromUid(clientUid) : null;
    const anchorId =
      clientUid && localSupportSessionId
        ? queueAnchorDocId(clientUid, localSupportSessionId)
        : null;
    const lockRef = lockId ? locksCollection.doc(lockId) : null;
    const anchorRef = anchorId ? anchorsCollection.doc(anchorId) : null;
    const supportSessionSnap = supportSessionRef
      ? await tx.get(supportSessionRef)
      : null;
    const lockSnap = lockRef ? await tx.get(lockRef) : null;
    const anchorSnap = anchorRef ? await tx.get(anchorRef) : null;

    if (supportSessionSnap?.exists) {
      const supportSession = supportSessionSnap.data() || {};
      const supportRequestId = ensureString(
        supportSession.queueRequestId || '',
        ''
      )
        .trim()
        .toUpperCase();
      const supportOwnerUid = ensureString(
        supportSession.clientUid || '',
        ''
      ).trim();
      if (
        (supportRequestId && supportRequestId !== normalizedRequestId) ||
        (clientUid && supportOwnerUid && supportOwnerUid !== clientUid)
      ) {
        throw new SupportQueuePolicyError('request_mismatch', 409);
      }
    }

    tx.delete(requestRef);
    if (
      lockRef &&
      lockSnap?.exists &&
      ensureString(lockSnap.data()?.requestId || '', '')
        .trim()
        .toUpperCase() === normalizedRequestId
    ) {
      tx.delete(lockRef);
    }
    if (
      anchorRef &&
      anchorSnap?.exists &&
      ensureString(anchorSnap.data()?.requestId || '', '')
        .trim()
        .toUpperCase() === normalizedRequestId
    ) {
      tx.delete(anchorRef);
    }
    if (supportSessionRef && supportSessionSnap?.exists) {
      tx.set(
        supportSessionRef,
        {
          queueRequestId: normalizedRequestId,
          queueStatus: 'cancelled',
          status: 'cancelled',
          queueReason: 'tech_removed',
          queueCancelledAt: now,
          updatedAt: now,
          expiresAt: admin.firestore.Timestamp.fromMillis(
            now + 30 * 24 * 60 * 60 * 1000
          ),
        },
        { merge: true }
      );
    }
    tx.set(
      outcomeRef,
      {
        requestId: normalizedRequestId,
        clientUid: clientUid || null,
        localSupportSessionId,
        status: 'removed',
        reason: 'tech_removed',
        techUid: normalizedTechUid,
        removedAt: now,
        updatedAt: now,
      },
      { merge: true }
    );

    return {
      ...decision,
      requestId: normalizedRequestId,
      requestData: request,
    };
  });
};

// Recusar/remover um request (apaga da fila e, se quiser, avisa o cliente)
app.delete('/api/requests/:id', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const id = ensureString(req.params.id || '', '').trim().slice(0, 64).toUpperCase();
  if (!/^[A-Za-z0-9_-]{1,64}$/.test(id)) {
    return res.status(400).json({ error: 'invalid_request_id' });
  }

  try {
    const techUid = ensureString(
      req.techAccess?.uid || req.user?.uid || '',
      ''
    ).trim();
    const result = await removeSupportQueueRequestByTechTransaction({
      requestId: id,
      techUid,
    });
    if (result.action !== 'remove' || !result.requestData) {
      return res.status(204).end();
    }

    const data = result.requestData;
    try {
      await persistQueueNotification({
        requestId: id,
        requestData: data,
        state: 'removed',
        techUid,
        techName:
          req.techAccess?.techDoc?.name ||
          req.techAccess?.techDoc?.displayName ||
          req.user?.name ||
          null,
        reason: 'tech_removed',
      });
    } catch (error) {
      console.error(
        'Failed to persist post-commit technical queue removal notification',
        error
      );
    }

    const targetSocketId = data.clientSocketId || data.clientId;
    if (targetSocketId) {
      try {
        io.to(targetSocketId).emit('support:rejected', { requestId: id });
      } catch (err) {
        console.error('Failed to emit rejection to client', err);
      }
    }
    emitQueueUpdated({ requestId: id, state: 'removed' });
    return res.status(204).end();
  } catch (err) {
    console.error('Failed to remove request', err);
    if (err instanceof SupportQueuePolicyError) {
      return res.status(err.status || 409).json({
        error: err.code,
        ...(err.details || {}),
      });
    }
    return res.status(500).json({ error: 'firestore_error' });
  }
});

const getFirestoreCollectionCount = async (collection) => {
  if (!collection || typeof collection.count !== 'function') {
    throw new Error('firestore_count_unavailable');
  }
  const snapshot = await collection.count().get();
  const count = Number(snapshot.data()?.count);
  if (!Number.isFinite(count) || count < 0) {
    throw new Error('invalid_firestore_count');
  }
  return count;
};

// Debug/saúde
app.get('/health', async (req, res) => {
  const deep = ensureString(req.query.deep || '', '').trim() === '1';
  const firestoreConfigured = isFirestoreReady();

  if (!deep) {
    return res.json({
      ok: true,
      service: 'up',
      firestoreConfigured,
      uptimeSec: Math.floor(process.uptime()),
      now: Date.now(),
    });
  }

  res.set('Cache-Control', 'private, no-store, max-age=0');
  const allowUnprotectedDeepHealth =
    !isProduction &&
    isExplicitlyEnabled(process.env.ALLOW_UNAUTHENTICATED_DEEP_HEALTH);
  if (!allowUnprotectedDeepHealth) {
    const expectedSecret = ensureLongString(
      process.env.HEALTH_DEEP_SECRET || '',
      '',
      4096
    ).trim();
    if (!expectedSecret) {
      console.error(
        'Protected deep health check requested without HEALTH_DEEP_SECRET.'
      );
      return res.status(503).json({ ok: false, error: 'deep_health_unavailable' });
    }

    const providedSecret = ensureLongString(
      req.get('x-health-secret') || '',
      '',
      4096
    );
    if (!timingSafeStringEqual(providedSecret, expectedSecret)) {
      return res.status(403).json({ ok: false, error: 'forbidden' });
    }
  }

  if (!firestoreConfigured) {
    return res.status(503).json({ ok: false, error: 'firestore_unavailable' });
  }

  try {
    const requestsCollection = getRequestsCollection();
    const sessionsCollection = getSessionsCollection();
    if (!requestsCollection || !sessionsCollection) {
      return res.status(503).json({ ok: false, error: 'firestore_unavailable' });
    }
    const [requestCount, sessionCount] = await Promise.all([
      getFirestoreCollectionCount(requestsCollection),
      getFirestoreCollectionCount(sessionsCollection),
    ]);
    return res.json({
      ok: true,
      service: 'up',
      firestoreConfigured,
      requests: requestCount,
      sessions: sessionCount,
      uptimeSec: Math.floor(process.uptime()),
      now: Date.now(),
    });
  } catch (err) {
    console.error('Failed to compute deep health status', err);
    return res.status(500).json({ ok: false, error: 'firestore_error' });
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
  const requestedPhoneRaw = ensureString(req.body?.phone || '', '').trim();
  const requestedPhone = requestedPhoneRaw ? normalizePhone(requestedPhoneRaw) : null;
  const verificationIdToken = ensureLongString(req.body?.verificationIdToken || '', '', 4096).trim();
  if (!uid || !name) {
    return res.status(400).json({ error: 'invalid_payload' });
  }
  if (requestedPhoneRaw && !requestedPhone) {
    return res.status(400).json({ error: 'invalid_phone', message: 'Telefone invalido.' });
  }

  try {
    const techRef = db.collection('techs').doc(uid);
    const techSnap = await techRef.get();
    const currentDoc = techSnap.exists ? techSnap.data() || {} : {};
    const previousName = ensureString(req.techAccess?.techDoc?.name || '', '') || null;
    const previousPhone = normalizePhone(currentDoc.phone || currentDoc.whatsappPhone || currentDoc.phoneNumber || '') || null;
    const phoneChanged = (requestedPhone || null) !== (previousPhone || null);
    let resolvedPhone = previousPhone;
    let phoneVerified = currentDoc.phoneVerified === true;
    let phoneVerifiedAt = Number.isFinite(Number(currentDoc.phoneVerifiedAt)) ? Number(currentDoc.phoneVerifiedAt) : null;
    let phoneVerificationUid = ensureString(currentDoc.phoneVerificationUid || '', '').trim() || null;
    let phoneVerificationMethod = ensureString(currentDoc.phoneVerificationMethod || '', '').trim() || null;

    if (phoneChanged) {
      resolvedPhone = requestedPhone || null;
      if (resolvedPhone) {
        if (!verificationIdToken) {
          return res.status(400).json({
            error: 'phone_verification_required',
            message: 'Verifique o novo telefone por SMS antes de salvar.',
          });
        }
        const verification = await verifySmsPhoneToken({
          verificationIdToken,
          expectedPhone: resolvedPhone,
        });
        if (!verification.ok) {
          const mappedError = mapPhoneVerificationError(verification.error);
          return res.status(mappedError.status).json({ error: mappedError.error, message: mappedError.message });
        }
        resolvedPhone = verification.phone || resolvedPhone;
        phoneVerified = true;
        phoneVerifiedAt = Date.now();
        phoneVerificationUid = verification.verificationUid || null;
        phoneVerificationMethod = 'sms';
      } else {
        phoneVerified = false;
        phoneVerifiedAt = null;
        phoneVerificationUid = null;
        phoneVerificationMethod = null;
      }
    } else if (resolvedPhone && verificationIdToken) {
      const verification = await verifySmsPhoneToken({
        verificationIdToken,
        expectedPhone: resolvedPhone,
      });
      if (!verification.ok) {
        const mappedError = mapPhoneVerificationError(verification.error);
        return res.status(mappedError.status).json({ error: mappedError.error, message: mappedError.message });
      }
      resolvedPhone = verification.phone || resolvedPhone;
      phoneVerified = true;
      phoneVerifiedAt = Date.now();
      phoneVerificationUid = verification.verificationUid || null;
      phoneVerificationMethod = 'sms';
    }

    const historyEntries = [];
    if (previousName !== name) {
      historyEntries.push(buildProfileHistoryEntry({ field: 'name', from: previousName, to: name, source: 'self' }));
    }
    if (phoneChanged) {
      historyEntries.push(
        buildProfileHistoryEntry({
          field: 'phone',
          from: previousPhone,
          to: resolvedPhone,
          source: 'self',
        })
      );
    }

    await techRef.set(
      {
        name,
        ...(phoneChanged
          ? {
              phone: resolvedPhone,
              whatsappPhone: resolvedPhone,
              phoneNumber: resolvedPhone,
              phoneVerified: phoneVerified === true,
              phoneVerifiedAt: phoneVerified === true ? phoneVerifiedAt : null,
              phoneVerificationUid: phoneVerified === true ? phoneVerificationUid : null,
              phoneVerificationMethod: phoneVerified === true ? phoneVerificationMethod : null,
            }
          : {}),
        ...(historyEntries.length
          ? {
              profileHistory: admin.firestore.FieldValue.arrayUnion(...historyEntries),
            }
          : {}),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    const authUpdates = {};
    if (name !== ensureString(req.user?.name || '', '')) {
      authUpdates.displayName = name;
    }
    if (phoneChanged && resolvedPhone) {
      authUpdates.phoneNumber = resolvedPhone;
    }
    if (phoneChanged && !resolvedPhone) {
      authUpdates.phoneNumber = null;
    }
    if (Object.keys(authUpdates).length) {
      try {
        await admin.auth().updateUser(uid, authUpdates);
      } catch (authError) {
        console.warn('Failed to update auth tech profile fields; keeping Firestore profile update', authError);
      }
    }

    return res.json({
      ok: true,
      uid,
      name,
      phone: resolvedPhone,
      phoneVerified: phoneVerified === true,
      phoneVerifiedAt: phoneVerified === true ? phoneVerifiedAt : null,
    });
  } catch (error) {
    console.error('Failed to update tech profile name', error);
    const mappedError = mapFirestoreWriteError(error);
    return res.status(mappedError.status).json({ error: mappedError.error, message: mappedError.message });
  }
});

app.post('/api/admin/verify-tech-phone', requireAuth(['tech']), requireSupervisor, async (req, res) => {
  const phone = normalizePhone(req.body?.phone || '');
  const verificationIdToken = ensureLongString(req.body?.verificationIdToken || '', '', 4096).trim();
  if (!phone || !verificationIdToken) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  const verification = await verifySmsPhoneToken({
    verificationIdToken,
    expectedPhone: phone,
  });
  if (!verification.ok) {
    const mappedError = mapPhoneVerificationError(verification.error);
    return res.status(mappedError.status).json({ error: mappedError.error, message: mappedError.message });
  }

  return res.json({
    ok: true,
    phone: verification.phone || phone,
    verificationUid: verification.verificationUid || null,
    method: 'sms',
  });
});

app.post('/api/tech/verify-phone', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const phone = normalizePhone(req.body?.phone || '');
  const verificationIdToken = ensureLongString(req.body?.verificationIdToken || '', '', 4096).trim();
  if (!phone || !verificationIdToken) {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  const verification = await verifySmsPhoneToken({
    verificationIdToken,
    expectedPhone: phone,
  });
  if (!verification.ok) {
    const mappedError = mapPhoneVerificationError(verification.error);
    return res.status(mappedError.status).json({ error: mappedError.error, message: mappedError.message });
  }

  return res.json({
    ok: true,
    phone: verification.phone || phone,
    verificationUid: verification.verificationUid || null,
    method: 'sms',
  });
});

app.post('/api/tech/profile-photo', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const uid = ensureString(req.user?.uid || '', '').trim();
  const photoURL = ensureLongString(req.body?.photoURL || '', '', 4096).trim();
  if (!uid || !photoURL) {
    return res.status(400).json({ error: 'invalid_payload' });
  }
  const techDoc = req.techAccess?.techDoc || {};
  if (
    !isAuthorizedTechProfilePhotoUrl({
      photoUrl: photoURL,
      authorizedPhotoUrl: techDoc.customPhotoURL,
      storagePath: techDoc.avatarPath,
      uid,
      bucketName: STORAGE_BUCKET_NAME,
    })
  ) {
    return res.status(400).json({ error: 'invalid_photo_url' });
  }

  try {
    const previousPhoto =
      ensureLongString(techDoc.customPhotoURL || techDoc.photoURL || '', '', 4096).trim() || null;
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

app.post('/api/auth/turnstile/verify', async (req, res) => {
  const config = resolveTechLoginTurnstileConfig();
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
      message: 'Confirme que voc\u00EA \u00E9 humano antes de continuar.',
    });
  }
  if (token.length > 2048) {
    return res.status(400).json({
      error: 'captcha_invalid',
      message: 'Token anti-bot inv\u00E1lido.',
    });
  }

  try {
    const verification = await verifyTechLoginTurnstileToken({
      token,
      remoteIpAddress: resolveRequestIpAddress(req),
      isProduction,
    });

    if (!verification.ok) {
      if (verification.error === 'captcha_hostname_mismatch') {
        return res.status(403).json({
          error: 'captcha_hostname_mismatch',
          message: 'Host inv\u00E1lido para esta chave do Cloudflare Turnstile.',
          hostname: verification.hostname || null,
        });
      }
      if (verification.error === 'captcha_action_mismatch') {
        return res.status(403).json({
          error: 'captcha_action_mismatch',
          message: 'A valida\u00E7\u00E3o anti-bot n\u00E3o corresponde ao login t\u00E9cnico.',
        });
      }
      if (verification.error === 'captcha_secret_missing' || verification.error === 'captcha_secret_invalid') {
        return res.status(503).json({
          error: verification.error,
          message: 'Cloudflare Turnstile n\u00E3o configurado corretamente no servidor.',
        });
      }
      return res.status(verification.status || 403).json({
        error: verification.error || 'captcha_invalid',
        message: 'Falha na validacao anti-bot. Tente novamente.',
        reason: verification.invalidReason || null,
      });
    }

    return res.json({ ok: true, hostname: verification.hostname || null });
  } catch (error) {
    console.error('Failed to verify Turnstile token for tech login', error);
    const mappedError = mapTurnstileRuntimeError(error);
    return res.status(503).json({
      error: mappedError.error,
      message: mappedError.message,
      hint: mappedError.hint,
    });
  }
});

app.get('/api/auth/me', requireAuth(), async (req, res) => {
  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  try {
    const decoded = req.user || {};
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
  const enabled = isExplicitlyEnabled(process.env.SUPERVISOR_BOOTSTRAP_ENABLED);
  if (!enabled) {
    return res.status(404).json({ error: 'not_found' });
  }

  if (!db) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }

  const secret = ensureLongString(
    process.env.SUPERVISOR_BOOTSTRAP_SECRET || '',
    '',
    4096
  ).trim();
  const expectedEmail = ensureLongString(
    process.env.SUPERVISOR_BOOTSTRAP_EMAIL || '',
    '',
    320
  )
    .trim()
    .toLowerCase();
  const email = ensureLongString(req.user?.email || '', '', 320)
    .trim()
    .toLowerCase();
  const providedSecret = ensureLongString(req.body?.secret || '', '', 4096);

  if (!secret || !expectedEmail) {
    console.error(
      'Supervisor bootstrap enabled without SUPERVISOR_BOOTSTRAP_SECRET and SUPERVISOR_BOOTSTRAP_EMAIL.'
    );
    return res.status(503).json({ error: 'bootstrap_unavailable' });
  }

  const secretMatches = timingSafeStringEqual(providedSecret, secret);
  const emailMatches = timingSafeStringEqual(email, expectedEmail);
  if (!secretMatches || !emailMatches || req.user?.email_verified !== true) {
    return res.status(403).json({ error: 'invalid_bootstrap_credentials' });
  }

  try {
    const uid = ensureString(req.user?.uid || '', '');
    if (!uid) {
      return res.status(401).json({ error: 'invalid_token' });
    }

    const userRecord = await admin.auth().getUser(uid);
    const userRecordEmail = ensureLongString(userRecord.email || '', '', 320)
      .trim()
      .toLowerCase();
    if (
      userRecord.emailVerified !== true ||
      !timingSafeStringEqual(userRecordEmail, expectedEmail)
    ) {
      return res.status(403).json({ error: 'invalid_bootstrap_credentials' });
    }

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
  const requestedPhoneRaw = ensureString(req.body?.phone || '', '').trim();
  const requestedPhone = requestedPhoneRaw ? normalizePhone(requestedPhoneRaw) : null;
  const verificationIdToken = ensureLongString(req.body?.verificationIdToken || '', '', 4096).trim();

  if (requestedPhoneRaw && !requestedPhone) {
    return res.status(400).json({ error: 'invalid_phone', message: 'Telefone invalido.' });
  }
  if (!email || !passwordTemp || passwordTemp.length < 6 || !name || !requestedPhone) {
    return res.status(400).json({ error: 'invalid_payload' });
  }
  if (!verificationIdToken) {
    return res.status(400).json({
      error: 'phone_verification_required',
      message: 'Verificacao por SMS obrigatoria para cadastrar novo tecnico.',
    });
  }

  try {
    const verification = await verifySmsPhoneToken({
      verificationIdToken,
      expectedPhone: requestedPhone,
    });
    if (!verification.ok) {
      const mappedError = mapPhoneVerificationError(verification.error);
      return res.status(mappedError.status).json({ error: mappedError.error, message: mappedError.message });
    }
    const verifiedPhone = verification.phone || requestedPhone;
    const phoneVerifiedAt = Date.now();

    const created = await admin.auth().createUser({
      email,
      password: passwordTemp,
      displayName: name,
      phoneNumber: verifiedPhone,
    });

    await admin.auth().setCustomUserClaims(created.uid, { role: 'tech' });

    await upsertTechDoc({
      uid: created.uid,
      email,
      name,
      active: true,
      role: 'tech',
      phone: verifiedPhone,
      phoneVerified: true,
      phoneVerifiedAt,
      phoneVerificationUid: verification.verificationUid || null,
      phoneVerificationMethod: 'sms',
    });

    return res.status(201).json({
      uid: created.uid,
      email: created.email || email,
      phone: verifiedPhone,
      phoneVerified: true,
      phoneVerifiedAt,
    });
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

    syncActiveTechSocketRoom(uid, active);
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
  const hasPhoneField = Object.prototype.hasOwnProperty.call(req.body || {}, 'phone');
  const requestedPhoneRaw = hasPhoneField ? ensureString(req.body?.phone || '', '').trim() : '';
  const requestedPhone = requestedPhoneRaw ? normalizePhone(requestedPhoneRaw) : null;
  const verificationIdToken = ensureLongString(req.body?.verificationIdToken || '', '', 4096).trim();

  if (!uid || !name) {
    return res.status(400).json({ error: 'invalid_payload' });
  }
  if (hasPhoneField && requestedPhoneRaw && !requestedPhone) {
    return res.status(400).json({ error: 'invalid_phone', message: 'Telefone invalido.' });
  }

  try {
    const techRef = db.collection('techs').doc(uid);
    const [currentUser, techSnap] = await Promise.all([admin.auth().getUser(uid), techRef.get()]);
    const currentDoc = techSnap.exists ? techSnap.data() || {} : {};
    const resolvedEmail = email || ensureString(currentUser.email || '', '').toLowerCase();
    if (!resolvedEmail) {
      return res.status(400).json({ error: 'invalid_payload', message: 'Email é obrigatório para salvar.' });
    }

    const previousPhone =
      normalizePhone(currentDoc.phone || currentDoc.whatsappPhone || currentDoc.phoneNumber || currentUser.phoneNumber || '') || null;
    let resolvedPhone = hasPhoneField ? (requestedPhone || null) : previousPhone;
    const phoneChanged = (resolvedPhone || null) !== (previousPhone || null);
    let phoneVerified = currentDoc.phoneVerified === true;
    let phoneVerifiedAt = Number.isFinite(Number(currentDoc.phoneVerifiedAt)) ? Number(currentDoc.phoneVerifiedAt) : null;
    let phoneVerificationUid = ensureString(currentDoc.phoneVerificationUid || '', '').trim() || null;
    let phoneVerificationMethod = ensureString(currentDoc.phoneVerificationMethod || '', '').trim() || null;

    if (resolvedPhone && verificationIdToken) {
      const verification = await verifySmsPhoneToken({
        verificationIdToken,
        expectedPhone: resolvedPhone,
      });
      if (!verification.ok) {
        const mappedError = mapPhoneVerificationError(verification.error);
        return res.status(mappedError.status).json({ error: mappedError.error, message: mappedError.message });
      }
      resolvedPhone = verification.phone || resolvedPhone;
      phoneVerified = true;
      phoneVerifiedAt = Date.now();
      phoneVerificationUid = verification.verificationUid || null;
      phoneVerificationMethod = 'sms';
    } else if (phoneChanged) {
      if (resolvedPhone && previousPhone) {
        return res.status(400).json({
          error: 'phone_verification_required',
          message: 'Verifique o novo telefone por SMS antes de salvar.',
        });
      }
      if (resolvedPhone && !previousPhone) {
        phoneVerified = true;
        phoneVerifiedAt = Date.now();
        phoneVerificationUid = ensureString(req.user?.uid || '', '').trim() || null;
        phoneVerificationMethod = 'admin_migrated';
      } else {
        phoneVerified = false;
        phoneVerifiedAt = null;
        phoneVerificationUid = null;
        phoneVerificationMethod = null;
      }
    }

    const updatePayload = { displayName: name };
    if (resolvedEmail !== ensureString(currentUser.email || '', '').toLowerCase()) {
      updatePayload.email = resolvedEmail;
    }
    if (phoneChanged) {
      updatePayload.phoneNumber = resolvedPhone || null;
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

    const profileHistoryEntries = [];
    if (phoneChanged) {
      profileHistoryEntries.push(
        buildProfileHistoryEntry({
          field: 'phone',
          from: previousPhone,
          to: resolvedPhone,
          source: 'supervisor',
        })
      );
    }

    await techRef.set(
      {
        uid,
        name,
        email: resolvedEmail,
        active,
        role,
        supervisor: role === 'supervisor',
        phone: resolvedPhone,
        whatsappPhone: resolvedPhone,
        phoneNumber: resolvedPhone,
        phoneVerified: resolvedPhone ? phoneVerified === true : false,
        phoneVerifiedAt: resolvedPhone && phoneVerified ? phoneVerifiedAt : null,
        phoneVerificationUid: resolvedPhone && phoneVerified ? phoneVerificationUid : null,
        phoneVerificationMethod: resolvedPhone && phoneVerified ? phoneVerificationMethod : null,
        ...(profileHistoryEntries.length
          ? {
              profileHistory: admin.firestore.FieldValue.arrayUnion(...profileHistoryEntries),
            }
          : {}),
        photoURL: ensureString(userRecord.photoURL || '', '') || null,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    syncActiveTechSocketRoom(uid, active);
    return res.json({
      ok: true,
      uid,
      phone: resolvedPhone,
      phoneVerified: resolvedPhone ? phoneVerified === true : false,
      phoneVerifiedAt: resolvedPhone && phoneVerified ? phoneVerifiedAt : null,
      phoneVerificationMethod: resolvedPhone && phoneVerified ? phoneVerificationMethod : null,
    });
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
    syncActiveTechSocketRoom(uid, false);
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
const CLIENT_MANUAL_VERIFICATION_CODE_TTL_MS = Math.max(
  60_000,
  Math.min(30 * 60_000, ensureInteger(process.env.CLIENT_MANUAL_VERIFICATION_CODE_TTL_MS, 10 * 60_000))
);
const CLIENT_MANUAL_VERIFICATION_MAX_ATTEMPTS = Math.max(
  1,
  Math.min(10, ensureInteger(process.env.CLIENT_MANUAL_VERIFICATION_MAX_ATTEMPTS, 5))
);

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

const normalizeWhatsAppTemplateParamName = (value = '') =>
  ensureString(value || '', '')
    .trim()
    .replace(/\{\{|\}\}/g, '')
    .replace(/[^a-zA-Z0-9_]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 80);

const parseWhatsAppTemplateParamNames = (value = '', fallback = []) => {
  const raw = ensureLongString(value || '', '', 512);
  const parsed = raw
    .split(/[,;|]/)
    .map((item) => normalizeWhatsAppTemplateParamName(item))
    .filter(Boolean);
  if (parsed.length) return Array.from(new Set(parsed));
  return ensureArray(fallback)
    .map((item) => normalizeWhatsAppTemplateParamName(item))
    .filter(Boolean);
};

const sanitizeWhatsAppTemplateTextParameter = (value = '', { fallback = '', maxLength = 512 } = {}) => {
  const normalized = ensureLongString(value || '', '', maxLength * 4)
    .replace(/\r/g, '\n')
    .replace(/\n+/g, ' ')
    .replace(/[*_`~]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  const safeValue = normalized || ensureLongString(fallback || '', '', maxLength);
  return ensureLongString(safeValue, '', maxLength).trim();
};

const sanitizeWhatsAppTemplateRichTextParameter = (value = '', { fallback = '', maxLength = 512 } = {}) => {
  const normalized = ensureLongString(value || '', '', maxLength * 4)
    .replace(/\r\n?/g, '\n')
    .replace(/[\u0000-\u0008\u000B-\u001F\u007F]/g, '')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
  const safeValue = normalized || ensureLongString(fallback || '', '', maxLength);
  return ensureLongString(safeValue, '', maxLength).trim();
};

const resolveSupportReportCredits = ({ sessionData = {}, clientSummary = null } = {}) => {
  const creditsConsumed = Math.max(0, ensureInteger(sessionData.creditsConsumed, 0));
  const creditsAfter =
    clientSummary && Number.isFinite(Number(clientSummary.credits)) ? Math.max(0, Number(clientSummary.credits)) : null;
  const creditsBefore = creditsAfter != null ? creditsAfter + creditsConsumed : null;
  const consumedDisplay = String(creditsConsumed);
  const consumedLabel = sessionData.isFreeFirstSupport ? '0 (primeiro suporte gratuito)' : String(creditsConsumed);
  return {
    creditsBefore,
    creditsConsumed,
    creditsConsumedDisplay: consumedDisplay,
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
    creditsConsumedTemplateDisplay: credits.creditsConsumedDisplay,
    creditsAfterDisplay: credits.creditsAfter == null ? '—' : String(credits.creditsAfter),
  };
};

const buildClientSupportReportText = (report) => {
  const body = [
    'SEGUE O RESUMO DO SEU ATENDIMENTO REALIZADO PELA SUPORTE X',
    `Ol\u00E1, ${report.clientName}! \uD83D\uDC4B`,
    'Seu atendimento foi conclu\u00EDdo com sucesso. Segue o resumo do que foi realizado no seu dispositivo:',
    SUPPORT_REPORT_DIVIDER,
    '\uD83D\uDCCB DADOS DO CLIENTE',
    `Nome: ${report.clientName}`,
    `Telefone: ${report.clientPhoneDisplay}`,
    `Data do atendimento: ${report.closedAtDisplay}`,
    `T\u00E9cnico respons\u00E1vel: ${report.techName}`,
    SUPPORT_REPORT_DIVIDER,
    '\u2699\uFE0F O QUE FOI IDENTIFICADO',
    `${report.symptom}`,
    `Resultado: ${report.outcomeLabel}`,
    SUPPORT_REPORT_DIVIDER,
    '\uD83D\uDEE0\uFE0F O QUE FOI FEITO',
    ...report.solutionItems.map((item) => `\u2022 ${item}`),
    SUPPORT_REPORT_DIVIDER,
    '\uD83D\uDCB3 CR\u00C9DITOS',
    `Antes: ${report.creditsBeforeDisplay}`,
    `Consumido: ${report.creditsConsumedDisplay}`,
    `Depois: ${report.creditsAfterDisplay}`,
    SUPPORT_REPORT_DIVIDER,
    '\uD83D\uDCDE SUPORTE',
    'Caso precise novamente, \u00E9 s\u00F3 abrir o aplicativo Suporte X e solicitar um novo atendimento.',
    '',
    'Mensagem enviada automaticamente ao cliente no encerramento do atendimento.',
  ];
  return body.join('\n');
};

const buildClientSupportWhatsAppSummaryText = (report) => {
  const safeCredit = (value) => {
    const normalized = ensureString(value || '', '').trim();
    if (!normalized) return 'x';
    return normalized;
  };
  const safeClientName =
    sanitizeWhatsAppTemplateTextParameter(report.clientName || '', { fallback: 'Cliente', maxLength: 80 }) || 'Cliente';
  const safePhone =
    sanitizeWhatsAppTemplateTextParameter(report.clientPhoneDisplay || '', { fallback: 'Nao informado', maxLength: 40 }) ||
    'Nao informado';
  const safeClosedAt =
    sanitizeWhatsAppTemplateTextParameter(report.closedAtDisplay || '', { fallback: 'Nao informado', maxLength: 48 }) ||
    'Nao informado';
  const safeTech =
    sanitizeWhatsAppTemplateTextParameter(report.techName || '', { fallback: 'Tecnico', maxLength: 80 }) || 'Tecnico';
  const safeOutcome =
    sanitizeWhatsAppTemplateTextParameter(report.outcomeLabel || '', { fallback: 'Nao informado', maxLength: 60 }) ||
    'Nao informado';
  const safeSymptom =
    sanitizeWhatsAppTemplateTextParameter(report.symptom || '', { fallback: 'Nao informado', maxLength: 220 }) ||
    'Nao informado';
  const solutionItems = ensureArray(report.solutionItems)
    .slice(0, 3)
    .map((item) => sanitizeWhatsAppTemplateTextParameter(item || '', { maxLength: 120 }))
    .filter(Boolean);

  const summary = [
    `DADOS: Nome ${safeClientName}; Telefone ${safePhone}; Data ${safeClosedAt}; Tecnico ${safeTech}`,
    `IDENTIFICADO: ${safeSymptom}; Resultado ${safeOutcome}`,
    `FEITO: ${(solutionItems.length ? solutionItems : ['Nao informado']).join(' / ')}`,
    `CREDITOS: Antes ${safeCredit(report.creditsBeforeDisplay)}; Consumido ${safeCredit(
      report.creditsConsumedDisplay
    )}; Depois ${safeCredit(report.creditsAfterDisplay)}`,
  ].join(' | ');

  return sanitizeWhatsAppTemplateTextParameter(summary, { maxLength: 700, fallback: 'Resumo indisponivel' });
};

const escapeHtmlForEmail = (value = '') =>
  ensureFullString(value || '', '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

const buildClientSupportReportEmailHtml = (report, textVersion) => {
  const safeClientName = escapeHtmlForEmail(report.clientName || 'Cliente');
  const safePhone = escapeHtmlForEmail(report.clientPhoneDisplay || 'N\u00E3o informado');
  const safeClosedAt = escapeHtmlForEmail(report.closedAtDisplay || 'N\u00E3o informado');
  const safeTechName = escapeHtmlForEmail(report.techName || 'N\u00E3o informado');
  const safeSymptom = escapeHtmlForEmail(report.symptom || 'N\u00E3o informado');
  const safeOutcome = escapeHtmlForEmail(report.outcomeLabel || 'N\u00E3o informado');
  const safeCreditsBefore = escapeHtmlForEmail(report.creditsBeforeDisplay || '\u2014');
  const safeCreditsConsumed = escapeHtmlForEmail(report.creditsConsumedDisplay || '\u2014');
  const safeCreditsAfter = escapeHtmlForEmail(report.creditsAfterDisplay || '\u2014');
  const solutionItems = ensureArray(report.solutionItems).filter(Boolean);
  const safeSolutionItems = solutionItems.length
    ? solutionItems
        .map((item) => `<li style="margin:0 0 6px 18px;">${escapeHtmlForEmail(item)}</li>`)
        .join('')
    : '<li style="margin:0 0 6px 18px;">N\u00E3o informado</li>';
  const safePreviewText =
    escapeHtmlForEmail(
      ensureString(textVersion || '', '')
        .replace(/\s+/g, ' ')
        .trim() || 'Resumo de atendimento'
    ) || 'Resumo de atendimento';
  return [
    '<!doctype html>',
    '<html lang="pt-BR">',
    '<body style="margin:0;padding:18px;background:#f4f7fb;color:#0f172a;font-family:Arial,Helvetica,sans-serif;">',
    `<span style="display:none!important;visibility:hidden;opacity:0;color:transparent;height:0;width:0;overflow:hidden;">${safePreviewText}</span>`,
    '<div style="max-width:760px;margin:0 auto;background:#ffffff;border:1px solid #dbe4ff;border-radius:12px;padding:26px 28px;">',
    '<p style="margin:0 0 20px 0;text-align:center;font-size:20px;line-height:1.3;font-weight:800;text-decoration:underline;color:#1e293b;">SEGUE O RESUMO DO SEU ATENDIMENTO REALIZADO PELA SUPORTE X</p>',
    `<p style="margin:0 0 8px 0;font-size:16px;line-height:1.3;color:#0f172a;">Ol\u00E1, <strong>${safeClientName}</strong>! 👋</p>`,
    '<p style="margin:0 0 16px 0;font-size:15px;line-height:1.5;color:#0f172a;">Seu atendimento foi conclu\u00EDdo com sucesso. Segue o resumo do que foi realizado no seu dispositivo:</p>',
    '<div style="margin:16px 0;border-top:1px solid #0f172a;width:340px;"></div>',
    '<p style="margin:0 0 8px 0;font-size:16px;font-weight:700;color:#0f172a;">📋 DADOS DO CLIENTE</p>',
    `<p style="margin:0 0 4px 0;font-size:15px;line-height:1.45;"><strong>Nome:</strong> ${safeClientName}</p>`,
    `<p style="margin:0 0 4px 0;font-size:15px;line-height:1.45;"><strong>Telefone:</strong> ${safePhone}</p>`,
    `<p style="margin:0 0 4px 0;font-size:15px;line-height:1.45;"><strong>Data do atendimento:</strong> ${safeClosedAt}</p>`,
    `<p style="margin:0 0 4px 0;font-size:15px;line-height:1.45;"><strong>T\u00E9cnico respons\u00E1vel:</strong> ${safeTechName}</p>`,
    '<div style="margin:16px 0;border-top:1px solid #0f172a;width:340px;"></div>',
    '<p style="margin:0 0 8px 0;font-size:16px;font-weight:700;color:#0f172a;">⚙️ O QUE FOI IDENTIFICADO</p>',
    `<p style="margin:0 0 4px 0;font-size:15px;line-height:1.45;">${safeSymptom}</p>`,
    `<p style="margin:0 0 4px 0;font-size:15px;line-height:1.45;"><strong>Resultado:</strong> ${safeOutcome}</p>`,
    '<div style="margin:16px 0;border-top:1px solid #0f172a;width:340px;"></div>',
    '<p style="margin:0 0 8px 0;font-size:16px;font-weight:700;color:#0f172a;">🛠️ O QUE FOI FEITO</p>',
    `<ul style="margin:0 0 4px 0;padding:0;list-style:disc;font-size:15px;line-height:1.45;color:#0f172a;">${safeSolutionItems}</ul>`,
    '<div style="margin:16px 0;border-top:1px solid #0f172a;width:340px;"></div>',
    '<p style="margin:0 0 8px 0;font-size:16px;font-weight:700;color:#0f172a;">💳 CR\u00C9DITOS</p>',
    `<p style="margin:0 0 4px 0;font-size:15px;line-height:1.45;"><strong>Antes:</strong> ${safeCreditsBefore}</p>`,
    `<p style="margin:0 0 4px 0;font-size:15px;line-height:1.45;"><strong>Consumido:</strong> ${safeCreditsConsumed}</p>`,
    `<p style="margin:0 0 4px 0;font-size:15px;line-height:1.45;"><strong>Depois:</strong> ${safeCreditsAfter}</p>`,
    '<div style="margin:16px 0;border-top:1px solid #0f172a;width:340px;"></div>',
    '<p style="margin:0 0 8px 0;font-size:16px;font-weight:700;color:#0f172a;">📞 SUPORTE</p>',
    '<p style="margin:0 0 4px 0;font-size:15px;line-height:1.45;">Caso precise novamente, \u00E9 s\u00F3 abrir o aplicativo Suporte X e solicitar um novo atendimento.</p>',
    '<p style="margin:22px 0 0 0;text-align:center;font-size:12px;line-height:1.35;color:#64748b;">Mensagem enviada automaticamente ao cliente no encerramento do atendimento.</p>',
    '</div>',
    '</body>',
    '</html>',
  ].join('');
};

const resolveMetaWhatsAppApiConfig = () => {
  const token = ensureLongString(process.env.WHATSAPP_ACCESS_TOKEN || '', '', 4096).trim();
  const phoneNumberId = ensureString(process.env.WHATSAPP_PHONE_NUMBER_ID || '', '').trim();
  const apiVersion = ensureString(process.env.WHATSAPP_API_VERSION || 'v21.0', '').trim() || 'v21.0';
  return {
    enabled: Boolean(token && phoneNumberId),
    token,
    phoneNumberId,
    apiVersion,
  };
};

const sendMetaWhatsAppTextMessage = async ({ toPhone = '', text = '', config = null } = {}) => {
  const resolvedConfig = config && typeof config === 'object' ? config : resolveMetaWhatsAppApiConfig();
  if (!resolvedConfig?.enabled) {
    return { ok: false, reason: 'not_configured', statusCode: 503 };
  }
  const recipient = normalizePhone(toPhone || '');
  if (!recipient) {
    return { ok: false, reason: 'missing_recipient', statusCode: 400 };
  }
  const safeText = ensureLongString(text || '', '', 3900).trim();
  if (!safeText) {
    return { ok: false, reason: 'missing_text', statusCode: 400 };
  }
  const endpoint = `https://graph.facebook.com/${encodeURIComponent(resolvedConfig.apiVersion)}/${encodeURIComponent(
    resolvedConfig.phoneNumberId
  )}/messages`;
  const payload = {
    messaging_product: 'whatsapp',
    recipient_type: 'individual',
    to: recipient.replace(/\D/g, ''),
    type: 'text',
    text: {
      preview_url: false,
      body: safeText,
    },
  };
  const body = JSON.stringify(payload);
  try {
    const response = await postJsonWithRuntimeFallback({
      url: endpoint,
      headers: {
        Authorization: `Bearer ${resolvedConfig.token}`,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body, 'utf8'),
      },
      body,
      timeoutMs: 12000,
    });
    const responsePayload = JSON.parse(ensureFullString(response.text || '{}', '{}') || '{}');
    if (!response.ok) {
      const providerMessage = ensureLongString(responsePayload?.error?.message || '', '', 512).trim();
      const providerDetails = ensureLongString(responsePayload?.error?.error_data?.details || '', '', 512).trim();
      const fallbackReason = ensureString(response.statusText || '', '').trim() || 'provider_error';
      const baseReason = providerMessage || fallbackReason;
      const reason = providerDetails ? `${baseReason} | details: ${providerDetails}` : baseReason;
      return {
        ok: false,
        reason,
        statusCode: response.status || 500,
        providerError: {
          code: Number.isFinite(Number(responsePayload?.error?.code)) ? Number(responsePayload.error.code) : null,
          details: providerDetails || null,
          fbtraceId: ensureString(responsePayload?.error?.fbtrace_id || '', '').trim() || null,
        },
      };
    }
    return {
      ok: true,
      providerMessageId: ensureString(responsePayload?.messages?.[0]?.id || '', '').trim() || null,
      recipient,
    };
  } catch (error) {
    return { ok: false, reason: ensureString(error?.message || '', 'provider_error'), statusCode: 500 };
  }
};

const resolveSupportReportChannelConfig = () => {
  const metaConfig = resolveMetaWhatsAppApiConfig();
  const whatsappTemplateName =
    ensureString(process.env.WHATSAPP_TEMPLATE_NAME || 'relatorio_whatsapp_cliente', '').trim() ||
    'relatorio_whatsapp_cliente';
  const whatsappTemplateLanguage = ensureString(process.env.WHATSAPP_TEMPLATE_LANGUAGE || 'pt_BR', '').trim() || 'pt_BR';
  const whatsappTemplateBodyParamNames = parseWhatsAppTemplateParamNames(
    process.env.WHATSAPP_TEMPLATE_BODY_PARAM_NAMES || '',
    [
      'nome_do_cliente',
      'data_atendimento',
      'tecnico_responsavel',
      'problema_identificado',
      'status_atendimento',
      'creditos_antes',
      'creditos_consumidos',
      'creditos_depois',
    ]
  ).slice(0, 8);
  const whatsappTemplateUseNamedParams = ensureBoolean(process.env.WHATSAPP_TEMPLATE_USE_NAMED_PARAMS, false);
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
      enabled: metaConfig.enabled,
      token: metaConfig.token,
      phoneNumberId: metaConfig.phoneNumberId,
      apiVersion: metaConfig.apiVersion,
      templateName: whatsappTemplateName,
      templateLanguage: whatsappTemplateLanguage,
      templateBodyParamNames: whatsappTemplateBodyParamNames,
      templateUseNamedParams: whatsappTemplateUseNamedParams,
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

const buildWhatsAppTemplateTextParameter = (
  text = '',
  parameterName = '',
  { allowFormatting = false, maxLength = 1024 } = {}
) => {
  const safeText = allowFormatting
    ? sanitizeWhatsAppTemplateRichTextParameter(text || '', { fallback: 'x', maxLength })
    : sanitizeWhatsAppTemplateTextParameter(text || '', { fallback: 'x', maxLength });
  const payload = {
    type: 'text',
    text: safeText || 'x',
  };
  const normalizedName = normalizeWhatsAppTemplateParamName(parameterName);
  if (normalizedName) payload.parameter_name = normalizedName;
  return payload;
};

const resolveSupportReportTemplateBodyParameters = ({
  report = null,
  clientName = 'Cliente',
  summaryText = '',
  config = null,
} = {}) => {
  const safeClientName =
    sanitizeWhatsAppTemplateTextParameter(report?.clientName || clientName || 'Cliente', {
      fallback: 'Cliente',
      maxLength: 80,
    }) || 'Cliente';
  const safeClosedAt =
    sanitizeWhatsAppTemplateTextParameter(report?.closedAtDisplay || '', {
      fallback: 'Nao informado',
      maxLength: 48,
    }) || 'Nao informado';
  const safeTechName =
    sanitizeWhatsAppTemplateTextParameter(report?.techName || '', { fallback: 'Nao informado', maxLength: 80 }) ||
    'Nao informado';
  const safeProblem =
    sanitizeWhatsAppTemplateTextParameter(report?.symptom || summaryText || '', { fallback: 'Nao informado', maxLength: 220 }) ||
    'Nao informado';
  const safeOutcome =
    sanitizeWhatsAppTemplateTextParameter(report?.outcomeLabel || '', { fallback: 'Nao informado', maxLength: 60 }) ||
    'Nao informado';
  const safeCreditsBefore =
    sanitizeWhatsAppTemplateTextParameter(report?.creditsBeforeDisplay || '', {
      fallback: 'Nao informado',
      maxLength: 24,
    }) || 'Nao informado';
  const rawConsumedCredit =
    ensureString(report?.creditsConsumedTemplateDisplay || report?.creditsConsumedDisplay || '', '').trim() || '0';
  const normalizedConsumedCredit =
    ensureString(rawConsumedCredit.match(/\d+/)?.[0] || rawConsumedCredit, '').trim() || '0';
  const safeCreditsConsumed =
    sanitizeWhatsAppTemplateTextParameter(normalizedConsumedCredit, { fallback: '0', maxLength: 24 }) || '0';
  const safeCreditsAfter =
    sanitizeWhatsAppTemplateTextParameter(report?.creditsAfterDisplay || '', {
      fallback: 'Nao informado',
      maxLength: 24,
    }) || 'Nao informado';
  const values = [
    { text: safeClientName, allowFormatting: false, maxLength: 80 },
    { text: safeClosedAt, allowFormatting: false, maxLength: 48 },
    { text: safeTechName, allowFormatting: false, maxLength: 80 },
    { text: safeProblem, allowFormatting: false, maxLength: 220 },
    { text: safeOutcome, allowFormatting: false, maxLength: 60 },
    { text: safeCreditsBefore, allowFormatting: false, maxLength: 24 },
    { text: safeCreditsConsumed, allowFormatting: false, maxLength: 24 },
    { text: safeCreditsAfter, allowFormatting: false, maxLength: 24 },
  ];
  const paramNames = ensureArray(config?.templateBodyParamNames)
    .map((value) => normalizeWhatsAppTemplateParamName(value))
    .filter(Boolean)
    .slice(0, values.length);
  const useNamedParams = ensureBoolean(config?.templateUseNamedParams, paramNames.length === values.length);
  if (useNamedParams && paramNames.length === values.length) {
    return values.map((value, index) => buildWhatsAppTemplateTextParameter(value.text, paramNames[index], value));
  }
  return values.map((value) => buildWhatsAppTemplateTextParameter(value.text, '', value));
};

const sendSupportReportViaWhatsApp = async ({
  toPhone = null,
  text = '',
  clientName = 'Cliente',
  summaryText = '',
  report = null,
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
  const templateBodyParameters = resolveSupportReportTemplateBodyParameters({
    report,
    clientName,
    summaryText,
    config,
  });
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
              parameters: templateBodyParameters,
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
      const providerErrorMessage = ensureLongString(payload?.error?.message || '', '', 512).trim();
      const providerErrorDetails = ensureLongString(payload?.error?.error_data?.details || '', '', 512).trim();
      const statusFallback = ensureString(response.statusText || '', '').trim() || 'provider_error';
      const reason = providerErrorMessage || statusFallback;
      const reasonWithDetails = providerErrorDetails ? `${reason} | details: ${providerErrorDetails}` : reason;
      return {
        channel: 'whatsapp',
        status: 'error',
        reason: reasonWithDetails,
        statusCode: response.status || 500,
        providerError: {
          code: Number.isFinite(Number(payload?.error?.code)) ? Number(payload.error.code) : null,
          details: providerErrorDetails || null,
          fbtraceId: ensureString(payload?.error?.fbtrace_id || '', '').trim() || null,
        },
      };
    }
    const providerMessageId = ensureString(payload?.messages?.[0]?.id || '', '').trim() || null;
    await persistWhatsAppApiMessage({
      phone: target,
      contactName: clientName || 'Cliente',
      text: fallbackText || summaryText || '',
      direction: 'outbound',
      from: 'system',
      status: 'sent',
      type: templateName ? 'template' : 'text',
      ts: Date.now(),
      providerMessageId,
      templateName: templateName || null,
      metadata: {
        origin: 'support_report',
        channel: 'whatsapp',
      },
    });
    return { channel: 'whatsapp', status: 'sent', recipient: target, providerMessageId };
  } catch (error) {
    return { channel: 'whatsapp', status: 'error', reason: ensureString(error?.message || '', 'provider_error') };
  }
};

const normalizeQueueAlertRecipientKey = (value) =>
  ensureString(value || '', '')
    .trim()
    .replace(/[^a-zA-Z0-9_-]/g, '_')
    .slice(0, 96) || null;

const resolveQueueAlertChannelConfig = () => {
  const queueAlertsEnabled = ensureBoolean(process.env.QUEUE_ALERTS_ENABLED, true);
  const whatsappToken = ensureLongString(process.env.WHATSAPP_ACCESS_TOKEN || '', '', 4096).trim();
  const whatsappPhoneNumberId = ensureString(process.env.WHATSAPP_PHONE_NUMBER_ID || '', '').trim();
  const whatsappApiVersion = ensureString(process.env.WHATSAPP_API_VERSION || 'v21.0', '').trim() || 'v21.0';
  const templateName =
    ensureString(
      process.env.WHATSAPP_QUEUE_ALERT_TEMPLATE_NAME || process.env.WHATSAPP_TEMPLATE_TECH || 'queue_wait_alert_v1',
      ''
    ).trim() || 'queue_wait_alert_v1';
  const templateLanguage =
    ensureString(
      process.env.WHATSAPP_QUEUE_ALERT_TEMPLATE_LANGUAGE || process.env.WHATSAPP_TEMPLATE_LANGUAGE || 'pt_BR',
      ''
    ).trim() || 'pt_BR';
  const whatsappEnabled = queueAlertsEnabled && Boolean(whatsappToken && whatsappPhoneNumberId);

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
  const emailReplyTo = normalizeEmail(process.env.SUPPORT_REPORT_EMAIL_REPLY_TO || '');
  const emailEnabled = queueAlertsEnabled && ensureBoolean(process.env.QUEUE_ALERT_EMAIL_ENABLED, true) && Boolean(emailApiKey && emailFrom);
  const emailThresholdMinutes = Math.max(5, ensureInteger(process.env.QUEUE_ALERT_EMAIL_THRESHOLD_MINUTES, 15));
  const emailStepMinutes = Math.max(5, ensureInteger(process.env.QUEUE_ALERT_EMAIL_STEP_MINUTES, 15));

  return {
    enabled: whatsappEnabled || emailEnabled,
    whatsappEnabled,
    token: whatsappToken,
    phoneNumberId: whatsappPhoneNumberId,
    apiVersion: whatsappApiVersion,
    templateName,
    templateLanguage,
    forceRecipient: normalizePhone(process.env.WHATSAPP_QUEUE_ALERT_FORCE_TO || ''),
    email: {
      enabled: emailEnabled,
      apiKey: emailApiKey,
      from: emailFrom,
      replyTo: emailReplyTo,
      thresholdMinutes: emailThresholdMinutes,
      stepMinutes: emailStepMinutes,
      forceRecipient: normalizeEmail(process.env.QUEUE_ALERT_EMAIL_FORCE_TO || ''),
    },
  };
};

const resolveQueueAlertRecipientContacts = async (techUid, techData = {}) => {
  const phoneFromTechDoc = normalizePhone(
    techData.whatsappPhone ||
      techData.phone ||
      techData.phoneNumber ||
      techData.contactPhone ||
      techData.profile?.whatsappPhone ||
      techData.profile?.phone ||
      ''
  );
  const emailFromTechDoc = normalizeEmail(techData.email || techData.profile?.email || techData.contactEmail || '');

  let userRecord = null;
  try {
    userRecord = await admin.auth().getUser(techUid);
  } catch (_error) {
    userRecord = null;
  }

  const phone = phoneFromTechDoc || normalizePhone(userRecord?.phoneNumber || '') || null;
  const email = emailFromTechDoc || normalizeEmail(userRecord?.email || '') || null;

  return { phone, email };
};

const listQueueAlertRecipients = async ({ requireWhatsApp = false, requireEmail = false } = {}) => {
  if (!db) return [];
  const snapshot = await db.collection('techs').where('active', '==', true).get();
  const recipients = [];
  const missingPhone = [];
  const missingEmail = [];

  for (const techDoc of snapshot.docs) {
    const techData = techDoc.data() || {};
    if (ensureBoolean(techData.receiveQueueAlerts, true) !== true) continue;
    if (ensureBoolean(techData.isOnDuty, true) !== true) continue;

    const uid = ensureString(techDoc.id || '', '').trim();
    const key = normalizeQueueAlertRecipientKey(uid);
    if (!uid || !key) continue;

    const contacts = await resolveQueueAlertRecipientContacts(uid, techData);
    const phone = contacts.phone || null;
    const email = contacts.email || null;
    if (!phone && !email) continue;
    if (!phone && requireWhatsApp) missingPhone.push(uid);
    if (!email && requireEmail) missingEmail.push(uid);

    recipients.push({
      uid,
      key,
      role: normalizeRole(techData.role || 'tech'),
      name: ensureString(techData.name || 'Tecnico', '').trim() || 'Tecnico',
      phone,
      email,
    });
  }

  if (missingPhone.length) {
    const now = Date.now();
    if (now - queueAlertLastMissingPhoneLogAt > 15 * 60 * 1000) {
      queueAlertLastMissingPhoneLogAt = now;
      console.warn('[queue-alert] Tecnicos ativos sem telefone para WhatsApp:', missingPhone.join(', '));
    }
  }
  if (missingEmail.length) {
    const now = Date.now();
    if (now - queueAlertLastMissingPhoneLogAt > 15 * 60 * 1000) {
      queueAlertLastMissingPhoneLogAt = now;
      console.warn('[queue-alert] Tecnicos ativos sem e-mail para alerta de backup:', missingEmail.join(', '));
    }
  }

  return recipients;
};

const resolveQueueAlertThresholdMinutes = (waitMinutes) => {
  if (!Number.isFinite(waitMinutes) || waitMinutes < QUEUE_ALERT_FIRST_THRESHOLD_MINUTES) return null;
  const step = Math.max(1, QUEUE_ALERT_STEP_MINUTES);
  return Math.floor(waitMinutes / step) * step;
};

const resolveQueueAlertEmailThresholdMinutes = (waitMinutes, config = null) => {
  const first = Math.max(5, ensureInteger(config?.email?.thresholdMinutes, 15));
  const step = Math.max(5, ensureInteger(config?.email?.stepMinutes, 15));
  if (!Number.isFinite(waitMinutes) || waitMinutes < first) return null;
  return Math.floor(waitMinutes / step) * step;
};

const sendQueueAlertViaWhatsApp = async ({
  techPhone = null,
  techName = 'Tecnico',
  waitMinutes = 0,
  requestId = '',
  clientName = 'Cliente',
  deviceModel = 'Nao informado',
  platform = 'Nao informado',
  config = null,
} = {}) => {
  if (!config?.whatsappEnabled) {
    return { channel: 'whatsapp', status: 'skipped', reason: 'not_configured' };
  }

  const target = config.forceRecipient || normalizePhone(techPhone || '');
  if (!target) {
    return { channel: 'whatsapp', status: 'skipped', reason: 'missing_recipient' };
  }

  const endpoint = `https://graph.facebook.com/${encodeURIComponent(config.apiVersion)}/${encodeURIComponent(
    config.phoneNumberId
  )}/messages`;
  const payload = {
    messaging_product: 'whatsapp',
    recipient_type: 'individual',
    to: target.replace(/\D/g, ''),
    type: 'template',
    template: {
      name: ensureString(config.templateName || 'queue_wait_alert_v1', '').trim() || 'queue_wait_alert_v1',
      language: { code: ensureString(config.templateLanguage || 'pt_BR', '').trim() || 'pt_BR' },
      components: [
        {
          type: 'body',
          parameters: [
            { type: 'text', text: ensureLongString(techName || 'Tecnico', 'Tecnico', 80) },
            { type: 'text', text: String(Math.max(QUEUE_ALERT_FIRST_THRESHOLD_MINUTES, Number(waitMinutes) || 0)) },
            { type: 'text', text: ensureLongString(clientName || 'Cliente sem nome', 'Cliente sem nome', 80) },
            { type: 'text', text: ensureLongString(requestId || 'sem_id', 'sem_id', 80) },
            { type: 'text', text: ensureLongString(deviceModel || 'Nao informado', 'Nao informado', 80) },
            { type: 'text', text: ensureLongString(platform || 'Nao informado', 'Nao informado', 80) },
          ],
        },
      ],
    },
  };
  const body = JSON.stringify(payload);

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
    const responsePayload = JSON.parse(ensureFullString(response.text || '{}', '{}') || '{}');
    if (!response.ok) {
      const reason =
        ensureString(responsePayload?.error?.message || '', '').trim() ||
        ensureString(response.statusText || '', '').trim() ||
        'provider_error';
      return { channel: 'whatsapp', status: 'error', reason, statusCode: response.status || 500 };
    }
    const providerMessageId = ensureString(responsePayload?.messages?.[0]?.id || '', '').trim() || null;
    const summaryText = [
      `Alerta de fila: ${Math.max(QUEUE_ALERT_FIRST_THRESHOLD_MINUTES, Number(waitMinutes) || 0)} min`,
      `Cliente: ${clientName || 'Cliente'}`,
      `Sessao: ${requestId || 'sem_id'}`,
    ].join(' | ');
    await persistWhatsAppApiMessage({
      phone: target,
      contactName: techName || 'Tecnico',
      text: summaryText,
      direction: 'outbound',
      from: 'system',
      status: 'sent',
      type: 'template',
      ts: Date.now(),
      providerMessageId,
      templateName: ensureString(config.templateName || 'queue_wait_alert_v1', '').trim() || 'queue_wait_alert_v1',
      metadata: {
        origin: 'queue_alert',
        requestId: requestId || null,
        waitMinutes: Math.max(QUEUE_ALERT_FIRST_THRESHOLD_MINUTES, Number(waitMinutes) || 0),
      },
    });
    return { channel: 'whatsapp', status: 'sent', recipient: target, providerMessageId };
  } catch (error) {
    return { channel: 'whatsapp', status: 'error', reason: ensureString(error?.message || '', 'provider_error') };
  }
};

const WHATSAPP_API_CONVERSATIONS_COLLECTION = 'whatsapp_api_conversations';

const normalizeWhatsAppApiConversationId = (value = '') =>
  ensureString(value || '', '')
    .trim()
    .replace(/[^a-zA-Z0-9_-]/g, '')
    .slice(0, 120) || '';

const normalizeWhatsAppApiMessageId = (value = '') =>
  ensureString(value || '', '')
    .trim()
    .replace(/[\/\\]/g, '_')
    .slice(0, 180) || '';

const normalizeWhatsAppApiPhoneDigits = (value = '') =>
  (normalizePhone(value || '') || '')
    .replace(/\D/g, '')
    .slice(0, 20);

const resolveWhatsAppApiConversationId = ({
  conversationId = '',
  phone = '',
  fallback = '',
} = {}) => {
  const phoneDigits = normalizeWhatsAppApiPhoneDigits(phone);
  if (phoneDigits) return `p_${phoneDigits}`;
  const normalizedConversationId = normalizeWhatsAppApiConversationId(conversationId);
  if (normalizedConversationId) return normalizedConversationId;
  const fallbackId = normalizeWhatsAppApiConversationId(fallback);
  if (fallbackId) return fallbackId;
  return `c_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
};

const getWhatsAppApiConversationsCollection = () => {
  if (!db) return null;
  try {
    return db.collection(WHATSAPP_API_CONVERSATIONS_COLLECTION);
  } catch (err) {
    console.error('Failed to access whatsapp_api_conversations collection', err);
    return null;
  }
};

const getWhatsAppApiMessagesCollection = (conversationId = '') => {
  const conversationsCollection = getWhatsAppApiConversationsCollection();
  const normalizedConversationId = normalizeWhatsAppApiConversationId(conversationId);
  if (!conversationsCollection || !normalizedConversationId) return null;
  try {
    return conversationsCollection.doc(normalizedConversationId).collection('messages');
  } catch (err) {
    console.error('Failed to access whatsapp_api_conversations messages subcollection', err);
    return null;
  }
};

const normalizeWhatsAppApiConversationDoc = (doc) => {
  if (!doc) return null;
  const data = doc.data() || {};
  const latestMessageAt = parseReportTimestamp(data.latestMessageAt || data.updatedAt || data.createdAt || null, null);
  const createdAt = parseReportTimestamp(data.createdAt || data.updatedAt || latestMessageAt || null, null);
  const updatedAt = parseReportTimestamp(data.updatedAt || latestMessageAt || createdAt || null, null);
  const unreadCountRaw = Number(data.unreadCount);
  const unreadCount = Number.isFinite(unreadCountRaw) ? Math.max(0, Math.round(unreadCountRaw)) : 0;
  const phone = normalizePhone(data.phone || '') || null;
  const phoneDigits = normalizeWhatsAppApiPhoneDigits(phone || data.phoneDigits || '');
  const pinnedAt = parseReportTimestamp(data.pinnedAt || null, null);
  const deletedAt = parseReportTimestamp(data.deletedAt || null, null);
  return {
    id: doc.id,
    phone,
    phoneDigits: phoneDigits || null,
    contactName: ensureString(data.contactName || '', '').trim() || 'Contato',
    latestMessageText: ensureLongString(data.latestMessageText || '', '', 4000).trim() || '',
    latestMessageAt,
    createdAt,
    updatedAt,
    unreadCount,
    pinnedAt,
    deletedAt,
    mergedIds: [doc.id],
    source: ensureString(data.source || 'meta_api', '').trim() || 'meta_api',
  };
};

const normalizeWhatsAppApiMessageDoc = (doc) => {
  if (!doc) return null;
  const data = doc.data() || {};
  const ts = parseReportTimestamp(data.ts || data.createdAt || data.updatedAt || null, Date.now());
  return {
    id: ensureString(data.id || doc.id || '', '').trim() || doc.id,
    conversationId:
      ensureString(data.conversationId || '', '').trim() || normalizeWhatsAppApiConversationId(doc.ref?.parent?.parent?.id || ''),
    from: ensureString(data.from || 'client', '').trim() || 'client',
    direction: ensureString(data.direction || '', '').trim() || null,
    type: ensureString(data.type || 'text', '').trim() || 'text',
    text: ensureLongString(data.text || '', '', 3900),
    status: ensureString(data.status || '', '').trim() || '',
    providerMessageId: ensureString(data.providerMessageId || '', '').trim() || null,
    templateName: ensureString(data.templateName || '', '').trim() || null,
    ts,
  };
};

const normalizeWhatsAppApiConversationIds = (values = []) => {
  const unique = new Set();
  ensureArray(values).forEach((value) => {
    const normalized = normalizeWhatsAppApiConversationId(value || '');
    if (normalized) unique.add(normalized);
  });
  return Array.from(unique);
};

const mergeWhatsAppApiConversations = (conversations = []) => {
  const grouped = new Map();

  ensureArray(conversations).forEach((conversation) => {
    if (!conversation || conversation.deletedAt) return;
    const key = conversation.phoneDigits ? `phone:${conversation.phoneDigits}` : `id:${conversation.id}`;
    const current = grouped.get(key);
    if (!current) {
      grouped.set(key, {
        ...conversation,
        mergedIds: normalizeWhatsAppApiConversationIds(conversation.mergedIds || [conversation.id]),
      });
      return;
    }

    const currentIsCanonical = current.phoneDigits && current.id === `p_${current.phoneDigits}`;
    const incomingIsCanonical = conversation.phoneDigits && conversation.id === `p_${conversation.phoneDigits}`;
    const currentLatest = parseReportTimestamp(current.latestMessageAt || current.updatedAt || 0, 0);
    const incomingLatest = parseReportTimestamp(conversation.latestMessageAt || conversation.updatedAt || 0, 0);
    const incomingIsNewer = incomingLatest > currentLatest;
    const latest = incomingIsNewer ? conversation : current;
    grouped.set(key, {
      ...current,
      id: incomingIsCanonical && !currentIsCanonical ? conversation.id : !currentIsCanonical && incomingIsNewer ? conversation.id : current.id,
      phone: current.phone || conversation.phone || null,
      phoneDigits: current.phoneDigits || conversation.phoneDigits || null,
      contactName:
        ensureString(latest.contactName || '', '').trim() && latest.contactName !== 'Contato'
          ? latest.contactName
          : current.contactName || conversation.contactName || 'Contato',
      latestMessageText: latest.latestMessageText || current.latestMessageText || conversation.latestMessageText || '',
      latestMessageAt: Math.max(currentLatest, incomingLatest),
      updatedAt: Math.max(
        parseReportTimestamp(current.updatedAt || currentLatest || 0, 0),
        parseReportTimestamp(conversation.updatedAt || incomingLatest || 0, 0)
      ),
      unreadCount: Math.max(0, Number(current.unreadCount || 0)) + Math.max(0, Number(conversation.unreadCount || 0)),
      pinnedAt: Math.max(parseReportTimestamp(current.pinnedAt || 0, 0), parseReportTimestamp(conversation.pinnedAt || 0, 0)) || null,
      mergedIds: normalizeWhatsAppApiConversationIds([
        ...(current.mergedIds || []),
        ...(conversation.mergedIds || []),
        current.id,
        conversation.id,
      ]),
    });
  });

  return Array.from(grouped.values()).sort((a, b) => {
    const pinnedDiff = parseReportTimestamp(b?.pinnedAt || 0, 0) - parseReportTimestamp(a?.pinnedAt || 0, 0);
    if (pinnedDiff !== 0) return pinnedDiff;
    const left = parseReportTimestamp(b?.latestMessageAt || b?.updatedAt || 0, 0);
    const right = parseReportTimestamp(a?.latestMessageAt || a?.updatedAt || 0, 0);
    return left - right;
  });
};

const getWhatsAppApiConversationAliasDocs = async (conversationId = '') => {
  const conversationsCollection = getWhatsAppApiConversationsCollection();
  const normalizedConversationId = normalizeWhatsAppApiConversationId(conversationId);
  if (!conversationsCollection || !normalizedConversationId) return [];

  const docsById = new Map();
  const primarySnapshot = await conversationsCollection.doc(normalizedConversationId).get();
  if (primarySnapshot.exists) docsById.set(primarySnapshot.id, primarySnapshot);

  const primaryData = primarySnapshot.exists ? primarySnapshot.data() || {} : {};
  const phoneDigits = normalizeWhatsAppApiPhoneDigits(primaryData.phone || primaryData.phoneDigits || '');
  const requestedPhoneDigits = normalizedConversationId.startsWith('p_')
    ? normalizeWhatsAppApiPhoneDigits(normalizedConversationId.slice(2))
    : '';
  const resolvedPhoneDigits = phoneDigits || requestedPhoneDigits;

  if (resolvedPhoneDigits) {
    const aliasDocs = await safeGetDocs(
      conversationsCollection.where('phoneDigits', '==', resolvedPhoneDigits).limit(80),
      'whatsapp api conversation aliases'
    );
    aliasDocs.forEach((doc) => docsById.set(doc.id, doc));
    const canonicalSnapshot = await conversationsCollection.doc(`p_${resolvedPhoneDigits}`).get();
    if (canonicalSnapshot.exists) docsById.set(canonicalSnapshot.id, canonicalSnapshot);
  }

  return Array.from(docsById.values());
};

const writeWhatsAppConversationAliasPatch = async (conversationId = '', patch = {}) => {
  const aliasDocs = await getWhatsAppApiConversationAliasDocs(conversationId);
  if (!aliasDocs.length) return [];
  await Promise.all(aliasDocs.map((doc) => doc.ref.set(patch, { merge: true })));
  return aliasDocs.map((doc) => doc.id);
};

const persistWhatsAppApiMessage = async ({
  conversationId = '',
  phone = '',
  contactName = '',
  text = '',
  direction = 'outbound',
  from = 'tech',
  status = 'sent',
  type = 'text',
  ts = Date.now(),
  providerMessageId = '',
  templateName = '',
  metadata = null,
} = {}) => {
  const conversationsCollection = getWhatsAppApiConversationsCollection();
  if (!conversationsCollection) return null;

  const normalizedPhone = normalizePhone(phone || '') || null;
  const phoneDigits = normalizeWhatsAppApiPhoneDigits(normalizedPhone || '');
  const resolvedConversationId = resolveWhatsAppApiConversationId({
    conversationId,
    phone: normalizedPhone || phoneDigits,
    fallback: phoneDigits || conversationId,
  });
  if (!resolvedConversationId) return null;

  const safeTs = parseReportTimestamp(ts, Date.now()) || Date.now();
  const messageText = ensureLongString(text || '', '', 3900).trim();
  const previewText =
    messageText ||
    (templateName ? `[Template] ${ensureString(templateName || '', '').trim()}` : '[Mensagem WhatsApp]');
  const messageId =
    normalizeWhatsAppApiMessageId(providerMessageId || '') ||
    `${safeTs.toString(36)}-${Math.random().toString(36).slice(2, 9)}`;
  const safeContactName = ensureString(contactName || '', '').trim() || (normalizedPhone || 'Contato');
  const conversationRef = conversationsCollection.doc(resolvedConversationId);
  const messageRef = conversationRef.collection('messages').doc(messageId);
  const now = Date.now();

  const conversationPayload = {
    id: resolvedConversationId,
    source: 'meta_api',
    phone: normalizedPhone,
    phoneDigits: phoneDigits || null,
    contactName: safeContactName,
    latestMessageText: previewText,
    latestMessageAt: safeTs,
    updatedAt: now,
    deletedAt: admin.firestore.FieldValue.delete(),
    deletedBy: admin.firestore.FieldValue.delete(),
  };
  const unreadIncrement = direction === 'inbound' && from !== 'tech' ? 1 : 0;
  if (unreadIncrement > 0) {
    conversationPayload.unreadCount = admin.firestore.FieldValue.increment(unreadIncrement);
  }
  const messagePayload = {
    id: messageId,
    conversationId: resolvedConversationId,
    source: 'meta_api',
    phone: normalizedPhone,
    from: ensureString(from || 'tech', '').trim() || 'tech',
    direction: ensureString(direction || 'outbound', '').trim() || 'outbound',
    type: ensureString(type || 'text', '').trim() || 'text',
    text: messageText,
    status: ensureString(status || '', '').trim() || '',
    providerMessageId: ensureString(providerMessageId || '', '').trim() || null,
    templateName: ensureString(templateName || '', '').trim() || null,
    ts: safeTs,
    createdAt: now,
    updatedAt: now,
    metadata: metadata && typeof metadata === 'object' ? metadata : null,
  };

  try {
    const conversationSnapshot = await conversationRef.get();
    if (!conversationSnapshot.exists) {
      conversationPayload.createdAt = now;
      conversationPayload.unreadCount = unreadIncrement > 0 ? unreadIncrement : 0;
    }
    await conversationRef.set(
      {
        ...conversationPayload,
      },
      { merge: true }
    );
    await messageRef.set(messagePayload, { merge: true });
  } catch (error) {
    console.error('Failed to persist WhatsApp API message', error);
    return null;
  }

  return {
    id: messageId,
    conversationId: resolvedConversationId,
    phone: normalizedPhone,
    contactName: safeContactName,
    text: messageText,
    direction: messagePayload.direction,
    from: messagePayload.from,
    type: messagePayload.type,
    status: messagePayload.status,
    providerMessageId: messagePayload.providerMessageId,
    templateName: messagePayload.templateName,
    ts: safeTs,
  };
};

const resolveWhatsAppWebhookVerifyToken = () =>
  ensureLongString(process.env.WHATSAPP_WEBHOOK_VERIFY_TOKEN || '', '', 512).trim();

const resolveMetaAppSecret = () =>
  ensureLongString(process.env.META_APP_SECRET || process.env.WHATSAPP_APP_SECRET || '', '', 512).trim();

const verifyMetaWebhookSignature = (req, { requireSecret = false } = {}) => {
  const appSecret = resolveMetaAppSecret();
  if (!appSecret) {
    return {
      ok: requireSecret !== true,
      skipped: true,
      reason: 'secret_missing',
    };
  }

  const signatureHeader = ensureString(req.get('x-hub-signature-256') || '', '').trim();
  if (!signatureHeader.startsWith('sha256=')) {
    return { ok: false, reason: 'missing_signature' };
  }

  const rawBody = Buffer.isBuffer(req.rawBody)
    ? req.rawBody
    : Buffer.from(JSON.stringify(req.body || {}), 'utf8');
  const expectedSignature = `sha256=${crypto.createHmac('sha256', appSecret).update(rawBody).digest('hex')}`;
  const receivedBuffer = Buffer.from(signatureHeader, 'utf8');
  const expectedBuffer = Buffer.from(expectedSignature, 'utf8');
  if (receivedBuffer.length !== expectedBuffer.length) {
    return { ok: false, reason: 'invalid_signature' };
  }

  return {
    ok: crypto.timingSafeEqual(receivedBuffer, expectedBuffer),
    reason: 'invalid_signature',
  };
};

const resolveWhatsAppWebhookMessageText = (message = {}) => {
  const type = ensureString(message?.type || 'text', '').trim() || 'text';
  if (type === 'text') return ensureLongString(message?.text?.body || '', '', 3900).trim();
  if (type === 'button') return ensureLongString(message?.button?.text || '', '', 3900).trim();
  if (type === 'interactive') {
    return (
      ensureLongString(message?.interactive?.button_reply?.title || '', '', 3900).trim() ||
      ensureLongString(message?.interactive?.list_reply?.title || '', '', 3900).trim()
    );
  }
  const caption =
    ensureLongString(message?.image?.caption || '', '', 3900).trim() ||
    ensureLongString(message?.video?.caption || '', '', 3900).trim() ||
    ensureLongString(message?.document?.caption || '', '', 3900).trim();
  if (caption) return caption;
  return `Mensagem WhatsApp (${type})`;
};

const extractWhatsAppWebhookMessages = (payload = {}) => {
  const entries = ensureArray(payload?.entry);
  const items = [];

  for (const entry of entries) {
    const changes = ensureArray(entry?.changes);
    for (const change of changes) {
      if (ensureString(change?.field || '', '').trim() !== 'messages') continue;
      const value = change?.value || {};
      const contacts = new Map(
        ensureArray(value?.contacts)
          .map((contact) => [ensureString(contact?.wa_id || '', '').trim(), contact])
          .filter(([waId]) => Boolean(waId))
      );

      for (const message of ensureArray(value?.messages)) {
        const fromDigits = ensureString(message?.from || '', '').replace(/\D/g, '');
        const contact = contacts.get(fromDigits) || null;
        const timestampSeconds = Number(message?.timestamp || 0);
        items.push({
          phone: normalizePhone(fromDigits || message?.from || ''),
          contactName:
            ensureString(contact?.profile?.name || '', '').trim() ||
            ensureString(message?.profile?.name || '', '').trim() ||
            normalizePhone(fromDigits || message?.from || '') ||
            'Contato',
          text: resolveWhatsAppWebhookMessageText(message),
          type: ensureString(message?.type || 'text', '').trim() || 'text',
          providerMessageId: ensureString(message?.id || '', '').trim(),
          ts: Number.isFinite(timestampSeconds) && timestampSeconds > 0 ? timestampSeconds * 1000 : Date.now(),
          metadata: {
            origin: 'meta_webhook',
            entryId: ensureString(entry?.id || '', '').trim() || null,
            phoneNumberId: ensureString(value?.metadata?.phone_number_id || '', '').trim() || null,
            displayPhoneNumber: ensureString(value?.metadata?.display_phone_number || '', '').trim() || null,
            waId: fromDigits || null,
          },
        });
      }
    }
  }

  return items.filter((item) => item.phone && item.providerMessageId);
};

const buildQueueAlertEmailText = ({
  techName = 'Tecnico',
  waitMinutes = 0,
  requestId = '',
  clientName = 'Cliente',
  deviceModel = 'Nao informado',
  platform = 'Nao informado',
} = {}) => {
  return [
    'Alerta de atendimento pendente',
    '',
    `O cliente ${clientName} esta aguardando atendimento ha ${waitMinutes} minutos na fila da SuporteX.`,
    '',
    `Tecnico: ${techName}`,
    `Sessao: ${requestId || 'sem_id'}`,
    `Aparelho: ${deviceModel}`,
    `Plataforma: ${platform}`,
    '',
    'Acesse o painel e realize esse atendimento assim que possivel.',
  ].join('\n');
};

const sendQueueAlertViaEmail = async ({
  techEmail = null,
  techName = 'Tecnico',
  waitMinutes = 0,
  requestId = '',
  clientName = 'Cliente',
  deviceModel = 'Nao informado',
  platform = 'Nao informado',
  config = null,
} = {}) => {
  if (!config?.email?.enabled) {
    return { channel: 'email', status: 'skipped', reason: 'not_configured' };
  }

  const recipient = config.email.forceRecipient || normalizeEmail(techEmail || '');
  if (!recipient) {
    return { channel: 'email', status: 'skipped', reason: 'missing_recipient' };
  }

  const text = buildQueueAlertEmailText({
    techName,
    waitMinutes,
    requestId,
    clientName,
    deviceModel,
    platform,
  });
  const html = [
    '<!doctype html>',
    '<html lang="pt-BR">',
    '<body style="font-family:Arial,Helvetica,sans-serif;background:#f6f8fb;padding:16px;color:#0f172a;">',
    '<div style="max-width:640px;margin:0 auto;background:#ffffff;border:1px solid #dbe4ff;border-radius:10px;padding:16px;">',
    '<h2 style="margin-top:0;margin-bottom:8px;">Alerta de atendimento pendente</h2>',
    `<p style="margin:0 0 10px 0;">Cliente <strong>${escapeHtmlForEmail(clientName)}</strong> aguardando ha <strong>${Number(waitMinutes) || 0} min</strong>.</p>`,
    `<p style="margin:0 0 6px 0;">Tecnico: <strong>${escapeHtmlForEmail(techName)}</strong></p>`,
    `<p style="margin:0 0 6px 0;">Sessao: <strong>${escapeHtmlForEmail(requestId || 'sem_id')}</strong></p>`,
    `<p style="margin:0 0 6px 0;">Aparelho: <strong>${escapeHtmlForEmail(deviceModel)}</strong></p>`,
    `<p style="margin:0 0 6px 0;">Plataforma: <strong>${escapeHtmlForEmail(platform)}</strong></p>`,
    '<p style="margin:12px 0 0 0;">Acesse o painel e realize esse atendimento assim que possivel.</p>',
    '</div>',
    '</body>',
    '</html>',
  ].join('');
  const subject = `[Fila SuporteX] ${clientName} aguardando ha ${waitMinutes} min`;

  return sendSupportReportViaEmail({
    toEmail: recipient,
    subject,
    text,
    html,
    config: config.email,
  });
};

const runQueueAlertSweep = async () => {
  if (queueAlertSweepInProgress) return;
  queueAlertSweepInProgress = true;

  try {
    const requestsCollection = getRequestsCollection();
    if (!requestsCollection) return;

    const config = resolveQueueAlertChannelConfig();
    if (!config.enabled) return;

    const recipients = await listQueueAlertRecipients({
      requireWhatsApp: config.whatsappEnabled,
      requireEmail: config.email?.enabled === true,
    });
    if (!recipients.length) return;

    const snapshot = await requestsCollection.where('state', '==', 'queued').get();
    if (snapshot.empty) return;

    const now = Date.now();
    for (const requestDoc of snapshot.docs) {
      const data = requestDoc.data() || {};
      const createdAt = toMillis(data.createdAt, null);
      if (!Number.isFinite(createdAt)) continue;

      const waitMinutes = Math.floor(Math.max(0, now - createdAt) / 60000);
      const thresholdMinutes = resolveQueueAlertThresholdMinutes(waitMinutes);
      const emailThresholdMinutes = resolveQueueAlertEmailThresholdMinutes(waitMinutes, config);
      if (!thresholdMinutes) continue;

      const sentRoot = data.queueAlerting?.sent && typeof data.queueAlerting.sent === 'object' ? data.queueAlerting.sent : {};
      const sentForThreshold =
        sentRoot[String(thresholdMinutes)] && typeof sentRoot[String(thresholdMinutes)] === 'object'
          ? sentRoot[String(thresholdMinutes)]
          : {};
      const emailSentRoot =
        data.queueAlerting?.emailSent && typeof data.queueAlerting.emailSent === 'object' ? data.queueAlerting.emailSent : {};
      const emailSentForThreshold =
        emailThresholdMinutes != null &&
        emailSentRoot[String(emailThresholdMinutes)] &&
        typeof emailSentRoot[String(emailThresholdMinutes)] === 'object'
          ? emailSentRoot[String(emailThresholdMinutes)]
          : {};

      const clientName = ensureString(data.clientName || 'Cliente sem nome', '').trim() || 'Cliente sem nome';
      const modelLabel = ensureString([data.brand, data.model].filter(Boolean).join(' '), '').trim() || 'Nao informado';
      const platformLabel = ensureString(
        data.platform || (data.osVersion ? `Android ${ensureString(data.osVersion || '', '').trim()}` : ''),
        ''
      ).trim() || 'Nao informado';

      const updates = {};
      let sentCount = 0;

      for (const recipient of recipients) {
        if (!recipient?.key) continue;
        const requestId = ensureString(requestDoc.id || data.requestId || '', '').trim() || 'sem_id';

        if (config.whatsappEnabled && !sentForThreshold[recipient.key]) {
          const result = await sendQueueAlertViaWhatsApp({
            techPhone: recipient.phone,
            techName: recipient.name,
            waitMinutes: thresholdMinutes,
            requestId,
            clientName,
            deviceModel: modelLabel,
            platform: platformLabel,
            config,
          });

          if (result.status === 'sent') {
            sentCount += 1;
            updates[`queueAlerting.sent.${thresholdMinutes}.${recipient.key}`] = {
              techUid: recipient.uid,
              techName: recipient.name || null,
              phone: recipient.phone || null,
              sentAt: now,
              waitMinutes: thresholdMinutes,
              providerMessageId: result.providerMessageId || null,
            };
          } else if (result.reason !== 'missing_recipient' && result.reason !== 'not_configured') {
            console.error(
              '[queue-alert] Falha ao enviar alerta WhatsApp',
              requestDoc.id,
              recipient.uid,
              result.reason || result.status
            );
          }
        }

        if (config.email?.enabled && emailThresholdMinutes != null && !emailSentForThreshold[recipient.key]) {
          const emailResult = await sendQueueAlertViaEmail({
            techEmail: recipient.email,
            techName: recipient.name,
            waitMinutes: emailThresholdMinutes,
            requestId,
            clientName,
            deviceModel: modelLabel,
            platform: platformLabel,
            config,
          });
          if (emailResult.status === 'sent') {
            sentCount += 1;
            updates[`queueAlerting.emailSent.${emailThresholdMinutes}.${recipient.key}`] = {
              techUid: recipient.uid,
              techName: recipient.name || null,
              email: recipient.email || null,
              sentAt: now,
              waitMinutes: emailThresholdMinutes,
              providerMessageId: emailResult.providerMessageId || null,
            };
          } else if (emailResult.reason !== 'missing_recipient' && emailResult.reason !== 'not_configured') {
            console.error(
              '[queue-alert] Falha ao enviar alerta e-mail de backup',
              requestDoc.id,
              recipient.uid,
              emailResult.reason || emailResult.status
            );
          }
        }
      }

      if (!sentCount) continue;

      updates['queueAlerting.lastThresholdProcessed'] = thresholdMinutes;
      updates['queueAlerting.updatedAt'] = now;
      try {
        await requestDoc.ref.update(updates);
      } catch (error) {
        console.error('[queue-alert] Falha ao persistir trilha de alertas enviados', requestDoc.id, error);
      }
    }
  } catch (error) {
    console.error('[queue-alert] Sweep failure', error);
  } finally {
    queueAlertSweepInProgress = false;
  }
};

const startQueueAlertScheduler = () => {
  if (queueAlertSweepTimer) return;
  const config = resolveQueueAlertChannelConfig();
  if (!config.enabled) {
    console.warn('[queue-alert] Scheduler desabilitado: configure WhatsApp ou e-mail (backup) e QUEUE_ALERTS_ENABLED.');
    return;
  }

  queueAlertSweepTimer = setInterval(() => {
    void runQueueAlertSweep();
  }, QUEUE_ALERT_SWEEP_INTERVAL_MS);
  if (typeof queueAlertSweepTimer.unref === 'function') {
    queueAlertSweepTimer.unref();
  }

  setTimeout(() => {
    void runQueueAlertSweep();
  }, 8_000);

  const channelLabels = [
    config.whatsappEnabled ? 'whatsapp' : null,
    config.email?.enabled ? `email>=${Math.max(5, ensureInteger(config.email.thresholdMinutes, 15))}min` : null,
  ]
    .filter(Boolean)
    .join(',');

  console.log(
    `[queue-alert] Scheduler ativo (${QUEUE_ALERT_SWEEP_INTERVAL_MS}ms, canais=${channelLabels || 'nenhum'}, template=${config.templateName}, idioma=${config.templateLanguage})`
  );
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

const formatDatePtBr = (timestamp = Date.now(), fallback = 'Nao informado') => {
  const millis = Number(timestamp);
  if (!Number.isFinite(millis) || millis <= 0) return fallback;
  try {
    return new Date(millis).toLocaleDateString('pt-BR', { timeZone: SUPPORT_REPORT_TIMEZONE });
  } catch (_error) {
    return fallback;
  }
};

const formatTimePtBr = (timestamp = Date.now(), fallback = 'Nao informado') => {
  const millis = Number(timestamp);
  if (!Number.isFinite(millis) || millis <= 0) return fallback;
  try {
    return new Date(millis).toLocaleTimeString('pt-BR', { timeZone: SUPPORT_REPORT_TIMEZONE });
  } catch (_error) {
    return fallback;
  }
};

const resolveTransactionalEmailConfig = ({ fromEnv = '', replyToEnv = '' } = {}) => {
  const apiKey = ensureLongString(
    process.env.RESEND_API_KEY || process.env.SUPPORT_REPORT_EMAIL_API_KEY || '',
    '',
    4096
  ).trim();
  const from =
    ensureString(
      process.env[fromEnv] ||
        process.env.SUPPORT_REPORT_EMAIL_FROM ||
        process.env.RESEND_FROM_EMAIL ||
        'Suporte X <no-reply@xavierassessoriadigital.com.br>',
      ''
    ).trim() || 'Suporte X <no-reply@xavierassessoriadigital.com.br>';
  return {
    enabled: Boolean(apiKey),
    apiKey,
    from,
    replyTo: normalizeEmail(process.env[replyToEnv] || process.env.SUPPORT_REPORT_EMAIL_REPLY_TO || ''),
  };
};

const resolveGenericWhatsAppTemplateConfig = ({
  templateNameEnv = '',
  templateNameDefault = '',
  languageEnv = '',
  bodyParamNamesEnv = '',
  bodyParamNamesDefault = [],
  useNamedParamsEnv = '',
  forceToEnv = '',
} = {}) => {
  const metaConfig = resolveMetaWhatsAppApiConfig();
  return {
    enabled: metaConfig.enabled,
    token: metaConfig.token,
    phoneNumberId: metaConfig.phoneNumberId,
    apiVersion: metaConfig.apiVersion,
    templateName: ensureString(process.env[templateNameEnv] || templateNameDefault, '').trim() || templateNameDefault,
    templateLanguage: ensureString(process.env[languageEnv] || 'pt_BR', '').trim() || 'pt_BR',
    templateBodyParamNames: parseWhatsAppTemplateParamNames(process.env[bodyParamNamesEnv] || '', bodyParamNamesDefault),
    templateUseNamedParams: ensureBoolean(process.env[useNamedParamsEnv], false),
    forceRecipient: normalizePhone(process.env[forceToEnv] || ''),
  };
};

const sendGenericWhatsAppTemplateMessage = async ({
  toPhone = '',
  contactName = 'Cliente',
  fallbackText = '',
  values = [],
  copyCodeButtonValue = '',
  config = null,
  origin = 'system',
} = {}) => {
  if (!config?.enabled) {
    return { channel: 'whatsapp', status: 'skipped', reason: 'not_configured' };
  }
  const target = config.forceRecipient || normalizePhone(toPhone || '');
  if (!target) {
    return { channel: 'whatsapp', status: 'skipped', reason: 'missing_recipient' };
  }
  const templateName = ensureString(config.templateName || '', '').trim();
  if (!templateName) {
    return { channel: 'whatsapp', status: 'skipped', reason: 'missing_template' };
  }
  const endpoint = `https://graph.facebook.com/${encodeURIComponent(config.apiVersion)}/${encodeURIComponent(
    config.phoneNumberId
  )}/messages`;
  const paramNames = ensureArray(config.templateBodyParamNames)
    .map((value) => normalizeWhatsAppTemplateParamName(value))
    .filter(Boolean)
    .slice(0, values.length);
  const useNamedParams = ensureBoolean(config.templateUseNamedParams, false) && paramNames.length === values.length;
  const parameters = values.map((entry, index) =>
    buildWhatsAppTemplateTextParameter(entry?.text || '', useNamedParams ? paramNames[index] : '', {
      allowFormatting: entry?.allowFormatting === true,
      maxLength: Math.max(1, ensureInteger(entry?.maxLength, 256)),
    })
  );
  const components = [
    {
      type: 'body',
      parameters,
    },
  ];
  const safeCopyCode = sanitizeWhatsAppTemplateTextParameter(copyCodeButtonValue || '', { fallback: '', maxLength: 32 });
  if (safeCopyCode) {
    components.push({
      type: 'button',
      sub_type: 'url',
      index: '0',
      parameters: [
        {
          type: 'text',
          text: safeCopyCode,
        },
      ],
    });
  }
  const bodyPayload = {
    messaging_product: 'whatsapp',
    recipient_type: 'individual',
    to: target.replace(/\D/g, ''),
    type: 'template',
    template: {
      name: templateName,
      language: { code: ensureString(config.templateLanguage || 'pt_BR', '').trim() || 'pt_BR' },
      components,
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
      const providerErrorMessage = ensureLongString(payload?.error?.message || '', '', 512).trim();
      const providerErrorDetails = ensureLongString(payload?.error?.error_data?.details || '', '', 512).trim();
      const statusFallback = ensureString(response.statusText || '', '').trim() || 'provider_error';
      const reason = providerErrorMessage || statusFallback;
      return {
        channel: 'whatsapp',
        status: 'error',
        reason: providerErrorDetails ? `${reason} | details: ${providerErrorDetails}` : reason,
        statusCode: response.status || 500,
      };
    }
    const providerMessageId = ensureString(payload?.messages?.[0]?.id || '', '').trim() || null;
    await persistWhatsAppApiMessage({
      phone: target,
      contactName: contactName || 'Cliente',
      text: fallbackText,
      direction: 'outbound',
      from: 'system',
      status: 'sent',
      type: 'template',
      ts: Date.now(),
      providerMessageId,
      templateName,
      metadata: {
        origin,
        channel: 'whatsapp',
      },
    });
    return { channel: 'whatsapp', status: 'sent', recipient: target, providerMessageId, templateName };
  } catch (error) {
    return { channel: 'whatsapp', status: 'error', reason: ensureString(error?.message || '', 'provider_error') };
  }
};

const buildCreditAddedNotificationPayload = ({ client = {}, creditChange = {} } = {}) => {
  const now = Date.now();
  const added = Math.max(0, ensureInteger(creditChange.appliedDelta, 0));
  const balance = Math.max(0, ensureInteger(creditChange.credits ?? client.credits, 0));
  const clientName = ensureString(client.name || creditChange.clientName || '', '').trim() || 'Cliente';
  const clientPhone = normalizePhone(client.phone || creditChange.clientPhone || '') || null;
  const clientEmail = normalizeEmail(client.primaryEmail || client.email || creditChange.clientEmail || '');
  const date = formatDatePtBr(now);
  const time = formatTimePtBr(now);
  return {
    clientName,
    clientPhone,
    clientEmail,
    added,
    balance,
    date,
    time,
    dateTime: `${date}, ${time}`,
    text: [
      'CREDITOS ADICIONADOS',
      `Ola, ${clientName}!`,
      '',
      `Informamos que foram adicionados ${added} novos creditos seus, para usar no aplicativo Suporte X.`,
      '',
      `Data: ${date}, ${time}`,
      `Creditos adicionados: ${added}`,
      `Saldo atual: ${balance} creditos`,
      'Mensagem automatica do sistema Suporte X.',
    ].join('\n'),
  };
};

const buildCreditAddedEmailHtml = (payload = {}) => {
  const safeClientName = escapeHtmlForEmail(payload.clientName || 'Cliente');
  const safeAdded = escapeHtmlForEmail(String(payload.added ?? 0));
  const safeBalance = escapeHtmlForEmail(String(payload.balance ?? 0));
  const safeDateTime = escapeHtmlForEmail(payload.dateTime || '');
  return [
    '<!doctype html><html><body style="margin:0;background:#f8fafc;font-family:Arial,sans-serif;color:#0f172a;">',
    '<div style="max-width:560px;margin:0 auto;padding:28px 18px;">',
    '<div style="background:#ffffff;border:1px solid #e2e8f0;border-radius:12px;padding:24px;">',
    '<h1 style="margin:0 0 16px 0;font-size:22px;line-height:1.2;color:#0f172a;">Creditos adicionados</h1>',
    `<p style="margin:0 0 14px 0;font-size:16px;line-height:1.5;">Ola, <strong>${safeClientName}</strong>!</p>`,
    `<p style="margin:0 0 18px 0;font-size:15px;line-height:1.5;">Foram adicionados <strong>${safeAdded}</strong> novos creditos na sua conta do aplicativo Suporte X.</p>`,
    '<div style="background:#f1f5f9;border-radius:10px;padding:16px;margin:0 0 18px 0;">',
    `<p style="margin:0 0 8px 0;font-size:14px;"><strong>Data:</strong> ${safeDateTime}</p>`,
    `<p style="margin:0 0 8px 0;font-size:14px;"><strong>Creditos adicionados:</strong> ${safeAdded}</p>`,
    `<p style="margin:0;font-size:14px;"><strong>Saldo atual:</strong> ${safeBalance} creditos</p>`,
    '</div>',
    '<p style="margin:0;font-size:13px;line-height:1.45;color:#64748b;">Mensagem automatica do sistema Suporte X.</p>',
    '</div></div></body></html>',
  ].join('');
};

const dispatchClientCreditAddedNotification = async ({ client = {}, creditChange = {} } = {}) => {
  const payload = buildCreditAddedNotificationPayload({ client, creditChange });
  const whatsappConfig = resolveGenericWhatsAppTemplateConfig({
    templateNameEnv: 'CREDIT_ADDED_WHATSAPP_TEMPLATE_NAME',
    templateNameDefault: 'creditos_adicionados',
    languageEnv: 'CREDIT_ADDED_WHATSAPP_TEMPLATE_LANGUAGE',
    bodyParamNamesEnv: 'CREDIT_ADDED_WHATSAPP_TEMPLATE_BODY_PARAM_NAMES',
    bodyParamNamesDefault: ['nome_do_cliente', 'creditos_texto', 'data', 'hora', 'creditos_adicionados', 'saldo_atual'],
    useNamedParamsEnv: 'CREDIT_ADDED_WHATSAPP_TEMPLATE_USE_NAMED_PARAMS',
    forceToEnv: 'CREDIT_ADDED_WHATSAPP_FORCE_TO',
  });
  const emailConfig = resolveTransactionalEmailConfig({
    fromEnv: 'CREDIT_ADDED_EMAIL_FROM',
    replyToEnv: 'CREDIT_ADDED_EMAIL_REPLY_TO',
  });
  const channels = await Promise.all([
    sendGenericWhatsAppTemplateMessage({
      toPhone: payload.clientPhone,
      contactName: payload.clientName,
      fallbackText: payload.text,
      origin: 'credit_added',
      config: whatsappConfig,
      values: [
        { text: payload.clientName, maxLength: 80 },
        { text: String(payload.added), maxLength: 12 },
        { text: payload.date, maxLength: 24 },
        { text: payload.time, maxLength: 24 },
        { text: String(payload.added), maxLength: 12 },
        { text: String(payload.balance), maxLength: 12 },
      ],
    }),
    sendSupportReportViaEmail({
      toEmail: payload.clientEmail,
      subject: `Suporte X - ${payload.added} credito${payload.added === 1 ? '' : 's'} adicionado${payload.added === 1 ? '' : 's'}`,
      text: payload.text,
      html: buildCreditAddedEmailHtml(payload),
      config: emailConfig,
    }),
  ]);
  const sent = channels.some((channel) => channel.status === 'sent');
  return {
    sent,
    status: sent ? 'sent' : 'error',
    reason: sent ? null : 'all_channels_failed',
    channels,
    dispatchedAt: Date.now(),
  };
};

const hashClientManualVerificationCode = ({ clientId = '', phone = '', code = '', salt = '' } = {}) =>
  crypto
    .createHash('sha256')
    .update([clientId, normalizePhone(phone || '') || '', ensureString(code || '', '').trim(), salt].join(':'))
    .digest('hex');

const validateClientManualVerificationCode = ({ clientId = '', phone = '', code = '', manualCode = null } = {}) => {
  if (!manualCode || typeof manualCode !== 'object') {
    return { ok: false, status: 400, error: 'verification_code_missing' };
  }
  const expiresAt = Number(manualCode.expiresAt || 0);
  if (!Number.isFinite(expiresAt) || expiresAt < Date.now()) {
    return { ok: false, status: 400, error: 'verification_code_expired' };
  }
  const attempts = Math.max(0, ensureInteger(manualCode.attempts, 0));
  const maxAttempts = Math.max(1, ensureInteger(manualCode.maxAttempts, CLIENT_MANUAL_VERIFICATION_MAX_ATTEMPTS));
  if (attempts >= maxAttempts) {
    return { ok: false, status: 429, error: 'verification_code_attempts_exceeded' };
  }
  const expectedPhone = normalizePhone(manualCode.phone || '');
  const receivedPhone = normalizePhone(phone || '');
  if (expectedPhone && receivedPhone && expectedPhone !== receivedPhone) {
    return { ok: false, status: 409, error: 'verification_phone_mismatch' };
  }
  const expectedHash = ensureString(manualCode.codeHash || '', '').trim();
  const salt = ensureString(manualCode.salt || '', '').trim();
  const receivedHash = hashClientManualVerificationCode({ clientId, phone: expectedPhone || receivedPhone || '', code, salt });
  if (!expectedHash || !salt || receivedHash !== expectedHash) {
    return { ok: false, status: 400, error: 'invalid_verification_code' };
  }
  return { ok: true };
};

const buildManualVerificationCodePayload = ({ client = {}, code = '' } = {}) => {
  const clientName = ensureString(client.name || '', '').trim() || 'Cliente';
  const clientPhone = normalizePhone(client.phone || '') || null;
  const clientEmail = normalizeEmail(client.primaryEmail || client.email || '');
  const ttlMinutes = Math.max(1, Math.round(CLIENT_MANUAL_VERIFICATION_CODE_TTL_MS / 60_000));
  const text = `Seu codigo de verificacao Suporte X e ${code}. Ele vale por ${ttlMinutes} minutos. Para sua seguranca, nao compartilhe.`;
  return { clientName, clientPhone, clientEmail, code, ttlMinutes, text };
};

const buildManualVerificationCodeEmailHtml = (payload = {}) => [
  '<!doctype html><html><body style="margin:0;background:#f8fafc;font-family:Arial,sans-serif;color:#0f172a;">',
  '<div style="max-width:520px;margin:0 auto;padding:28px 18px;">',
  '<div style="background:#ffffff;border:1px solid #e2e8f0;border-radius:12px;padding:24px;">',
  '<h1 style="margin:0 0 14px 0;font-size:21px;">Codigo de verificacao</h1>',
  `<p style="margin:0 0 16px 0;font-size:15px;line-height:1.5;">Ola, ${escapeHtmlForEmail(payload.clientName || 'Cliente')}.</p>`,
  `<p style="margin:0 0 16px 0;font-size:15px;line-height:1.5;">Use o codigo abaixo para confirmar seu cadastro no Suporte X. Ele vale por ${escapeHtmlForEmail(String(payload.ttlMinutes || 10))} minutos.</p>`,
  `<div style="font-size:28px;letter-spacing:6px;font-weight:700;text-align:center;background:#f1f5f9;border-radius:10px;padding:18px;margin:0 0 16px 0;">${escapeHtmlForEmail(payload.code || '')}</div>`,
  '<p style="margin:0;font-size:13px;line-height:1.45;color:#64748b;">Para sua seguranca, nao compartilhe este codigo.</p>',
  '</div></div></body></html>',
].join('');

const dispatchClientManualVerificationCode = async ({ client = {}, code = '' } = {}) => {
  const payload = buildManualVerificationCodePayload({ client, code });
  const whatsappConfig = resolveGenericWhatsAppTemplateConfig({
    templateNameEnv: 'CLIENT_VERIFICATION_WHATSAPP_TEMPLATE_NAME',
    templateNameDefault: 'codigo_de_verificacao',
    languageEnv: 'CLIENT_VERIFICATION_WHATSAPP_TEMPLATE_LANGUAGE',
    bodyParamNamesEnv: 'CLIENT_VERIFICATION_WHATSAPP_TEMPLATE_BODY_PARAM_NAMES',
    bodyParamNamesDefault: ['codigo'],
    useNamedParamsEnv: 'CLIENT_VERIFICATION_WHATSAPP_TEMPLATE_USE_NAMED_PARAMS',
    forceToEnv: 'CLIENT_VERIFICATION_WHATSAPP_FORCE_TO',
  });
  const emailConfig = resolveTransactionalEmailConfig({
    fromEnv: 'CLIENT_VERIFICATION_EMAIL_FROM',
    replyToEnv: 'CLIENT_VERIFICATION_EMAIL_REPLY_TO',
  });
  const channels = await Promise.all([
    sendGenericWhatsAppTemplateMessage({
      toPhone: payload.clientPhone,
      contactName: payload.clientName,
      fallbackText: payload.text,
      origin: 'client_manual_verification',
      config: whatsappConfig,
      copyCodeButtonValue: ensureBoolean(process.env.CLIENT_VERIFICATION_WHATSAPP_COPY_CODE_BUTTON, true)
        ? payload.code
        : '',
      values: [{ text: payload.code, maxLength: 12 }],
    }),
    sendSupportReportViaEmail({
      toEmail: payload.clientEmail,
      subject: 'Suporte X - codigo de verificacao',
      text: payload.text,
      html: buildManualVerificationCodeEmailHtml(payload),
      config: emailConfig,
    }),
  ]);
  const sent = channels.some((channel) => channel.status === 'sent');
  return {
    sent,
    status: sent ? 'sent' : 'error',
    reason: sent ? null : 'all_channels_failed',
    channels,
    expiresInMs: CLIENT_MANUAL_VERIFICATION_CODE_TTL_MS,
    dispatchedAt: Date.now(),
  };
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
        report,
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

    const pageWidth = doc.page.width;
    const pageHeight = doc.page.height;
    const cardX = 36;
    const cardY = 36;
    const cardWidth = pageWidth - cardX * 2;
    const cardHeight = pageHeight - cardY * 2;
    const contentX = cardX + 24;
    const contentWidth = cardWidth - 48;
    const sectionDividerWidth = Math.min(340, contentWidth);
    const solutionItems = ensureArray(report.solutionItems).filter(Boolean);

    doc.save();
    doc.roundedRect(cardX, cardY, cardWidth, cardHeight, 10).lineWidth(1).fillAndStroke('#ffffff', '#dbe4ff');
    doc.restore();
    doc.x = contentX;
    doc.y = cardY + 24;

    const writeLine = (text = '', options = {}) => {
      doc.font(options.bold ? 'Helvetica-Bold' : 'Helvetica')
        .fontSize(options.size || 13)
        .fillColor(options.color || '#0f172a')
        .text(ensureString(text || '', ''), contentX, doc.y, {
          width: contentWidth,
          lineGap: options.lineGap == null ? 2 : options.lineGap,
          align: options.align || 'left',
          underline: Boolean(options.underline),
        });
    };

    const writeSectionDivider = () => {
      const y = doc.y + 6;
      doc.save();
      doc.strokeColor('#0f172a').lineWidth(1).moveTo(contentX, y).lineTo(contentX + sectionDividerWidth, y).stroke();
      doc.restore();
      doc.y = y + 10;
    };

    writeLine(`Relat\u00F3rio de atendimento - Sess\u00E3o ${report.sessionId || '\u2014'}`, {
      bold: true,
      size: 16,
      color: '#0b2b63',
      lineGap: 3,
    });
    doc.moveDown(0.3);
    writeLine('SEGUE O RESUMO DO SEU ATENDIMENTO REALIZADO PELA SUPORTE X', {
      bold: true,
      size: 14,
      align: 'center',
      underline: true,
      lineGap: 3,
    });
    doc.moveDown(0.5);
    writeLine(`Ol\u00E1, ${report.clientName}!`, { size: 13, lineGap: 3 });
    writeLine('Seu atendimento foi conclu\u00EDdo com sucesso. Segue o resumo do que foi realizado no seu dispositivo.', {
      size: 12,
      lineGap: 3,
    });
    writeSectionDivider();
    writeLine('DADOS DO CLIENTE', { bold: true, size: 13, lineGap: 3 });
    writeLine(`Nome: ${report.clientName}`);
    writeLine(`Telefone: ${report.clientPhoneDisplay}`);
    writeLine(`Data do atendimento: ${report.closedAtDisplay}`);
    writeLine(`T\u00E9cnico respons\u00E1vel: ${report.techName}`);
    writeSectionDivider();
    writeLine('O QUE FOI IDENTIFICADO', { bold: true, size: 13, lineGap: 3 });
    writeLine(`${report.symptom}`);
    writeLine(`Resultado: ${report.outcomeLabel}`);
    writeSectionDivider();
    writeLine('O QUE FOI FEITO', { bold: true, size: 13, lineGap: 3 });
    if (solutionItems.length) {
      solutionItems.forEach((item) => writeLine(`\u2022 ${item}`));
    } else {
      writeLine('\u2022 N\u00E3o informado');
    }
    writeSectionDivider();
    writeLine('CR\u00C9DITOS', { bold: true, size: 13, lineGap: 3 });
    writeLine(`Antes: ${report.creditsBeforeDisplay}`);
    writeLine(`Consumido: ${report.creditsConsumedDisplay}`);
    writeLine(`Depois: ${report.creditsAfterDisplay}`);
    writeSectionDivider();
    writeLine('SUPORTE', { bold: true, size: 13, lineGap: 3 });
    writeLine('Caso precise novamente, \u00E9 s\u00F3 abrir o aplicativo Suporte X e solicitar um novo atendimento.');
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

app.get('/api/whatsapp-api/webhook', (req, res) => {
  const mode = ensureString(req.query?.['hub.mode'] || '', '').trim();
  const token = ensureLongString(req.query?.['hub.verify_token'] || '', '', 512).trim();
  const challenge = ensureLongString(req.query?.['hub.challenge'] || '', '', 2048).trim();
  const expectedToken = resolveWhatsAppWebhookVerifyToken();

  if (
    mode === 'subscribe' &&
    expectedToken &&
    challenge &&
    timingSafeStringEqual(token, expectedToken)
  ) {
    return res.status(200).send(challenge);
  }

  return res.status(403).send('Forbidden');
});

app.post('/api/whatsapp-api/webhook', async (req, res) => {
  const allowUnsignedWebhook =
    !isProduction &&
    isExplicitlyEnabled(process.env.ALLOW_UNSIGNED_META_WEBHOOK);
  const signature = verifyMetaWebhookSignature(req, {
    requireSecret: !allowUnsignedWebhook,
  });
  if (!signature.ok) {
    console.warn('[whatsapp-webhook] Assinatura invalida:', signature.reason);
    if (signature.reason === 'secret_missing') {
      return res.status(503).json({ error: 'webhook_unavailable' });
    }
    return res.status(403).json({ error: 'invalid_signature' });
  }
  if (signature.skipped) {
    console.warn(
      '[whatsapp-webhook] Assinatura ignorada somente fora de producao; configure META_APP_SECRET.'
    );
  }

  const messages = extractWhatsAppWebhookMessages(req.body || {});
  if (!messages.length) {
    return res.sendStatus(200);
  }

  try {
    const persisted = [];
    for (const message of messages) {
      const saved = await persistWhatsAppApiMessage({
        phone: message.phone,
        contactName: message.contactName,
        text: message.text,
        direction: 'inbound',
        from: 'client',
        status: 'received',
        type: message.type,
        ts: message.ts,
        providerMessageId: message.providerMessageId,
        metadata: message.metadata,
      });
      if (saved) persisted.push(saved);
    }
    return res.status(200).json({ ok: true, received: messages.length, persisted: persisted.length });
  } catch (error) {
    console.error('[whatsapp-webhook] Falha ao processar mensagens recebidas', error);
    return res.sendStatus(500);
  }
});

app.get('/api/whatsapp-api/conversations', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const conversationsCollection = getWhatsAppApiConversationsCollection();
  if (!conversationsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }
  const limitRaw = Number(req.query.limit);
  const queryLimit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(Math.round(limitRaw), 400)) : 200;
  try {
    const docs = await safeGetDocs(
      conversationsCollection.orderBy('updatedAt', 'desc').limit(queryLimit),
      'whatsapp api conversations list'
    );
    const conversations = mergeWhatsAppApiConversations(
      docs
      .map((doc) => normalizeWhatsAppApiConversationDoc(doc))
      .filter(Boolean)
    );
    return res.json({
      conversations,
      meta: {
        count: conversations.length,
        source: 'meta_api',
      },
    });
  } catch (error) {
    console.error('Failed to list WhatsApp API conversations', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.get('/api/whatsapp-api/conversations/:id/messages', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const conversationsCollection = getWhatsAppApiConversationsCollection();
  if (!conversationsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }
  const conversationId = normalizeWhatsAppApiConversationId(req.params.id || '');
  if (!conversationId) {
    return res.status(400).json({ error: 'invalid_conversation_id' });
  }
  const limitRaw = Number(req.query.limit);
  const queryLimit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(Math.round(limitRaw), 500)) : 300;
  try {
    const aliasDocs = await getWhatsAppApiConversationAliasDocs(conversationId);
    if (!aliasDocs.length) {
      return res.status(404).json({ error: 'conversation_not_found' });
    }
    const conversation = mergeWhatsAppApiConversations(aliasDocs.map((doc) => normalizeWhatsAppApiConversationDoc(doc)).filter(Boolean))[0] || null;
    if (!conversation) {
      return res.status(404).json({ error: 'conversation_not_found' });
    }

    const docsById = new Map();
    await Promise.all(
      aliasDocs.map(async (aliasDoc) => {
        const messagesCollection = getWhatsAppApiMessagesCollection(aliasDoc.id);
        if (!messagesCollection) return;
        const aliasDeletedAt = parseReportTimestamp((aliasDoc.data() || {}).deletedAt || null, null);
        const docs = await safeGetDocs(
          messagesCollection.orderBy('ts', 'desc').limit(queryLimit),
          'whatsapp api conversation messages'
        );
        docs.forEach((doc) => {
          if (aliasDeletedAt) {
            const messageTs = parseReportTimestamp((doc.data() || {}).ts || null, 0);
            if (messageTs <= aliasDeletedAt) return;
          }
          docsById.set(doc.id, doc);
        });
      })
    );
    const messages = Array.from(docsById.values())
      .map((doc) => {
        const message = normalizeWhatsAppApiMessageDoc(doc);
        if (message) message.conversationId = conversation.id;
        return message;
      })
      .filter(Boolean)
      .sort((a, b) => parseReportTimestamp(a?.ts || 0, 0) - parseReportTimestamp(b?.ts || 0, 0))
      .slice(-queryLimit);
    return res.json({
      conversation,
      messages,
      meta: {
        count: messages.length,
        source: 'meta_api',
      },
    });
  } catch (error) {
    console.error('Failed to list WhatsApp API conversation messages', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/whatsapp-api/conversations/:id/read', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const conversationsCollection = getWhatsAppApiConversationsCollection();
  if (!conversationsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }
  const conversationId = normalizeWhatsAppApiConversationId(req.params.id || '');
  if (!conversationId) {
    return res.status(400).json({ error: 'invalid_conversation_id' });
  }
  try {
    const updatedIds = await writeWhatsAppConversationAliasPatch(conversationId, {
      unreadCount: 0,
      readAt: Date.now(),
      readBy: normalizeIdentifier(req.techAccess?.uid || '') || null,
    });
    if (!updatedIds.length) {
      return res.status(404).json({ error: 'conversation_not_found' });
    }
    return res.json({ ok: true, conversationId, updatedIds });
  } catch (error) {
    console.error('Failed to mark WhatsApp API conversation as read', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/whatsapp-api/conversations/:id/unread', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const conversationsCollection = getWhatsAppApiConversationsCollection();
  if (!conversationsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }
  const conversationId = normalizeWhatsAppApiConversationId(req.params.id || '');
  if (!conversationId) {
    return res.status(400).json({ error: 'invalid_conversation_id' });
  }
  try {
    const updatedIds = await writeWhatsAppConversationAliasPatch(conversationId, {
      unreadCount: Math.max(1, ensureInteger(req.body?.unreadCount, 1)),
      unreadMarkedAt: Date.now(),
      unreadMarkedBy: normalizeIdentifier(req.techAccess?.uid || '') || null,
    });
    if (!updatedIds.length) {
      return res.status(404).json({ error: 'conversation_not_found' });
    }
    return res.json({ ok: true, conversationId, updatedIds });
  } catch (error) {
    console.error('Failed to mark WhatsApp API conversation as unread', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/whatsapp-api/conversations/:id/pin', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const conversationsCollection = getWhatsAppApiConversationsCollection();
  if (!conversationsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }
  const conversationId = normalizeWhatsAppApiConversationId(req.params.id || '');
  if (!conversationId) {
    return res.status(400).json({ error: 'invalid_conversation_id' });
  }
  const pinned = ensureBoolean(req.body?.pinned, true);
  try {
    const patch = pinned
      ? {
          pinnedAt: Date.now(),
          pinnedBy: normalizeIdentifier(req.techAccess?.uid || '') || null,
        }
      : {
          pinnedAt: admin.firestore.FieldValue.delete(),
          pinnedBy: admin.firestore.FieldValue.delete(),
        };
    const updatedIds = await writeWhatsAppConversationAliasPatch(conversationId, patch);
    if (!updatedIds.length) {
      return res.status(404).json({ error: 'conversation_not_found' });
    }
    return res.json({ ok: true, conversationId, pinned, updatedIds });
  } catch (error) {
    console.error('Failed to pin WhatsApp API conversation', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.delete('/api/whatsapp-api/conversations/:id', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const conversationsCollection = getWhatsAppApiConversationsCollection();
  if (!conversationsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }
  const conversationId = normalizeWhatsAppApiConversationId(req.params.id || '');
  if (!conversationId) {
    return res.status(400).json({ error: 'invalid_conversation_id' });
  }
  try {
    const deletedAt = Date.now();
    const updatedIds = await writeWhatsAppConversationAliasPatch(conversationId, {
      deletedAt,
      deletedBy: normalizeIdentifier(req.techAccess?.uid || '') || null,
      unreadCount: 0,
    });
    if (!updatedIds.length) {
      return res.status(404).json({ error: 'conversation_not_found' });
    }
    return res.json({ ok: true, conversationId, deletedAt, updatedIds });
  } catch (error) {
    console.error('Failed to delete WhatsApp API conversation', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/whatsapp-api/conversations/:id/messages', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const conversationsCollection = getWhatsAppApiConversationsCollection();
  if (!conversationsCollection) {
    return res.status(503).json({ error: 'firestore_unavailable' });
  }
  const conversationId = normalizeWhatsAppApiConversationId(req.params.id || '');
  if (!conversationId) {
    return res.status(400).json({ error: 'invalid_conversation_id' });
  }
  const text = ensureLongString(req.body?.text || '', '', 3900).trim();
  if (!text) {
    return res.status(400).json({ error: 'missing_text' });
  }

  try {
    const conversationRef = conversationsCollection.doc(conversationId);
    const conversationSnap = await conversationRef.get();
    const conversationData = conversationSnap.exists ? conversationSnap.data() || {} : {};
    const targetPhone = normalizePhone(req.body?.phone || conversationData.phone || '');
    if (!targetPhone) {
      return res.status(400).json({ error: 'missing_recipient', message: 'Informe um telefone WhatsApp valido.' });
    }
    const contactName =
      ensureString(req.body?.contactName || conversationData.contactName || '', '').trim() || 'Contato';
    const sendResult = await sendMetaWhatsAppTextMessage({
      toPhone: targetPhone,
      text,
    });
    if (!sendResult.ok) {
      return res.status(sendResult.statusCode || 500).json({
        error: 'provider_error',
        reason: sendResult.reason || 'provider_error',
        providerError: sendResult.providerError || null,
      });
    }

    const persistedMessage = await persistWhatsAppApiMessage({
      conversationId,
      phone: targetPhone,
      contactName,
      text,
      direction: 'outbound',
      from: 'tech',
      status: 'sent',
      type: 'text',
      ts: Date.now(),
      providerMessageId: sendResult.providerMessageId || null,
      metadata: {
        origin: 'tech_panel',
        senderTechUid: normalizeIdentifier(req.techAccess?.uid || ''),
      },
    });

    if (!persistedMessage) {
      return res.status(500).json({ error: 'storage_failed' });
    }

    return res.json({
      ok: true,
      message: persistedMessage,
      conversationId: persistedMessage.conversationId,
    });
  } catch (error) {
    console.error('Failed to send WhatsApp API message from panel', error);
    return res.status(500).json({ error: 'server_error' });
  }
});

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
      Array.from(uniqueById.values()).map((doc) => buildSessionState(doc.id, { snapshot: doc, includeLogs: false }))
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

app.post('/api/sessions/:id/closure-draft', requireAuth(['tech']), requireTechAccess, async (req, res) => {
  const id = req.params.id;
  if (!getSessionsCollection()) {
    console.error('Firestore not configured. Cannot save closure draft.');
    return res.status(503).json({ error: 'firestore_unavailable' });
  }
  try {
    const snapshot = await getSessionSnapshot(id);
    if (!snapshot) {
      return res.status(404).json({ error: 'session_not_found' });
    }
    const session = snapshot.data() || {};
    const requesterUid = ensureString(
      req.techAccess?.uid || req.user?.uid || '',
      ''
    ).trim();
    if (!requesterUid || requesterUid !== getSessionTechUid(session)) {
      return res.status(403).json({ error: 'forbidden' });
    }
    const payload = req.body || {};
    const hasOwn = (key) => Object.prototype.hasOwnProperty.call(payload, key);
    const nowTs = Date.now();

    const outcomeRaw = hasOwn('outcome') ? payload.outcome : session.outcome;
    const outcome = ensureString(outcomeRaw || 'resolved', 'resolved').trim() || 'resolved';
    const symptomRaw = hasOwn('symptom') ? payload.symptom : session.symptom;
    const solutionRaw = hasOwn('solution') ? payload.solution : session.solution;

    const updates = {
      outcome,
      symptom: ensureString(symptomRaw ?? '', '').trim() || null,
      solution: ensureString(solutionRaw ?? '', '').trim() || null,
      updatedAt: nowTs,
      reportDraftUpdatedAt: nowTs,
    };

    if (hasOwn('notes')) {
      updates.notes = ensureString(payload.notes ?? '', '').trim() || null;
    }

    const technicianScoreRaw =
      typeof payload.technicianSatisfactionScore !== 'undefined' ? payload.technicianSatisfactionScore : payload.npsScore;
    if (typeof technicianScoreRaw !== 'undefined') {
      const technicianScore = Number(technicianScoreRaw);
      if (!Number.isNaN(technicianScore)) {
        const clamped = Math.max(0, Math.min(10, Math.round(technicianScore)));
        updates.technicianSatisfactionScore = clamped;
        updates.npsScore = clamped;
      }
    }

    await snapshot.ref.set(updates, { merge: true });
    await emitSessionUpdated(id);
    return res.json({ ok: true, savedAt: nowTs });
  } catch (err) {
    console.error('Failed to save closure draft', err);
    return res.status(500).json({ error: 'firestore_error' });
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
    const closerUid = ensureString(
      req.techAccess?.uid || req.user?.uid || '',
      ''
    ).trim();
    if (!closerUid || closerUid !== getSessionTechUid(session)) {
      return res.status(403).json({ error: 'forbidden' });
    }
    const payload = req.body || {};
    const skipClientReportDispatch = ensureBoolean(payload.skipClientReportDispatch, false);
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
      const supportFinalization = await finalizeSupportSessionFromRealtime({
        realtimeSessionId: id,
        realtimeSession: {
          ...session,
          ...reportUpdates,
          sessionId: id,
          status: 'closed',
          closedAt: session.closedAt || nowTs,
        },
        actorUid: closerUid,
        actorRole: 'tech',
        authorizedTech: true,
        summary: {
          problemSummary: reportUpdates.symptom,
          solutionSummary: reportUpdates.solution,
          ...(typeof reportUpdates.notes !== 'undefined'
            ? { internalNotes: reportUpdates.notes }
            : {}),
        },
        now: Number(session.closedAt || nowTs),
      });
      await emitSessionUpdated(id);
      const reportDispatch = skipClientReportDispatch
        ? { status: 'skipped', reason: 'manual_only' }
        : await dispatchClientSupportReportForSession({
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
      return res.json({
        ok: true,
        alreadyClosed: true,
        reportDispatch,
        supportFinalization,
      });
    }

    const closedAt = Date.now();
    const room = `s:${id}`;
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
    const supportFinalization = await finalizeSupportSessionFromRealtime({
      realtimeSessionId: id,
      realtimeSession: {
        ...session,
        ...updates,
        sessionId: id,
      },
      actorUid: closerUid,
      actorRole: 'tech',
      authorizedTech: true,
      summary: {
        problemSummary: reportUpdates.symptom,
        solutionSummary: reportUpdates.solution,
        ...(typeof reportUpdates.notes !== 'undefined'
          ? { internalNotes: reportUpdates.notes }
          : {}),
      },
      now: closedAt,
    });
    io.to(room).emit('session:command', {
      ...commandEvent,
      type: 'session_end',
      normalizedType: 'end',
    });
    io.to(room).emit('session:ended', { sessionId: id, reason });
    io.socketsLeave(room);
    ['tech', 'client'].forEach((role) => {
      const roleRoom = sessionRoleSocketRoom(id, role);
      if (roleRoom) io.socketsLeave(roleRoom);
    });
    await emitSessionUpdated(id);

    const reportDispatch = skipClientReportDispatch
      ? { status: 'skipped', reason: 'manual_only' }
      : await dispatchClientSupportReportForSession({
          sessionRef: snapshot.ref,
          sessionId: id,
          sessionData: {
            ...session,
            ...updates,
            sessionId: id,
          },
          payload,
        });

    return res.json({ ok: true, reportDispatch, supportFinalization });
  } catch (err) {
    console.error('Failed to close session', err);
    if (err instanceof SupportSessionClosureError) {
      return res.status(err.status).json({ error: err.code });
    }
    return res.status(500).json({ error: 'firestore_error' });
  }
});

app.post('/api/sessions/:id/client-close', requireAuth(), async (req, res) => {
  const id = normalizeSessionId(req.params.id);
  if (!/^[A-Za-z0-9_-]{1,64}$/.test(id)) {
    return res.status(400).json({ error: 'invalid_session_id' });
  }
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
    if (!authUid || authUid !== getSessionClientUid(session)) {
      return res.status(403).json({ error: 'forbidden' });
    }

    const currentStatus = ensureString(session.status || '', '').trim().toLowerCase();
    const now = Date.now();
    if (['closed', 'ended', 'completed'].includes(currentStatus)) {
      const supportFinalization = await finalizeSupportSessionFromRealtime({
        realtimeSessionId: id,
        realtimeSession: {
          ...session,
          sessionId: id,
          status: 'closed',
          closedAt: Number(session.closedAt || now),
        },
        actorUid: authUid,
        actorRole: 'client',
        authorizedTech: false,
        now: Number(session.closedAt || now),
      });
      return res.json({ ok: true, alreadyClosed: true, supportFinalization });
    }
    if (!['active', 'accepted', 'in_progress'].includes(currentStatus)) {
      return res.status(409).json({ error: 'session_not_active' });
    }

    const eventId = `${now.toString(36)}-${randomLowercaseId(8)}`;
    const commandEvent = {
      id: eventId,
      sessionId: id,
      type: 'end',
      rawType: 'session_end',
      data: null,
      by: authUid,
      reason: 'client_ended',
      ts: now,
      kind: 'command',
    };
    const nextTelemetry =
      typeof session.telemetry === 'object' && session.telemetry !== null
        ? { ...session.telemetry }
        : {};
    nextTelemetry.shareActive = false;
    nextTelemetry.callActive = false;
    nextTelemetry.remoteActive = false;
    nextTelemetry.updatedAt = now;

    const updates = {
      status: 'closed',
      closedAt: now,
      updatedAt: now,
      lastCommandAt: now,
      handleTimeMs: now - Number(session.acceptedAt || session.createdAt || now),
      telemetry: nextTelemetry,
      'extra.telemetry': nextTelemetry,
      'extra.lastCommand': commandEvent,
    };
    if (typeof nextTelemetry.network !== 'undefined') updates['extra.network'] = nextTelemetry.network;
    if (typeof nextTelemetry.health !== 'undefined') updates['extra.health'] = nextTelemetry.health;
    if (typeof nextTelemetry.permissions !== 'undefined') updates['extra.permissions'] = nextTelemetry.permissions;
    if (typeof nextTelemetry.alerts !== 'undefined') updates['extra.alerts'] = nextTelemetry.alerts;

    await snapshot.ref.collection('events').doc(eventId).set(commandEvent);
    await snapshot.ref.set(updates, { merge: true });
    const supportFinalization = await finalizeSupportSessionFromRealtime({
      realtimeSessionId: id,
      realtimeSession: {
        ...session,
        ...updates,
        sessionId: id,
      },
      actorUid: authUid,
      actorRole: 'client',
      authorizedTech: false,
      now,
    });

    const room = `s:${id}`;
    io.to(room).emit('session:command', {
      ...commandEvent,
      type: 'session_end',
      normalizedType: 'end',
    });
    io.to(room).emit('session:ended', { sessionId: id, reason: 'client_ended' });
    io.socketsLeave(room);
    ['tech', 'client'].forEach((role) => {
      const roleRoom = sessionRoleSocketRoom(id, role);
      if (roleRoom) io.socketsLeave(roleRoom);
    });
    await emitSessionUpdated(id);

    return res.json({ ok: true, supportFinalization });
  } catch (err) {
    console.error('Failed to close client session', err);
    if (err instanceof SupportSessionClosureError) {
      return res.status(err.status).json({ error: err.code });
    }
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
  startManualDeclineReconciler();
  startQueueAlertScheduler();
});
