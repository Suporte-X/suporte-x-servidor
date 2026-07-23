'use strict';

const crypto = require('node:crypto');

const ACTIVE_TECH_SOCKET_ROOM = 'auth:tech:active';
const SEND_INLINE_SCRIPT_HASHES = Object.freeze([
  "'sha256-+IURfyEPlnmyjpiHaf/XR8XestXEq122TahS+2eZxVc='",
  "'sha256-k84/ioLoPx0Kk3w8VWbcvVSLJmTkdVvQ9yiqahNIhZ4='",
]);

const normalizeRoomValue = (value, maxLength = 256) =>
  String(value || '')
    .trim()
    .slice(0, maxLength);

const uniqueValues = (values = []) => {
  const result = [];
  values.forEach((value) => {
    const normalized = normalizeRoomValue(value);
    if (normalized && !result.includes(normalized)) result.push(normalized);
  });
  return result;
};

const clientSocketRoom = (uid) => {
  const normalizedUid = normalizeRoomValue(uid);
  if (!normalizedUid) return null;
  return `auth:user:${Buffer.from(normalizedUid, 'utf8').toString('base64url')}`;
};

const sessionRoleSocketRoom = (sessionId, role) => {
  const normalizedSessionId = normalizeRoomValue(sessionId, 128).replace(/[^a-zA-Z0-9_-]/g, '');
  const normalizedRole = normalizeRoomValue(role, 16).toLowerCase();
  if (!normalizedSessionId || !['client', 'tech'].includes(normalizedRole)) return null;
  return `s:${normalizedSessionId}:${normalizedRole}`;
};

const isExplicitlyEnabled = (value) =>
  String(value || '')
    .trim()
    .toLowerCase() === 'true';

const timingSafeStringEqual = (left, right) => {
  const digest = (value) =>
    crypto
      .createHash('sha256')
      .update(String(value ?? ''), 'utf8')
      .digest();

  return crypto.timingSafeEqual(digest(left), digest(right));
};

const buildContentSecurityPolicy = ({ isProduction = false } = {}) => {
  const connectSources = [
    "'self'",
    'https://*.googleapis.com',
    'https://*.firebaseio.com',
    'wss://*.firebaseio.com',
    'https://*.firebaseapp.com',
    'https://challenges.cloudflare.com',
    'https://accounts.google.com',
    ...(isProduction
      ? ['wss://suportex.app', 'wss://www.suportex.app']
      : ['ws:', 'wss:']),
  ];
  const directives = [
    ["default-src", "'self'"],
    ["base-uri", "'none'"],
    ["object-src", "'none'"],
    ["frame-ancestors", "'none'"],
    ["form-action", "'self'"],
    [
      "script-src",
      "'self'",
      ...SEND_INLINE_SCRIPT_HASHES,
      'https://www.gstatic.com',
      'https://apis.google.com',
      'https://accounts.google.com',
      'https://challenges.cloudflare.com',
    ],
    ["script-src-attr", "'none'"],
    ["style-src", "'self'", 'https://fonts.googleapis.com'],
    ["style-src-attr", "'unsafe-inline'"],
    ["font-src", "'self'", 'data:', 'https://fonts.gstatic.com'],
    [
      "img-src",
      "'self'",
      'data:',
      'blob:',
      'https://firebasestorage.googleapis.com',
      'https://*.googleusercontent.com',
      'https://images.unsplash.com',
      'https://graph.facebook.com',
      'https://*.fbcdn.net',
      'https://*.fbsbx.com',
      'https://*.whatsapp.net',
    ],
    [
      "media-src",
      "'self'",
      'blob:',
      'https://firebasestorage.googleapis.com',
    ],
    ["connect-src", ...connectSources],
    [
      "frame-src",
      'https://challenges.cloudflare.com',
      'https://accounts.google.com',
      'https://*.firebaseapp.com',
    ],
    ["worker-src", "'self'", 'blob:'],
    ["manifest-src", "'self'"],
    ...(isProduction ? [["upgrade-insecure-requests"]] : []),
  ];
  return directives
    .map(([name, ...sources]) => `${name}${sources.length ? ` ${sources.join(' ')}` : ''}`)
    .join('; ');
};

const createWebSecurityHeadersMiddleware = ({ isProduction = false } = {}) => {
  const contentSecurityPolicy = buildContentSecurityPolicy({ isProduction });
  return (req, res, next) => {
    res.setHeader('Content-Security-Policy', contentSecurityPolicy);
    const requestPath = String(req?.path || req?.url || '').split('?')[0];
    if (
      requestPath.startsWith('/api/') ||
      requestPath === '/health' ||
      requestPath === '/healthz'
    ) {
      res.setHeader('Cache-Control', 'private, no-store, max-age=0');
      res.setHeader('Pragma', 'no-cache');
    }
    next();
  };
};

const isAuthorizedTechProfilePhotoUrl = ({
  photoUrl = '',
  authorizedPhotoUrl = '',
  storagePath = '',
  uid = '',
  bucketName = '',
} = {}) => {
  const normalizedPhotoUrl = String(photoUrl || '').trim();
  const normalizedAuthorizedUrl = String(authorizedPhotoUrl || '').trim();
  const normalizedStoragePath = String(storagePath || '').trim();
  const normalizedUid = normalizeRoomValue(uid, 256);
  const normalizedBucketName = String(bucketName || '').trim();
  if (
    !normalizedPhotoUrl ||
    normalizedPhotoUrl !== normalizedAuthorizedUrl ||
    !normalizedUid ||
    !normalizedBucketName ||
    !normalizedStoragePath.startsWith(`chat/avatars/${normalizedUid}/`)
  ) {
    return false;
  }

  try {
    const parsed = new URL(normalizedPhotoUrl);
    if (
      parsed.protocol !== 'https:' ||
      parsed.hostname !== 'firebasestorage.googleapis.com' ||
      parsed.port
    ) {
      return false;
    }
    const pathMatch = parsed.pathname.match(/^\/v0\/b\/([^/]+)\/o\/(.+)$/);
    if (!pathMatch) return false;
    const parsedBucket = decodeURIComponent(pathMatch[1]);
    const parsedStoragePath = decodeURIComponent(pathMatch[2]);
    return (
      parsedBucket === normalizedBucketName &&
      parsedStoragePath === normalizedStoragePath &&
      parsed.searchParams.get('alt') === 'media' &&
      Boolean(parsed.searchParams.get('token'))
    );
  } catch (_error) {
    return false;
  }
};

const mayReplaceClientUidLink = ({ hasVerifiedIdentityProof = false } = {}) =>
  hasVerifiedIdentityProof === true;

const selectClientPhoneForIdentity = ({
  existingPhone = null,
  claimedPhone = null,
  hasVerifiedIdentityProof = false,
} = {}) =>
  hasVerifiedIdentityProof
    ? normalizeRoomValue(claimedPhone) || normalizeRoomValue(existingPhone) || null
    : normalizeRoomValue(existingPhone) || normalizeRoomValue(claimedPhone) || null;

const buildClientIdentityLookupPlan = ({
  linkedClientId = null,
  linkedDeviceClientId = null,
  verificationClientIds = [],
  uidDocId = null,
  phoneDocId = null,
  hasVerifiedIdentityProof = false,
  allowDeviceIdentityLookup = false,
} = {}) => {
  const deviceIdentityCandidates = allowDeviceIdentityLookup ? [linkedDeviceClientId] : [];
  const candidateIds = hasVerifiedIdentityProof
    ? uniqueValues([
        ...verificationClientIds,
        phoneDocId,
        linkedClientId,
        ...deviceIdentityCandidates,
        uidDocId,
      ])
    : uniqueValues([linkedClientId || deviceIdentityCandidates[0] || uidDocId]);

  const fallbackClientId = hasVerifiedIdentityProof
    ? uniqueValues([
        ...verificationClientIds,
        phoneDocId,
        linkedClientId,
        ...deviceIdentityCandidates,
        uidDocId,
      ])[0] || null
    : normalizeRoomValue(linkedClientId) ||
      normalizeRoomValue(deviceIdentityCandidates[0]) ||
      normalizeRoomValue(uidDocId) ||
      null;

  return {
    candidateIds,
    fallbackClientId,
    allowPhoneLookup: hasVerifiedIdentityProof === true,
    // Device correlation is reserved for authenticated technical/internal
    // context. It is never enabled for a customer identity claim.
    allowDeviceLookup: allowDeviceIdentityLookup === true,
    allowDeviceLinkWrite: hasVerifiedIdentityProof === true,
  };
};

module.exports = {
  ACTIVE_TECH_SOCKET_ROOM,
  SEND_INLINE_SCRIPT_HASHES,
  buildContentSecurityPolicy,
  buildClientIdentityLookupPlan,
  clientSocketRoom,
  createWebSecurityHeadersMiddleware,
  isExplicitlyEnabled,
  isAuthorizedTechProfilePhotoUrl,
  mayReplaceClientUidLink,
  selectClientPhoneForIdentity,
  sessionRoleSocketRoom,
  timingSafeStringEqual,
};
