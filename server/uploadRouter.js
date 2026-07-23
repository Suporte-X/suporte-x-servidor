const crypto = require('node:crypto');
const path = require('node:path');
const express = require('express');
const multer = require('multer');

const LIMITS = Object.freeze({
  attachmentBytes: 10 * 1024 * 1024,
  audioBytes: 20 * 1024 * 1024,
  avatarBytes: 5 * 1024 * 1024,
});

const MAX_REQUEST_BYTES = Math.max(LIMITS.attachmentBytes, LIMITS.audioBytes, LIMITS.avatarBytes);

const IMAGE_EXTENSIONS = new Set(['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp', 'heic', 'heif']);
const AUDIO_EXTENSIONS = new Set(['webm', 'm4a', 'aac', 'mp3', 'ogg', 'wav']);
const IMAGE_MIME_BY_EXTENSION = Object.freeze({
  jpg: new Set(['image/jpeg', 'image/jpg']),
  jpeg: new Set(['image/jpeg', 'image/jpg']),
  png: new Set(['image/png']),
  gif: new Set(['image/gif']),
  webp: new Set(['image/webp']),
  bmp: new Set(['image/bmp', 'image/x-ms-bmp']),
  heic: new Set(['image/heic', 'image/heif']),
  heif: new Set(['image/heif', 'image/heic']),
});
const AUDIO_MIME_BY_EXTENSION = Object.freeze({
  webm: new Set(['audio/webm', 'video/webm']),
  m4a: new Set(['audio/mp4', 'audio/x-m4a']),
  aac: new Set(['audio/aac', 'audio/x-aac']),
  mp3: new Set(['audio/mpeg', 'audio/mp3']),
  ogg: new Set(['audio/ogg', 'application/ogg']),
  wav: new Set(['audio/wav', 'audio/x-wav']),
});
const CANONICAL_MIME_BY_EXTENSION = Object.freeze({
  jpg: 'image/jpeg',
  jpeg: 'image/jpeg',
  png: 'image/png',
  gif: 'image/gif',
  webp: 'image/webp',
  bmp: 'image/bmp',
  heic: 'image/heic',
  heif: 'image/heif',
  webm: 'audio/webm',
  m4a: 'audio/mp4',
  aac: 'audio/aac',
  mp3: 'audio/mpeg',
  ogg: 'audio/ogg',
  wav: 'audio/wav',
});
const HEIF_BRANDS = new Set([
  'heic',
  'heix',
  'hevc',
  'hevx',
  'heim',
  'heis',
  'hevm',
  'hevs',
  'mif1',
  'msf1',
]);
const M4A_BRANDS = new Set(['M4A ', 'M4B ', 'isom', 'iso2', 'mp41', 'mp42', 'qt  ']);
const TERMINAL_SESSION_STATES = new Set(['closed', 'ended', 'cancelled', 'canceled', 'rejected']);

class HttpError extends Error {
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

function createUploadRouter({ auth, db, bucket, clock = () => Date.now(), logger = console } = {}) {
  if (!auth || typeof auth.verifyIdToken !== 'function') {
    throw new Error('createUploadRouter requires auth.verifyIdToken');
  }
  if (!db || typeof db.collection !== 'function') {
    throw new Error('createUploadRouter requires Firestore db');
  }
  if (!bucket || typeof bucket.file !== 'function' || typeof bucket.name !== 'string') {
    throw new Error('createUploadRouter requires Storage bucket');
  }

  const router = express.Router();
  const multipart = multer({
    storage: multer.memoryStorage(),
    limits: {
      fileSize: MAX_REQUEST_BYTES,
      files: 1,
    },
  });

  const requireAuth = createAuthMiddleware(auth);
  const requireActiveTech = createActiveTechMiddleware(db);
  const parseSingleFile = createMultipartMiddleware(multipart);

  router.post(
    '/session-attachment',
    requireAuth,
    parseSingleFile,
    asyncHandler(async (req, res) => {
      const result = await handleSessionUpload(req, {
        kind: 'attachment',
        maxBytes: LIMITS.attachmentBytes,
        endpointLabel: 'session-attachment',
        mimeValidator: isAllowedImageMime,
        db,
        bucket,
        clock,
      });
      await writeSessionUploadMetadata({ db, ...result });
      res.status(200).json({ ok: true, upload: result.responseUpload });
    }, logger)
  );

  router.post(
    '/session-audio',
    requireAuth,
    parseSingleFile,
    asyncHandler(async (req, res) => {
      const result = await handleSessionUpload(req, {
        kind: 'audio',
        maxBytes: LIMITS.audioBytes,
        endpointLabel: 'session-audio',
        mimeValidator: isAllowedAudioMime,
        db,
        bucket,
        clock,
      });
      await writeSessionUploadMetadata({ db, ...result });
      res.status(200).json({ ok: true, upload: result.responseUpload });
    }, logger)
  );

  router.post(
    '/avatar',
    requireAuth,
    requireActiveTech,
    parseSingleFile,
    asyncHandler(async (req, res) => {
      const file = requireFile(req.file);
      validateFileSize(file, LIMITS.avatarBytes);
      const extension = pickExtension(file.originalname, file.mimetype, 'avatar');
      const normalizedMime = normalizeMimeType(file.mimetype, extension, 'image/jpeg');
      if (!isAllowedImageMime(normalizedMime, extension)) {
        throw new HttpError(400, 'invalid_mime_type', 'Apenas imagens sao permitidas para avatar.');
      }
      validateFileSignature(file, 'image', extension);

      const uploadId = randomId();
      const now = clock();
      const uid = req.user.uid;
      const originalName = sanitizeOriginalName(file.originalname, `avatar-${now}.${extension}`);
      const storagePath = `chat/avatars/${uid}/${now}-${uploadId}-${originalName}`;

      const uploaded = await uploadToStorage({
        bucket,
        file,
        storagePath,
        contentType: normalizedMime,
        customMetadata: {
          uploadKind: 'avatar',
          ownerUid: uid,
        },
      });

      const responseUpload = {
        uploadId,
        kind: 'avatar',
        path: storagePath,
        downloadURL: uploaded.downloadURL,
        contentType: normalizedMime,
        size: file.size,
        fileName: originalName,
        uploadedByUid: uid,
        createdAt: now,
      };

      await writeAvatarUploadMetadata({
        db,
        uid,
        responseUpload,
        now,
      });

      res.status(200).json({ ok: true, upload: responseUpload });
    }, logger)
  );

  return router;
}

async function handleSessionUpload(req, {
  kind,
  maxBytes,
  endpointLabel,
  mimeValidator,
  db,
  bucket,
  clock,
}) {
  const file = requireFile(req.file);
  validateFileSize(file, maxBytes);

  const sessionId = asNonEmptyString(req.body?.sessionId);
  if (!sessionId) {
    throw new HttpError(400, 'session_id_required', `${endpointLabel} requires sessionId.`);
  }

  const extension = pickExtension(file.originalname, file.mimetype, kind);
  const normalizedMime = normalizeMimeType(file.mimetype, extension, kind === 'audio' ? 'audio/webm' : 'image/jpeg');
  if (!mimeValidator(normalizedMime, extension)) {
    throw new HttpError(400, 'invalid_mime_type', `MIME type ${normalizedMime} não permitido para ${kind}.`);
  }
  validateFileSignature(file, kind === 'audio' ? 'audio' : 'image', extension);

  const sessionSnap = await db.collection('sessions').doc(sessionId).get();
  if (!sessionSnap.exists) {
    throw new HttpError(404, 'session_not_found', 'Sessão não encontrada.');
  }

  const sessionData = sessionSnap.data() || {};
  const sessionStatus = asNonEmptyString(sessionData.status)?.toLowerCase() || '';
  if (TERMINAL_SESSION_STATES.has(sessionStatus)) {
    throw new HttpError(409, 'session_not_active', 'A sessão já foi encerrada.');
  }

  const membership = resolveSessionMembership(req.user, sessionData);
  if (!membership.allowed) {
    throw new HttpError(403, 'not_session_member', 'Usuário não pertence à sessão informada.');
  }
  if (membership.role === 'tech') {
    await assertActiveTech(db, req.user.uid);
  }

  const uploadId = randomId();
  const now = clock();
  const originalName = sanitizeOriginalName(
    file.originalname,
    `${kind}-${now}.${extension}`
  );
  const folder = kind === 'audio' ? 'audio' : 'attachments';
  const storagePath = `sessions/${sessionId}/${folder}/${now}-${uploadId}-${originalName}`;

  const uploaded = await uploadToStorage({
    bucket,
    file,
    storagePath,
    contentType: normalizedMime,
    customMetadata: {
      uploadKind: kind,
      sessionId,
      uploadedByUid: req.user.uid,
      uploadedByRole: membership.role,
    },
  });

  const messageId = asNonEmptyString(req.body?.messageId) || null;
  const responseUpload = {
    uploadId,
    kind,
    sessionId,
    messageId,
    path: storagePath,
    downloadURL: uploaded.downloadURL,
    contentType: normalizedMime,
    size: file.size,
    fileName: originalName,
    uploadedByUid: req.user.uid,
    uploadedByRole: membership.role,
    createdAt: now,
  };

  return {
    responseUpload,
    sessionId,
    messageId,
    uploadId,
    storagePath,
    file,
    normalizedMime,
    now,
    uploadedByUid: req.user.uid,
    uploadedByRole: membership.role,
  };
}

async function writeSessionUploadMetadata({
  db,
  responseUpload,
  sessionId,
  uploadId,
  uploadedByUid,
  uploadedByRole,
  normalizedMime,
  storagePath,
  file,
  messageId,
  now,
}) {
  await db.collection('sessions')
    .doc(sessionId)
    .collection('uploads')
    .doc(uploadId)
    .set({
      ...responseUpload,
      createdAt: now,
      updatedAt: now,
      contentType: normalizedMime,
      path: storagePath,
      size: file.size,
      messageId: messageId || null,
      uploadedByUid,
      uploadedByRole,
    });
}

async function writeAvatarUploadMetadata({ db, uid, responseUpload, now }) {
  await db.collection('techs').doc(uid).set({
    customPhotoURL: responseUpload.downloadURL,
    avatarPath: responseUpload.path,
    avatarContentType: responseUpload.contentType,
    avatarUpdatedAt: now,
    updatedAt: now,
  }, { merge: true });

  await db.collection('techs')
    .doc(uid)
    .collection('uploads')
    .doc(responseUpload.uploadId)
    .set({
      ...responseUpload,
      createdAt: now,
      updatedAt: now,
    });
}

function resolveSessionMembership(user, session = {}) {
  const uid = asNonEmptyString(user?.uid);
  if (!uid) return { allowed: false, role: null };

  const clientUids = collectCandidateStrings([
    session.clientUid,
    session.client?.uid,
    session.client?.clientUid,
    session.requesterUid,
    session.request?.clientUid,
  ]);

  const techUids = collectCandidateStrings([
    session.techUid,
    session.technicianUid,
    session.tech?.uid,
    session.extra?.techUid,
    session.extra?.tech?.uid,
  ]);

  if (clientUids.has(uid)) return { allowed: true, role: 'client' };
  if (techUids.has(uid)) return { allowed: true, role: 'tech' };
  return { allowed: false, role: null };
}

function collectCandidateStrings(values) {
  const output = new Set();
  for (const value of values) {
    if (typeof value !== 'string') continue;
    const normalized = value.trim();
    if (!normalized) continue;
    output.add(normalized);
  }
  return output;
}

function requireFile(file) {
  if (!file || !file.buffer || !Number.isFinite(file.size)) {
    throw new HttpError(400, 'file_required', 'Nenhum arquivo foi enviado.');
  }
  return file;
}

function validateFileSize(file, maxBytes) {
  if (file.size > maxBytes) {
    throw new HttpError(400, 'file_too_large', `Arquivo excede limite de ${maxBytes} bytes.`);
  }
}

function normalizeMimeType(mimeType, extension, fallback) {
  if (typeof mimeType === 'string' && mimeType.trim()) {
    const normalized = mimeType.split(';')[0].trim().toLowerCase();
    if (normalized !== 'application/octet-stream') return normalized;
  }
  return CANONICAL_MIME_BY_EXTENSION[extension] || fallback;
}

function pickExtension(originalName, mimeType, kind) {
  const fromName = path.extname(asNonEmptyString(originalName) || '').replace('.', '').toLowerCase();
  if (fromName) return fromName;

  const normalizedMime = typeof mimeType === 'string' ? mimeType.split(';')[0].trim().toLowerCase() : '';
  const mimeToExtension = {
    'image/jpeg': 'jpg',
    'image/jpg': 'jpg',
    'image/png': 'png',
    'image/webp': 'webp',
    'image/gif': 'gif',
    'audio/webm': 'webm',
    'video/webm': 'webm',
    'audio/mp4': 'm4a',
    'audio/aac': 'aac',
    'audio/mpeg': 'mp3',
    'audio/ogg': 'ogg',
    'audio/wav': 'wav',
    'audio/x-wav': 'wav',
  };
  if (mimeToExtension[normalizedMime]) {
    return mimeToExtension[normalizedMime];
  }
  return kind === 'audio' ? 'webm' : 'bin';
}

function sanitizeOriginalName(originalName, fallback) {
  const source = asNonEmptyString(originalName) || fallback;
  const parsed = path.parse(source);
  const safeBase = parsed.name.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 64) || 'file';
  const safeExt = parsed.ext.replace(/[^a-zA-Z0-9.]/g, '').slice(0, 10);
  return `${safeBase}${safeExt || ''}`;
}

async function uploadToStorage({ bucket, file, storagePath, contentType, customMetadata = {} }) {
  const downloadToken = randomId();
  await bucket.file(storagePath).save(file.buffer, {
    resumable: false,
    metadata: {
      contentType,
      cacheControl: 'private, max-age=3600',
      metadata: {
        firebaseStorageDownloadTokens: downloadToken,
        ...customMetadata,
      },
    },
  });

  const encodedPath = encodeURIComponent(storagePath);
  const downloadURL = `https://firebasestorage.googleapis.com/v0/b/${bucket.name}/o/${encodedPath}?alt=media&token=${downloadToken}`;
  return { downloadURL };
}

function isAllowedImageMime(mimeType, extension) {
  return isAllowedMediaMime({
    mimeType,
    extension,
    allowedExtensions: IMAGE_EXTENSIONS,
    mimeByExtension: IMAGE_MIME_BY_EXTENSION,
  });
}

function isAllowedAudioMime(mimeType, extension) {
  return isAllowedMediaMime({
    mimeType,
    extension,
    allowedExtensions: AUDIO_EXTENSIONS,
    mimeByExtension: AUDIO_MIME_BY_EXTENSION,
  });
}

function isAllowedMediaMime({
  mimeType,
  extension,
  allowedExtensions,
  mimeByExtension,
}) {
  const normalizedExtension = asNonEmptyString(extension)?.toLowerCase() || '';
  if (!allowedExtensions.has(normalizedExtension)) return false;
  const normalizedMime = asNonEmptyString(mimeType)?.toLowerCase() || '';
  if (!normalizedMime || normalizedMime === 'application/octet-stream') return true;
  return mimeByExtension[normalizedExtension]?.has(normalizedMime) === true;
}

function validateFileSignature(file, kind, extension) {
  const buffer = file?.buffer;
  const valid =
    kind === 'audio'
      ? hasAllowedAudioSignature(buffer, extension)
      : hasAllowedImageSignature(buffer, extension);
  if (!valid) {
    throw new HttpError(
      400,
      'invalid_file_signature',
      kind === 'audio'
        ? 'O conteúdo do arquivo não corresponde a um áudio permitido.'
        : 'O conteúdo do arquivo não corresponde a uma imagem permitida.'
    );
  }
}

function hasAllowedImageSignature(buffer, extension) {
  if (!Buffer.isBuffer(buffer)) return false;
  switch (extension) {
    case 'jpg':
    case 'jpeg':
      return startsWithBytes(buffer, [0xff, 0xd8, 0xff]);
    case 'png':
      return startsWithBytes(buffer, [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
    case 'gif':
      return startsWithAscii(buffer, 'GIF87a') || startsWithAscii(buffer, 'GIF89a');
    case 'webp':
      return startsWithAscii(buffer, 'RIFF') && asciiAt(buffer, 8, 4) === 'WEBP';
    case 'bmp':
      return startsWithAscii(buffer, 'BM');
    case 'heic':
    case 'heif':
      return hasIsoBaseMediaBrand(buffer, HEIF_BRANDS);
    default:
      return false;
  }
}

function hasAllowedAudioSignature(buffer, extension) {
  if (!Buffer.isBuffer(buffer)) return false;
  switch (extension) {
    case 'webm':
      return startsWithBytes(buffer, [0x1a, 0x45, 0xdf, 0xa3]);
    case 'm4a':
      return hasIsoBaseMediaBrand(buffer, M4A_BRANDS);
    case 'aac':
      return buffer.length >= 2 && buffer[0] === 0xff && (buffer[1] & 0xf6) === 0xf0;
    case 'mp3':
      return (
        startsWithAscii(buffer, 'ID3') ||
        (buffer.length >= 2 && buffer[0] === 0xff && (buffer[1] & 0xe0) === 0xe0)
      );
    case 'ogg':
      return startsWithAscii(buffer, 'OggS');
    case 'wav':
      return startsWithAscii(buffer, 'RIFF') && asciiAt(buffer, 8, 4) === 'WAVE';
    default:
      return false;
  }
}

function hasIsoBaseMediaBrand(buffer, allowedBrands) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 12 || asciiAt(buffer, 4, 4) !== 'ftyp') {
    return false;
  }
  const scanLimit = Math.min(buffer.length, 64);
  for (let offset = 8; offset + 4 <= scanLimit; offset += 4) {
    if (allowedBrands.has(asciiAt(buffer, offset, 4))) return true;
  }
  return false;
}

function startsWithBytes(buffer, bytes) {
  if (!Buffer.isBuffer(buffer) || buffer.length < bytes.length) return false;
  return bytes.every((value, index) => buffer[index] === value);
}

function startsWithAscii(buffer, value) {
  return asciiAt(buffer, 0, value.length) === value;
}

function asciiAt(buffer, offset, length) {
  if (!Buffer.isBuffer(buffer) || offset < 0 || buffer.length < offset + length) return '';
  return buffer.toString('ascii', offset, offset + length);
}

function randomId() {
  return crypto.randomUUID();
}

function asNonEmptyString(value) {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  return trimmed || null;
}

function createAuthMiddleware(auth) {
  return async (req, res, next) => {
    try {
      const token = extractBearerToken(req.headers.authorization);
      if (!token) {
        throw new HttpError(401, 'missing_auth_token', 'Authorization Bearer token obrigatorio.');
      }
      req.user = await auth.verifyIdToken(token);
      next();
    } catch (error) {
      if (error instanceof HttpError) {
        res.status(error.status).json({ error: error.code, message: error.message });
        return;
      }
      res.status(403).json({ error: 'invalid_auth_token', message: 'Token inválido.' });
    }
  };
}

function createActiveTechMiddleware(db) {
  return async (req, res, next) => {
    try {
      const role = asNonEmptyString(req.user?.role)?.toLowerCase() || 'user';
      if (role !== 'tech') {
        throw new HttpError(403, 'insufficient_role', 'Permissao tecnica obrigatoria.');
      }
      await assertActiveTech(db, req.user?.uid);
      next();
    } catch (error) {
      const status = Number.isInteger(error?.status) ? error.status : 403;
      const code = typeof error?.code === 'string' ? error.code : 'tech_inactive';
      res.status(status).json({ error: code, message: error?.message || 'Tecnico inativo.' });
    }
  };
}

async function assertActiveTech(db, uid) {
  const normalizedUid = asNonEmptyString(uid);
  if (!normalizedUid) {
    throw new HttpError(403, 'tech_inactive', 'Tecnico inativo ou nao cadastrado.');
  }
  const snapshot = await db.collection('techs').doc(normalizedUid).get();
  if (!snapshot.exists || snapshot.data()?.active !== true) {
    throw new HttpError(403, 'tech_inactive', 'Tecnico inativo ou nao cadastrado.');
  }
}

function extractBearerToken(header) {
  if (typeof header !== 'string') return null;
  const [scheme, token] = header.split(' ');
  if (!scheme || !token) return null;
  if (!/^Bearer$/i.test(scheme)) return null;
  return token.trim() || null;
}

function createMultipartMiddleware(multipart) {
  return (req, res, next) => {
    multipart.single('file')(req, res, (error) => {
      if (!error) {
        next();
        return;
      }
      if (error.code === 'LIMIT_FILE_SIZE') {
        res.status(400).json({
          error: 'file_too_large',
          message: `Arquivo excede limite maximo de ${MAX_REQUEST_BYTES} bytes.`,
        });
        return;
      }
      res.status(400).json({
        error: 'invalid_multipart_payload',
        message: 'Payload multipart inválido.',
      });
    });
  };
}

function asyncHandler(handler, logger) {
  return async (req, res) => {
    try {
      await handler(req, res);
    } catch (error) {
      const status = Number.isInteger(error?.status) ? error.status : 500;
      const code = typeof error?.code === 'string' ? error.code : 'upload_failed';
      const message =
        status >= 500
          ? 'Falha interna no upload.'
          : error?.message || 'Nao foi possivel concluir o upload.';
      if (status >= 500) {
        logger.error('[upload]', {
          code,
          name: typeof error?.name === 'string' ? error.name : 'Error',
        });
      }
      res.status(status).json({ error: code, message });
    }
  };
}

module.exports = {
  createUploadRouter,
  LIMITS,
  resolveSessionMembership,
  isAllowedImageMime,
  isAllowedAudioMime,
  validateFileSignature,
  extractBearerToken,
};
