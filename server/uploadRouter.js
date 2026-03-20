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
    parseSingleFile,
    asyncHandler(async (req, res) => {
      const file = requireFile(req.file);
      validateFileSize(file, LIMITS.avatarBytes);
      const extension = pickExtension(file.originalname, file.mimetype, 'avatar');
      const normalizedMime = normalizeMimeType(file.mimetype, extension, 'image/jpeg');
      if (!isAllowedImageMime(normalizedMime, extension)) {
        throw new HttpError(400, 'invalid_mime_type', 'Apenas imagens sao permitidas para avatar.');
      }

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
    throw new HttpError(400, 'invalid_mime_type', `MIME type ${normalizedMime} nao permitido para ${kind}.`);
  }

  const sessionSnap = await db.collection('sessions').doc(sessionId).get();
  if (!sessionSnap.exists) {
    throw new HttpError(404, 'session_not_found', 'Sessao nao encontrada.');
  }

  const membership = resolveSessionMembership(req.user, sessionSnap.data());
  if (!membership.allowed) {
    throw new HttpError(403, 'not_session_member', 'Usuario nao pertence a sessao informada.');
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
    return mimeType.split(';')[0].trim().toLowerCase();
  }
  if (extension === 'webm') return 'audio/webm';
  if (extension && IMAGE_EXTENSIONS.has(extension)) {
    return extension === 'jpg' ? 'image/jpeg' : `image/${extension}`;
  }
  return fallback;
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
  if (typeof mimeType === 'string' && mimeType.startsWith('image/')) return true;
  if ((mimeType === 'application/octet-stream' || !mimeType) && extension && IMAGE_EXTENSIONS.has(extension)) {
    return true;
  }
  return false;
}

function isAllowedAudioMime(mimeType, extension) {
  if (typeof mimeType === 'string' && mimeType.startsWith('audio/')) return true;
  if (mimeType === 'video/webm') return true;
  if ((mimeType === 'application/octet-stream' || !mimeType) && extension && AUDIO_EXTENSIONS.has(extension)) {
    return true;
  }
  return false;
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
      res.status(403).json({ error: 'invalid_auth_token', message: 'Token invalido.' });
    }
  };
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
        message: 'Payload multipart invalido.',
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
      const message = error?.message || 'Falha interna no upload.';
      if (status >= 500) {
        logger.error('[upload]', message, error);
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
  extractBearerToken,
};
