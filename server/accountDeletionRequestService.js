const crypto = require('node:crypto');

const DEFAULT_GRACE_PERIOD_MS = 40 * 24 * 60 * 60 * 1000;
const DEFAULT_RESOLVED_RETENTION_MS = 180 * 24 * 60 * 60 * 1000;
const DEFAULT_COLLECTIONS = Object.freeze({
  requests: 'account_deletion_requests',
  clients: 'clients',
  clientProfiles: 'client_profiles',
  clientLinks: 'client_app_links',
  clientVerifications: 'client_verifications',
  supportRequests: 'requests',
  supportSessions: 'support_sessions',
  creditOrders: 'credit_orders',
});
const MATERIAL_ACTIVITIES = new Set(['support_request', 'credit_purchase']);

class AccountDeletionRequestError extends Error {
  constructor(status, code, message = code) {
    super(message);
    this.name = 'AccountDeletionRequestError';
    this.status = status;
    this.code = code;
  }
}

function createAccountDeletionRequestService({
  db,
  accountDeletionService,
  clock = () => Date.now(),
  gracePeriodMs = DEFAULT_GRACE_PERIOD_MS,
  resolvedRetentionMs = DEFAULT_RESOLVED_RETENTION_MS,
  collections = {},
  notify = null,
  logger = console,
} = {}) {
  if (!db || typeof db.collection !== 'function' || typeof db.runTransaction !== 'function') {
    throw new Error(
      'createAccountDeletionRequestService requires Firestore with collection() and runTransaction()'
    );
  }
  if (
    !accountDeletionService ||
    typeof accountDeletionService.deleteAccountAfterGracePeriod !== 'function'
  ) {
    throw new Error(
      'createAccountDeletionRequestService requires accountDeletionService.deleteAccountAfterGracePeriod()'
    );
  }

  const names = { ...DEFAULT_COLLECTIONS, ...collections };
  const safeGracePeriodMs = Math.max(24 * 60 * 60 * 1000, Number(gracePeriodMs) || 0);
  const safeResolvedRetentionMs = Math.max(
    30 * 24 * 60 * 60 * 1000,
    Number(resolvedRetentionMs) || 0
  );
  const log = normalizeLogger(logger);

  async function requestDeletion({ uid, reason, confirmation } = {}) {
    const normalizedUid = normalizeIdentifier(uid, 256);
    const normalizedReason = normalizeReason(reason);
    if (!normalizedUid) throw new AccountDeletionRequestError(401, 'invalid_token');
    if (confirmation !== 'EXCLUIR CONTA') {
      throw new AccountDeletionRequestError(400, 'confirmation_required');
    }
    if (!normalizedReason) {
      throw new AccountDeletionRequestError(400, 'reason_required');
    }

    const identity = await resolveIdentity(normalizedUid);
    const requestId = requestIdForUid(normalizedUid);
    const requestRef = db.collection(names.requests).doc(requestId);
    const now = clock();
    const eligibleAt = now + safeGracePeriodMs;

    const outcome = await db.runTransaction(async (transaction) => {
      const snapshot = await transaction.get(requestRef);
      const existing = snapshot.exists ? snapshot.data() || {} : {};
      const existingStatus = normalizeStatus(existing.status);
      if (existingStatus === 'pending' || existingStatus === 'processing') {
        return { created: false, record: { ...existing, requestId } };
      }

      const record = {
        requestId,
        uid: normalizedUid,
        uidHash: sha256(normalizedUid),
        clientId: identity.clientId,
        clientName: identity.clientName,
        clientEmail: identity.clientEmail,
        clientPhone: identity.clientPhone,
        reason: normalizedReason,
        status: 'pending',
        requestedAt: now,
        eligibleAt,
        updatedAt: now,
        source: 'android_app',
        gracePeriodDays: Math.round(safeGracePeriodMs / (24 * 60 * 60 * 1000)),
        cancellationPolicy: 'material_activity',
      };
      transaction.set(requestRef, record);
      return { created: true, record };
    });

    if (outcome.created) await notifySafely('requested', outcome.record);
    return publicRequestResult(outcome.record);
  }

  async function cancelForActivity({
    uid,
    activity,
    awaitNotification = true,
  } = {}) {
    const normalizedUid = normalizeIdentifier(uid, 256);
    const normalizedActivity = normalizeActivity(activity);
    if (!normalizedUid) throw new AccountDeletionRequestError(401, 'invalid_token');
    if (!normalizedActivity) {
      throw new AccountDeletionRequestError(400, 'invalid_activity');
    }

    const requestId = requestIdForUid(normalizedUid);
    const requestRef = db.collection(names.requests).doc(requestId);
    const now = clock();
    const outcome = await db.runTransaction(async (transaction) => {
      const snapshot = await transaction.get(requestRef);
      if (!snapshot.exists) {
        return { cancelled: false, status: 'none', record: null };
      }
      const existing = snapshot.data() || {};
      const status = normalizeStatus(existing.status);
      if (status !== 'pending') {
        return { cancelled: false, status, record: { ...existing, requestId } };
      }

      const change = {
        status: 'cancelled',
        cancelledAt: now,
        cancellationActivity: normalizedActivity,
        updatedAt: now,
        expiresAt: new Date(now + safeResolvedRetentionMs),
      };
      transaction.set(requestRef, change, { merge: true });
      return {
        cancelled: true,
        status: 'cancelled',
        record: { ...existing, ...change, requestId },
      };
    });

    if (outcome.cancelled) {
      const notification = notifySafely('cancelled', outcome.record);
      if (awaitNotification) {
        await notification;
      } else {
        void notification;
      }
    }
    return { ok: true, cancelled: outcome.cancelled, status: outcome.status };
  }

  async function processDueRequests({ limit = 50 } = {}) {
    const safeLimit = Math.max(1, Math.min(200, Number(limit) || 50));
    const snapshot = await db
      .collection(names.requests)
      .where('status', '==', 'pending')
      .limit(safeLimit)
      .get();
    const now = clock();
    const results = [];

    for (const doc of snapshot.docs) {
      const record = { ...(doc.data() || {}), requestId: doc.id };
      if (toMillis(record.eligibleAt) > now) continue;

      const detectedActivity = await findMaterialActivityAfter(record);
      if (detectedActivity) {
        const cancellation = await cancelForActivity({
          uid: record.uid,
          activity: detectedActivity,
        });
        results.push({ requestId: doc.id, ...cancellation });
        continue;
      }

      const claimed = await claimForProcessing(doc.ref, now);
      if (!claimed) continue;
      try {
        const deletionResult =
          await accountDeletionService.deleteAccountAfterGracePeriod({
            uid: record.uid,
            requestId: doc.id,
          });
        const completedAt = clock();
        await doc.ref.set(
          {
            status: 'completed',
            completedAt,
            updatedAt: completedAt,
            reason: null,
            expiresAt: new Date(completedAt + safeResolvedRetentionMs),
          },
          { merge: true }
        );
        await notifySafely('completed', {
          ...record,
          status: 'completed',
          completedAt,
          reason: null,
        });
        results.push({
          requestId: doc.id,
          ok: true,
          status: 'completed',
          deletionResult,
        });
      } catch (error) {
        const failedAt = clock();
        await doc.ref.set(
          {
            status: 'pending',
            lastProcessingError: safeErrorCode(error),
            lastProcessingFailedAt: failedAt,
            updatedAt: failedAt,
          },
          { merge: true }
        );
        log.error('Failed to process due account deletion request', {
          requestId: doc.id,
          code: safeErrorCode(error),
        });
        results.push({
          requestId: doc.id,
          ok: false,
          status: 'pending',
          error: safeErrorCode(error),
        });
      }
    }
    return results;
  }

  async function claimForProcessing(requestRef, now) {
    return db.runTransaction(async (transaction) => {
      const snapshot = await transaction.get(requestRef);
      if (!snapshot.exists) return false;
      const current = snapshot.data() || {};
      if (
        normalizeStatus(current.status) !== 'pending' ||
        toMillis(current.eligibleAt) > now
      ) {
        return false;
      }
      transaction.set(
        requestRef,
        { status: 'processing', processingAt: now, updatedAt: now },
        { merge: true }
      );
      return true;
    });
  }

  async function findMaterialActivityAfter(record) {
    const requestedAt = toMillis(record.requestedAt);
    if (!requestedAt) return null;
    const identityQueries = [
      [names.supportRequests, 'clientUid', record.uid, 'support_request'],
      [names.supportRequests, 'clientRecordId', record.clientId, 'support_request'],
      [names.supportSessions, 'clientUid', record.uid, 'support_request'],
      [names.supportSessions, 'clientId', record.clientId, 'support_request'],
      [names.creditOrders, 'clientId', record.clientId, 'credit_purchase'],
    ];

    for (const [collectionName, field, value, activity] of identityQueries) {
      if (!normalizeIdentifier(value, 256)) continue;
      const activitySnapshot = await db
        .collection(collectionName)
        .where(field, '==', value)
        .get();
      const found = activitySnapshot.docs.some((doc) => {
        const data = doc.data() || {};
        const activityAt = toMillis(
          data.createdAt || data.requestedAt || data.updatedAt || data.acceptedAt
        );
        return activityAt > requestedAt;
      });
      if (found) return activity;
    }
    return null;
  }

  async function resolveIdentity(uid) {
    const directLink = await db.collection(names.clientLinks).doc(uid).get();
    let clientId = directLink.exists
      ? normalizeIdentifier(directLink.data()?.clientId, 128)
      : null;
    if (!clientId) {
      const links = await db
        .collection(names.clientLinks)
        .where('clientUid', '==', uid)
        .get();
      clientId = normalizeIdentifier(links.docs[0]?.data()?.clientId, 128) || null;
    }
    if (!clientId) {
      const fallbackId = `uid_${normalizeIdentifier(uid, 256).replace(/[^a-zA-Z0-9]/g, '')}`;
      const fallback = await db.collection(names.clients).doc(fallbackId).get();
      if (fallback.exists) clientId = fallbackId;
    }

    let client = {};
    if (clientId) {
      const [clientSnapshot, profileSnapshot, verificationSnapshot] = await Promise.all([
        db.collection(names.clients).doc(clientId).get(),
        db.collection(names.clientProfiles).doc(clientId).get(),
        db.collection(names.clientVerifications).doc(clientId).get(),
      ]);
      client = {
        ...(verificationSnapshot.exists ? verificationSnapshot.data() || {} : {}),
        ...(profileSnapshot.exists ? profileSnapshot.data() || {} : {}),
        ...(clientSnapshot.exists ? clientSnapshot.data() || {} : {}),
      };
    }
    return {
      clientId,
      clientName: normalizeText(client.name, 160) || null,
      clientEmail:
        normalizeEmail(
          client.email || client.verifiedEmail || client.primaryEmail
        ) || null,
      clientPhone:
        normalizeText(
          client.phone || client.verifiedPhone || client.primaryPhone,
          64
        ) || null,
    };
  }

  async function notifySafely(type, record) {
    if (typeof notify !== 'function') return;
    try {
      const result = await notify({
        type,
        request: sanitizeForNotification(record),
      });
      const update = {
        updatedAt: clock(),
        [`${type}NotificationStatus`]:
          normalizeText(result?.status, 32) || 'completed',
      };
      if (result?.providerMessageId) {
        update[`${type}NotificationId`] =
          normalizeText(result.providerMessageId, 256);
      }
      await db.collection(names.requests).doc(record.requestId).set(update, { merge: true });
    } catch (error) {
      log.warn('Failed to notify account deletion request event', {
        requestId: record.requestId,
        type,
        code: safeErrorCode(error),
      });
      await db.collection(names.requests).doc(record.requestId).set(
        {
          updatedAt: clock(),
          [`${type}NotificationStatus`]: 'error',
          [`${type}NotificationError`]: safeErrorCode(error),
        },
        { merge: true }
      );
    }
  }

  return { requestDeletion, cancelForActivity, processDueRequests };
}

function publicRequestResult(record) {
  return {
    ok: true,
    status: normalizeStatus(record.status) || 'pending',
    requestId: normalizeIdentifier(record.requestId, 128),
    requestedAt: toMillis(record.requestedAt),
    eligibleAt: toMillis(record.eligibleAt),
    gracePeriodDays: Math.max(1, Number(record.gracePeriodDays) || 40),
  };
}

function sanitizeForNotification(record) {
  return {
    requestId: normalizeIdentifier(record.requestId, 128),
    clientId: normalizeIdentifier(record.clientId, 128),
    clientName: normalizeText(record.clientName, 160) || null,
    clientEmail: normalizeEmail(record.clientEmail) || null,
    clientPhone: normalizeText(record.clientPhone, 64) || null,
    reason: normalizeReason(record.reason) || null,
    status: normalizeStatus(record.status),
    requestedAt: toMillis(record.requestedAt),
    eligibleAt: toMillis(record.eligibleAt),
    cancelledAt: toMillis(record.cancelledAt),
    completedAt: toMillis(record.completedAt),
    cancellationActivity: normalizeActivity(record.cancellationActivity),
  };
}

function normalizeActivity(value) {
  const activity = normalizeText(value, 64).toLowerCase();
  return MATERIAL_ACTIVITIES.has(activity) ? activity : '';
}

function normalizeReason(value) {
  return normalizeText(value, 1000);
}

function normalizeIdentifier(value, maxLength) {
  if (typeof value !== 'string' && typeof value !== 'number') return '';
  return String(value).trim().slice(0, maxLength);
}

function normalizeEmail(value) {
  const email = normalizeText(value, 254).toLowerCase();
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? email : '';
}

function normalizeText(value, maxLength) {
  if (typeof value !== 'string' && typeof value !== 'number') return '';
  return String(value).trim().slice(0, maxLength);
}

function normalizeStatus(value) {
  return normalizeText(value, 32).toLowerCase();
}

function requestIdForUid(uid) {
  return sha256(uid);
}

function sha256(value) {
  return crypto.createHash('sha256').update(String(value), 'utf8').digest('hex');
}

function toMillis(value) {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (value instanceof Date) return value.getTime();
  if (value && typeof value.toMillis === 'function') return Number(value.toMillis()) || 0;
  if (value && typeof value === 'object') {
    const seconds = Number(value.seconds ?? value._seconds);
    const nanos = Number(value.nanoseconds ?? value._nanoseconds ?? 0);
    if (Number.isFinite(seconds) && Number.isFinite(nanos)) {
      return seconds * 1000 + Math.floor(nanos / 1_000_000);
    }
  }
  return 0;
}

function safeErrorCode(error) {
  const value = normalizeText(error?.code || error?.name || 'unknown_error', 96)
    .toLowerCase();
  return /^[a-z0-9_:/.-]+$/.test(value) ? value : 'unknown_error';
}

function normalizeLogger(logger) {
  return {
    error: typeof logger?.error === 'function' ? logger.error.bind(logger) : () => {},
    warn: typeof logger?.warn === 'function' ? logger.warn.bind(logger) : () => {},
  };
}

module.exports = {
  AccountDeletionRequestError,
  DEFAULT_GRACE_PERIOD_MS,
  MATERIAL_ACTIVITIES,
  createAccountDeletionRequestService,
  publicRequestResult,
  requestIdForUid,
};
