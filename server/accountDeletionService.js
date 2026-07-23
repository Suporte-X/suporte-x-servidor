const crypto = require('node:crypto');

const DEFAULT_COLLECTIONS = Object.freeze({
  operations: 'account_deletion_operations',
  clients: 'clients',
  clientProfiles: 'client_profiles',
  clientLinks: 'client_app_links',
  clientVerifications: 'client_verifications',
  pnvRequests: 'pnv_requests',
  sessions: 'sessions',
  supportSessions: 'support_sessions',
  supportReports: 'support_reports',
  requests: 'requests',
  queueNotifications: 'queue_notifications',
  queueLocks: 'support_queue_locks',
  queueAnchors: 'support_queue_anchors',
  queueOutcomes: 'support_queue_outcomes',
  supportTechLocks: 'support_tech_locks',
  clientDevices: 'client_devices',
  clientNotifications: 'client_notifications',
  notificationEvents: 'notification_events',
  adminNotifications: 'admin_notifications',
  creditOrders: 'credit_orders',
  creditAdjustments: 'credit_adjustment_requests',
  whatsappConversations: 'whatsapp_api_conversations',
  legacyRooms: 'legacy_webrtc_rooms',
});

const TERMINAL_SUPPORT_STATES = new Set([
  'closed',
  'ended',
  'completed',
  'cancelled',
  'canceled',
  'rejected',
  'failed',
  'expired',
  'removed',
]);

const DEFAULT_OPERATION_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const DEFAULT_PROCESSING_STALE_MS = 2 * 60 * 1000;

class AccountDeletionError extends Error {
  constructor(status, code, message = code) {
    super(message);
    this.name = 'AccountDeletionError';
    this.status = status;
    this.code = code;
  }
}

function createAccountDeletionService({
  db,
  auth,
  bucket,
  verifyPnvToken = null,
  normalizePhone = defaultNormalizePhone,
  clock = () => Date.now(),
  logger = console,
  collections = {},
  operationTtlMs = DEFAULT_OPERATION_TTL_MS,
  processingStaleMs = DEFAULT_PROCESSING_STALE_MS,
  retainFinancialRecords = true,
} = {}) {
  if (
    !db ||
    typeof db.collection !== 'function' ||
    typeof db.recursiveDelete !== 'function' ||
    typeof db.runTransaction !== 'function'
  ) {
    throw new Error(
      'createAccountDeletionService requires Firestore with collection(), recursiveDelete() and runTransaction()'
    );
  }
  if (!auth || typeof auth.deleteUser !== 'function') {
    throw new Error('createAccountDeletionService requires auth.deleteUser()');
  }
  if (!bucket || typeof bucket.deleteFiles !== 'function') {
    throw new Error('createAccountDeletionService requires bucket.deleteFiles()');
  }

  const names = { ...DEFAULT_COLLECTIONS, ...collections };
  const log = normalizeLogger(logger);

  async function deleteAccount({
    uid,
    confirmation,
    idempotencyKey,
    pnvToken = '',
    pnvPhone = '',
  } = {}) {
    const normalizedUid = normalizeIdentifier(uid, 256);
    const normalizedKey = normalizeIdempotencyKey(idempotencyKey);
    if (!normalizedUid) {
      throw new AccountDeletionError(401, 'invalid_token');
    }
    if (confirmation !== 'EXCLUIR CONTA') {
      throw new AccountDeletionError(400, 'confirmation_required');
    }
    if (!normalizedKey) {
      throw new AccountDeletionError(400, 'idempotency_key_required');
    }

    // One lock document per Firebase UID prevents concurrent deletion attempts
    // with different idempotency keys from racing across server instances.
    const operationId = sha256(normalizedUid);
    const operationRef = db.collection(names.operations).doc(operationId);
    const existingOperation = await operationRef.get();
    const existingData = existingOperation.exists ? existingOperation.data() || {} : {};
    if (existingData.status === 'completed' && existingData.result) {
      return clonePublicResult(existingData.result);
    }
    if (
      existingData.status === 'processing' &&
      Number(existingData.updatedAt || 0) > clock() - processingStaleMs
    ) {
      throw new AccountDeletionError(409, 'deletion_in_progress');
    }

    let context = normalizeStoredContext(existingData.context, normalizePhone);
    if (!context) {
      context = await resolveServerIdentity(normalizedUid);
    }
    await requireRecentPnvWhenNeeded({
      context,
      token: pnvToken,
      claimedPhone: pnvPhone,
    });
    await assertNoActiveSupport(context);

    const startedAt = clock();
    const claimResult = await db.runTransaction(async (transaction) => {
      const currentSnapshot = await transaction.get(operationRef);
      const currentData = currentSnapshot.exists
        ? currentSnapshot.data() || {}
        : {};
      if (currentData.status === 'completed' && currentData.result) {
        return {
          completed: true,
          result: clonePublicResult(currentData.result),
        };
      }
      if (
        currentData.status === 'processing' &&
        Number(currentData.updatedAt || 0) > startedAt - processingStaleMs
      ) {
        throw new AccountDeletionError(409, 'deletion_in_progress');
      }
      transaction.set(operationRef, {
        status: 'processing',
        uidHash: sha256(normalizedUid),
        idempotencyHash: sha256(normalizedKey),
        context,
        createdAt: Number(currentData.createdAt || existingData.createdAt || startedAt),
        updatedAt: startedAt,
        expiresAt: new Date(startedAt + operationTtlMs),
      });
      return { completed: false, result: null };
    });
    if (claimResult.completed) {
      return claimResult.result;
    }

    try {
      // Close the race between the initial eligibility check and the
      // transactional deletion claim. Queue creation also reads this claim in
      // its own transaction, so either the support request wins first and this
      // check observes it, or the deletion claim wins and queueing is blocked.
      await assertNoActiveSupport(context);

      const sessionIds = await resolveSessionIds(context);
      context = {
        ...context,
        sessionIds: uniqueStrings([...(context.sessionIds || []), ...sessionIds]),
      };
      await operationRef.set(
        {
          context,
          updatedAt: clock(),
        },
        { merge: true }
      );

      const result = await executeDeletion({
        context,
        operationId,
      });

      try {
        const completedAt = clock();
        await operationRef.set({
          status: 'completed',
          uidHash: sha256(normalizedUid),
          idempotencyHash: sha256(normalizedKey),
          result,
          createdAt: Number(existingData.createdAt || startedAt),
          completedAt,
          updatedAt: completedAt,
          expiresAt: new Date(completedAt + operationTtlMs),
        });
      } catch (error) {
        // At this point user data and Auth are already deleted. A bookkeeping
        // failure must not turn a completed privacy operation into a user error.
        log.error('Failed to persist completed account deletion operation', error);
      }

      return clonePublicResult(result);
    } catch (error) {
      const failureCode =
        error instanceof AccountDeletionError
          ? error.code
          : 'account_deletion_failed';
      try {
        await operationRef.set(
          {
            status: 'failed',
            context,
            errorCode: failureCode,
            updatedAt: clock(),
            expiresAt: new Date(clock() + operationTtlMs),
          },
          { merge: true }
        );
      } catch (writeError) {
        log.error('Failed to persist failed account deletion operation', writeError);
      }
      throw error;
    }
  }

  async function resolveServerIdentity(uid) {
    const linkCollection = db.collection(names.clientLinks);
    const linkDocs = [];
    const directLink = await linkCollection.doc(uid).get();
    if (directLink.exists) linkDocs.push(directLink);
    linkDocs.push(...(await queryDocs(names.clientLinks, 'clientUid', uid)));

    const clientIds = uniqueStrings(
      linkDocs.map((doc) => normalizeIdentifier(doc.data()?.clientId, 128))
    );
    if (clientIds.length > 1) {
      throw new AccountDeletionError(409, 'identity_conflict');
    }

    let clientId = clientIds[0] || null;
    if (!clientId) {
      const uidClientId = clientDocIdFromUid(uid);
      const uidClientSnapshot = await db.collection(names.clients).doc(uidClientId).get();
      if (uidClientSnapshot.exists) clientId = uidClientId;
    }

    const relatedLinks = [...linkDocs];
    if (clientId) {
      relatedLinks.push(...(await queryDocs(names.clientLinks, 'clientId', clientId)));
    }
    const uniqueLinks = uniqueDocs(relatedLinks);
    const linkedUids = uniqueStrings([
      uid,
      ...uniqueLinks.map((doc) => normalizeIdentifier(doc.data()?.clientUid, 256)),
    ]);

    let verifiedPhone = null;
    if (clientId) {
      const verificationSnapshot = await db
        .collection(names.clientVerifications)
        .doc(clientId)
        .get();
      if (verificationSnapshot.exists) {
        const verification = verificationSnapshot.data() || {};
        if (String(verification.status || '').trim().toLowerCase() === 'verified') {
          verifiedPhone =
            normalizePhone(verification.verifiedPhone) ||
            normalizePhone(verification.primaryPhone);
        }
      }
    }
    if (!verifiedPhone) {
      const verifiedLink = uniqueLinks
        .map((doc) => doc.data() || {})
        .find((data) => data.phoneVerified === true && normalizePhone(data.phone));
      verifiedPhone = verifiedLink ? normalizePhone(verifiedLink.phone) : null;
    }

    return {
      uid,
      clientId,
      linkedUids,
      verifiedPhone,
      sessionIds: [],
    };
  }

  async function requireRecentPnvWhenNeeded({ context, token, claimedPhone }) {
    if (!context.verifiedPhone) return;
    if (!token || typeof verifyPnvToken !== 'function') {
      throw new AccountDeletionError(403, 'pnv_verification_required');
    }

    let verification;
    try {
      verification = await verifyPnvToken({
        token: String(token).trim(),
        expectedPhone: context.verifiedPhone,
        claimedPhone: normalizePhone(claimedPhone),
        uid: context.uid,
      });
    } catch (error) {
      log.warn('PNV verifier rejected account deletion proof', {
        code: safeErrorCode(error),
      });
      throw new AccountDeletionError(403, 'invalid_pnv_verification');
    }

    const verifiedPhone = normalizePhone(verification?.phone);
    if (verification?.ok !== true || !verifiedPhone || verifiedPhone !== context.verifiedPhone) {
      throw new AccountDeletionError(403, 'invalid_pnv_verification');
    }
  }

  async function assertNoActiveSupport(context) {
    const checks = [
      [names.sessions, ['clientUid', 'clientRecordId', 'clientPhone']],
      [names.supportSessions, ['clientUid', 'clientId', 'clientPhone']],
      [names.requests, ['clientUid', 'clientRecordId', 'clientPhone']],
    ];

    for (const [collectionName, fields] of checks) {
      const docs = await collectIdentityDocs(collectionName, fields, context);
      const active = docs.some((doc) => isActiveSupportRecord(doc.data() || {}));
      if (active) {
        throw new AccountDeletionError(
          409,
          'active_support',
          'End the active support or queue request before deleting the account.'
        );
      }
    }
  }

  async function resolveSessionIds(context) {
    const realtimeSessions = await collectIdentityDocs(
      names.sessions,
      ['clientUid', 'clientRecordId', 'clientPhone'],
      context
    );
    const supportSessions = await collectIdentityDocs(
      names.supportSessions,
      ['clientUid', 'clientId', 'clientPhone'],
      context
    );
    return uniqueStrings([
      ...realtimeSessions.map((doc) => doc.id),
      ...supportSessions.map((doc) => normalizeIdentifier(doc.data()?.sessionId, 128)),
      ...supportSessions.map((doc) =>
        normalizeIdentifier(doc.data()?.realtimeSessionId, 128)
      ),
    ]);
  }

  async function executeDeletion({ context, operationId }) {
    const deletedCounts = {};
    const retained = [];
    const sessionIds = uniqueStrings(context.sessionIds || []);

    // Storage first: if object deletion fails, Firestore still contains enough
    // identity context to retry. recursiveDelete removes all known and unknown
    // Firestore subcollections for that session.
    for (const sessionId of sessionIds) {
      await bucket.deleteFiles({ prefix: `sessions/${sessionId}/` });
      await db.recursiveDelete(db.collection(names.sessions).doc(sessionId));
    }
    if (sessionIds.length) {
      deletedCounts.sessions = sessionIds.length;
      deletedCounts.storageSessionPrefixes = sessionIds.length;
    }

    if (context.verifiedPhone) {
      const whatsappDocs = await collectWhatsappDocuments(context.verifiedPhone);
      for (const doc of whatsappDocs) {
        await db.recursiveDelete(doc.ref);
      }
      if (whatsappDocs.length) {
        deletedCounts.whatsappConversations = whatsappDocs.length;
      }
    }

    const deleteSpecs = [
      [names.pnvRequests, ['clientUid', 'clientId', 'phone']],
      [names.clientDevices, ['clientUid', 'clientId']],
      [names.clientNotifications, ['clientUid', 'clientId']],
      [names.notificationEvents, ['clientUid', 'clientId']],
      [names.requests, ['clientUid', 'clientRecordId', 'clientPhone']],
      [names.queueNotifications, ['clientUid', 'clientRecordId', 'clientPhone']],
      [names.queueLocks, ['clientUid']],
      [names.queueAnchors, ['clientUid']],
      [names.queueOutcomes, ['clientUid']],
      [names.supportTechLocks, ['realtimeSessionId', 'sessionId']],
      [names.legacyRooms, ['ownerUid']],
      [names.supportSessions, ['clientUid', 'clientId', 'clientPhone']],
      [names.supportReports, ['clientUid', 'clientId', 'clientPhone']],
      [names.adminNotifications, ['metadata.clientId']],
    ];

    for (const [collectionName, fields] of deleteSpecs) {
      const docs = await collectIdentityDocs(collectionName, fields, context);
      await deleteDocuments(docs);
      if (docs.length) deletedCounts[collectionName] = docs.length;
    }

    const creditOrders = await collectIdentityDocs(
      names.creditOrders,
      ['clientUid', 'clientId'],
      context
    );
    const creditAdjustments = await collectIdentityDocs(
      names.creditAdjustments,
      ['clientUid', 'clientId'],
      context
    );
    if (retainFinancialRecords) {
      await anonymizeCreditOrders(creditOrders, operationId);
      await anonymizeCreditAdjustments(creditAdjustments, operationId);
      if (creditOrders.length || creditAdjustments.length) {
        retained.push({
          category: 'financial_ledger',
          reason: 'legal_or_accounting_obligation',
        });
      }
    } else {
      await deleteDocuments([...creditOrders, ...creditAdjustments]);
    }

    const links = await collectIdentityDocs(
      names.clientLinks,
      ['clientUid', 'clientId'],
      context
    );
    await deleteDocuments(links);
    if (links.length) deletedCounts[names.clientLinks] = links.length;

    if (context.clientId) {
      const directRefs = [
        db.collection(names.clients).doc(context.clientId),
        db.collection(names.clientProfiles).doc(context.clientId),
        db.collection(names.clientVerifications).doc(context.clientId),
      ];
      for (const ref of directRefs) {
        await db.recursiveDelete(ref);
      }
      deletedCounts.clientContext = directRefs.length;
    }

    // Firebase Auth deletion is deliberately the final destructive phase.
    const authUids = uniqueStrings(context.linkedUids || [context.uid]);
    const nonCurrentUids = authUids.filter((linkedUid) => linkedUid !== context.uid);
    for (const linkedUid of nonCurrentUids) {
      await deleteAuthUserIfPresent(linkedUid);
    }
    await deleteAuthUserIfPresent(context.uid);

    return {
      ok: true,
      deleted: true,
      deletedAt: clock(),
      deletedCounts,
      retained,
    };
  }

  async function collectIdentityDocs(collectionName, fields, context) {
    const valuesByField = new Map();
    for (const field of fields) {
      const values = [];
      if (field === 'clientUid') values.push(...(context.linkedUids || []));
      if (field === 'ownerUid') values.push(...(context.linkedUids || []));
      if (field === 'realtimeSessionId' || field === 'sessionId') {
        values.push(...(context.sessionIds || []));
      }
      if (field === 'clientId' || field === 'clientRecordId' || field === 'metadata.clientId') {
        values.push(context.clientId);
      }
      if (field === 'phone' || field === 'clientPhone') values.push(context.verifiedPhone);
      valuesByField.set(field, uniqueStrings(values));
    }

    const docs = [];
    for (const [field, values] of valuesByField.entries()) {
      for (const value of values) {
        docs.push(...(await queryDocs(collectionName, field, value)));
      }
    }
    return uniqueDocs(docs);
  }

  async function collectWhatsappDocuments(phone) {
    const normalizedPhone = normalizePhone(phone);
    if (!normalizedPhone) return [];
    const digits = normalizedPhone.replace(/\D/g, '');
    const collection = db.collection(names.whatsappConversations);
    const docs = [];
    const canonical = await collection.doc(`p_${digits}`).get();
    if (canonical.exists) docs.push(canonical);
    docs.push(...(await queryDocs(names.whatsappConversations, 'phoneDigits', digits)));
    docs.push(...(await queryDocs(names.whatsappConversations, 'phone', normalizedPhone)));
    return uniqueDocs(docs);
  }

  async function queryDocs(collectionName, field, value) {
    if (!value) return [];
    const snapshot = await db.collection(collectionName).where(field, '==', value).get();
    return snapshot?.docs || [];
  }

  async function deleteDocuments(docs) {
    for (const doc of uniqueDocs(docs)) {
      await db.recursiveDelete(doc.ref);
    }
  }

  async function anonymizeCreditOrders(docs, operationId) {
    for (const doc of uniqueDocs(docs)) {
      const data = doc.data() || {};
      await doc.ref.set({
        packageId: data.packageId || null,
        status: data.status || null,
        paymentMethod: data.paymentMethod || null,
        amountCents: finiteNumberOrNull(data.amountCents),
        whatsappRequested: data.whatsappRequested === true,
        pixPlaceholder: data.pixPlaceholder === true,
        cardPlaceholder: data.cardPlaceholder === true,
        createdAt: data.createdAt || null,
        updatedAt: data.updatedAt || null,
        privacyDeleted: true,
        privacyDeletedAt: clock(),
        privacyDeletionRef: operationId,
      });
    }
  }

  async function anonymizeCreditAdjustments(docs, operationId) {
    for (const doc of uniqueDocs(docs)) {
      const data = doc.data() || {};
      const change = data.creditChange && typeof data.creditChange === 'object'
        ? data.creditChange
        : {};
      // The original document ID is `${clientId}_${idempotencyKey}`. Merely
      // clearing fields would leave a customer identifier in the Firestore
      // path, so retain the ledger under a deterministic anonymous path and
      // remove the identifying source document only after the copy succeeds.
      const anonymousId = `privacy_${sha256(doc.ref.path)}`;
      const anonymousRef = db.collection(names.creditAdjustments).doc(anonymousId);
      await anonymousRef.set({
        idempotencyHash: data.idempotencyKey
          ? sha256(data.idempotencyKey)
          : null,
        requestedBy: data.requestedBy || null,
        creditChange: {
          previousCredits: finiteNumberOrNull(change.previousCredits),
          credits: finiteNumberOrNull(change.credits),
          requestedDelta: finiteNumberOrNull(change.requestedDelta),
          appliedDelta: finiteNumberOrNull(change.appliedDelta),
        },
        createdAt: data.createdAt || null,
        privacyDeleted: true,
        privacyDeletedAt: clock(),
        privacyDeletionRef: operationId,
      });
      if (anonymousRef.path !== doc.ref.path) {
        await db.recursiveDelete(doc.ref);
      }
    }
  }

  async function deleteAuthUserIfPresent(uid) {
    if (!uid) return;
    try {
      await auth.deleteUser(uid);
    } catch (error) {
      const code = String(error?.code || '');
      if (code === 'auth/user-not-found' || code === 'user-not-found') return;
      throw error;
    }
  }

  return {
    deleteAccount,
    resolveServerIdentity,
    assertNoActiveSupport,
  };
}

function isActiveSupportRecord(data) {
  const state = String(data.status || data.state || data.queueStatus || '')
    .trim()
    .toLowerCase();
  if (!state) return true;
  return !TERMINAL_SUPPORT_STATES.has(state);
}

function isAccountDeletionBlocking(data) {
  const status = String(data?.status || '').trim().toLowerCase();
  return status === 'processing' || status === 'completed';
}

function normalizeStoredContext(value, normalizePhone = defaultNormalizePhone) {
  if (!value || typeof value !== 'object') return null;
  const uid = normalizeIdentifier(value.uid, 256);
  if (!uid) return null;
  return {
    uid,
    clientId: normalizeIdentifier(value.clientId, 128) || null,
    linkedUids: uniqueStrings([uid, ...(Array.isArray(value.linkedUids) ? value.linkedUids : [])]),
    verifiedPhone: normalizePhone(value.verifiedPhone),
    sessionIds: uniqueStrings(Array.isArray(value.sessionIds) ? value.sessionIds : []),
  };
}

function defaultNormalizePhone(value) {
  if (typeof value !== 'string' && typeof value !== 'number') return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const digits = raw.replace(/\D/g, '');
  if (digits.length < 10 || digits.length > 15) return null;
  if (raw.startsWith('+')) return `+${digits}`;
  if (raw.startsWith('00') && digits.length > 2) return `+${digits.slice(2)}`;
  if ((digits.length === 10 || digits.length === 11) && !digits.startsWith('55')) {
    return `+55${digits}`;
  }
  return `+${digits}`;
}

function normalizeIdempotencyKey(value) {
  if (typeof value !== 'string') return '';
  const normalized = value.trim();
  return /^[a-zA-Z0-9._:-]{1,160}$/.test(normalized) ? normalized : '';
}

function normalizeIdentifier(value, maxLength) {
  if (typeof value !== 'string' && typeof value !== 'number') return '';
  return String(value).trim().slice(0, maxLength);
}

function clientDocIdFromUid(uid) {
  const safe = normalizeIdentifier(uid, 256).replace(/[^a-zA-Z0-9]/g, '');
  return `uid_${safe}`;
}

function uniqueStrings(values) {
  return Array.from(
    new Set(
      values
        .map((value) => normalizeIdentifier(value, 512))
        .filter(Boolean)
    )
  );
}

function uniqueDocs(docs) {
  const byPath = new Map();
  for (const doc of docs || []) {
    if (!doc?.ref) continue;
    const path = doc.ref.path || `${doc.ref.parent?.id || ''}/${doc.id || ''}`;
    if (!path) continue;
    byPath.set(path, doc);
  }
  return Array.from(byPath.values());
}

function finiteNumberOrNull(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function sha256(value) {
  return crypto.createHash('sha256').update(String(value), 'utf8').digest('hex');
}

function normalizeLogger(logger) {
  return {
    error: typeof logger?.error === 'function' ? logger.error.bind(logger) : () => {},
    warn: typeof logger?.warn === 'function' ? logger.warn.bind(logger) : () => {},
  };
}

function safeErrorCode(error) {
  const raw = String(error?.code || error?.name || 'unknown_error')
    .trim()
    .toLowerCase();
  return /^[a-z0-9_:/.-]{1,96}$/.test(raw) ? raw : 'unknown_error';
}

function clonePublicResult(result) {
  return {
    ok: result?.ok === true,
    deleted: result?.deleted === true,
    deletedAt: Number(result?.deletedAt || 0) || null,
    deletedCounts:
      result?.deletedCounts && typeof result.deletedCounts === 'object'
        ? { ...result.deletedCounts }
        : {},
    retained: Array.isArray(result?.retained)
      ? result.retained.map((item) => ({ ...item }))
      : [],
  };
}

module.exports = {
  AccountDeletionError,
  DEFAULT_COLLECTIONS,
  createAccountDeletionService,
  defaultNormalizePhone,
  isAccountDeletionBlocking,
  isActiveSupportRecord,
};
