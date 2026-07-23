'use strict';

const ACTIVE_SESSION_STATUSES = new Set(['active', 'accepted', 'in_progress']);
const LOCAL_SUPPORT_SESSION_ID_PATTERN = /^[A-Za-z0-9._:-]{1,128}$/;

class ClientSessionRecoveryError extends Error {
  constructor(code, status = 400) {
    super(code);
    this.name = 'ClientSessionRecoveryError';
    this.code = code;
    this.status = status;
  }
}

function createClientSessionRecoveryService({ db, queryLimit = 10 } = {}) {
  if (!db || typeof db.collection !== 'function') {
    throw new Error('createClientSessionRecoveryService requires Firestore db');
  }
  const safeQueryLimit = Math.max(1, Math.min(25, Number(queryLimit) || 10));

  return {
    async findActiveSession({ uid, localSupportSessionId } = {}) {
      const normalizedUid = normalizeText(uid, 256);
      const normalizedLocalId = normalizeText(localSupportSessionId, 128);
      if (!normalizedUid) {
        throw new ClientSessionRecoveryError('invalid_token', 401);
      }
      if (
        !normalizedLocalId ||
        !LOCAL_SUPPORT_SESSION_ID_PATTERN.test(normalizedLocalId)
      ) {
        throw new ClientSessionRecoveryError(
          'invalid_local_support_session_id',
          400
        );
      }

      const supportSessionSnap = await db
        .collection('support_sessions')
        .doc(normalizedLocalId)
        .get();
      const supportSession = supportSessionSnap.exists
        ? supportSessionSnap.data() || {}
        : null;

      // A caller never learns whether an identifier belongs to another account.
      if (
        supportSession &&
        normalizeText(supportSession.clientUid, 256) !== normalizedUid
      ) {
        return inactiveResult();
      }

      const directSessionIds = uniqueStrings([
        supportSession?.realtimeSessionId,
        supportSession?.sessionId,
      ]);
      for (const sessionId of directSessionIds) {
        const sessionSnap = await db.collection('sessions').doc(sessionId).get();
        const result = resultFromSnapshot({
          snapshot: sessionSnap,
          uid: normalizedUid,
          localSupportSessionId: normalizedLocalId,
        });
        if (result) return result;
      }

      // Compatibility path for accepted sessions created before the direct
      // realtimeSessionId link was persisted in support_sessions.
      const sessionsCollection = db.collection('sessions');
      const candidateSnapshots = await Promise.all([
        sessionsCollection
          .where('supportSessionId', '==', normalizedLocalId)
          .limit(safeQueryLimit)
          .get(),
        sessionsCollection
          .where('supportProfile.localSupportSessionId', '==', normalizedLocalId)
          .limit(safeQueryLimit)
          .get(),
        sessionsCollection
          .where(
            'extra.supportProfile.localSupportSessionId',
            '==',
            normalizedLocalId
          )
          .limit(safeQueryLimit)
          .get(),
      ]);
      const candidateDocs = new Map();
      for (const querySnapshot of candidateSnapshots) {
        for (const snapshot of querySnapshot.docs || []) {
          candidateDocs.set(snapshot.id, snapshot);
        }
      }
      const matching = [...candidateDocs.values()]
        .map((snapshot) => ({
          snapshot,
          data: snapshot.data() || {},
        }))
        .filter(({ snapshot }) =>
          Boolean(
            resultFromSnapshot({
              snapshot,
              uid: normalizedUid,
              localSupportSessionId: normalizedLocalId,
            })
          )
        )
        .sort(
          (left, right) =>
            sessionSortTimestamp(right.data) - sessionSortTimestamp(left.data)
        );

      if (!matching.length) return inactiveResult();
      return resultFromSnapshot({
        snapshot: matching[0].snapshot,
        uid: normalizedUid,
        localSupportSessionId: normalizedLocalId,
      });
    },
  };
}

function resultFromSnapshot({
  snapshot,
  uid,
  localSupportSessionId,
} = {}) {
  if (!snapshot?.exists) return null;
  const data = snapshot.data() || {};
  if (normalizeText(data.clientUid, 256) !== uid) return null;
  if (!ACTIVE_SESSION_STATUSES.has(normalizeText(data.status, 64).toLowerCase())) {
    return null;
  }

  const storedLocalId = normalizeText(
    data.supportSessionId ||
      data.supportProfile?.localSupportSessionId ||
      data.extra?.supportProfile?.localSupportSessionId,
    128
  );
  if (!storedLocalId || storedLocalId !== localSupportSessionId) return null;

  const sessionId =
    normalizeText(data.sessionId, 64) || normalizeText(snapshot.id, 64);
  if (!sessionId) return null;
  const techName =
    normalizeText(data.techName, 128) ||
    normalizeText(data.tech?.techName, 128) ||
    normalizeText(data.tech?.name, 128) ||
    null;

  return {
    ok: true,
    active: true,
    sessionId,
    techName,
    status: normalizeText(data.status, 64).toLowerCase(),
  };
}

function inactiveResult() {
  return {
    ok: true,
    active: false,
  };
}

function sessionSortTimestamp(data = {}) {
  const value = Number(
    data.acceptedAt || data.updatedAt || data.createdAt || data.requestedAt || 0
  );
  return Number.isFinite(value) ? value : 0;
}

function uniqueStrings(values) {
  return [...new Set(values.map((value) => normalizeText(value, 128)).filter(Boolean))];
}

function normalizeText(value, maxLength) {
  if (typeof value !== 'string') return '';
  return value.trim().slice(0, maxLength);
}

module.exports = {
  ClientSessionRecoveryError,
  createClientSessionRecoveryService,
};
