'use strict';

class SupportSessionClosureError extends Error {
  constructor(code, status = 409) {
    super(code);
    this.name = 'SupportSessionClosureError';
    this.code = code;
    this.status = status;
  }
}

const compactString = (value, maxLength = 4096) => {
  if (typeof value !== 'string') return '';
  return value.trim().slice(0, maxLength);
};

const hasOwn = (value, key) =>
  Boolean(value && Object.prototype.hasOwnProperty.call(value, key));

const realtimeTechUid = (session = {}) =>
  compactString(
    session.techUid ||
      session.tech?.techUid ||
      session.tech?.uid ||
      '',
    256
  );

const realtimeTechName = (session = {}) =>
  compactString(
    session.techName ||
      session.tech?.techName ||
      session.tech?.name ||
      '',
    256
  );

const realtimeClientUid = (session = {}) =>
  compactString(session.clientUid || '', 256);

const localSupportSessionIdFromRealtime = (session = {}) =>
  compactString(
    session.supportSessionId ||
      session.localSupportSessionId ||
      session.supportProfile?.localSupportSessionId ||
      '',
    128
  );

const summaryField = (summary, primaryKey, legacyKey, maxLength) => {
  const key = hasOwn(summary, primaryKey)
    ? primaryKey
    : hasOwn(summary, legacyKey)
      ? legacyKey
      : null;
  if (!key) return { supplied: false, value: null };
  const value = summary[key];
  if (value == null) return { supplied: true, value: null };
  return {
    supplied: true,
    value: compactString(value, maxLength) || null,
  };
};

const sanitizeTechnicianSummary = (summary = {}) => {
  const problem = summaryField(
    summary,
    'problemSummary',
    'symptom',
    2000
  );
  const solution = summaryField(
    summary,
    'solutionSummary',
    'solution',
    4000
  );
  const notes = summaryField(
    summary,
    'internalNotes',
    'notes',
    4000
  );
  return {
    ...(problem.supplied ? { problemSummary: problem.value } : {}),
    ...(solution.supplied ? { solutionSummary: solution.value } : {}),
    ...(notes.supplied ? { internalNotes: notes.value } : {}),
  };
};

const assertClosureActor = ({
  realtimeSession,
  actorUid,
  actorRole,
  authorizedTech,
}) => {
  const uid = compactString(actorUid, 256);
  const role = compactString(actorRole, 32).toLowerCase();
  const techUid = realtimeTechUid(realtimeSession);
  const clientUid = realtimeClientUid(realtimeSession);

  if (role === 'client') {
    if (!uid || !clientUid || uid !== clientUid) {
      throw new SupportSessionClosureError('forbidden', 403);
    }
    return { role, uid, allowTechnicianSummary: false };
  }

  if (role === 'tech') {
    if (
      !uid ||
      !techUid ||
      uid !== techUid ||
      authorizedTech !== true
    ) {
      throw new SupportSessionClosureError('forbidden', 403);
    }
    return { role, uid, allowTechnicianSummary: true };
  }

  if (role === 'server') {
    if (
      authorizedTech !== true ||
      !uid ||
      !techUid ||
      uid !== techUid
    ) {
      throw new SupportSessionClosureError('forbidden', 403);
    }
    return {
      role,
      uid,
      allowTechnicianSummary: true,
    };
  }

  throw new SupportSessionClosureError('forbidden', 403);
};

const valuesMatch = (left, right) => {
  if (left === right) return true;
  if (left == null || right == null) return false;
  if (
    typeof left.toMillis === 'function' &&
    typeof right.toMillis === 'function'
  ) {
    return left.toMillis() === right.toMillis();
  }
  return false;
};

const buildSupportSessionClosure = ({
  realtimeSessionId = '',
  realtimeSession = null,
  supportSession = null,
  actorUid = '',
  actorRole = '',
  authorizedTech = false,
  summary = {},
  now = Date.now(),
  expiresAt = null,
} = {}) => {
  if (!realtimeSession || typeof realtimeSession !== 'object') {
    throw new SupportSessionClosureError('realtime_session_not_found', 404);
  }
  if (!supportSession || typeof supportSession !== 'object') {
    throw new SupportSessionClosureError('support_session_not_found', 404);
  }

  const sessionId = compactString(realtimeSessionId, 64);
  if (!sessionId) {
    throw new SupportSessionClosureError('invalid_session_id', 400);
  }
  const declaredRealtimeSessionId = compactString(
    realtimeSession.sessionId || '',
    64
  );
  if (declaredRealtimeSessionId && declaredRealtimeSessionId !== sessionId) {
    throw new SupportSessionClosureError('realtime_session_mismatch', 409);
  }
  const actor = assertClosureActor({
    realtimeSession,
    actorUid,
    actorRole,
    authorizedTech,
  });

  const clientUid = realtimeClientUid(realtimeSession);
  const storedClientUid = compactString(supportSession.clientUid || '', 256);
  if (clientUid && storedClientUid && clientUid !== storedClientUid) {
    throw new SupportSessionClosureError('support_session_owner_mismatch', 409);
  }

  const storedRealtimeSessionId = compactString(
    supportSession.realtimeSessionId ||
      supportSession.sessionId ||
      '',
    64
  );
  if (storedRealtimeSessionId && storedRealtimeSessionId !== sessionId) {
    throw new SupportSessionClosureError('support_session_realtime_mismatch', 409);
  }

  const techUid = realtimeTechUid(realtimeSession);
  const storedTechUid = compactString(supportSession.techId || '', 256);
  if (techUid && storedTechUid && techUid !== storedTechUid) {
    throw new SupportSessionClosureError('support_session_tech_mismatch', 409);
  }

  const currentStatus = compactString(supportSession.status, 64).toLowerCase();
  if (currentStatus === 'cancelled' || currentStatus === 'canceled') {
    return {
      alreadyFinalized: true,
      finalStatus: 'cancelled',
      patch: {},
      shouldWrite: false,
    };
  }

  const alreadyCompleted = currentStatus === 'completed';
  const endedAt =
    Number.isFinite(Number(supportSession.endedAt)) &&
    Number(supportSession.endedAt) > 0
      ? Number(supportSession.endedAt)
      : Number(now);
  const effectiveExpiresAt = supportSession.expiresAt || expiresAt;
  const desired = {
    status: 'completed',
    queueStatus: 'completed',
    endedAt,
    sessionId,
    realtimeSessionId: sessionId,
    ...(effectiveExpiresAt ? { expiresAt: effectiveExpiresAt } : {}),
    ...(techUid ? { techId: techUid } : {}),
    ...(realtimeTechName(realtimeSession)
      ? { techName: realtimeTechName(realtimeSession) }
      : {}),
    ...(actor.allowTechnicianSummary
      ? sanitizeTechnicianSummary(summary)
      : {}),
  };

  const patch = {};
  for (const [key, value] of Object.entries(desired)) {
    if (!valuesMatch(supportSession[key], value)) patch[key] = value;
  }
  if (Object.keys(patch).length) patch.updatedAt = Number(now);

  return {
    alreadyFinalized: alreadyCompleted,
    finalStatus: 'completed',
    patch,
    shouldWrite: Object.keys(patch).length > 0,
  };
};

module.exports = {
  SupportSessionClosureError,
  buildSupportSessionClosure,
  localSupportSessionIdFromRealtime,
  sanitizeTechnicianSummary,
};
