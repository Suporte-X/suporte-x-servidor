'use strict';

class SupportQueuePolicyError extends Error {
  constructor(code, status = 409, details = {}) {
    super(code);
    this.name = 'SupportQueuePolicyError';
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

const normalizeId = (value, maxLength = 256) =>
  String(value || '')
    .trim()
    .slice(0, maxLength);

const normalizedState = (value, fallback = '') =>
  normalizeId(value, 64).toLowerCase() || fallback;

const ACTIVE_REALTIME_SESSION_STATES = new Set([
  'active',
  'accepted',
  'in_progress',
]);
const TERMINAL_REALTIME_SESSION_STATES = new Set([
  'closed',
  'ended',
  'completed',
  'cancelled',
  'canceled',
]);

const decideTechSupportAvailability = ({
  requestedSessionId,
  lockedSessionId = '',
  lockedSession = null,
} = {}) => {
  const requestedId = normalizeId(requestedSessionId, 64);
  const lockedId = normalizeId(lockedSessionId, 64);
  if (!requestedId) {
    throw new SupportQueuePolicyError('invalid_payload', 400);
  }

  if (
    lockedId &&
    lockedId !== requestedId &&
    lockedSession &&
    ACTIVE_REALTIME_SESSION_STATES.has(
      normalizedState(lockedSession.status)
    )
  ) {
    throw new SupportQueuePolicyError('active_session_exists', 409, {
      sessionId: lockedId,
    });
  }

  return {
    allowed: true,
    requestedSessionId: requestedId,
  };
};

const decideTechSessionClaim = ({ session, techUid } = {}) => {
  const uid = normalizeId(techUid);
  if (!uid) {
    throw new SupportQueuePolicyError('invalid_token', 401);
  }
  if (!session || typeof session !== 'object') {
    throw new SupportQueuePolicyError('session_not_found', 404);
  }

  const currentTechUid = normalizeId(
    session.techUid ||
      session.tech?.techUid ||
      session.tech?.uid ||
      session.techId ||
      session.tech?.techId ||
      session.tech?.id
  );
  if (currentTechUid && currentTechUid !== uid) {
    throw new SupportQueuePolicyError('already_claimed', 409);
  }

  const status = normalizedState(session.status, 'open');
  if (TERMINAL_REALTIME_SESSION_STATES.has(status)) {
    throw new SupportQueuePolicyError('session_not_claimable', 409);
  }

  return {
    action: currentTechUid === uid ? 'reuse' : 'claim',
    status: 'active',
    techUid: uid,
  };
};

const decideTechQueueRemoval = ({ request = null, outcome = null } = {}) => {
  const outcomeStatus = normalizedState(outcome?.status);
  if (['accepted', 'active', 'in_progress', 'completed'].includes(outcomeStatus)) {
    throw new SupportQueuePolicyError('request_not_queued', 409, {
      sessionId:
        normalizeId(
          outcome?.realtimeSessionId || outcome?.sessionId || '',
          64
        ) || null,
    });
  }

  if (!request) {
    return {
      action: 'already_removed',
      removed: false,
    };
  }

  if (normalizedState(request.state, 'queued') !== 'queued') {
    throw new SupportQueuePolicyError('request_not_queued', 409);
  }

  return {
    action: 'remove',
    removed: true,
  };
};

const assertOwnedSupportSession = ({ supportSession, authUid, localSupportSessionId }) => {
  if (!supportSession) {
    throw new SupportQueuePolicyError('local_support_session_not_found', 404, {
      localSupportSessionId,
    });
  }
  const ownerUid = normalizeId(supportSession.clientUid);
  if (!ownerUid || ownerUid !== authUid) {
    throw new SupportQueuePolicyError('forbidden', 403, { localSupportSessionId });
  }
};

const isQueuedRequestForOwner = (request, authUid) =>
  Boolean(
    request &&
      normalizedState(request.state, 'queued') === 'queued' &&
      normalizeId(request.clientUid) === authUid
  );

const decideQueueReservation = ({
  authUid,
  localSupportSessionId,
  supportSession,
  anchor = null,
  anchorRequest = null,
  uidLock = null,
  lockRequest = null,
  generatedRequestId,
} = {}) => {
  const uid = normalizeId(authUid);
  const localId = normalizeId(localSupportSessionId, 128);
  const candidateRequestId = normalizeId(generatedRequestId, 64).toUpperCase();
  if (!uid) throw new SupportQueuePolicyError('missing_token', 401);
  if (!localId) throw new SupportQueuePolicyError('local_support_session_required', 400);
  if (!candidateRequestId) throw new SupportQueuePolicyError('invalid_request_id', 500);

  assertOwnedSupportSession({
    supportSession,
    authUid: uid,
    localSupportSessionId: localId,
  });

  const supportStatus = normalizedState(supportSession.status);
  const queueStatus = normalizedState(supportSession.queueStatus);
  const realtimeSessionId =
    normalizeId(
      supportSession.realtimeSessionId ||
        supportSession.sessionId ||
        ''
    ) || null;
  const supportRequestId = normalizeId(supportSession.queueRequestId, 64).toUpperCase() || null;

  if (
    realtimeSessionId ||
    ['accepted', 'in_progress', 'completed'].includes(queueStatus) ||
    ['in_progress', 'completed'].includes(supportStatus)
  ) {
    return {
      action: 'already_accepted',
      requestId:
        supportRequestId ||
        normalizeId(anchor?.requestId, 64).toUpperCase() ||
        candidateRequestId,
      localSupportSessionId: localId,
      realtimeSessionId,
      reused: true,
    };
  }

  if (['cancelled', 'canceled'].includes(queueStatus) || ['cancelled', 'canceled'].includes(supportStatus)) {
    throw new SupportQueuePolicyError('local_support_session_cancelled', 409, {
      localSupportSessionId: localId,
      requestId: supportRequestId,
    });
  }

  const anchorRequestId = normalizeId(anchor?.requestId, 64).toUpperCase();
  if (anchorRequestId && anchorRequest) {
    const anchorOwner = normalizeId(anchorRequest.clientUid);
    const anchorLocalId = normalizeId(anchorRequest.localSupportSessionId, 128);
    if (anchorOwner !== uid || anchorLocalId !== localId) {
      throw new SupportQueuePolicyError('queue_anchor_conflict', 409, {
        localSupportSessionId: localId,
      });
    }
    if (isQueuedRequestForOwner(anchorRequest, uid)) {
      return {
        action: 'reuse',
        requestId: anchorRequestId,
        localSupportSessionId: localId,
        reused: true,
      };
    }
  }

  const lockRequestId = normalizeId(uidLock?.requestId, 64).toUpperCase();
  if (lockRequestId && lockRequest && isQueuedRequestForOwner(lockRequest, uid)) {
    const lockedLocalId =
      normalizeId(uidLock?.localSupportSessionId, 128) ||
      normalizeId(lockRequest.localSupportSessionId, 128);
    if (lockedLocalId === localId) {
      return {
        action: 'reuse',
        requestId: lockRequestId,
        localSupportSessionId: localId,
        reused: true,
      };
    }
    throw new SupportQueuePolicyError('active_support_request_exists', 409, {
      requestId: lockRequestId,
      localSupportSessionId: lockedLocalId || null,
    });
  }

  return {
    action: 'create',
    requestId: anchorRequestId && !anchorRequest ? anchorRequestId : candidateRequestId,
    localSupportSessionId: localId,
    reused: false,
  };
};

const decideQueueCancellation = ({
  authUid,
  requestedRequestId = '',
  requestedLocalSupportSessionId = '',
  request = null,
  supportSession = null,
  outcome = null,
} = {}) => {
  const uid = normalizeId(authUid);
  if (!uid) throw new SupportQueuePolicyError('missing_token', 401);

  const suppliedRequestId = normalizeId(requestedRequestId, 64).toUpperCase();
  const suppliedLocalId = normalizeId(requestedLocalSupportSessionId, 128);
  const storedRequestId =
    normalizeId(request?.requestId, 64).toUpperCase() ||
    normalizeId(supportSession?.queueRequestId, 64).toUpperCase() ||
    normalizeId(outcome?.requestId, 64).toUpperCase();
  const storedLocalId =
    normalizeId(request?.localSupportSessionId, 128) ||
    normalizeId(outcome?.localSupportSessionId, 128);
  if (suppliedRequestId && storedRequestId && suppliedRequestId !== storedRequestId) {
    throw new SupportQueuePolicyError('request_mismatch', 409);
  }
  if (suppliedLocalId && storedLocalId && suppliedLocalId !== storedLocalId) {
    throw new SupportQueuePolicyError('request_mismatch', 409);
  }

  const requestId =
    suppliedRequestId ||
    storedRequestId ||
    null;
  const localSupportSessionId =
    suppliedLocalId ||
    storedLocalId ||
    null;

  const owners = [
    request?.clientUid,
    supportSession?.clientUid,
    outcome?.clientUid,
  ]
    .map((value) => normalizeId(value))
    .filter(Boolean);
  if (owners.some((owner) => owner !== uid)) {
    throw new SupportQueuePolicyError('forbidden', 403, {
      requestId,
      localSupportSessionId,
    });
  }

  const outcomeStatus = normalizedState(outcome?.status);
  const supportStatus = normalizedState(supportSession?.queueStatus || supportSession?.status);
  if (
    ['cancelled', 'canceled'].includes(outcomeStatus) ||
    ['cancelled', 'canceled'].includes(supportStatus)
  ) {
    return {
      action: 'already_cancelled',
      requestId,
      localSupportSessionId,
      removed: false,
    };
  }
  if (['accepted', 'in_progress', 'completed'].includes(outcomeStatus)) {
    throw new SupportQueuePolicyError('request_not_queued', 409, {
      requestId,
      localSupportSessionId,
    });
  }

  if (request) {
    if (!owners.length) {
      throw new SupportQueuePolicyError('forbidden', 403, {
        requestId,
        localSupportSessionId,
      });
    }
    if (normalizedState(request.state, 'queued') !== 'queued') {
      throw new SupportQueuePolicyError('request_not_queued', 409, {
        requestId,
        localSupportSessionId,
      });
    }
    return {
      action: 'cancel',
      requestId,
      localSupportSessionId,
      removed: true,
    };
  }

  if (supportSession && normalizeId(supportSession.clientUid) === uid) {
    return {
      action: 'cancel',
      requestId,
      localSupportSessionId,
      removed: false,
    };
  }

  if (outcome && normalizeId(outcome.clientUid) === uid) {
    throw new SupportQueuePolicyError('request_not_queued', 409, {
      requestId,
      localSupportSessionId,
    });
  }

  throw new SupportQueuePolicyError('support_request_not_found', 404, {
    requestId,
    localSupportSessionId,
  });
};

const evaluateAuthoritativeBilling = ({
  client,
  requestId,
} = {}) => {
  if (!client) {
    throw new SupportQueuePolicyError('client_not_registered', 409);
  }
  const credits = Math.max(0, Number.parseInt(client.credits, 10) || 0);
  const freeFirstSupportUsed = client.freeFirstSupportUsed === true;
  const isFreeFirstSupport = !freeFirstSupportUsed;
  const creditsConsumed = isFreeFirstSupport ? 0 : 1;
  if (!isFreeFirstSupport && credits < creditsConsumed) {
    throw new SupportQueuePolicyError('credit_required', 409, {
      requestId: normalizeId(requestId, 64).toUpperCase() || null,
      credits,
      freeFirstSupportUsed,
    });
  }
  return {
    isFreeFirstSupport,
    creditsConsumed,
    creditsBefore: credits,
    creditsAfter: isFreeFirstSupport ? credits : credits - creditsConsumed,
    freeFirstSupportUsedAfter: freeFirstSupportUsed || isFreeFirstSupport,
  };
};

const buildClientBillingUpdates = ({
  client,
  profile = {},
  billing,
  now,
  deriveStatus,
} = {}) => {
  const supportsUsedBefore = Math.max(0, Number.parseInt(client?.supportsUsed, 10) || 0);
  const profileCompleted = client?.profileCompleted === true;
  const totalSessions = Math.max(0, Number.parseInt(profile?.totalSessions, 10) || 0);
  const totalPaidSessions = Math.max(0, Number.parseInt(profile?.totalPaidSessions, 10) || 0);
  const totalFreeSessions = Math.max(0, Number.parseInt(profile?.totalFreeSessions, 10) || 0);
  const totalCreditsPurchased = Math.max(0, Number.parseInt(profile?.totalCreditsPurchased, 10) || 0);
  const totalCreditsUsed = Math.max(0, Number.parseInt(profile?.totalCreditsUsed, 10) || 0);

  return {
    client: {
      credits: billing.creditsAfter,
      supportsUsed: supportsUsedBefore + 1,
      freeFirstSupportUsed: billing.freeFirstSupportUsedAfter,
      profileCompleted,
      status: deriveStatus({
        credits: billing.creditsAfter,
        freeFirstSupportUsed: billing.freeFirstSupportUsedAfter,
      }),
      updatedAt: now,
      lastSessionAt: now,
      lastSeenAt: now,
    },
    profile: {
      totalSessions: totalSessions + 1,
      totalPaidSessions: totalPaidSessions + (billing.isFreeFirstSupport ? 0 : 1),
      totalFreeSessions: totalFreeSessions + (billing.isFreeFirstSupport ? 1 : 0),
      totalCreditsPurchased,
      totalCreditsUsed: totalCreditsUsed + billing.creditsConsumed,
      lastSupportAt: now,
      updatedAt: now,
    },
  };
};

module.exports = {
  SupportQueuePolicyError,
  buildClientBillingUpdates,
  decideTechQueueRemoval,
  decideTechSessionClaim,
  decideTechSupportAvailability,
  decideQueueCancellation,
  decideQueueReservation,
  evaluateAuthoritativeBilling,
};
