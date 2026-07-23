'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const DAY_MS = 24 * 60 * 60 * 1000;
const NOW = 1_720_000_000_000;

async function loadSubject() {
  return import('../scripts/firestoreRetentionCleanup.mjs');
}

function makeDoc(collection, id, data) {
  return {
    id,
    ref: {
      id,
      path: `${collection}/${id}`,
    },
    data: () => data,
  };
}

function createCollectionDb(seed) {
  return {
    collection(name) {
      return {
        async get() {
          return {
            docs: (seed[name] || []).map(({ id, data }) =>
              makeDoc(name, id, data)
            ),
          };
        },
      };
    },
  };
}

test('coleta timestamps Firestore/Date/ISO e somente sessões realmente encerradas', async () => {
  const { collectCandidates } = await loadSubject();
  const old = NOW - 40 * DAY_MS;
  const recent = NOW - 2 * DAY_MS;
  const db = createCollectionDb({
    pnv_requests: [
      {
        id: 'pnv-old',
        data: {
          createdAt: { seconds: Math.floor(old / 1000), nanoseconds: 0 },
          updatedAt: recent,
        },
      },
      { id: 'pnv-recent', data: { createdAt: recent } },
    ],
    sessions: [
      {
        id: 'session-closed',
        data: {
          status: 'CLOSED',
          closedAt: { toMillis: () => old },
          updatedAt: recent,
        },
      },
      {
        id: 'session-canceled',
        data: { status: 'canceled', endedAt: new Date(old) },
      },
      {
        id: 'session-active',
        data: { status: 'active', updatedAt: old },
      },
      {
        id: 'session-recent',
        data: { status: 'closed', closedAt: recent },
      },
    ],
    support_sessions: [
      {
        id: 'support-completed',
        data: { status: 'completed', endedAt: new Date(old) },
      },
      {
        id: 'support-queued',
        data: { status: 'queued', updatedAt: old },
      },
    ],
    support_reports: [
      {
        id: 'report-old',
        data: { updatedAt: new Date(old).toISOString() },
      },
      {
        id: 'report-no-date',
        data: { status: 'completed' },
      },
    ],
    queue_notifications: [
      {
        id: 'queue-accepted',
        data: { state: 'accepted', updatedAt: old },
      },
      {
        id: 'queue-still-waiting',
        data: { state: 'queued', updatedAt: old },
      },
    ],
    support_queue_anchors: [
      {
        id: 'anchor-accepted',
        data: { status: 'accepted', acceptedAt: old },
      },
    ],
    support_queue_outcomes: [
      {
        id: 'outcome-cancelled',
        data: { status: 'cancelled', cancelledAt: old },
      },
    ],
    support_tech_locks: [
      {
        id: 'lock-expired-session',
        data: {
          realtimeSessionId: 'session-closed',
          status: 'active',
          updatedAt: old,
        },
      },
      {
        id: 'lock-missing-session',
        data: {
          realtimeSessionId: 'session-already-removed',
          status: 'active',
          updatedAt: old,
        },
      },
      {
        id: 'lock-recent',
        data: {
          realtimeSessionId: 'session-already-removed',
          status: 'active',
          updatedAt: recent,
        },
      },
    ],
    account_deletion_operations: [
      {
        id: 'operation-expired',
        data: { status: 'failed', expiresAt: new Date(old) },
      },
      {
        id: 'operation-current',
        data: { status: 'failed', expiresAt: new Date(NOW + DAY_MS) },
      },
    ],
    privacy_deletion_requests: [
      {
        id: 'privacy-expired',
        data: {
          status: 'received',
          expiresAt: { seconds: Math.floor(old / 1000), nanoseconds: 0 },
        },
      },
    ],
    legacy_webrtc_rooms: [
      {
        id: 'room-expired',
        data: { status: 'active', expiresAt: old },
      },
    ],
  });

  const candidates = await collectCandidates(
    db,
    {
      pnvDays: 15,
      sessionDays: 30,
      supportSessionDays: 30,
      supportReportDays: 30,
      queueDays: 30,
    },
    { now: NOW }
  );

  assert.deepEqual(
    candidates.map((candidate) => candidate.path).sort(),
    [
      'account_deletion_operations/operation-expired',
      'legacy_webrtc_rooms/room-expired',
      'pnv_requests/pnv-old',
      'privacy_deletion_requests/privacy-expired',
      'queue_notifications/queue-accepted',
      'sessions/session-canceled',
      'sessions/session-closed',
      'support_queue_anchors/anchor-accepted',
      'support_queue_outcomes/outcome-cancelled',
      'support_reports/report-old',
      'support_sessions/support-completed',
      'support_tech_locks/lock-expired-session',
      'support_tech_locks/lock-missing-session',
    ].sort()
  );
  const realtimeCandidates = candidates.filter(
    (candidate) => candidate.collection === 'sessions'
  );
  assert.deepEqual(
    realtimeCandidates.map((candidate) => candidate.storagePrefix).sort(),
    ['sessions/session-canceled/', 'sessions/session-closed/']
  );
});

test('exclusão apaga mídia antes do documento e usa recursiveDelete', async () => {
  const { deleteCandidates } = await loadSubject();
  const events = [];
  const db = {
    doc(path) {
      return { path };
    },
    async recursiveDelete(ref) {
      events.push(`firestore:${ref.path}`);
    },
  };
  const bucket = {
    async deleteFiles({ prefix }) {
      events.push(`storage:${prefix}`);
    },
  };

  const result = await deleteCandidates(
    { db, bucket },
    [
      {
        path: 'sessions/session-1',
        storagePrefix: 'sessions/session-1/',
      },
      {
        path: 'support_sessions/support-1',
        storagePrefix: null,
      },
    ]
  );

  assert.deepEqual(result, { deleted: 2, failed: [] });
  assert.deepEqual(events, [
    'storage:sessions/session-1/',
    'firestore:sessions/session-1',
    'firestore:support_sessions/support-1',
  ]);
});

test('falha de Storage preserva o documento da sessão para nova tentativa', async () => {
  const { deleteCandidates } = await loadSubject();
  const recursiveDeletes = [];
  const db = {
    doc(path) {
      return { path };
    },
    async recursiveDelete(ref) {
      recursiveDeletes.push(ref.path);
    },
  };
  const bucket = {
    async deleteFiles() {
      throw new Error('storage unavailable');
    },
  };

  const result = await deleteCandidates(
    { db, bucket },
    [{
      path: 'sessions/session-1',
      storagePrefix: 'sessions/session-1/',
    }]
  );

  assert.equal(result.deleted, 0);
  assert.equal(result.failed.length, 1);
  assert.equal(result.failed[0].path, 'sessions/session-1');
  assert.deepEqual(recursiveDeletes, []);
});
