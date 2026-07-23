#!/usr/bin/env node

import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

export const DEFAULTS = Object.freeze({
  pnvDays: 15,
  sessionDays: 30,
  supportSessionDays: 30,
  supportReportDays: 30,
  queueDays: 30,
  sampleSize: 20
});

export const TERMINAL_SESSION_STATUSES = new Set([
  "closed",
  "ended",
  "completed",
  "cancelled",
  "canceled",
  "rejected",
  "failed",
  "expired",
  "removed"
]);

export const FINAL_QUEUE_STATUSES = new Set([
  "accepted",
  "completed",
  "cancelled",
  "canceled",
  "rejected",
  "failed",
  "expired",
  "removed",
  "closed",
  "ended"
]);

const TIMESTAMP_FIELDS = Object.freeze({
  pnv: ["createdAt", "processedAt", "updatedAt"],
  session: [
    "closedAt",
    "endedAt",
    "completedAt",
    "cancelledAt",
    "canceledAt",
    "updatedAt",
    "createdAt"
  ],
  report: ["completedAt", "endedAt", "updatedAt", "createdAt"],
  expiry: ["expiresAt"],
  queue: [
    "completedAt",
    "endedAt",
    "closedAt",
    "cancelledAt",
    "canceledAt",
    "acceptedAt",
    "updatedAt",
    "createdAt"
  ]
});

export function parseArgs(argv) {
  const args = {
    execute: false,
    projectId: "",
    storageBucket: "",
    pnvDays: DEFAULTS.pnvDays,
    sessionDays: DEFAULTS.sessionDays,
    supportSessionDays: DEFAULTS.supportSessionDays,
    supportReportDays: DEFAULTS.supportReportDays,
    queueDays: DEFAULTS.queueDays,
    sampleSize: DEFAULTS.sampleSize
  };

  for (let i = 0; i < argv.length; i += 1) {
    const current = argv[i];
    const next = argv[i + 1];
    switch (current) {
      case "--execute":
        args.execute = true;
        break;
      case "--project-id":
        args.projectId = next || "";
        i += 1;
        break;
      case "--storage-bucket":
        args.storageBucket = next || "";
        i += 1;
        break;
      case "--pnv-days":
        args.pnvDays = Number(next || DEFAULTS.pnvDays);
        i += 1;
        break;
      case "--session-days":
        args.sessionDays = Number(next || DEFAULTS.sessionDays);
        i += 1;
        break;
      case "--support-session-days":
        args.supportSessionDays = Number(next || DEFAULTS.supportSessionDays);
        i += 1;
        break;
      case "--support-report-days":
        args.supportReportDays = Number(next || DEFAULTS.supportReportDays);
        i += 1;
        break;
      case "--queue-days":
        args.queueDays = Number(next || DEFAULTS.queueDays);
        i += 1;
        break;
      case "--sample-size":
        args.sampleSize = Number(next || DEFAULTS.sampleSize);
        i += 1;
        break;
      case "--help":
      case "-h":
        printHelp();
        args.help = true;
        break;
      default:
        break;
    }
  }

  for (const [key, fallback] of [
    ["pnvDays", DEFAULTS.pnvDays],
    ["sessionDays", DEFAULTS.sessionDays],
    ["supportSessionDays", DEFAULTS.supportSessionDays],
    ["supportReportDays", DEFAULTS.supportReportDays],
    ["queueDays", DEFAULTS.queueDays]
  ]) {
    if (!Number.isFinite(args[key]) || args[key] < 0) args[key] = fallback;
  }
  if (!Number.isFinite(args.sampleSize) || args.sampleSize < 1) {
    args.sampleSize = DEFAULTS.sampleSize;
  }

  return args;
}

function printHelp() {
  console.log(`
Uso:
  # na raiz do repositorio web-servidor
  node server/scripts/firestoreRetentionCleanup.mjs [opcoes]

  # dentro da pasta server/
  node scripts/firestoreRetentionCleanup.mjs [opcoes]

Padrao:
  - modo dry-run (somente lista candidatos)
  - pnv_requests: 15 dias
  - sessions encerradas e suas midias: 30 dias
  - support_sessions finalizadas: 30 dias
  - support_reports: 30 dias
  - registros finais de fila: 30 dias
  - documentos temporarios: quando expiresAt for atingido

Opcoes:
  --execute                  Apaga os candidatos (inclui subcolecoes e midias)
  --project-id <id>          Forca projeto Firebase/GCP
  --storage-bucket <nome>    Bucket usado para midias em sessions/<id>/
  --pnv-days <dias>          Retencao de pnv_requests
  --session-days <dias>      Retencao de sessions encerradas e midias
  --support-session-days <d> Retencao de support_sessions finalizadas
  --support-report-days <d>  Retencao de support_reports
  --queue-days <dias>        Retencao de registros finais de fila
  --sample-size <n>          Quantidade de caminhos exibidos no dry-run
  --help                     Exibe esta ajuda
`.trim());
}

export function cutoffMillis(days, now = Date.now()) {
  return now - days * 24 * 60 * 60 * 1000;
}

export function timestampToMillis(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (value instanceof Date) {
    const millis = value.getTime();
    return Number.isFinite(millis) ? millis : null;
  }
  if (value && typeof value.toMillis === "function") {
    const millis = Number(value.toMillis());
    return Number.isFinite(millis) ? millis : null;
  }
  if (value && typeof value === "object") {
    const seconds = Number(value.seconds ?? value._seconds);
    const nanos = Number(value.nanoseconds ?? value._nanoseconds ?? 0);
    if (Number.isFinite(seconds) && Number.isFinite(nanos)) {
      return seconds * 1000 + Math.floor(nanos / 1_000_000);
    }
  }
  if (typeof value === "string" && value.trim()) {
    const numeric = Number(value);
    if (Number.isFinite(numeric)) return numeric;
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export function resolveEpochMillis(data, fields = TIMESTAMP_FIELDS.session) {
  for (const field of fields) {
    const millis = timestampToMillis(data?.[field]);
    if (millis !== null) return millis;
  }
  return null;
}

function normalizedStatus(data) {
  return String(data?.status || data?.state || data?.queueStatus || "")
    .trim()
    .toLowerCase();
}

function candidateSummary(
  collectionName,
  ref,
  data,
  { timestampFields, storagePrefix = null } = {}
) {
  return {
    key: `${collectionName}/${ref.id}`,
    path: ref.path,
    collection: collectionName,
    ts: resolveEpochMillis(data, timestampFields),
    status: normalizedStatus(data) || null,
    storagePrefix
  };
}

export async function collectCandidates(db, args, { now = Date.now() } = {}) {
  const pnvCutoff = cutoffMillis(args.pnvDays, now);
  const realtimeSessionCutoff = cutoffMillis(args.sessionDays, now);
  const supportSessionCutoff = cutoffMillis(args.supportSessionDays, now);
  const reportCutoff = cutoffMillis(args.supportReportDays, now);
  const queueCutoff = cutoffMillis(args.queueDays, now);

  const [
    pnvSnap,
    realtimeSessionSnap,
    supportSessionSnap,
    reportSnap,
    queueNotificationSnap,
    queueAnchorSnap,
    queueOutcomeSnap,
    techLockSnap,
    deletionOperationSnap,
    privacyRequestSnap,
    legacyRoomSnap
  ] = await Promise.all([
    db.collection("pnv_requests").get(),
    db.collection("sessions").get(),
    db.collection("support_sessions").get(),
    db.collection("support_reports").get(),
    db.collection("queue_notifications").get(),
    db.collection("support_queue_anchors").get(),
    db.collection("support_queue_outcomes").get(),
    db.collection("support_tech_locks").get(),
    db.collection("account_deletion_operations").get(),
    db.collection("privacy_deletion_requests").get(),
    db.collection("legacy_webrtc_rooms").get()
  ]);

  const candidatesByPath = new Map();
  const realtimeSessionIds = new Set(
    realtimeSessionSnap.docs.map((doc) => String(doc.id || "").trim()).filter(Boolean)
  );
  const expiredRealtimeSessionIds = new Set();

  for (const doc of pnvSnap.docs) {
    const data = doc.data() || {};
    const ts = resolveEpochMillis(data, TIMESTAMP_FIELDS.pnv);
    if (ts !== null && ts <= pnvCutoff) {
      const entry = candidateSummary("pnv_requests", doc.ref, data, {
        timestampFields: TIMESTAMP_FIELDS.pnv
      });
      candidatesByPath.set(entry.path, entry);
    }
  }

  for (const doc of realtimeSessionSnap.docs) {
    const data = doc.data() || {};
    const ts = resolveEpochMillis(data, TIMESTAMP_FIELDS.session);
    const status = normalizedStatus(data);
    if (
      ts !== null &&
      ts <= realtimeSessionCutoff &&
      TERMINAL_SESSION_STATUSES.has(status)
    ) {
      const entry = candidateSummary("sessions", doc.ref, data, {
        timestampFields: TIMESTAMP_FIELDS.session,
        storagePrefix: `sessions/${doc.id}/`
      });
      candidatesByPath.set(entry.path, entry);
      expiredRealtimeSessionIds.add(String(doc.id));
    }
  }

  for (const doc of supportSessionSnap.docs) {
    const data = doc.data() || {};
    const ts = resolveEpochMillis(data, TIMESTAMP_FIELDS.session);
    const status = normalizedStatus(data);
    if (
      ts !== null &&
      ts <= supportSessionCutoff &&
      TERMINAL_SESSION_STATUSES.has(status)
    ) {
      const entry = candidateSummary("support_sessions", doc.ref, data, {
        timestampFields: TIMESTAMP_FIELDS.session
      });
      candidatesByPath.set(entry.path, entry);
    }
  }

  for (const doc of reportSnap.docs) {
    const data = doc.data() || {};
    const ts = resolveEpochMillis(data, TIMESTAMP_FIELDS.report);
    if (ts !== null && ts <= reportCutoff) {
      const entry = candidateSummary("support_reports", doc.ref, data, {
        timestampFields: TIMESTAMP_FIELDS.report
      });
      candidatesByPath.set(entry.path, entry);
    }
  }

  for (const [collectionName, snapshot] of [
    ["queue_notifications", queueNotificationSnap],
    ["support_queue_anchors", queueAnchorSnap],
    ["support_queue_outcomes", queueOutcomeSnap]
  ]) {
    for (const doc of snapshot.docs) {
      const data = doc.data() || {};
      const ts = resolveEpochMillis(data, TIMESTAMP_FIELDS.queue);
      const status = normalizedStatus(data);
      if (
        ts !== null &&
        ts <= queueCutoff &&
        FINAL_QUEUE_STATUSES.has(status)
      ) {
        const entry = candidateSummary(collectionName, doc.ref, data, {
          timestampFields: TIMESTAMP_FIELDS.queue
        });
        candidatesByPath.set(entry.path, entry);
      }
    }
  }

  for (const doc of techLockSnap.docs) {
    const data = doc.data() || {};
    const ts = resolveEpochMillis(data, TIMESTAMP_FIELDS.queue);
    const sessionId = String(
      data.realtimeSessionId || data.sessionId || ""
    ).trim();
    const referencesExpiredSession = expiredRealtimeSessionIds.has(sessionId);
    const referencesMissingSession =
      Boolean(sessionId) && !realtimeSessionIds.has(sessionId);
    if (
      ts !== null &&
      ts <= queueCutoff &&
      (referencesExpiredSession || referencesMissingSession)
    ) {
      const entry = candidateSummary("support_tech_locks", doc.ref, data, {
        timestampFields: TIMESTAMP_FIELDS.queue
      });
      candidatesByPath.set(entry.path, entry);
    }
  }

  for (const [collectionName, snapshot] of [
    ["account_deletion_operations", deletionOperationSnap],
    ["privacy_deletion_requests", privacyRequestSnap],
    ["legacy_webrtc_rooms", legacyRoomSnap]
  ]) {
    for (const doc of snapshot.docs) {
      const data = doc.data() || {};
      const expiresAt = resolveEpochMillis(data, TIMESTAMP_FIELDS.expiry);
      if (expiresAt !== null && expiresAt <= now) {
        const entry = candidateSummary(collectionName, doc.ref, data, {
          timestampFields: TIMESTAMP_FIELDS.expiry
        });
        candidatesByPath.set(entry.path, entry);
      }
    }
  }

  return Array.from(candidatesByPath.values()).sort(
    (a, b) => (a.ts ?? 0) - (b.ts ?? 0)
  );
}

export async function deleteCandidates({ db, bucket = null }, candidates) {
  let deleted = 0;
  const failed = [];

  for (const candidate of candidates) {
    try {
      if (candidate.storagePrefix) {
        if (!bucket || typeof bucket.deleteFiles !== "function") {
          throw new Error("storage_bucket_required");
        }
        // Storage goes first. If it fails, Firestore retains the ownership and
        // timestamp context needed for a safe retry.
        await bucket.deleteFiles({ prefix: candidate.storagePrefix });
      }
      await db.recursiveDelete(db.doc(candidate.path));
      deleted += 1;
    } catch (error) {
      failed.push({
        path: candidate.path,
        message: String(error?.message || error)
      });
    }
  }

  return { deleted, failed };
}

function printPreview(candidates, sampleSize) {
  const sample = candidates.slice(0, sampleSize);
  if (sample.length === 0) {
    console.log("Nenhum candidato encontrado para limpeza.");
    return;
  }
  console.log(`Amostra (${sample.length}/${candidates.length}):`);
  for (const entry of sample) {
    const date = entry.ts ? new Date(entry.ts).toISOString() : "sem-data";
    const suffix = entry.status ? ` status=${entry.status}` : "";
    const media = entry.storagePrefix ? ` midia=${entry.storagePrefix}` : "";
    console.log(`- ${entry.path} (${date})${suffix}${media}`);
  }
}

async function loadFirebaseAdmin(projectId) {
  let adminModule;
  try {
    adminModule = await import("firebase-admin");
  } catch (_error) {
    throw new Error("Dependencia ausente: firebase-admin");
  }

  const admin = adminModule.default || adminModule;
  if (admin.apps.length === 0) {
    const options = projectId ? { projectId } : {};
    admin.initializeApp(options);
  }
  return admin;
}

function resolveStorageBucketName(args) {
  return (
    args.storageBucket ||
    process.env.FIREBASE_STORAGE_BUCKET ||
    process.env.CENTRAL_FIREBASE_STORAGE_BUCKET ||
    `${args.projectId || "suporte-x-19ae8"}.firebasestorage.app`
  );
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) return;
  const admin = await loadFirebaseAdmin(args.projectId);
  const db = admin.firestore();

  console.log("=== Firestore Retention Cleanup ===");
  console.log(`Modo: ${args.execute ? "EXECUTE (apaga)" : "DRY-RUN (nao apaga)"}`);
  if (args.projectId) console.log(`Projeto forcado: ${args.projectId}`);
  console.log(`Retencao pnv_requests: ${args.pnvDays} dias`);
  console.log(`Retencao sessions encerradas e midias: ${args.sessionDays} dias`);
  console.log(
    `Retencao support_sessions finalizadas: ${args.supportSessionDays} dias`
  );
  console.log(`Retencao support_reports: ${args.supportReportDays} dias`);
  console.log(`Retencao de registros finais de fila: ${args.queueDays} dias`);

  const candidates = await collectCandidates(db, args);
  console.log(`Total de candidatos: ${candidates.length}`);
  printPreview(candidates, args.sampleSize);

  if (!args.execute || candidates.length === 0) {
    console.log("Finalizado sem exclusao.");
    return;
  }

  const bucketName = resolveStorageBucketName(args);
  const bucket = admin.storage().bucket(bucketName);
  console.log(`Bucket de midias: ${bucketName}`);
  const result = await deleteCandidates({ db, bucket }, candidates);
  console.log(`Documentos excluidos: ${result.deleted}`);
  if (result.failed.length > 0) {
    console.log(`Falhas: ${result.failed.length}`);
    for (const row of result.failed.slice(0, args.sampleSize)) {
      console.log(`- ${row.path}: ${row.message}`);
    }
    process.exitCode = 2;
  }
}

const invokedPath = process.argv[1] ? path.resolve(process.argv[1]) : "";
const modulePath = fileURLToPath(import.meta.url);
if (invokedPath === modulePath) {
  main().catch((error) => {
    console.error("Erro ao executar limpeza:", error?.message || error);
    process.exitCode = 1;
  });
}
