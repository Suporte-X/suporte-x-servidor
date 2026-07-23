#!/usr/bin/env node

import path from "node:path";
import process from "node:process";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);

const ALLOWED_STATUSES = new Set([
  "received",
  "processing",
  "completed",
  "rejected"
]);
const STATUS_TRANSITIONS = Object.freeze({
  received: new Set(["processing"]),
  processing: new Set(["completed", "rejected", "received"]),
  completed: new Set(),
  rejected: new Set()
});

export function parseArgs(argv) {
  const args = {
    help: false,
    requestId: "",
    revealContact: false,
    setStatus: "",
    actor: "",
    status: "",
    limit: 50
  };

  for (let index = 0; index < argv.length; index += 1) {
    const current = argv[index];
    const next = argv[index + 1];
    switch (current) {
      case "--request-id":
        args.requestId = String(next || "").trim();
        index += 1;
        break;
      case "--reveal-contact":
        args.revealContact = true;
        break;
      case "--set-status":
        args.setStatus = String(next || "").trim().toLowerCase();
        index += 1;
        break;
      case "--actor":
        args.actor = normalizeActor(next);
        index += 1;
        break;
      case "--status":
        args.status = String(next || "").trim().toLowerCase();
        index += 1;
        break;
      case "--limit":
        args.limit = Number(next || 50);
        index += 1;
        break;
      case "--help":
      case "-h":
        args.help = true;
        break;
      default:
        break;
    }
  }

  if (!Number.isFinite(args.limit)) args.limit = 50;
  args.limit = Math.max(1, Math.min(200, Math.trunc(args.limit)));
  if (args.requestId && !isValidRequestId(args.requestId)) {
    throw new Error("invalid_request_id");
  }
  if (args.setStatus && !ALLOWED_STATUSES.has(args.setStatus)) {
    throw new Error("invalid_status");
  }
  if (args.status && !ALLOWED_STATUSES.has(args.status)) {
    throw new Error("invalid_status_filter");
  }
  if ((args.revealContact || args.setStatus) && !args.requestId) {
    throw new Error("request_id_required");
  }
  if ((args.revealContact || args.setStatus) && !args.actor) {
    throw new Error("actor_required");
  }
  return args;
}

function printHelp() {
  console.log(`
Uso seguro (executar localmente dentro de server/):
  npm run privacy:requests -- --limit 50
  npm run privacy:requests -- --status received
  npm run privacy:requests -- --request-id <uuid>
  npm run privacy:requests -- --request-id <uuid> --set-status processing --actor <identificador-operacional>
  npm run privacy:requests -- --request-id <uuid> --reveal-contact --actor <mesmo-identificador>
  npm run privacy:requests -- --request-id <uuid> --set-status completed --actor <mesmo-identificador>

Seguranca:
  - listagem e consulta comum nunca exibem contato ou contactHash;
  - revelar contato exige pedido previamente assumido pelo mesmo operador;
  - concluir ou rejeitar remove contato e indice do documento;
  - o acesso usa Firebase Admin e PRIVACY_CONTACT_ENCRYPTION_KEY locais;
  - nao redirecione a saida de --reveal-contact para logs.
`.trim());
}

function isValidRequestId(value) {
  return /^[a-f0-9]{8}-[a-f0-9]{4}-[1-5][a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/i.test(
    String(value || "")
  );
}

function normalizeActor(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._:-]/g, "")
    .slice(0, 96);
}

function timestampToIso(value) {
  let millis = null;
  if (typeof value === "number" && Number.isFinite(value)) {
    millis = value;
  } else if (value instanceof Date) {
    millis = value.getTime();
  } else if (value && typeof value.toMillis === "function") {
    millis = Number(value.toMillis());
  } else if (value && typeof value === "object") {
    const seconds = Number(value.seconds ?? value._seconds);
    const nanos = Number(value.nanoseconds ?? value._nanoseconds ?? 0);
    if (Number.isFinite(seconds) && Number.isFinite(nanos)) {
      millis = seconds * 1000 + Math.floor(nanos / 1_000_000);
    }
  }
  return Number.isFinite(millis) ? new Date(millis).toISOString() : null;
}

export function sanitizeRequestMetadata(id, data = {}) {
  return {
    requestId: String(data.requestId || id || "").trim() || null,
    requestType: String(data.requestType || "").trim() || null,
    source: String(data.source || "").trim() || null,
    status: String(data.status || "received").trim().toLowerCase(),
    contactType: String(data.contactType || "").trim().toLowerCase() || null,
    createdAt: timestampToIso(data.createdAt),
    updatedAt: timestampToIso(data.updatedAt),
    expiresAt: timestampToIso(data.expiresAt),
    claimedAt: timestampToIso(data.claimedAt),
    resolvedAt: timestampToIso(data.resolvedAt),
    contactPurgedAt: timestampToIso(data.contactPurgedAt)
  };
}

export async function operatorHash(protector, actor) {
  if (!protector || typeof protector.hash !== "function") {
    throw new Error("privacy_contact_protection_not_configured");
  }
  const normalizedActor = normalizeActor(actor);
  if (!normalizedActor) throw new Error("actor_required");
  return protector.hash(normalizedActor, { type: "operator" });
}

export async function revealContactForOperator({
  protector,
  requestId,
  data,
  actor
}) {
  const status = String(data?.status || "received").trim().toLowerCase();
  if (status !== "processing") {
    throw new Error("request_must_be_processing");
  }
  const expectedActorHash = await operatorHash(protector, actor);
  if (
    !data.processingByHash ||
    data.processingByHash !== expectedActorHash
  ) {
    throw new Error("request_claimed_by_another_operator");
  }
  if (!data.contact || !data.contactType) {
    throw new Error("request_contact_unavailable");
  }
  return protector.open(data.contact, {
    type: data.contactType,
    requestId: data.requestId || requestId
  });
}

export function buildStatusTransition({
  currentData,
  nextStatus,
  actorHash: resolvedActorHash,
  now,
  deleteField
}) {
  const currentStatus = String(currentData?.status || "received")
    .trim()
    .toLowerCase();
  const normalizedNext = String(nextStatus || "").trim().toLowerCase();
  if (!ALLOWED_STATUSES.has(currentStatus) || !ALLOWED_STATUSES.has(normalizedNext)) {
    throw new Error("invalid_status");
  }
  if (!STATUS_TRANSITIONS[currentStatus]?.has(normalizedNext)) {
    throw new Error("invalid_status_transition");
  }
  if (!resolvedActorHash) throw new Error("actor_required");

  const patch = {
    status: normalizedNext,
    updatedAt: now
  };

  if (normalizedNext === "processing") {
    patch.processingByHash = resolvedActorHash;
    patch.claimedAt = now;
    return patch;
  }

  if (
    currentStatus === "processing" &&
    (
      !currentData?.processingByHash ||
      currentData.processingByHash !== resolvedActorHash
    )
  ) {
    throw new Error("request_claimed_by_another_operator");
  }

  if (normalizedNext === "received") {
    patch.processingByHash = deleteField;
    patch.claimedAt = deleteField;
    return patch;
  }

  patch.resolvedAt = now;
  patch.resolvedByHash = resolvedActorHash;
  patch.contactPurgedAt = now;
  patch.contact = deleteField;
  patch.contactHash = deleteField;
  return patch;
}

function loadRuntime() {
  const { db, firebaseProjectId } = require("../firebase");
  const { FieldValue } = require("firebase-admin/firestore");
  const {
    createPrivacyContactProtector
  } = require("../privacyRouter");
  const encryptionKey = String(
    process.env.PRIVACY_CONTACT_ENCRYPTION_KEY || ""
  ).trim();
  if (!encryptionKey) {
    throw new Error("PRIVACY_CONTACT_ENCRYPTION_KEY_not_configured");
  }
  return {
    db,
    firebaseProjectId,
    deleteField: FieldValue.delete(),
    protector: createPrivacyContactProtector(encryptionKey)
  };
}

async function loadRequest(collection, requestId) {
  const snapshot = await collection.doc(requestId).get();
  if (!snapshot.exists) throw new Error("request_not_found");
  return snapshot;
}

async function setRequestStatus({
  db,
  collection,
  requestId,
  nextStatus,
  actor,
  protector,
  deleteField
}) {
  const actorDigest = await operatorHash(protector, actor);
  const ref = collection.doc(requestId);
  return db.runTransaction(async (transaction) => {
    const snapshot = await transaction.get(ref);
    if (!snapshot.exists) throw new Error("request_not_found");
    const patch = buildStatusTransition({
      currentData: snapshot.data() || {},
      nextStatus,
      actorHash: actorDigest,
      now: Date.now(),
      deleteField
    });
    transaction.set(ref, patch, { merge: true });
    return {
      ...sanitizeRequestMetadata(snapshot.id, {
        ...(snapshot.data() || {}),
        ...patch
      }),
      status: nextStatus
    };
  });
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printHelp();
    return;
  }

  const runtime = loadRuntime();
  const collection = runtime.db.collection("privacy_deletion_requests");

  if (!args.requestId) {
    const snapshot = await collection
      .orderBy("createdAt", "desc")
      .limit(args.limit)
      .get();
    const rows = snapshot.docs
      .map((doc) => sanitizeRequestMetadata(doc.id, doc.data() || {}))
      .filter((row) => !args.status || row.status === args.status);
    console.log(JSON.stringify({
      projectId: runtime.firebaseProjectId,
      count: rows.length,
      requests: rows
    }, null, 2));
    return;
  }

  if (args.setStatus) {
    const result = await setRequestStatus({
      ...runtime,
      collection,
      requestId: args.requestId,
      nextStatus: args.setStatus,
      actor: args.actor
    });
    console.log(JSON.stringify({
      ok: true,
      projectId: runtime.firebaseProjectId,
      request: result
    }, null, 2));
    return;
  }

  const snapshot = await loadRequest(collection, args.requestId);
  const data = snapshot.data() || {};
  const metadata = sanitizeRequestMetadata(snapshot.id, data);
  if (!args.revealContact) {
    console.log(JSON.stringify({
      projectId: runtime.firebaseProjectId,
      request: metadata
    }, null, 2));
    return;
  }

  const contact = await revealContactForOperator({
    protector: runtime.protector,
    requestId: snapshot.id,
    data,
    actor: args.actor
  });
  // This is the only branch that emits decrypted contact, and it requires the
  // explicit --reveal-contact flag plus the same operator claim.
  console.log(JSON.stringify({
    projectId: runtime.firebaseProjectId,
    request: metadata,
    contact
  }, null, 2));
}

const invokedPath = process.argv[1] ? path.resolve(process.argv[1]) : "";
const modulePath = fileURLToPath(import.meta.url);
if (invokedPath === modulePath) {
  run().catch((error) => {
    console.error(`Falha: ${String(error?.message || error)}`);
    process.exitCode = 1;
  });
}
