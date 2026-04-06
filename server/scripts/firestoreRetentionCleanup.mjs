#!/usr/bin/env node

import process from "node:process";

const DEFAULTS = {
  pnvDays: 15,
  supportSessionDays: 30,
  supportReportDays: 30,
  sampleSize: 20
};

const FINAL_SESSION_STATUSES = new Set(["completed", "cancelled"]);

function parseArgs(argv) {
  const args = {
    execute: false,
    projectId: "",
    pnvDays: DEFAULTS.pnvDays,
    supportSessionDays: DEFAULTS.supportSessionDays,
    supportReportDays: DEFAULTS.supportReportDays,
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
      case "--pnv-days":
        args.pnvDays = Number(next || DEFAULTS.pnvDays);
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
      case "--sample-size":
        args.sampleSize = Number(next || DEFAULTS.sampleSize);
        i += 1;
        break;
      case "--help":
      case "-h":
        printHelpAndExit(0);
        break;
      default:
        break;
    }
  }

  if (!Number.isFinite(args.pnvDays) || args.pnvDays < 0) args.pnvDays = DEFAULTS.pnvDays;
  if (!Number.isFinite(args.supportSessionDays) || args.supportSessionDays < 0) {
    args.supportSessionDays = DEFAULTS.supportSessionDays;
  }
  if (!Number.isFinite(args.supportReportDays) || args.supportReportDays < 0) {
    args.supportReportDays = DEFAULTS.supportReportDays;
  }
  if (!Number.isFinite(args.sampleSize) || args.sampleSize < 1) args.sampleSize = DEFAULTS.sampleSize;

  return args;
}

function printHelpAndExit(code) {
  console.log(`
Uso:
  # na raiz do repositorio web-servidor
  node server/scripts/firestoreRetentionCleanup.mjs [opcoes]

  # dentro da pasta server/
  node scripts/firestoreRetentionCleanup.mjs [opcoes]

Padrao:
  - modo dry-run (somente lista candidatos)
  - pnv_requests: 15 dias
  - support_sessions finalizadas: 30 dias
  - support_reports: 30 dias

Opcoes:
  --execute                  Apaga os documentos candidatos
  --project-id <id>          Forca projeto Firebase/GCP
  --pnv-days <dias>          Retencao de pnv_requests
  --support-session-days <d> Retencao de support_sessions finalizadas
  --support-report-days <d>  Retencao de support_reports
  --sample-size <n>          Quantidade de caminhos exibidos no dry-run
  --help                     Exibe esta ajuda
`.trim());
  process.exit(code);
}

function cutoffMillis(days) {
  return Date.now() - days * 24 * 60 * 60 * 1000;
}

function resolveEpochMillis(data) {
  const candidates = [data.updatedAt, data.createdAt, data.endedAt];
  for (const value of candidates) {
    if (typeof value === "number" && Number.isFinite(value)) return value;
  }
  return null;
}

function candidateSummary(collectionName, ref, data) {
  const when = resolveEpochMillis(data);
  return {
    key: `${collectionName}/${ref.id}`,
    path: ref.path,
    ts: when,
    status: typeof data.status === "string" ? data.status : null
  };
}

async function loadFirebaseAdmin(projectId) {
  let adminModule;
  try {
    adminModule = await import("firebase-admin");
  } catch (err) {
    console.error("Dependencia ausente: firebase-admin");
    console.error("Instale com: npm install firebase-admin");
    process.exit(1);
  }

  const admin = adminModule.default || adminModule;
  if (admin.apps.length === 0) {
    const options = projectId ? { projectId } : {};
    admin.initializeApp(options);
  }
  return admin;
}

async function collectCandidates(db, args) {
  const pnvCutoff = cutoffMillis(args.pnvDays);
  const sessionCutoff = cutoffMillis(args.supportSessionDays);
  const reportCutoff = cutoffMillis(args.supportReportDays);

  const [pnvSnap, sessionSnap, reportSnap] = await Promise.all([
    db.collection("pnv_requests").get(),
    db.collection("support_sessions").get(),
    db.collection("support_reports").get()
  ]);

  const candidatesByPath = new Map();

  for (const doc of pnvSnap.docs) {
    const data = doc.data() || {};
    const ts = resolveEpochMillis(data);
    if (ts !== null && ts <= pnvCutoff) {
      const entry = candidateSummary("pnv_requests", doc.ref, data);
      candidatesByPath.set(entry.path, entry);
    }
  }

  for (const doc of sessionSnap.docs) {
    const data = doc.data() || {};
    const ts = resolveEpochMillis(data);
    const status = typeof data.status === "string" ? data.status : "";
    if (ts !== null && ts <= sessionCutoff && FINAL_SESSION_STATUSES.has(status)) {
      const entry = candidateSummary("support_sessions", doc.ref, data);
      candidatesByPath.set(entry.path, entry);
    }
  }

  for (const doc of reportSnap.docs) {
    const data = doc.data() || {};
    const ts = resolveEpochMillis(data);
    if (ts !== null && ts <= reportCutoff) {
      const entry = candidateSummary("support_reports", doc.ref, data);
      candidatesByPath.set(entry.path, entry);
    }
  }

  return Array.from(candidatesByPath.values()).sort((a, b) => (a.ts ?? 0) - (b.ts ?? 0));
}

async function deleteCandidates(db, candidates) {
  const writer = db.bulkWriter();
  let deleted = 0;
  const failed = [];

  writer.onWriteError((error) => {
    const path = error?.documentRef?.path || "unknown";
    failed.push({ path, message: error.message });
    return false;
  });

  for (const candidate of candidates) {
    writer.delete(db.doc(candidate.path));
    deleted += 1;
  }

  await writer.close();
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
    console.log(`- ${entry.path} (${date})${suffix}`);
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const admin = await loadFirebaseAdmin(args.projectId);
  const db = admin.firestore();

  console.log("=== Firestore Retention Cleanup ===");
  console.log(`Modo: ${args.execute ? "EXECUTE (apaga)" : "DRY-RUN (nao apaga)"}`);
  if (args.projectId) {
    console.log(`Projeto forcado: ${args.projectId}`);
  }
  console.log(`Retencao pnv_requests: ${args.pnvDays} dias`);
  console.log(`Retencao support_sessions finalizadas: ${args.supportSessionDays} dias`);
  console.log(`Retencao support_reports: ${args.supportReportDays} dias`);

  const candidates = await collectCandidates(db, args);
  console.log(`Total de candidatos: ${candidates.length}`);
  printPreview(candidates, args.sampleSize);

  if (!args.execute || candidates.length === 0) {
    console.log("Finalizado sem exclusao.");
    return;
  }

  const result = await deleteCandidates(db, candidates);
  console.log(`Documentos deletados: ${result.deleted}`);
  if (result.failed.length > 0) {
    console.log(`Falhas: ${result.failed.length}`);
    for (const row of result.failed.slice(0, args.sampleSize)) {
      console.log(`- ${row.path}: ${row.message}`);
    }
    process.exitCode = 2;
  }
}

main().catch((err) => {
  console.error("Erro ao executar limpeza:", err?.message || err);
  process.exit(1);
});
