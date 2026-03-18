const fs = require('fs');
const path = require('path');
const { initializeApp, cert, getApps } = require('firebase-admin/app');
const { getFirestore } = require('firebase-admin/firestore');

const ensureString = (value) => {
  if (typeof value === 'string') return value.trim();
  return '';
};

const parseJsonObject = (raw) => {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return parsed;
    }
  } catch (error) {
    return null;
  }
  return null;
};

const decodeBase64Json = (raw) => {
  if (!raw) return null;
  try {
    const decoded = Buffer.from(raw, 'base64').toString('utf8');
    return parseJsonObject(decoded);
  } catch (_error) {
    return null;
  }
};

const readJsonFile = (filePath) => {
  if (!filePath) return null;
  try {
    if (!fs.existsSync(filePath)) return null;
    const raw = fs.readFileSync(filePath, 'utf8');
    return parseJsonObject(raw);
  } catch (_error) {
    return null;
  }
};

const localCredentialCandidates = () => [
  ensureString(process.env.GOOGLE_APPLICATION_CREDENTIALS),
  path.resolve(__dirname, '..', '.secrets', 'firebase-admin.json'),
  path.resolve(__dirname, '..', '.secrets', 'service-account.json'),
];

const resolveServiceAccount = () => {
  const jsonFromEnv = parseJsonObject(ensureString(process.env.FIREBASE_SERVICE_ACCOUNT_JSON));
  if (jsonFromEnv) return jsonFromEnv;

  const fromB64 = decodeBase64Json(ensureString(process.env.GCP_SA_KEY_B64));
  if (fromB64) return fromB64;

  for (const candidate of localCredentialCandidates()) {
    const fromFile = readJsonFile(candidate);
    if (fromFile) return fromFile;
  }

  const projectId =
    ensureString(process.env.FIREBASE_PROJECT_ID) ||
    ensureString(process.env.GCP_PROJECT_ID) ||
    'suporte-x-19ae8';

  const clientEmail =
    ensureString(process.env.FIREBASE_CLIENT_EMAIL) || ensureString(process.env.GCP_CLIENT_EMAIL);
  const privateKeyRaw =
    ensureString(process.env.FIREBASE_PRIVATE_KEY) || ensureString(process.env.GCP_PRIVATE_KEY);

  if (clientEmail && privateKeyRaw) {
    return {
      project_id: projectId,
      client_email: clientEmail,
      private_key: privateKeyRaw.replace(/\\n/g, '\n'),
    };
  }

  throw new Error(
    'Credenciais Firebase Admin ausentes. Configure FIREBASE_SERVICE_ACCOUNT_JSON, GOOGLE_APPLICATION_CREDENTIALS, um arquivo local em .secrets/firebase-admin.json, ou FIREBASE_CLIENT_EMAIL/FIREBASE_PRIVATE_KEY.'
  );
};

const serviceAccount = resolveServiceAccount();
const projectId =
  ensureString(process.env.FIREBASE_PROJECT_ID) ||
  ensureString(process.env.GCP_PROJECT_ID) ||
  ensureString(serviceAccount.project_id) ||
  'suporte-x-19ae8';

const app =
  getApps().length > 0
    ? getApps()[0]
    : initializeApp({
        credential: cert(serviceAccount),
        projectId,
      });

const db = getFirestore(app);
db.settings({ ignoreUndefinedProperties: true });

module.exports = { db, firebaseProjectId: projectId };
