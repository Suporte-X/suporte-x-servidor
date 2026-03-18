#!/usr/bin/env node
const admin = require('firebase-admin');
const { db, firebaseProjectId } = require('../firebase');

const TARGET_PROJECT_ID = 'suporte-x-19ae8';

const parseArgs = (argv) => {
  const parsed = {};
  for (let i = 0; i < argv.length; i += 1) {
    const item = argv[i];
    if (!item.startsWith('--')) continue;
    const key = item.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      parsed[key] = 'true';
      continue;
    }
    parsed[key] = next;
    i += 1;
  }
  return parsed;
};

const ensureString = (value) => (typeof value === 'string' ? value.trim() : '');
const normalizeRole = (value) => (ensureString(value).toLowerCase() === 'supervisor' ? 'supervisor' : 'tech');
const toBoolean = (value, fallback = true) => {
  const raw = ensureString(value).toLowerCase();
  if (!raw) return fallback;
  return ['1', 'true', 'yes', 'y', 'sim'].includes(raw);
};

const args = parseArgs(process.argv.slice(2));
const email = ensureString(args.email).toLowerCase();
const password = ensureString(args.password);
const name = ensureString(args.name);
const role = normalizeRole(args.role);
const active = toBoolean(args.active, true);

if (!email || !password || !name) {
  console.error(
    'Uso: node server/scripts/createTechUser.js --email <email> --password <senha> --name <nome> [--role tech|supervisor] [--active true|false]'
  );
  process.exit(1);
}

if (firebaseProjectId !== TARGET_PROJECT_ID) {
  console.error(`Projeto Firebase incorreto: esperado ${TARGET_PROJECT_ID}, atual ${firebaseProjectId}`);
  process.exit(1);
}

(async () => {
  try {
    const created = await admin.auth().createUser({
      email,
      password,
      displayName: name,
      disabled: !active,
    });

    await admin.auth().setCustomUserClaims(created.uid, {
      role: 'tech',
      supervisor: role === 'supervisor',
      disabled: !active,
    });

    await db.collection('techs').doc(created.uid).set(
      {
        uid: created.uid,
        email,
        name,
        active,
        role,
        supervisor: role === 'supervisor',
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    console.log(`created_uid: ${created.uid}`);
    console.log(`email: ${email}`);
    console.log(`role: ${role}`);
    console.log(`project: ${firebaseProjectId}`);
  } catch (error) {
    console.error('Falha ao criar tecnico:', error.message || error);
    process.exit(1);
  }
})();
