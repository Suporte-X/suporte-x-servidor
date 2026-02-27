#!/usr/bin/env node
const admin = require('firebase-admin');
require('../firebase');

const uid = process.argv[2];

if (!uid) {
  console.error('Uso: node server/scripts/setTechClaim.js <uid>');
  process.exit(1);
}

(async () => {
  try {
    await admin.auth().setCustomUserClaims(uid, { role: 'tech' });
    console.log(`Claim role=tech definido para UID ${uid}`);
    process.exit(0);
  } catch (error) {
    console.error('Falha ao definir claim:', error.message || error);
    process.exit(1);
  }
})();
