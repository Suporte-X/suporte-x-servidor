#!/usr/bin/env node
const admin = require('firebase-admin');
const { firebaseProjectId } = require('../firebase');

const TARGET_PROJECT_ID = 'suporte-x-19ae8';
const uid = process.argv[2];

if (!uid) {
  console.error('Uso: node server/scripts/setTechClaim.js <uid>');
  process.exit(1);
}

if (firebaseProjectId !== TARGET_PROJECT_ID) {
  console.error(
    `❌ Projeto Firebase incorreto: esperado ${TARGET_PROJECT_ID}, atual ${firebaseProjectId}`,
  );
  process.exit(1);
}

(async () => {
  try {
    await admin.auth().setCustomUserClaims(uid, { role: 'tech' });
    const user = await admin.auth().getUser(uid);

    console.log(`✅ Claim role=tech definida para UID ${uid} no projeto ${firebaseProjectId}`);
    console.log('Claims atuais:', user.customClaims || {});
    console.log('Agora faça logout/login para renovar o token.');
    process.exit(0);
  } catch (error) {
    console.error('❌ Falha ao definir claim:', error.message || error);
    process.exit(1);
  }
})();
