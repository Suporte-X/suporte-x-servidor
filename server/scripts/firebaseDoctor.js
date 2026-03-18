#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const projectId = process.env.FIREBASE_PROJECT_ID || process.env.GCP_PROJECT_ID || 'suporte-x-19ae8';
const repoRoot = path.resolve(__dirname, '..', '..');
const localCandidates = [
  process.env.GOOGLE_APPLICATION_CREDENTIALS || '',
  path.join(repoRoot, '.secrets', 'firebase-admin.json'),
  path.join(repoRoot, '.secrets', 'service-account.json'),
].filter(Boolean);

const findCredentialFile = () => localCandidates.find((filePath) => fs.existsSync(filePath)) || null;

const safeExec = (command, args) => {
  try {
    return execSync([command, ...args].join(' '), {
      cwd: repoRoot,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
      env: process.env,
      shell: true,
    }).trim();
  } catch (error) {
    const stderr = (error.stderr || '').toString().trim();
    const stdout = (error.stdout || '').toString().trim();
    return stderr || stdout || error.message;
  }
};

const credentialFile = findCredentialFile();
const serverEnvReady =
  Boolean(process.env.FIREBASE_SERVICE_ACCOUNT_JSON) ||
  Boolean(process.env.GCP_SA_KEY_B64) ||
  Boolean(credentialFile) ||
  (Boolean(process.env.FIREBASE_CLIENT_EMAIL) && Boolean(process.env.FIREBASE_PRIVATE_KEY));

console.log(`project: ${projectId}`);
console.log(`firebase_json: ${path.join(repoRoot, 'firebase.json')}`);
console.log(`firebaserc: ${path.join(repoRoot, '.firebaserc')}`);
console.log(`local_credential_file: ${credentialFile || 'missing'}`);
console.log(`server_admin_ready: ${serverEnvReady ? 'yes' : 'no'}`);
console.log('firebase_login_list:');
console.log(safeExec('firebase', ['login:list']) || '(no output)');

if (!serverEnvReady) {
  console.log('');
  console.log('next_step: put a service account JSON at .secrets/firebase-admin.json');
}
