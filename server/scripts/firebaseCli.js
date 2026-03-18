#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

const repoRoot = path.resolve(__dirname, '..', '..');
const defaultProjectId = process.env.FIREBASE_PROJECT_ID || 'suporte-x-19ae8';
const localCredentialCandidates = [
  process.env.GOOGLE_APPLICATION_CREDENTIALS || '',
  path.join(repoRoot, '.secrets', 'firebase-admin.json'),
  path.join(repoRoot, '.secrets', 'service-account.json'),
].filter(Boolean);

const credentialFile = localCredentialCandidates.find((filePath) => fs.existsSync(filePath)) || null;
const args = process.argv.slice(2);
const hasProjectFlag = args.includes('--project') || args.includes('-P');

const env = { ...process.env };
if (credentialFile && !env.GOOGLE_APPLICATION_CREDENTIALS) {
  env.GOOGLE_APPLICATION_CREDENTIALS = credentialFile;
}
if (!hasProjectFlag) {
  args.push('--project', defaultProjectId);
}

const result = spawnSync('firebase', args, {
  cwd: repoRoot,
  env,
  stdio: 'inherit',
  shell: true,
});

process.exit(result.status ?? 1);
