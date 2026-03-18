#!/usr/bin/env node
import { createRequire } from 'module';

const require = createRequire(import.meta.url);
const admin = require('firebase-admin');
require('../firebase');

const endpoint = process.argv[2] || 'http://127.0.0.1:9222';
const targetUrlPart = process.argv[3] || 'https://suportex.app/central.html';

const fetchJson = async (url) => {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} em ${url}`);
  }
  return response.json();
};

const findTarget = async () => {
  const tabs = await fetchJson(`${endpoint}/json/list`);
  const tab = tabs.find((item) => item?.type === 'page' && String(item.url || '').includes(targetUrlPart));
  if (!tab?.webSocketDebuggerUrl) {
    throw new Error(`Aba alvo nao encontrada: ${targetUrlPart}`);
  }
  return tab;
};

const cdpEval = async ({ wsUrl, expression, awaitPromise = true, timeoutMs = 15000 }) =>
  new Promise((resolve, reject) => {
    const ws = new WebSocket(wsUrl);
    let seq = 0;
    const pending = new Map();

    const send = (method, params = {}) =>
      new Promise((res, rej) => {
        const id = ++seq;
        pending.set(id, { res, rej });
        ws.send(JSON.stringify({ id, method, params }));
      });

    const cleanup = (err, result) => {
      try {
        ws.close();
      } catch (_error) {}
      if (err) reject(err);
      else resolve(result);
    };

    const timer = setTimeout(() => cleanup(new Error('Timeout CDP evaluate')), timeoutMs);

    ws.addEventListener('open', async () => {
      try {
        await send('Runtime.enable');
        const evalResult = await send('Runtime.evaluate', {
          expression,
          awaitPromise,
          returnByValue: true,
        });
        clearTimeout(timer);
        cleanup(null, evalResult);
      } catch (error) {
        clearTimeout(timer);
        cleanup(error);
      }
    });

    ws.addEventListener('message', (event) => {
      let payload = null;
      try {
        payload = JSON.parse(event.data);
      } catch (_error) {
        return;
      }
      if (!payload?.id) return;
      const item = pending.get(payload.id);
      if (!item) return;
      pending.delete(payload.id);
      if (payload.error) item.rej(new Error(payload.error.message || 'CDP error'));
      else item.res(payload.result);
    });

    ws.addEventListener('error', (error) => {
      clearTimeout(timer);
      cleanup(error);
    });
  });

const expression = `
(async () => {
  try {
    const appMod = await import('https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js');
    const authMod = await import('https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js');
    const apps = appMod.getApps();
    if (!apps.length) return { ok: false, reason: 'no_firebase_app' };
    const app = apps[0];
    const auth = authMod.getAuth(app);
    const user = auth.currentUser;
    if (!user) return { ok: false, reason: 'no_auth_user' };
    const token = await user.getIdToken(true);
    return {
      ok: true,
      uid: user.uid || null,
      email: user.email || null,
      token,
    };
  } catch (error) {
    return { ok: false, reason: 'eval_error', message: String(error?.message || error) };
  }
})()
`;

const run = async () => {
  const tab = await findTarget();
  const evalResponse = await cdpEval({ wsUrl: tab.webSocketDebuggerUrl, expression });
  const value = evalResponse?.result?.value || null;
  if (!value?.ok || !value?.token) {
    console.log(JSON.stringify({ ok: false, step: 'browser_token', detail: value }, null, 2));
    process.exit(1);
  }

  const decoded = await admin.auth().verifyIdToken(value.token);
  console.log(
    JSON.stringify(
      {
        ok: true,
        tokenUid: value.uid || null,
        tokenEmail: value.email || null,
        decodedUid: decoded?.uid || null,
        decodedAud: decoded?.aud || null,
        decodedIss: decoded?.iss || null,
        exp: decoded?.exp || null,
        iat: decoded?.iat || null,
      },
      null,
      2
    )
  );
};

run().catch((error) => {
  console.error(JSON.stringify({ ok: false, error: error?.message || String(error) }, null, 2));
  process.exit(1);
});
