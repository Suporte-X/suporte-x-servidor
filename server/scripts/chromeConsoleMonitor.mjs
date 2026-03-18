#!/usr/bin/env node

const targetUrlPart = process.argv[2] || 'supportex.app/central.html';
const durationSeconds = Number(process.argv[3] || 120);
const endpoint = process.argv[4] || 'http://127.0.0.1:9222';

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const asText = (value) => {
  if (!value) return '';
  if (typeof value.value !== 'undefined') return String(value.value);
  if (typeof value.unserializableValue !== 'undefined') return String(value.unserializableValue);
  if (typeof value.description === 'string') return value.description;
  if (typeof value.type === 'string') return value.type;
  return '';
};

const pickTarget = async () => {
  const response = await fetch(`${endpoint}/json/list`);
  if (!response.ok) {
    throw new Error(`Nao foi possivel listar abas CDP (${response.status}).`);
  }
  const list = await response.json();
  const page = list.find((item) => item?.type === 'page' && String(item.url || '').includes(targetUrlPart));
  if (!page) {
    throw new Error(`Aba nao encontrada contendo: ${targetUrlPart}`);
  }
  if (!page.webSocketDebuggerUrl) {
    throw new Error('Aba sem webSocketDebuggerUrl.');
  }
  return page;
};

const run = async () => {
  const page = await pickTarget();
  const ws = new WebSocket(page.webSocketDebuggerUrl);
  let seq = 0;
  const pending = new Map();
  const stats = {
    consoleEvents: 0,
    logEvents: 0,
    invalidToken: 0,
    firestorePerm: 0,
    websocketClosed: 0,
  };

  const send = (method, params = {}) =>
    new Promise((resolve, reject) => {
      const id = ++seq;
      pending.set(id, { resolve, reject, method });
      ws.send(JSON.stringify({ id, method, params }));
      setTimeout(() => {
        if (pending.has(id)) {
          pending.delete(id);
          reject(new Error(`Timeout CDP: ${method}`));
        }
      }, 15000);
    });

  const reportLine = (prefix, text) => {
    const line = `[${new Date().toISOString()}] ${prefix} ${text}`.trim();
    console.log(line);
    const normalized = line.toLowerCase();
    if (normalized.includes('invalid_token')) stats.invalidToken += 1;
    if (normalized.includes('missing or insufficient permissions')) stats.firestorePerm += 1;
    if (normalized.includes('websocket is closed before the connection is established')) stats.websocketClosed += 1;
  };

  ws.addEventListener('message', (event) => {
    let payload = null;
    try {
      payload = JSON.parse(event.data);
    } catch (_error) {
      return;
    }
    if (payload.id) {
      const entry = pending.get(payload.id);
      if (!entry) return;
      pending.delete(payload.id);
      if (payload.error) entry.reject(new Error(payload.error.message || 'CDP error'));
      else entry.resolve(payload.result);
      return;
    }

    if (payload.method === 'Runtime.consoleAPICalled') {
      stats.consoleEvents += 1;
      const type = payload.params?.type || 'log';
      const args = Array.isArray(payload.params?.args) ? payload.params.args.map(asText).filter(Boolean) : [];
      reportLine(`console.${type}:`, args.join(' | '));
      return;
    }

    if (payload.method === 'Log.entryAdded') {
      stats.logEvents += 1;
      const level = payload.params?.entry?.level || 'info';
      const text = payload.params?.entry?.text || '';
      reportLine(`log.${level}:`, text);
      return;
    }

    if (payload.method === 'Runtime.exceptionThrown') {
      const text = payload.params?.exceptionDetails?.text || 'exception';
      reportLine('exception:', text);
      return;
    }
  });

  await new Promise((resolve, reject) => {
    ws.addEventListener('open', resolve, { once: true });
    ws.addEventListener('error', reject, { once: true });
  });

  await send('Runtime.enable');
  await send('Log.enable');
  await send('Network.enable');

  reportLine('monitor:', `capturando ${durationSeconds}s em ${page.url}`);
  await wait(Math.max(1, durationSeconds) * 1000);

  reportLine('summary:', JSON.stringify(stats));
  ws.close();
};

run().catch((error) => {
  console.error(`[monitor] erro: ${error?.message || error}`);
  process.exit(1);
});
