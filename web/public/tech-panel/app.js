import { initializeApp } from "https://www.gstatic.com/firebasejs/10.14.1/firebase-app.js";
import {
  getAuth,
  onAuthStateChanged,
  signInAnonymously
} from "https://www.gstatic.com/firebasejs/10.14.1/firebase-auth.js";
import {
  addDoc,
  collection,
  doc,
  getDoc,
  getDocs,
  getFirestore,
  query,
  runTransaction,
  setDoc,
  updateDoc,
  where
} from "https://www.gstatic.com/firebasejs/10.14.1/firebase-firestore.js";

const COLLECTIONS = {
  clients: "clients",
  clientProfiles: "client_profiles",
  supportSessions: "support_sessions",
  supportReports: "support_reports",
  creditOrders: "credit_orders",
  creditPackages: "credit_packages",
  clientAppLinks: "client_app_links",
  clientVerifications: "client_verifications",
  pnvRequests: "pnv_requests"
};

const PNV_STATUS = {
  VERIFIED: "verified",
  PENDING: "pending",
  MISMATCH: "mismatch",
  MANUAL_REQUIRED: "manual_required"
};

const state = {
  selectedClientId: null,
  selectedPhone: null,
  packages: [],
  activeSession: null
};

const firebaseConfig = window.SUPORTEX_FIREBASE_CONFIG;
const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);

const authStatus = document.querySelector("#auth-status");
const queueList = document.querySelector("#queue-list");
const historyList = document.querySelector("#history-list");
const ordersList = document.querySelector("#orders-list");
const profileContainer = document.querySelector("#client-profile");
const intakePanel = document.querySelector("#intake-panel");
const intakeForm = document.querySelector("#intake-form");
const intakeEmpty = document.querySelector("#intake-empty");
const intakeSessionMeta = document.querySelector("#intake-session-meta");
const intakeSessionId = document.querySelector("#intake-session-id");
const intakeClearBtn = document.querySelector("#intake-clear");
const refreshAllBtn = document.querySelector("#refresh-all");
const searchClientBtn = document.querySelector("#search-client-btn");
const searchPhoneInput = document.querySelector("#search-phone");

setup();

async function setup() {
  bindActions();
  try {
    await signInAnonymously(auth);
  } catch (err) {
    console.error(err);
    authStatus.textContent = "Falha de autenticação";
  }

  onAuthStateChanged(auth, async (user) => {
    authStatus.textContent = user ? `Conectado: ${user.uid.slice(0, 8)}...` : "Sem autenticação";
    await loadAll();
  });
}

function bindActions() {
  refreshAllBtn.addEventListener("click", () => {
    loadAll();
  });

  intakeForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    await handleIntakeSubmit();
  });

  intakeClearBtn.addEventListener("click", () => {
    clearIntakePanel();
  });

  searchClientBtn.addEventListener("click", async () => {
    const normalized = normalizePhone(searchPhoneInput.value);
    if (!normalized) {
      alert("Digite um telefone válido.");
      return;
    }
    const clientId = clientDocIdFromPhone(normalized);
    await loadClientProfile(clientId);
  });
}

async function loadAll() {
  await Promise.all([
    loadPackages(),
    loadQueue(),
    loadOrders()
  ]);

  if (state.selectedClientId) {
    await loadClientProfile(state.selectedClientId);
  }
}

async function loadPackages() {
  const snap = await getDocs(collection(db, COLLECTIONS.creditPackages));
  state.packages = snap.docs
    .map((item) => ({ id: item.id, ...item.data() }))
    .filter((pkg) => pkg.active !== false)
    .sort((a, b) => (a.displayOrder ?? 0) - (b.displayOrder ?? 0));
}

async function loadQueue() {
  queueList.innerHTML = "<small>Carregando...</small>";
  const snap = await getDocs(collection(db, COLLECTIONS.supportSessions));
  const sessions = snap.docs
    .map((docItem) => ({ id: docItem.id, ...docItem.data() }))
    .filter((item) => item.status === "queued" || item.status === "in_progress")
    .sort((a, b) => (b.startedAt ?? 0) - (a.startedAt ?? 0))
    .slice(0, 40);

  const rows = await Promise.all(sessions.map(loadQueueRow));

  if (!rows.length) {
    queueList.innerHTML = "<small>Sem solicitações pendentes.</small>";
    clearIntakePanelIfSessionMissing();
    return;
  }

  queueList.innerHTML = "";
  rows.forEach((row) => {
    const card = renderQueueCard(row);
    queueList.appendChild(card);
  });

  clearIntakePanelIfSessionMissing(rows);
}

async function loadQueueRow(session) {
  let client = null;
  if (session.clientId) {
    const clientSnap = await getDoc(doc(db, COLLECTIONS.clients, session.clientId));
    if (clientSnap.exists()) {
      client = { id: clientSnap.id, ...clientSnap.data() };
    }
  }

  const verification = client ? await loadVerification(client.id) : null;
  return { session, client, verification };
}

function renderQueueCard(row) {
  const { session, client, verification } = row;
  const isNewClient = !client;
  const freePending = isNewClient ? true : client.freeFirstSupportUsed === false;
  const noCredit = !freePending && (client?.credits ?? 0) <= 0;
  const verificationUi = verificationPresentation(verification);

  const card = document.createElement("article");
  card.className = `queue-item ${isNewClient ? "is-new" : ""} ${noCredit ? "no-credit" : ""}`;
  card.innerHTML = `
    <div class="queue-header-row">
      <strong>${client?.name || "Cliente não cadastrado"}</strong>
      ${client ? `<button class="verification-dot ${verificationUi.className}" type="button" title="${escapeHtml(verificationUi.tooltip)}" data-verify-client="${client.id}"></button>` : ""}
    </div>
    <div>Telefone: ${session.clientPhone || client?.phone || "-"}</div>
    <div>Status da sessão: ${session.status || "-"}</div>
    <div class="queue-meta">
      ${isNewClient ? '<span class="badge warn">Cadastro inicial pendente</span>' : ""}
      ${noCredit ? '<span class="badge danger">Sem crédito</span>' : ""}
      ${!isNewClient && !noCredit ? '<span class="badge ok">Com crédito</span>' : ""}
      ${session.status === "queued" ? '<span class="badge neutral">Aguardando aceite</span>' : ""}
      ${session.status === "in_progress" ? '<span class="badge active">Em atendimento</span>' : ""}
    </div>
    <div class="profile-actions queue-actions">
      ${session.status === "queued" ? `<button type="button" data-accept-session="${session.id}">Aceitar atendimento</button>` : ""}
      <button type="button" data-open-intake="${session.id}">Cadastro inicial</button>
      ${client ? `<button type="button" data-open-client="${client.id}">Abrir ficha</button>` : ""}
    </div>
  `;

  const acceptBtn = card.querySelector("button[data-accept-session]");
  if (acceptBtn) {
    acceptBtn.addEventListener("click", async () => {
      await acceptSupportSession(session.id);
      const updatedRow = await loadQueueRow({ ...session, status: "in_progress" });
      openIntakePanel(updatedRow);
      await loadQueue();
    });
  }

  const intakeBtn = card.querySelector("button[data-open-intake]");
  intakeBtn.addEventListener("click", () => {
    openIntakePanel(row);
  });

  const openClientBtn = card.querySelector("button[data-open-client]");
  if (openClientBtn) {
    openClientBtn.addEventListener("click", async () => {
      await loadClientProfile(client.id);
    });
  }

  const verifyBtn = card.querySelector("button[data-verify-client]");
  if (verifyBtn) {
    verifyBtn.addEventListener("click", async () => {
      await onVerificationIndicatorClick(client, verification, session.id);
    });
  }

  return card;
}

async function acceptSupportSession(sessionId) {
  const now = Date.now();
  await updateDoc(doc(db, COLLECTIONS.supportSessions, sessionId), {
    status: "in_progress",
    techId: auth.currentUser?.uid || null,
    techName: "Painel Técnico",
    updatedAt: now
  });
}

function openIntakePanel(row) {
  state.activeSession = row.session;
  intakeSessionId.value = row.session.id;
  intakeSessionMeta.textContent = `Sessão ${row.session.id.slice(0, 8)} · ${row.session.status || "queued"}`;

  const nameInput = intakeForm.querySelector("#intake-name");
  const phoneInput = intakeForm.querySelector("#intake-phone");
  const emailInput = intakeForm.querySelector("#intake-email");
  const notesInput = intakeForm.querySelector("#intake-notes");

  nameInput.value = row.client?.name || row.session.clientName || "";
  phoneInput.value = row.client?.phone || row.session.clientPhone || "";
  emailInput.value = row.client?.primaryEmail || "";
  notesInput.value = row.client?.notes || "";

  intakeEmpty.classList.add("hidden");
  intakeForm.classList.remove("hidden");
  intakePanel.classList.add("is-active");
  nameInput.focus();
}

function clearIntakePanel() {
  state.activeSession = null;
  intakeSessionId.value = "";
  intakeSessionMeta.textContent = "Aguardando aceite de atendimento";
  intakeForm.reset();
  intakeForm.classList.add("hidden");
  intakeEmpty.classList.remove("hidden");
  intakePanel.classList.remove("is-active");
}

function clearIntakePanelIfSessionMissing(rows = null) {
  if (!state.activeSession) return;
  const source = rows || [];
  const sessionStillVisible = source.some((row) => row.session.id === state.activeSession.id);
  if (!sessionStillVisible) {
    clearIntakePanel();
  }
}

async function handleIntakeSubmit() {
  if (!state.activeSession) {
    alert("Aceite um atendimento para abrir o cadastro inicial.");
    return;
  }

  const payload = {
    name: intakeForm.querySelector("#intake-name").value.trim(),
    phone: normalizePhone(intakeForm.querySelector("#intake-phone").value),
    email: intakeForm.querySelector("#intake-email").value.trim() || null,
    notes: intakeForm.querySelector("#intake-notes").value.trim() || null
  };

  if (!payload.name || !payload.phone) {
    alert("Nome e telefone são obrigatórios.");
    return;
  }

  try {
    const client = await registerOrUpdateClient(payload);
    await bindClientToSession({
      sessionId: state.activeSession.id,
      client,
      fallbackName: payload.name
    });
    await linkClientUidFromSession(state.activeSession, client);
    await triggerVerificationFromTechnician({
      client,
      supportSessionId: state.activeSession.id,
      clientUid: state.activeSession.clientUid || null,
      manualFallback: false
    });

    await loadClientProfile(client.id);
    await loadQueue();
    alert("Cliente cadastrado e verificação em segundo plano iniciada.");
  } catch (err) {
    console.error(err);
    alert("Não foi possível concluir o cadastro inicial.");
  }
}

async function bindClientToSession({ sessionId, client, fallbackName }) {
  const isFreeFirstSupport = client.freeFirstSupportUsed !== true;
  const creditsToConsume = isFreeFirstSupport ? 0 : 1;
  await updateDoc(doc(db, COLLECTIONS.supportSessions, sessionId), {
    clientId: client.id,
    clientPhone: client.phone,
    clientName: client.name || fallbackName || "Cliente",
    isFreeFirstSupport,
    creditsConsumed: creditsToConsume,
    requiresTechnicianRegistration: false,
    updatedAt: Date.now()
  });
}

async function linkClientUidFromSession(session, client) {
  const clientUid = String(session.clientUid || "").trim();
  if (!clientUid) return;

  const now = Date.now();
  await setDoc(doc(db, COLLECTIONS.clientAppLinks, clientUid), {
    clientUid,
    clientId: client.id,
    phone: client.phone,
    supportSessionId: session.id,
    updatedAt: now,
    createdAt: now
  }, { merge: true });
}

async function triggerVerificationFromTechnician({ client, supportSessionId, clientUid, manualFallback }) {
  const now = Date.now();
  const status = manualFallback ? PNV_STATUS.MANUAL_REQUIRED : PNV_STATUS.PENDING;

  await setDoc(doc(db, COLLECTIONS.clientVerifications, client.id), {
    clientId: client.id,
    primaryPhone: client.phone,
    status,
    source: manualFallback ? "technician_manual_fallback" : "technician_registration",
    lastTriggerAt: now,
    updatedAt: now
  }, { merge: true });

  await addDoc(collection(db, COLLECTIONS.pnvRequests), {
    clientId: client.id,
    clientUid: clientUid || null,
    supportSessionId: supportSessionId || null,
    manualFallback: Boolean(manualFallback),
    status: manualFallback ? "manual_pending" : "pending",
    createdAt: now,
    updatedAt: now
  });
}

async function loadClientProfile(clientId) {
  const clientSnap = await getDoc(doc(db, COLLECTIONS.clients, clientId));
  if (!clientSnap.exists()) {
    profileContainer.className = "profile-empty";
    profileContainer.textContent = "Cliente não encontrado.";
    historyList.innerHTML = "";
    state.selectedClientId = null;
    state.selectedPhone = null;
    return;
  }

  const client = { id: clientSnap.id, ...clientSnap.data() };
  const profileSnap = await getDoc(doc(db, COLLECTIONS.clientProfiles, client.id));
  const profile = profileSnap.exists() ? profileSnap.data() : null;
  const verification = await loadVerification(client.id);

  state.selectedClientId = client.id;
  state.selectedPhone = client.phone || null;
  renderClientProfile(client, profile, verification);
  await loadHistory(client.id);
  await loadOrders(client.id);
}

function renderClientProfile(client, profile, verification) {
  const financialStatus = !client.freeFirstSupportUsed
    ? "primeiro atendimento grátis pendente"
    : (client.credits ?? 0) > 0
      ? "com crédito"
      : "sem crédito";
  const verificationUi = verificationPresentation(verification);

  profileContainer.className = "";
  profileContainer.innerHTML = `
    <div class="profile-grid">
      <div><strong>Nome</strong>${client.name || "-"}</div>
      <div><strong>Telefone</strong>${client.phone || "-"}</div>
      <div><strong>E-mail principal</strong>${client.primaryEmail || "-"}</div>
      <div><strong>Créditos disponíveis</strong>${client.credits ?? 0}</div>
      <div><strong>Atendimentos usados</strong>${client.supportsUsed ?? 0}</div>
      <div><strong>Primeiro grátis usado</strong>${client.freeFirstSupportUsed ? "Sim" : "Não"}</div>
      <div><strong>Status financeiro</strong>${financialStatus}</div>
      <div>
        <strong>Verificação</strong>
        <button type="button" class="verification-dot ${verificationUi.className}" id="btn-verification-indicator" title="${escapeHtml(verificationUi.tooltip)}"></button>
      </div>
      <div><strong>Total de sessões</strong>${profile?.totalSessions ?? 0}</div>
      <div><strong>Total pago</strong>${profile?.totalPaidSessions ?? 0}</div>
      <div><strong>Total grátis</strong>${profile?.totalFreeSessions ?? 0}</div>
      <div><strong>Créditos comprados</strong>${profile?.totalCreditsPurchased ?? 0}</div>
      <div><strong>Créditos usados</strong>${profile?.totalCreditsUsed ?? 0}</div>
      <div><strong>Observações</strong>${client.notes || "-"}</div>
    </div>
    <div class="profile-actions">
      <button type="button" id="btn-add-note">Adicionar observação</button>
      <button type="button" id="btn-add-credit">Adicionar crédito manualmente</button>
      <button type="button" id="btn-remove-credit">Remover crédito manualmente</button>
      <button type="button" id="btn-close-session">Registrar atendimento concluído</button>
      ${verificationUi.isVerified ? "" : '<button type="button" id="btn-manual-fallback">Verificar manualmente</button>'}
    </div>
  `;

  profileContainer.querySelector("#btn-add-note")
    .addEventListener("click", async () => {
      const note = prompt("Digite a observação:");
      if (!note) return;
      await appendClientNotes(client.id, note);
      await loadClientProfile(client.id);
    });

  profileContainer.querySelector("#btn-add-credit")
    .addEventListener("click", async () => {
      const amount = Number(prompt("Quantos créditos adicionar?", "1"));
      if (!Number.isFinite(amount) || amount <= 0) return;
      await adjustCredits(client.id, Math.floor(amount));
      await loadClientProfile(client.id);
    });

  profileContainer.querySelector("#btn-remove-credit")
    .addEventListener("click", async () => {
      const amount = Number(prompt("Quantos créditos remover?", "1"));
      if (!Number.isFinite(amount) || amount <= 0) return;
      await adjustCredits(client.id, -Math.floor(amount));
      await loadClientProfile(client.id);
    });

  profileContainer.querySelector("#btn-close-session")
    .addEventListener("click", async () => {
      const problemSummary = prompt("Resumo do problema:");
      const solutionSummary = prompt("Solução aplicada:");
      await closeLatestOpenSession(client.id, {
        problemSummary,
        solutionSummary,
        internalNotes: "Fechado pelo painel técnico"
      });
      await loadClientProfile(client.id);
      await loadQueue();
    });

  profileContainer.querySelector("#btn-verification-indicator")
    .addEventListener("click", async () => {
      await onVerificationIndicatorClick(client, verification, state.activeSession?.id || null);
    });

  const fallbackBtn = profileContainer.querySelector("#btn-manual-fallback");
  if (fallbackBtn) {
    fallbackBtn.addEventListener("click", async () => {
      await triggerManualFallback(client, state.activeSession?.id || null);
    });
  }
}

async function onVerificationIndicatorClick(client, verification, supportSessionId) {
  const ui = verificationPresentation(verification);
  if (ui.isVerified) {
    alert("Verificação confirmada com sucesso.");
    return;
  }

  const shouldRunFallback = confirm("Verificação pendente/falhou/divergente. Deseja iniciar fallback manual agora?");
  if (!shouldRunFallback) return;
  await triggerManualFallback(client, supportSessionId);
}

async function triggerManualFallback(client, supportSessionId) {
  try {
    const clientUid = await resolveClientUidByClientId(client.id);
    await triggerVerificationFromTechnician({
      client,
      supportSessionId,
      clientUid,
      manualFallback: true
    });
    await loadClientProfile(client.id);
    await loadQueue();
    alert("Fallback manual solicitado.");
  } catch (err) {
    console.error(err);
    alert("Não foi possível iniciar o fallback manual.");
  }
}

async function resolveClientUidByClientId(clientId) {
  const linksSnap = await getDocs(
    query(collection(db, COLLECTIONS.clientAppLinks), where("clientId", "==", clientId))
  );
  const first = linksSnap.docs[0];
  if (!first) return null;
  const data = first.data();
  return data.clientUid || first.id;
}

async function loadVerification(clientId) {
  const snapshot = await getDoc(doc(db, COLLECTIONS.clientVerifications, clientId));
  if (!snapshot.exists()) return null;
  return { id: snapshot.id, ...snapshot.data() };
}

function verificationPresentation(verification) {
  const status = String(verification?.status || "").trim().toLowerCase();
  if (status === PNV_STATUS.VERIFIED) {
    return {
      className: "ok",
      tooltip: "Verificado",
      isVerified: true
    };
  }

  if (status === PNV_STATUS.MISMATCH) {
    return {
      className: "danger",
      tooltip: "Divergente",
      isVerified: false
    };
  }

  if (status === PNV_STATUS.MANUAL_REQUIRED) {
    return {
      className: "danger",
      tooltip: "Fallback manual necessário",
      isVerified: false
    };
  }

  return {
    className: "danger",
    tooltip: "Pendente ou falhou",
    isVerified: false
  };
}

async function loadHistory(clientId) {
  historyList.innerHTML = "<small>Carregando histórico...</small>";
  const sessionsSnap = await getDocs(query(collection(db, COLLECTIONS.supportSessions)));
  const reportsSnap = await getDocs(query(collection(db, COLLECTIONS.supportReports)));

  const sessions = sessionsSnap.docs
    .map((item) => ({ id: item.id, ...item.data() }))
    .filter((session) => session.clientId === clientId)
    .sort((a, b) => (b.startedAt ?? 0) - (a.startedAt ?? 0));

  const reportsBySession = new Map(
    reportsSnap.docs.map((item) => {
      const data = item.data();
      return [data.sessionId, { id: item.id, ...data }];
    })
  );

  if (!sessions.length) {
    historyList.innerHTML = "<small>Sem histórico de atendimento.</small>";
    return;
  }

  historyList.innerHTML = "";
  sessions.forEach((session) => {
    const report = reportsBySession.get(session.id);
    const item = document.createElement("article");
    item.className = "history-item";
    item.innerHTML = `
      <strong>${formatDate(session.startedAt)} - ${session.status || "-"}</strong>
      <div>Técnico: ${session.techName || session.techId || "-"}</div>
      <div>Problema: ${session.problemSummary || "-"}</div>
      <div>Solução: ${session.solutionSummary || report?.solutionApplied || "-"}</div>
      <div>Observações: ${session.internalNotes || report?.actionsTaken || "-"}</div>
      <div>Gratuito: ${session.isFreeFirstSupport ? "Sim" : "Não"} | Créditos consumidos: ${session.creditsConsumed ?? 0}</div>
    `;
    historyList.appendChild(item);
  });
}

async function loadOrders(clientId = null) {
  ordersList.innerHTML = "<small>Carregando pedidos...</small>";
  const snap = await getDocs(collection(db, COLLECTIONS.creditOrders));
  const rows = snap.docs
    .map((item) => ({ id: item.id, ...item.data() }))
    .filter((order) => (clientId ? order.clientId === clientId : true))
    .sort((a, b) => (b.createdAt ?? 0) - (a.createdAt ?? 0))
    .slice(0, 40);

  if (!rows.length) {
    ordersList.innerHTML = "<small>Sem pedidos de crédito.</small>";
    return;
  }

  ordersList.innerHTML = "";
  rows.forEach((order) => {
    const pkg = state.packages.find((item) => item.id === order.packageId);
    const item = document.createElement("article");
    item.className = "order-item";
    item.innerHTML = `
      <strong>${pkg?.name || order.packageId || "-"}</strong>
      <div>Cliente: ${order.clientId || "-"}</div>
      <div>Método: ${order.paymentMethod || "-"}</div>
      <div>Status: ${order.status || "-"}</div>
      <div>Valor: ${formatMoney(order.amountCents || 0)}</div>
      <div>WhatsApp: ${order.whatsappRequested ? "Sim" : "Não"}</div>
    `;
    ordersList.appendChild(item);
  });
}

async function registerOrUpdateClient(payload) {
  const now = Date.now();
  const clientId = clientDocIdFromPhone(payload.phone);
  const clientRef = doc(db, COLLECTIONS.clients, clientId);
  const profileRef = doc(db, COLLECTIONS.clientProfiles, clientId);

  await runTransaction(db, async (tx) => {
    const snap = await tx.get(clientRef);
    const oldData = snap.exists() ? snap.data() : {};
    const credits = Number(oldData.credits ?? 0);
    const supportsUsed = Number(oldData.supportsUsed ?? 0);
    const freeUsed = Boolean(oldData.freeFirstSupportUsed ?? false);

    tx.set(clientRef, {
      phone: payload.phone,
      name: payload.name,
      primaryEmail: payload.email || oldData.primaryEmail || null,
      notes: payload.notes || oldData.notes || null,
      credits,
      supportsUsed,
      freeFirstSupportUsed: freeUsed,
      status: deriveClientStatus({ credits, freeFirstSupportUsed: freeUsed }),
      createdAt: oldData.createdAt || now,
      updatedAt: now
    }, { merge: true });

    if (!snap.exists()) {
      tx.set(profileRef, {
        clientId,
        totalSessions: 0,
        totalPaidSessions: 0,
        totalFreeSessions: 0,
        totalCreditsPurchased: 0,
        totalCreditsUsed: 0,
        lastSupportAt: null,
        createdAt: now,
        updatedAt: now
      }, { merge: true });
    }
  });

  const snapshot = await getDoc(clientRef);
  return { id: snapshot.id, ...snapshot.data() };
}

async function appendClientNotes(clientId, note) {
  const clientRef = doc(db, COLLECTIONS.clients, clientId);
  const snapshot = await getDoc(clientRef);
  if (!snapshot.exists()) return;
  const old = snapshot.data().notes || "";
  const merged = old ? `${old}\n- ${note}` : `- ${note}`;
  await updateDoc(clientRef, {
    notes: merged,
    updatedAt: Date.now()
  });
}

async function adjustCredits(clientId, delta) {
  await runTransaction(db, async (tx) => {
    const clientRef = doc(db, COLLECTIONS.clients, clientId);
    const snap = await tx.get(clientRef);
    if (!snap.exists()) return;
    const current = snap.data();
    const credits = Number(current.credits ?? 0);
    const freeFirstSupportUsed = Boolean(current.freeFirstSupportUsed ?? false);
    const nextCredits = Math.max(0, credits + delta);
    tx.set(clientRef, {
      credits: nextCredits,
      status: deriveClientStatus({
        credits: nextCredits,
        freeFirstSupportUsed
      }),
      updatedAt: Date.now()
    }, { merge: true });
  });
}

async function closeLatestOpenSession(clientId, payload) {
  const sessionsSnap = await getDocs(collection(db, COLLECTIONS.supportSessions));
  const latest = sessionsSnap.docs
    .map((item) => ({ id: item.id, ...item.data() }))
    .filter((row) => row.clientId === clientId && (row.status === "in_progress" || row.status === "queued"))
    .sort((a, b) => (b.startedAt ?? 0) - (a.startedAt ?? 0))[0];

  if (!latest) {
    alert("Nenhuma sessão aberta encontrada para este cliente.");
    return;
  }

  const now = Date.now();
  const sessionRef = doc(db, COLLECTIONS.supportSessions, latest.id);
  const clientRef = doc(db, COLLECTIONS.clients, clientId);
  const profileRef = doc(db, COLLECTIONS.clientProfiles, clientId);

  await runTransaction(db, async (tx) => {
    const sessionSnap = await tx.get(sessionRef);
    const clientSnap = await tx.get(clientRef);
    const profileSnap = await tx.get(profileRef);
    if (!sessionSnap.exists() || !clientSnap.exists()) return;

    const session = sessionSnap.data();
    const client = clientSnap.data();
    const profile = profileSnap.exists() ? profileSnap.data() : {};
    const alreadyApplied = Boolean(session.billingAppliedAt);

    if (!alreadyApplied) {
      const isFree = Boolean(session.isFreeFirstSupport);
      const creditsConsumed = Number(session.creditsConsumed ?? (isFree ? 0 : 1));
      const oldCredits = Number(client.credits ?? 0);
      const oldSupportsUsed = Number(client.supportsUsed ?? 0);
      const freeUsed = Boolean(client.freeFirstSupportUsed ?? false);
      const nextCredits = isFree ? oldCredits : Math.max(0, oldCredits - creditsConsumed);
      const freeUsedAfter = freeUsed || isFree;

      tx.set(clientRef, {
        credits: nextCredits,
        supportsUsed: oldSupportsUsed + 1,
        freeFirstSupportUsed: freeUsedAfter,
        status: deriveClientStatus({
          credits: nextCredits,
          freeFirstSupportUsed: freeUsedAfter
        }),
        updatedAt: now
      }, { merge: true });

      tx.set(profileRef, {
        clientId,
        totalSessions: Number(profile.totalSessions ?? 0) + 1,
        totalPaidSessions: Number(profile.totalPaidSessions ?? 0) + (isFree ? 0 : 1),
        totalFreeSessions: Number(profile.totalFreeSessions ?? 0) + (isFree ? 1 : 0),
        totalCreditsPurchased: Number(profile.totalCreditsPurchased ?? 0),
        totalCreditsUsed: Number(profile.totalCreditsUsed ?? 0) + creditsConsumed,
        lastSupportAt: now,
        updatedAt: now
      }, { merge: true });
    }

    tx.set(sessionRef, {
      status: "completed",
      endedAt: now,
      problemSummary: payload.problemSummary || session.problemSummary || null,
      solutionSummary: payload.solutionSummary || session.solutionSummary || null,
      internalNotes: payload.internalNotes || session.internalNotes || null,
      billingAppliedAt: session.billingAppliedAt || now,
      updatedAt: now
    }, { merge: true });
  });

  await addDoc(collection(db, COLLECTIONS.supportReports), {
    sessionId: latest.id,
    clientId,
    techId: auth.currentUser?.uid || null,
    createdAt: now,
    summary: payload.problemSummary || null,
    actionsTaken: payload.internalNotes || null,
    solutionApplied: payload.solutionSummary || null,
    followUpNeeded: false
  });
}

function deriveClientStatus(client) {
  if (!client.freeFirstSupportUsed) return "first_support_pending";
  if ((client.credits ?? 0) > 0) return "with_credit";
  return "without_credit";
}

function normalizePhone(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const digits = raw.replace(/[^\d+]/g, "");
  const justDigits = digits.replace(/\D/g, "");
  if (justDigits.length < 10) return null;
  return digits.startsWith("+") ? digits : `+${justDigits}`;
}

function clientDocIdFromPhone(phone) {
  return `phone_${phone.replace(/\D/g, "")}`;
}

function formatDate(ts) {
  if (!ts) return "-";
  return new Date(ts).toLocaleString("pt-BR");
}

function formatMoney(cents) {
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL"
  }).format((Number(cents) || 0) / 100);
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
