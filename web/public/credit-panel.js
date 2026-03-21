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
  updateDoc
} from "https://www.gstatic.com/firebasejs/10.14.1/firebase-firestore.js";

const COLLECTIONS = {
  clients: "clients",
  clientProfiles: "client_profiles",
  supportSessions: "support_sessions",
  supportReports: "support_reports",
  creditOrders: "credit_orders",
  creditPackages: "credit_packages"
};

const state = {
  selectedClientId: null,
  selectedPhone: null,
  packages: []
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
const quickRegisterForm = document.querySelector("#quick-register-form");
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
    authStatus.textContent = "Falha de autenticacao";
  }
  onAuthStateChanged(auth, async (user) => {
    authStatus.textContent = user ? `Conectado: ${user.uid.slice(0, 8)}...` : "Sem autenticacao";
    await loadAll();
  });
}

function bindActions() {
  refreshAllBtn.addEventListener("click", () => {
    loadAll();
  });

  quickRegisterForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = {
      name: document.querySelector("#reg-name").value.trim(),
      phone: normalizePhone(document.querySelector("#reg-phone").value),
      email: document.querySelector("#reg-email").value.trim() || null,
      notes: document.querySelector("#reg-notes").value.trim() || null
    };
    if (!payload.name || !payload.phone) {
      alert("Nome e telefone sao obrigatorios.");
      return;
    }
    try {
      const clientId = await registerOrUpdateClient(payload);
      quickRegisterForm.reset();
      await loadClientProfile(clientId);
      await loadQueue();
    } catch (err) {
      console.error(err);
      alert("Nao foi possivel cadastrar o cliente.");
    }
  });

  searchClientBtn.addEventListener("click", async () => {
    const normalized = normalizePhone(searchPhoneInput.value);
    if (!normalized) {
      alert("Digite um telefone valido.");
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

  const rows = await Promise.all(
    sessions.map(async (session) => {
      const clientId = session.clientId || null;
      if (!clientId) {
        return { session, client: null };
      }
      const clientSnap = await getDoc(doc(db, COLLECTIONS.clients, clientId));
      return {
        session,
        client: clientSnap.exists() ? { id: clientSnap.id, ...clientSnap.data() } : null
      };
    })
  );

  if (!rows.length) {
    queueList.innerHTML = "<small>Sem solicitacoes pendentes.</small>";
    return;
  }

  queueList.innerHTML = "";
  rows.forEach(({ session, client }) => {
    const isNewClient = !client;
    const freePending = isNewClient ? true : client.freeFirstSupportUsed === false;
    const noCredit = !freePending && (client?.credits ?? 0) <= 0;

    const card = document.createElement("article");
    card.className = `queue-item ${isNewClient ? "is-new" : ""} ${noCredit ? "no-credit" : ""}`;
    card.innerHTML = `
      <strong>${client?.name || "Cliente nao cadastrado"}</strong>
      <div>Telefone: ${session.clientPhone || client?.phone || "-"}</div>
      <div>Status da sessao: ${session.status || "-"}</div>
      <div class="queue-meta">
        ${isNewClient ? '<span class="badge warn">Primeiro atendimento gratis</span>' : ""}
        ${noCredit ? '<span class="badge danger">Sem credito</span>' : ""}
        ${!isNewClient && !noCredit ? '<span class="badge ok">Com credito</span>' : ""}
      </div>
      <div class="profile-actions">
        <button type="button" data-open-client="${client?.id || ""}" data-phone="${session.clientPhone || ""}">
          Abrir ficha
        </button>
      </div>
    `;

    const openBtn = card.querySelector("button[data-open-client]");
    openBtn.addEventListener("click", async () => {
      if (client?.id) {
        await loadClientProfile(client.id);
        return;
      }
      const phone = normalizePhone(openBtn.dataset.phone || "");
      if (phone) {
        document.querySelector("#reg-phone").value = phone;
      }
      document.querySelector("#reg-name").focus();
      alert("Cliente nao cadastrado. Use o formulario de cadastro rapido.");
    });

    queueList.appendChild(card);
  });
}

async function loadClientProfile(clientId) {
  const clientSnap = await getDoc(doc(db, COLLECTIONS.clients, clientId));
  if (!clientSnap.exists()) {
    profileContainer.className = "profile-empty";
    profileContainer.textContent = "Cliente nao encontrado.";
    historyList.innerHTML = "";
    state.selectedClientId = null;
    state.selectedPhone = null;
    return;
  }

  const client = { id: clientSnap.id, ...clientSnap.data() };
  const profileSnap = await getDoc(doc(db, COLLECTIONS.clientProfiles, client.id));
  const profile = profileSnap.exists() ? profileSnap.data() : null;

  state.selectedClientId = client.id;
  state.selectedPhone = client.phone || null;
  renderClientProfile(client, profile);
  await loadHistory(client.id);
  await loadOrders(client.id);
}

function renderClientProfile(client, profile) {
  const financialStatus = !client.freeFirstSupportUsed
    ? "primeiro atendimento gratis pendente"
    : (client.credits ?? 0) > 0
      ? "com credito"
      : "sem credito";

  profileContainer.className = "";
  profileContainer.innerHTML = `
    <div class="profile-grid">
      <div><strong>Nome</strong>${client.name || "-"}</div>
      <div><strong>Telefone</strong>${client.phone || "-"}</div>
      <div><strong>E-mail principal</strong>${client.primaryEmail || "-"}</div>
      <div><strong>Creditos disponiveis</strong>${client.credits ?? 0}</div>
      <div><strong>Atendimentos usados</strong>${client.supportsUsed ?? 0}</div>
      <div><strong>Primeiro gratis usado</strong>${client.freeFirstSupportUsed ? "Sim" : "Nao"}</div>
      <div><strong>Status financeiro</strong>${financialStatus}</div>
      <div><strong>Total de sessoes</strong>${profile?.totalSessions ?? 0}</div>
      <div><strong>Total pago</strong>${profile?.totalPaidSessions ?? 0}</div>
      <div><strong>Total gratis</strong>${profile?.totalFreeSessions ?? 0}</div>
      <div><strong>Creditos comprados</strong>${profile?.totalCreditsPurchased ?? 0}</div>
      <div><strong>Creditos usados</strong>${profile?.totalCreditsUsed ?? 0}</div>
      <div><strong>Observacoes</strong>${client.notes || "-"}</div>
    </div>
    <div class="profile-actions">
      <button type="button" id="btn-add-note">Adicionar observacao</button>
      <button type="button" id="btn-add-credit">Adicionar credito manualmente</button>
      <button type="button" id="btn-remove-credit">Remover credito manualmente</button>
      <button type="button" id="btn-close-session">Registrar atendimento concluido</button>
    </div>
  `;

  profileContainer.querySelector("#btn-add-note")
    .addEventListener("click", async () => {
      const note = prompt("Digite a observacao:");
      if (!note) return;
      await appendClientNotes(client.id, note);
      await loadClientProfile(client.id);
    });

  profileContainer.querySelector("#btn-add-credit")
    .addEventListener("click", async () => {
      const amount = Number(prompt("Quantos creditos adicionar?", "1"));
      if (!Number.isFinite(amount) || amount <= 0) return;
      await adjustCredits(client.id, Math.floor(amount));
      await loadClientProfile(client.id);
    });

  profileContainer.querySelector("#btn-remove-credit")
    .addEventListener("click", async () => {
      const amount = Number(prompt("Quantos creditos remover?", "1"));
      if (!Number.isFinite(amount) || amount <= 0) return;
      await adjustCredits(client.id, -Math.floor(amount));
      await loadClientProfile(client.id);
    });

  profileContainer.querySelector("#btn-close-session")
    .addEventListener("click", async () => {
      const problemSummary = prompt("Resumo do problema:");
      const solutionSummary = prompt("Solucao aplicada:");
      await closeLatestOpenSession(client.id, {
        problemSummary,
        solutionSummary,
        internalNotes: "Fechado pelo painel tecnico"
      });
      await loadClientProfile(client.id);
      await loadQueue();
    });
}

async function loadHistory(clientId) {
  historyList.innerHTML = "<small>Carregando historico...</small>";
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
    historyList.innerHTML = "<small>Sem historico de atendimento.</small>";
    return;
  }

  historyList.innerHTML = "";
  sessions.forEach((session) => {
    const report = reportsBySession.get(session.id);
    const item = document.createElement("article");
    item.className = "history-item";
    item.innerHTML = `
      <strong>${formatDate(session.startedAt)} - ${session.status || "-"}</strong>
      <div>Tecnico: ${session.techName || session.techId || "-"}</div>
      <div>Problema: ${session.problemSummary || "-"}</div>
      <div>Solucao: ${session.solutionSummary || report?.solutionApplied || "-"}</div>
      <div>Observacoes: ${session.internalNotes || report?.actionsTaken || "-"}</div>
      <div>Gratuito: ${session.isFreeFirstSupport ? "Sim" : "Nao"} | Creditos consumidos: ${session.creditsConsumed ?? 0}</div>
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
    ordersList.innerHTML = "<small>Sem pedidos de credito.</small>";
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
      <div>Metodo: ${order.paymentMethod || "-"}</div>
      <div>Status: ${order.status || "-"}</div>
      <div>Valor: ${formatMoney(order.amountCents || 0)}</div>
      <div>WhatsApp: ${order.whatsappRequested ? "Sim" : "Nao"}</div>
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
  return clientId;
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
    alert("Nenhuma sessao aberta encontrada para este cliente.");
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
    techId: null,
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
