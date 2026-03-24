import {
  onAuthStateChanged,
  signOut,
} from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js';
import {
  getFirestore,
  collection,
  doc,
  addDoc,
  getDocs,
  limit,
  onSnapshot,
  orderBy,
  query,
  setDoc,
  serverTimestamp,
  where,
  Timestamp,
} from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js';
import { ensureFirebaseApp as ensureSharedFirebaseApp, ensureFirebaseAuth as ensureSharedFirebaseAuth } from '/firebase-client.js';

const SessionStates = Object.freeze({
  IDLE: 'IDLE',
  ACTIVE: 'ACTIVE',
  ENDED: 'ENDED',
});

const CallStates = Object.freeze({
  IDLE: 'IDLE',
  OUTGOING_RINGING: 'OUTGOING_RINGING',
  INCOMING_RINGING: 'INCOMING_RINGING',
  CONNECTING: 'CONNECTING',
  IN_CALL: 'IN_CALL',
  ENDED: 'ENDED',
  DECLINED: 'DECLINED',
  TIMEOUT: 'TIMEOUT',
  FAILED: 'FAILED',
});

const state = {
  queue: [],
  sessions: [],
  metrics: null,
  techProfile: null,
  authToken: null,
  isSupervisor: false,
  supervisorTechs: [],
  selectedSupervisorUid: null,
  supervisorMobileTab: 'list',
  techIdentifiers: new Set(),
  selectedSessionId: null,
  sessionFilter: 'all',
  joinedSessionId: null,
  sessionState: SessionStates.IDLE,
  activeSessionId: null,
  chatBySession: new Map(),
  telemetryBySession: new Map(),
  clientContextBySession: new Map(),
  clientContextByRequest: new Map(),
  clientContextFetchedAt: new Map(),
  renderedChatSessionId: null,
  clientModal: {
    sessionId: null,
    requestId: null,
    context: null,
    formDirty: false,
  },
  clientsHub: {
    items: [],
    query: '',
    loading: false,
    lastLoadedAt: 0,
  },
  commandState: {
    shareActive: false,
    remoteActive: false,
    callActive: false,
  },
  lightbox: {
    isOpen: false,
    imageUrl: '',
    zoom: 1,
  },
  call: {
    sessionId: null,
    status: CallStates.IDLE,
    direction: null,
    callId: null,
    fromUid: null,
    toUid: null,
    pc: null,
    callDocRef: null,
    localIceRef: null,
    remoteIceRef: null,
    ringTimeoutId: null,
    remoteIceUnsub: null,
    remoteIceIds: new Set(),
    localIceCount: 0,
    remoteIceCount: 0,
    muted: false,
    offerSent: false,
    answerSent: false,
    pendingRemoteIce: [],
    remoteOfferApplying: false,
    remoteAnswerApplying: false,
    connectedAtMs: null,
    statusTickerId: null,
  },
  media: {
    sessionId: null,
    pc: null,
    ctrlChannel: null,
    rtcMetricsIntervalId: null,
    eventsSessionId: null,
    eventsUnsub: null,
    eventsRef: null,
    eventsStartedAtMs: 0,
    processedEventIds: new Set(),
    pendingRemoteIce: [],
    local: {
      screen: null,
      audio: null,
    },
    senders: {
      screen: [],
      audio: [],
    },
    remoteStream: null,
    remoteAudioStream: null,
  },
  whiteboard: {
    canvas: null,
    ctx: null,
    queue: [],
    buffer: [],
    bufferTimer: null,
    strokes: new Map(),
    rafId: null,
    resizeRafId: null,
    metrics: null,
    lastMetricsAt: 0,
    lastSize: { width: 0, height: 0 },
    droppedPoints: 0,
  },
  legacyShare: {
    room: null,
    pc: null,
    remoteStream: null,
    remoteAudioStream: null,
    active: false,
    pendingRoom: null,
  },
  chatComposer: {
    recording: false,
    recorder: null,
    stream: null,
    chunks: [],
    startedAt: 0,
    timerId: null,
    uploading: false,
  },
};

let firestoreInstance = null;
let authInstance = null;
let authReadyPromise = null;
let toastTimerId = null;

const QUEUE_RETRY_INITIAL_DELAY_MS = 5000;
const QUEUE_RETRY_MAX_DELAY_MS = 60000;
const QUEUE_AUTO_REFRESH_INTERVAL_MS = 7000;
const TEMPORARY_QUEUE_ERROR_STATUS = new Set([500, 502, 503, 504]);
let queueRetryDelayMs = QUEUE_RETRY_INITIAL_DELAY_MS;
let queueRetryTimer = null;
let queueLoadPromise = null;
let queueUnavailable = false;
let queueAutoRefreshIntervalId = null;

const CHAT_DEBUG_LOGS_ENABLED = (() => {
  try {
    return window.SUPORTEX_DEBUG_CHAT_LOGS === true || window.localStorage?.getItem('sx_debug_chat') === '1';
  } catch (_error) {
    return false;
  }
})();

const debugChatLog = (...args) => {
  if (!CHAT_DEBUG_LOGS_ENABLED) return;
  console.log(...args);
};

const CALL_RING_TIMEOUT_MS = 20000;
const CALL_STATUS_LABELS = {
  [CallStates.OUTGOING_RINGING]: 'Chamando…',
  [CallStates.INCOMING_RINGING]: 'Chamada recebida',
  [CallStates.CONNECTING]: 'Conectando…',
  [CallStates.IN_CALL]: 'Em chamada',
  [CallStates.ENDED]: 'Chamada encerrada',
  [CallStates.DECLINED]: 'Chamada recusada',
  [CallStates.TIMEOUT]: 'Sem resposta',
  [CallStates.FAILED]: 'Falha na chamada',
};

function ensureString(v) {
  if (v === undefined || v === null) return '';
  return String(v);
}

const ensureFirebaseApp = () => {
  try {
    return ensureSharedFirebaseApp();
  } catch (error) {
    console.error('Erro ao inicializar Firebase', error);
    return null;
  }
};

const ensureFirestore = () => {
  if (firestoreInstance) return firestoreInstance;
  const app = ensureFirebaseApp();
  if (!app) return null;
  try {
    firestoreInstance = getFirestore(app);
  } catch (error) {
    console.error('Erro ao inicializar Firestore', error);
    firestoreInstance = null;
  }
  return firestoreInstance;
};

const waitForAuthState = () =>
  new Promise((resolve, reject) => {
    if (!authInstance) {
      reject(new Error('Auth não inicializado'));
      return;
    }
    const unsub = onAuthStateChanged(
      authInstance,
      (user) => {
        unsub();
        resolve(user || null);
      },
      (error) => {
        unsub();
        reject(error);
      }
    );
  });

const ensureAuth = async () => {
  if (authReadyPromise) return authReadyPromise;
  const app = ensureFirebaseApp();
  if (!app) return null;
  if (!authInstance) {
    try {
      authInstance = ensureSharedFirebaseAuth();
    } catch (error) {
      console.error('Erro ao inicializar Firebase Auth', error);
      return null;
    }
  }
  if (authInstance.currentUser) {
    return authInstance.currentUser;
  }

  authReadyPromise = waitForAuthState();
  try {
    return await authReadyPromise;
  } finally {
    authReadyPromise = null;
  }
};

const redirectToTechLogin = (reason = '') => {
  const params = new URLSearchParams();
  params.set('next', '/central.html');
  if (reason) params.set('reason', reason);
  window.location.replace(`/tech-login.html?${params.toString()}`);
};

const ensureTechAccess = async (authUser) => {
  if (!authUser) {
    redirectToTechLogin('auth_required');
    return null;
  }
  const token = await getIdToken(true);
  const response = await fetch('/api/auth/me', {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    await signOut(authInstance).catch(() => {});
    let reason = payload?.error || 'auth_failed';
    if (reason === 'tech_inactive') reason = 'inactive';
    if (reason === 'insufficient_role') reason = 'not_tech';
    redirectToTechLogin(reason);
    return null;
  }

  return response.json();
};


const getIdToken = async (forceRefresh = false) => {
  const user = await ensureAuth();
  if (!user) throw new Error('auth_required');
  const idToken = await user.getIdToken(forceRefresh);
  state.authToken = idToken;
  return idToken;
};

const authFetch = async (url, options = {}, { forceRefresh = false } = {}) => {
  const token = await getIdToken(forceRefresh);
  const headers = {
    ...(options.headers || {}),
    Authorization: `Bearer ${token}`,
  };
  return fetch(url, { ...options, headers });
};

const generateTempPasswordClient = (length = 12) => {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789@#$%';
  let out = '';
  const size = Math.max(8, Number(length) || 12);
  for (let i = 0; i < size; i += 1) {
    out += chars[Math.floor(Math.random() * chars.length)];
  }
  return out;
};

const sessionRealtimeSubscriptions = new Map();
let pendingSessionsPromise = null;

const unsubscribeSessionRealtime = (sessionId) => {
  const entry = sessionRealtimeSubscriptions.get(sessionId);
  if (!entry) return;
  try {
    if (typeof entry.messages === 'function') entry.messages();
  } catch (error) {
    console.warn('Falha ao cancelar listener de mensagens', error);
  }
  try {
    if (typeof entry.events === 'function') entry.events();
  } catch (error) {
    console.warn('Falha ao cancelar listener de eventos', error);
  }
  try {
    if (typeof entry.call === 'function') entry.call();
  } catch (error) {
    console.warn('Falha ao cancelar listener de chamada', error);
  }
  sessionRealtimeSubscriptions.delete(sessionId);
};

const unsubscribeAllSessionRealtime = () => {
  sessionRealtimeSubscriptions.forEach((_value, sessionId) => unsubscribeSessionRealtime(sessionId));
  sessionRealtimeSubscriptions.clear();
};

const dom = {
  queue: document.getElementById('queue'),
  queueEmpty: document.getElementById('queueEmpty'),
  queueRetry: document.getElementById('queueRetry'),
  availability: document.getElementById('availabilityLabel'),
  techStatus: document.getElementById('techStatus'),
  techRole: document.getElementById('techRole'),
  techRoleSecondary: document.getElementById('techRoleSecondary'),
  activeSessionsLabel: document.getElementById('activeSessionsLabel'),
  metricAttendances: document.querySelector('[data-metric="attendances"]'),
  metricQueue: document.querySelector('[data-metric="queue"]'),
  metricFcr: document.querySelector('[data-metric="fcr"]'),
  metricFcrDetail: document.querySelector('[data-metric="fcr-detail"]'),
  metricNps: document.querySelector('[data-metric="nps"]'),
  metricNpsDetail: document.querySelector('[data-metric="nps-detail"]'),
  metricHandle: document.querySelector('[data-metric="handle"]'),
  metricWait: document.querySelector('[data-metric="wait"]'),
  contextDevice: document.getElementById('contextDevice'),
  contextIdentity: document.getElementById('contextIdentity'),
  contextIdentityAction: document.getElementById('contextIdentityAction'),
  contextNetwork: document.getElementById('contextNetwork'),
  contextHealth: document.getElementById('contextHealth'),
  contextPermissions: document.getElementById('contextPermissions'),
  contextStorage: document.getElementById('contextStorage'),
  contextBattery: document.getElementById('contextBattery'),
  contextTemperature: document.getElementById('contextTemperature'),
  contextDeviceImage: document.getElementById('contextDeviceImage'),
  contextTimeline: document.getElementById('contextTimeline'),
  sessionPlaceholder: document.getElementById('sessionPlaceholder'),
  indicatorNetwork: document.getElementById('indicatorNetwork'),
  indicatorQuality: document.getElementById('indicatorQuality'),
  indicatorAlerts: document.getElementById('indicatorAlerts'),
  techIdentity: document.querySelector('.tech-identity'),
  techInitials: document.getElementById('techAvatar'),
  techName: document.getElementById('topbarTechName'),
  techPhoto: document.getElementById('techPhoto'),
  logoutBtn: document.getElementById('logoutBtn'),
  profileMenuTrigger: document.getElementById('profileMenuTrigger'),
  profileMenu: document.getElementById('profileMenu'),
  menuProfile: document.getElementById('menuProfile'),
  menuReports: document.getElementById('menuReports'),
  menuClients: document.getElementById('menuClients'),
  menuSupervisor: document.getElementById('menuSupervisor'),
  profileModal: document.getElementById('profileModal'),
  profileForm: document.getElementById('profileForm'),
  profileNameInput: document.getElementById('profileNameInput'),
  profileEmailInput: document.getElementById('profileEmailInput'),
  profileStatusInput: document.getElementById('profileStatusInput'),
  profilePhotoInput: document.getElementById('profilePhotoInput'),
  profileResetPassword: document.getElementById('profileResetPassword'),
  profileResult: document.getElementById('profileResult'),
  profileCancel: document.getElementById('profileCancel'),
  supervisorModal: document.getElementById('supervisorModal'),
  supervisorList: document.getElementById('supervisorList'),
  supervisorSearch: document.getElementById('supervisorSearch'),
  supervisorDetailForm: document.getElementById('supervisorDetailForm'),
  supervisorEmpty: document.getElementById('supervisorEmpty'),
  supervisorDetailResult: document.getElementById('supervisorDetailResult'),
  selectedTechAvatar: document.getElementById('selectedTechAvatar'),
  selectedTechName: document.getElementById('selectedTechName'),
  selectedTechUid: document.getElementById('selectedTechUid'),
  copyTechUidBtn: document.getElementById('copyTechUidBtn'),
  editTechName: document.getElementById('editTechName'),
  editTechEmail: document.getElementById('editTechEmail'),
  editTechStatus: document.getElementById('editTechStatus'),
  editTechRole: document.getElementById('editTechRole'),
  editTechReset: document.getElementById('editTechReset'),
  editTechDelete: document.getElementById('editTechDelete'),
  supervisorNewTech: document.getElementById('supervisorNewTech'),
  supervisorNewTechMobile: document.getElementById('supervisorNewTechMobile'),
  tabList: document.getElementById('tabList'),
  tabDetails: document.getElementById('tabDetails'),
  supervisorListPanel: document.getElementById('supervisorListPanel'),
  supervisorDetailPanel: document.getElementById('supervisorDetailPanel'),
  createTechModal: document.getElementById('createTechModal'),
  createTechForm: document.getElementById('createTechForm'),
  createTechName: document.getElementById('createTechName'),
  createTechEmail: document.getElementById('createTechEmail'),
  createTechPassword: document.getElementById('createTechPassword'),
  createTechGenerate: document.getElementById('createTechGenerate'),
  createTechResult: document.getElementById('createTechResult'),
  clientModal: document.getElementById('clientModal'),
  clientModalTitle: document.getElementById('clientModalTitle'),
  clientModalSubtitle: document.getElementById('clientModalSubtitle'),
  clientModalAlert: document.getElementById('clientModalAlert'),
  clientModalSummary: document.getElementById('clientModalSummary'),
  clientModalHistory: document.getElementById('clientModalHistory'),
  clientRegisterForm: document.getElementById('clientRegisterForm'),
  clientRegisterName: document.getElementById('clientRegisterName'),
  clientRegisterPhone: document.getElementById('clientRegisterPhone'),
  clientRegisterEmail: document.getElementById('clientRegisterEmail'),
  clientRegisterNotes: document.getElementById('clientRegisterNotes'),
  clientRegisterSubmit: document.getElementById('clientRegisterSubmit'),
  clientRegisterRefresh: document.getElementById('clientRegisterRefresh'),
  clientRegisterResult: document.getElementById('clientRegisterResult'),
  clientAddCreditBtn: document.getElementById('clientAddCreditBtn'),
  clientRemoveCreditBtn: document.getElementById('clientRemoveCreditBtn'),
  clientAddNoteBtn: document.getElementById('clientAddNoteBtn'),
  clientRequestManualVerificationBtn: document.getElementById('clientRequestManualVerificationBtn'),
  clientConfirmManualVerificationBtn: document.getElementById('clientConfirmManualVerificationBtn'),
  clientMarkMismatchBtn: document.getElementById('clientMarkMismatchBtn'),
  clientsHubModal: document.getElementById('clientsHubModal'),
  clientsHubSearch: document.getElementById('clientsHubSearch'),
  clientsHubRefresh: document.getElementById('clientsHubRefresh'),
  clientsHubAlert: document.getElementById('clientsHubAlert'),
  clientsHubList: document.getElementById('clientsHubList'),
  techDataset: document.body,
  topbarTechName: document.getElementById('topbarTechName'),
  filterMine: document.getElementById('filterMine'),
  filterQueue: document.getElementById('filterQueue'),
  filterAll: document.getElementById('filterAll'),
  chatThread: document.getElementById('chatThread'),
  chatForm: document.getElementById('chatForm'),
  chatInput: document.getElementById('chatInput'),
  chatAudioBtn: document.getElementById('chatAudioBtn'),
  chatAttachBtn: document.getElementById('chatAttachBtn'),
  chatFileInput: document.getElementById('chatFileInput'),
  chatMediaStatus: document.getElementById('chatMediaStatus'),
  chatUploadProgress: document.getElementById('chatUploadProgress'),
  quickReplies: document.querySelectorAll('.quick-replies button[data-reply]'),
  sessionVideo: document.getElementById('sessionVideo'),
  sessionAudio: document.getElementById('sessionAudio'),
  videoShell: document.getElementById('videoShell'),
  whiteboardCanvas: document.getElementById('whiteboardCanvas'),
  controlStart: document.getElementById('controlStart'),
  controlQuality: document.getElementById('controlQuality'),
  controlRemote: document.getElementById('controlRemote'),
  controlFullscreen: document.getElementById('controlFullscreen'),
  controlPip: document.getElementById('controlPip'),
  controlStats: document.getElementById('controlStats'),
  controlMenuToggle: document.getElementById('controlMenuToggle'),
  controlMenuPanel: document.getElementById('controlMenuPanel'),
  controlMenuBackdrop: document.getElementById('controlMenuBackdrop'),
  remoteTextInput: document.getElementById('remoteTextInput'),
  webSharePanel: document.getElementById('webSharePanel'),
  webShareRoom: document.getElementById('webShareRoom'),
  webShareConnect: document.getElementById('webShareConnect'),
  webShareDisconnect: document.getElementById('webShareDisconnect'),
  webShareStatus: document.getElementById('webShareStatus'),
  callModal: document.getElementById('callModal'),
  callModalStatus: document.getElementById('callModalStatus'),
  callModalName: document.getElementById('callModalName'),
  callModalSession: document.getElementById('callModalSession'),
  callModalAccept: document.getElementById('callModalAccept'),
  callModalDecline: document.getElementById('callModalDecline'),
  callModalMute: document.getElementById('callModalMute'),
  callModalHangup: document.getElementById('callModalHangup'),
  imageLightbox: document.getElementById('imageLightbox'),
  imageLightboxImage: document.getElementById('imageLightboxImage'),
  imageLightboxClose: document.getElementById('imageLightboxClose'),
  imageLightboxDownload: document.getElementById('imageLightboxDownload'),
  imageLightboxOpen: document.getElementById('imageLightboxOpen'),
  imageLightboxZoomIn: document.getElementById('imageLightboxZoomIn'),
  imageLightboxZoomOut: document.getElementById('imageLightboxZoomOut'),
  closureForm: document.getElementById('closureForm'),
  closureOutcome: document.getElementById('closureOutcome'),
  closureSymptom: document.getElementById('closureSymptom'),
  closureSolution: document.getElementById('closureSolution'),
  closureNps: document.getElementById('closureNps'),
  closureFcr: document.getElementById('closureFcr'),
  closureSubmit: document.getElementById('closureSubmit'),
  toast: document.getElementById('toast'),
};

const getLegacyRoomFromQuery = () => {
  try {
    return new URLSearchParams(window.location.search).get('room');
  } catch (error) {
    console.warn('Falha ao ler room da URL', error);
    return null;
  }
};

const getTechDatasetElement = () => dom.techIdentity || dom.techDataset;

const getTechDataset = () => getTechDatasetElement()?.dataset || {};

const updateTechDataset = (entries = {}) => {
  const target = getTechDatasetElement();
  if (!target) return;
  Object.entries(entries).forEach(([key, value]) => {
    if (value === undefined || value === null) return;
    target.dataset[key] = String(value);
  });
};

const showToast = (message) => {
  if (!dom.toast) return;
  if (toastTimerId) {
    clearTimeout(toastTimerId);
    toastTimerId = null;
  }
  dom.toast.textContent = message;
  dom.toast.hidden = !message;
  if (!message) return;
  toastTimerId = setTimeout(() => {
    hideToast();
  }, 5000);
};

const hideToast = () => {
  if (!dom.toast) return;
  if (toastTimerId) {
    clearTimeout(toastTimerId);
    toastTimerId = null;
  }
  dom.toast.textContent = '';
  dom.toast.hidden = true;
};

const logCall = (...args) => {
  console.info('[CALL]', ...args);
};

const getCallSessionInfo = (sessionId) => {
  const session = state.sessions.find((entry) => entry.sessionId === sessionId) || null;
  const name = session?.clientName || 'Cliente';
  const label = session?.sessionId ? `Sessão ${session.sessionId}` : 'Sessão —';
  return { session, name, label };
};

const generateCallId = () => {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
};

const clearCallTimeout = () => {
  if (state.call.ringTimeoutId) {
    clearTimeout(state.call.ringTimeoutId);
    state.call.ringTimeoutId = null;
  }
};

const formatCallElapsed = (elapsedMs) => {
  if (!Number.isFinite(elapsedMs) || elapsedMs < 0) return '00:00';
  const totalSeconds = Math.floor(elapsedMs / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) {
    return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
  }
  return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
};

const stopCallStatusTicker = ({ resetConnectedAt = true } = {}) => {
  if (state.call.statusTickerId) {
    clearInterval(state.call.statusTickerId);
    state.call.statusTickerId = null;
  }
  if (resetConnectedAt) {
    state.call.connectedAtMs = null;
  }
};

const ensureCallStatusTicker = () => {
  if (state.call.statusTickerId) return;
  state.call.statusTickerId = setInterval(() => {
    updateCallModal();
  }, 1000);
};

const getCallModalStatusText = () => {
  const baseText = CALL_STATUS_LABELS[state.call.status] || 'Chamada em andamento';
  if (state.call.status !== CallStates.IN_CALL || !state.call.connectedAtMs) {
    return baseText;
  }
  const elapsed = Date.now() - state.call.connectedAtMs;
  return `${baseText} • ${formatCallElapsed(elapsed)}`;
};

const updateCallControlLabel = () => {
  if (!dom.controlQuality) return;
  switch (state.call.status) {
    case CallStates.OUTGOING_RINGING:
      dom.controlQuality.textContent = 'Cancelar chamada';
      break;
    case CallStates.INCOMING_RINGING:
      dom.controlQuality.textContent = 'Chamada recebida';
      break;
    case CallStates.CONNECTING:
    case CallStates.IN_CALL:
      dom.controlQuality.textContent = 'Encerrar chamada';
      break;
    default:
      dom.controlQuality.textContent = 'Iniciar chamada';
      break;
  }
};

const updateCallModal = () => {
  if (!dom.callModal) return;
  if (state.call.status === CallStates.IDLE) {
    dom.callModal.hidden = true;
    return;
  }
  const { name, label } = getCallSessionInfo(state.call.sessionId);
  if (dom.callModalName) dom.callModalName.textContent = name;
  if (dom.callModalSession) dom.callModalSession.textContent = label;
  if (dom.callModalStatus) {
    dom.callModalStatus.textContent = getCallModalStatusText();
  }

  const incoming = state.call.status === CallStates.INCOMING_RINGING;
  const outgoing = state.call.status === CallStates.OUTGOING_RINGING;
  const active = [CallStates.CONNECTING, CallStates.IN_CALL].includes(state.call.status);

  if (dom.callModalAccept) dom.callModalAccept.hidden = !incoming;
  if (dom.callModalDecline) {
    dom.callModalDecline.hidden = !(incoming || outgoing);
    dom.callModalDecline.textContent = outgoing ? 'Cancelar' : 'Recusar';
  }
  if (dom.callModalMute) {
    dom.callModalMute.hidden = !active;
    dom.callModalMute.textContent = state.call.muted ? 'Ativar microfone' : 'Silenciar';
  }
  if (dom.callModalHangup) dom.callModalHangup.hidden = !active;

  dom.callModal.hidden = false;
};

const setCallState = (nextState, { sessionId, direction, callId } = {}) => {
  const previous = state.call.status;
  state.call.status = nextState;
  if (sessionId !== undefined) state.call.sessionId = sessionId;
  if (direction !== undefined) state.call.direction = direction;
  if (callId !== undefined) state.call.callId = callId;
  if (nextState === CallStates.IN_CALL) {
    if (!state.call.connectedAtMs) {
      state.call.connectedAtMs = Date.now();
    }
    ensureCallStatusTicker();
  } else if (previous === CallStates.IN_CALL || state.call.statusTickerId || state.call.connectedAtMs) {
    stopCallStatusTicker();
  }
  state.commandState.callActive = [CallStates.CONNECTING, CallStates.IN_CALL].includes(nextState);
  updateCallControlLabel();
  updateCallModal();
  if (previous !== nextState) {
    logCall('state ->', nextState, 'session=', state.call.sessionId);
  }
};

const cleanupCallSession = ({ message = null } = {}) => {
  clearCallTimeout();
  stopCallStatusTicker();
  if (state.call.remoteIceUnsub) {
    try {
      state.call.remoteIceUnsub();
    } catch (error) {
      console.warn('Falha ao cancelar listener ICE remoto', error);
    }
  }
  state.call.remoteIceUnsub = null;
  state.call.remoteIceIds = new Set();
  state.call.localIceCount = 0;
  state.call.remoteIceCount = 0;
  state.call.offerSent = false;
  state.call.answerSent = false;
  state.call.pendingRemoteIce = [];
  state.call.remoteOfferApplying = false;
  state.call.remoteAnswerApplying = false;
  state.call.callDocRef = null;
  state.call.localIceRef = null;
  state.call.remoteIceRef = null;
  state.call.direction = null;
  state.call.callId = null;
  state.call.fromUid = null;
  state.call.toUid = null;
  state.call.muted = false;
  stopCallMedia();
  setCallState(CallStates.IDLE, { sessionId: null, direction: null });
  updateCallModal();
  if (message) {
    addChatMessage({ author: 'Sistema', text: message, kind: 'system' });
  }
};

const CTRL_CHANNEL_LABEL = 'ctrl';
const POINTER_MOVE_THROTTLE_MS = 33;
const TEXT_SEND_DEBOUNCE_MS = 80;
const WHITEBOARD_BATCH_INTERVAL_MS = 16;
const WHITEBOARD_MAX_POINTS_PER_FRAME = 400;
const WHITEBOARD_MAX_QUEUE_SIZE = 5000;
const WHITEBOARD_COALESCE_THRESHOLD = 1500;
const WHITEBOARD_METRICS_INTERVAL_MS = 2000;
const RTC_METRICS_INTERVAL_MS = 5000;
const WHITEBOARD_COMMAND_TYPES = new Set(['whiteboard', 'whiteboard_event', 'draw', 'drawing', 'wb']);
const WHITEBOARD_DEBUG = (() => {
  try {
    return (
      Boolean(window.__WHITEBOARD_DEBUG__) ||
      new URLSearchParams(window.location.search).has('wbMetrics')
    );
  } catch (error) {
    console.warn('Falha ao detectar parâmetro wbMetrics', error);
    return false;
  }
})();
const RTC_METRICS_DEBUG = (() => {
  try {
    return (
      Boolean(window.__RTC_METRICS_DEBUG__) ||
      new URLSearchParams(window.location.search).has('rtcMetrics')
    );
  } catch (error) {
    console.warn('Falha ao detectar parâmetro rtcMetrics', error);
    return false;
  }
})();

const hasActiveVideo = () => Boolean(dom.sessionVideo && dom.sessionVideo.srcObject && !dom.sessionVideo.hidden);

const canSendControlCommand = () =>
  Boolean(state.commandState.remoteActive && state.media.ctrlChannel && state.media.ctrlChannel.readyState === 'open');

const resetRemoteControlChannel = () => {
  if (!state.media.ctrlChannel) return;
  try {
    state.media.ctrlChannel.onopen = null;
    state.media.ctrlChannel.onclose = null;
    state.media.ctrlChannel.onerror = null;
    state.media.ctrlChannel.onmessage = null;
    state.media.ctrlChannel.close();
  } catch (error) {
    console.warn('Falha ao encerrar DataChannel de controle', error);
  }
  state.media.ctrlChannel = null;
};

const setCtrlChannel = (channel) => {
  if (!channel) return;
  if (state.media.ctrlChannel && state.media.ctrlChannel !== channel) {
    resetRemoteControlChannel();
  }
  state.media.ctrlChannel = channel;
  channel.onopen = () => console.log('[CTRL] open');
  channel.onclose = () => console.log('[CTRL] close');
  channel.onerror = (event) => console.log('[CTRL] error', event);
  channel.onmessage = handleCtrlChannelMessage;
};

const ensureCtrlChannelForOffer = (pc) => {
  if (!pc) return null;
  if (state.media.ctrlChannel && state.media.ctrlChannel.readyState !== 'closed') {
    return state.media.ctrlChannel;
  }
  try {
    const channel = pc.createDataChannel(CTRL_CHANNEL_LABEL, { ordered: true });
    setCtrlChannel(channel);
    return channel;
  } catch (error) {
    console.warn('Falha ao criar DataChannel de controle', error);
    return null;
  }
};

const isMediaPcUnhealthy = (pc) =>
  !pc ||
  pc.signalingState === 'closed' ||
  ['failed', 'closed'].includes(pc.connectionState) ||
  (!state.commandState.shareActive && pc.connectionState === 'disconnected');

const resetMediaPeerConnection = (sessionId) => {
  if (state.media.pc) {
    try {
      state.media.pc.ontrack = null;
      state.media.pc.onicecandidate = null;
      state.media.pc.onconnectionstatechange = null;
      state.media.pc.ondatachannel = null;
      state.media.pc.close();
    } catch (error) {
      console.warn('Falha ao resetar PeerConnection de mídia', error);
    }
  }
  if (state.media.ctrlChannel) {
    resetRemoteControlChannel();
  }
  state.media.pc = null;
  state.media.sessionId = sessionId || null;
  state.media.pendingRemoteIce = [];
  clearRemoteVideo();
};

const flushPendingMediaIce = async (pc) => {
  if (!pc?.remoteDescription) return;
  const pending = Array.isArray(state.media.pendingRemoteIce) ? [...state.media.pendingRemoteIce] : [];
  state.media.pendingRemoteIce = [];
  for (const candidate of pending) {
    try {
      await pc.addIceCandidate(candidate);
    } catch (error) {
      console.warn('Falha ao aplicar ICE pendente de mídia', error);
    }
  }
};

const sendCtrlCommand = (command) => {
  if (!command || !canSendControlCommand()) return;
  try {
    state.media.ctrlChannel.send(JSON.stringify(command));
  } catch (error) {
    console.warn('Falha ao enviar comando de controle', error);
  }
};

const renegotiateRemoteControl = async (sessionId) => {
  if (!sessionId) return;
  const pc = ensurePeerConnection(sessionId);
  if (!pc) return;
  ensureCtrlChannelForOffer(pc);
  try {
    const offer = await pc.createOffer({ iceRestart: true });
    await pc.setLocalDescription(offer);
    if (socket && !socket.disconnected) {
      socket.emit('signal:offer', { sessionId, sdp: pc.localDescription });
    }
  } catch (error) {
    console.warn('Falha ao renegociar canal de controle remoto', error);
  }
};

const getVideoFrameSize = (videoEl, rect) => {
  const frameW = videoEl.videoWidth;
  const frameH = videoEl.videoHeight;
  return {
    frameW: frameW || rect.width,
    frameH: frameH || rect.height,
  };
};

const getVideoContentRect = (videoEl, rectOverride = null) => {
  const rect = rectOverride || videoEl.getBoundingClientRect();
  const { frameW, frameH } = getVideoFrameSize(videoEl, rect);
  const style = window.getComputedStyle(videoEl);
  const objectFit = style.objectFit || 'contain';

  let drawW = rect.width;
  let drawH = rect.height;
  let offX = 0;
  let offY = 0;

  if (frameW > 0 && frameH > 0) {
    const scaleContain = Math.min(rect.width / frameW, rect.height / frameH);
    const scaleCover = Math.max(rect.width / frameW, rect.height / frameH);
    const scale = objectFit === 'cover' ? scaleCover : scaleContain;

    drawW = frameW * scale;
    drawH = frameH * scale;
    offX = (rect.width - drawW) / 2;
    offY = (rect.height - drawH) / 2;
  }

  const contentLeft = rect.left + offX;
  const contentTop = rect.top + offY;

  return {
    rect,
    frameW,
    frameH,
    drawW,
    drawH,
    offX,
    offY,
    contentLeft,
    contentTop,
  };
};

const getNormalizedXY = (videoEl, event) => {
  const rectSource = videoEl.getBoundingClientRect();
  const { rect, drawW, drawH, offX, offY } = getVideoContentRect(videoEl, rectSource);
  const hasOffset = typeof event?.offsetX === 'number' && typeof event?.offsetY === 'number';
  const isVideoTarget = event?.target === videoEl || event?.currentTarget === videoEl;
  const localX = hasOffset && isVideoTarget
    ? event.offsetX
    : (typeof event?.clientX === 'number' ? event.clientX : rect.left) - rect.left;
  const localY = hasOffset && isVideoTarget
    ? event.offsetY
    : (typeof event?.clientY === 'number' ? event.clientY : rect.top) - rect.top;
  if (!drawW || !drawH) {
    return {
      x: 0,
      y: 0,
      width: rect.width,
      height: rect.height,
      inBounds: true,
    };
  }
  const inBounds =
    localX >= offX &&
    localX <= offX + drawW &&
    localY >= offY &&
    localY <= offY + drawH;
  const x = (localX - offX) / drawW;
  const y = (localY - offY) / drawH;

  return {
    x: Math.min(1, Math.max(0, x)),
    y: Math.min(1, Math.max(0, y)),
    width: rect.width,
    height: rect.height,
    inBounds,
  };
};

const normalizeOriginTimestamp = (value) => {
  if (typeof value !== 'number' || Number.isNaN(value)) return null;
  if (value > 1e12) return value / 1e6;
  return value;
};

const estimatePayloadBytes = (payload) => {
  if (!WHITEBOARD_DEBUG) return 0;
  if (payload == null) return 0;
  if (typeof payload === 'string') return payload.length;
  try {
    return JSON.stringify(payload).length;
  } catch (error) {
    console.warn('Falha ao estimar tamanho do payload', error);
    return 0;
  }
};

const initWhiteboardMetrics = () => ({
  windowStart: performance.now(),
  receivedEvents: 0,
  receivedPoints: 0,
  receivedBytes: 0,
  drawnPoints: 0,
  droppedPoints: 0,
  totalNetworkMs: 0,
  totalRenderMs: 0,
  totalE2eMs: 0,
  latencySamples: 0,
  networkSamples: [],
  renderSamples: [],
  e2eSamples: [],
  maxNetworkMs: 0,
  maxRenderMs: 0,
  maxE2eMs: 0,
  lastReceivedAt: null,
  gapSamples: 0,
  gapSum: 0,
  gapSumSquares: 0,
  maxGapMs: 0,
  maxQueueLen: 0,
  maxBufferedAmount: 0,
});

const resetWhiteboardMetrics = () => {
  state.whiteboard.metrics = initWhiteboardMetrics();
  state.whiteboard.lastMetricsAt = performance.now();
  state.whiteboard.droppedPoints = 0;
};

const getWhiteboardPercentiles = (samples, percentiles = [50, 95, 99]) => {
  if (!samples.length) return {};
  const sorted = [...samples].sort((a, b) => a - b);
  const result = {};
  percentiles.forEach((pct) => {
    const index = Math.max(0, Math.min(sorted.length - 1, Math.floor((pct / 100) * (sorted.length - 1))));
    result[pct] = sorted[index];
  });
  return result;
};

const reportWhiteboardMetrics = ({ force = false } = {}) => {
  if (!WHITEBOARD_DEBUG || !state.whiteboard.metrics) return;
  const now = performance.now();
  const elapsed = now - state.whiteboard.metrics.windowStart;
  if (!force && elapsed < WHITEBOARD_METRICS_INTERVAL_MS) return;
  const metrics = state.whiteboard.metrics;
  const avgNetwork = metrics.latencySamples ? metrics.totalNetworkMs / metrics.latencySamples : 0;
  const avgRender = metrics.latencySamples ? metrics.totalRenderMs / metrics.latencySamples : 0;
  const avgE2e = metrics.latencySamples ? metrics.totalE2eMs / metrics.latencySamples : 0;
  const eventsPerSec = metrics.receivedEvents ? (metrics.receivedEvents / elapsed) * 1000 : 0;
  const pointsPerSec = metrics.receivedPoints ? (metrics.receivedPoints / elapsed) * 1000 : 0;
  const bytesPerSec = metrics.receivedBytes ? (metrics.receivedBytes / elapsed) * 1000 : 0;
  const gapAvg = metrics.gapSamples ? metrics.gapSum / metrics.gapSamples : 0;
  const gapVariance = metrics.gapSamples
    ? metrics.gapSumSquares / metrics.gapSamples - gapAvg * gapAvg
    : 0;
  const gapJitter = Math.sqrt(Math.max(0, gapVariance));
  const networkPercentiles = getWhiteboardPercentiles(metrics.networkSamples);
  const renderPercentiles = getWhiteboardPercentiles(metrics.renderSamples);
  const e2ePercentiles = getWhiteboardPercentiles(metrics.e2eSamples);
  const bufferedAmount =
    state.media.ctrlChannel && typeof state.media.ctrlChannel.bufferedAmount === 'number'
      ? state.media.ctrlChannel.bufferedAmount
      : null;
  if (typeof bufferedAmount === 'number') {
    metrics.maxBufferedAmount = Math.max(metrics.maxBufferedAmount, bufferedAmount);
  }

  console.info('[WB][metrics]', {
    windowMs: Math.round(elapsed),
    events: metrics.receivedEvents,
    points: metrics.receivedPoints,
    drawnPoints: metrics.drawnPoints,
    droppedPoints: metrics.droppedPoints,
    eventsPerSec: Math.round(eventsPerSec),
    pointsPerSec: Math.round(pointsPerSec),
    kbPerSec: Math.round(bytesPerSec / 1024),
    avgNetworkMs: Math.round(avgNetwork),
    avgRenderMs: Math.round(avgRender),
    avgE2eMs: Math.round(avgE2e),
    p50NetworkMs: Math.round(networkPercentiles[50] || 0),
    p95NetworkMs: Math.round(networkPercentiles[95] || 0),
    p99NetworkMs: Math.round(networkPercentiles[99] || 0),
    maxNetworkMs: Math.round(metrics.maxNetworkMs || 0),
    p50RenderMs: Math.round(renderPercentiles[50] || 0),
    p95RenderMs: Math.round(renderPercentiles[95] || 0),
    p99RenderMs: Math.round(renderPercentiles[99] || 0),
    maxRenderMs: Math.round(metrics.maxRenderMs || 0),
    p50E2eMs: Math.round(e2ePercentiles[50] || 0),
    p95E2eMs: Math.round(e2ePercentiles[95] || 0),
    p99E2eMs: Math.round(e2ePercentiles[99] || 0),
    maxE2eMs: Math.round(metrics.maxE2eMs || 0),
    avgGapMs: Math.round(gapAvg),
    maxGapMs: Math.round(metrics.maxGapMs || 0),
    jitterGapMs: Math.round(gapJitter),
    queueLen: state.whiteboard.queue.length,
    maxQueueLen: metrics.maxQueueLen,
    bufferedAmount,
    maxBufferedAmount: metrics.maxBufferedAmount,
  });

  resetWhiteboardMetrics();
};

const scheduleWhiteboardResize = () => {
  if (state.whiteboard.resizeRafId) return;
  state.whiteboard.resizeRafId = requestAnimationFrame(() => {
    state.whiteboard.resizeRafId = null;
    syncWhiteboardCanvasSize();
  });
};

const syncWhiteboardCanvasSize = () => {
  if (!state.whiteboard.canvas || !state.whiteboard.ctx || !dom.sessionVideo) return;
  const rect = dom.sessionVideo.getBoundingClientRect();
  if (!rect.width || !rect.height) return;
  const dpr = window.devicePixelRatio || 1;
  const width = Math.round(rect.width * dpr);
  const height = Math.round(rect.height * dpr);
  if (state.whiteboard.lastSize.width === width && state.whiteboard.lastSize.height === height) return;
  state.whiteboard.canvas.width = width;
  state.whiteboard.canvas.height = height;
  state.whiteboard.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  state.whiteboard.lastSize = { width, height };
};

const initWhiteboardCanvas = () => {
  if (!dom.whiteboardCanvas || !dom.sessionVideo) return;
  state.whiteboard.canvas = dom.whiteboardCanvas;
  state.whiteboard.ctx = dom.whiteboardCanvas.getContext('2d');
  resetWhiteboardMetrics();
  syncWhiteboardCanvasSize();
  window.addEventListener('resize', scheduleWhiteboardResize);
  dom.sessionVideo.addEventListener('loadedmetadata', scheduleWhiteboardResize);
  dom.sessionVideo.addEventListener('resize', scheduleWhiteboardResize);
};

const mapPointToCanvas = (point, mapping) => {
  if (!mapping || typeof point.x !== 'number' || typeof point.y !== 'number') return null;
  const { drawW, drawH, offX, offY, frameW, frameH } = mapping;
  const useNormalized = point.x >= 0 && point.x <= 1 && point.y >= 0 && point.y <= 1;
  if (useNormalized) {
    return {
      x: offX + point.x * drawW,
      y: offY + point.y * drawH,
    };
  }
  const useFrameUnits =
    point.units === 'frame' ||
    point.unit === 'frame' ||
    point.frame === true ||
    (frameW > 1 && frameH > 1 && point.x <= frameW && point.y <= frameH);
  if (useFrameUnits && frameW > 0 && frameH > 0) {
    return {
      x: offX + (point.x / frameW) * drawW,
      y: offY + (point.y / frameH) * drawH,
    };
  }
  return { x: point.x, y: point.y };
};

const clearWhiteboardCanvas = () => {
  if (!state.whiteboard.ctx || !state.whiteboard.canvas) return;
  state.whiteboard.ctx.clearRect(0, 0, state.whiteboard.canvas.width, state.whiteboard.canvas.height);
  state.whiteboard.strokes.clear();
};

const drawWhiteboardPoint = (point, mapping, meta = {}) => {
  if (!state.whiteboard.ctx || !dom.sessionVideo) return;
  const resolvedMapping = mapping || getVideoContentRect(dom.sessionVideo);
  const coords = mapPointToCanvas(point, resolvedMapping);
  if (!coords) return;
  const strokeId = point.strokeId || point.stroke || 'default';
  const color = point.color || '#22c55e';
  const size = typeof point.size === 'number' ? point.size : 2;
  const action = point.action || point.phase || 'move';
  const ctx = state.whiteboard.ctx;
  const existing = state.whiteboard.strokes.get(strokeId) || null;

  if (action === 'clear') {
    clearWhiteboardCanvas();
    return;
  }

  ctx.lineCap = 'round';
  ctx.lineJoin = 'round';
  ctx.lineWidth = size;
  ctx.strokeStyle = color;

  if (action === 'start' || !existing) {
    state.whiteboard.strokes.set(strokeId, { x: coords.x, y: coords.y, color, size });
    ctx.beginPath();
    ctx.moveTo(coords.x, coords.y);
    ctx.lineTo(coords.x + 0.01, coords.y + 0.01);
    ctx.stroke();
  } else {
    ctx.beginPath();
    ctx.moveTo(existing.x, existing.y);
    ctx.lineTo(coords.x, coords.y);
    ctx.stroke();
    state.whiteboard.strokes.set(strokeId, { x: coords.x, y: coords.y, color, size });
  }

  if (action === 'end') {
    state.whiteboard.strokes.delete(strokeId);
  }

  if (WHITEBOARD_DEBUG && state.whiteboard.metrics) {
    const t1 = meta.receivedAt ?? performance.now();
    const t2 = performance.now();
    const t0 = meta.originTs;
    const renderMs = t2 - t1;
    state.whiteboard.metrics.drawnPoints += 1;
    state.whiteboard.metrics.totalRenderMs += renderMs;
    state.whiteboard.metrics.renderSamples.push(renderMs);
    state.whiteboard.metrics.maxRenderMs = Math.max(state.whiteboard.metrics.maxRenderMs, renderMs);
    if (typeof t0 === 'number') {
      const networkMs = t1 - t0;
      const e2eMs = t2 - t0;
      state.whiteboard.metrics.totalNetworkMs += networkMs;
      state.whiteboard.metrics.totalE2eMs += e2eMs;
      state.whiteboard.metrics.networkSamples.push(networkMs);
      state.whiteboard.metrics.e2eSamples.push(e2eMs);
      state.whiteboard.metrics.maxNetworkMs = Math.max(state.whiteboard.metrics.maxNetworkMs, networkMs);
      state.whiteboard.metrics.maxE2eMs = Math.max(state.whiteboard.metrics.maxE2eMs, e2eMs);
      state.whiteboard.metrics.latencySamples += 1;
    }
  }
};

const getWhiteboardPointAction = (point = {}) => point.action || point.phase || 'move';

const getWhiteboardStrokeId = (point = {}) => point.strokeId || point.stroke || 'default';

const coalesceWhiteboardQueue = () => {
  if (state.whiteboard.queue.length < WHITEBOARD_COALESCE_THRESHOLD) return;
  const seenMoves = new Set();
  const pruned = [];
  let dropped = 0;

  for (let i = state.whiteboard.queue.length - 1; i >= 0; i -= 1) {
    const entry = state.whiteboard.queue[i];
    const action = getWhiteboardPointAction(entry.point);
    if (action === 'move') {
      const strokeId = getWhiteboardStrokeId(entry.point);
      const key = String(strokeId);
      if (seenMoves.has(key)) {
        dropped += 1;
        continue;
      }
      seenMoves.add(key);
    }
    pruned.push(entry);
  }

  if (!dropped) return;
  pruned.reverse();
  state.whiteboard.queue = pruned;
  state.whiteboard.droppedPoints += dropped;
  if (WHITEBOARD_DEBUG && state.whiteboard.metrics) {
    state.whiteboard.metrics.droppedPoints += dropped;
  }
};

const enqueueWhiteboardPoints = (points, meta = {}) => {
  if (!points.length) return;
  const receivedAt = meta.receivedAt || performance.now();
  const byteSize = meta.byteSize || 0;

  if (WHITEBOARD_DEBUG && state.whiteboard.metrics) {
    state.whiteboard.metrics.receivedEvents += 1;
    state.whiteboard.metrics.receivedPoints += points.length;
    state.whiteboard.metrics.receivedBytes += byteSize;
    if (state.whiteboard.metrics.lastReceivedAt != null) {
      const gap = receivedAt - state.whiteboard.metrics.lastReceivedAt;
      state.whiteboard.metrics.gapSamples += 1;
      state.whiteboard.metrics.gapSum += gap;
      state.whiteboard.metrics.gapSumSquares += gap * gap;
      state.whiteboard.metrics.maxGapMs = Math.max(state.whiteboard.metrics.maxGapMs, gap);
    }
    state.whiteboard.metrics.lastReceivedAt = receivedAt;
  }

  for (const point of points) {
    const originTs = normalizeOriginTimestamp(point.originTs ?? point.t0 ?? meta.originTs ?? point.ts);
    state.whiteboard.queue.push({ point, meta: { receivedAt, originTs } });
  }

  if (state.whiteboard.queue.length > WHITEBOARD_MAX_QUEUE_SIZE) {
    const overflow = state.whiteboard.queue.length - WHITEBOARD_MAX_QUEUE_SIZE;
    state.whiteboard.queue.splice(0, overflow);
    state.whiteboard.droppedPoints += overflow;
    if (WHITEBOARD_DEBUG && state.whiteboard.metrics) {
      state.whiteboard.metrics.droppedPoints += overflow;
    }
    console.warn('[WB] Fila estourada, descartando pontos antigos', overflow);
  }

  if (WHITEBOARD_DEBUG && state.whiteboard.metrics) {
    state.whiteboard.metrics.maxQueueLen = Math.max(
      state.whiteboard.metrics.maxQueueLen,
      state.whiteboard.queue.length,
    );
  }

  coalesceWhiteboardQueue();
  scheduleWhiteboardRender();
};

const flushWhiteboardBuffer = () => {
  if (state.whiteboard.bufferTimer) {
    clearTimeout(state.whiteboard.bufferTimer);
    state.whiteboard.bufferTimer = null;
  }
  if (!state.whiteboard.buffer.length) return;
  const batches = state.whiteboard.buffer.splice(0, state.whiteboard.buffer.length);
  batches.forEach((batch) => {
    enqueueWhiteboardPoints(batch.points, batch.meta);
  });
};

const scheduleWhiteboardBufferFlush = () => {
  if (state.whiteboard.bufferTimer) return;
  state.whiteboard.bufferTimer = setTimeout(flushWhiteboardBuffer, WHITEBOARD_BATCH_INTERVAL_MS);
};

const processWhiteboardQueue = () => {
  state.whiteboard.rafId = null;
  if (!state.whiteboard.queue.length || !state.whiteboard.ctx || !dom.sessionVideo) return;
  const mapping = getVideoContentRect(dom.sessionVideo);
  const batch = state.whiteboard.queue.splice(0, WHITEBOARD_MAX_POINTS_PER_FRAME);

  for (const entry of batch) {
    drawWhiteboardPoint(entry.point, mapping, entry.meta);
  }

  reportWhiteboardMetrics();
  if (state.whiteboard.queue.length) {
    scheduleWhiteboardRender();
  }
};

const scheduleWhiteboardRender = () => {
  if (state.whiteboard.rafId) return;
  state.whiteboard.rafId = requestAnimationFrame(processWhiteboardQueue);
};

const isWhiteboardMessage = (message) => {
  if (!message || typeof message !== 'object') return false;
  const type = message.type || message.t || message.kind;
  if (type && WHITEBOARD_COMMAND_TYPES.has(String(type).toLowerCase())) return true;
  if (message.whiteboard === true) return true;
  return Boolean(message.points || message.batch || message.events);
};

const normalizeWhiteboardPayload = (payload = {}) => {
  if (!payload) return [];
  if (Array.isArray(payload)) return payload;
  const points = payload.points || payload.batch || payload.events || null;
  if (Array.isArray(points)) return points;
  if (typeof payload.x === 'number' && typeof payload.y === 'number') return [payload];
  return [];
};

const ingestWhiteboardPayload = (payload = {}) => {
  if (!payload) return;
  const receivedAt = performance.now();
  const byteSize = estimatePayloadBytes(payload);
  const baseOrigin = normalizeOriginTimestamp(payload.originTs ?? payload.t0 ?? payload.ts);
  const points = normalizeWhiteboardPayload(payload).map((point) => ({
    ...point,
    originTs: normalizeOriginTimestamp(point.originTs ?? point.t0 ?? baseOrigin),
  }));
  state.whiteboard.buffer.push({ points, meta: { receivedAt, originTs: baseOrigin, byteSize } });
  scheduleWhiteboardBufferFlush();
};

function handleCtrlChannelMessage(event) {
  if (!event?.data) return;
  if (typeof event.data !== 'string') return;
  try {
    const message = JSON.parse(event.data);
    if (isWhiteboardMessage(message)) {
      ingestWhiteboardPayload(message.payload ?? message.data ?? message);
    }
  } catch (error) {
    console.warn('Falha ao processar mensagem do canal de controle', error);
  }
}

const updateFullscreenLabel = () => {
  if (!dom.controlFullscreen) return;
  dom.controlFullscreen.textContent = document.fullscreenElement ? 'Sair tela cheia' : 'Tela cheia';
};

const updatePipLabel = () => {
  if (!dom.controlPip) return;
  dom.controlPip.textContent = document.pictureInPictureElement ? 'Fechar janela' : 'Janela flutuante';
};

const setControlMenuOpen = (isOpen) => {
  if (!dom.videoShell || !dom.controlMenuToggle || !dom.controlMenuPanel) return;
  dom.videoShell.classList.toggle('control-menu-open', isOpen);
  dom.controlMenuToggle.setAttribute('aria-expanded', String(isOpen));
  dom.controlMenuPanel.setAttribute('aria-hidden', String(!isOpen));
  if (dom.controlMenuBackdrop) {
    dom.controlMenuBackdrop.hidden = !isOpen;
  }
};

const toggleControlMenu = () => {
  if (!dom.videoShell) return;
  const isOpen = dom.videoShell.classList.contains('control-menu-open');
  setControlMenuOpen(!isOpen);
};

const clearQueueRetryTimer = () => {
  if (queueRetryTimer) {
    clearTimeout(queueRetryTimer);
    queueRetryTimer = null;
  }
};

const resetQueueRetryTimer = () => {
  clearQueueRetryTimer();
  queueRetryDelayMs = QUEUE_RETRY_INITIAL_DELAY_MS;
};

const resetQueueRetryState = () => {
  resetQueueRetryTimer();
  queueUnavailable = false;
  if (dom.queueRetry) {
    dom.queueRetry.hidden = true;
  }
  hideToast();
};

const scheduleQueueRetry = (statusText = '') => {
  clearQueueRetryTimer();
  const delay = queueRetryDelayMs;
  queueRetryTimer = window.setTimeout(() => {
    queueRetryTimer = null;
    loadQueue();
  }, delay);
  const seconds = Math.round(delay / 1000);
  const context = statusText ? ` (${statusText})` : '';
  console.warn(`[queue] Fila indisponível${context}. Nova tentativa em ${seconds}s.`);
  queueRetryDelayMs = Math.min(queueRetryDelayMs * 2, QUEUE_RETRY_MAX_DELAY_MS);
};

const startQueueAutoRefresh = () => {
  if (queueAutoRefreshIntervalId) return;
  queueAutoRefreshIntervalId = trackInterval(
    window.setInterval(() => {
      Promise.all([loadQueue(), loadSessions(), loadMetrics()]).catch((error) => {
        console.warn('[dashboard] auto-refresh failed', error?.message || error);
      });
    }, QUEUE_AUTO_REFRESH_INTERVAL_MS)
  );
};

const isFirestorePermissionError = (error) => {
  const code = ensureString(error?.code || '').toLowerCase();
  const message = ensureString(error?.message || '').toLowerCase();
  return code.includes('permission-denied') || message.includes('insufficient permissions');
};

const updateQueueMetrics = (size) => {
  if (!state.metrics) return;
  state.metrics = {
    ...state.metrics,
    queueSize: typeof size === 'number' ? size : null,
    lastUpdated: Date.now(),
  };
  renderMetrics();
};

const markQueueUnavailable = ({ statusText = '' } = {}) => {
  if (!queueUnavailable) {
    queueUnavailable = true;
    queueRetryDelayMs = QUEUE_RETRY_INITIAL_DELAY_MS;
  }
  if (dom.queueRetry) {
    dom.queueRetry.hidden = false;
  }
  const hasCachedQueue = Array.isArray(state.queue) && state.queue.length > 0;
  showToast(
    hasCachedQueue
      ? 'Fila indisponível no momento. Exibindo último estado conhecido.'
      : 'Fila indisponível. Tente novamente.'
  );
  renderQueue();
  updateQueueMetrics(hasCachedQueue ? state.queue.length : 0);
  scheduleQueueRetry(statusText);
};

const isTemporaryQueueFailureStatus = (status) => TEMPORARY_QUEUE_ERROR_STATUS.has(Number(status));

const normalizeIdentifier = (value) => {
  if (typeof value === 'string' && value.trim()) return value.trim().toLowerCase();
  return null;
};

const updateTechIdentifiers = (tech) => {
  const identifiers = new Set();
  if (tech) {
    const add = (value) => {
      if (value == null) return;
      const normalized = normalizeIdentifier(typeof value === 'string' ? value : String(value));
      if (normalized) identifiers.add(normalized);
    };
    add(tech.uid);
    add(tech.id);
    add(tech.email);
    add(tech.name);
  }
  state.techIdentifiers = identifiers;
  return identifiers;
};

const syncAuthToTechProfile = (authUser) => {
  if (!authUser || !authUser.uid) return;
  updateTechDataset({ techUid: authUser.uid, uid: authUser.uid });
  const profile = getTechProfile();
  if (profile.uid !== authUser.uid) {
    state.techProfile = {
      ...profile,
      uid: authUser.uid,
      id: profile.id || authUser.uid,
    };
    updateTechIdentifiers(state.techProfile);
  }
  updateTechIdentity();
};

const getTechProfile = () => {
  const dataset = dom.techIdentity?.dataset || {};
  const candidates = [
    typeof window !== 'undefined' ? window.__CENTRAL_TECH__ : null,
    typeof window !== 'undefined' ? window.__TECH__ : null,
    typeof window !== 'undefined' ? window.centralTech : null,
    typeof window !== 'undefined' ? window.__CENTRAL_CONTEXT__?.tech : null,
  ];
  const context = candidates.find((candidate) => candidate && typeof candidate === 'object') || {};
  const previous = state.techProfile || {};
  const resolvedUid =
    context.uid ||
    context.techUid ||
    context.id ||
    dataset.techUid ||
    dataset.techId ||
    dataset.uid ||
    previous.uid ||
    previous.id ||
    null;
  const resolvedId =
    context.id ||
    context.techId ||
    dataset.techId ||
    dataset.techUid ||
    previous.id ||
    previous.uid ||
    resolvedUid ||
    null;
  const resolvedEmail =
    context.email ||
    context.techEmail ||
    dataset.techEmail ||
    dataset.email ||
    previous.email ||
    null;
  const resolvedName =
    previous.name ||
    dataset.techName ||
    dataset.name ||
    context.name ||
    context.techName ||
    dom.topbarTechName?.textContent?.trim() ||
    'Técnico';
  const tech = {
    ...previous,
    ...context,
    uid: resolvedUid,
    id: resolvedId,
    name: resolvedName,
    email: resolvedEmail,
  };
  state.techProfile = tech;
  updateTechIdentifiers(tech);
  if (dom.techIdentity) {
    if (tech.uid) dom.techIdentity.dataset.techUid = tech.uid;
    else delete dom.techIdentity.dataset.techUid;
    if (tech.id) dom.techIdentity.dataset.techId = tech.id;
    else delete dom.techIdentity.dataset.techId;
    if (tech.name) dom.techIdentity.dataset.techName = tech.name;
    else delete dom.techIdentity.dataset.techName;
    if (tech.email) dom.techIdentity.dataset.techEmail = tech.email;
    else delete dom.techIdentity.dataset.techEmail;
  }
  return state.techProfile;
};

const ensureTechIdentifiers = () => {
  if (state.techIdentifiers instanceof Set && state.techIdentifiers.size) {
    return state.techIdentifiers;
  }
  const profile = state.techProfile || getTechProfile();
  return updateTechIdentifiers(profile);
};

const extractSessionIdentifiers = (session) => {
  if (!session || typeof session !== 'object') return [];
  const identifiers = [];
  const push = (value) => {
    if (value != null) identifiers.push(value);
  };
  push(session.techUid);
  push(session.techId);
  push(session.techEmail);
  push(session.techName);
  const extra = session.extra || {};
  if (extra) {
    push(extra.techUid);
    push(extra.techId);
    push(extra.techEmail);
    push(extra.techName);
    if (extra.tech && typeof extra.tech === 'object') {
      push(extra.tech.uid);
      push(extra.tech.id);
      push(extra.tech.email);
      push(extra.tech.name);
    }
  }
  return identifiers;
};

const sessionMatchesCurrentTech = (session) => {
  const identifiers = ensureTechIdentifiers();
  if (!(identifiers instanceof Set) || identifiers.size === 0) {
    return true;
  }
  const candidates = extractSessionIdentifiers(session)
    .map((value) => normalizeIdentifier(String(value)))
    .filter(Boolean);
  if (!candidates.length) return false;
  return candidates.some((candidate) => identifiers.has(candidate));
};

const filterSessionsForCurrentTech = (sessions) => {
  if (!Array.isArray(sessions)) return [];
  const identifiers = ensureTechIdentifiers();
  if (!(identifiers instanceof Set) || identifiers.size === 0) {
    return sessions;
  }
  return sessions.filter((session) => sessionMatchesCurrentTech(session));
};

const pickSessionQueryConstraint = (tech) => {
  if (!tech || typeof tech !== 'object') return null;
  const attempts = [
    ['tech.techUid', tech.uid],
    ['techUid', tech.uid],
    ['techId', tech.id],
    ['techEmail', tech.email],
    ['tech.uid', tech.uid],
    ['tech.id', tech.id],
    ['tech.email', tech.email],
  ];
  for (const [field, value] of attempts) {
    if (typeof value === 'string' && value.trim()) {
      return { field, value };
    }
  }
  return null;
};

const SOCKET_URL = window.location.origin;
let socket = null;
let socketAuthRecoveryInFlight = false;
let socketInvalidTokenCounter = 0;
let socketRealtimeDisabled = false;
let socketRealtimeDisabledNotified = false;
let sessionJoinInFlightId = null;

const buildSocketAuthPayload = async ({ forceRefresh = false } = {}) => {
  const token = await getIdToken(forceRefresh);
  return { token, panel: 'tech', requireAuth: true };
};

const connectSocketWithToken = async (authUser) => {
  if (!window.io || !authUser) return null;
  const authPayload = await buildSocketAuthPayload({ forceRefresh: true });
  if (socket) {
    socket.auth = authPayload;
    if (socket.disconnected) socket.connect();
    return socket;
  }

  socket = window.io(SOCKET_URL, {
    transports: ['websocket', 'polling'],
    upgrade: true,
    withCredentials: true,
    reconnection: true,
    reconnectionAttempts: Infinity,
    reconnectionDelay: 500,
    reconnectionDelayMax: 5000,
    randomizationFactor: 0.5,
    timeout: 20000,
    auth: (cb) => {
      buildSocketAuthPayload()
        .then((payload) => cb(payload))
        .catch(() => cb({ panel: 'tech', requireAuth: true }));
    },
  });
  setupSocketHandlers();
  return socket;
};
let socketUpgradeLogsRegistered = false;

const CHAT_RENDER_LIMIT = 100;
const TIMELINE_RENDER_LIMIT = 80;

const pendingRenderJobs = [];
let pendingRafId = null;

function setSessionState(nextState, sessionId = null) {
  if (!Object.values(SessionStates).includes(nextState)) return;
  const changed = state.sessionState !== nextState || state.activeSessionId !== sessionId;
  state.sessionState = nextState;
  state.activeSessionId = sessionId;
  if (changed && document && document.body) {
    document.body.dataset.sessionState = nextState;
  }
}

function isSessionCurrent(sessionId) {
  if (!sessionId) return false;
  return (
    state.activeSessionId === sessionId ||
    state.joinedSessionId === sessionId ||
    state.selectedSessionId === sessionId
  );
}

function markSessionActive(sessionId) {
  if (!sessionId) return;
  setSessionState(SessionStates.ACTIVE, sessionId);
}

function markSessionEnded(sessionId, reason = 'peer_ended') {
  if (!sessionId) return;
  if (state.sessionState === SessionStates.IDLE) return;
  if (!isSessionCurrent(sessionId)) return;
  setSessionState(SessionStates.ENDED, sessionId);
  resetDashboard({ sessionId, reason });
}

function scheduleRender(fn) {
  if (typeof fn !== 'function') return;
  pendingRenderJobs.push(fn);
  if (pendingRafId) return;
  pendingRafId = requestAnimationFrame(() => {
    const jobs = pendingRenderJobs.splice(0, pendingRenderJobs.length);
    pendingRafId = null;
    for (const job of jobs) {
      try {
        job();
      } catch (error) {
        console.error('Render job failed', error);
      }
    }
  });
}

function cancelScheduledRenders() {
  if (pendingRafId) {
    cancelAnimationFrame(pendingRafId);
    pendingRafId = null;
  }
  pendingRenderJobs.length = 0;
}

const sessionResources = {
  timeouts: new Set(),
  intervals: new Set(),
  observers: new Set(),
  socketHandlers: new Map(),
};

function trackTimeout(id) {
  if (typeof id === 'number') sessionResources.timeouts.add(id);
  return id;
}

function trackInterval(id) {
  if (typeof id === 'number') sessionResources.intervals.add(id);
  return id;
}

function trackObserver(observer) {
  if (observer && typeof observer.disconnect === 'function') {
    sessionResources.observers.add(observer);
  }
  return observer;
}

function registerSocketHandler(eventName, handler) {
  if (!socket || typeof eventName !== 'string' || typeof handler !== 'function') return;
  const existing = sessionResources.socketHandlers.get(eventName);
  if (existing) {
    socket.off(eventName, existing);
  }
  sessionResources.socketHandlers.set(eventName, handler);
  socket.on(eventName, handler);
}

const toMillis = (value) => {
  if (!value && value !== 0) return null;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (value instanceof Date) return value.getTime();
  if (typeof value === 'string') {
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? null : parsed;
  }
  if (typeof value === 'object' && value !== null) {
    if (typeof value.toMillis === 'function') {
      return value.toMillis();
    }
    if (typeof value.toDate === 'function') {
      const date = value.toDate();
      return date instanceof Date ? date.getTime() : null;
    }
    if (typeof value.seconds === 'number') {
      const nanos = typeof value.nanoseconds === 'number' ? value.nanoseconds : 0;
      return value.seconds * 1000 + Math.floor(nanos / 1e6);
    }
  }
  return null;
};

const describeTimelineEvent = (event) => {
  if (!event || typeof event !== 'object') return null;
  const directText = event.text || event.description || event.label || event.message || event.title;
  if (typeof directText === 'string' && directText.trim()) return directText.trim();
  const type =
    typeof event.type === 'string'
      ? event.type
      : typeof event.eventType === 'string'
        ? event.eventType
        : typeof event.kind === 'string'
          ? event.kind
          : typeof event.name === 'string'
            ? event.name
            : null;
  if (!type) return null;
  const normalized = type.toLowerCase();
  const dictionary = {
    queue_entered: 'Cliente entrou na fila',
    request_created: 'Cliente entrou na fila',
    session_accepted: 'Atendimento aceito pelo técnico',
    session_closed: 'Atendimento encerrado',
    share_start: 'Compartilhamento de tela iniciado',
    share_stop: 'Compartilhamento de tela encerrado',
    remote_start: 'Acesso remoto iniciado',
    remote_stop: 'Acesso remoto encerrado',
    call_start: 'Chamada iniciada',
    call_stop: 'Chamada encerrada',
  };
  if (dictionary[normalized]) return dictionary[normalized];
  return normalized.replace(/[_-]+/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
};

const normalizeSessionDoc = (doc) => {
  if (!doc) return null;
  const data = typeof doc.data === 'function' ? doc.data() : {};
  const sessionId = data.sessionId || doc.id;
  const requestedAt = toMillis(data.requestedAt || data.timestamps?.requestedAt || data.createdAt);
  const acceptedAt = toMillis(data.acceptedAt || data.timestamps?.acceptedAt || data.startedAt);
  const closedAt = toMillis(data.closedAt || data.timestamps?.closedAt || data.finishedAt);
  const waitTimeMsRaw = typeof data.waitTimeMs === 'number' ? data.waitTimeMs : null;
  const handleTimeMsRaw = typeof data.handleTimeMs === 'number' ? data.handleTimeMs : null;
  const waitTimeMs =
    waitTimeMsRaw != null ? waitTimeMsRaw : acceptedAt && requestedAt ? acceptedAt - requestedAt : null;
  const handleTimeMs =
    handleTimeMsRaw != null ? handleTimeMsRaw : closedAt && acceptedAt ? closedAt - acceptedAt : null;
  const extra = typeof data.extra === 'object' && data.extra !== null ? { ...data.extra } : {};
  const telemetry = normalizeTelemetryPayload(
    typeof data.telemetry === 'object' && data.telemetry !== null ? { ...data.telemetry } : { ...extra.telemetry }
  );
  const chatLog = Array.isArray(data.chatLog)
    ? data.chatLog
    : Array.isArray(extra.chatLog)
      ? extra.chatLog
      : [];
  const timeline = Array.isArray(extra.timeline) ? extra.timeline.map((item) => ({ ...item })) : [];
  const tech = getTechProfile();
  return {
    sessionId,
    requestId: data.requestId || data.request?.id || sessionId,
    techName: data.techName || data.tech?.name || tech.name,
    techId: data.tech?.id || data.techId || tech.id || tech.uid || null,
    techUid: data.tech?.uid || data.techUid || tech.uid || tech.id || null,
    techEmail: data.tech?.email || data.techEmail || tech.email || null,
    clientRecordId: data.clientRecordId || data.client?.id || null,
    clientPhone: data.clientPhone || data.client?.phone || null,
    clientName: data.clientName || data.client?.name || data.client?.displayName || 'Cliente',
    brand: data.brand || data.device?.brand || data.client?.device?.brand || null,
    model: data.model || data.device?.model || data.client?.device?.model || null,
    osVersion: data.osVersion || data.device?.osVersion || data.client?.device?.osVersion || null,
    plan: data.plan || data.client?.plan || data.context?.plan || null,
    issue: data.issue || data.client?.issue || data.context?.issue || null,
    requestedAt: requestedAt || null,
    acceptedAt: acceptedAt || null,
    waitTimeMs: waitTimeMs != null ? waitTimeMs : null,
    status: data.status || (closedAt ? 'closed' : 'active'),
    closedAt: closedAt || null,
    handleTimeMs: handleTimeMs != null ? handleTimeMs : null,
    firstContactResolution:
      typeof data.firstContactResolution === 'boolean'
        ? data.firstContactResolution
        : typeof data.outcome?.firstContactResolution === 'boolean'
          ? data.outcome.firstContactResolution
          : null,
    npsScore:
      typeof data.npsScore === 'number'
        ? data.npsScore
        : typeof data.outcome?.npsScore === 'number'
          ? data.outcome.npsScore
          : null,
    outcome: data.outcome || null,
    symptom: data.symptom || null,
    solution: data.solution || null,
    requiresTechnicianRegistration: Boolean(data.requiresTechnicianRegistration),
    supportSessionId: data.supportSessionId || data.localSupportSessionId || null,
    chatLog,
    telemetry,
    extra: { ...extra, chatLog, timeline },
  };
};

const normalizeMessageDoc = (doc) => {
  if (!doc) return null;
  const data = typeof doc.data === 'function' ? doc.data() : {};
  const typeRaw = data.type || (data.audioUrl ? 'audio' : data.imageUrl ? 'image' : data.fileUrl ? 'file' : 'text');
  const type = typeof typeRaw === 'string' ? typeRaw.trim().toLowerCase() : 'text';
  const textRaw = data.text || data.body || data.message || '';
  const text = typeof textRaw === 'string' ? textRaw.trim() : '';
  const audioUrl = typeof data.audioUrl === 'string' ? data.audioUrl.trim() : '';
  const imageUrl = typeof data.imageUrl === 'string' ? data.imageUrl.trim() : '';
  const fileUrl = typeof data.fileUrl === 'string' ? data.fileUrl.trim() : '';
  const fileName = typeof data.fileName === 'string' ? data.fileName.trim() : '';
  const contentTypeRaw = data.contentType || data.mimeType || '';
  const mimeType = typeof contentTypeRaw === 'string' ? contentTypeRaw.trim() : '';
  const fileSizeRaw = data.size ?? data.fileSize;
  const fileSize = typeof fileSizeRaw === 'number' && Number.isFinite(fileSizeRaw) ? fileSizeRaw : null;
  const hasRenderableContent =
    Boolean(text || audioUrl || imageUrl || fileUrl) ||
    (type === 'image' && !imageUrl) ||
    (type === 'audio' && !audioUrl) ||
    (type === 'file' && !fileUrl);
  if (!hasRenderableContent) return null;
  const ts =
    toMillis(data.ts) ||
    toMillis(data.timestamp) ||
    toMillis(data.createdAt) ||
    toMillis(data.sentAt) ||
    Date.now();
  const fromRaw = data.from || data.author || data.sender || 'client';
  const from = typeof fromRaw === 'string' ? fromRaw : 'client';
  return {
    id: data.id || doc.id,
    from,
    type,
    text,
    audioUrl,
    imageUrl,
    fileUrl,
    fileName,
    mimeType,
    fileSize,
    status: typeof data.status === 'string' ? data.status : '',
    ts,
  };
};

const normalizeChatMessage = (message, { defaultFrom = 'client' } = {}) => {
  if (!message) return null;
  const ts = typeof message.ts === 'number' ? message.ts : Date.now();
  const id =
    message.id ||
    message.messageId ||
    message.clientMessageId ||
    message.msgId ||
    `${message.sessionId || 'session'}-${ts}`;
  const from = message.from || defaultFrom;
  const typeRaw =
    message.type || (message.audioUrl ? 'audio' : message.imageUrl ? 'image' : message.fileUrl ? 'file' : 'text');
  const type = typeof typeRaw === 'string' ? typeRaw.trim().toLowerCase() : 'text';
  const textRaw = message.text || message.body || message.message || '';
  const text = typeof textRaw === 'string' ? textRaw.trim() : '';
  const audioUrl = typeof message.audioUrl === 'string' ? message.audioUrl.trim() : '';
  const imageUrl = typeof message.imageUrl === 'string' ? message.imageUrl.trim() : '';
  const fileUrl = typeof message.fileUrl === 'string' ? message.fileUrl.trim() : '';
  const fileName = typeof message.fileName === 'string' ? message.fileName.trim() : '';
  const contentTypeRaw = message.contentType || message.mimeType || '';
  const mimeType = typeof contentTypeRaw === 'string' ? contentTypeRaw.trim() : '';
  const fileSizeRaw = message.size ?? message.fileSize;
  const fileSize = typeof fileSizeRaw === 'number' && Number.isFinite(fileSizeRaw) ? fileSizeRaw : null;
  const hasRenderableContent =
    Boolean(text || audioUrl || imageUrl || fileUrl) ||
    (type === 'image' && !imageUrl) ||
    (type === 'audio' && !audioUrl) ||
    (type === 'file' && !fileUrl);
  if (!hasRenderableContent) return null;
  return {
    ...message,
    id,
    from,
    type,
    text,
    audioUrl,
    imageUrl,
    fileUrl,
    fileName,
    mimeType,
    fileSize,
    status: typeof message.status === 'string' ? message.status : '',
    ts,
  };
};

const normalizeEventDoc = (doc) => {
  if (!doc) return null;
  const data = typeof doc.data === 'function' ? doc.data() : {};
  const at =
    toMillis(data.at) ||
    toMillis(data.timestamp) ||
    toMillis(data.ts) ||
    toMillis(data.createdAt) ||
    toMillis(data.updatedAt) ||
    toMillis(doc?.createTime) ||
    toMillis(doc?.updateTime) ||
    null;
  const telemetryPayload = {};
  if (typeof data.shareActive === 'boolean') telemetryPayload.shareActive = data.shareActive;
  if (typeof data.remoteActive === 'boolean') telemetryPayload.remoteActive = data.remoteActive;
  if (typeof data.callActive === 'boolean') telemetryPayload.callActive = data.callActive;
  if (typeof data.network !== 'undefined') telemetryPayload.network = data.network;
  if (typeof data.net !== 'undefined' && typeof telemetryPayload.network === 'undefined') telemetryPayload.network = data.net;
  if (typeof data.health !== 'undefined') telemetryPayload.health = data.health;
  if (typeof data.permissions !== 'undefined') telemetryPayload.permissions = data.permissions;
  if (typeof data.alerts !== 'undefined') telemetryPayload.alerts = data.alerts;
  if (typeof data.batteryLevel !== 'undefined') telemetryPayload.batteryLevel = data.batteryLevel;
  if (typeof data.battery !== 'undefined' && typeof telemetryPayload.batteryLevel === 'undefined') {
    telemetryPayload.batteryLevel = data.battery;
  }
  if (typeof data.batteryCharging !== 'undefined') telemetryPayload.batteryCharging = data.batteryCharging;
  if (typeof data.temperatureC !== 'undefined') telemetryPayload.temperatureC = data.temperatureC;
  if (typeof data.storageFreeBytes !== 'undefined') telemetryPayload.storageFreeBytes = data.storageFreeBytes;
  if (typeof data.storageTotalBytes !== 'undefined') telemetryPayload.storageTotalBytes = data.storageTotalBytes;
  if (typeof data.deviceImageUrl !== 'undefined') telemetryPayload.deviceImageUrl = data.deviceImageUrl;
  if (typeof data.telemetry === 'object' && data.telemetry !== null) {
    Object.assign(telemetryPayload, data.telemetry);
  }
  const normalizedTelemetry = normalizeTelemetryPayload(telemetryPayload);
  return {
    id: doc.id,
    at,
    text: describeTimelineEvent(data),
    telemetry: normalizedTelemetry,
  };
};

const handleEventsSnapshot = (sessionId, snapshot) => {
  const events = snapshot.docs.map((docSnap) => normalizeEventDoc(docSnap)).filter(Boolean);
  const timeline = events
    .map((evt) => ({
      at: evt.at || Date.now(),
      text: evt.text || 'Atualização registrada',
    }))
    .sort((a, b) => (a.at || 0) - (b.at || 0))
    .slice(-TIMELINE_RENDER_LIMIT);
  const telemetryUpdates = events.reduce((acc, evt) => {
    if (evt.telemetry && Object.keys(evt.telemetry).length) {
      Object.assign(acc, evt.telemetry);
    }
    return acc;
  }, {});
  if (!timeline.length && !Object.keys(telemetryUpdates).length) return;
  const current = normalizeTelemetryPayload(state.telemetryBySession.get(sessionId) || {});
  const merged = normalizeTelemetryPayload({ ...current });
  if (Object.keys(telemetryUpdates).length) {
    Object.assign(merged, telemetryUpdates, { updatedAt: Date.now() });
  }
  if (timeline.length) {
    merged.timeline = timeline;
  }
  state.telemetryBySession.set(sessionId, merged);
  const index = state.sessions.findIndex((s) => s.sessionId === sessionId);
  if (index >= 0) {
    const session = state.sessions[index];
    const extra = { ...(session.extra || {}) };
    if (timeline.length) extra.timeline = timeline;
    if (Object.keys(telemetryUpdates).length) {
      const mergedExtraTelemetry = normalizeTelemetryPayload({ ...(extra.telemetry || {}), ...telemetryUpdates });
      extra.telemetry = mergedExtraTelemetry;
      if (typeof mergedExtraTelemetry.network !== 'undefined') extra.network = mergedExtraTelemetry.network;
      if (typeof mergedExtraTelemetry.health !== 'undefined') extra.health = mergedExtraTelemetry.health;
      if (typeof mergedExtraTelemetry.permissions !== 'undefined') extra.permissions = mergedExtraTelemetry.permissions;
      if (typeof mergedExtraTelemetry.alerts !== 'undefined') extra.alerts = mergedExtraTelemetry.alerts;
    }
    state.sessions[index] = {
      ...session,
      extra,
      telemetry: normalizeTelemetryPayload({ ...(session.telemetry || {}), ...merged }),
    };
  }
  if (state.selectedSessionId === sessionId) {
    renderSessions();
  }
};

const subscribeToSessionRealtime = async (sessionId) => {
  if (!sessionId) return;
  try {
    const user = await ensureAuth();
    if (!user) {
      console.warn('Auth indisponível. Listener da sessão não será iniciado.', sessionId);
      return;
    }
  } catch (error) {
    console.error('Falha ao autenticar antes de escutar sessão', sessionId, error);
    return;
  }
  const db = ensureFirestore();
  if (!db) return;
  if (sessionRealtimeSubscriptions.has(sessionId)) return;
  const sessionRef = doc(db, 'sessions', sessionId);
  const messagesRef = collection(sessionRef, 'messages');
  const eventsRef = collection(sessionRef, 'events');
  let unsubMessages = null;
  let unsubEvents = null;
  let unsubCall = null;
  try {
    const messagesQuery = query(messagesRef, orderBy('ts', 'asc'), limit(CHAT_RENDER_LIMIT * 2));
    unsubMessages = onSnapshot(
      messagesQuery,
      (snapshot) => {
        const dedupedMessages = new Map();
        snapshot.docs.forEach((docSnap) => {
          const msg = normalizeMessageDoc(docSnap);
          if (!msg) return;
          debugChatLog('[chat] message received', { source: 'firestore', sessionId, raw: docSnap.data(), message: msg });
          dedupedMessages.set(msg.id, msg);
        });
        const messages = Array.from(dedupedMessages.values()).sort((a, b) => a.ts - b.ts);
        state.chatBySession.set(sessionId, messages);
        const lastMessage = messages.length ? messages[messages.length - 1] : null;
        const index = state.sessions.findIndex((s) => s.sessionId === sessionId);
        if (index >= 0) {
          const session = state.sessions[index];
          const extra = { ...(session.extra || {}), chatLog: messages };
          if (lastMessage) extra.lastMessageAt = lastMessage.ts;
          state.sessions[index] = { ...session, chatLog: messages, extra };
        }
        if (state.renderedChatSessionId === sessionId) {
          state.renderedChatSessionId = null;
          renderChatForSession();
        } else if (state.selectedSessionId === sessionId) {
          renderChatForSession();
        }
      },
      (error) => {
        console.error('Falha ao escutar mensagens da sessão', sessionId, error);
        if (isFirestorePermissionError(error)) {
          unsubscribeSessionRealtime(sessionId);
        }
      }
    );
  } catch (error) {
    console.error('Falha ao iniciar listener de mensagens da sessão', sessionId, error);
  }
  try {
    const eventsQuery = query(eventsRef, orderBy('ts', 'asc'), limit(TIMELINE_RENDER_LIMIT * 2));
    unsubEvents = onSnapshot(
      eventsQuery,
      (snapshot) => handleEventsSnapshot(sessionId, snapshot),
      (error) => {
        console.error('Falha ao escutar eventos da sessão', sessionId, error);
        if (isFirestorePermissionError(error)) {
          unsubscribeSessionRealtime(sessionId);
        }
      }
    );
  } catch (error) {
    console.error('Falha ao iniciar listener de eventos da sessão', sessionId, error);
  }
  try {
    unsubCall = onSnapshot(
      doc(sessionRef, 'call', 'active'),
      (snapshot) => {
        void handleCallSnapshot(sessionId, snapshot);
      },
      (error) => {
        console.error('Falha ao escutar chamada da sessão', sessionId, error);
        if (isFirestorePermissionError(error)) {
          unsubscribeSessionRealtime(sessionId);
        }
      }
    );
  } catch (error) {
    console.error('Falha ao iniciar listener de chamada da sessão', sessionId, error);
  }
  sessionRealtimeSubscriptions.set(sessionId, { messages: unsubMessages, events: unsubEvents, call: unsubCall });
};

const updateSessionRealtimeSubscriptions = (sessions) => {
  const activeIds = new Set(
    (sessions || [])
      .filter((s) => s?.sessionId && s.status === 'active' && sessionMatchesCurrentTech(s))
      .map((s) => s.sessionId)
  );
  sessionRealtimeSubscriptions.forEach((_value, sessionId) => {
    if (!activeIds.has(sessionId)) {
      unsubscribeSessionRealtime(sessionId);
    }
  });
  activeIds.forEach((sessionId) => {
    if (!sessionRealtimeSubscriptions.has(sessionId)) {
      subscribeToSessionRealtime(sessionId);
    }
  });
};

const updateMetricsFromSessions = (sessions) => {
  if (!Array.isArray(sessions)) return;
  const relevantSessions = filterSessionsForCurrentTech(sessions);
  const now = new Date();
  const startOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
  const todaysSessions = relevantSessions.filter((session) => {
    const basis = session.acceptedAt || session.requestedAt || session.closedAt || 0;
    return basis >= startOfDay;
  });
  const closedToday = todaysSessions.filter((session) => session.status === 'closed');
  const waitTimes = todaysSessions
    .map((session) => session.waitTimeMs)
    .filter((ms) => typeof ms === 'number' && ms >= 0);
  const averageWaitMs = waitTimes.length ? waitTimes.reduce((a, b) => a + b, 0) / waitTimes.length : null;
  const handleTimes = closedToday
    .map((session) => session.handleTimeMs)
    .filter((ms) => typeof ms === 'number' && ms >= 0);
  const averageHandleMs = handleTimes.length ? handleTimes.reduce((a, b) => a + b, 0) / handleTimes.length : null;
  const fcrValues = closedToday
    .filter((session) => typeof session.firstContactResolution === 'boolean')
    .map((session) => (session.firstContactResolution ? 1 : 0));
  const fcrPercentage = fcrValues.length
    ? Math.round((fcrValues.reduce((a, b) => a + b, 0) / fcrValues.length) * 100)
    : null;
  const npsScores = closedToday
    .map((session) => (typeof session.npsScore === 'number' ? session.npsScore : null))
    .filter((score) => score !== null && !Number.isNaN(score));
  let nps = null;
  if (npsScores.length) {
    const promoters = npsScores.filter((score) => score >= 9).length;
    const detractors = npsScores.filter((score) => score <= 6).length;
    nps = Math.round(((promoters - detractors) / npsScores.length) * 100);
  }
  const metrics = {
    attendancesToday: todaysSessions.length,
    activeSessions: sessions.filter((session) => session.status === 'active').length,
    averageWaitMs,
    averageHandleMs,
    fcrPercentage,
    nps,
    queueSize: Array.isArray(state.queue) ? state.queue.length : null,
    lastUpdated: Date.now(),
  };
  state.metrics = metrics;
  renderMetrics();
};

const ensureChatStore = (sessionId) => {
  if (!sessionId) return [];
  if (!state.chatBySession.has(sessionId)) {
    state.chatBySession.set(sessionId, []);
  }
  return state.chatBySession.get(sessionId);
};

const syncSessionStores = (session) => {
  if (!session || !session.sessionId) return;
  const { sessionId } = session;
  const chatLog = Array.isArray(session.chatLog)
    ? session.chatLog
    : Array.isArray(session.extra?.chatLog)
      ? session.extra.chatLog
      : [];
  if (chatLog.length) {
    const normalized = chatLog
      .map((entry) => normalizeChatMessage({ ...entry, sessionId }, { defaultFrom: 'client' }))
      .filter(Boolean)
      .sort((a, b) => a.ts - b.ts);
    state.chatBySession.set(sessionId, normalized);
  } else {
    state.chatBySession.set(sessionId, []);
  }

  const telemetrySource =
    (typeof session.telemetry === 'object' && session.telemetry !== null && session.telemetry) ||
    (typeof session.extra?.telemetry === 'object' && session.extra.telemetry !== null ? session.extra.telemetry : null);
  if (telemetrySource) {
    state.telemetryBySession.set(sessionId, normalizeTelemetryPayload({ ...telemetrySource }));
  } else {
    state.telemetryBySession.delete(sessionId);
  }
};

const pushChatToStore = (sessionId, message) => {
  if (!sessionId || !message) return;
  const bucket = ensureChatStore(sessionId);
  const duplicateIndex = bucket.findIndex((entry) => entry.id === message.id);
  if (duplicateIndex >= 0) {
    bucket[duplicateIndex] = {
      ...bucket[duplicateIndex],
      ...message,
      ts: Math.max(bucket[duplicateIndex].ts || 0, message.ts || 0),
    };
  } else {
    bucket.push(message);
  }
  if (bucket.length > CHAT_RENDER_LIMIT) bucket.splice(0, bucket.length - CHAT_RENDER_LIMIT);
  state.chatBySession.set(sessionId, bucket.sort((a, b) => a.ts - b.ts));
};

const ingestChatMessage = (message, { isSelf = false, source = 'unknown' } = {}) => {
  if (!message || !message.sessionId) return;
  const normalized = normalizeChatMessage(message, { defaultFrom: isSelf ? 'tech' : 'client' });
  if (!normalized) return;
  debugChatLog('[chat] message received', { source, sessionId: message.sessionId, message: normalized });
  const previousSize = (state.chatBySession.get(message.sessionId) || []).length;
  pushChatToStore(message.sessionId, normalized);
  const currentSize = (state.chatBySession.get(message.sessionId) || []).length;
  const sessionIndex = state.sessions.findIndex((s) => s.sessionId === message.sessionId);
  if (sessionIndex >= 0) {
    const updatedLog = ensureChatStore(message.sessionId);
    const existing = state.sessions[sessionIndex];
    state.sessions[sessionIndex] = {
      ...existing,
      chatLog: updatedLog,
      extra: { ...(existing.extra || {}), chatLog: updatedLog, lastMessageAt: normalized.ts },
    };
  }
  if (currentSize === previousSize) return;
  if (state.renderedChatSessionId === message.sessionId) {
    const session = state.sessions.find((s) => s.sessionId === message.sessionId);
    const isTech = normalized.from === 'tech';
    addChatMessage({
      author: isTech ? (getTechDataset().techName || 'Você') : session?.clientName || normalized.from,
      text: normalized.text,
      type: normalized.type,
      audioUrl: normalized.audioUrl,
      imageUrl: normalized.imageUrl,
      fileUrl: normalized.fileUrl,
      fileName: normalized.fileName,
      mimeType: normalized.mimeType,
      fileSize: normalized.fileSize,
      kind: isTech ? 'self' : 'client',
      ts: normalized.ts,
    });
  }
};

const getTelemetryForSession = (sessionId) => {
  if (!sessionId) return null;
  return state.telemetryBySession.get(sessionId) || null;
};

const resetCommandState = () => {
  state.commandState = {
    shareActive: false,
    remoteActive: false,
    callActive: false,
  };
  if (dom.controlStart) dom.controlStart.textContent = 'Solicitar visualização';
  if (dom.controlRemote) dom.controlRemote.textContent = 'Solicitar acesso remoto';
  if (dom.controlQuality) dom.controlQuality.textContent = 'Iniciar chamada';
  if (dom.controlStats) dom.controlStats.textContent = 'Encerrar suporte';
  updateCallControlLabel();
};

const stopStreamTracks = (stream) => {
  if (!stream) return;
  try {
    stream.getTracks().forEach((track) => {
      try {
        track.stop();
      } catch (err) {
        console.warn('Falha ao encerrar track local', err);
      }
    });
  } catch (err) {
    console.warn('Falha ao encerrar stream local', err);
  }
};

const clearRemoteVideo = ({ stopTracks = false } = {}) => {
  if (state.media.remoteStream) {
    state.media.remoteStream.getTracks().forEach((track) => {
      track.onended = null;
    });
    if (stopTracks) {
      stopStreamTracks(state.media.remoteStream);
    }
  }
  state.media.remoteStream = null;
  if (dom.sessionVideo) {
    dom.sessionVideo.srcObject = null;
    dom.sessionVideo.setAttribute('hidden', 'hidden');
  }
  if (dom.sessionPlaceholder) {
    dom.sessionPlaceholder.removeAttribute('hidden');
  }
  updateMediaDisplay();
};

const clearRemoteAudio = ({ stopTracks = false } = {}) => {
  if (state.media.remoteAudioStream && state.media.remoteAudioStream !== state.media.remoteStream) {
    state.media.remoteAudioStream.getTracks().forEach((track) => {
      track.onended = null;
    });
    if (stopTracks) {
      stopStreamTracks(state.media.remoteAudioStream);
    }
  }
  state.media.remoteAudioStream = null;
  if (dom.sessionAudio) {
    if (state.legacyShare.remoteAudioStream) {
      dom.sessionAudio.srcObject = state.legacyShare.remoteAudioStream;
    } else {
      dom.sessionAudio.srcObject = null;
      dom.sessionAudio.pause();
      dom.sessionAudio.setAttribute('hidden', 'hidden');
    }
  }
  updateMediaDisplay();
};

const stopCallMedia = () => {
  stopStreamTracks(state.media.local.audio);
  state.media.local.audio = null;
  clearRemoteAudio();
  if (state.call.pc) {
    try {
      state.call.pc.ontrack = null;
      state.call.pc.onicecandidate = null;
      state.call.pc.onconnectionstatechange = null;
      state.call.pc.close();
    } catch (error) {
      console.warn('Falha ao encerrar PeerConnection de chamada', error);
    }
  }
  state.call.pc = null;
  updateMediaDisplay();
};

const startCallAudioMedia = async (sessionId) => {
  if (!sessionId) return null;
  const pc = ensureCallPeerConnection(sessionId);
  if (!pc) return null;
  if (state.media.local.audio) {
    return pc;
  }
  const stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: false });
  removeSendersForType('audio', { pc, useStoredSenders: false, stopTracks: true });
  stream.getTracks().forEach((track) => {
    pc.addTrack(track, stream);
    track.addEventListener('ended', () => {
      void endActiveCall({ reason: 'local_track_ended' });
    });
  });
  state.media.senders.audio = [];
  stopStreamTracks(state.media.local.audio);
  state.media.local.audio = stream;
  logCall('CALL media tracks added');
  updateMediaDisplay();
  return pc;
};

const clearLegacyVideo = () => {
  if (state.legacyShare.remoteStream) {
    state.legacyShare.remoteStream.getTracks().forEach((track) => {
      track.onended = null;
    });
    stopStreamTracks(state.legacyShare.remoteStream);
  }
  state.legacyShare.remoteStream = null;
  updateMediaDisplay();
};

const clearLegacyAudio = () => {
  if (
    state.legacyShare.remoteAudioStream &&
    state.legacyShare.remoteAudioStream !== state.legacyShare.remoteStream
  ) {
    state.legacyShare.remoteAudioStream.getTracks().forEach((track) => {
      track.onended = null;
    });
    stopStreamTracks(state.legacyShare.remoteAudioStream);
  }
  state.legacyShare.remoteAudioStream = null;
  if (dom.sessionAudio && !state.media.remoteAudioStream) {
    dom.sessionAudio.pause();
    dom.sessionAudio.setAttribute('hidden', 'hidden');
    dom.sessionAudio.srcObject = null;
  } else if (dom.sessionAudio && state.media.remoteAudioStream) {
    dom.sessionAudio.srcObject = state.media.remoteAudioStream;
  }
  updateMediaDisplay();
};

const updateMediaDisplay = () => {
  scheduleRender(() => {
    const activeVideoStream =
      state.media.local.screen || state.media.remoteStream || state.legacyShare.remoteStream;
    const hasVideo = Boolean(activeVideoStream);
    if (hasVideo && !state.commandState.shareActive) {
      state.commandState.shareActive = true;
      if (dom.controlStart) dom.controlStart.textContent = 'Encerrar visualização';
    }
    if (dom.sessionVideo) {
      if (hasVideo) {
        dom.sessionVideo.removeAttribute('hidden');
        if (dom.sessionVideo.srcObject !== activeVideoStream) {
          dom.sessionVideo.srcObject = activeVideoStream;
        }
      } else {
        dom.sessionVideo.setAttribute('hidden', 'hidden');
        dom.sessionVideo.srcObject = null;
      }
    }
    if (dom.whiteboardCanvas) {
      dom.whiteboardCanvas.hidden = !hasVideo;
      if (hasVideo) {
        scheduleWhiteboardResize();
      }
    }
    if (dom.sessionPlaceholder) {
      if (hasVideo) {
        dom.sessionPlaceholder.setAttribute('hidden', 'hidden');
      } else {
        dom.sessionPlaceholder.removeAttribute('hidden');
      }
    }
  });
};

const teardownPeerConnection = () => {
  if (state.media.rtcMetricsIntervalId) {
    clearInterval(state.media.rtcMetricsIntervalId);
    sessionResources.intervals.delete(state.media.rtcMetricsIntervalId);
    state.media.rtcMetricsIntervalId = null;
  }
  if (state.media.pc) {
    try {
      state.media.pc.ontrack = null;
      state.media.pc.onicecandidate = null;
      state.media.pc.onconnectionstatechange = null;
      state.media.pc.ondatachannel = null;
      state.media.pc.close();
    } catch (err) {
      console.warn('Falha ao encerrar PeerConnection', err);
    }
  }
  if (state.media.ctrlChannel) {
    resetRemoteControlChannel();
  }
  state.media.pc = null;
  state.media.sessionId = null;
  if (state.media.eventsUnsub) {
    try {
      state.media.eventsUnsub();
    } catch (err) {
      console.warn('Falha ao cancelar listener de eventos WebRTC', err);
    }
  }
  state.media.eventsUnsub = null;
  state.media.eventsRef = null;
  state.media.eventsSessionId = null;
  state.media.eventsStartedAtMs = 0;
  state.media.processedEventIds = new Set();
  state.media.pendingRemoteIce = [];
  state.media.senders = { screen: [], audio: [] };
  stopStreamTracks(state.media.local.screen);
  stopStreamTracks(state.media.local.audio);
  state.media.local = { screen: null, audio: null };
  clearRemoteVideo({ stopTracks: true });
  clearRemoteAudio({ stopTracks: true });
};

const stopRtcMetrics = () => {
  if (!state.media.rtcMetricsIntervalId) return;
  clearInterval(state.media.rtcMetricsIntervalId);
  sessionResources.intervals.delete(state.media.rtcMetricsIntervalId);
  state.media.rtcMetricsIntervalId = null;
};

const startRtcMetrics = (pc) => {
  if (!RTC_METRICS_DEBUG || !pc || state.media.rtcMetricsIntervalId) return;
  const logMetrics = async () => {
    if (!pc || pc.connectionState === 'closed') {
      stopRtcMetrics();
      return;
    }
    try {
      const stats = await pc.getStats();
      const inboundRtp = [];
      let selectedPair = null;
      stats.forEach((report) => {
        if (report.type === 'inbound-rtp' && (report.kind === 'video' || report.mediaType === 'video')) {
          inboundRtp.push({
            id: report.id,
            framesDecoded: report.framesDecoded ?? null,
            framesDropped: report.framesDropped ?? null,
            jitter: report.jitter ?? null,
            jitterBufferDelay: report.jitterBufferDelay ?? null,
            totalDecodeTime: report.totalDecodeTime ?? null,
            keyFramesDecoded: report.keyFramesDecoded ?? null,
          });
        }
        if (report.type === 'candidate-pair' && report.state === 'succeeded') {
          if (!selectedPair || report.selected || report.nominated) {
            selectedPair = report;
          }
        }
      });
      const candidatePair = selectedPair
        ? {
            id: selectedPair.id,
            currentRoundTripTime: selectedPair.currentRoundTripTime ?? null,
            availableIncomingBitrate: selectedPair.availableIncomingBitrate ?? null,
          }
        : null;
      console.info('[RTC][metrics]', {
        inboundRtp,
        candidatePair,
      });
    } catch (error) {
      console.warn('Falha ao coletar métricas WebRTC', error);
    }
  };
  logMetrics();
  state.media.rtcMetricsIntervalId = trackInterval(setInterval(logMetrics, RTC_METRICS_INTERVAL_MS));
};

const ensurePeerConnection = (sessionId) => {
  if (!sessionId) return null;
  if (state.media.pc && state.media.sessionId && state.media.sessionId !== sessionId) {
    teardownPeerConnection();
  }
  if (state.media.pc && isMediaPcUnhealthy(state.media.pc)) {
    resetMediaPeerConnection(sessionId);
  }
  if (state.media.pc) return state.media.pc;

  const pc = new RTCPeerConnection({
    iceServers: [{ urls: 'stun:stun.l.google.com:19302' }],
  });

  pc.onicecandidate = async (event) => {
    if (!event.candidate) return;
    if (socket && !socket.disconnected) {
      socket.emit('signal:candidate', { sessionId, candidate: event.candidate });
    }
    if (state.media.eventsRef && state.media.eventsSessionId === sessionId) {
      try {
        await addDoc(state.media.eventsRef, {
          type: 'ice',
          from: 'tech',
          candidate: event.candidate.candidate,
          sdpMid: event.candidate.sdpMid,
          sdpMLineIndex: event.candidate.sdpMLineIndex,
          createdAt: serverTimestamp(),
        });
      } catch (error) {
        console.warn('Falha ao registrar ICE no Firestore', error);
      }
    }
  };

  pc.onconnectionstatechange = () => {
    if (['disconnected', 'failed', 'closed'].includes(pc.connectionState) && isMediaPcUnhealthy(pc)) {
      resetMediaPeerConnection(sessionId);
    }
  };

  pc.ondatachannel = (event) => {
    if (!event?.channel) return;
    if (event.channel.label === CTRL_CHANNEL_LABEL) {
      setCtrlChannel(event.channel);
    }
  };

  pc.ontrack = (event) => {
    if (!event || !event.track) return;
    if (event.track.kind === 'video') {
      const stream = event.streams?.[0] || new MediaStream([event.track]);
      state.media.remoteStream = stream;
      if (dom.sessionVideo) {
        dom.sessionVideo.srcObject = stream;
        dom.sessionVideo.removeAttribute('hidden');
        const playPromise = dom.sessionVideo.play();
        if (playPromise && typeof playPromise.catch === 'function') {
          playPromise.catch(() => {});
        }
      }
      if (dom.sessionPlaceholder) dom.sessionPlaceholder.setAttribute('hidden', 'hidden');
      event.track.onended = () => {
        if (state.media.remoteStream === stream) {
          clearRemoteVideo();
        }
      };
    }
    if (event.track.kind === 'audio') {
      const audioStream = state.media.remoteAudioStream || new MediaStream();
      audioStream.addTrack(event.track);
      state.media.remoteAudioStream = audioStream;
      if (dom.sessionAudio) {
        dom.sessionAudio.srcObject = audioStream;
        dom.sessionAudio.removeAttribute('hidden');
        const playPromise = dom.sessionAudio.play();
        if (playPromise && typeof playPromise.catch === 'function') {
          playPromise.catch(() => {});
        }
      }
      event.track.onended = () => {
        if (state.media.remoteAudioStream) {
          const tracks = state.media.remoteAudioStream.getTracks().filter((t) => t !== event.track);
          const stream = new MediaStream(tracks);
          state.media.remoteAudioStream = stream.getTracks().length ? stream : null;
          if (!state.media.remoteAudioStream && dom.sessionAudio) {
            clearRemoteAudio();
          } else if (state.media.remoteAudioStream && dom.sessionAudio) {
            dom.sessionAudio.srcObject = state.media.remoteAudioStream;
          }
        }
      };
    }
    updateMediaDisplay();
  };

  state.media.pc = pc;
  state.media.sessionId = sessionId;
  state.media.pendingRemoteIce = [];
  startRtcMetrics(pc);
  return pc;
};

const ensureWebRtcEventListener = async (sessionId) => {
  if (!sessionId) return;
  if (state.media.eventsSessionId === sessionId && state.media.eventsUnsub) return;
  if (state.media.eventsUnsub) {
    try {
      state.media.eventsUnsub();
    } catch (error) {
      console.warn('Falha ao limpar listener antigo de eventos WebRTC', error);
    }
  }
  state.media.eventsUnsub = null;
  state.media.eventsRef = null;
  state.media.eventsSessionId = null;
  state.media.eventsStartedAtMs = 0;
  state.media.processedEventIds = new Set();
  state.media.pendingRemoteIce = [];

  try {
    const user = await ensureAuth();
    if (!user) {
      console.warn('Auth indisponível. Listener WebRTC não será iniciado.', sessionId);
      return;
    }
  } catch (error) {
    console.error('Falha ao autenticar antes do WebRTC', sessionId, error);
    return;
  }

  const db = ensureFirestore();
  if (!db) return;
  state.media.eventsStartedAtMs = Date.now() - 2000;
  const since = Timestamp.fromMillis(state.media.eventsStartedAtMs);

  const eventsRef = collection(db, 'sessions', sessionId, 'events');
  const eventsQuery = query(eventsRef, where('createdAt', '>=', since), orderBy('createdAt', 'asc'));
  state.media.eventsRef = eventsRef;
  state.media.eventsSessionId = sessionId;
  state.media.eventsUnsub = onSnapshot(
    eventsQuery,
    async (snapshot) => {
      if (state.media.eventsSessionId !== sessionId) return;
      for (const change of snapshot.docChanges()) {
        if (change.type !== 'added') continue;
        if (state.media.processedEventIds.has(change.doc.id)) continue;
        state.media.processedEventIds.add(change.doc.id);
        const data = change.doc.data() || {};
        if (data.from !== 'client') continue;
        let pc = ensurePeerConnection(sessionId);
        if (!pc) continue;
        if (isMediaPcUnhealthy(pc)) {
          resetMediaPeerConnection(sessionId);
          pc = ensurePeerConnection(sessionId);
          if (!pc) continue;
        }
        if (data.type === 'offer' && data.sdp) {
          try {
            if (pc.signalingState !== 'stable') {
              console.info('[WEBRTC] oferta ignorada por estado inválido', pc.signalingState);
              resetMediaPeerConnection(sessionId);
              pc = ensurePeerConnection(sessionId);
              if (!pc || pc.signalingState !== 'stable') {
                continue;
              }
            }
            await pc.setRemoteDescription({ type: 'offer', sdp: data.sdp });
            await flushPendingMediaIce(pc);
            const answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);
            await addDoc(eventsRef, {
              type: 'answer',
              from: 'tech',
              sdp: pc.localDescription?.sdp || answer.sdp,
              createdAt: serverTimestamp(),
            });
          } catch (error) {
            console.error('Erro ao processar oferta WebRTC', error);
          }
        }
        if (data.type === 'ice' && data.candidate) {
          try {
            const candidate = {
              candidate: data.candidate,
              sdpMid: data.sdpMid ?? null,
              sdpMLineIndex: data.sdpMLineIndex ?? null,
            };
            if (!pc.remoteDescription) {
              state.media.pendingRemoteIce.push(candidate);
              continue;
            }
            await pc.addIceCandidate(candidate);
          } catch (error) {
            console.error('Erro ao adicionar ICE WebRTC', error);
          }
        }
      }
    },
    (error) => {
      console.error('Falha ao escutar eventos WebRTC da sessão', sessionId, error);
    }
  );
};

const ensureCallPeerConnection = (sessionId) => {
  if (!sessionId) return null;
  if (state.call.pc && state.call.sessionId && state.call.sessionId !== sessionId) {
    try {
      state.call.pc.close();
    } catch (error) {
      console.warn('Falha ao encerrar PeerConnection de chamada', error);
    }
    state.call.pc = null;
  }
  if (state.call.pc) return state.call.pc;

  const pc = new RTCPeerConnection({
    iceServers: [{ urls: 'stun:stun.l.google.com:19302' }],
  });

  pc.onicecandidate = async (event) => {
    if (!event.candidate) return;
    if (state.call.sessionId === sessionId && state.call.localIceRef) {
      try {
        await addDoc(state.call.localIceRef, {
          type: 'ice',
          from: 'tech',
          sdp: event.candidate.candidate,
          candidate: event.candidate.candidate,
          sdpMid: event.candidate.sdpMid,
          sdpMLineIndex: event.candidate.sdpMLineIndex,
          callId: state.call.callId || null,
          createdAt: Date.now(),
        });
        state.call.localIceCount += 1;
        logCall('ICE local count', state.call.localIceCount);
      } catch (error) {
        console.warn('Falha ao registrar ICE de chamada', error);
      }
    }
  };

  pc.onconnectionstatechange = () => {
    if (state.call.sessionId === sessionId && pc.connectionState === 'connected') {
      setCallState(CallStates.IN_CALL, { sessionId });
      logCall('CALL connected');
    }
    if (['disconnected', 'failed', 'closed'].includes(pc.connectionState)) {
      clearRemoteAudio();
      if (state.call.sessionId === sessionId) {
        logCall('CALL disconnected', pc.connectionState);
      }
    }
  };

  pc.ontrack = (event) => {
    if (!event || !event.track) return;
    if (event.track.kind !== 'audio') return;
    const audioStream = state.media.remoteAudioStream || new MediaStream();
    audioStream.addTrack(event.track);
    state.media.remoteAudioStream = audioStream;
    if (dom.sessionAudio) {
      dom.sessionAudio.srcObject = audioStream;
      dom.sessionAudio.removeAttribute('hidden');
      const playPromise = dom.sessionAudio.play();
      if (playPromise && typeof playPromise.catch === 'function') {
        playPromise.catch(() => {});
      }
    }
    event.track.onended = () => {
      if (state.media.remoteAudioStream) {
        const tracks = state.media.remoteAudioStream.getTracks().filter((t) => t !== event.track);
        const stream = new MediaStream(tracks);
        state.media.remoteAudioStream = stream.getTracks().length ? stream : null;
        if (!state.media.remoteAudioStream && dom.sessionAudio) {
          clearRemoteAudio();
        } else if (state.media.remoteAudioStream && dom.sessionAudio) {
          dom.sessionAudio.srcObject = state.media.remoteAudioStream;
        }
      }
    };
    updateMediaDisplay();
  };

  state.call.pc = pc;
  return pc;
};

const ensureCallRefs = (sessionId, direction = state.call.direction) => {
  const db = ensureFirestore();
  if (!db || !sessionId) return null;
  const techToClient = direction === 'tech_to_client';
  return {
    callDocRef: doc(db, 'sessions', sessionId, 'call', 'active'),
    localIceRef: collection(
      db,
      'sessions',
      sessionId,
      techToClient ? 'call_ice_client' : 'call_ice_tech'
    ),
    remoteIceRef: collection(
      db,
      'sessions',
      sessionId,
      techToClient ? 'call_ice_tech' : 'call_ice_client'
    ),
  };
};

const updateCallDoc = async (sessionId, updates) => {
  const refs = ensureCallRefs(sessionId);
  if (!refs) return false;
  try {
    await setDoc(refs.callDocRef, updates, { merge: true });
    return true;
  } catch (error) {
    console.error('Falha ao atualizar doc de chamada', error);
    return false;
  }
};

const prepareCallSession = (sessionId, data = {}) => {
  state.call.sessionId = sessionId;
  state.call.direction = data.direction || state.call.direction;
  const refs = ensureCallRefs(sessionId, state.call.direction);
  if (refs) {
    state.call.callDocRef = refs.callDocRef;
    state.call.localIceRef = refs.localIceRef;
    state.call.remoteIceRef = refs.remoteIceRef;
  }
  state.call.callId = data.callId || state.call.callId || null;
  state.call.fromUid = data.fromUid || state.call.fromUid || null;
  state.call.toUid = data.toUid || state.call.toUid || null;
};

const scheduleCallTimeout = (sessionId) => {
  clearCallTimeout();
  state.call.ringTimeoutId = setTimeout(() => {
    if (!sessionId || state.call.sessionId !== sessionId) return;
    if (![CallStates.OUTGOING_RINGING, CallStates.INCOMING_RINGING].includes(state.call.status)) return;
    logCall('CALL timeout');
    const endedAt = Date.now();
    void updateCallDoc(sessionId, {
      status: 'timeout',
      reason: 'timeout',
      endedAt,
      updatedAt: endedAt,
    });
  }, CALL_RING_TIMEOUT_MS);
};

const ensureRemoteIceListener = (sessionId) => {
  if (!sessionId || !state.call.remoteIceRef) return;
  if (state.call.remoteIceUnsub) return;
  const remoteQuery = query(state.call.remoteIceRef, orderBy('createdAt', 'asc'));
  state.call.remoteIceUnsub = onSnapshot(
    remoteQuery,
    async (snapshot) => {
      const pc = ensureCallPeerConnection(sessionId);
      if (!pc) return;
      for (const change of snapshot.docChanges()) {
        if (change.type !== 'added') continue;
        if (state.call.remoteIceIds.has(change.doc.id)) continue;
        state.call.remoteIceIds.add(change.doc.id);
        const data = change.doc.data() || {};
        if (state.call.callId && data.callId && data.callId !== state.call.callId) continue;
        const candidateValue = data.sdp || data.candidate;
        if (!candidateValue) continue;
        const iceObj = {
          candidate: candidateValue,
          sdpMid: data.sdpMid ?? null,
          sdpMLineIndex: data.sdpMLineIndex ?? null,
        };
        if (!pc.remoteDescription) {
          state.call.pendingRemoteIce.push(iceObj);
          continue;
        }
        try {
          await pc.addIceCandidate(iceObj);
          state.call.remoteIceCount += 1;
          logCall('ICE remoto aplicado', state.call.remoteIceCount);
        } catch (error) {
          console.error('Erro ao adicionar ICE remoto da chamada', error);
        }
      }
    },
    (error) => {
      console.error('Falha ao escutar ICE remoto da chamada', sessionId, error);
    }
  );
};

const flushPendingIce = async (pc) => {
  if (!pc?.remoteDescription) return;
  const pending = state.call.pendingRemoteIce || [];
  state.call.pendingRemoteIce = [];
  for (const ice of pending) {
    try {
      await pc.addIceCandidate(ice);
    } catch (error) {
      console.warn('Falha ao aplicar ICE pendente da chamada', error);
    }
  }
};

const handleCallAccepted = async (sessionId, data) => {
  if (!sessionId) return;
  prepareCallSession(sessionId, data);
  clearCallTimeout();
  if (state.call.status !== CallStates.IN_CALL) {
    setCallState(CallStates.CONNECTING, { sessionId, direction: data.direction, callId: state.call.callId });
  }
  await startCallAudioMedia(sessionId);
  ensureRemoteIceListener(sessionId);

  const pc = ensureCallPeerConnection(sessionId);
  if (!pc) return;

  if (state.call.direction === 'tech_to_client') {
    if (!state.call.offerSent) {
      state.call.offerSent = true;
      try {
        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        await updateCallDoc(sessionId, {
          status: 'accepted',
          offerSdp: pc.localDescription?.sdp || offer.sdp,
          updatedAt: Date.now(),
        });
        logCall('CALL offer saved/applied');
      } catch (error) {
        state.call.offerSent = false;
        throw error;
      }
    }
    if (data.answerSdp && !pc.currentRemoteDescription && !state.call.remoteAnswerApplying) {
      state.call.remoteAnswerApplying = true;
      try {
        await pc.setRemoteDescription({ type: 'answer', sdp: data.answerSdp });
        await flushPendingIce(pc);
        logCall('CALL answer saved/applied');
      } finally {
        state.call.remoteAnswerApplying = false;
      }
    }
  }

  if (state.call.direction === 'client_to_tech') {
    if (data.offerSdp && !pc.currentRemoteDescription && !state.call.remoteOfferApplying) {
      state.call.remoteOfferApplying = true;
      try {
        await pc.setRemoteDescription({ type: 'offer', sdp: data.offerSdp });
        await flushPendingIce(pc);
        logCall('CALL offer saved/applied');
      } finally {
        state.call.remoteOfferApplying = false;
      }
      if (!state.call.answerSent) {
        state.call.answerSent = true;
        try {
          const answer = await pc.createAnswer();
          await pc.setLocalDescription(answer);
          await updateCallDoc(sessionId, {
            status: 'accepted',
            answerSdp: pc.localDescription?.sdp || answer.sdp,
            updatedAt: Date.now(),
          });
          logCall('CALL answer saved/applied');
        } catch (error) {
          state.call.answerSent = false;
          throw error;
        }
      }
    }
  }
};

const handleCallTermination = (sessionId, data = {}) => {
  if (!sessionId || state.call.sessionId !== sessionId) return;
  clearCallTimeout();
  const resolvedState =
    data.status === 'declined'
      ? CallStates.DECLINED
      : data.reason === 'timeout'
        ? CallStates.TIMEOUT
        : CallStates.ENDED;
  setCallState(resolvedState, { sessionId, direction: state.call.direction });
  const reason =
    data.reason === 'timeout'
      ? 'Sem resposta.'
      : data.status === 'declined'
        ? 'Chamada recusada.'
        : 'Chamada encerrada.';
  cleanupCallSession({ message: reason });
};

const handleCallSnapshot = async (sessionId, snapshot) => {
  if (!sessionId) return;
  if (!snapshot.exists()) {
    if (state.call.sessionId === sessionId) {
      cleanupCallSession({ message: 'Chamada finalizada.' });
    }
    return;
  }
  const data = snapshot.data() || {};
  if (!data.status || !data.direction) return;
  const direction = data.direction;
  const incoming = direction === 'client_to_tech';
  const outgoing = direction === 'tech_to_client';
  if (!incoming && !outgoing) return;

  if (state.call.sessionId && state.call.sessionId !== sessionId && state.call.status !== CallStates.IDLE) {
    logCall('CALL recebida em outra sessão enquanto ocupado', sessionId);
    return;
  }

  prepareCallSession(sessionId, data);
  if (incoming) {
    selectSessionById(sessionId);
  }

  if (data.status === 'ringing') {
    if (state.call.status === CallStates.IDLE) {
      state.call.remoteIceIds = new Set();
      state.call.remoteIceCount = 0;
      state.call.localIceCount = 0;
      state.call.offerSent = false;
      state.call.answerSent = false;
    }
    setCallState(incoming ? CallStates.INCOMING_RINGING : CallStates.OUTGOING_RINGING, {
      sessionId,
      direction,
      callId: state.call.callId,
    });
    scheduleCallTimeout(sessionId);
    return;
  }

  if (data.status === 'accepted') {
    try {
      await handleCallAccepted(sessionId, data);
    } catch (error) {
      console.error('Falha ao processar chamada aceita', error);
      cleanupCallSession({ message: 'Não foi possível iniciar a chamada.' });
    }
    return;
  }

  if (data.status === 'declined' || data.status === 'ended') {
    handleCallTermination(sessionId, { ...data, status: data.status });
  }
};

const startOutgoingCall = async () => {
  const session = getSelectedSession();
  if (!session) {
    addChatMessage({ author: 'Sistema', text: 'Nenhuma sessão selecionada.', kind: 'system' });
    return;
  }
  if (state.call.status !== CallStates.IDLE) {
    showToast('Já existe uma chamada em andamento.');
    return;
  }
  try {
    const user = await ensureAuth();
    if (!user) {
      showToast('Auth indisponível. Não foi possível iniciar a chamada.');
      return;
    }
  } catch (error) {
    console.error('Falha ao autenticar antes da chamada', error);
    showToast('Auth indisponível. Não foi possível iniciar a chamada.');
    return;
  }
  const callId = generateCallId();
  const tech = getTechProfile();
  prepareCallSession(session.sessionId, {
    direction: 'tech_to_client',
    callId,
    fromUid: tech.uid,
    toUid: session.clientUid || null,
  });
  state.call.offerSent = false;
  state.call.answerSent = false;
  state.call.remoteIceIds = new Set();
  state.call.remoteIceCount = 0;
  state.call.localIceCount = 0;
  const now = Date.now();
  const updated = await updateCallDoc(session.sessionId, {
    status: 'ringing',
    direction: 'tech_to_client',
    callId,
    fromUid: tech.uid || null,
    fromName: tech.name || null,
    toUid: session.clientUid || null,
    createdAt: now,
    updatedAt: now,
  });
  if (!updated) {
    cleanupCallSession({ message: 'Não foi possível iniciar a chamada.' });
    return;
  }
  setCallState(CallStates.OUTGOING_RINGING, {
    sessionId: session.sessionId,
    direction: 'tech_to_client',
    callId,
  });
  scheduleCallTimeout(session.sessionId);
};

const acceptIncomingCall = async () => {
  if (state.call.status !== CallStates.INCOMING_RINGING || !state.call.sessionId) return;
  const sessionId = state.call.sessionId;
  const acceptedAt = Date.now();
  const updated = await updateCallDoc(sessionId, {
    status: 'accepted',
    acceptedAt,
    updatedAt: acceptedAt,
  });
  if (!updated) {
    showToast('Não foi possível aceitar a chamada.');
    return;
  }
  setCallState(CallStates.CONNECTING, { sessionId, direction: state.call.direction });
};

const declineIncomingCall = async () => {
  if (![CallStates.INCOMING_RINGING, CallStates.OUTGOING_RINGING].includes(state.call.status) || !state.call.sessionId) {
    return;
  }
  const sessionId = state.call.sessionId;
  const isOutgoing = state.call.status === CallStates.OUTGOING_RINGING;
  const endedAt = Date.now();
  const updated = await updateCallDoc(sessionId, {
    status: isOutgoing ? 'ended' : 'declined',
    endedAt,
    reason: isOutgoing ? 'canceled' : 'declined',
    updatedAt: endedAt,
  });
  if (!updated) {
    showToast('Não foi possível recusar a chamada.');
  }
  cleanupCallSession();
};

const endActiveCall = async ({ reason = 'ended' } = {}) => {
  if (!state.call.sessionId) return;
  const sessionId = state.call.sessionId;
  setCallState(CallStates.ENDED, { sessionId, direction: state.call.direction });
  const endedAt = Date.now();
  await updateCallDoc(sessionId, {
    status: 'ended',
    endedAt,
    reason,
    updatedAt: endedAt,
  });
  cleanupCallSession();
};

const toggleCallMute = () => {
  const stream = state.media.local.audio;
  if (!stream) return;
  const nextMuted = !state.call.muted;
  stream.getAudioTracks().forEach((track) => {
    track.enabled = !nextMuted;
  });
  state.call.muted = nextMuted;
  logCall('CALL media muted', nextMuted);
  updateCallModal();
};

const setLegacyStatus = (message) => {
  if (!dom.webShareStatus) return;
  dom.webShareStatus.textContent = message;
};

const updateLegacyControls = () => {
  if (dom.webShareConnect) {
    dom.webShareConnect.disabled = state.legacyShare.active;
  }
  if (dom.webShareDisconnect) {
    dom.webShareDisconnect.disabled = !state.legacyShare.active;
  }
  if (dom.webShareRoom) {
    dom.webShareRoom.disabled = state.legacyShare.active;
  }
};

const teardownLegacyShare = ({ keepRoom = false } = {}) => {
  if (state.legacyShare.pc) {
    try {
      state.legacyShare.pc.ontrack = null;
      state.legacyShare.pc.onicecandidate = null;
      state.legacyShare.pc.onconnectionstatechange = null;
      state.legacyShare.pc.close();
    } catch (err) {
      console.warn('Falha ao encerrar PeerConnection legado', err);
    }
  }
  state.legacyShare.pc = null;
  if (!keepRoom) state.legacyShare.room = null;
  state.legacyShare.active = false;
  state.legacyShare.pendingRoom = null;
  clearLegacyVideo();
  clearLegacyAudio();
  updateLegacyControls();
};

const ensureLegacyPeerConnection = (room) => {
  if (!room) return null;
  if (state.legacyShare.pc && state.legacyShare.room && state.legacyShare.room !== room) {
    teardownLegacyShare({ keepRoom: true });
  }
  if (state.legacyShare.pc && state.legacyShare.room === room) return state.legacyShare.pc;

  const pc = new RTCPeerConnection({
    iceServers: [{ urls: 'stun:stun.l.google.com:19302' }],
  });

  pc.onicecandidate = (event) => {
    if (!event.candidate || !socket || socket.disconnected) return;
    socket.emit('signal', { room, data: event.candidate });
  };

  pc.onconnectionstatechange = () => {
    if (pc.connectionState === 'connected') {
      setLegacyStatus('Compartilhamento web conectado.');
      return;
    }
    if (['disconnected', 'failed', 'closed'].includes(pc.connectionState)) {
      setLegacyStatus('Compartilhamento web desconectado.');
      clearLegacyVideo();
      clearLegacyAudio();
    }
  };

  pc.ontrack = (event) => {
    if (!event || !event.track) return;
    if (event.track.kind === 'video') {
      const stream = event.streams?.[0] || new MediaStream([event.track]);
      state.legacyShare.remoteStream = stream;
      event.track.addEventListener('ended', () => {
        if (state.legacyShare.remoteStream === stream) {
          clearLegacyVideo();
        }
      });
    }
    if (event.track.kind === 'audio') {
      const audioStream = state.legacyShare.remoteAudioStream || new MediaStream();
      audioStream.addTrack(event.track);
      state.legacyShare.remoteAudioStream = audioStream;
      if (dom.sessionAudio) {
        dom.sessionAudio.srcObject = audioStream;
        dom.sessionAudio.removeAttribute('hidden');
        const playPromise = dom.sessionAudio.play();
        if (playPromise && typeof playPromise.catch === 'function') {
          playPromise.catch(() => {});
        }
      }
      event.track.addEventListener('ended', () => {
        if (state.legacyShare.remoteAudioStream) {
          const tracks = state.legacyShare.remoteAudioStream.getTracks().filter((t) => t !== event.track);
          const stream = new MediaStream(tracks);
          state.legacyShare.remoteAudioStream = stream.getTracks().length ? stream : null;
          if (!state.legacyShare.remoteAudioStream && dom.sessionAudio) {
            dom.sessionAudio.pause();
            dom.sessionAudio.setAttribute('hidden', 'hidden');
          } else if (state.legacyShare.remoteAudioStream && dom.sessionAudio) {
            dom.sessionAudio.srcObject = state.legacyShare.remoteAudioStream;
          }
        }
      });
    }
    updateMediaDisplay();
  };

  state.legacyShare.pc = pc;
  state.legacyShare.room = room;
  return pc;
};

const activateLegacyShare = (room) => {
  const normalized = typeof room === 'string' ? room.trim() : '';
  if (!normalized) {
    setLegacyStatus('Informe o código de 6 dígitos para conectar.');
    return;
  }

  if (state.legacyShare.room && state.legacyShare.room !== normalized) {
    teardownLegacyShare();
  }

  state.legacyShare.active = true;
  state.legacyShare.room = normalized;
  state.legacyShare.pendingRoom = null;
  updateLegacyControls();

  if (socket && !socket.disconnected) {
    const joinPayload = { room: normalized, role: 'tech' };
    if (state.authToken) {
      joinPayload.idToken = state.authToken;
    }
    socket.emit('join', joinPayload, (ack) => {
      if (ack && ack.ok) return;
      const errorCode = ensureString(ack?.err || '').toLowerCase();
      if (errorCode === 'forbidden') {
        setLegacyStatus('Acesso negado ao compartilhamento web desta sessão.');
      } else if (errorCode.includes('token')) {
        setLegacyStatus('Falha de autenticação ao entrar no compartilhamento web.');
      } else {
        setLegacyStatus('Não foi possível conectar ao compartilhamento web.');
      }
      teardownLegacyShare({ keepRoom: true });
      if (errorCode.includes('token')) {
        state.legacyShare.pendingRoom = normalized;
      }
    });
  } else {
    state.legacyShare.pendingRoom = normalized;
  }

  ensureLegacyPeerConnection(normalized);
  setLegacyStatus('Aguardando o cliente iniciar o compartilhamento…');
};

const disconnectLegacyShare = () => {
  teardownLegacyShare();
  setLegacyStatus('Nenhum compartilhamento web ativo.');
};

const handleLegacySignal = async (payload) => {
  if (!state.legacyShare.active || !payload) return;
  const room = state.legacyShare.room;
  if (!room) return;

  const pc = ensureLegacyPeerConnection(room);
  if (!pc) return;

  if (payload.type === 'offer' || (payload.sdp && payload.type)) {
    try {
      await pc.setRemoteDescription(payload);
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      if (socket && !socket.disconnected) {
        socket.emit('signal', { room, data: pc.localDescription });
      }
    } catch (error) {
      console.error('Erro ao processar oferta web', error);
      setLegacyStatus('Falha ao aceitar a oferta do cliente.');
    }
    return;
  }

  if (payload.type === 'answer') {
    try {
      await pc.setRemoteDescription(payload);
    } catch (error) {
      console.error('Erro ao aplicar answer web', error);
    }
    return;
  }

  if (payload.candidate) {
    try {
      await pc.addIceCandidate(payload);
    } catch (error) {
      console.error('Erro ao adicionar ICE web', error);
    }
  }
};

const removeSendersForType = (type, { pc = null, useStoredSenders = true, stopTracks = false } = {}) => {
  const activePc = pc || state.media.pc;
  if (!activePc) return;
  if (!useStoredSenders) {
    const senders = activePc.getSenders().filter((sender) => sender.track?.kind === type);
    senders.forEach((sender) => {
      try {
        activePc.removeTrack(sender);
      } catch (err) {
        console.warn('Falha ao remover sender', err);
      }
      if (stopTracks && sender.track) {
        try {
          sender.track.stop();
        } catch (err) {
          console.warn('Falha ao encerrar track removida', err);
        }
      }
    });
    state.media.senders[type] = [];
    return;
  }
  const senders = state.media.senders[type] || [];
  const activeSenders = new Set(activePc.getSenders());
  senders.forEach((sender) => {
    if (!activeSenders.has(sender)) return;
    try {
      activePc.removeTrack(sender);
    } catch (err) {
      console.warn('Falha ao remover sender', err);
    }
  });
  state.media.senders[type] = [];
};

const startLocalScreenShare = async () => {
  const session = getSelectedSession();
  if (!session) return;
  if (state.media.local.screen) {
    updateMediaDisplay();
    return;
  }
  try {
    const stream = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: true });
    const pc = ensurePeerConnection(session.sessionId);
    if (!pc) return;
    removeSendersForType('screen');
    const senders = stream.getTracks().map((track) => {
      const sender = pc.addTrack(track, stream);
      track.addEventListener('ended', () => {
        stopLocalScreenShare(true);
      });
      return sender;
    });
    state.media.senders.screen = senders;
    stopStreamTracks(state.media.local.screen);
    state.media.local.screen = stream;
    if (dom.sessionVideo) {
      dom.sessionVideo.srcObject = stream;
      dom.sessionVideo.muted = true;
      dom.sessionVideo.removeAttribute('hidden');
      const playPromise = dom.sessionVideo.play();
      if (playPromise && typeof playPromise.catch === 'function') playPromise.catch(() => {});
    }
    if (dom.sessionPlaceholder) dom.sessionPlaceholder.setAttribute('hidden', 'hidden');
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    socket.emit('signal:offer', { sessionId: session.sessionId, sdp: pc.localDescription });
    state.commandState.shareActive = true;
    if (dom.controlStart) dom.controlStart.textContent = 'Encerrar visualização';
    updateMediaDisplay();
  } catch (error) {
    console.error('Falha ao iniciar compartilhamento local', error);
    addChatMessage({ author: 'Sistema', text: 'Não foi possível iniciar o compartilhamento de tela.', kind: 'system' });
  }
};

const stopLocalScreenShare = async (notifyRemote = false) => {
  removeSendersForType('screen');
  stopStreamTracks(state.media.local.screen);
  state.media.local.screen = null;
  if (!state.media.remoteStream) {
    if (dom.sessionVideo) {
      dom.sessionVideo.srcObject = null;
      dom.sessionVideo.setAttribute('hidden', 'hidden');
    }
    if (dom.sessionPlaceholder) dom.sessionPlaceholder.removeAttribute('hidden');
  }
  updateMediaDisplay();
  if (notifyRemote && socket && !socket.disconnected) {
    try {
      const sessionId = state.media.sessionId || (getSelectedSession()?.sessionId ?? null);
      const { session } = await sendSessionCommand('share_stop', {}, { silent: true, sessionId });
      registerCommand({ sessionId: session.sessionId, type: 'share_stop', by: 'tech', ts: Date.now() }, { local: true });
    } catch (err) {
      console.warn('Falha ao notificar parada de compartilhamento', err);
    }
  }
  state.commandState.shareActive = false;
  const targetSessionId = state.media.sessionId || (getSelectedSession()?.sessionId ?? null);
  if (targetSessionId) {
    resetMediaPeerConnection(targetSessionId);
  }
  if (dom.controlStart) dom.controlStart.textContent = 'Solicitar visualização';
};

const startLocalCall = async () => {
  const session = getSelectedSession();
  if (!session) return;
  if (state.media.local.audio) {
    return;
  }
  try {
    const pc = await startCallAudioMedia(session.sessionId);
    if (!pc) return;
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    socket.emit('signal:offer', { sessionId: session.sessionId, sdp: pc.localDescription });
    state.commandState.callActive = true;
    if (dom.controlQuality) dom.controlQuality.textContent = 'Encerrar chamada';
    updateMediaDisplay();
  } catch (error) {
    console.error('Falha ao iniciar chamada local', error);
    addChatMessage({ author: 'Sistema', text: 'Não foi possível iniciar a chamada.', kind: 'system' });
  }
};

const stopLocalCall = async (notifyRemote = false) => {
  stopCallMedia();
  updateMediaDisplay();
  if (notifyRemote && socket && !socket.disconnected) {
    try {
      const sessionId = state.media.sessionId || (getSelectedSession()?.sessionId ?? null);
      const { session } = await sendSessionCommand('call_end', {}, { silent: true, sessionId });
      registerCommand({ sessionId: session.sessionId, type: 'call_end', by: 'tech', ts: Date.now() }, { local: true });
    } catch (err) {
      console.warn('Falha ao notificar fim da chamada', err);
    }
  }
  state.commandState.callActive = false;
  if (dom.controlQuality) dom.controlQuality.textContent = 'Iniciar chamada';
};

const LIGHTBOX_MIN_ZOOM = 0.5;
const LIGHTBOX_MAX_ZOOM = 4;
const LIGHTBOX_ZOOM_STEP = 0.2;

const clampLightboxZoom = (value) => Math.min(LIGHTBOX_MAX_ZOOM, Math.max(LIGHTBOX_MIN_ZOOM, value));

const updateLightboxZoom = () => {
  if (!dom.imageLightboxImage) return;
  dom.imageLightboxImage.style.transform = `scale(${state.lightbox.zoom})`;
};

const updateLightboxActions = (imageUrl) => {
  if (dom.imageLightboxDownload) {
    dom.imageLightboxDownload.href = imageUrl;
  }
  if (dom.imageLightboxOpen) {
    dom.imageLightboxOpen.href = imageUrl;
  }
};

const setLightboxZoom = (nextZoom) => {
  state.lightbox.zoom = clampLightboxZoom(nextZoom);
  updateLightboxZoom();
};

const closeImageLightbox = () => {
  if (!state.lightbox.isOpen) return;
  state.lightbox.isOpen = false;
  state.lightbox.imageUrl = '';
  state.lightbox.zoom = 1;
  if (dom.imageLightboxImage) {
    dom.imageLightboxImage.removeAttribute('src');
    dom.imageLightboxImage.style.transform = 'scale(1)';
  }
  if (dom.imageLightbox) {
    dom.imageLightbox.hidden = true;
  }
};

const openImageLightbox = (imageUrl) => {
  if (!imageUrl || !dom.imageLightbox || !dom.imageLightboxImage) return;
  state.lightbox.isOpen = true;
  state.lightbox.imageUrl = imageUrl;
  state.lightbox.zoom = 1;
  dom.imageLightbox.hidden = false;
  dom.imageLightboxImage.src = imageUrl;
  updateLightboxActions(imageUrl);
  updateLightboxZoom();
};

const bindImageLightboxControls = () => {
  if (!dom.imageLightbox) return;

  dom.imageLightboxClose?.addEventListener('click', closeImageLightbox);
  dom.imageLightboxZoomIn?.addEventListener('click', () => setLightboxZoom(state.lightbox.zoom + LIGHTBOX_ZOOM_STEP));
  dom.imageLightboxZoomOut?.addEventListener('click', () => setLightboxZoom(state.lightbox.zoom - LIGHTBOX_ZOOM_STEP));

  dom.imageLightbox.addEventListener('click', (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    if (target.dataset.lightboxClose === 'true') {
      closeImageLightbox();
    }
  });

  dom.imageLightbox.addEventListener(
    'wheel',
    (event) => {
      if (!state.lightbox.isOpen) return;
      event.preventDefault();
      const direction = event.deltaY < 0 ? 1 : -1;
      setLightboxZoom(state.lightbox.zoom + direction * LIGHTBOX_ZOOM_STEP);
    },
    { passive: false }
  );

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      closeImageLightbox();
      return;
    }
    if (!state.lightbox.isOpen) return;
    if (event.key === '+' || event.key === '=') {
      event.preventDefault();
      setLightboxZoom(state.lightbox.zoom + LIGHTBOX_ZOOM_STEP);
    }
    if (event.key === '-') {
      event.preventDefault();
      setLightboxZoom(state.lightbox.zoom - LIGHTBOX_ZOOM_STEP);
    }
  });
};

const createChatEntryElement = ({
  author,
  text,
  type = 'text',
  audioUrl = '',
  imageUrl = '',
  fileUrl = '',
  fileName = '',
  mimeType = '',
  fileSize = null,
  kind = 'client',
  ts = Date.now(),
}) => {
  const entry = document.createElement('div');
  entry.className = 'message';
  if (kind === 'self') entry.classList.add('self');
  if (kind === 'system') entry.classList.add('system');

  const header = document.createElement('div');
  header.className = 'message-header';
  header.textContent = `${formatTime(ts)} • ${author}`;
  entry.appendChild(header);

  const body = document.createElement('div');
  body.className = 'message-body';
  if (type === 'audio' && audioUrl) {
    const audio = document.createElement('audio');
    audio.controls = true;
    audio.src = audioUrl;
    body.appendChild(audio);
  } else if (type === 'image' && imageUrl) {
    const image = document.createElement('img');
    image.src = imageUrl;
    image.alt = 'Imagem enviada no chat';
    image.loading = 'lazy';
    image.className = 'message-image';
    image.tabIndex = 0;
    image.role = 'button';
    image.addEventListener('click', () => openImageLightbox(imageUrl));
    image.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        openImageLightbox(imageUrl);
      }
    });
    body.appendChild(image);
  } else if (type === 'image' && !imageUrl) {
    const missingUrlNode = document.createElement('div');
    missingUrlNode.className = 'message-text';
    missingUrlNode.textContent = 'anexo sem URL';
    body.appendChild(missingUrlNode);
  } else if (type === 'file' && fileUrl) {
    const link = document.createElement('a');
    link.href = fileUrl;
    link.target = '_blank';
    link.rel = 'noopener noreferrer';
    const fallbackName = fileName || 'Abrir anexo';
    const sizeLabel = typeof fileSize === 'number' && fileSize >= 0 ? ` (${Math.max(1, Math.round(fileSize / 1024))} KB)` : '';
    link.textContent = text || `${fallbackName}${sizeLabel}`;
    if (mimeType) link.type = mimeType;
    body.appendChild(link);
  }

  if (text && (type === 'text' || type === 'audio' || type === 'image' || type === 'file')) {
    const textNode = document.createElement('div');
    textNode.className = 'message-text';
    textNode.textContent = text;
    body.appendChild(textNode);
  }

  entry.appendChild(body);
  return entry;
};

const isNearBottom = (element) => {
  if (!element) return true;
  const { scrollTop, scrollHeight, clientHeight } = element;
  return scrollHeight - (scrollTop + clientHeight) <= 12;
};

const renderChatForSession = () => {
  scheduleRender(() => {
    if (!dom.chatThread) return;
    const container = dom.chatThread;
    const session = getSelectedSession();
    if (!session) {
      if (state.renderedChatSessionId !== null) {
        container.replaceChildren();
        state.renderedChatSessionId = null;
        container.appendChild(
          createChatEntryElement({
            author: 'Sistema',
            text: 'Selecione uma sessão para conversar com o cliente.',
            kind: 'system',
          })
        );
      }
      return;
    }

    if (state.renderedChatSessionId === session.sessionId) return;

    const history = state.chatBySession.get(session.sessionId) || [];
    const messages = history.slice(-CHAT_RENDER_LIMIT);
    const fragment = document.createDocumentFragment();
    const techName = getTechDataset().techName || 'Você';
    if (!messages.length) {
      fragment.appendChild(
        createChatEntryElement({
          author: 'Sistema',
          text: 'Sem mensagens trocadas ainda nesta sessão.',
          kind: 'system',
        })
      );
    } else {
      messages.forEach((msg) => {
        const isTech = msg.from === 'tech';
        fragment.appendChild(
          createChatEntryElement({
            author: isTech ? techName : session.clientName || msg.from,
            text: msg.text,
            type: msg.type,
            audioUrl: msg.audioUrl,
            imageUrl: msg.imageUrl,
            fileUrl: msg.fileUrl,
            fileName: msg.fileName,
            mimeType: msg.mimeType,
            fileSize: msg.fileSize,
            kind: isTech ? 'self' : 'client',
            ts: msg.ts,
          })
        );
      });
    }
    container.replaceChildren(fragment);
    state.renderedChatSessionId = session.sessionId;
    requestAnimationFrame(() => {
      container.scrollTop = container.scrollHeight;
    });
  });
};

const joinSelectedSession = () => {
  if (!socket) return;
  const session = getSelectedSession();
  if (!session || session.status !== 'active') return;
  const sessionId = session.sessionId;
  if (!sessionId) return;
  if (sessionJoinInFlightId === sessionId) return;
  if (state.joinedSessionId && state.joinedSessionId !== sessionId) {
    cleanupSession({ rebindHandlers: true });
  }
  if (state.joinedSessionId === sessionId) return;
  sessionJoinInFlightId = sessionId;
  socket.emit('session:join', { sessionId, role: 'tech', userType: 'tech' }, (ack) => {
    if (sessionJoinInFlightId === sessionId) {
      sessionJoinInFlightId = null;
    }
    if (ack?.ok) {
      const wasAlreadyJoined = state.joinedSessionId === sessionId;
      markSessionActive(sessionId);
      state.joinedSessionId = sessionId;
      state.media.sessionId = sessionId;
      if (!wasAlreadyJoined) {
        addChatMessage({
          author: 'Sistema',
          text: `Entrou na sala da sessão ${sessionId}.`,
          kind: 'system',
        });
      }
      renderChatForSession();
    } else {
      addChatMessage({
        author: 'Sistema',
        text: `Falha ao entrar na sessão ${sessionId}: ${ack?.err || 'erro desconhecido'}.`,
        kind: 'system',
      });
    }
  });
};

const syncWebRtcForSelectedSession = () => {
  const session = getSelectedSession();
  if (!session || session.status !== 'active') {
    if (state.media.eventsUnsub) {
      try {
        state.media.eventsUnsub();
      } catch (error) {
        console.warn('Falha ao cancelar listener WebRTC da sessão', error);
      }
    }
    state.media.eventsUnsub = null;
    state.media.eventsRef = null;
    state.media.eventsSessionId = null;
    state.media.eventsStartedAtMs = 0;
    state.media.processedEventIds = new Set();
    state.media.pendingRemoteIce = [];
    return;
  }
  void ensureWebRtcEventListener(session.sessionId);
};

const setChatMediaStatus = (text = '') => {
  if (!dom.chatMediaStatus) return;
  dom.chatMediaStatus.textContent = text;
};

const formatFirebaseError = (error, fallbackMessage = 'Falha no upload.') => {
  if (!error) return fallbackMessage;
  const code = typeof error.code === 'string' && error.code.trim() ? error.code.trim() : null;
  const message = typeof error.message === 'string' && error.message.trim() ? error.message.trim() : null;
  if (code && message) return `${fallbackMessage} (${code}: ${message})`;
  if (code) return `${fallbackMessage} (${code})`;
  if (message) return `${fallbackMessage} (${message})`;
  return fallbackMessage;
};

const setUploadProgress = (value = null) => {
  if (!dom.chatUploadProgress) return;
  if (typeof value !== 'number' || Number.isNaN(value)) {
    dom.chatUploadProgress.hidden = true;
    dom.chatUploadProgress.value = 0;
    return;
  }
  dom.chatUploadProgress.hidden = false;
  dom.chatUploadProgress.value = Math.max(0, Math.min(100, value));
};

const generateMessageId = () => {
  const now = Date.now();
  return typeof crypto !== 'undefined' && crypto.randomUUID ? crypto.randomUUID() : `${now}-${Math.random()}`;
};

const persistChatMessage = async (sessionId, payload) => {
  try {
    await ensureAuth();
    const db = ensureFirestore();
    if (!db) return;
    const messageRef = doc(db, 'sessions', sessionId, 'messages', payload.id);
    await setDoc(
      messageRef,
      {
        ...payload,
        sessionId,
        createdAt: serverTimestamp(),
      },
      { merge: true }
    );
  } catch (error) {
    console.warn('Falha ao persistir mensagem no Firestore', error);
  }
};

const normalizeMimeType = (value, fallback = 'application/octet-stream') => {
  if (typeof value !== 'string') return fallback;
  const normalized = value.split(';')[0].trim().toLowerCase();
  return normalized || fallback;
};

const detectExtensionFromMimeType = (mimeType, fallback = 'bin') => {
  const normalized = normalizeMimeType(mimeType, '');
  const map = {
    'image/jpeg': 'jpg',
    'image/jpg': 'jpg',
    'image/png': 'png',
    'image/webp': 'webp',
    'image/gif': 'gif',
    'image/heic': 'heic',
    'image/heif': 'heif',
    'audio/webm': 'webm',
    'video/webm': 'webm',
    'audio/mp4': 'm4a',
    'audio/aac': 'aac',
    'audio/mpeg': 'mp3',
    'audio/ogg': 'ogg',
    'audio/wav': 'wav',
    'audio/x-wav': 'wav',
  };
  return map[normalized] || fallback;
};

const sanitizeUploadFileName = (value, fallback = 'upload.bin') => {
  const source = typeof value === 'string' && value.trim() ? value.trim() : fallback;
  const cleaned = source.replace(/[^a-zA-Z0-9_.-]/g, '_').slice(-120);
  return cleaned || fallback;
};

const parseJsonSafely = async (response) => response.json().catch(() => ({}));

const createUploadError = (status, payload, fallbackMessage) => {
  const parts = [];
  const message = typeof payload?.error === 'string' && payload.error.trim()
    ? payload.error.trim()
    : typeof payload?.message === 'string' && payload.message.trim()
      ? payload.message.trim()
      : '';
  if (message) parts.push(message);
  if (status) parts.push(`HTTP ${status}`);
  const error = new Error(parts.length ? parts.join(' | ') : fallbackMessage);
  error.code = payload?.code || payload?.error || 'upload_failed';
  error.status = status || null;
  error.payload = payload || null;
  return error;
};

const uploadBlobViaBackend = async (
  sessionId,
  messageId,
  blob,
  {
    endpoint = '/api/upload/session-attachment',
    fileName = `${messageId}.bin`,
    contentType = 'application/octet-stream',
    fallbackError = 'Falha no upload.',
  } = {}
) => {
  await ensureAuth();
  const token = await getIdToken(false);
  const normalizedType = normalizeMimeType(contentType);
  const finalName = sanitizeUploadFileName(fileName, `${messageId}.${detectExtensionFromMimeType(normalizedType, 'bin')}`);
  const body = new FormData();
  body.append('sessionId', sessionId);
  body.append('messageId', messageId);
  body.append('file', blob, finalName);

  setUploadProgress(15);
  setChatMediaStatus('Enviando arquivo com validacao segura...');

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}` },
    body,
  });
  const payload = await parseJsonSafely(response);
  if (!response.ok) {
    throw createUploadError(response.status, payload, fallbackError);
  }

  const upload = payload?.upload || payload || {};
  const downloadURL = upload.downloadURL || upload.downloadUrl || upload.url || null;
  if (!downloadURL) {
    throw createUploadError(response.status, payload, 'Upload concluido sem URL retornada.');
  }
  setUploadProgress(100);
  return {
    ...upload,
    downloadURL,
    contentType: normalizeMimeType(upload.contentType || normalizedType),
    size: Number(upload.size) || blob?.size || 0,
  };
};
const sendChatPayload = (payload, { clearInput = false } = {}) => {
  const session = getSelectedSession();
  if (!session) {
    addChatMessage({ author: 'Sistema', text: 'Nenhuma sessão selecionada.', kind: 'system' });
    return;
  }
  if (!socket || socket.disconnected) {
    addChatMessage({ author: 'Sistema', text: 'Sem conexão com o servidor.', kind: 'system' });
    return;
  }
  const mergedPayload = {
    sessionId: session.sessionId,
    from: 'tech',
    ts: Date.now(),
    id: generateMessageId(),
    ...payload,
  };
  socket.emit('session:chat:send', mergedPayload, (ack) => {
    if (ack?.ok) {
      if (clearInput && dom.chatInput) dom.chatInput.value = '';
      setChatMediaStatus('');
    } else {
      addChatMessage({
        author: 'Sistema',
        text: ack?.err ? `Não foi possível enviar a mensagem: ${ack.err}` : 'Não foi possível enviar a mensagem.',
        kind: 'system',
      });
    }
  });
  void persistChatMessage(session.sessionId, mergedPayload);
};

const sendChatMessage = (text) => {
  sendChatPayload({ type: 'text', text }, { clearInput: true });
};

const sendAttachmentMessage = async (file) => {
  if (!file) return;
  const session = getSelectedSession();
  if (!session) {
    addChatMessage({ author: 'Sistema', text: 'Nenhuma sessão selecionada.', kind: 'system' });
    return;
  }
  state.chatComposer.uploading = true;
  const messageId = generateMessageId();
  setChatMediaStatus('Fazendo upload… 0%');
  try {
    const isImage = typeof file.type === 'string' && file.type.startsWith('image/');
    const contentType = normalizeMimeType(file.type || 'application/octet-stream');
    const upload = await uploadBlobViaBackend(session.sessionId, messageId, file, {
      endpoint: '/api/upload/session-attachment',
      fileName: sanitizeUploadFileName(file.name || `attachment-${messageId}.${detectExtensionFromMimeType(contentType, 'bin')}`),
      contentType,
      fallbackError: 'Falha ao enviar anexo.',
    });
    const url = upload.downloadURL;
    const finalType = normalizeMimeType(upload.contentType || contentType);
    const finalSize = Number(upload.size) || file.size;
    if (isImage) {
      sendChatPayload({
        id: messageId,
        type: 'image',
        imageUrl: url,
        text: file.name || 'Imagem',
        fileName: file.name || 'imagem',
        contentType: finalType,
        size: finalSize,
      });
    } else {
      sendChatPayload({
        id: messageId,
        type: 'file',
        fileUrl: url,
        text: file.name || 'Arquivo',
        fileName: file.name || 'arquivo',
        contentType: finalType,
        mimeType: finalType,
        fileSize: finalSize,
        size: finalSize,
      });
    }
  } catch (error) {
    console.error('Falha no upload de anexo via backend', {
      error,
      sessionId: session.sessionId,
      fileName: file?.name || null,
      contentType: file?.type || null,
      size: file?.size || null,
    });
    addChatMessage({
      author: 'Sistema',
      text: formatFirebaseError(error, 'Falha ao enviar anexo.'),
      kind: 'system',
    });
    setChatMediaStatus(formatFirebaseError(error, 'Falha ao enviar anexo.'));
  } finally {
    state.chatComposer.uploading = false;
    setUploadProgress(null);
  }
};

const stopChatAudioCapture = () => {
  if (state.chatComposer.timerId) {
    clearInterval(state.chatComposer.timerId);
    state.chatComposer.timerId = null;
  }
  if (state.chatComposer.stream) {
    state.chatComposer.stream.getTracks().forEach((track) => track.stop());
  }
  state.chatComposer.stream = null;
  state.chatComposer.recorder = null;
  state.chatComposer.recording = false;
  state.chatComposer.chunks = [];
  state.chatComposer.startedAt = 0;
  if (dom.chatAudioBtn) dom.chatAudioBtn.classList.remove('recording');
};

const toggleAudioRecording = async () => {
  if (state.chatComposer.recording && state.chatComposer.recorder) {
    state.chatComposer.recorder.stop();
    return;
  }
  if (!navigator.mediaDevices?.getUserMedia || typeof MediaRecorder === 'undefined') {
    addChatMessage({ author: 'Sistema', text: 'Gravação de áudio não suportada neste navegador.', kind: 'system' });
    return;
  }
  try {
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    const recorder = new MediaRecorder(stream);
    state.chatComposer.stream = stream;
    state.chatComposer.recorder = recorder;
    state.chatComposer.chunks = [];
    state.chatComposer.startedAt = Date.now();
    state.chatComposer.recording = true;
    if (dom.chatAudioBtn) dom.chatAudioBtn.classList.add('recording');
    setChatMediaStatus('Gravando… 00:00');
    state.chatComposer.timerId = setInterval(() => {
      const elapsedSec = Math.floor((Date.now() - state.chatComposer.startedAt) / 1000);
      const mm = String(Math.floor(elapsedSec / 60)).padStart(2, '0');
      const ss = String(elapsedSec % 60).padStart(2, '0');
      setChatMediaStatus(`Gravando… ${mm}:${ss}`);
    }, 500);
    recorder.ondataavailable = (event) => {
      if (event.data?.size) state.chatComposer.chunks.push(event.data);
    };
    recorder.onstop = async () => {
      const blob = new Blob(state.chatComposer.chunks, { type: recorder.mimeType || 'audio/webm' });
      stopChatAudioCapture();
      const session = getSelectedSession();
      if (!session) return;
      const messageId = generateMessageId();
      state.chatComposer.uploading = true;
      setChatMediaStatus('Fazendo upload… 0%');
      try {
        const contentType = normalizeMimeType(blob.type || recorder.mimeType || 'audio/webm');
        const fileName = `audio-${messageId}.${detectExtensionFromMimeType(contentType, 'webm')}`;
        const upload = await uploadBlobViaBackend(session.sessionId, messageId, blob, {
          endpoint: '/api/upload/session-audio',
          fileName,
          contentType,
          fallbackError: 'Falha ao enviar áudio.',
        });
        const url = upload.downloadURL;
        const finalType = normalizeMimeType(upload.contentType || contentType);
        const finalSize = Number(upload.size) || blob.size;
        sendChatPayload({
          id: messageId,
          type: 'audio',
          audioUrl: url,
          text: 'Áudio',
          fileName,
          contentType: finalType,
          size: finalSize,
        });
      } catch (error) {
        console.error('Falha no envio de áudio via backend', {
          error,
          sessionId: session.sessionId,
          contentType: blob?.type || recorder?.mimeType || null,
          size: blob?.size || null,
        });
        addChatMessage({
          author: 'Sistema',
          text: formatFirebaseError(error, 'Falha ao enviar áudio.'),
          kind: 'system',
        });
        setChatMediaStatus(formatFirebaseError(error, 'Falha ao enviar áudio.'));
      } finally {
        state.chatComposer.uploading = false;
        setUploadProgress(null);
      }
    };
    recorder.start();
  } catch (error) {
    console.error('Falha ao iniciar gravação', error);
    addChatMessage({ author: 'Sistema', text: 'Não foi possível iniciar a gravação.', kind: 'system' });
  }
};

const sendSessionCommand = (type, extra = {}, { silent = false, sessionId: overrideSessionId = null } = {}) => {
  const session = overrideSessionId
    ? state.sessions.find((s) => s.sessionId === overrideSessionId) || null
    : getSelectedSession();
  if (!session) {
    if (!silent) {
      addChatMessage({ author: 'Sistema', text: 'Nenhuma sessão selecionada para enviar comandos.', kind: 'system' });
    }
    return Promise.reject(new Error('no-session'));
  }
  if (!socket || socket.disconnected) {
    if (!silent) {
      addChatMessage({ author: 'Sistema', text: 'Sem conexão com o servidor.', kind: 'system' });
    }
    return Promise.reject(new Error('no-connection'));
  }
  return new Promise((resolve, reject) => {
    socket.emit('session:command', { sessionId: session.sessionId, type, ...extra }, (ack) => {
      if (ack?.ok) {
        resolve({ session, ack });
      } else {
        if (!silent) {
          addChatMessage({
            author: 'Sistema',
            text: ack?.err ? `Falha ao enviar comando (${ack.err}).` : 'Falha ao enviar comando.',
            kind: 'system',
          });
        }
        reject(new Error(ack?.err || 'command-error'));
      }
    });
  });
};

const emitSessionCommand = (type, extra = {}, onSuccess) => {
  sendSessionCommand(type, extra)
    .then(({ session }) => {
      const command = {
        sessionId: session.sessionId,
        type,
        data: extra?.data || null,
        by: 'tech',
        ts: Date.now(),
      };
      registerCommand(command, { local: true });
      if (typeof onSuccess === 'function') onSuccess();
    })
    .catch(() => {});
};

function handleSessionEnded(sessionId, reason = 'peer_ended') {
  if (!sessionId) return;
  if (state.media.sessionId === sessionId) {
    teardownPeerConnection();
    resetCommandState();
  }
  if (state.call.sessionId === sessionId && state.call.status !== CallStates.IDLE) {
    void endActiveCall({ reason: 'session_end' });
  }
  if (state.joinedSessionId === sessionId) {
    state.joinedSessionId = null;
  }
  const ts = Date.now();
  const index = state.sessions.findIndex((s) => s.sessionId === sessionId);
  if (index >= 0) {
    const session = state.sessions[index];
    const updated = {
      ...session,
      status: 'closed',
      closedAt: session.closedAt || ts,
    };
    state.sessions[index] = updated;
  }
  if (state.selectedSessionId === sessionId) {
    let text = 'Sessão encerrada.';
    if (reason === 'peer_ended') text = 'O cliente encerrou a sessão.';
    if (reason === 'tech_ended') text = 'Você encerrou a sessão.';
    addChatMessage({
      author: 'Sistema',
      text,
      kind: 'system',
    });
  }
  renderSessions();
}

function resetDashboard({ sessionId = null, reason = 'peer_ended' } = {}) {
  const targetSessionId =
    sessionId ||
    state.activeSessionId ||
    state.joinedSessionId ||
    state.selectedSessionId ||
    null;

  if (state.call.sessionId && state.call.status !== CallStates.IDLE) {
    void endActiveCall({ reason: 'session_reset' });
  }

  cleanupSession({ rebindHandlers: true });
  unsubscribeAllSessionRealtime();

  if (targetSessionId) {
    state.chatBySession.delete(targetSessionId);
    state.telemetryBySession.delete(targetSessionId);
  }

  state.selectedSessionId = null;

  renderSessions();
  renderChatForSession();
  updateMediaDisplay();

  if (dom.closureForm) {
    dom.closureForm.reset();
  }

  scheduleRender(() => {
    if (dom.chatThread) {
      const message =
        reason === 'tech_ended'
          ? 'Atendimento encerrado. Painel pronto para o próximo atendimento.'
          : 'Painel pronto para o próximo atendimento.';
      dom.chatThread.replaceChildren(
        createChatEntryElement({
          author: 'Sistema',
          text: message,
          kind: 'system',
        })
      );
    }
  });

  setSessionState(SessionStates.IDLE, null);

  loadQueue();
  Promise.all([loadSessions(), loadMetrics()]).catch((error) => {
    console.warn('Falha ao atualizar dados após reset', error);
  });
}

function handleCommandEffects(command, { local = false } = {}) {
  if (!command) return;
  const by = command.by || (local ? 'tech' : 'unknown');
  switch (command.type) {
    case 'share_start':
      state.commandState.shareActive = true;
      if (dom.controlStart) dom.controlStart.textContent = 'Encerrar visualização';
      break;
    case 'share_stop':
      state.commandState.shareActive = false;
      if (dom.controlStart) dom.controlStart.textContent = 'Solicitar visualização';
      resetMediaPeerConnection(command.sessionId || state.media.sessionId || null);
      break;
    case 'remote_enable':
      state.commandState.remoteActive = true;
      if (dom.controlRemote) dom.controlRemote.textContent = 'Revogar acesso remoto';
      dom.sessionVideo?.focus({ preventScroll: true });
      break;
    case 'remote_disable':
      state.commandState.remoteActive = false;
      if (dom.controlRemote) dom.controlRemote.textContent = 'Solicitar acesso remoto';
      resetRemoteControlChannel();
      break;
    case 'call_start':
      if (state.call.status !== CallStates.IDLE) {
        logCall('CALL comando legacy ignorado (chamada ativa).');
        break;
      }
      state.commandState.callActive = true;
      if (dom.controlQuality) dom.controlQuality.textContent = 'Encerrar chamada';
      if (by !== 'tech') {
        startLocalCall();
      }
      break;
    case 'call_end':
      if (state.call.status !== CallStates.IDLE) {
        void endActiveCall({ reason: 'legacy_end' });
        break;
      }
      state.commandState.callActive = false;
      if (dom.controlQuality) dom.controlQuality.textContent = 'Iniciar chamada';
      stopLocalCall(false);
      clearRemoteAudio();
      break;
    case 'session_end':
      handleSessionEnded(command.sessionId, command.reason || 'peer_ended');
      markSessionEnded(command.sessionId, command.reason || 'peer_ended');
      break;
    default:
      break;
  }
}

function registerCommand(command, { local = false } = {}) {
  if (!command || !command.sessionId) return;
  const normalized = {
    ...command,
    ts: command.ts || Date.now(),
    id: command.id || `${command.sessionId}-${command.ts || Date.now()}`,
    by: command.by || (local ? 'tech' : 'unknown'),
  };
  if (normalized.type === 'session_end' && local) {
    normalized.reason = normalized.reason || 'tech_ended';
  }
  const index = state.sessions.findIndex((s) => s.sessionId === normalized.sessionId);
  if (index >= 0) {
    const session = state.sessions[index];
    const log = Array.isArray(session.commandLog) ? [...session.commandLog] : [];
    const exists = log.some((entry) => entry.id === normalized.id);
    if (!exists) log.push(normalized);
    const extra = { ...(session.extra || {}), commandLog: log, lastCommand: normalized };
    state.sessions[index] = { ...session, commandLog: log, extra };
  }
  if (state.selectedSessionId === normalized.sessionId) {
    addChatMessage({
      author: 'Sistema',
      text: `Comando ${normalized.type} executado por ${normalized.by || 'desconhecido'}.`,
      kind: 'system',
      ts: normalized.ts,
    });
  }
  handleCommandEffects(normalized, { local });
}

const bindSessionControls = () => {
  if (dom.controlStart) {
    dom.controlStart.addEventListener('click', () => {
      const nextType = state.commandState.shareActive ? 'share_stop' : 'share_start';
      emitSessionCommand(nextType);
    });
  }

  if (dom.controlRemote) {
    dom.controlRemote.addEventListener('click', () => {
      const nextType = state.commandState.remoteActive ? 'remote_disable' : 'remote_enable';
      emitSessionCommand(nextType, {}, () => {
        if (nextType === 'remote_enable') {
          const session = getSelectedSession();
          if (!session) return;
          void renegotiateRemoteControl(session.sessionId);
        }
      });
    });
  }

  if (dom.controlQuality) {
    dom.controlQuality.addEventListener('click', () => {
      if (state.call.status === CallStates.IDLE) {
        void startOutgoingCall();
        return;
      }
      if (state.call.status === CallStates.INCOMING_RINGING) {
        showToast('Chamada recebida. Use o modal para aceitar ou recusar.');
        return;
      }
      void endActiveCall({ reason: 'hangup' });
    });
  }

  if (dom.controlStats) {
    dom.controlStats.addEventListener('click', () => {
      emitSessionCommand('session_end');
    });
  }
};

const bindCallModalControls = () => {
  if (dom.callModalAccept) {
    dom.callModalAccept.addEventListener('click', () => {
      void acceptIncomingCall();
    });
  }
  if (dom.callModalDecline) {
    dom.callModalDecline.addEventListener('click', () => {
      void declineIncomingCall();
    });
  }
  if (dom.callModalHangup) {
    dom.callModalHangup.addEventListener('click', () => {
      void endActiveCall({ reason: 'hangup' });
    });
  }
  if (dom.callModalMute) {
    dom.callModalMute.addEventListener('click', () => {
      toggleCallMute();
    });
  }
};

const bindControlMenu = () => {
  if (!dom.controlMenuToggle || !dom.videoShell) return;
  dom.controlMenuToggle.addEventListener('click', toggleControlMenu);
  dom.controlMenuBackdrop?.addEventListener('click', () => setControlMenuOpen(false));
  dom.controlMenuPanel?.addEventListener('click', (event) => {
    if (event.target instanceof HTMLButtonElement) {
      setControlMenuOpen(false);
    }
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      setControlMenuOpen(false);
    }
  });
};

const bindViewControls = () => {
  if (dom.controlFullscreen) {
    if (!document.fullscreenEnabled) {
      dom.controlFullscreen.hidden = true;
    } else {
      updateFullscreenLabel();
      dom.controlFullscreen.addEventListener('click', async () => {
        if (!hasActiveVideo()) {
          showToast('Nenhuma visualização ativa.');
          return;
        }
        try {
          if (document.fullscreenElement) {
            await document.exitFullscreen();
          } else {
            await dom.sessionVideo.requestFullscreen();
          }
        } catch (error) {
          console.error('Falha ao alternar tela cheia', error);
          showToast('Não foi possível abrir a tela cheia.');
        }
      });
      document.addEventListener('fullscreenchange', updateFullscreenLabel);
    }
  }

  if (dom.controlPip) {
    if (!document.pictureInPictureEnabled || typeof dom.sessionVideo?.requestPictureInPicture !== 'function') {
      dom.controlPip.hidden = true;
    } else {
      updatePipLabel();
      dom.controlPip.addEventListener('click', async () => {
        if (!hasActiveVideo()) {
          showToast('Nenhuma visualização ativa.');
          return;
        }
        try {
          if (document.pictureInPictureElement) {
            await document.exitPictureInPicture();
          } else {
            await dom.sessionVideo.requestPictureInPicture();
          }
        } catch (error) {
          console.error('Falha ao abrir picture-in-picture', error);
          showToast('Não foi possível abrir a janela flutuante.');
        }
      });
      dom.sessionVideo?.addEventListener('enterpictureinpicture', updatePipLabel);
      dom.sessionVideo?.addEventListener('leavepictureinpicture', updatePipLabel);
    }
  }
};

const bindRemoteControlEvents = () => {
  if (!dom.sessionVideo) return;
  const videoEl = dom.sessionVideo;
  const captureTargets = [videoEl].filter(Boolean);
  if (!videoEl.hasAttribute('tabindex')) {
    videoEl.tabIndex = 0;
  }

  let lastToastAt = 0;
  const warnControlUnavailable = () => {
    const now = Date.now();
    if (now - lastToastAt < 2000) return;
    lastToastAt = now;
    showToast('Controle remoto indisponível. Aguarde o cliente autorizar.');
  };

  const canSendPointer = () => {
    if (!state.commandState.remoteActive) return false;
    if (!hasActiveVideo()) return false;
    if (!canSendControlCommand()) {
      warnControlUnavailable();
      return false;
    }
    return true;
  };

  const pointerState = {
    active: false,
    pointerId: null,
    lastMoveAt: 0,
    pendingMove: null,
    moveTimer: null,
    lastCoords: null,
  };

  const textState = {
    buffer: '',
    debounceTimer: null,
  };

  const resetPointer = () => {
    pointerState.active = false;
    try {
      if (pointerState.pointerId != null) {
        videoEl.releasePointerCapture(pointerState.pointerId);
      }
    } catch (_) {}
    pointerState.pointerId = null;
    pointerState.lastMoveAt = 0;
    pointerState.pendingMove = null;
    pointerState.lastCoords = null;
    if (pointerState.moveTimer) {
      clearTimeout(pointerState.moveTimer);
      pointerState.moveTimer = null;
    }
  };

  const focusRemoteInput = () => {
    if (dom.remoteTextInput) {
      dom.remoteTextInput.focus({ preventScroll: true });
    } else {
      videoEl.focus({ preventScroll: true });
    }
  };

  const focusRemoteVideo = () => {
    videoEl.focus({ preventScroll: true });
  };

  const isEditableTarget = (target) => {
    if (!target || !(target instanceof HTMLElement)) return false;
    const tag = target.tagName.toLowerCase();
    return tag === 'input' || tag === 'textarea' || target.isContentEditable;
  };

  const shouldBlockKey = (event) => {
    const blockedKeys = new Set([
      'ArrowUp',
      'ArrowDown',
      'ArrowLeft',
      'ArrowRight',
      ' ',
      'PageUp',
      'PageDown',
      'Home',
      'End',
      'Backspace',
      'Enter',
    ]);
    return blockedKeys.has(event.key);
  };

  const handleRemoteKeyGuard = (event) => {
    if (!state.commandState.remoteActive) return;
    if (isEditableTarget(event.target)) return;
    if (event.target === videoEl || event.target === dom.remoteTextInput) return;
    if (!shouldBlockKey(event)) return;
    event.preventDefault();
    event.stopPropagation();
  };

  document.addEventListener('keydown', handleRemoteKeyGuard, true);

  const scheduleTextSend = () => {
    if (textState.debounceTimer) {
      clearTimeout(textState.debounceTimer);
    }
    textState.debounceTimer = setTimeout(() => {
      if (!state.commandState.remoteActive) return;
      if (!canSendControlCommand()) {
        warnControlUnavailable();
        return;
      }
      sendCtrlCommand({ t: 'set_text', text: textState.buffer, append: false });
    }, TEXT_SEND_DEBOUNCE_MS);
  };

  const resetTextBuffer = ({ sendClear = false } = {}) => {
    if (textState.debounceTimer) {
      clearTimeout(textState.debounceTimer);
      textState.debounceTimer = null;
    }
    textState.buffer = '';
    if (dom.remoteTextInput) {
      dom.remoteTextInput.value = '';
    }
    if (!sendClear) return;
    if (!state.commandState.remoteActive) return;
    if (!canSendControlCommand()) {
      warnControlUnavailable();
      return;
    }
    sendCtrlCommand({ t: 'set_text', text: '', append: false });
  };

  const updateTextBuffer = (value) => {
    textState.buffer = value;
    if (dom.remoteTextInput && dom.remoteTextInput.value !== value) {
      dom.remoteTextInput.value = value;
    }
    scheduleTextSend();
  };

  const flushPointerMove = () => {
    pointerState.moveTimer = null;
    if (!pointerState.active || !pointerState.pendingMove) return;
    if (!canSendPointer()) {
      resetPointer();
      return;
    }
    sendCtrlCommand({
      t: 'pointer_move',
      x: pointerState.pendingMove.x,
      y: pointerState.pendingMove.y,
    });
    pointerState.lastMoveAt = performance.now();
    pointerState.pendingMove = null;
  };

  const onPointerDown = (event) => {
    if (event.button !== 0 && event.pointerType === 'mouse') return;
    if (!canSendPointer()) return;
    event.preventDefault();
    event.stopPropagation();
    focusRemoteInput();
    pointerState.active = true;
    pointerState.pointerId = event.pointerId;
    pointerState.lastMoveAt = 0;
    pointerState.pendingMove = null;
    const coords = getNormalizedXY(videoEl, event);
    if (!coords.inBounds) {
      resetPointer();
      return;
    }
    pointerState.lastCoords = coords;
    sendCtrlCommand({
      t: 'pointer_move',
      x: coords.x,
      y: coords.y,
    });
    sendCtrlCommand({
      t: 'pointer_down',
      x: coords.x,
      y: coords.y,
    });
    focusRemoteVideo();
    try {
      videoEl.setPointerCapture(event.pointerId);
    } catch (_error) {
      // ignore capture failures
    }
  };

  const onPointerMove = (event) => {
    if (!pointerState.active || pointerState.pointerId !== event.pointerId) return;
    if (!canSendPointer()) return;
    event.preventDefault();
    event.stopPropagation();
    const coords = getNormalizedXY(videoEl, event);
    if (!coords.inBounds) {
      return;
    }
    pointerState.lastCoords = coords;
    const now = performance.now();
    const elapsed = now - pointerState.lastMoveAt;
    if (elapsed >= POINTER_MOVE_THROTTLE_MS) {
      if (pointerState.moveTimer) {
        clearTimeout(pointerState.moveTimer);
        pointerState.moveTimer = null;
      }
      pointerState.pendingMove = null;
      sendCtrlCommand({
        t: 'pointer_move',
        x: coords.x,
        y: coords.y,
      });
      pointerState.lastMoveAt = now;
    } else {
      pointerState.pendingMove = coords;
      if (!pointerState.moveTimer) {
        pointerState.moveTimer = setTimeout(flushPointerMove, POINTER_MOVE_THROTTLE_MS - elapsed);
      }
    }
  };

  const onPointerUp = (event) => {
    if (!pointerState.active || pointerState.pointerId !== event.pointerId) return;
    if (!canSendPointer()) {
      resetPointer();
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    const coords = getNormalizedXY(videoEl, event);
    const finalCoords = coords.inBounds ? coords : pointerState.lastCoords;
    if (pointerState.moveTimer) {
      clearTimeout(pointerState.moveTimer);
      pointerState.moveTimer = null;
    }
    pointerState.pendingMove = null;
    if (finalCoords) {
      sendCtrlCommand({
        t: 'pointer_move',
        x: finalCoords.x,
        y: finalCoords.y,
      });
      sendCtrlCommand({
        t: 'pointer_up',
        x: finalCoords.x,
        y: finalCoords.y,
      });
    }
    resetPointer();
  };

  captureTargets.forEach((target) => {
    target.addEventListener('pointerdown', onPointerDown, { passive: false, capture: true });
    target.addEventListener('pointermove', onPointerMove, { passive: false, capture: true });
    target.addEventListener('pointerup', onPointerUp, { passive: false, capture: true });
    target.addEventListener('pointercancel', onPointerUp, { passive: false, capture: true });
  });
  window.addEventListener('pointerup', onPointerUp, { passive: false, capture: true });
  window.addEventListener('pointercancel', onPointerUp, { passive: false, capture: true });

  const handleSpecialKey = (event) => {
    if (!state.commandState.remoteActive) return false;
    if (!canSendControlCommand()) {
      warnControlUnavailable();
      event.preventDefault();
      return true;
    }
    const { key } = event;
    if (key === 'Escape' || key === 'BrowserBack') {
      event.preventDefault();
      sendCtrlCommand({ t: 'back' });
      return true;
    }
    const navigationKeys = new Set(['Tab', 'ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight']);
    if (navigationKeys.has(key)) {
      event.preventDefault();
      sendCtrlCommand({ t: 'key', key, shift: event.shiftKey });
      return true;
    }
    return false;
  };

  if (dom.remoteTextInput) {
    dom.remoteTextInput.addEventListener('input', () => {
      if (!state.commandState.remoteActive) return;
      if (!canSendControlCommand()) {
        warnControlUnavailable();
        return;
      }
      updateTextBuffer(dom.remoteTextInput.value);
    });
    dom.remoteTextInput.addEventListener('keydown', (event) => {
      handleSpecialKey(event);
    });
  }

  videoEl.addEventListener('keydown', (event) => {
    const handled = handleSpecialKey(event);
    if (handled) return;
    if (!state.commandState.remoteActive) return;
    if (!canSendControlCommand()) {
      warnControlUnavailable();
      return;
    }
    const { key } = event;
    const isPrintable = key.length === 1 && !event.ctrlKey && !event.metaKey && !event.altKey;
    if (isPrintable) {
      event.preventDefault();
      updateTextBuffer(`${textState.buffer}${key}`);
      return;
    }
    if (key === 'Enter') {
      event.preventDefault();
      if (event.shiftKey) {
        sendCtrlCommand({ t: 'key', key: 'Enter', shift: true });
        updateTextBuffer(`${textState.buffer}\n`);
      } else {
        sendCtrlCommand({ t: 'key', key: 'Enter', shift: false });
        resetTextBuffer();
      }
      return;
    }
    if (key === 'Backspace' || key === 'Delete') {
      event.preventDefault();
      updateTextBuffer(textState.buffer.slice(0, -1));
    }
  });
};

const formatTime = (timestamp) => {
  if (!timestamp) return '—';
  const date = new Date(timestamp);
  return `${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
};

const formatDateTime = (timestamp) => {
  if (!timestamp) return '—';
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return '—';
  return date.toLocaleString('pt-BR');
};

const formatDuration = (ms) => {
  if (typeof ms !== 'number' || Number.isNaN(ms) || ms < 0) return '—';
  const totalSeconds = Math.round(ms / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  const hours = Math.floor(minutes / 60);
  if (hours >= 1) {
    const remMinutes = minutes % 60;
    return `${String(hours).padStart(2, '0')}:${String(remMinutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
  }
  return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
};

const formatRelative = (ms) => {
  if (typeof ms !== 'number' || ms < 0) return 'agora';
  const minutes = Math.round(ms / 60000);
  if (minutes <= 1) return 'há instantes';
  if (minutes < 60) return `há ${minutes} min`;
  const hours = Math.round(minutes / 60);
  return `há ${hours} h`;
};

const computeInitials = (name) => {
  if (!name) return 'SX';
  const parts = name.trim().split(/\s+/);
  if (!parts.length) return 'SX';
  const first = parts[0][0] || '';
  const last = parts.length > 1 ? parts[parts.length - 1][0] : '';
  return `${first}${last}`.toUpperCase();
};

const formatBytesHuman = (bytes) => {
  const value = Number(bytes);
  if (!Number.isFinite(value) || value <= 0) return '—';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = value;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  const precision = size >= 10 || unit === 0 ? 0 : 1;
  return `${size.toFixed(precision)}${units[unit]}`;
};

const normalizePhone = (value) => {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const digits = raw.replace(/\D/g, '');
  if (digits.length < 10) return '';
  return `+${digits}`;
};

const normalizeTelemetryPayload = (value) => {
  const payload = value && typeof value === 'object' ? { ...value } : {};
  if (typeof payload.network === 'undefined' && typeof payload.net !== 'undefined') {
    payload.network = payload.net;
  }
  if (typeof payload.batteryLevel === 'undefined' && typeof payload.battery === 'number') {
    payload.batteryLevel = payload.battery;
  }
  return payload;
};

const formatNetworkLabel = (telemetry, session) => {
  const value = telemetry?.network || telemetry?.net || session?.extra?.network || '';
  if (!value) return session?.status === 'active' ? 'Aguardando dados do app' : 'Sessão encerrada';
  const normalized = String(value).trim().toLowerCase();
  if (normalized === 'wifi' || normalized === 'wi-fi') return 'Wi-Fi';
  if (normalized === 'cell' || normalized === 'cellular' || normalized === 'rede movel') return 'Rede móvel';
  if (normalized === 'ethernet') return 'Ethernet';
  if (normalized === 'bluetooth') return 'Bluetooth';
  if (normalized === 'offline') return 'Offline';
  return String(value);
};

const formatPermissionLabel = (telemetry, session) => {
  const value = telemetry?.permissions ?? session?.extra?.permissions;
  if (!value) return 'Sem registros';
  if (typeof value === 'string') return value;
  if (typeof value !== 'object') return 'Sem registros';
  const tags = [];
  if (typeof value.accessibilityEnabled === 'boolean') {
    tags.push(`Acessibilidade: ${value.accessibilityEnabled ? 'ok' : 'pendente'}`);
  }
  if (typeof value.microphoneGranted === 'boolean') {
    tags.push(`Microfone: ${value.microphoneGranted ? 'ok' : 'pendente'}`);
  }
  if (typeof value.overlayEnabled === 'boolean') {
    tags.push(`Sobreposição: ${value.overlayEnabled ? 'ok' : 'pendente'}`);
  }
  return tags.length ? tags.join(' • ') : 'Sem registros';
};

const deriveHealthLabel = (telemetry, session) => {
  const explicit = telemetry?.health || session?.extra?.health;
  if (explicit) return String(explicit);

  const battery = Number(telemetry?.batteryLevel);
  const freeBytes = Number(telemetry?.storageFreeBytes);
  const totalBytes = Number(telemetry?.storageTotalBytes);
  const temp = Number(telemetry?.temperatureC);
  const lowStorageRatio =
    Number.isFinite(freeBytes) && Number.isFinite(totalBytes) && totalBytes > 0
      ? freeBytes / totalBytes
      : null;

  if (
    (Number.isFinite(battery) && battery <= 10) ||
    (Number.isFinite(temp) && temp >= 43) ||
    (lowStorageRatio != null && lowStorageRatio <= 0.05)
  ) {
    return 'Crítico';
  }
  if (
    (Number.isFinite(battery) && battery <= 20) ||
    (Number.isFinite(temp) && temp >= 39) ||
    (lowStorageRatio != null && lowStorageRatio <= 0.12)
  ) {
    return 'Atenção';
  }
  if (
    Number.isFinite(battery) ||
    Number.isFinite(temp) ||
    lowStorageRatio != null
  ) {
    return 'Bom';
  }
  return session?.status === 'active' ? 'Aguardando dados do app' : 'Sessão encerrada';
};

const formatBatteryLabel = (telemetry) => {
  const battery = Number(telemetry?.batteryLevel);
  if (!Number.isFinite(battery)) return 'Aguardando dados do app';
  const charging = telemetry?.batteryCharging === true ? ' (Carregando)' : '';
  return `${Math.max(0, Math.min(100, Math.round(battery)))}%${charging}`;
};

const formatTemperatureLabel = (telemetry) => {
  const temp = Number(telemetry?.temperatureC);
  if (!Number.isFinite(temp)) return 'Aguardando dados do app';
  return `${temp.toFixed(1)}°C`;
};

const formatStorageLabel = (telemetry) => {
  const free = Number(telemetry?.storageFreeBytes);
  const total = Number(telemetry?.storageTotalBytes);
  if (!Number.isFinite(free) || !Number.isFinite(total) || total <= 0) {
    return 'Aguardando dados do app';
  }
  return `${formatBytesHuman(free)} de ${formatBytesHuman(total)}`;
};

const getSelectedSession = () => {
  if (!state.selectedSessionId) return null;
  return state.sessions.find((s) => s.sessionId === state.selectedSessionId) || null;
};

const selectSessionById = (sessionId) => {
  if (!sessionId || state.selectedSessionId === sessionId) return;
  state.selectedSessionId = sessionId;
  state.renderedChatSessionId = null;
  resetCommandState();
  if (state.media.sessionId && state.media.sessionId !== sessionId) {
    teardownPeerConnection();
  }
  renderSessions();
};

const selectDefaultSession = () => {
  const previous = state.selectedSessionId;
  if (previous) {
    const previousSession = state.sessions.find((s) => s.sessionId === previous) || null;
    if (previousSession && previousSession.status === 'active') return;
  }
  const active = state.sessions.find((s) => s.status === 'active');
  const chosen = active || null;
  state.selectedSessionId = chosen ? chosen.sessionId : null;
  if (previous !== state.selectedSessionId) {
    if (state.joinedSessionId === previous) {
      state.joinedSessionId = null;
    }
    if (sessionJoinInFlightId === previous) {
      sessionJoinInFlightId = null;
    }
    state.renderedChatSessionId = null;
    resetCommandState();
    if (state.media.sessionId && state.media.sessionId !== state.selectedSessionId) {
      teardownPeerConnection();
    }
  }
};

const normalizeQueueVerificationStatus = (value) =>
  ensureString(value || '', '').trim().toLowerCase();

const resolveQueueTicketTone = (request = {}, needsRegistration = false) => {
  const supportsUsed = Number(request.supportsUsed) || 0;
  const freeFirstSupportUsed = Boolean(request.freeFirstSupportUsed);
  const hasPreviousSupport = supportsUsed > 0 || freeFirstSupportUsed;
  const verificationStatus = normalizeQueueVerificationStatus(request.verificationStatus);
  const isVerified = verificationStatus === 'verified';

  if (needsRegistration && !hasPreviousSupport) return 'new';
  if (needsRegistration || !isVerified) return 'attention';
  return 'ready';
};

const renderQueue = () => {
  scheduleRender(() => {
    if (!dom.queue) return;
    const items = Array.isArray(state.queue) ? state.queue : [];
    const hasActiveSession = state.sessions.some((session) => session.status === 'active');
    if (!items.length) {
      dom.queue.replaceChildren();
      dom.queueEmpty?.removeAttribute('hidden');
      return;
    }

    dom.queueEmpty?.setAttribute('hidden', 'hidden');
    const fragment = document.createDocumentFragment();
    const now = Date.now();

    items.forEach((req) => {
      const article = document.createElement('article');
      const needsRegistration = Boolean(req.requiresTechnicianRegistration || !req.clientRegistered || !req.profileCompleted);
      const queueTone = resolveQueueTicketTone(req, needsRegistration);
      article.className = 'ticket';
      if (queueTone === 'new') article.classList.add('is-new-client');
      if (queueTone === 'attention') article.classList.add('is-attention-client');

      const header = document.createElement('div');
      header.className = 'ticket-header';
      const title = document.createElement('span');
      title.className = 'ticket-title';
      const displayName = req.clientName || 'Cliente';
      title.textContent = `#${req.requestId} • ${displayName}`;
      header.appendChild(title);
      const sla = document.createElement('span');
      sla.className = 'badge';
      const waitMs = now - req.createdAt;
      if (waitMs > 12 * 60000) sla.classList.add('danger');
      else if (waitMs > 5 * 60000) sla.classList.add('warning');
      else sla.classList.add('success');
      sla.textContent = `Espera ${formatRelative(waitMs)}`;
      header.appendChild(sla);
      article.appendChild(header);

      if (req.plan || req.issue) {
        const body = document.createElement('div');
        body.className = 'ticket-body';
        if (req.plan) {
          const plan = document.createElement('div');
          plan.className = 'badge dot';
          plan.textContent = req.plan;
          body.appendChild(plan);
        }
        if (req.issue) {
          const issue = document.createElement('div');
          issue.className = 'muted small';
          issue.textContent = req.issue;
          body.appendChild(issue);
        }
        article.appendChild(body);
      }

      const footer = document.createElement('div');
      footer.className = 'ticket-footer';
      const device = document.createElement('span');
      const deviceParts = [req.brand, req.model, req.osVersion ? `Android ${req.osVersion}` : null].filter(Boolean);
      device.textContent = deviceParts.length ? deviceParts.join(' • ') : 'Dispositivo não informado';
      footer.appendChild(device);
      const waited = document.createElement('span');
      waited.textContent = formatRelative(waitMs);
      footer.appendChild(waited);
      article.appendChild(footer);

      const actions = document.createElement('div');
      actions.className = 'ticket-actions';
      const acceptBtn = document.createElement('button');
      acceptBtn.className = 'tag-btn primary';
      acceptBtn.type = 'button';
      acceptBtn.textContent = 'Aceitar';
      if (hasActiveSession) {
        acceptBtn.disabled = true;
        acceptBtn.title = 'Finalize a sessão ativa antes de aceitar um novo chamado.';
      }
      acceptBtn.addEventListener('click', () => {
        if (hasActiveSession) {
          addChatMessage({
            author: 'Sistema',
            text: 'Finalize a sessão ativa atual antes de aceitar um novo chamado.',
            kind: 'system',
          });
          return;
        }
        acceptRequest(req.requestId);
      });
      actions.appendChild(acceptBtn);
      const transferBtn = document.createElement('button');
      transferBtn.className = 'tag-btn';
      transferBtn.type = 'button';
      transferBtn.textContent = 'Ver detalhes';
      if (queueTone !== 'ready') transferBtn.classList.add('warn');
      transferBtn.addEventListener('click', () => {
        openClientModal({
          requestId: req.requestId,
          seedContext: state.clientContextByRequest.get(req.requestId) || null,
        });
      });
      actions.appendChild(transferBtn);
      article.appendChild(actions);

      fragment.appendChild(article);
    });

    dom.queue.replaceChildren(fragment);
  });
};

const setClientModalAlert = (message = '', tone = '') => {
  if (!dom.clientModalAlert) return;
  dom.clientModalAlert.textContent = message || '';
  dom.clientModalAlert.classList.remove('client-alert-ok', 'client-alert-warn', 'client-alert-danger');
  if (tone === 'ok') dom.clientModalAlert.classList.add('client-alert-ok');
  if (tone === 'warn') dom.clientModalAlert.classList.add('client-alert-warn');
  if (tone === 'danger') dom.clientModalAlert.classList.add('client-alert-danger');
};

const setClientRegisterResult = (message = '', tone = '') => {
  if (!dom.clientRegisterResult) return;
  dom.clientRegisterResult.textContent = message || '';
  dom.clientRegisterResult.classList.remove('client-alert-ok', 'client-alert-warn', 'client-alert-danger');
  if (tone === 'ok') dom.clientRegisterResult.classList.add('client-alert-ok');
  if (tone === 'warn') dom.clientRegisterResult.classList.add('client-alert-warn');
  if (tone === 'danger') dom.clientRegisterResult.classList.add('client-alert-danger');
};

const setClientModalFormDirty = (dirty = false) => {
  state.clientModal.formDirty = Boolean(dirty);
};

const contextToneFromVerification = (status) => {
  const normalized = String(status || '').trim().toLowerCase();
  if (normalized === 'verified') return 'ok';
  if (normalized === 'pending') return 'warn';
  if (normalized) return 'danger';
  return 'warn';
};

const cacheClientContext = (context, { sessionId = null, requestId = null } = {}) => {
  if (!context || typeof context !== 'object') return;
  const sid = sessionId || context?.anchor?.sessionId || context?.session?.sessionId || null;
  const rid = requestId || context?.anchor?.requestId || context?.request?.requestId || null;
  if (sid) {
    state.clientContextBySession.set(sid, context);
    state.clientContextFetchedAt.set(sid, Date.now());
  }
  if (rid) {
    state.clientContextByRequest.set(rid, context);
  }
};

const fetchClientContext = async ({ sessionId = '', requestId = '', clientRecordId = '', clientUid = '', phone = '' } = {}) => {
  const params = new URLSearchParams();
  if (sessionId) params.set('sessionId', String(sessionId));
  if (requestId) params.set('requestId', String(requestId));
  if (clientRecordId) params.set('clientRecordId', String(clientRecordId));
  if (clientUid) params.set('clientUid', String(clientUid));
  if (phone) params.set('phone', normalizePhone(phone));
  const query = params.toString();
  const response = await authFetch(`/api/client-context${query ? `?${query}` : ''}`);
  const payload = await parseJsonSafely(response);
  if (!response.ok) {
    throw new Error(payload?.error || 'Falha ao carregar contexto do cliente.');
  }
  cacheClientContext(payload, { sessionId, requestId });
  return payload;
};

const ensureSessionClientContext = async (sessionId, { force = false } = {}) => {
  if (!sessionId) return null;
  const cached = state.clientContextBySession.get(sessionId) || null;
  const fetchedAt = Number(state.clientContextFetchedAt.get(sessionId) || 0);
  if (!force && cached && Date.now() - fetchedAt < 15000) {
    return cached;
  }
  try {
    const context = await fetchClientContext({ sessionId });
    if (state.selectedSessionId === sessionId) {
      renderSessions();
    }
    if (state.clientModal.sessionId === sessionId) {
      renderClientModalContext(context);
    }
    return context;
  } catch (error) {
    console.warn('Falha ao atualizar contexto do cliente da sessão', error);
    return cached;
  }
};

const renderContextIdentity = (session, context) => {
  if (!dom.contextIdentity) return;
  const identityItem = dom.contextIdentity.closest('.context-item');
  const client = context?.client || null;
  const requiresRegistration = Boolean(
    context?.needsRegistration ||
    context?.anchor?.requiresTechnicianRegistration ||
    session?.requiresTechnicianRegistration
  );
  const displayName = client?.name || session?.clientName || 'Cliente';
  dom.contextIdentity.textContent = requiresRegistration ? `${displayName} (não cadastrado)` : displayName;
  if (identityItem) identityItem.classList.toggle('attention', requiresRegistration);

  if (dom.contextIdentityAction) {
    dom.contextIdentityAction.hidden = false;
    dom.contextIdentityAction.classList.remove('warn', 'ok');
    const verificationTone = contextToneFromVerification(context?.verification?.status || '');
    const buttonTone = requiresRegistration ? 'warn' : (verificationTone === 'ok' ? 'ok' : 'warn');
    dom.contextIdentityAction.classList.add(buttonTone);
    dom.contextIdentityAction.textContent = requiresRegistration ? 'Cadastrar' : 'Ficha';
    dom.contextIdentityAction.onclick = () => {
      openClientModal({
        sessionId: session?.sessionId || null,
        seedContext: context || null,
      });
    };
  }
};

const clientSummaryRow = (label, value) =>
  `<div class="summary-item"><strong>${escapeHtml(label)}</strong><span>${escapeHtml(value ?? '—')}</span></div>`;

const renderClientModalContext = (context) => {
  const current = context && typeof context === 'object' ? context : null;
  state.clientModal.context = current;

  const anchorSessionId = current?.anchor?.sessionId || state.clientModal.sessionId || null;
  const anchorRequestId = current?.anchor?.requestId || state.clientModal.requestId || null;
  if (dom.clientModalTitle) {
    dom.clientModalTitle.textContent = current?.needsRegistration ? 'Cadastrar cliente' : 'Ficha do cliente';
  }
  if (dom.clientModalSubtitle) {
    if (anchorSessionId) dom.clientModalSubtitle.textContent = `Sessão ${anchorSessionId}`;
    else if (anchorRequestId) dom.clientModalSubtitle.textContent = `Chamado #${anchorRequestId}`;
    else dom.clientModalSubtitle.textContent = 'Contexto sem sessão/chamado ativo.';
  }

  const verificationStatus = current?.verification?.status || '';
  const verificationTone = current?.verificationTone || contextToneFromVerification(verificationStatus);
  if (!current) {
    setClientModalAlert('Carregando contexto do cliente...', 'warn');
  } else if (current.needsRegistration) {
    setClientModalAlert('Cliente ainda não cadastrado. Use o formulário para concluir o cadastro inicial.', 'danger');
  } else {
    const verificationLabel = verificationStatus ? `Verificação: ${verificationStatus}` : 'Verificação pendente';
    setClientModalAlert(`Cliente cadastrado. ${verificationLabel}.`, verificationTone);
  }

  if (dom.clientModalSummary) {
    const client = current?.client || null;
    const profile = current?.profile || null;
    const anchorPhone = current?.anchor?.clientPhone || '';
    dom.clientModalSummary.innerHTML = [
      clientSummaryRow('Nome', client?.name || current?.request?.clientName || 'Cliente'),
      clientSummaryRow('Telefone', client?.phone || anchorPhone || '—'),
      clientSummaryRow('Email', client?.primaryEmail || '—'),
      clientSummaryRow('Créditos', client?.credits ?? 0),
      clientSummaryRow('Atendimentos usados', client?.supportsUsed ?? 0),
      clientSummaryRow('Primeiro grátis usado', client?.freeFirstSupportUsed ? 'Sim' : 'Não'),
      clientSummaryRow('Total sessões', profile?.totalSessions ?? 0),
      clientSummaryRow('Total créditos usados', profile?.totalCreditsUsed ?? 0),
      clientSummaryRow('Criado por', client?.createdByTechName || client?.createdByTechEmail || '—'),
      clientSummaryRow('Status verificação', verificationStatus || 'pending'),
      clientSummaryRow('Telefone verificado', current?.verification?.verifiedPhone || '—'),
      clientSummaryRow('Motivo técnico', current?.verification?.mismatchReason || '—'),
    ].join('');
  }

  if (dom.clientModalHistory) {
    const rows = Array.isArray(current?.recentSupportSessions) ? current.recentSupportSessions : [];
    if (!rows.length) {
      dom.clientModalHistory.innerHTML = '<div class="muted small">Sem histórico para este cliente.</div>';
    } else {
      dom.clientModalHistory.innerHTML = rows
        .map((item) => {
          const started = item.startedAt ? formatDateTime(item.startedAt) : '—';
          return `
            <article class="client-history-item">
              <strong>${escapeHtml(started)} • ${escapeHtml(item.status || '-')}</strong>
              <div>Técnico: ${escapeHtml(item.techName || '-')}</div>
              <div>Problema: ${escapeHtml(item.problemSummary || '-')}</div>
              <div>Solução: ${escapeHtml(item.solutionSummary || '-')}</div>
            </article>
          `;
        })
        .join('');
    }
  }

  if (!state.clientModal.formDirty) {
    if (dom.clientRegisterName) {
      dom.clientRegisterName.value = current?.client?.name || current?.request?.clientName || '';
    }
    if (dom.clientRegisterPhone) {
      dom.clientRegisterPhone.value = current?.client?.phone || current?.anchor?.clientPhone || '';
    }
    if (dom.clientRegisterEmail) {
      dom.clientRegisterEmail.value = current?.client?.primaryEmail || '';
    }
    if (dom.clientRegisterNotes) {
      dom.clientRegisterNotes.value = current?.client?.notes || '';
    }
  }
  if (dom.clientRegisterSubmit) {
    dom.clientRegisterSubmit.textContent = current?.needsRegistration ? 'Cadastrar cliente' : 'Salvar alterações';
  }

  const hasClient = Boolean(current?.client?.id);
  if (dom.clientAddCreditBtn) dom.clientAddCreditBtn.disabled = !hasClient;
  if (dom.clientRemoveCreditBtn) dom.clientRemoveCreditBtn.disabled = !hasClient;
  if (dom.clientAddNoteBtn) dom.clientAddNoteBtn.disabled = !hasClient;
  if (dom.clientRequestManualVerificationBtn) dom.clientRequestManualVerificationBtn.disabled = !hasClient;
  if (dom.clientConfirmManualVerificationBtn) dom.clientConfirmManualVerificationBtn.disabled = !hasClient;
  if (dom.clientMarkMismatchBtn) dom.clientMarkMismatchBtn.disabled = !hasClient;
};

const closeClientModal = () => {
  if (dom.clientModal) dom.clientModal.hidden = true;
  state.clientModal.sessionId = null;
  state.clientModal.requestId = null;
  state.clientModal.context = null;
  setClientModalFormDirty(false);
  setClientModalAlert('', '');
  setClientRegisterResult('', '');
};

const refreshClientModalContext = async ({ force = false } = {}) => {
  const sessionId = state.clientModal.sessionId || state.clientModal.context?.anchor?.sessionId || null;
  const requestId = state.clientModal.requestId || state.clientModal.context?.anchor?.requestId || null;
  const clientRecordId = state.clientModal.context?.client?.id || null;
  const clientUid = state.clientModal.context?.anchor?.clientUid || null;
  const phone = state.clientModal.context?.anchor?.clientPhone || dom.clientRegisterPhone?.value || '';
  if (!sessionId && !requestId && !clientRecordId && !phone) return null;
  const context = await fetchClientContext({
    sessionId,
    requestId,
    clientRecordId,
    clientUid,
    phone,
  });
  renderClientModalContext(context);
  if (sessionId && force) {
    state.clientContextFetchedAt.set(sessionId, Date.now());
  }
  return context;
};

const openClientModal = async ({ sessionId = null, requestId = null, seedContext = null } = {}) => {
  state.clientModal.sessionId = sessionId || seedContext?.anchor?.sessionId || null;
  state.clientModal.requestId = requestId || seedContext?.anchor?.requestId || null;
  state.clientModal.context = seedContext || null;
  setClientModalFormDirty(false);
  if (dom.clientModal) dom.clientModal.hidden = false;
  setClientRegisterResult('', '');
  renderClientModalContext(seedContext);
  try {
    await refreshClientModalContext({ force: true });
  } catch (error) {
    console.error('Falha ao abrir modal de cliente', error);
    setClientModalAlert('Não foi possível carregar a ficha completa deste cliente.', 'danger');
  }
};

const submitClientRegistration = async () => {
  const payload = {
    sessionId: state.clientModal.sessionId || state.clientModal.context?.anchor?.sessionId || null,
    requestId: state.clientModal.requestId || state.clientModal.context?.anchor?.requestId || null,
    name: dom.clientRegisterName?.value?.trim() || '',
    phone: normalizePhone(dom.clientRegisterPhone?.value || ''),
    email: dom.clientRegisterEmail?.value?.trim() || '',
    notes: dom.clientRegisterNotes?.value?.trim() || '',
  };
  if (!payload.name || !payload.phone) {
    setClientRegisterResult('Nome e telefone são obrigatórios.', 'danger');
    return;
  }

  if (dom.clientRegisterSubmit) dom.clientRegisterSubmit.disabled = true;
  setClientRegisterResult('Salvando cadastro...', 'warn');
  try {
    const response = await authFetch('/api/client-context/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await parseJsonSafely(response);
    if (!response.ok) {
      throw new Error(data?.error || 'Falha ao salvar cadastro.');
    }
    setClientModalFormDirty(false);
    cacheClientContext(data, { sessionId: payload.sessionId, requestId: payload.requestId });
    renderClientModalContext(data);
    const triggerTone = data?.verificationTrigger?.status === 'error' ? 'warn' : 'ok';
    setClientRegisterResult(data?.verificationTrigger?.message || 'Cadastro salvo com sucesso.', triggerTone);
    await Promise.all([loadQueue({ manual: true }), loadSessions()]);
  } catch (error) {
    console.error('Falha ao registrar cliente', error);
    setClientRegisterResult(error.message || 'Não foi possível salvar o cadastro.', 'danger');
  } finally {
    if (dom.clientRegisterSubmit) dom.clientRegisterSubmit.disabled = false;
  }
};

const adjustClientCreditsFromModal = async (delta) => {
  const clientId = state.clientModal.context?.client?.id || null;
  if (!clientId || !Number.isFinite(delta) || delta === 0) return;
  setClientRegisterResult('Atualizando créditos...', 'warn');
  try {
    const response = await authFetch('/api/client-context/credits', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ clientId, delta }),
    });
    const data = await parseJsonSafely(response);
    if (!response.ok) throw new Error(data?.error || 'Falha ao atualizar créditos.');
    cacheClientContext(data, {
      sessionId: state.clientModal.sessionId || state.clientModal.context?.anchor?.sessionId || null,
      requestId: state.clientModal.requestId || state.clientModal.context?.anchor?.requestId || null,
    });
    renderClientModalContext(data);
    setClientRegisterResult('Créditos atualizados.', 'ok');
    renderSessions();
  } catch (error) {
    console.error('Falha ao atualizar créditos', error);
    setClientRegisterResult(error.message || 'Falha ao atualizar créditos.', 'danger');
  }
};

const addClientNoteFromModal = async () => {
  const clientId = state.clientModal.context?.client?.id || null;
  if (!clientId) return;
  const note = window.prompt('Digite a observação que deseja anexar ao cliente:');
  if (!note || !note.trim()) return;
  setClientRegisterResult('Salvando observação...', 'warn');
  try {
    const response = await authFetch('/api/client-context/note', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ clientId, note: note.trim() }),
    });
    const data = await parseJsonSafely(response);
    if (!response.ok) throw new Error(data?.error || 'Falha ao salvar observação.');
    cacheClientContext(data, {
      sessionId: state.clientModal.sessionId || state.clientModal.context?.anchor?.sessionId || null,
      requestId: state.clientModal.requestId || state.clientModal.context?.anchor?.requestId || null,
    });
    renderClientModalContext(data);
    setClientRegisterResult('Observação adicionada com sucesso.', 'ok');
  } catch (error) {
    console.error('Falha ao adicionar observação', error);
    setClientRegisterResult(error.message || 'Falha ao salvar observação.', 'danger');
  }
};

const requestManualVerificationFromModal = async () => {
  const clientId = state.clientModal.context?.client?.id || null;
  if (!clientId) return;
  setClientRegisterResult('Solicitando fallback manual...', 'warn');
  try {
    const response = await authFetch('/api/client-context/verification/request-manual', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ clientId }),
    });
    const data = await parseJsonSafely(response);
    if (!response.ok) throw new Error(data?.error || 'Falha ao solicitar fallback manual.');
    cacheClientContext(data, {
      sessionId: state.clientModal.sessionId || state.clientModal.context?.anchor?.sessionId || null,
      requestId: state.clientModal.requestId || state.clientModal.context?.anchor?.requestId || null,
    });
    renderClientModalContext(data);
    setClientRegisterResult('Fallback manual solicitado.', 'ok');
    await loadQueue({ manual: true });
  } catch (error) {
    console.error('Falha ao solicitar fallback manual', error);
    setClientRegisterResult(error.message || 'Falha ao solicitar fallback manual.', 'danger');
  }
};

const confirmManualVerificationFromModal = async () => {
  const clientId = state.clientModal.context?.client?.id || null;
  if (!clientId) return;
  const suggestedPhone = state.clientModal.context?.client?.phone || state.clientModal.context?.anchor?.clientPhone || '';
  const informedPhone = window.prompt('Informe o telefone confirmado manualmente:', suggestedPhone);
  const normalizedPhone = normalizePhone(informedPhone || '');
  if (!normalizedPhone) {
    setClientRegisterResult('Telefone invalido para confirmacao manual.', 'danger');
    return;
  }

  setClientRegisterResult('Confirmando verificacao manual...', 'warn');
  try {
    const response = await authFetch('/api/client-context/verification/confirm-manual', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ clientId, verifiedPhone: normalizedPhone }),
    });
    const data = await parseJsonSafely(response);
    if (!response.ok) throw new Error(data?.error || 'Falha ao confirmar verificacao manual.');
    cacheClientContext(data, {
      sessionId: state.clientModal.sessionId || state.clientModal.context?.anchor?.sessionId || null,
      requestId: state.clientModal.requestId || state.clientModal.context?.anchor?.requestId || null,
    });
    renderClientModalContext(data);
    setClientRegisterResult('Verificacao manual confirmada.', 'ok');
    await loadQueue({ manual: true });
  } catch (error) {
    console.error('Falha ao confirmar verificacao manual', error);
    setClientRegisterResult(error.message || 'Falha ao confirmar verificacao manual.', 'danger');
  }
};

const markVerificationMismatchFromModal = async () => {
  const clientId = state.clientModal.context?.client?.id || null;
  if (!clientId) return;
  const reason =
    window.prompt('Motivo da divergencia:', 'phone_divergent_manual') ||
    'phone_divergent_manual';
  setClientRegisterResult('Marcando cliente como divergente...', 'warn');
  try {
    const response = await authFetch('/api/client-context/verification/mark-mismatch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ clientId, reason }),
    });
    const data = await parseJsonSafely(response);
    if (!response.ok) throw new Error(data?.error || 'Falha ao marcar divergencia.');
    cacheClientContext(data, {
      sessionId: state.clientModal.sessionId || state.clientModal.context?.anchor?.sessionId || null,
      requestId: state.clientModal.requestId || state.clientModal.context?.anchor?.requestId || null,
    });
    renderClientModalContext(data);
    setClientRegisterResult('Cliente marcado como divergente.', 'ok');
    await loadQueue({ manual: true });
  } catch (error) {
    console.error('Falha ao marcar divergencia', error);
    setClientRegisterResult(error.message || 'Falha ao marcar divergencia.', 'danger');
  }
};

const bindClientModal = () => {
  dom.clientModal?.addEventListener('click', (event) => {
    if (event.target?.dataset?.closeClient === 'true') {
      closeClientModal();
    }
  });
  dom.clientRegisterForm?.addEventListener('submit', (event) => {
    event.preventDefault();
    void submitClientRegistration();
  });
  [dom.clientRegisterName, dom.clientRegisterPhone, dom.clientRegisterEmail, dom.clientRegisterNotes]
    .filter(Boolean)
    .forEach((field) => {
      field.addEventListener('input', () => {
        setClientModalFormDirty(true);
      });
    });
  dom.clientRegisterRefresh?.addEventListener('click', () => {
    setClientModalFormDirty(false);
    void refreshClientModalContext({ force: true });
  });
  dom.clientAddCreditBtn?.addEventListener('click', () => {
    void adjustClientCreditsFromModal(1);
  });
  dom.clientRemoveCreditBtn?.addEventListener('click', () => {
    void adjustClientCreditsFromModal(-1);
  });
  dom.clientAddNoteBtn?.addEventListener('click', () => {
    void addClientNoteFromModal();
  });
  dom.clientRequestManualVerificationBtn?.addEventListener('click', () => {
    void requestManualVerificationFromModal();
  });
  dom.clientConfirmManualVerificationBtn?.addEventListener('click', () => {
    void confirmManualVerificationFromModal();
  });
  dom.clientMarkMismatchBtn?.addEventListener('click', () => {
    void markVerificationMismatchFromModal();
  });
};

const setClientsHubAlert = (message = '', tone = '') => {
  if (!dom.clientsHubAlert) return;
  dom.clientsHubAlert.textContent = message || '';
  dom.clientsHubAlert.classList.remove('client-alert-ok', 'client-alert-warn', 'client-alert-danger');
  if (tone === 'ok') dom.clientsHubAlert.classList.add('client-alert-ok');
  if (tone === 'warn') dom.clientsHubAlert.classList.add('client-alert-warn');
  if (tone === 'danger') dom.clientsHubAlert.classList.add('client-alert-danger');
};

const closeClientsHubModal = () => {
  if (dom.clientsHubModal) dom.clientsHubModal.hidden = true;
};

const normalizeVerificationStatus = (value) => {
  const status = String(value || '').trim().toLowerCase();
  return status || 'pending';
};

const verificationStatusLabel = (status) => {
  if (status === 'verified') return 'Verificado';
  if (status === 'pending') return 'Pendente';
  if (status === 'manual_required') return 'Manual necessario';
  if (status === 'mismatch') return 'Divergente';
  if (status === 'rejected') return 'Rejeitado';
  return status;
};

const toneFromVerificationStatus = (status) => {
  if (status === 'verified') return 'ok';
  if (status === 'pending') return 'warn';
  return 'danger';
};

const renderClientsHubList = () => {
  if (!dom.clientsHubList) return;
  const items = Array.isArray(state.clientsHub.items) ? state.clientsHub.items : [];
  if (!items.length) {
    dom.clientsHubList.innerHTML = '<div class="muted small">Nenhum cliente encontrado.</div>';
    return;
  }

  const fragment = document.createDocumentFragment();
  items.forEach((client) => {
    const profileCompleted = Boolean(client?.profileCompleted);
    const verificationStatus = normalizeVerificationStatus(client?.verificationStatus || client?.verification?.status);
    const verificationTone = toneFromVerificationStatus(verificationStatus);
    const row = document.createElement('article');
    row.className = `clients-hub-row${profileCompleted ? '' : ' is-pending'}`;

    row.innerHTML = `
      <div class="clients-hub-top">
        <div class="clients-hub-name">
          <strong>${escapeHtml(client?.name || 'Cliente sem nome')}</strong>
          <span class="clients-hub-phone">${escapeHtml(client?.phone || 'Telefone não informado')}</span>
        </div>
        <div class="clients-hub-actions">
          <button type="button" class="primary-btn" data-client-open="${escapeHtml(client?.id || '')}">Abrir ficha</button>
        </div>
      </div>
      <div class="clients-hub-tags">
        <span class="clients-hub-tag ${profileCompleted ? 'ok' : 'danger'}">${profileCompleted ? 'Cadastro completo' : 'Cadastro pendente'}</span>
        <span class="clients-hub-tag ${verificationTone}">Verificação: ${escapeHtml(verificationStatusLabel(verificationStatus))}</span>
        <span class="clients-hub-tag ${client?.freeFirstSupportUsed ? 'warn' : 'ok'}">${client?.freeFirstSupportUsed ? 'Primeiro grátis usado' : 'Primeiro grátis disponível'}</span>
        <span class="clients-hub-tag">Créditos: ${escapeHtml(String(client?.credits ?? 0))}</span>
        <span class="clients-hub-tag">Atendimentos: ${escapeHtml(String(client?.supportsUsed ?? 0))}</span>
      </div>
      <div class="muted small">Atualizado em ${escapeHtml(client?.updatedAt ? formatDateTime(client.updatedAt) : '—')}</div>
    `;

    const openButton = row.querySelector('[data-client-open]');
    openButton?.addEventListener('click', () => {
      closeClientsHubModal();
      void openClientModal({
        seedContext: {
          anchor: {
            clientPhone: client?.phone || null,
            clientUid: null,
            requiresTechnicianRegistration: !profileCompleted,
          },
          client: {
            id: client?.id || null,
            name: client?.name || null,
            phone: client?.phone || null,
            primaryEmail: client?.primaryEmail || null,
            notes: client?.notes || null,
            credits: Number.isFinite(client?.credits) ? client.credits : 0,
            supportsUsed: Number.isFinite(client?.supportsUsed) ? client.supportsUsed : 0,
            freeFirstSupportUsed: Boolean(client?.freeFirstSupportUsed),
          },
          profile: client?.profile || null,
          verification: client?.verification || { status: verificationStatus },
          needsRegistration: !profileCompleted,
          recentSupportSessions: [],
        },
      });
    });

    fragment.appendChild(row);
  });

  dom.clientsHubList.replaceChildren(fragment);
};

const loadClientsHub = async ({ force = false } = {}) => {
  if (!dom.clientsHubList) return;
  if (state.clientsHub.loading && !force) return;
  const query = dom.clientsHubSearch?.value?.trim() || state.clientsHub.query || '';
  state.clientsHub.query = query;
  state.clientsHub.loading = true;
  setClientsHubAlert('Carregando clientes...', 'warn');

  try {
    const params = new URLSearchParams();
    params.set('limit', '250');
    if (query) params.set('q', query);
    const response = await authFetch(`/api/clients?${params.toString()}`);
    const payload = await parseJsonSafely(response);
    if (!response.ok) {
      throw new Error(payload?.error || 'Falha ao carregar clientes.');
    }
    state.clientsHub.items = Array.isArray(payload?.items) ? payload.items : [];
    state.clientsHub.lastLoadedAt = Date.now();
    renderClientsHubList();
    const total = state.clientsHub.items.length;
    setClientsHubAlert(`${total} cliente${total === 1 ? '' : 's'} carregado${total === 1 ? '' : 's'}.`, total ? 'ok' : 'warn');
  } catch (error) {
    console.error('Falha ao carregar lista de clientes', error);
    state.clientsHub.items = [];
    renderClientsHubList();
    setClientsHubAlert(error.message || 'Falha ao carregar clientes.', 'danger');
  } finally {
    state.clientsHub.loading = false;
  }
};

const openClientsHubModal = async () => {
  if (dom.clientsHubModal) dom.clientsHubModal.hidden = false;
  if (dom.clientsHubSearch) dom.clientsHubSearch.value = state.clientsHub.query || '';
  renderClientsHubList();
  await loadClientsHub({ force: true });
};

const bindClientsHubModal = () => {
  dom.clientsHubModal?.addEventListener('click', (event) => {
    if (event.target?.dataset?.closeClientsHub === 'true') {
      closeClientsHubModal();
    }
  });

  dom.clientsHubRefresh?.addEventListener('click', () => {
    void loadClientsHub({ force: true });
  });

  dom.clientsHubSearch?.addEventListener('keydown', (event) => {
    if (event.key !== 'Enter') return;
    event.preventDefault();
    void loadClientsHub({ force: true });
  });
};

const updateTechIdentity = () => {
  const tech = getTechProfile();
  const name = tech.name || 'Técnico';
  if (dom.techName) dom.techName.textContent = name;
  if (dom.topbarTechName) dom.topbarTechName.textContent = name;
  if (dom.techInitials) dom.techInitials.textContent = computeInitials(name);
  if (dom.techRole) dom.techRole.textContent = tech.role || 'Técnico';
  if (dom.techRoleSecondary) dom.techRoleSecondary.textContent = tech.role || 'Técnico';
  if (dom.techPhoto) {
    const photo = tech.photoURL || '';
    if (photo) {
      dom.techPhoto.src = photo;
      dom.techPhoto.hidden = false;
      dom.techPhoto.style.display = 'block';
      if (dom.techInitials) {
        dom.techInitials.hidden = true;
        dom.techInitials.style.display = 'none';
      }
    } else {
      dom.techPhoto.hidden = true;
      dom.techPhoto.style.display = 'none';
      if (dom.techInitials) {
        dom.techInitials.hidden = false;
        dom.techInitials.style.display = 'grid';
      }
    }
  }
};

const renderSessions = () => {
  selectDefaultSession();

  scheduleRender(() => {
    const activeSessions = state.sessions.filter((s) => s.status === 'active');
    const activeCount = activeSessions.length;
    const label = activeCount === 1 ? '1 em andamento' : `${activeCount} em andamento`;
    const availabilityLabel = activeCount ? 'Em atendimento' : 'Disponível';
    const techStatusLabel = activeCount ? 'Em atendimento agora' : 'Aguardando chamados';

    if (dom.activeSessionsLabel) dom.activeSessionsLabel.textContent = label;
    if (dom.techStatus) dom.techStatus.textContent = techStatusLabel;
    if (dom.availability) dom.availability.textContent = availabilityLabel;

    const session = getSelectedSession();
    const telemetry = session
      ? normalizeTelemetryPayload(
          getTelemetryForSession(session.sessionId) || session.telemetry || session.extra?.telemetry || {}
        )
      : null;
    const sessionClientContext = session ? state.clientContextBySession.get(session.sessionId) || null : null;

    if (!session) {
      if (dom.contextDevice) dom.contextDevice.textContent = '—';
      if (dom.contextIdentity) dom.contextIdentity.textContent = 'Nenhum atendimento selecionado';
      if (dom.contextIdentityAction) {
        dom.contextIdentityAction.hidden = true;
        dom.contextIdentityAction.onclick = null;
        dom.contextIdentityAction.classList.remove('warn', 'ok');
      }
      dom.contextIdentity?.closest('.context-item')?.classList.remove('attention');
      if (dom.contextNetwork) dom.contextNetwork.textContent = '—';
      if (dom.contextHealth) dom.contextHealth.textContent = '—';
      if (dom.contextPermissions) dom.contextPermissions.textContent = '—';
      if (dom.contextStorage) dom.contextStorage.textContent = '—';
      if (dom.contextBattery) dom.contextBattery.textContent = '—';
      if (dom.contextTemperature) dom.contextTemperature.textContent = '—';
      if (dom.contextDeviceImage) dom.contextDeviceImage.src = '/meramente-ilustrativo.webp';
      if (dom.sessionPlaceholder) dom.sessionPlaceholder.textContent = 'Aguardando seleção de sessão';
      if (dom.indicatorNetwork) dom.indicatorNetwork.textContent = '—';
      if (dom.indicatorQuality) dom.indicatorQuality.textContent = '—';
      if (dom.indicatorAlerts) dom.indicatorAlerts.textContent = '—';
      if (dom.contextTimeline) {
        dom.contextTimeline.replaceChildren(
          (() => {
            const entry = document.createElement('div');
            entry.className = 'timeline-entry';
            entry.textContent = 'Sem eventos registrados ainda.';
            return entry;
          })()
        );
      }
      if (dom.closureForm) {
        dom.closureSubmit.disabled = true;
        dom.closureSubmit.textContent = 'Encerrar suporte e disparar pesquisa';
        dom.closureOutcome.disabled = true;
        dom.closureSymptom.disabled = true;
        dom.closureSolution.disabled = true;
        dom.closureNps.disabled = true;
        dom.closureFcr.disabled = true;
      }
      return;
    }

    const deviceParts = [session.brand, session.model, session.osVersion ? `Android ${session.osVersion}` : null].filter(Boolean);
    if (dom.contextDevice) dom.contextDevice.textContent = deviceParts.length ? deviceParts.join(' • ') : 'Dispositivo não informado';
    renderContextIdentity(session, sessionClientContext);

    if (telemetry && typeof telemetry.shareActive === 'boolean' && dom.controlStart) {
      state.commandState.shareActive = telemetry.shareActive;
      dom.controlStart.textContent = telemetry.shareActive ? 'Encerrar visualização' : 'Solicitar visualização';
    }
    if (telemetry && typeof telemetry.remoteActive === 'boolean' && dom.controlRemote) {
      state.commandState.remoteActive = telemetry.remoteActive;
      dom.controlRemote.textContent = telemetry.remoteActive ? 'Revogar acesso remoto' : 'Solicitar acesso remoto';
      if (telemetry.remoteActive) {
        dom.sessionVideo?.focus({ preventScroll: true });
      }
    }
    if (telemetry && typeof telemetry.callActive === 'boolean' && dom.controlQuality && state.call.status === CallStates.IDLE) {
      state.commandState.callActive = telemetry.callActive;
      dom.controlQuality.textContent = telemetry.callActive ? 'Encerrar chamada' : 'Iniciar chamada';
    }

    const networkLabel = formatNetworkLabel(telemetry, session);
    const healthLabel = deriveHealthLabel(telemetry, session);
    const permissionsLabel = formatPermissionLabel(telemetry, session);
    const alertsLabel = telemetry?.alerts || session.extra?.alerts || (session.status === 'active' ? 'Sem alertas' : 'Encerrada');
    const storageLabel = formatStorageLabel(telemetry);
    const batteryLabel = formatBatteryLabel(telemetry);
    const temperatureLabel = formatTemperatureLabel(telemetry);
    const deviceImageUrl = typeof telemetry?.deviceImageUrl === 'string' && telemetry.deviceImageUrl.trim()
      ? telemetry.deviceImageUrl.trim()
      : typeof session?.extra?.deviceImageUrl === 'string' && session.extra.deviceImageUrl.trim()
        ? session.extra.deviceImageUrl.trim()
        : '/meramente-ilustrativo.webp';
    if (dom.contextNetwork) dom.contextNetwork.textContent = networkLabel;
    if (dom.contextHealth) dom.contextHealth.textContent = healthLabel;
    if (dom.contextPermissions) dom.contextPermissions.textContent = permissionsLabel;
    if (dom.contextStorage) dom.contextStorage.textContent = storageLabel;
    if (dom.contextBattery) dom.contextBattery.textContent = batteryLabel;
    if (dom.contextTemperature) dom.contextTemperature.textContent = temperatureLabel;
    if (dom.contextDeviceImage) dom.contextDeviceImage.src = deviceImageUrl;
    if (dom.indicatorNetwork) dom.indicatorNetwork.textContent = networkLabel;
    if (dom.indicatorQuality) dom.indicatorQuality.textContent = session.status === 'active' ? 'Online' : 'Finalizada';
    if (dom.indicatorAlerts) dom.indicatorAlerts.textContent = alertsLabel;

    if (dom.sessionPlaceholder) {
      dom.sessionPlaceholder.textContent =
        session.status === 'active'
          ? `Sessão ${session.sessionId} • aguardando conexão`
          : `Sessão ${session.sessionId} encerrada ${formatRelative(Date.now() - (session.closedAt || session.acceptedAt))}`;
    }

    if (dom.contextTimeline) {
      const timelineEvents = [
        session.requestedAt ? { at: session.requestedAt, text: 'Cliente entrou na fila' } : null,
        session.acceptedAt ? { at: session.acceptedAt, text: 'Atendimento aceito pelo técnico' } : null,
        session.closedAt ? { at: session.closedAt, text: 'Atendimento encerrado' } : null,
      ]
        .filter(Boolean)
        .sort((a, b) => (a.at || 0) - (b.at || 0))
        .slice(-TIMELINE_RENDER_LIMIT);

      if (!timelineEvents.length) {
        const entry = document.createElement('div');
        entry.className = 'timeline-entry';
        entry.textContent = 'Sem eventos registrados ainda.';
        dom.contextTimeline.replaceChildren(entry);
      } else {
        const fragment = document.createDocumentFragment();
        timelineEvents.forEach((evt) => {
          const entry = document.createElement('div');
          entry.className = 'timeline-entry';
          entry.textContent = `${formatTime(evt.at)} • ${evt.text}`;
          fragment.appendChild(entry);
        });
        dom.contextTimeline.replaceChildren(fragment);
      }
    }

    if (dom.closureForm) {
      const isClosed = session.status === 'closed';
      dom.closureSubmit.disabled = isClosed;
      dom.closureSubmit.textContent = isClosed ? 'Atendimento encerrado' : 'Encerrar suporte e disparar pesquisa';
      dom.closureOutcome.disabled = isClosed;
      dom.closureSymptom.disabled = isClosed;
      dom.closureSolution.disabled = isClosed;
      dom.closureNps.disabled = isClosed;
      dom.closureFcr.disabled = isClosed;
    }
  });

  renderChatForSession();
  updateMediaDisplay();
  joinSelectedSession();
  syncWebRtcForSelectedSession();
  const selected = getSelectedSession();
  if (selected?.sessionId) {
    void ensureSessionClientContext(selected.sessionId);
  }
};

const renderMetrics = () => {
  if (!state.metrics) return;
  const metrics = state.metrics;
  scheduleRender(() => {
    if (dom.metricAttendances) dom.metricAttendances.textContent = metrics.attendancesToday ?? 0;
    if (dom.metricQueue) dom.metricQueue.textContent = `Fila atual: ${metrics.queueSize ?? 0}`;
    if (dom.metricFcr) dom.metricFcr.textContent = typeof metrics.fcrPercentage === 'number' ? `${metrics.fcrPercentage}%` : '—';
    if (dom.metricFcrDetail)
      dom.metricFcrDetail.textContent = metrics.fcrPercentage != null ? 'Base: atendimentos encerrados hoje' : 'Aguardando dados';
    if (dom.metricNps) dom.metricNps.textContent = typeof metrics.nps === 'number' ? metrics.nps : '—';
    if (dom.metricNpsDetail)
      dom.metricNpsDetail.textContent = metrics.nps != null ? 'Cálculo: promotores - detratores' : 'Coletado ao encerrar';
    if (dom.metricHandle)
      dom.metricHandle.textContent = metrics.averageHandleMs != null ? formatDuration(metrics.averageHandleMs) : '—';
    if (dom.metricWait)
      dom.metricWait.textContent =
        metrics.averageWaitMs != null ? `Espera média ${formatDuration(metrics.averageWaitMs)}` : 'Espera média —';
  });
};

const addChatMessage = ({
  author,
  text,
  type = 'text',
  audioUrl = '',
  imageUrl = '',
  fileUrl = '',
  fileName = '',
  mimeType = '',
  fileSize = null,
  kind = 'client',
  ts = Date.now(),
}) => {
  if (!text && !audioUrl && !imageUrl && !fileUrl && !(type === 'image')) return;
  scheduleRender(() => {
    if (!dom.chatThread) return;
    const container = dom.chatThread;
    const shouldStick = isNearBottom(container);
    const entry = createChatEntryElement({ author, text, type, audioUrl, imageUrl, fileUrl, fileName, mimeType, fileSize, kind, ts });
    container.appendChild(entry);
    while (container.children.length > CHAT_RENDER_LIMIT) {
      container.removeChild(container.firstChild);
    }
    if (shouldStick) {
      requestAnimationFrame(() => {
        container.scrollTop = container.scrollHeight;
      });
    }
  });
};

const acceptRequest = async (requestId) => {
  if (!requestId) return;
  try {
    const user = await ensureAuth();
    if (!user || !user.uid) {
      throw new Error('auth_required');
    }

    const token = await getIdToken(false);
    const techName = ensureString(state.techProfile?.name || user.displayName || user.email || '', '').trim() || 'Técnico';
    const res = await fetch(`/api/requests/${requestId}/accept`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        techUid: user.uid,
        techId: user.uid,
        techName,
        techEmail: ensureString(user.email || '', '').trim() || null,
      }),
    });

    if (!res.ok) {
      const payload = await res.json().catch(() => ({}));
      if (res.status === 409 && payload?.error === 'active_session_exists') {
        const existingSessionId = ensureString(payload?.sessionId || '', '').trim();
        if (existingSessionId) {
          selectSessionById(existingSessionId);
          renderSessions();
          throw new Error(`Você já está em atendimento na sessão ${existingSessionId}. Encerre-a antes de aceitar outro chamado.`);
        }
        throw new Error('Você já está em atendimento. Encerre a sessão ativa antes de aceitar outro chamado.');
      }
      throw new Error(payload.error || 'Falha ao aceitar chamado');
    }

    const payload = await res.json().catch(() => ({}));
    const acceptedSessionId = ensureString(payload.sessionId || '', '').trim();
    const acceptedLabel = acceptedSessionId ? `Sessão ${acceptedSessionId}` : `Chamado ${requestId}`;
    addChatMessage({ author: 'Sistema', text: `Aceite realizado com sucesso (${acceptedLabel}).`, kind: 'system' });
    loadQueue({ manual: true });
    await Promise.all([loadSessions(), loadMetrics()]);
    if (acceptedSessionId) {
      selectSessionById(acceptedSessionId);
      renderSessions();
    }
  } catch (error) {
    console.error(error);
    addChatMessage({ author: 'Sistema', text: error.message || 'Não foi possível aceitar o chamado.', kind: 'system' });
  }
};

const loadQueue = async ({ manual = false } = {}) => {
  if (queueLoadPromise) {
    return queueLoadPromise;
  }

  if (manual) {
    resetQueueRetryTimer();
  }

  queueLoadPromise = (async () => {
    try {
      const authUser = await ensureAuth();
      if (!authUser) throw new Error('auth_required');
      const token = await getIdToken(false);
      const response = await fetch('/api/requests?status=queued', {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (!response.ok) {
        if (isTemporaryQueueFailureStatus(response.status)) {
          markQueueUnavailable({ statusText: `status ${response.status}` });
        } else {
          resetQueueRetryState();
          const statusText = `status ${response.status}`;
          console.warn(`[queue] Erro ao carregar fila (${statusText}).`);
          state.queue = [];
          renderQueue();
          updateQueueMetrics(0);
        }
        return [];
      }

      const data = await response.json().catch(() => []);
      state.queue = Array.isArray(data) ? data : [];
      state.clientContextByRequest.clear();
      if (Array.isArray(state.queue)) {
        state.queue.forEach((req) => {
          if (!req?.requestId) return;
          const context = {
            anchor: {
              requestId: req.requestId,
              sessionId: null,
              clientPhone: req.clientPhone || null,
              clientUid: req.clientUid || null,
              status: req.state || null,
              requiresTechnicianRegistration: Boolean(req.requiresTechnicianRegistration || !req.clientRegistered),
            },
            request: {
              requestId: req.requestId,
              state: req.state || null,
              createdAt: req.createdAt || null,
              clientName: req.clientName || null,
              brand: req.brand || null,
              model: req.model || null,
              osVersion: req.osVersion || null,
            },
            session: null,
            client: req.clientRegistered
              ? {
                  id: req.clientRecordId || null,
                  name: req.clientName || 'Cliente',
                  phone: req.clientPhone || null,
                  credits: Number(req.credits) || 0,
                  supportsUsed: Number(req.supportsUsed) || 0,
                  freeFirstSupportUsed: Boolean(req.freeFirstSupportUsed),
                }
              : null,
            profile: null,
            verification: req.verificationStatus ? { status: req.verificationStatus } : null,
            verificationTone: req.verificationStatus === 'verified' ? 'ok' : 'warn',
            needsRegistration: Boolean(req.requiresTechnicianRegistration || !req.clientRegistered),
            recentSupportSessions: [],
          };
          state.clientContextByRequest.set(req.requestId, context);
        });
      }
      renderQueue();
      updateQueueMetrics(Array.isArray(state.queue) ? state.queue.length : null);
      resetQueueRetryState();
      return state.queue;
    } catch (_error) {
      markQueueUnavailable({ statusText: 'falha de rede' });
      return [];
    }
  })();

  try {
    return await queueLoadPromise;
  } finally {
    queueLoadPromise = null;
  }
};

const fetchSessionsFromApi = async (authUser) => {
  const token = await getIdToken(false);
  const endpoint = state.sessionFilter === 'mine' ? '/api/sessions?mine=1' : '/api/sessions';
  const response = await fetch(endpoint, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    const errorPayload = await response.json().catch(() => ({}));
    throw new Error(errorPayload.error || 'api_error');
  }

  const payload = await response.json().catch(() => ({}));
  return Array.isArray(payload.sessions) ? payload.sessions : [];
};

const loadSessions = async ({ skipMetrics = false } = {}) => {
  if (pendingSessionsPromise) {
    try {
      const sessions = await pendingSessionsPromise;
      if (!skipMetrics) updateMetricsFromSessions(sessions);
      return sessions;
    } catch (error) {
      console.error('Erro ao aguardar carregamento de sessões', error);
      if (!skipMetrics) updateMetricsFromSessions([]);
      return [];
    }
  }

  let authUser = null;
  try {
    authUser = await ensureAuth();
  } catch (error) {
    console.error('Falha ao autenticar antes de carregar sessões', error);
  }
  if (authUser) {
    syncAuthToTechProfile(authUser);
  }

  if (!authUser) {
    state.sessions = [];
    state.clientContextBySession.clear();
    state.clientContextFetchedAt.clear();
    renderSessions();
    if (!skipMetrics) updateMetricsFromSessions([]);
    return [];
  }

  pendingSessionsPromise = (async () => {
    const sessions = await fetchSessionsFromApi(authUser);
    const filtered = filterSessionsForCurrentTech(sessions);
    filtered.sort((a, b) => (b.acceptedAt || b.requestedAt || 0) - (a.acceptedAt || a.requestedAt || 0));
    return filtered;
  })();

  let sessions = [];
  try {
    sessions = await pendingSessionsPromise;
  } catch (_error) {
    sessions = [];
  } finally {
    pendingSessionsPromise = null;
  }

  let filteredSessions = sessions;
  if (state.sessionFilter === 'queue') {
    filteredSessions = sessions.filter((session) => session.status === 'queued');
  }
  state.sessions = filteredSessions;
  const sessionIdSet = new Set(state.sessions.map((session) => session.sessionId));
  state.chatBySession.forEach((_value, key) => {
    if (!sessionIdSet.has(key)) {
      state.chatBySession.delete(key);
    }
  });
  state.telemetryBySession.forEach((_value, key) => {
    if (!sessionIdSet.has(key)) {
      state.telemetryBySession.delete(key);
    }
  });
  state.clientContextBySession.forEach((_value, key) => {
    if (!sessionIdSet.has(key)) {
      state.clientContextBySession.delete(key);
      state.clientContextFetchedAt.delete(key);
    }
  });
  state.sessions.forEach(syncSessionStores);
  updateSessionRealtimeSubscriptions(sessions);
  renderSessions();
  if (!skipMetrics) {
    updateMetricsFromSessions(sessions);
  }
  return sessions;
};

const loadMetrics = async () => {
  try {
    const sessions = state.sessions.length ? state.sessions : await loadSessions({ skipMetrics: true });
    updateMetricsFromSessions(sessions);
  } catch (error) {
    console.error('Erro ao atualizar métricas a partir do Firestore', error);
  }
};

const initChat = () => {
  if (dom.chatThread) {
    dom.chatThread.innerHTML = '';
    addChatMessage({ author: 'Sistema', text: 'Painel conectado. Aguardando chamados.', kind: 'system' });
  }
  if (dom.chatForm) {
    dom.chatForm.addEventListener('submit', (event) => {
      event.preventDefault();
      const text = dom.chatInput.value.trim();
      if (!text) return;
      sendChatMessage(text);
    });
  }
  if (dom.chatAudioBtn) {
    dom.chatAudioBtn.addEventListener('click', () => {
      void toggleAudioRecording();
    });
  }
  if (dom.chatAttachBtn && dom.chatFileInput) {
    dom.chatAttachBtn.addEventListener('click', () => {
      if (state.chatComposer.uploading) return;
      dom.chatFileInput.click();
    });
    dom.chatFileInput.addEventListener('change', () => {
      const [file] = dom.chatFileInput.files || [];
      if (file) {
        void sendAttachmentMessage(file);
      }
      dom.chatFileInput.value = '';
    });
  }
  dom.quickReplies.forEach((button) => {
    button.addEventListener('click', () => {
      const template = button.dataset.reply;
      if (!template) return;
      dom.chatInput.value = template;
      dom.chatInput.focus();
    });
  });
};


const bindSessionFilters = () => {
  const applyFilter = async (filter) => {
    state.sessionFilter = filter;
    await Promise.all([loadSessions(), loadMetrics()]);
  };

  dom.filterMine?.addEventListener('click', () => {
    void applyFilter('mine');
  });
  dom.filterQueue?.addEventListener('click', () => {
    void applyFilter('queue');
  });
  dom.filterAll?.addEventListener('click', () => {
    void applyFilter('all');
  });
};

const bindPanelsToSessionHeight = () => {
  const triple = document.querySelector('.triple-panels');
  const sessionPanel = document.querySelector('.session-panel');
  if (!triple || !sessionPanel) return;

  let rafId = null;
  const applyHeight = () => {
    rafId = null;
    const height = Math.ceil(sessionPanel.getBoundingClientRect().height);
    triple.style.setProperty('--session-panel-h', `${height}px`);
  };

  const observer = trackObserver(
    new ResizeObserver(() => {
      if (rafId) cancelAnimationFrame(rafId);
      rafId = requestAnimationFrame(applyHeight);
    })
  );
  observer.observe(sessionPanel);

  applyHeight();
  window.addEventListener('resize', () => {
    if (rafId) cancelAnimationFrame(rafId);
    rafId = requestAnimationFrame(applyHeight);
  });
};

const bindClosureForm = () => {
  if (!dom.closureForm) return;
  dom.closureForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    const session = getSelectedSession();
    if (!session) {
      addChatMessage({ author: 'Sistema', text: 'Nenhuma sessão selecionada.', kind: 'system' });
      return;
    }
    if (session.status === 'closed') {
      addChatMessage({ author: 'Sistema', text: 'Essa sessão já foi encerrada.', kind: 'system' });
      return;
    }

    dom.closureSubmit.disabled = true;
    dom.closureSubmit.textContent = 'Enviando…';
    const payload = {
      outcome: dom.closureOutcome.value,
      symptom: dom.closureSymptom.value.trim(),
      solution: dom.closureSolution.value.trim(),
      firstContactResolution: dom.closureFcr.checked,
    };
    const nps = dom.closureNps.value;
    if (nps !== '') payload.npsScore = Number(nps);
    try {
      const authUser = await ensureAuth();
      if (!authUser) throw new Error('auth_required');
      const token = await getIdToken(false);
      const res = await fetch(`/api/sessions/${session.sessionId}/close`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || 'Erro ao encerrar atendimento');
      }
      addChatMessage({ author: 'Sistema', text: `Sessão ${session.sessionId} encerrada.`, kind: 'system' });
      dom.closureForm.reset();
      await Promise.all([loadSessions(), loadMetrics()]);
    } catch (error) {
      console.error(error);
      addChatMessage({ author: 'Sistema', text: error.message || 'Falha ao encerrar a sessão.', kind: 'system' });
    } finally {
      dom.closureSubmit.disabled = false;
      dom.closureSubmit.textContent = 'Encerrar suporte e disparar pesquisa';
    }
  });
};

const bindQueueRetryButton = () => {
  if (!dom.queueRetry) return;
  dom.queueRetry.addEventListener('click', () => {
    if (dom.queueRetry.disabled) return;
    dom.queueRetry.disabled = true;
    resetQueueRetryTimer();
    loadQueue({ manual: true }).finally(() => {
      dom.queueRetry.disabled = false;
    });
  });
};

const bindLegacyShareControls = () => {
  if (dom.webShareConnect) {
    dom.webShareConnect.addEventListener('click', () => {
      activateLegacyShare(dom.webShareRoom?.value || '');
    });
  }
  if (dom.webShareDisconnect) {
    dom.webShareDisconnect.addEventListener('click', () => {
      disconnectLegacyShare();
    });
  }
  if (dom.webShareRoom) {
    dom.webShareRoom.addEventListener('keydown', (event) => {
      if (event.key === 'Enter') {
        event.preventDefault();
        activateLegacyShare(dom.webShareRoom.value);
      }
    });
  }

  const roomFromUrl = getLegacyRoomFromQuery();
  if (dom.webShareRoom && roomFromUrl) {
    dom.webShareRoom.value = roomFromUrl;
  }
  if (roomFromUrl) {
    if (socket && !socket.disconnected) {
      activateLegacyShare(roomFromUrl);
    } else {
      state.legacyShare.pendingRoom = roomFromUrl;
      setLegacyStatus('Preparando conexão com o compartilhamento web…');
    }
  }
  updateLegacyControls();
};


const closeProfileMenu = () => {
  if (dom.profileMenu) dom.profileMenu.classList.remove('is-open');
  if (dom.profileMenuTrigger) dom.profileMenuTrigger.setAttribute('aria-expanded', 'false');
};

const openProfileMenu = () => {
  if (dom.profileMenu) dom.profileMenu.classList.add('is-open');
  if (dom.profileMenuTrigger) dom.profileMenuTrigger.setAttribute('aria-expanded', 'true');
};

const openProfileModal = () => {
  if (dom.profileNameInput) dom.profileNameInput.value = state.techProfile?.name || '';
  if (dom.profileEmailInput) dom.profileEmailInput.value = state.techProfile?.email || '—';
  if (dom.profileStatusInput) dom.profileStatusInput.value = state.techProfile?.active === false ? 'Inativo' : 'Ativo';
  if (dom.profilePhotoInput) dom.profilePhotoInput.value = '';
  if (dom.profileResult) dom.profileResult.textContent = '';
  if (dom.profileModal) dom.profileModal.hidden = false;
};

const uploadCustomProfilePhoto = async (file) => {
  if (!file) return null;
  if (!file.type?.startsWith('image/')) throw new Error('Formato inválido. Envie uma imagem.');
  await ensureAuth();
  const token = await getIdToken(false);
  const body = new FormData();
  body.append('file', file, sanitizeUploadFileName(file.name || `avatar-${Date.now()}.jpg`));
  const response = await fetch('/api/upload/avatar', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
    },
    body,
  });
  const payload = await parseJsonSafely(response);
  if (!response.ok) {
    throw createUploadError(response.status, payload, 'Falha ao enviar foto de perfil.');
  }

  const downloadURL = payload?.upload?.downloadURL || payload?.upload?.downloadUrl || payload?.downloadURL || null;
  if (!downloadURL) {
    throw createUploadError(response.status, payload, 'Upload da foto concluido sem URL.');
  }
  return downloadURL;
};

const escapeHtml = (value) =>
  String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');

const safeImageUrl = (value) => {
  const raw = typeof value === 'string' ? value.trim() : '';
  if (!raw) return '';
  try {
    const parsed = new URL(raw, window.location.origin);
    if (parsed.protocol === 'https:' || parsed.protocol === 'http:') return parsed.toString();
  } catch (_error) {
    return '';
  }
  return '';
};

const buildAvatarMarkup = (tech = {}) => {
  const nameRaw = typeof tech.name === 'string' && tech.name.trim() ? tech.name.trim() : 'tecnico';
  const emailRaw = typeof tech.email === 'string' ? tech.email.trim() : '';
  const initials = computeInitials(nameRaw || emailRaw || 'TC');
  const photoURL = safeImageUrl(tech.photoURL);
  const safeName = escapeHtml(tech.name || 'técnico');
  if (photoURL) {
    return `<div class="profile-avatar"><img src="${photoURL}" alt="Avatar de ${tech.name || 'técnico'}" loading="lazy" referrerpolicy="no-referrer" /></div>`;
  }
  return `<div class="profile-avatar">${initials}</div>`;
};

const buildSafeAvatarMarkup = (tech = {}) => {
  const nameRaw = typeof tech.name === 'string' && tech.name.trim() ? tech.name.trim() : 'tecnico';
  const emailRaw = typeof tech.email === 'string' ? tech.email.trim() : '';
  const safeName = escapeHtml(nameRaw);
  const initials = escapeHtml(computeInitials(nameRaw || emailRaw || 'TC'));
  const photoURL = safeImageUrl(tech.photoURL);
  if (photoURL) {
    return `<div class="profile-avatar"><img src="${photoURL}" alt="Avatar de ${safeName}" loading="lazy" referrerpolicy="no-referrer" /></div>`;
  }
  return `<div class="profile-avatar">${initials}</div>`;
};

const renderSupervisorList = (techs = []) => {
  if (!dom.supervisorList) return;
  dom.supervisorList.innerHTML = '';
  if (!techs.length) {
    dom.supervisorList.innerHTML = '<div class="muted small">Nenhum técnico encontrado.</div>';
    return;
  }

  const search = (dom.supervisorSearch?.value || '').trim().toLowerCase();
  const filtered = techs.filter((tech) => {
    const name = (tech.name || '').toLowerCase();
    const email = (tech.email || '').toLowerCase();
    return !search || name.includes(search) || email.includes(search) || (tech.uid || '').toLowerCase().includes(search);
  });

  if (!filtered.length) {
    dom.supervisorList.innerHTML = '<div class="muted small">Nenhum técnico corresponde à busca.</div>';
    return;
  }

  filtered.forEach((tech) => {
    const row = document.createElement('button');
    row.type = 'button';
    row.className = `supervisor-row ${state.selectedSupervisorUid === tech.uid ? 'active' : ''}`;
    const roleLabel = tech.supervisor === true ? 'Supervisor' : 'Técnico';
    const safeName = escapeHtml(tech.name || 'Sem nome');
    const safeEmail = escapeHtml(tech.email || 'Sem email');
    const safePhotoURL = safeImageUrl(tech.photoURL);
    const avatarMarkup = buildSafeAvatarMarkup({
      ...tech,
      name: safeName,
      email: safeEmail,
      photoURL: safePhotoURL,
    });
    row.innerHTML = `
      <div style="display:flex;gap:10px;align-items:center;">
        ${avatarMarkup}
        <div>
          <strong>${safeName}</strong>
          <div class="small muted">${safeEmail}</div>
        </div>
      </div>
      <div class="supervisor-tags">
        <span class="supervisor-tag ${tech.active ? 'status-active' : 'status-inactive'}">${tech.active ? 'Ativo' : 'Inativo'}</span>
        <span class="supervisor-tag">${roleLabel}</span>
      </div>
    `;
    row.addEventListener('click', () => {
      state.selectedSupervisorUid = tech.uid;
      renderSupervisorList(state.supervisorTechs);
      renderSupervisorDetails();
      if (window.innerWidth <= 900) setSupervisorTab('details');
    });
    dom.supervisorList.appendChild(row);
  });
};

const renderSupervisorDetails = () => {
  const tech = state.supervisorTechs.find((entry) => entry.uid === state.selectedSupervisorUid);
  if (!tech) {
    if (dom.supervisorEmpty) dom.supervisorEmpty.hidden = false;
    if (dom.supervisorDetailForm) dom.supervisorDetailForm.hidden = true;
    return;
  }

  if (dom.supervisorEmpty) dom.supervisorEmpty.hidden = true;
  if (dom.supervisorDetailForm) dom.supervisorDetailForm.hidden = false;
  if (dom.selectedTechAvatar) {
    const originalTechName = tech.name;
    const avatarAltName = String(tech.name || '')
      .replace(/[<>"'`]/g, '')
      .trim();
    tech.name = avatarAltName || 'tecnico';
    const photoURL = safeImageUrl(tech.photoURL);
    if (photoURL) {
      dom.selectedTechAvatar.innerHTML = `<img src="${photoURL}" alt="Avatar de ${tech.name || 'técnico'}" loading="lazy" referrerpolicy="no-referrer" />`;
    } else {
      dom.selectedTechAvatar.textContent = computeInitials(tech.name || tech.email || 'TC');
    }
    tech.name = originalTechName;
  }
  if (dom.selectedTechName) dom.selectedTechName.textContent = tech.name || 'Sem nome';
  if (dom.selectedTechUid) dom.selectedTechUid.textContent = tech.uid || '';
  if (dom.copyTechUidBtn) {
    dom.copyTechUidBtn.disabled = !tech.uid;
    dom.copyTechUidBtn.setAttribute('aria-disabled', String(!tech.uid));
    dom.copyTechUidBtn.title = tech.uid ? 'Copiar ID do técnico' : 'ID indisponível';
  }
  if (dom.editTechName) dom.editTechName.value = tech.name || '';
  if (dom.editTechEmail) dom.editTechEmail.value = tech.email || '';
  if (dom.editTechStatus) dom.editTechStatus.value = tech.active ? 'active' : 'inactive';
  if (dom.editTechRole) dom.editTechRole.value = tech.supervisor === true ? 'supervisor' : 'tech';
  if (dom.supervisorDetailResult) dom.supervisorDetailResult.textContent = '';
};

const setSupervisorTab = (tab) => {
  state.supervisorMobileTab = tab === 'details' ? 'details' : 'list';
  const showList = state.supervisorMobileTab === 'list';
  dom.tabList?.classList.toggle('active', showList);
  dom.tabDetails?.classList.toggle('active', !showList);
  dom.supervisorListPanel?.classList.toggle('mobile-hidden', !showList && window.innerWidth <= 900);
  dom.supervisorDetailPanel?.classList.toggle('mobile-hidden', showList && window.innerWidth <= 900);
};

const loadSupervisorTechs = async () => {
  if (!state.isSupervisor) return;
  const response = await authFetch('/api/admin/list-techs');
  if (!response.ok) {
    showToast('Falha ao carregar lista de técnicos.');
    return;
  }
  const payload = await response.json().catch(() => ({}));
  state.supervisorTechs = Array.isArray(payload.techs) ? payload.techs : [];
  if (!state.selectedSupervisorUid && state.supervisorTechs.length) {
    state.selectedSupervisorUid = state.supervisorTechs[0].uid;
  }
  if (state.selectedSupervisorUid && !state.supervisorTechs.some((entry) => entry.uid === state.selectedSupervisorUid)) {
    state.selectedSupervisorUid = state.supervisorTechs[0]?.uid || null;
  }
  renderSupervisorList(state.supervisorTechs);
  renderSupervisorDetails();
};

const bindProfileMenu = () => {
  dom.profileMenuTrigger?.addEventListener('click', (event) => {
    event.stopPropagation();
    const opened = dom.profileMenu?.classList.contains('is-open');
    if (opened) closeProfileMenu();
    else openProfileMenu();
  });

  dom.profileMenuTrigger?.addEventListener('keydown', (event) => {
    if (event.key !== 'Enter' && event.key !== ' ') return;
    event.preventDefault();
    const opened = dom.profileMenu?.classList.contains('is-open');
    if (opened) closeProfileMenu();
    else openProfileMenu();
  });

  document.addEventListener('click', (event) => {
    if (dom.techIdentity?.contains(event.target)) return;
    closeProfileMenu();
  });

  dom.menuReports?.addEventListener('click', () => {
    closeProfileMenu();
    showToast('Meus relatórios em breve.');
  });

  dom.menuClients?.addEventListener('click', async () => {
    closeProfileMenu();
    await openClientsHubModal();
  });

  dom.menuProfile?.addEventListener('click', () => {
    closeProfileMenu();
    openProfileModal();
  });

  dom.profileCancel?.addEventListener('click', () => {
    if (dom.profileModal) dom.profileModal.hidden = true;
  });

  dom.profileModal?.addEventListener('click', (event) => {
    if (event.target?.dataset?.closeProfile === 'true') {
      dom.profileModal.hidden = true;
    }
  });

  dom.profileResetPassword?.addEventListener('click', async () => {
    const uid = state.techProfile?.uid;
    if (!uid) return;
    const newPasswordTemp = generateTempPasswordClient();
    const response = await authFetch('/api/tech/reset-my-password', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ newPasswordTemp }),
    });
    if (!response.ok) {
      if (dom.profileResult) dom.profileResult.textContent = 'Falha ao resetar senha.';
      return;
    }
    if (dom.profileResult) dom.profileResult.textContent = `Senha temporária: ${newPasswordTemp}`;
  });

  dom.profileForm?.addEventListener('submit', async (event) => {
    event.preventDefault();
    const name = dom.profileNameInput?.value?.trim();
    if (!name) return;
    const selectedPhoto = dom.profilePhotoInput?.files?.[0] || null;

    if (dom.profileResult) dom.profileResult.textContent = 'Salvando alterações...';

    const response = await authFetch('/api/tech/profile-name', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      const message = payload?.message || 'Falha ao atualizar nome.';
      showToast('Falha ao atualizar perfil.');
      if (dom.profileResult) dom.profileResult.textContent = message;
      return;
    }

    let customPhotoURL = null;
    if (selectedPhoto) {
      try {
        customPhotoURL = await uploadCustomProfilePhoto(selectedPhoto);
        const photoResponse = await authFetch('/api/tech/profile-photo', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ photoURL: customPhotoURL }),
        });
        if (!photoResponse.ok) {
          throw new Error('Falha ao salvar foto personalizada.');
        }
      } catch (error) {
        console.error(error);
        if (dom.profileResult) dom.profileResult.textContent = error.message || 'Falha ao atualizar foto.';
        return;
      }
    }

    const refreshedProfile = await ensureTechAccess(await ensureAuth());
    state.techProfile = {
      ...(state.techProfile || {}),
      ...(refreshedProfile?.techDoc || {}),
      name,
      photoURL: refreshedProfile?.photoURL || customPhotoURL || state.techProfile?.photoURL || null,
      profileHistory: Array.isArray(refreshedProfile?.profileHistory)
        ? refreshedProfile.profileHistory
        : state.techProfile?.profileHistory || [],
    };
    updateTechIdentity();
    if (dom.profileModal) dom.profileModal.hidden = true;
    showToast('Perfil atualizado.');
  });

  dom.menuSupervisor?.addEventListener('click', async () => {
    closeProfileMenu();
    if (!state.isSupervisor) return;
    if (dom.supervisorModal) dom.supervisorModal.hidden = false;
    setSupervisorTab('list');
    await loadSupervisorTechs();
  });

  dom.supervisorModal?.addEventListener('click', (event) => {
    if (event.target?.dataset?.closeSupervisor === 'true') {
      dom.supervisorModal.hidden = true;
    }
  });

  dom.tabList?.addEventListener('click', () => setSupervisorTab('list'));
  dom.tabDetails?.addEventListener('click', () => setSupervisorTab('details'));

  dom.supervisorSearch?.addEventListener('input', () => renderSupervisorList(state.supervisorTechs));

  dom.copyTechUidBtn?.addEventListener('click', async () => {
    const tech = state.supervisorTechs.find((entry) => entry.uid === state.selectedSupervisorUid);
    const uid = tech?.uid;
    if (!uid) {
      showToast('ID técnico indisponível.');
      return;
    }

    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(uid);
      } else {
        const tempInput = document.createElement('input');
        tempInput.value = uid;
        tempInput.setAttribute('readonly', 'true');
        tempInput.style.position = 'absolute';
        tempInput.style.left = '-9999px';
        document.body.appendChild(tempInput);
        tempInput.select();
        const copied = document.execCommand('copy');
        document.body.removeChild(tempInput);
        if (!copied) throw new Error('fallback copy failed');
      }
      showToast('ID do técnico copiado.');
    } catch (error) {
      console.error('Falha ao copiar UID do técnico', error);
      showToast('Não foi possível copiar o ID do técnico.');
    }
  });

  dom.supervisorNewTech?.addEventListener('click', () => {
    if (dom.createTechModal) dom.createTechModal.hidden = false;
  });
  dom.supervisorNewTechMobile?.addEventListener('click', () => {
    if (dom.createTechModal) dom.createTechModal.hidden = false;
  });

  dom.createTechModal?.addEventListener('click', (event) => {
    if (event.target?.dataset?.closeCreateTech === 'true') {
      dom.createTechModal.hidden = true;
    }
  });

  dom.supervisorDetailForm?.addEventListener('submit', async (event) => {
    event.preventDefault();
    const uid = state.selectedSupervisorUid;
    if (!uid) return;
    const payload = {
      uid,
      name: dom.editTechName?.value?.trim() || '',
      email: dom.editTechEmail?.value?.trim().toLowerCase() || '',
      active: dom.editTechStatus?.value === 'active',
      role: dom.editTechRole?.value || 'tech',
    };
    const response = await authFetch('/api/admin/update-tech', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const payloadError = await response.json().catch(() => ({}));
      if (dom.supervisorDetailResult) dom.supervisorDetailResult.textContent = payloadError?.message || 'Falha ao salvar alterações.';
      return;
    }
    if (dom.supervisorDetailResult) dom.supervisorDetailResult.textContent = 'Alterações salvas com sucesso.';
    await loadSupervisorTechs();
  });

  dom.editTechReset?.addEventListener('click', async () => {
    const uid = state.selectedSupervisorUid;
    if (!uid) return;
    const newPasswordTemp = generateTempPasswordClient();
    const response = await authFetch('/api/admin/reset-tech-password', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ uid, newPasswordTemp }),
    });
    if (response.ok && dom.supervisorDetailResult) {
      dom.supervisorDetailResult.textContent = `Senha temporária: ${newPasswordTemp}`;
    }
  });

  dom.editTechDelete?.addEventListener('click', async () => {
    const tech = state.supervisorTechs.find((entry) => entry.uid === state.selectedSupervisorUid);
    if (!tech) return;
    const confirmed = window.confirm(`Excluir o técnico ${tech.name || tech.uid}?`);
    if (!confirmed) return;
    await authFetch('/api/admin/delete-tech', {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ uid: tech.uid }),
    });
    state.selectedSupervisorUid = null;
    await loadSupervisorTechs();
  });

  dom.createTechGenerate?.addEventListener('click', () => {
    if (dom.createTechPassword) dom.createTechPassword.value = generateTempPasswordClient();
  });

  dom.createTechForm?.addEventListener('submit', async (event) => {
    event.preventDefault();
    const payload = {
      name: dom.createTechName?.value?.trim(),
      email: dom.createTechEmail?.value?.trim().toLowerCase(),
      passwordTemp: dom.createTechPassword?.value || '',
    };
    const response = await authFetch('/api/admin/create-tech', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await response.json().catch(() => ({}));
    if (response.ok) {
      if (dom.createTechResult) dom.createTechResult.textContent = `Técnico criado. UID: ${data.uid}`;
      dom.createTechForm.reset();
      await loadSupervisorTechs();
      return;
    }
    if (dom.createTechResult) dom.createTechResult.textContent = data?.message || 'Falha ao criar técnico.';
  });
};

const bootstrap = async () => {
  try {
    const authUser = await ensureAuth();
    const profile = await ensureTechAccess(authUser);
    if (!profile) return;
    document.body.style.visibility = 'visible';

    setSessionState(SessionStates.IDLE, null);
    resetCommandState();
    bindPanelsToSessionHeight();
    bindSessionControls();
    bindCallModalControls();
    bindControlMenu();
    bindViewControls();
    bindImageLightboxControls();
    initWhiteboardCanvas();
    bindRemoteControlEvents();
    initChat();
    bindClosureForm();
    bindQueueRetryButton();
    startQueueAutoRefresh();
    bindLegacyShareControls();
    bindSessionFilters();
    bindProfileMenu();
    bindClientModal();
    bindClientsHubModal();
    syncAuthToTechProfile(authUser);
    state.isSupervisor = profile.supervisor === true;
    state.techProfile = {
      ...(state.techProfile || {}),
      uid: profile.uid || authUser?.uid || null,
      name: profile.techDoc?.name || profile.name || authUser?.displayName || 'Técnico',
      email: profile.email || authUser?.email || null,
      photoURL: profile.techDoc?.customPhotoURL || profile.photoURL || authUser?.photoURL || null,
      profileHistory: Array.isArray(profile.profileHistory)
        ? profile.profileHistory
        : Array.isArray(profile.techDoc?.profileHistory)
          ? profile.techDoc.profileHistory
          : [],
      role: state.isSupervisor ? 'Supervisor' : 'Técnico',
    };
    if (dom.menuSupervisor) dom.menuSupervisor.hidden = !state.isSupervisor;
    updateTechIdentifiers(state.techProfile);
    updateTechIdentity();
    if (dom.logoutBtn) {
      dom.logoutBtn.addEventListener('click', async () => {
        await signOut(authInstance).catch(() => {});
        redirectToTechLogin('signed_out');
      });
    }
    await connectSocketWithToken(authUser);
    loadQueue();
    await Promise.all([loadSessions(), loadMetrics()]);
  } catch (error) {
    console.error('Falha ao autenticar no Firebase', error);
    redirectToTechLogin('auth_failed');
  }
};

function registerSocketUpgradeLogs() {
  if (socketUpgradeLogsRegistered) return;
  const engine = socket?.io?.engine;
  if (!engine) return;
  socketUpgradeLogsRegistered = true;
  engine.on('upgrade', (transport) => {
    console.log('[socket] upgraded to', transport?.name || 'desconhecido');
  });
  engine.on('upgradeError', (error) => {
    console.warn('[socket] upgradeError', error);
  });
}

function handleSocketConnect() {
  socketInvalidTokenCounter = 0;
  socketAuthRecoveryInFlight = false;
  socketRealtimeDisabled = false;
  socketRealtimeDisabledNotified = false;
  sessionJoinInFlightId = null;
  if (socket?.id) {
    const transport = socket.io?.engine?.transport?.name || 'desconhecido';
    console.log('[socket] connected', socket.id, 'via', transport);
    registerSocketUpgradeLogs();
  }
  addChatMessage({ author: 'Sistema', text: 'Conectado ao servidor de sinalização.', kind: 'system' });
  loadQueue({ manual: true });
  state.joinedSessionId = null;
  joinSelectedSession();
  if (state.legacyShare.pendingRoom) {
    activateLegacyShare(state.legacyShare.pendingRoom);
  }
}

function disableRealtimeSocket(reason = 'auth_failed') {
  if (socketRealtimeDisabled) return;
  socketRealtimeDisabled = true;
  if (socket?.io) {
    socket.io.opts.reconnection = false;
  }
  if (socket && (socket.connected || socket.active)) {
    socket.disconnect();
  }
  if (!socketRealtimeDisabledNotified) {
    const reasonText = reason === 'auth_failed' ? 'falha de autenticação' : 'instabilidade de conexão';
    addChatMessage({
      author: 'Sistema',
      text: `Tempo real pausado por ${reasonText}. O painel segue em atualização automática.`,
      kind: 'system',
    });
    socketRealtimeDisabledNotified = true;
  }
}

async function handleSocketConnectError(error) {
  if (!socketRealtimeDisabled) {
    console.warn('[socket] connect_error', error);
  }
  const reason = ensureString(error?.message || error?.description || '').toLowerCase();
  if (!reason.includes('invalid_token') && !reason.includes('missing_token')) {
    return;
  }

  if (socketRealtimeDisabled) {
    return;
  }

  if (socketAuthRecoveryInFlight) {
    return;
  }

  socketAuthRecoveryInFlight = true;
  socketInvalidTokenCounter += 1;

  try {
    const authPayload = await buildSocketAuthPayload({ forceRefresh: true });
    if (socket) {
      socket.auth = authPayload;
      if (socket.disconnected && !socket.active) {
        socket.connect();
      }
    }
  } catch (refreshError) {
    console.error('[socket] failed to refresh token after connect_error', refreshError);
  } finally {
    socketAuthRecoveryInFlight = false;
  }

  if (socketInvalidTokenCounter >= 2) {
    disableRealtimeSocket('auth_failed');
  }
}

function handleSocketDisconnect() {
  addChatMessage({ author: 'Sistema', text: 'Desconectado. Tentando reconectar…', kind: 'system' });
  if (state.legacyShare.active) {
    setLegacyStatus('Conexão perdida. Tentando reconectar…');
  }
}

function handleQueueUpdated() {
  loadQueue({ manual: true });
  loadMetrics();
}

function handleSessionUpdated(session) {
  if (!session || !session.sessionId) return;
  const normalizedSession = {
    ...session,
    telemetry: normalizeTelemetryPayload(
      (typeof session.telemetry === 'object' && session.telemetry !== null
        ? session.telemetry
        : session.extra?.telemetry) || {}
    ),
  };
  if (!sessionMatchesCurrentTech(session)) {
    const existingIndex = state.sessions.findIndex((s) => s.sessionId === session.sessionId);
    if (existingIndex >= 0) {
      state.sessions.splice(existingIndex, 1);
      state.chatBySession.delete(session.sessionId);
      state.telemetryBySession.delete(session.sessionId);
      updateSessionRealtimeSubscriptions(state.sessions);
      renderSessions();
      loadMetrics();
    }
    return;
  }
  const index = state.sessions.findIndex((s) => s.sessionId === session.sessionId);
  if (index >= 0) {
    state.sessions[index] = {
      ...state.sessions[index],
      ...normalizedSession,
      extra: { ...(state.sessions[index].extra || {}), ...(normalizedSession.extra || {}) },
    };
    syncSessionStores(state.sessions[index]);
  } else {
    state.sessions.unshift(normalizedSession);
    syncSessionStores(normalizedSession);
  }
  renderSessions();
  updateSessionRealtimeSubscriptions(state.sessions);
  loadMetrics();
}

function handleSessionChat(message) {
  debugChatLog('[chat] message received', { source: 'socket', raw: message });
  ingestChatMessage(message, { source: 'socket' });
}

function handleSessionCommandEvent(command) {
  if (isWhiteboardMessage(command?.data ?? command)) {
    ingestWhiteboardPayload(command.data ?? command);
    return;
  }
  registerCommand(command);
}

function handleSessionStatus(status) {
  if (!status || !status.sessionId) return;
  if (!state.sessions.some((s) => s.sessionId === status.sessionId)) return;
  const ts = status.ts || Date.now();
  const current = normalizeTelemetryPayload(getTelemetryForSession(status.sessionId) || {});
  const data = normalizeTelemetryPayload(typeof status.data === 'object' && status.data !== null ? status.data : {});
  if (!Object.keys(data).length) return;
  const hasChanges = Object.entries(data).some(([key, value]) => current[key] !== value);
  if (!hasChanges) return;
  const merged = normalizeTelemetryPayload({ ...current, ...data, updatedAt: ts });
  state.telemetryBySession.set(status.sessionId, merged);
  const index = state.sessions.findIndex((s) => s.sessionId === status.sessionId);
  if (index >= 0) {
    const session = state.sessions[index];
    const extra = { ...(session.extra || {}), telemetry: merged };
    if (typeof data.network !== 'undefined') extra.network = data.network;
    if (typeof data.net !== 'undefined' && typeof extra.network === 'undefined') extra.network = data.net;
    if (typeof data.health !== 'undefined') extra.health = data.health;
    if (typeof data.permissions !== 'undefined') extra.permissions = data.permissions;
    if (typeof data.alerts !== 'undefined') extra.alerts = data.alerts;
    if (typeof data.batteryLevel !== 'undefined') extra.batteryLevel = data.batteryLevel;
    if (typeof data.batteryCharging !== 'undefined') extra.batteryCharging = data.batteryCharging;
    if (typeof data.temperatureC !== 'undefined') extra.temperatureC = data.temperatureC;
    if (typeof data.storageFreeBytes !== 'undefined') extra.storageFreeBytes = data.storageFreeBytes;
    if (typeof data.storageTotalBytes !== 'undefined') extra.storageTotalBytes = data.storageTotalBytes;
    if (typeof data.deviceImageUrl !== 'undefined') extra.deviceImageUrl = data.deviceImageUrl;
    state.sessions[index] = { ...session, telemetry: merged, extra };
  }
  if (state.selectedSessionId === status.sessionId) {
    renderSessions();
  }
}

function handleSessionEndedEvent(payload) {
  if (!payload || !payload.sessionId) return;
  const reason = payload.reason || 'peer_ended';
  handleSessionEnded(payload.sessionId, reason);
  markSessionEnded(payload.sessionId, reason);
}

function handlePeerLeft() {
  if (
    state.legacyShare.active &&
    !state.joinedSessionId &&
    !state.activeSessionId &&
    !state.selectedSessionId
  ) {
    teardownLegacyShare();
    setLegacyStatus('Cliente encerrou o compartilhamento web.');
    return;
  }
  const sessionId = state.joinedSessionId || state.activeSessionId || state.selectedSessionId || null;
  if (!sessionId) return;
  addChatMessage({ author: 'Sistema', text: 'Cliente desconectou do atendimento.', kind: 'system' });
  sendSessionCommand('session_end', { reason: 'peer_left' }, { silent: true, sessionId })
    .then(({ session }) => {
      registerCommand(
        {
          sessionId: session.sessionId,
          type: 'session_end',
          reason: 'peer_left',
          by: 'tech',
          ts: Date.now(),
        },
        { local: true }
      );
    })
    .catch(() => {});
  handleSessionEnded(sessionId, 'peer_left');
  markSessionEnded(sessionId, 'peer_left');
}

async function handleSignalOffer({ sessionId, sdp }) {
  if (!sessionId || !sdp) return;
  if (state.joinedSessionId && state.joinedSessionId !== sessionId) return;
  try {
    let pc = ensurePeerConnection(sessionId);
    if (!pc) return;
    if (isMediaPcUnhealthy(pc)) {
      resetMediaPeerConnection(sessionId);
      pc = ensurePeerConnection(sessionId);
      if (!pc) return;
    }
    const remote = sdp.type ? sdp : { type: 'offer', sdp };
    if (pc.signalingState !== 'stable') {
      resetMediaPeerConnection(sessionId);
      pc = ensurePeerConnection(sessionId);
      if (!pc) return;
    }
    if (pc.signalingState !== 'stable') {
      console.info('[CALL] oferta ignorada por estado inválido', pc.signalingState);
      return;
    }
    await pc.setRemoteDescription(remote);
    await flushPendingMediaIce(pc);
    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    socket.emit('signal:answer', { sessionId, sdp: pc.localDescription });
  } catch (error) {
    console.error('Erro ao processar oferta remota', error);
  }
}

async function handleSignalAnswer({ sessionId, sdp }) {
  if (!sessionId || !sdp) return;
  if (state.media.sessionId && state.media.sessionId !== sessionId) return;
  try {
    const pc = ensurePeerConnection(sessionId);
    if (!pc) return;
    if (pc.signalingState !== 'have-local-offer' && pc.signalingState !== 'have-remote-pranswer') {
      if (pc.signalingState === 'stable') {
        console.info('[CALL] answer duplicada ignorada', sessionId);
        return;
      }
      console.warn('[CALL] answer ignorada por estado inválido', pc.signalingState);
      return;
    }
    const answer = sdp.type ? sdp : { type: 'answer', sdp };
    await pc.setRemoteDescription(answer);
    await flushPendingMediaIce(pc);
  } catch (error) {
    console.error('Erro ao aplicar answer remota', error);
  }
}

async function handleSignalCandidate({ sessionId, candidate }) {
  if (!sessionId || !candidate) return;
  if (state.media.sessionId && state.media.sessionId !== sessionId) return;
  try {
    const pc = ensurePeerConnection(sessionId);
    if (!pc) return;
    if (!pc.remoteDescription) {
      state.media.pendingRemoteIce.push(candidate);
      return;
    }
    await pc.addIceCandidate(candidate);
  } catch (error) {
    console.error('Erro ao adicionar ICE candidate', error);
  }
}

function setupSocketHandlers() {
  if (!socket) return;
  registerSocketHandler('connect', handleSocketConnect);
  registerSocketHandler('connect_error', handleSocketConnectError);
  registerSocketHandler('disconnect', handleSocketDisconnect);
  registerSocketHandler('queue:updated', handleQueueUpdated);
  registerSocketHandler('session:updated', handleSessionUpdated);
  registerSocketHandler('session:chat:new', handleSessionChat);
  registerSocketHandler('session:command', handleSessionCommandEvent);
  registerSocketHandler('session:status', handleSessionStatus);
  registerSocketHandler('session:ended', handleSessionEndedEvent);
  registerSocketHandler('peer-left', handlePeerLeft);
  registerSocketHandler('signal', handleLegacySignal);
  registerSocketHandler('signal:offer', handleSignalOffer);
  registerSocketHandler('signal:answer', handleSignalAnswer);
  registerSocketHandler('signal:candidate', handleSignalCandidate);
}

function cleanupSession({ rebindHandlers = false } = {}) {
  sessionResources.timeouts.forEach((timeoutId) => clearTimeout(timeoutId));
  sessionResources.timeouts.clear();
  sessionResources.intervals.forEach((intervalId) => clearInterval(intervalId));
  sessionResources.intervals.clear();
  queueAutoRefreshIntervalId = null;
  socketAuthRecoveryInFlight = false;
  socketInvalidTokenCounter = 0;
  socketRealtimeDisabled = false;
  socketRealtimeDisabledNotified = false;
  sessionJoinInFlightId = null;
  sessionResources.observers.forEach((observer) => {
    if (observer && typeof observer.disconnect === 'function') observer.disconnect();
  });
  sessionResources.observers.clear();
  unsubscribeAllSessionRealtime();
  if (socket) {
    sessionResources.socketHandlers.forEach((handler, eventName) => {
      socket.off(eventName, handler);
    });
  }
  sessionResources.socketHandlers.clear();
  cancelScheduledRenders();
  teardownPeerConnection();
  teardownLegacyShare();
  resetCommandState();
  state.whiteboard.queue.length = 0;
  state.whiteboard.buffer.length = 0;
  if (state.whiteboard.bufferTimer) {
    clearTimeout(state.whiteboard.bufferTimer);
    state.whiteboard.bufferTimer = null;
  }
  if (state.whiteboard.rafId) {
    cancelAnimationFrame(state.whiteboard.rafId);
    state.whiteboard.rafId = null;
  }
  clearWhiteboardCanvas();
  setSessionState(SessionStates.IDLE, null);
  state.joinedSessionId = null;
  state.media.sessionId = null;
  state.renderedChatSessionId = null;
  scheduleRender(() => {
    dom.chatThread?.replaceChildren();
  });
  if (rebindHandlers) {
    setupSocketHandlers();
  }
}

window.addEventListener('beforeunload', () => {
  cleanupSession();
});

bootstrap();
