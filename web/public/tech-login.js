import {
  GoogleAuthProvider,
  browserLocalPersistence,
  setPersistence,
  signInWithPopup,
  signInWithEmailAndPassword,
  onAuthStateChanged,
  signOut,
} from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js';
import { ensureFirebaseApp, ensureFirebaseAuth, resolveFirebaseConfig } from '/firebase-client.js';

const dom = {
  googleLoginBtn: document.getElementById('googleLoginBtn'),
  emailLoginBtn: document.getElementById('emailLoginBtn'),
  emailLoginForm: document.getElementById('emailLoginForm'),
  emailInput: document.getElementById('emailInput'),
  passwordInput: document.getElementById('passwordInput'),
  turnstileContainer: document.getElementById('techLoginTurnstile'),
  loginMessage: document.getElementById('loginMessage'),
};

const params = new URLSearchParams(window.location.search);
const nextPath = params.get('next') || '/central.html';
const TURNSTILE_SCRIPT_ID = 'tech-login-cloudflare-turnstile';

const turnstileState = {
  enabled: false,
  siteKey: '',
  widgetId: null,
  scriptPromise: null,
  token: '',
};

const setMessage = (text, isError = false) => {
  if (!dom.loginMessage) return;
  dom.loginMessage.textContent = text;
  dom.loginMessage.style.color = isError ? '#fca5a5' : '';
};

const setBusy = (busy) => {
  if (dom.googleLoginBtn) dom.googleLoginBtn.disabled = busy;
  if (dom.emailLoginBtn) dom.emailLoginBtn.disabled = busy;
  if (dom.emailInput) dom.emailInput.disabled = busy;
  if (dom.passwordInput) dom.passwordInput.disabled = busy;
};

const mapAuthError = (error) => {
  const code = error?.code || '';
  if (code === 'auth/unauthorized-domain') {
    return 'Dom\u00EDnio n\u00E3o autorizado no Firebase Auth. Adicione este dom\u00EDnio em Authorized domains.';
  }
  if (code === 'auth/operation-not-allowed') return 'Provider desativado no Firebase Console. Ative o login com Google.';
  if (code === 'auth/popup-blocked') return 'Popup bloqueado pelo navegador. Permita popups e tente novamente.';
  if (code === 'auth/popup-closed-by-user') return 'Popup de autentica\u00E7\u00E3o fechado antes de concluir o login.';
  if (code === 'auth/invalid-api-key') return 'API key inv\u00E1lida no config do Firebase carregado.';
  if (code === 'auth/invalid-credential') return 'E-mail ou senha inv\u00E1lidos.';
  if (code === 'auth/user-not-found') return 'E-mail ou senha inv\u00E1lidos.';
  if (code === 'auth/wrong-password') return 'E-mail ou senha inv\u00E1lidos.';
  if (code === 'auth/too-many-requests') return 'Muitas tentativas. Aguarde alguns minutos e tente novamente.';
  return error?.message || 'N\u00E3o foi poss\u00EDvel autenticar.';
};

const resolveTurnstileConfig = () => {
  const source = window.__CENTRAL_CONFIG__?.techLoginTurnstile || null;
  const siteKey = typeof source?.siteKey === 'string' ? source.siteKey.trim() : '';
  const enabled = source?.enabled === true && Boolean(siteKey);
  return { enabled, siteKey };
};


const loadTurnstileScript = () => {
  if (window.turnstile?.render) return Promise.resolve();
  if (turnstileState.scriptPromise) return turnstileState.scriptPromise;

  turnstileState.scriptPromise = new Promise((resolve, reject) => {
    const existingScript = document.getElementById(TURNSTILE_SCRIPT_ID);
    if (existingScript) {
      existingScript.addEventListener('load', () => resolve(), { once: true });
      existingScript.addEventListener('error', () => reject(new Error('captcha_script_failed')), { once: true });
      return;
    }

    const script = document.createElement('script');
    script.id = TURNSTILE_SCRIPT_ID;
    script.src = 'https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit';
    script.async = true;
    script.defer = true;
    script.onload = () => resolve();
    script.onerror = () => reject(new Error('captcha_script_failed'));
    document.head.appendChild(script);
  });

  return turnstileState.scriptPromise;
};


const waitForTurnstile = async (timeoutMs = 12000) => {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (window.turnstile?.render) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error('captcha_unavailable');
};


const resetTurnstileWidget = () => {
  turnstileState.token = '';
  if (!turnstileState.enabled) return;
  if (!window.turnstile?.reset) return;
  if (turnstileState.widgetId == null) return;
  window.turnstile.reset(turnstileState.widgetId);
};



const initializeTurnstile = async () => {
  const config = resolveTurnstileConfig();
  turnstileState.enabled = config.enabled;
  turnstileState.siteKey = config.siteKey;
  turnstileState.token = '';

  if (!turnstileState.enabled) {
    if (dom.turnstileContainer) dom.turnstileContainer.style.display = 'none';
    throw new Error('Prote\u00E7\u00E3o anti-bot indispon\u00EDvel. Tente novamente em instantes.');
  }

  if (!dom.turnstileContainer) {
    throw new Error('Prote\u00E7\u00E3o anti-bot indispon\u00EDvel. Tente novamente em instantes.');
  }

  setMessage('Carregando prote\u00E7\u00E3o anti-bot...');
  dom.turnstileContainer.style.display = '';
  await loadTurnstileScript();
  await waitForTurnstile();

  turnstileState.widgetId = window.turnstile.render(dom.turnstileContainer, {
    sitekey: turnstileState.siteKey,
    theme: 'dark',
    action: 'tech_login',
    callback: (token) => {
      turnstileState.token = typeof token === 'string' ? token : '';
      if (turnstileState.token) setMessage('');
    },
    'expired-callback': () => {
      turnstileState.token = '';
      setMessage('Valida\u00E7\u00E3o expirada. Confirme novamente para entrar.', true);
    },
    'error-callback': () => {
      turnstileState.token = '';
      setMessage('N\u00E3o foi poss\u00EDvel carregar a valida\u00E7\u00E3o anti-bot. Tente novamente em instantes.', true);
    },
  });

  setMessage('');
};

const verifyTurnstile = async (action) => {
  if (!turnstileState.enabled) {
    throw new Error('Prote\u00E7\u00E3o anti-bot indispon\u00EDvel. Tente novamente em instantes.');
  }
  if (!window.turnstile || turnstileState.widgetId == null) {
    throw new Error('Prote\u00E7\u00E3o anti-bot indispon\u00EDvel. Tente novamente em instantes.');
  }

  const currentToken =
    turnstileState.token ||
    (typeof window.turnstile.getResponse === 'function' ? window.turnstile.getResponse(turnstileState.widgetId) : '');
  const token = typeof currentToken === 'string' ? currentToken.trim() : '';
  if (!token) {
    throw new Error('Confirme que voc\u00EA \u00E9 humano antes de continuar.');
  }

  const response = await fetch('/api/auth/turnstile/verify', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ token, action }),
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    if (payload?.error === 'captcha_required') {
      throw new Error('Confirme que voc\u00EA \u00E9 humano antes de continuar.');
    }
    if (payload?.error === 'captcha_unavailable' || payload?.error === 'captcha_verification_failed') {
      throw new Error('Prote\u00E7\u00E3o anti-bot indispon\u00EDvel no momento. Tente novamente em instantes.');
    }
    throw new Error(payload?.message || 'Falha na valida\u00E7\u00E3o anti-bot. Tente novamente.');
  }
};

ensureFirebaseApp();
const auth = ensureFirebaseAuth();
console.info('[Tech Login] firebaseConfig carregado', resolveFirebaseConfig());

const validateTechAccess = async (user) => {
  const token = await user.getIdToken(true);
  const res = await fetch('/api/auth/me', { headers: { Authorization: `Bearer ${token}` } });
  if (!res.ok) {
    await signOut(auth).catch(() => {});
    if (res.status === 403) {
      const payload = await res.json().catch(() => ({}));
      if (payload?.error === 'tech_inactive') throw new Error('Conta desativada.');
      throw new Error('Sem permiss\u00E3o.');
    }
    throw new Error('Falha ao validar acesso t\u00E9cnico.');
  }
  return res.json();
};

const completeLogin = async (user) => {
  setMessage('Validando acesso t\u00E9cnico...');
  await validateTechAccess(user);
  window.location.replace(nextPath);
};

const init = async () => {
  await setPersistence(auth, browserLocalPersistence);
  try {
    await initializeTurnstile();
  } catch (error) {
    setMessage(error?.message || 'Falha ao iniciar valida\u00E7\u00E3o anti-bot.', true);
  }

  onAuthStateChanged(auth, async (user) => {
    if (!user) return;
    try {
      await completeLogin(user);
    } catch (err) {
      setMessage(err.message || 'N\u00E3o foi poss\u00EDvel autenticar.', true);
    }
  });

  dom.googleLoginBtn?.addEventListener('click', async () => {
    setBusy(true);
    try {
      setMessage('Validando anti-bot...');
      await verifyTurnstile('tech_login_google');
      const provider = new GoogleAuthProvider();
      const cred = await signInWithPopup(auth, provider);
      await completeLogin(cred.user);
      resetTurnstileWidget();
    } catch (err) {
      resetTurnstileWidget();
      setMessage(mapAuthError(err), true);
    } finally {
      setBusy(false);
    }
  });

  dom.emailLoginForm?.addEventListener('submit', async (event) => {
    event.preventDefault();
    setBusy(true);
    try {
      const email = dom.emailInput?.value?.trim();
      const password = dom.passwordInput?.value || '';
      setMessage('Validando anti-bot...');
      await verifyTurnstile('tech_login_password');
      const cred = await signInWithEmailAndPassword(auth, email, password);
      await completeLogin(cred.user);
      resetTurnstileWidget();
    } catch (err) {
      resetTurnstileWidget();
      setMessage(mapAuthError(err), true);
    } finally {
      setBusy(false);
    }
  });

  const reason = params.get('reason');
  if (reason === 'not_tech' || reason === 'access_denied') setMessage('Sem permiss\u00E3o.', true);
  if (reason === 'inactive' || reason === 'tech_inactive') setMessage('Conta desativada.', true);
  if (reason === 'signed_out') setMessage('Sess\u00E3o encerrada com sucesso.');
};

init().catch((error) => {
  setMessage(mapAuthError(error), true);
});
