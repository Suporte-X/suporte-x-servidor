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
  recaptchaContainer: document.getElementById('techLoginRecaptcha'),
  loginMessage: document.getElementById('loginMessage'),
};

const params = new URLSearchParams(window.location.search);
const nextPath = params.get('next') || '/central.html';
const RECAPTCHA_SCRIPT_ID = 'tech-login-recaptcha-enterprise';

const recaptchaState = {
  enabled: false,
  siteKey: '',
  mode: 'none',
  widgetId: null,
  scriptPromise: null,
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

const resolveRecaptchaConfig = () => {
  const source = window.__CENTRAL_CONFIG__?.techLoginRecaptcha || null;
  const siteKey = typeof source?.siteKey === 'string' ? source.siteKey.trim() : '';
  const enabled = source?.enabled === true && Boolean(siteKey);
  return {
    enabled,
    siteKey,
  };
};

const loadRecaptchaEnterpriseScript = () => {
  if (window.grecaptcha?.enterprise) return Promise.resolve();
  if (recaptchaState.scriptPromise) return recaptchaState.scriptPromise;

  recaptchaState.scriptPromise = new Promise((resolve, reject) => {
    const existingScript = document.getElementById(RECAPTCHA_SCRIPT_ID);
    if (existingScript) {
      existingScript.addEventListener('load', () => resolve(), { once: true });
      existingScript.addEventListener('error', () => reject(new Error('captcha_script_failed')), { once: true });
      return;
    }

    const script = document.createElement('script');
    script.id = RECAPTCHA_SCRIPT_ID;
    script.src = 'https://www.google.com/recaptcha/enterprise.js';
    script.async = true;
    script.defer = true;
    script.onload = () => resolve();
    script.onerror = () => reject(new Error('captcha_script_failed'));
    document.head.appendChild(script);
  });

  return recaptchaState.scriptPromise;
};

const waitForRecaptchaEnterprise = async (timeoutMs = 12000) => {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (window.grecaptcha?.enterprise) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error('captcha_unavailable');
};

const resetRecaptchaWidget = () => {
  if (!recaptchaState.enabled) return;
  if (recaptchaState.mode !== 'widget') return;
  if (!window.grecaptcha?.enterprise) return;
  if (recaptchaState.widgetId == null) return;
  window.grecaptcha.enterprise.reset(recaptchaState.widgetId);
};

const executeRecaptchaToken = async (action) => {
  if (!window.grecaptcha?.enterprise || typeof window.grecaptcha.enterprise.execute !== 'function') {
    throw new Error('Prote\u00E7\u00E3o anti-bot indispon\u00EDvel. Recarregue a p\u00E1gina e tente novamente.');
  }

  return new Promise((resolve, reject) => {
    window.grecaptcha.enterprise.ready(async () => {
      try {
        const token = await window.grecaptcha.enterprise.execute(recaptchaState.siteKey, { action });
        resolve(token);
      } catch (error) {
        reject(error);
      }
    });
  });
};

const initializeRecaptcha = async () => {
  const config = resolveRecaptchaConfig();
  recaptchaState.enabled = config.enabled;
  recaptchaState.siteKey = config.siteKey;

  if (!recaptchaState.enabled) {
    recaptchaState.mode = 'none';
    if (dom.recaptchaContainer) dom.recaptchaContainer.style.display = 'none';
    return;
  }

  setMessage('Carregando prote\u00E7\u00E3o anti-bot...');
  await loadRecaptchaEnterpriseScript();
  await waitForRecaptchaEnterprise();
  const enterprise = window.grecaptcha?.enterprise;
  const canRender = typeof enterprise?.render === 'function' && Boolean(dom.recaptchaContainer);
  const canExecute = typeof enterprise?.execute === 'function';

  if (canRender) {
    recaptchaState.widgetId = enterprise.render(dom.recaptchaContainer, {
      sitekey: recaptchaState.siteKey,
      theme: 'dark',
    });
    recaptchaState.mode = 'widget';
    setMessage('');
    return;
  }

  if (dom.recaptchaContainer) {
    dom.recaptchaContainer.style.display = 'none';
  }

  if (canExecute) {
    recaptchaState.mode = 'execute';
    setMessage('');
    return;
  }

  throw new Error('Prote\u00E7\u00E3o anti-bot indispon\u00EDvel. Recarregue a p\u00E1gina e tente novamente.');
};

const verifyRecaptcha = async (action) => {
  if (!recaptchaState.enabled) return;

  let token = '';
  if (recaptchaState.mode === 'widget') {
    if (!window.grecaptcha?.enterprise || recaptchaState.widgetId == null) {
      throw new Error('Prote\u00E7\u00E3o anti-bot indispon\u00EDvel. Recarregue a p\u00E1gina e tente novamente.');
    }

    token = window.grecaptcha.enterprise.getResponse(recaptchaState.widgetId);
    if (!token) {
      throw new Error('Confirme o reCAPTCHA antes de continuar.');
    }
  } else if (recaptchaState.mode === 'execute') {
    token = await executeRecaptchaToken(action);
    if (typeof token !== 'string' || !token.trim()) {
      throw new Error('Falha na valida\u00E7\u00E3o anti-bot. Recarregue a p\u00E1gina e tente novamente.');
    }
  } else {
    throw new Error('Prote\u00E7\u00E3o anti-bot indispon\u00EDvel. Recarregue a p\u00E1gina e tente novamente.');
  }

  const response = await fetch('/api/auth/recaptcha/verify', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ token, action }),
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    if (payload?.error === 'captcha_required') {
      throw new Error('Confirme o reCAPTCHA antes de continuar.');
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
    await initializeRecaptcha();
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
      await verifyRecaptcha('tech_login_google');
      const provider = new GoogleAuthProvider();
      const cred = await signInWithPopup(auth, provider);
      await completeLogin(cred.user);
      resetRecaptchaWidget();
    } catch (err) {
      resetRecaptchaWidget();
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
      await verifyRecaptcha('tech_login_password');
      const cred = await signInWithEmailAndPassword(auth, email, password);
      await completeLogin(cred.user);
      resetRecaptchaWidget();
    } catch (err) {
      resetRecaptchaWidget();
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
