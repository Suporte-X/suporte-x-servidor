import { initializeApp, getApps } from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js';
import {
  getAuth,
  GoogleAuthProvider,
  browserLocalPersistence,
  setPersistence,
  signInWithPopup,
  signInWithEmailAndPassword,
  onAuthStateChanged,
  signOut,
} from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js';

const DEFAULT_FIREBASE_CONFIG = {
  apiKey: 'AIzaSyAooFHhk6ewqKPkXVX48CCWVVoV0eOUesI',
  authDomain: 'suporte-x-19ae8.firebaseapp.com',
  projectId: 'suporte-x-19ae8',
  storageBucket: 'suporte-x-19ae8.firebasestorage.app',
  messagingSenderId: '603259295557',
  appId: '1:603259295557:web:00ca6e9fe02ff5fbe0902c',
};

const dom = {
  googleLoginBtn: document.getElementById('googleLoginBtn'),
  emailLoginForm: document.getElementById('emailLoginForm'),
  emailInput: document.getElementById('emailInput'),
  passwordInput: document.getElementById('passwordInput'),
  loginMessage: document.getElementById('loginMessage'),
};

const params = new URLSearchParams(window.location.search);
const nextPath = params.get('next') || '/central.html';

const setMessage = (text, isError = false) => {
  if (!dom.loginMessage) return;
  dom.loginMessage.textContent = text;
  dom.loginMessage.style.color = isError ? '#fca5a5' : '';
};

const config = window.__CENTRAL_CONFIG__?.firebase || DEFAULT_FIREBASE_CONFIG;
const app = getApps().length ? getApps()[0] : initializeApp(config);
const auth = getAuth(app);

const validateTechAccess = async (user) => {
  const token = await user.getIdToken(true);
  const res = await fetch('/api/auth/me', { headers: { Authorization: `Bearer ${token}` } });
  if (!res.ok) {
    await signOut(auth).catch(() => {});
    if (res.status === 403) {
      throw new Error('Acesso negado: sua conta não é técnico ativo.');
    }
    throw new Error('Falha ao validar acesso técnico.');
  }
  return res.json();
};

const completeLogin = async (user) => {
  setMessage('Validando acesso técnico...');
  await validateTechAccess(user);
  window.location.href = nextPath;
};

const init = async () => {
  await setPersistence(auth, browserLocalPersistence);

  onAuthStateChanged(auth, async (user) => {
    if (!user) return;
    try {
      await completeLogin(user);
    } catch (err) {
      setMessage(err.message || 'Não foi possível autenticar.', true);
    }
  });

  dom.googleLoginBtn?.addEventListener('click', async () => {
    try {
      const provider = new GoogleAuthProvider();
      const cred = await signInWithPopup(auth, provider);
      await completeLogin(cred.user);
    } catch (err) {
      setMessage(err.message || 'Erro no login Google.', true);
    }
  });

  dom.emailLoginForm?.addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      const email = dom.emailInput?.value?.trim();
      const password = dom.passwordInput?.value || '';
      const cred = await signInWithEmailAndPassword(auth, email, password);
      await completeLogin(cred.user);
    } catch (err) {
      setMessage(err.message || 'Erro no login por e-mail/senha.', true);
    }
  });

  const reason = params.get('reason');
  if (reason === 'access_denied') setMessage('Acesso negado. Somente técnicos ativos podem entrar.', true);
  if (reason === 'signed_out') setMessage('Sessão encerrada com sucesso.');
};

init().catch((error) => {
  setMessage(error.message || 'Falha ao iniciar login técnico.', true);
});
