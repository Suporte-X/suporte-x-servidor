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

ensureFirebaseApp();
const auth = ensureFirebaseAuth();
console.info('[Tech Login] firebaseConfig carregado', resolveFirebaseConfig());

const mapAuthError = (error) => {
  const code = error?.code || '';
  if (code === 'auth/unauthorized-domain') return 'Domínio não autorizado no Firebase Auth. Adicione este domínio em Authorized domains.';
  if (code === 'auth/operation-not-allowed') return 'Provider desativado no Firebase Console. Ative o login com Google.';
  if (code === 'auth/popup-blocked') return 'Popup bloqueado pelo navegador. Permita popups e tente novamente.';
  if (code === 'auth/popup-closed-by-user') return 'Popup de autenticação fechado antes de concluir o login.';
  if (code === 'auth/invalid-api-key') return 'API key inválida no config do Firebase carregado.';
  return error?.message || 'Não foi possível autenticar.';
};

const validateTechAccess = async (user) => {
  const token = await user.getIdToken(true);
  const res = await fetch('/api/auth/me', { headers: { Authorization: `Bearer ${token}` } });
  if (!res.ok) {
    await signOut(auth).catch(() => {});
    if (res.status === 403) {
      const payload = await res.json().catch(() => ({}));
      if (payload?.error === 'tech_inactive') throw new Error('Conta desativada.');
      throw new Error('Sem permissão.');
    }
    throw new Error('Falha ao validar acesso técnico.');
  }
  return res.json();
};

const completeLogin = async (user) => {
  setMessage('Validando acesso técnico...');
  await validateTechAccess(user);
  window.location.replace(nextPath);
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
      setMessage(mapAuthError(err), true);
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
      setMessage(mapAuthError(err), true);
    }
  });

  const reason = params.get('reason');
  if (reason === 'not_tech' || reason === 'access_denied') setMessage('Sem permissão.', true);
  if (reason === 'inactive' || reason === 'tech_inactive') setMessage('Conta desativada.', true);
  if (reason === 'signed_out') setMessage('Sessão encerrada com sucesso.');
};

init().catch((error) => {
  setMessage(mapAuthError(error), true);
});
