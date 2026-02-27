import { initializeApp, getApps } from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js';
import { getAuth } from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js';

export const DEFAULT_FIREBASE_CONFIG = {
  apiKey: 'AIzaSyAooFHhk6ewqKPkXVX48CCWVVoV0eOUesI',
  authDomain: 'suporte-x-19ae8.firebaseapp.com',
  projectId: 'suporte-x-19ae8',
  storageBucket: 'suporte-x-19ae8.firebasestorage.app',
  messagingSenderId: '603259295557',
  appId: '1:603259295557:web:00ca6e9fe02ff5fbe0902c',
  measurementId: 'G-KF1CQYGZVF',
};

const REQUIRED_FIREBASE_KEYS = ['apiKey', 'authDomain', 'projectId'];

let firebaseConfigCache = null;
let firebaseAppInstance = null;
let authInstance = null;

const isValidFirebaseConfig = (config) => {
  if (!config || typeof config !== 'object') return false;
  return REQUIRED_FIREBASE_KEYS.every(
    (key) => typeof config[key] === 'string' && config[key].trim().length > 0
  );
};

const mergeWithDefaultFirebaseConfig = (config) => {
  if (!config || typeof config !== 'object') return { ...DEFAULT_FIREBASE_CONFIG };
  const filteredEntries = Object.entries(config).filter(([, value]) => value !== undefined && value !== null);
  return { ...DEFAULT_FIREBASE_CONFIG, ...Object.fromEntries(filteredEntries) };
};

export const resolveFirebaseConfig = () => {
  if (firebaseConfigCache) return firebaseConfigCache;
  const sources = [
    { name: 'window.__FIREBASE_CONFIG__', config: typeof window !== 'undefined' ? window.__FIREBASE_CONFIG__ : null },
    { name: 'window.firebaseConfig', config: typeof window !== 'undefined' ? window.firebaseConfig : null },
    { name: 'window.__firebaseConfig__', config: typeof window !== 'undefined' ? window.__firebaseConfig__ : null },
    { name: 'window.__firebaseConfig', config: typeof window !== 'undefined' ? window.__firebaseConfig : null },
    { name: 'window.__CENTRAL_CONFIG__.firebase', config: typeof window !== 'undefined' ? window.__CENTRAL_CONFIG__?.firebase : null },
    { name: 'window.__APP_CONFIG__.firebase', config: typeof window !== 'undefined' ? window.__APP_CONFIG__?.firebase : null },
  ];

  for (const source of sources) {
    if (!isValidFirebaseConfig(source.config)) continue;
    firebaseConfigCache = mergeWithDefaultFirebaseConfig(source.config);
    console.info('[Firebase] Config carregada de', source.name, firebaseConfigCache);
    return firebaseConfigCache;
  }

  firebaseConfigCache = mergeWithDefaultFirebaseConfig(DEFAULT_FIREBASE_CONFIG);
  console.info('[Firebase] Config carregada de DEFAULT_FIREBASE_CONFIG', firebaseConfigCache);
  return firebaseConfigCache;
};

export const ensureFirebaseApp = () => {
  if (firebaseAppInstance) return firebaseAppInstance;
  const config = resolveFirebaseConfig();
  const apps = getApps();
  firebaseAppInstance = apps.length ? apps[0] : initializeApp(config);
  return firebaseAppInstance;
};

export const ensureFirebaseAuth = () => {
  if (authInstance) return authInstance;
  const app = ensureFirebaseApp();
  authInstance = getAuth(app);
  return authInstance;
};

