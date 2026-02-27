const CACHE_NAME = 'CACHE_V4';
const PRECACHE_URLS = [
  '/style.css',
  '/central.css',
  '/common.js',
  '/central.js',
  '/icon-192.png',
  '/icon-512.png',
  '/manifest.json',
];

const shouldBypassCache = (request, url) => {
  if (request.method !== 'GET') return true;
  if (request.headers.get('cache-control')?.includes('no-store')) return true;
  if (url.pathname === '/central.html') return true;
  if (url.pathname === '/tech-login.html') return true;
  if (url.pathname.startsWith('/tech-login')) return true;
  if (url.pathname === '/firebase-client.js') return true;
  if (url.pathname === '/central-config.js') return true;
  if (url.pathname.startsWith('/api/')) return true;
  if (url.pathname.startsWith('/socket.io/')) return true;
  return false;
};

const shouldSkipCachePut = (request, response, url) => {
  if (!response || response.status !== 200) return true;
  if (response.type !== 'basic' && response.type !== 'default') return true;
  if (shouldBypassCache(request, url)) return true;
  const cacheControl = response.headers.get('cache-control') || '';
  if (cacheControl.includes('no-store') || cacheControl.includes('private')) return true;
  return false;
};

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches
      .open(CACHE_NAME)
      .then((cache) => cache.addAll(PRECACHE_URLS))
      .catch((error) => console.warn('Falha ao pré-cachear assets', error))
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) =>
        Promise.all(
          keys
            .filter((key) => key.startsWith('CACHE_') && key !== CACHE_NAME)
            .map((key) => caches.delete(key))
        )
      )
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const { request } = event;
  if (request.method !== 'GET') return;

  const url = new URL(request.url);

  if (request.destination === 'document' || shouldBypassCache(request, url)) {
    event.respondWith(fetch(request, { cache: 'no-store' }).catch(() => caches.match(request)));
    return;
  }

  if (url.origin === self.location.origin) {
    event.respondWith(
      caches.open(CACHE_NAME).then(async (cache) => {
        const cached = await cache.match(request);
        if (cached) return cached;
        try {
          const response = await fetch(request);
          if (!shouldSkipCachePut(request, response, url)) {
            cache.put(request, response.clone());
          }
          return response;
        } catch (error) {
          if (cached) return cached;
          throw error;
        }
      })
    );
    return;
  }

  event.respondWith(fetch(request));
});
