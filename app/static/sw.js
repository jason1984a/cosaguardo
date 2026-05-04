/**
 * CosaGuardo Service Worker
 * Strategia:
 *  - Network-first per HTML, JSON, e API → utente vede sempre dati freschi
 *  - Cache-first per static (CSS, JS, icone, font)
 *  - Auto-update: nuove versioni del SW si attivano IMMEDIATAMENTE,
 *    così quando deployi gli utenti non restano bloccati su codice vecchio.
 *
 * IMPORTANTE: il numero `CACHE_VERSION` va incrementato a ogni deploy
 * che modifica file statici (CSS/JS/icone). In automatico è gestito
 * dal timestamp di build, ma puoi anche bumpare manualmente.
 */

const CACHE_VERSION = "v2026.05.04-2";
const STATIC_CACHE = `cosaguardo-static-${CACHE_VERSION}`;
const RUNTIME_CACHE = `cosaguardo-runtime-${CACHE_VERSION}`;

// File essenziali precaricati all'install (per offline base)
const PRECACHE_URLS = [
  "/static/css/style.css",
  "/static/app.js",
  "/static/js/feedback.js",
  "/static/icons/icon-192.png",
  "/static/icons/icon-512.png",
  "/static/manifest.webmanifest",
];

// ─── INSTALL ─────────────────────────────────────────────────────────────
self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(STATIC_CACHE).then((cache) =>
      // addAll fallisce se anche un solo file fallisce — usiamo add singoli
      // per essere tolleranti ai 404 su file rinominati durante deploy.
      Promise.all(
        PRECACHE_URLS.map((url) =>
          cache.add(url).catch((err) =>
            console.warn(`[SW] Precache skip: ${url}`, err.message)
          )
        )
      )
    ).then(() => self.skipWaiting())  // forza l'attivazione immediata
  );
});

// ─── ACTIVATE: pulisce cache vecchie ─────────────────────────────────────
self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys
          .filter((k) => k.startsWith("cosaguardo-") && !k.endsWith(CACHE_VERSION))
          .map((k) => caches.delete(k))
      )
    ).then(() => self.clients.claim())  // prende controllo subito di tutte le tab
  );
});

// ─── FETCH: routing intelligente per tipo di richiesta ───────────────────
self.addEventListener("fetch", (event) => {
  const req = event.request;
  const url = new URL(req.url);

  // Solo GET — POST/PUT non vanno mai cacheati
  if (req.method !== "GET") return;

  // Skip cross-origin (TMDb, Google, GA4, ecc) — passa diretto alla rete
  if (url.origin !== self.location.origin) return;

  // Skip endpoint admin/auth — evita problemi di sessione
  if (url.pathname.startsWith("/admin") ||
      url.pathname.startsWith("/auth/") ||
      url.pathname.startsWith("/login") ||
      url.pathname.startsWith("/register") ||
      url.pathname.startsWith("/logout") ||
      url.pathname.startsWith("/feedback")) {
    return;
  }

  // Pagine detail (film/serie/persona) → SEMPRE dalla rete, mai cache.
  // Motivo: cachare HTML semi-completo o di film diversi causa "schede vuote"
  // o contenuto sbagliato al back-navigation. Meglio aspettare la rete.
  if (url.pathname.startsWith("/film/") ||
      url.pathname.startsWith("/serie/") ||
      url.pathname.startsWith("/persona/") ||
      url.pathname.startsWith("/results") ||
      url.pathname === "/recommend") {
    event.respondWith(networkOnly(req));
    return;
  }

  // Static files (CSS, JS, immagini, font, manifest, icone) → cache-first
  if (url.pathname.startsWith("/static/") ||
      url.pathname === "/favicon.ico" ||
      url.pathname.endsWith(".webmanifest")) {
    event.respondWith(cacheFirst(req));
    return;
  }

  // Endpoint dinamici JSON → network-first (con fallback cache se offline)
  if (url.pathname.startsWith("/search") ||
      url.pathname.startsWith("/watch-providers") ||
      url.pathname.startsWith("/cinema-news") ||
      url.pathname.startsWith("/news") ||
      url.pathname.startsWith("/home-picks") ||
      url.pathname.startsWith("/tmdb-id") ||
      url.pathname.startsWith("/scopri/json")) {
    event.respondWith(networkFirst(req));
    return;
  }

  // HTML pages → network-first così l'utente vede sempre l'ultima versione
  // ma con fallback a cache offline (utente vedrà l'ultima pagina visitata)
  if (req.mode === "navigate" || req.headers.get("accept")?.includes("text/html")) {
    event.respondWith(networkFirst(req));
    return;
  }

  // Default: passthrough rete
});

// ─── STRATEGIE DI CACHING ────────────────────────────────────────────────

async function cacheFirst(req) {
  const cache = await caches.open(STATIC_CACHE);
  const cached = await cache.match(req);
  if (cached) return cached;

  try {
    const fresh = await fetch(req);
    if (fresh.ok) cache.put(req, fresh.clone());
    return fresh;
  } catch (err) {
    // Offline e file non in cache → 504
    return new Response("Offline", { status: 504, statusText: "Offline" });
  }
}

// Network-only: sempre dalla rete, non cacha mai. Per pagine dinamiche
// (detail film/serie, risultati ricerca) dove servire HTML stale è peggio
// che mostrare un breve loader del browser.
async function networkOnly(req) {
  try {
    return await fetch(req);
  } catch (err) {
    // Se davvero offline, mostra pagina cortesia
    if (req.mode === "navigate") {
      return offlinePage();
    }
    return new Response(JSON.stringify({ error: "offline" }), {
      status: 503,
      headers: { "Content-Type": "application/json" },
    });
  }
}

async function networkFirst(req) {
  const cache = await caches.open(RUNTIME_CACHE);

  try {
    const fresh = await fetch(req);
    if (fresh.ok) {
      // Salva in cache solo le response 200 OK
      cache.put(req, fresh.clone()).catch(() => {});
    }
    return fresh;
  } catch (err) {
    // Network down → fallback cache
    const cached = await cache.match(req);
    if (cached) return cached;

    // Niente in cache: per HTML mostra una pagina offline minimale
    if (req.mode === "navigate") {
      return offlinePage();
    }

    // Per JSON: response vuota
    return new Response(JSON.stringify({ error: "offline" }), {
      status: 503,
      headers: { "Content-Type": "application/json" },
    });
  }
}

// Pagina HTML mostrata quando l'utente è offline
function offlinePage() {
  return new Response(
    `<!DOCTYPE html><html lang="it"><head><meta charset="utf-8">
     <title>Offline — CosaGuardo</title>
     <meta name="viewport" content="width=device-width,initial-scale=1">
     <style>
       body{font-family:system-ui,sans-serif;background:#0b1020;color:#f5f7ff;
            display:flex;align-items:center;justify-content:center;
            min-height:100vh;margin:0;padding:20px;text-align:center;}
       .box{max-width:380px;}
       h1{font-size:1.4rem;margin:0 0 12px;}
       p{color:#aab3d1;line-height:1.5;}
       button{margin-top:18px;padding:10px 20px;border:none;border-radius:10px;
              background:#6ea8fe;color:#0b1020;font-weight:700;cursor:pointer;}
     </style></head><body><div class="box">
     <h1>📡 Sei offline</h1>
     <p>CosaGuardo ha bisogno di una connessione per mostrarti consigli aggiornati.</p>
     <button onclick="location.reload()">Riprova</button>
     </div></body></html>`,
    { status: 503, headers: { "Content-Type": "text/html; charset=utf-8" } }
  );
}

// ─── MESSAGE HANDLER (per skip-waiting forzato lato client) ──────────────
self.addEventListener("message", (event) => {
  if (event.data && event.data.type === "SKIP_WAITING") {
    self.skipWaiting();
  }
});
