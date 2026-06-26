# 🔴 Cloudflare Cache DYNAMIC — Dossier di ripresa

**Data**: 07 giugno 2026 (notte, sessione interrotta per stanchezza)
**Stato**: setup Cloudflare completato al 90%, ma cache edge NON funziona
**Sintomo**: `cf-cache-status: DYNAMIC` su OGNI URL pubblica testata, anche dopo tutti i fix tentati

---

## 1. Setup Cloudflare COMPLETATO (non rifare)

Tutto questo è stato fatto, verificato, e funziona:

✅ **Account Cloudflare Free** creato su `mfantini84@gmail.com`
✅ **Dominio `cosaguardo.com`** aggiunto al pannello Cloudflare → status "Active"
✅ **Nameserver** cambiati su Namecheap da Namecheap BasicDNS a Custom DNS:
   - `kipp.ns.cloudflare.com`
   - `zainab.ns.cloudflare.com`
✅ **Propagazione DNS** completata (verificata via whatsmydns + whois)
✅ **DNS records importati** correttamente (9 record: A, CNAME www, 5x MX email forwarding, 2x TXT Google + SPF)
✅ **Universal SSL Certificate** active, copre `*.cosaguardo.com, cosaguardo.com`, scadenza 2026-09-04 (Managed, auto-renew)
✅ **SSL/TLS encryption mode**: **Full (strict)** (verificato HTTPS end-to-end)
✅ **Always Use HTTPS**: ON
✅ **Automatic HTTPS Rewrites**: ON
✅ **TLS 1.3**: Enabled
✅ **HTTP/2, HTTP/3, HTTP/2 to Origin**: Enabled
✅ **0-RTT Connection Resumption**: Enabled
✅ **Early Hints**: Enabled
✅ **Brotli**: attivo (default su Free, header `content-encoding: br` confermato)
✅ **Proxy ON** sui 2 record DNS principali:
   - A `cosaguardo.com` → `216.24.57.1` (Render) → **Proxied** (arancione)
   - CNAME `www` → `cosaguardo.onrender.com` → **Proxied** (arancione)
✅ **MX e TXT records**: tutti in **DNS only** (grigio), corretto

**Sito accessibile e funzionante** via Cloudflare. Non rifare nessuno di questi step.

---

## 2. Cosa stiamo cercando di fare

**Obiettivo**: ridurre la banda Render (fatturata $45 di overage sui 100GB inclusi del piano Starter, totale fattura $52.44 di maggio 2026) cachando le pagine pubbliche statiche su Cloudflare CDN.

**Stima impatto atteso**: -50-70% banda Render (cioè da 255 GB/mese a 80-130 GB/mese).

**URL pubbliche cachable** (contenuto identico per tutti gli utenti, alta volume crawler):
- `/migliori-*` (es. `/migliori-thriller-su-netflix`) — pagine SEO programmatic
- `/dove-vedere` (hub piattaforme)
- `/dove-vedere/{slug}` (es. `/dove-vedere/stranger-things`)
- `/come/*` (NB: nel middleware backend NON ho incluso `/come/`, vedi sez. 3)
- `/come-funziona` (statica)
- `/cosa-serve` (statica)
- `/film/{id}` (es. `/film/693134`)
- `/serie/{id}` (es. `/serie/1396`)
- `/persona/{id}` (es. `/persona/287`)

**URL NON cachable** (dynamic, user-specific):
- `/` (homepage personalizzata se loggato)
- `/scopri*` (filtri dinamici)
- `/admin/*` (admin tools)
- `/api/*` (API endpoints)
- `/login*`, `/register*`, `/logout*`, `/profile*`, `/raccolta*`
- Tutti i POST

**Decisione strategica scelta** ("Opzione A" nel nostro dialogo): cacha aggressivamente `/film/*` e `/serie/*` anche se hanno elementi user-specifici (es. "Aggiungi alla raccolta"). Accettiamo che utenti loggati vedano versione "anonima" delle pagine. Il click POST funziona comunque, lo stato si aggiorna alla prossima sessione/hard reload.

---

## 3. Backend modifications COMPLETED (sono GIUSTE, non rifare)

### File modificato: `app/main.py`

Aggiunto **middleware `cloudflare_cache_headers`** subito dopo `head_to_get_fallback` (riga ~566) e prima di `init_db()`.

**Cosa fa**:
1. Su URL nella whitelist cachable, **rimuove `Vary: Cookie`** dal response header (mantenendo altri Vary come `Accept-Encoding`)
2. Emette esplicito `Cache-Control: public, max-age=7200, s-maxage=14400`
3. Su altre URL, NON tocca niente

**Whitelist regex usata** in backend middleware:
```python
_CACHEABLE_PATH_PATTERNS = [
    re.compile(r"^/migliori-"),         # /migliori-thriller-su-netflix, ecc.
    re.compile(r"^/dove-vedere(/|$)"),  # /dove-vedere e /dove-vedere/{slug}
    re.compile(r"^/film/\d+"),          # /film/{tmdb_id}
    re.compile(r"^/serie/\d+"),         # /serie/{tmdb_id}
    re.compile(r"^/persona/\d+"),       # /persona/{tmdb_id}
    re.compile(r"^/come-funziona$"),    # /come-funziona (statica)
    re.compile(r"^/cosa-serve$"),       # /cosa-serve (statica)
]
```

**NOTA**: `/come/*` (es. `/come/michael-jordan`) NON è nel middleware backend, ma è nelle Cache Rules Cloudflare. Disallineamento minore, da risolvere ma non blocker.

**Validazione middleware backend**:

Lanciando `curl -I https://cosaguardo.com/migliori-film-thriller-su-netflix` si vede:

```
Cache-Control: public, max-age=7200, s-maxage=14400   ← OK, emesso dal middleware
vary: Accept-Encoding                                  ← OK, no più Cookie
```

✅ **Il backend funziona PERFETTAMENTE**. Non va modificato. Headers sono esattamente quelli giusti per essere cachati da Cloudflare.

---

## 4. Il problema: Cloudflare ignora gli header

Anche con header corretti dal backend, Cloudflare risponde:

```
cf-cache-status: DYNAMIC
```

Su **OGNI** URL testata (ho provato `/dove-vedere`, `/migliori-film-thriller-su-netflix`, `/film/693134`).

E **OGNI request arriva a Render** (verificato perché `rndr-id` cambia ad ogni request → Render ha generato una nuova response, NON è cache hit).

---

## 5. Cosa abbiamo provato

### Tentativo 1 — Cache Rules nuove (Caching → Cache Rules)

**Rule 1: "Cache pubblic pages - aggressive"** (creata, Active, Order 2)

Expression Preview (verificata pulita, niente più virgolette accidentali):
```
(http.request.uri.path wildcard r"/migliori-*") or
(http.request.uri.path eq "/dove-vedere") or
(http.request.uri.path wildcard r"/dove-vedere/*") or
(http.request.uri.path wildcard r"/come/*") or
(http.request.uri.path eq "/come-funziona") or
(http.request.uri.path eq "/cosa-serve") or
(http.request.uri.path wildcard r"/film/*") or
(http.request.uri.path wildcard r"/serie/*") or
(http.request.uri.path wildcard r"/persona/*")
```

Action:
- Cache eligibility: **Eligible for cache** ✅
- Edge TTL: **Ignore cache-control header and use this TTL** → **4 hours** ✅
- Browser TTL: **Override origin and use this TTL** → **2 hours** ✅
- Cache key: lasciato default (no Custom)
- Cookie bypass: **NON disponibile su Free** (richiede Enterprise)

**Risultato**: DYNAMIC ❌

**Rule 2: "Bypass cache - dynamic/user pages"** (creata, Active, Order 1)

Expression Preview:
```
(http.request.uri.path eq "/") or
(http.request.uri.path wildcard r"/scopri*") or
(http.request.uri.path wildcard r"/admin/*") or
(http.request.uri.path wildcard r"/api/*") or
(http.request.uri.path wildcard r"/login*") or
(http.request.uri.path wildcard r"/register*") or
(http.request.uri.path wildcard r"/logout*") or
(http.request.uri.path wildcard r"/profile*") or
(http.request.uri.path wildcard r"/raccolta*") or
(http.request.method eq "POST")
```

Action: Bypass cache. Active.

### Tentativo 2 — Purge Everything

Cloudflare → Caching → Configuration → Purge Cache → Purge Everything. Aspettato 2 min, ri-testato. Sempre DYNAMIC.

### Tentativo 3 — Page Rules legacy (Rules → Page Rules)

Creata Page Rule:
- URL pattern: `cosaguardo.com/migliori-*`
- Cache Level: Cache Everything
- Edge Cache TTL: 4 hours
- Browser Cache TTL: 2 hours

Disabilitate temporaneamente le 2 Cache Rules nuove per non avere conflitti.

**Risultato**: DYNAMIC ❌ (uguale)

### Tentativo 4 — Test browser umano (non solo curl)

Hipotizzato che curl venisse trattato da Cloudflare come "trusted bot" (cache bypass per debug). Aperto Chrome (no incognito), hard reload (Ctrl+Shift+R) due volte di fila su `/migliori-film-thriller-su-netflix`. F12 Network → cf-cache-status.

**Risultato**: DYNAMIC anche da Chrome ❌

---

## 6. Output curl finale (per riferimento)

```
> curl -I https://cosaguardo.com/dove-vedere
HTTP/1.1 200 OK
Date: Sun, 07 Jun 2026 01:43:57 GMT
Content-Type: text/html; charset=utf-8
Connection: keep-alive
Cache-Control: public, max-age=7200, s-maxage=14400
rndr-id: c60fc5a6-4dc4-4753
Server: cloudflare
vary: Accept-Encoding
x-render-origin-server: uvicorn
cf-cache-status: DYNAMIC
CF-RAY: a07bf807ede8d4d9-LAX
alt-svc: h3=":443"; ma=86400
```

**Tutto sembra giusto MA cache non funziona.**

---

## 7. Ipotesi da investigare nella prossima sessione

In ordine di probabilità:

### Ipotesi 1 — Caching Level globale settato male
Verificare: Cloudflare → **Caching** → **Configuration** → cercare opzione **"Caching Level"**.
- Se è "No query string" o "Ignore query string" o "No cache" → cambiare a **"Standard"**
- Default dovrebbe essere "Standard" ma da verificare

### Ipotesi 2 — "Development Mode" accidentalmente ON
Cloudflare → **Caching** → **Configuration** → cercare **"Development Mode"** toggle.
Se ON, Cloudflare bypassa tutta la cache per 3 ore. Spegnerlo se attivo.

### Ipotesi 3 — Browser Cache TTL globale sovrascrive Page Rule
Cloudflare → **Caching** → **Configuration** → **"Browser Cache TTL"**.
Dovrebbe essere "Respect Existing Headers" o un valore alto.

### Ipotesi 4 — Settings "Always Online" o "Tiered Cache" influenza
Verificare se ci sono opzioni che bloccano il cache HTML.

### Ipotesi 5 — Cookie `session` viene mandato dal browser/curl e Cloudflare decide bypass automatico
Su piano Free, anche senza Cache Rules, Cloudflare a volte vede cookie sospetti e bypassa cache. Da testare in finestra TOTALMENTE pulita:
1. Aprire incognito nuova
2. NON visitare prima la home (per evitare creare cookie session)
3. Andare DIRETTAMENTE su `/migliori-film-thriller-su-netflix`
4. F12 → Network → cf-cache-status

Se diventa MISS/HIT in incognito senza cookie, abbiamo trovato la causa.

### Ipotesi 6 — Bug propagazione Cache Rules su piano Free
A volte le regole impiegano 1-2 ore reali per propagarsi su tutti gli edge. Aspettare 24h e ri-testare prima di altre modifiche.

### Ipotesi 7 — Aprire ticket Cloudflare Support
Se nessuna delle sopra funziona, Cloudflare Free ha supporto via ticket. Spiegare situazione: backend emette header corretti, regole Cache configurate, ma cf-cache-status è sempre DYNAMIC. Tempo risposta tipico: 3-7 giorni.

---

## 8. Cosa NON modificare (è giusto)

❌ NON modificare il middleware `cloudflare_cache_headers` in `main.py` — funziona perfettamente
❌ NON cambiare SSL mode da "Full (strict)" a "Full" o "Flexible"
❌ NON modificare le regex del middleware backend
❌ NON disattivare i proxy DNS (record A e CNAME devono restare Proxied/arancioni)

---

## 9. Stato sito attualmente

✅ **Sito accessibile e funzionante** via Cloudflare
✅ **HTTPS forzato** correttamente
✅ **Browser cache parziale attivo** — `Cache-Control: max-age=7200` significa che gli utenti che ricaricano la stessa pagina entro 2h NON vanno al backend (gestito dal browser, NON da Cloudflare)
✅ **Risorse statiche (CSS, JS, immagini static)** probabilmente cachate da Cloudflare auto (estensione-based default)
❌ **Pagine HTML cachate da Cloudflare CDN**: NO. Tutte vanno al backend.

**Impatto sulla banda Render**:
- Effetto stimato attuale: -10-20% banda (solo browser cache + static auto cache)
- Effetto sperato dopo fix: -50-70% banda (con edge HTML cache funzionante)

---

## 10. File modificati in questa sessione (deployati su Render)

1. `core/recommendation_api.py` — Strategia D filtro titoli leggibili esteso a `/scopri+piattaforma` (17 punti di applicazione, fix screenshot Dhurandhar)
2. `app/templates/base.html` — Aggiunto meta tag Bing Webmaster verification (`<meta name="msvalidate.01" content="116CC066785DEC4F54FDD670B8A9AD91" />`)
3. `app/main.py` — Aggiunto middleware `cloudflare_cache_headers` (riga ~566-660)

Tutti deployati su Render via git push, verificati funzionanti.

---

## 11. Come riprendere nella nuova chat

**Apri nuova chat e dichiara**:

> "Riprendo da `cloudflare_cache_debug_07_06_2026.md`. Cloudflare setup completato ma `cf-cache-status: DYNAMIC` su tutte le URL nonostante backend middleware emetta header corretti (`Cache-Control: public, max-age=7200, s-maxage=14400` e `Vary: Accept-Encoding`, no più `Vary: Cookie`). Iniziamo verificando Ipotesi 1-2 (Caching Level e Development Mode). Allego anche il file `cosaguardo_handover.md` per il contesto generale del progetto."

**Carica i 2 file in chat**:
1. `cloudflare_cache_debug_07_06_2026.md` (questo)
2. `cosaguardo_handover.md` (contesto generale)

Il prossimo assistente avrà tutto per partire senza perdita di contesto.
