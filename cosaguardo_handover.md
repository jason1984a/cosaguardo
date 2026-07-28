# CosaGuardo — Handover Document
**Data**: 05 giugno 2026 (aggiornato da v01/06)
**Scopo**: Continuazione lavoro in nuova chat senza ricaricare 100+ file storici.

> **Novità v28/05**: SEO cleanup crawl budget (PR-1 + PR-2, sez. 6), Google Ads LANCIATO con problema da diagnosticare (sez. 10-bis), Meta Business RESTRICTED in attesa review (sez. 10). Vedi TODO sez. 14 per i prossimi step immediati.
>
> **Novità v08/06**: Cloudflare edge cache HTML CHIUSA come non fattibile (Render = Cloudflare-for-SaaS / O2O, Enterprise-only) — main.py e pannello Cloudflare ripuliti. PR-3 noindex VERIFICATA end-to-end in prod. **Blocco bot al CDN edge FATTO** (Block AI bots + AI Labyrinth + WAF custom rule scraper SEO + rate limiting pagine pesanti) → leva banda principale, da monitorare 3-5 giorni. **Fix frammentazione sessioni Clarity** (Consent v2 in base.html) + privacy policy aggiornata con Clarity.
>
> **Novità v09/06**: **Peso pagina ridotto -56%** (`/migliori-*` da 121 KB → 53 KB): CSS inline → `components.css?v=1`, JS layout → `base.js?v=1` (defer). Guscio `base.html` da 97 KB → 30 KB. RICORDA il cache-busting `?v=N` ad ogni modifica di questi file. Resta aperto (non urgente): JS inline in detail.html/index.html. Vedi sez. 14.
>
> **Novità v11/06**: **Sistema feedback rating consigli COMPLETATO** (DB + API anonima + microform su results.html + dashboard `/admin/feedback-stats`). Misura la qualità delle liste dell'algoritmo (voto 1-10 + motivi + testo libero). Selection bias: non leggere medie sotto 30 risposte. **Fix nav admin**: tasto "← Home Admin" su utenti/streaming-alerts/dashboard. Vedi sez. 14.
>
> **Novità v15/06 (sessione 5)** — vedi **sez. 15** per i dettagli completi:
> 1. **INCIDENTE BOT / origin-IP RISOLTO** (priorità assoluta della giornata): scraper da datacenter (Apple 17.0.0.0/8 + AWS) colpivano l'**IP di origine di Render direttamente** (216.24.57.1), **bypassando Cloudflare** → CPU 100%, risposte da 200-585s, sito giù più volte. Le regole Cloudflare (WAF, rate-limit, Under Attack) NON servivano a niente (i bot saltano CF). **Soluzione**: blocco lato app in `main.py` (middleware `block_direct_origin`) che respinge `/film` `/serie` `/persona` quando l'IP reale (`cf-connecting-ip`, affidabile) è di un datacenter (lista `_DATACENTER_CIDRS`). Toggle env `EDGE_GUARD=0`. **Learning chiave**: l'architettura O2O di Render **strippa gli header custom** e aggiunge `cf-ray` anche ai colpi diretti → impossibile distinguere CF-vs-diretto via header; l'unico segnale affidabile è `cf-connecting-ip`.
> 2. **MOTORE CONSIGLI FILM RICOSTRUITO — TMDb-primario** (IN TEST, non ancora validato live): la vecchia sorgente candidati era SOLO il grafo locale MovieLens (9.742 film, fermo ~2018, no serie, no tmdb_id) → seed risolti sul film sbagliato (Joker→cartone, Parasite→film anni '80) e similarità per co-visione non tematica. `recommendation_api.py` riscritto per rispecchiare `recommendation_tv.py` (TMDb /similar+/recommendations + scoring per keyword/temi). Idea X/Y integrata. MovieLens resta nel file ma INUTILIZZATO (futuro bonus).
> 3. **Cache versione algoritmo** in `main.py` (`_ALGO_VERSION="movie-tmdb-v2"` nella chiave `search_cache`) → i cambi di algoritmo invalidano da soli le liste vecchie. Bump a "v3" al prossimo cambio.
> 4. **Filtro slug SEO non-latini + denylist** in `seo_pages.py` (`_seo_slug_ok()`) — chiude il buco di Strategia D sul lato generazione pagine SEO.
>
> **Novità v28/05 (sessione 2)**: Google Ads — diagnosi problema completata e keyword "cosa guardare stasera" MESSA IN PAUSA (traffico spazzatura, sez. 10-bis). Nuova feature LANCIATA: pagina affiliazione Amazon `/cosa-serve` + menu drawer di navigazione (sez. 6-bis). ⚠️ Scadenza critica account Amazon Associates: 3 vendite qualificate entro 180gg dall'iscrizione o chiusura automatica (sez. 10-ter).
>
> **Novità v01/06 (sessione 3)**: Bug fix `/scopri` (filtro voto + toggle tipo persi nei link, sez. 6-ter). Chip generi nelle schede detail ora CLICCABILI → `/scopri` filtrato (sez. 6-ter). **PR-3 SEO LANCIATA** (sez. 6-quater): diagnosi crollo impressioni 19-20/05 (= fine "honeymoon effect" Google, non penalty), chiuso leak indicizzazione su `/film/{id}`, `/serie/{id}`, `/persona/{id}` (50K pagine in eccesso indicizzate, top traffic erano pagine generiche non-curate). Strategia: noindex se non in seo_titles, altrimenti canonical → `/dove-vedere/{slug}`.
>
> **Novità v05/06 (sessione 4)**: Strategia ads pivotata — **Google Ads PAUSATO** (0 conversioni su €46/7gg, query intent sbagliato), **IG Ads SCALATO** da €7 a €13/giorno (3 sign_up reali da utenti sconosciuti su €6-15 di spesa, CPC €0,12, sez. 10-bis). Home ottimizzata per first-impression utenti anon da IG (sottotitolo riformulato "Il tuo consigliere streaming personale", 3-step numerati con timeline tratteggiata mobile, riordino sezioni anon vs logged, sez. 6-quinquies). Hero `/dove-vedere` compatto su mobile (sez. 6-sexies). **Strategia D LANCIATA**: filtro `pick_readable_title()` su 17 feed TMDb per nascondere titoli in devanagari/hangul/kanji/arabo/cirillico con fallback automatico a `original_title` se latino — fix screenshot "Dhurandhar" primo in /migliori-thriller-su-netflix (sez. 6-septies). JustWatch Affiliate Program: richiesta partnership inviata via mail (in attesa, sez. 10-quater).

---

## 1. Project Overview

**CosaGuardo** è un sito italiano di raccomandazioni film e serie TV.
- **Dominio**: `cosaguardo.com`
- **Hosting**: Render Starter ($7/mese, 512MB RAM, 0.5 CPU)
- **Repository**: `github.com/jason1984a/cosaguardo`, branch `main`
- **Owner local path**: `C:\Users\m.fantini\Desktop\cosaguardo`
- **Owner**: Marco Fantini
- **Modello business**: gratis per utenti, monetizzazione via affiliazioni piattaforme streaming

**Value proposition**: utente inserisce 2-3 titoli che ama → riceve consigli su cosa vedere dopo, mostrando provider streaming disponibili in Italia per ogni titolo.

**Posizionamento competitor**: vs JustWatch (catalogo navigabile) noi siamo motore di raccomandazione (= partiamo dall'utente). Italiano-first ma porta aperta a espansione futura.

---

## 2. Stack Tecnico

- **Backend**: Python 3, FastAPI
- **DB**: SQLite3 raw (no ORM), WAL mode, persistente su `/data/cosaguardo.db` (Render disk, env `DATABASE_PATH`)
- **Templates**: Jinja2
- **Frontend**: Vanilla JS (no React/Vue), CSS custom + icone Lucide via CDN lazy
- **API esterna principale**: TMDb (chiave in env `TMDB_API_KEY`)
- **Analytics**: GA4 (`G-HMNFXJ98H1`, Property ID `535320105`), Microsoft Clarity (`wqf6yajpcb`), Meta Pixel **DISATTIVATO** (placeholder no-op). Google Tag globale presente: **`GT-K4CPRGGN`** (installato sul sito insieme a GA4). ⚠️ NON far sovrascrivere questo tag da Google Ads (il wizard lo propone: "Utilizza il tag Google trovato" → sovrascriverebbe le impostazioni GA4/eventi custom). Vedi sez. 10-bis.
- **Email**: nessun SMTP attivo, solo lead collection in DB
- **Uptime monitoring**: UptimeRobot free, status page pubblica `stats.uptimerobot.com/rfvarmexRH`, 3 monitor (Home, Dove Vedere Hub, Scheda Film), 5min interval, alert email

---

## 3. Struttura Repository

```
cosaguardo/
├── app/
│   ├── main.py                # FastAPI app principale (tutte le route)
│   ├── db.py                  # SQLite helpers, schema, init_db
│   └── templates/
│       ├── base.html          # Layout (head, nav, footer, progress bar nav)
│       ├── index.html         # Homepage
│       ├── detail.html        # Scheda film/serie
│       ├── results.html       # Risultati raccomandazioni
│       ├── come_funziona.html # Pagina /come-funziona (value prop + FAQ)
│       ├── la_mia_raccolta.html
│       ├── profilo.html
│       ├── admin_login.html
│       ├── admin_home.html         # NUOVO: hub navigazione admin
│       ├── admin_utenti.html
│       ├── admin_streaming_alerts.html
│       ├── admin_report.html       # NUOVO: analytics dashboard DB-only
│       ├── admin_seo.html          # NUOVO: SEO refresh dashboard
│       └── ...altri
└── core/
    ├── recommendation_api.py  # TMDb API integration, similarity engine film
    ├── recommendation_tv.py   # Equivalente per serie TV
    ├── seo_pages.py           # Pagine SEO + refresh settimanale (5 moduli)
    └── admin_metrics.py       # NUOVO: aggregazioni DB per /admin/report
```

**IMPORTANTE**: `core/` sta al ROOT, non dentro `app/`. Path corretto per git: `core/seo_pages.py` (NON `app/core/...`).

---

## 4. Convenzioni Codice

### Database (`db.py`)
- **Try/finally con conn.close() nel finally** su TUTTE le funzioni (memory leak fix applicato a 38 funzioni).
- Schema migration: `CREATE TABLE IF NOT EXISTS` + `PRAGMA table_info` + `ALTER TABLE ADD COLUMN` in try/except per migration idempotenti.
- Funzioni `snake_case`
- Idempotenza dove possibile (es. `INSERT OR IGNORE`)

### Route (`main.py`)
- Admin endpoints protetti da `_check_admin(request)` con redirect a `/admin` se non loggato
- Logger: `log = logging.getLogger("cosaguardo")`
- `time` importato come `_time` (alias), `datetime` da `datetime`
- Regex preconfigurate come `_perf_re = re.compile(...)` per riuso
- Timeout su requests esterne: di solito 4-6 secondi
- Cache aggressive in DB per riduzione carico TMDb
- **Pattern parallelizzazione**: `ThreadPoolExecutor(max_workers=N)` + `as_completed` con `timeout` sui future per chiamate TMDb multiple (es. film_detail similar, recommend search)

### Templates
- **CSS inline `<style>`** dentro template per componenti critici (signup-card, cg-alert, install-banner, episodi, strip "Come funziona"). Evita dipendenze da `style.css` globale.
- **onclick inline** preferito a `addEventListener` per UI critica (più affidabile durante streaming HTML)
- **Posizionamento UI** via JS `getBoundingClientRect()`, non solo CSS (es. popover auto-flip)
- Context processor globale injetta `is_logged_in` + `user_email` in ogni TemplateResponse
- **CSS `order` per riordino mobile** invece di duplicare HTML (es. home mobile mette search prima della strip)

### Thread di background
- `scheduled_restart` (6h, configurabile via env `AUTO_RESTART_HOURS` / `DISABLE_AUTO_RESTART`)
- `seasons_refresh` (cache `series_seasons_cache` lazy refresh ogni 24h)
- `dashboard_recs_prefetch` (precarica daily recommendations dopo login)
- **`_seo_refresh_worker`** (lunedì 03:00 Europe/Rome, threading.Timer, DST-aware via zoneinfo)
- **`_seo_catchup_if_overdue`** (startup: se ultimo run >7gg → esegue subito)
- Tutti con dedup via set/dict inflight per evitare doppi lavori paralleli

### Middleware HTTP
- `block_bad_bots` (28 bot UA bloccati)
- `perf_logger` (log durata di ogni request)
- **`head_to_get_fallback`** (NUOVO): trasforma HEAD→GET nello scope ASGI per supportare uptime monitors (UptimeRobot free fa HEAD). Aggiunto in ULTIMO (eseguito PER PRIMO, ordine LIFO middleware FastAPI).
- Altri middleware esistenti (cookie consent, redirect, ecc.)

---

## 5. Convenzioni di Comunicazione (Owner ↔ Claude)

- **Lingua**: italiano sempre
- **Comandi git**: brevi, formato preferito separato in code-blocks distinti:
  ```
  git add ...
  ```
  ```
  git commit -m "..." -m "..."
  ```
  ```
  git push origin main
  ```
- **Output files**: in `/mnt/user-data/outputs/` con replica della struttura repo (es. `app/templates/`, `app/main.py`, `core/...`). Sempre `present_files` con il path completo.
- **Path al PC owner**: `C:\Users\m.fantini\Desktop\cosaguardo` (Windows, comandi DOS-style spesso usati)
- **Tono**: pratico, no fluff, decisioni chiare con pro/contro, raccomandazione finale netta
- **Validazione**: prima di consegnare file modificati, sempre check:
  - Python: `ast.parse()`
  - Jinja: render compile via `jinja2.Environment` con stub `base.html`
  - JS: `node --check` su blocchi estratti
  - CSS: count braces balanced (filtrando commenti Jinja `{# #}`)
  - Per modifiche complesse: smoke test runtime con TestClient (es. middleware HEAD→GET testato così)

---

## 6. Feature Implementate (cronologico)

### Hub SEO e onboarding
- Pagine `/come/` e `/dove-vedere/` per long-tail keyword
- Onboarding multi-step alla registrazione
- Slugify titoli + cache per SEO titles

### Phase 1 — Tracking serie
- Tabella `user_series_tracking` per stato serie utente
- Tabella `series_seasons_cache` per detection nuove stagioni (refresh 24h)
- Banner novità in homepage (max 5 alert visibili)
- Pagina `/le-mie-serie` con filtro stato

### Phase 2 — Detection nuove stagioni (versione vecchia, basata su user_series_tracking)
- Helper batch `get_series_seasons_cache_batch`
- Refresh lazy ogni 24h con thread daemon
- Pre-fetch cache in background al track

### Profilo & Raccolta
- Profilo snellito (rimossi accordion preferiti/visti)
- Card-link verso `/la-mia-raccolta`
- Dedup duplicati cross-kind in raccolta (preferiti + visti + alert + ricerche)
- Tab system + remove multi-kind

### Scelti per te
- `/home-picks` con gate (`len(searches)+len(liked) < 2: return []`)
- Mostra suggerimenti homepage solo a utenti già engaged

### Performance & Anti-bot
- `_prefetch_daily_recs_async` con Event-based wait
- Trigger da login/register/home
- 28/28 chiamate TMDb con timeout
- Robots.txt esteso a 28 bot bloccati + middleware `block_bad_bots`
- Scheduled restart 6h per memory drift
- **Parallelizzazione `film_detail` similar**: ThreadPoolExecutor max_workers=6 timeout=4s per i 6 poster TMDb (stesso pattern già usato in search principale). Da seriale ~3s a parallel ~500ms su cache miss.

### Analytics & Tracking
- GA4 attivo con consent gating
- Microsoft Clarity attivo
- Meta Pixel **placeholder no-op** (account FB bannati, strada B traffic-only)
- Helper `window.cgTrack()` unificato per eventi cross-platform
- Post-register conversion trigger via `?registered=1` query param

### UX fixes
- Bug popover detail.html auto-flip up + render off-screen per altezza accurata
- og:url/og:image fix da `onrender.com` a `cosaguardo.com` (4 occorrenze)
- Bug "Trova simili" prefill handler in `index.html`
- Redirect 301 middleware da `cosaguardo.onrender.com` → `cosaguardo.com`

### Poster lightbox
- Click su poster apre modal con poster grande + CTA "▶ Dove vederlo in streaming"
- ESC chiude, click backdrop chiude, body scroll lock, focus management
- Fade-in 180ms, responsive mobile

### Signup Card/Prompt v2 (CSS inline)
- Home: `cg-signup-card` tra search panel e cinema strip
- Results: `cg-signup-prompt` SOPRA i consigli (non sotto)
- Dismiss separato via sessionStorage (`cg_signup_card_dismissed`, `cg_signup_prompt_dismissed`)
- CTA come pulsante gradient blu/viola con shadow

### Streaming Alerts (A+B)
- Tabella `streaming_alerts` (email, tmdb_id, content_type, title, user_id, notified_at, created_at) UNIQUE
- Helper: `add_streaming_alert()` idempotente, `count_streaming_alerts_for_title()`, `list_streaming_alerts(limit, offset)`, `streaming_alerts_stats()`, `top_requested_titles(limit)`
- Route `POST /api/streaming-alert` con regex email + content_type whitelist
- Quando provider vuoti su detail.html: mostra **A** form "Avvisami quando arriva" + **B** card "Vedi titoli simili"
- Email pre-fill se utente loggato

### Admin Streaming Alerts page
- `/admin/streaming-alerts` protetta da `_check_admin`
- Stats grid, top 10 titoli richiesti, tabella 200 alert recenti

### Sezione Episodi (serie TV)
- Tabella `series_episodes_cache(tmdb_id, season_number, episodes_json, cached_at)` con TTL 7 giorni
- Route `GET /api/series/{id}/season/{n}/episodes` cache server 7gg + cache HTTP 1h
- Tab orizzontale per stagione, lazy load on tab click, episodi futuri con badge "⏳ In uscita"
- Pill "X stagioni" / "X episodi" cliccabili → aprono sezione Episodi sulla **prima** stagione

### Pagina `/come-funziona` (21/05/2026)
- Pagina full SEO con value prop, 3 step, 4 diff vs competitor (JustWatch), callout valorizzazione registrazione, 5 FAQ
- JSON-LD `FAQPage` + `BreadcrumbList` per rich snippet Google
- Meta description e ultima FAQ menzionano "AI" come da decisione opzione C (algoritmo intelligente nel corpo, AI nei meta/FAQ — onesto)
- Mini-strip "Come funziona in 3 step" in homepage tra hero e search panel (icone Lucide: pencil-line/sparkles/tv)
- Su mobile (≤600px) strip diventa layout verticale "icona + frase" stacked
- CSS `order` per riordino mobile della home: hero → platform → trending → personal (se loggati) → search → strip → resto

### Refresh SEO settimanale automatico (21/05/2026)
**Architettura completa in `core/seo_pages.py`** — 5 moduli orchestrati da `weekly_seo_refresh()`:
- **Modulo 1 — Scheduler**: thread daemon `_seo_refresh_worker` con `threading.Timer`, lunedì 03:00 Europe/Rome via `zoneinfo` (DST-aware). All'avvio app `_seo_catchup_if_overdue` esegue subito se ultimo run >7gg. Disabilitabile via `DISABLE_SEO_REFRESH=1`.
- **Modulo 2 — Refresh evergreen diff-only**: `populate_seo_titles_db()` aggiorna `updated_at` SOLO se cambia un campo significativo (popularity >=0.5, vote >=0.1, poster, overview). Sitemap `<lastmod>` ora veritiero, no fake freshness. Top 350 movie + 350 tv = 700 evergreen.
- **Modulo 3 — New releases**: `populate_new_releases()` fetcha now_playing/upcoming region=IT + airing_today/on_the_air, finestra -90gg/+60gg, cap 300, `source='new_release'`, auto-cleanup >180gg.
- **Modulo 4 — Detection nuove stagioni** (NUOVA versione integrata in seo_pages): `detect_new_seasons()` refetch `number_of_seasons` per top 200 serie. Se incrementato → `seasons_bumped_at=now`, `updated_at=now`, banner "🆕 Stagione N disponibile" in `dove_vedere.html` (60gg).
- **Modulo 5 — Dashboard `/admin/seo`**: stats (totali, evergreen/new_release split, bump stagioni 30gg), ultimo refresh, log ultime 10 esecuzioni, bottone "Esegui adesso" (background), endpoint HTTP `POST /admin/seo-refresh-trigger?token=XXX` per cron esterni futuri (env `SEO_REFRESH_TOKEN`).

**Schema DB nuovo/migrato**:
- `seo_titles` migrata con colonne nuove: `source`, `seasons_count`, `seasons_bumped_at`, `release_date` (ALTER TABLE soft idempotente)
- Nuova tabella `seo_refresh_log`: id, started_at, finished_at, trigger, evergreen_*, new_*, seasons_detected, duration_seconds, error
- Cap totale: 1000 (700 evergreen + 300 new_release) — sotto soglia thin content

### Admin Report `/admin/report` (21/05/2026)
**Architettura DB-only in `core/admin_metrics.py`** — GA4 Data API NON integrata per scelta:
- Setup richiedeva service account GCP, owner ha org policy che blocca chiavi JSON
- Soluzione: dati GA4 accessibili via 3 deep-link nativi nel template (Apri GA4 / Acquisizione / Pagine)
- Tutte le metriche dal DB: nuovi utenti, utenti attivi (= ≥1 ricerca), ricerche, top seeds, alert, top alert, serie tracciate, feedback (breakdown like/dislike), daily recs, users with recs
- Ogni metrica ha confronto vs periodo precedente (`current`, `previous`, `delta_pct`)
- Selector periodo: oggi / 7gg / 30gg con bound timezone Europe/Rome → UTC per query SQLite
- Niente cache: pagina admin-only, traffico negligibile, dati sempre live

### Admin Home `/admin/home` (21/05/2026)
- Landing dopo login (era `/admin/utenti`, ora `/admin/home`)
- Widget status sistema (verde, statico) linkato a UptimeRobot status page pubblica
- Mini-stats colpo d'occhio (5 numeri: utenti totali, +7gg, ricerche, preferiti, alert pendenti)
- Grid 4 card menu: Utenti / Report / SEO / Streaming Alerts
- 5 link esterni: UptimeRobot, GA4, Search Console, Clarity, Render dashboard
- Tutti i template admin (`admin_report`, `admin_seo`, `admin_streaming_alerts`) hanno bottone "← Admin Home" per coerenza

### Performance UX (21/05/2026)
- **Navigation Progress Bar** in `base.html`: bar viola gradient 3px fixed top z-index 99999, si attiva al click su link interni e form POST. Salto al 25% al click, crescita graduale 90%, completion su `pageshow`/`beforeunload`. Skip intelligenti: external, hash, mailto/tel/js, ctrl/cmd/shift, target=_blank. Safety reset 4s. Rispetta `prefers-reduced-motion`. Self-contained ~1KB, no NProgress.js.
- **Lucide CLS placeholder**: CSS `i[data-lucide]:empty { 1em×1em }` riserva spazio prima del render lucide.js, evita layout shift. Non interferisce con icone che hanno `style="width:..."` inline (specificity).

### Tag verifica Tradedoubler
- `<!--Tradedoubler site verification 3484363-->` in `base.html` dopo `google-site-verification`

### PWA Install Banner (user-earned)
- Banner appare solo se ≥2 visite, sessione lunga, o utente loggato + 30s minimi
- Solo mobile, escluse pagine admin/login/register/recommend
- Dismiss permanente, tracking GA4
- iOS Safari guida visiva 3 step, Android Chrome `beforeinstallprompt` nativo

### SEO Cleanup crawl budget — PR-1 + PR-2 (28/05/2026)
**Contesto/problema**: Search Console mostrava ~33-40K pagine indicizzate (salite anche dopo il primo intervento per lag di Google) per un sito con ~70 utenti reali. La causa: Google indicizzava **ogni pagina `/persona/{id}`** (cast/crew TMDb di ogni film — decine di migliaia) + combinazioni `/scopri?filtri`, paginate, sub-versioni. Questo **drenava il crawl budget** dalle pagine che contano: solo **6 `/come/` indicizzate su ~750** in sitemap. Le `/come/` ricevono traffico reale (16 click/3 mesi, query "film simili a X", posizione media 9) ma Google non arrivava a crawlarle.

**PR-1 — noindex su `/persona/{id}`**:
- `app/templates/persona.html`: aggiunto `{% block meta_robots %}noindex, follow{% endblock %}`
- `follow` = Google non indicizza ma segue i link interni (es. /persona/Tom-Cruise → /film/{id})
- Pagine restano navigabili agli utenti, solo deindicizzate da Google

**PR-2 — audit + noindex su route low-value/thin/duplicate**:
- `base.html` ha `<meta name="robots" content="{% block meta_robots %}index, follow{% endblock %}">` (default index)
- **noindex statico** (block nel template): `login.html`, `register.html`, `la_mia_raccolta.html` (user pages, no SEO value). `installa.html` LASCIATA index (decisione conservativa).
- **noindex condizionale** (calcolato in `main.py`, passato al template via context `meta_robots` + `{% block meta_robots %}{{ meta_robots or "index, follow" }}{% endblock %}`):
  - `scopri()`: noindex se `has_filters or page > 1` (combinazioni esponenziali); index su /scopri homepage
  - `dove_vedere_hub()`: noindex se `page > 1` (thin duplicate); index su p=1 con qualsiasi tipo
  - `platform_page()`: noindex se `tipo in ("film","serie")` (sub-version); index su tipo=tutti default
  - `come_simili()` 404 fallback: noindex
- **Restano INDEX**: home, `/film/{id}` `/serie/{id}` detail, `/dove-vedere/{slug}` `/come/{slug}` evergreen, `/come-funziona`, `/migliori` `/migliori-{slug}`, `/piattaforma/{slug}` default, `/scopri` homepage, `/privacy` `/termini`
- Validato: 11 render Jinja con context fake per ogni combinazione, ast.parse OK, lavori precedenti preservati

**Tempi attesi deindex**: 4-8 settimane (Google ricrawla lentamente; il numero "Indicizzate" può SALIRE prima di scendere = normale, Google scopre nuove pagine prima di tornare sulle vecchie noindex-ate).

**PR-3 FATTA e VERIFICATA** (live 01/06, verificata in prod 08/06): noindex su `/film/{id}` e `/serie/{id}` NON presenti in `seo_titles` DB. Vedi sez. 6-quater per dettagli e sez. 14 per la verifica curl end-to-end. Solo i ~700+700 curati in `seo_titles` restano INDEX (+ canonical verso `/dove-vedere/{slug}`); tutto il resto è `noindex, follow`.

**Slug da auditare**: `/come/michael` e altri slug troppo generici visti indicizzati (probabile bug slugify o titolo ambiguo nel DB).

---

## 6-bis. Pagina Affiliazione Amazon + Menu Drawer (28/05/2026 sessione 2)

### Pagina `/cosa-serve` — affiliazione Amazon
- **Nuovo template** `app/templates/cosa_serve.html` (stampo = `come_funziona.html`: CSS inline page-scoped, variabili esistenti, JSON-LD BreadcrumbList).
- **Nuova route** in `main.py`: `@app.get("/cosa-serve")` → `cosa_serve.html`, posizionata dopo `/come-funziona`.
- **Scopo**: monetizzazione via Amazon Associates (tag affiliato **`cosaguardo-21`**). Hardware adiacente al guardare film/serie: streaming device, smart TV, audio, proiettori.
- **4 sezioni / 8 card**: (1) Rendi smart la TV [Fire TV Stick 4K Plus, Google TV Streamer], (2) Nuova smart TV [TV 4K 50-55", OLED], (3) Audio [soundbar, cuffie wireless], (4) Cinema [proiettore, accessori/cavo HDMI].
- **Stato attuale = LINK GENERICI** (ricerche Amazon `amazon.it/s?k=...&tag=cosaguardo-21`), NON prodotti specifici. Decisione consapevole: link a ricerca non "muore" mai (no manutenzione link rotti).
- **8 icone illustrative SVG** (line-art, colore primary, riquadro sfumato) al posto di foto prodotto. NON sono foto reali per scelta obbligata (vedi sotto).
- **Disclosure affiliazione** OBBLIGATORIA (ToS Amazon): box "Trasparenza" a FONDO pagina (su richiesta owner, non in testa). Menziona "In qualità di Affiliato Amazon...".
- Tutti i link hanno `rel="nofollow sponsored noopener"` + `target="_blank"`.
- **Sitemap aggiornata**: aggiunti `/cosa-serve` e `/come-funziona` (quest'ultimo MANCAVA — bug preesistente fixato) in `static_urls` del `sitemap()` in main.py.

### Menu Drawer di navigazione (in `base.html`)
- **Problema risolto**: nav top affollata (su mobile <480px già compressa); aggiungere voci non sostenibile.
- **Soluzione**: bottone "Menu" (hamburger) raggruppato col logo a SINISTRA (wrapper `.header-left`). Apre drawer laterale con TUTTE le sezioni: Home, Scopri, Dove vedere, Migliori, Cosa serve, La mia raccolta, Come funziona + Profilo/Accedi.
- **Stile**: icone drawer = SVG line-art coerenti con la nav top (stroke currentColor, fill none, `var(--muted)`). NO emoji (richiesta owner: continuità visiva).
- **JS self-contained**: open/close da trigger/×/backdrop/ESC/click-link, scroll lock, focus management, `prefers-reduced-motion`. Validato node --check.
- **Mobile <480px**: drawer trigger mostra solo icona, label "Menu" nascosta.
- La nav top a destra (Cerca, Home, Scopri, Raccolta, Login) resta INVARIATA.

### ⚠️ Vincolo immagini prodotto Amazon (perché icone e non foto)
- Le foto prodotto Amazon/produttori sono **copyright**: vietato scaricarle/hotlinkarle. Violazione = rischio ban account affiliato.
- **SiteStripe "Immagine" RIMOSSO** da Amazon per la maggior parte degli affiliati (oggi solo "Scarica link" testuale disponibile — verificato 28/05 sullo screenshot owner). La mia indicazione iniziale "usa SiteStripe Immagine" era basata su versione vecchia.
- **PA-API (Product Advertising API)** = unico modo ufficiale per foto reali via codice. MA richiede vendite attribuite alle chiamate API negli ultimi 30gg per mantenere l'accesso (paradosso noto: servono vendite-da-API per usare l'API). Realistica solo con flusso vendite costante = obiettivo medio-lungo termine.
- **Decisione**: restare con icone generiche + link generici PER ORA. Foto reali via PA-API = TODO futuro quando il sito venderà con continuità.

### File prodotti questa sessione (in outputs)
- `app/templates/cosa_serve.html` (nuovo)
- `app/templates/base.html` (drawer + header-left)
- `app/main.py` (route /cosa-serve + sitemap)
- Validati: ast.parse (main.py), Jinja render (template), node --check (drawer JS), brace balance.

---

## 6-ter. Bug fix UX + Chip generi cliccabili (01/06/2026 sessione 3)

### Bug fix `/scopri`: filtro voto e toggle tipo persi nei link
- **Problema segnalato dall'owner**: su `/scopri?tipo=serie&genere=commedia&voto=8`, cliccando "Pagina successiva" il filtro `voto=8` veniva perso (e idem cliccando i toggle Film/Serie).
- **Causa**: in `scopri.html`, i link costruiti a mano (paginazione righe 259+267, toggle tipo righe 39+41) NON includevano `&voto={{ voto }}` nella querystring. Il form GET filtri funzionava perché ha un radio `voto` interno (incluso automaticamente al submit).
- **Fix**: aggiunto `&voto={{ voto }}` nei 4 link. Per i toggle tipo usata stessa logica condizionale degli altri filtri (`{% if voto %}&voto={{ voto }}{% endif %}`). Link "Rimuovi voto" (riga 131) lasciato com'è (è il link che serve a toglierlo).
- **File modificato**: `app/templates/scopri.html`.

### Chip generi cliccabili nelle schede detail
- **Richiesta owner**: "capita nelle schede che gli utenti clicchino sui chip dei generi (es. Thriller, Fantascienza) ricevendo dead click; mandali su /scopri filtrato".
- **Implementazione**: mappa `GENRE_TO_SCOPRI_SLUG` in `main.py` (~30 voci: copre i 19 generi film + 16 serie di TMDb-it). Mappa: `azione→azione`, `crime→crimine`, `romance→romantico`, `action & adventure→azione`, `sci-fi & fantasy→fantascienza`, etc. Generi senza match in `/scopri` (Avventura, Famiglia, Fantasy, Guerra, Storia, Musica, Soap, Reality, ecc.) → `None` → chip resta `<span>` non-cliccabile (no dead click, no perdita info).
- Funzione `genre_to_scopri_slug(name)` esposta al template come `genre_to_slug`. Routes `film_detail` e `serie_detail` passano anche `scopri_tipo` ("film" o "serie") per costruire l'URL corretto.
- **Markup**: in `detail.html` due punti (righe ~1716 + ~2144), chip ora è `<a class="detail-genre-tag detail-genre-tag--link" href="/scopri?tipo=...&genere=slug">` quando mappato, altrimenti `<span>` come prima.
- **CSS aggiunto**: `.detail-genre-tag--link` con hover (background acceso + translateY) e `:focus-visible` accessibile.
- **aria-label** grammaticalmente corretto: "altri film" vs "altre serie".
- **File modificati**: `app/main.py`, `app/templates/detail.html`.

### Validazione
- `ast.parse` main.py OK.
- Render Jinja scenari reali: "La guerra dei mondi" (Fantascienza+Thriller → entrambi cliccabili) e serie con "Action & Adventure" + "Famiglia" (primo cliccabile, secondo `<span>`). Asserzioni passate.

---

## 6-quater. PR-3 — Chiusura leak indicizzazione SEO (01/06/2026 sessione 3) ⭐

### Contesto del problema
- **Sintomi riferiti dall'owner**: calo brusco impressioni Google nei giorni 19-20/05 (da 6.641 a 1.850, poi 473 il 21/05). Numero pagine indicizzate cresciuto a 50.543 (vs ~5K iniziali). Ipotesi owner: "Google ci vede come bot/AI per troppe pagine".

### Diagnosi rigorosa basata sui dati (CSV GSC + screenshot Performance)
**Tabella indicizzate vs impressioni** (estratta dal CSV `Coverage-2026-06-01.zip` Grafico.csv):
| Data | Indicizzate | Impressioni |
|---|---|---|
| 09/05 | 11.945 | 2.061 |
| 12/05 | 30.207 | 219 |
| 15/05 | 30.207 | 4.216 |
| **18/05** | **32.986** | **7.602** (picco) |
| 19/05 | 39.881 | 6.641 |
| **20/05** | **39.881** | **1.850** (crollo -72%) |
| 21/05 | 39.881 | 473 |
| 29/05 | 50.543 | 174 |

**Conclusioni fattuali**:
- Le impressioni sono CRESCIUTE mentre l'indicizzazione cresceva (4-18 mag) → l'aumento pagine NON ha causato il calo.
- Il crollo è stato A PRECIPIZIO in 24-48h, non graduale → non è "penalty qualità" (sarebbe stato graduale).
- **Causa più probabile**: fine del **Google "honeymoon effect" / fresh site bonus** — boost temporaneo di visibilità che Google dà ai siti nuovi indicizzati in massa, poi ricalibra il ranking su segnali utente reali (CTR, dwell time). Timing perfetto con i dati (boost 5-19 mag, ricalibrazione 20 mag).
- **L'ipotesi "ci vedono come bot/AI" è scartata**: niente segnali per dirlo, pattern incompatibile.

**Però le 50K indicizzate ERANO un problema reale**, ma per ragione DIVERSA:
- **Cannibalizzazione**: top pagina per click era `/serie/297640` (14 click, 1.312 impr) — una pagina generica `/serie/{id}`. Le pagine SEO curate (`/dove-vedere/*`, `/come/*`) avevano 41-102 impr ciascuna. Google dava autorità alle pagine generiche invece che a quelle curate, perché Google le riconosceva come autorevoli sui titoli.
- **Top query erano nomi di attori**: "rachel mcadams" (1.182 impr, 2 click = CTR 0,17%), "nina singh" (206 impr). Pagine `/persona/{id}` portavano traffico inutile su intento non servibile (utente vuole bio, noi offriamo raccomandazioni).
- **Crawl budget drenato**: Google scansionava 50K pagine generiche.

**Bug del codice scoperto durante audit**:
- L'owner pensava di aver fatto noindex su `/persona/{id}`, ma il context delle route detail e persona NON passava `meta_robots` → ereditavano il default `index, follow` di `base.html`. Le 5.935 pagine "noindex" che GSC vedeva erano da `/scopri` filtrato e `/dove-vedere?p=>1`, non da persone/film/serie.

### Strategia PR-3 (deployata)
**Scelta architetturale**: niente 301 redirect (avrebbe rotto UX della scheda detail ricca). Invece:

| Caso | Trattamento |
|---|---|
| `/persona/{id}` | `noindex, follow` SEMPRE |
| `/film/{id}` o `/serie/{id}` IN `seo_titles` | `index, follow` + `<link rel="canonical">` → `/dove-vedere/{slug}` |
| `/film/{id}` o `/serie/{id}` NON in `seo_titles` | `noindex, follow` |
| Link "raccomandazioni simili" in `detail.html` | Preferisce `/dove-vedere/{slug}`; fallback `/film/{id}` con `rel="nofollow"` |

**Decisioni di design** (importanti):
- **Canonical invece di 301**: gli utenti continuano a vedere la scheda detail ricca (poster, sinossi, simili, tracking serie), Google concentra autorità su `/dove-vedere/{slug}`.
- **`base.html` ora ha fallback intelligente** sul block `meta_robots`: legge la variabile context se presente, altrimenti default `index, follow`. Permette a route future di noindexare passando solo la variabile, senza modificare il template figlio. Risolve il problema "ho dimenticato di sovrascrivere il block".
- **`og_url` ora punta al canonical** quando disponibile (consistency anteprime social).
- Helper `get_slug_by_tmdb_id(tmdb_id, content_type)` già esistente in `core.recommendation_api`, restituisce slug se titolo è in `seo_titles` SEO curati, altrimenti `None`. Usata per decidere index vs noindex.

### File modificati PR-3
- `app/main.py`: route `film_detail`, `serie_detail`, `persona_detail` calcolano `meta_robots` + `canonical_url` e li passano al context. Aggiunto campo `seo_slug` ai dict del payload `similar[]`.
- `app/templates/detail.html`: nuovo block `meta_robots`, nuovo `<link rel="canonical">`, link "simili" con preferenza `/dove-vedere/{slug}`, og_url usa canonical.
- `app/templates/base.html`: block `meta_robots` ora legge da context con fallback.
- **Validato**: ast.parse OK; render Jinja scenari A (titolo curato → index+canonical+link a dove-vedere) e B (non curato → noindex, no canonical) tutti corretti.

### ⚠️ Cosa aspettarsi DOPO il deploy (dato importante per non far panico)
- **Settimana 1**: Google rileva i nuovi `noindex` quando ri-crawla. Vedi PRIMA salire contatore "Esclusa noindex" in GSC (5.935 oggi → dovrebbe crescere parecchio), POI scendere "Indicizzate".
- **Settimane 2-4**: indicizzate scendono progressivamente da 50K verso ~5-8K target (curate + canonicalizzate).
- **Settimane 4-8**: impressioni POSSONO rimanere basse o scendere ancora PRIMA di risalire — normale. Google ricalcola autorità sui nuovi URL canonici. **NON rollback, NON altre modifiche SEO nel frattempo**.
- **Monitoring**:
  - GSC > Indicizzazione: "Esclusa noindex" deve SALIRE (sta digerendo).
  - GSC > Performance > Pagine: top pagine devono diventare `/dove-vedere/*` e `/come/*` invece di `/serie/297640`, `/film/58591`, `/persona/53714`.
  - GSC > Performance > Query: meno nomi attori, più titoli film/serie.
- **Antipattern da evitare**: NO "richiedi indicizzazione" per le `/film/{id}` (le vogliamo proprio fuori).

### Decisione collegata: NIENTE pagine SEO per top attori
- **Domanda owner**: "ha senso generare pagine specifiche SEO per i top 100/200 attori?"
- **Decisione**: NO, almeno non ora. Quattro ragioni:
  1. Wikipedia/IMDb/MYmovies vincono sempre su nomi di attori, non li battiamo.
  2. CosaGuardo è motore raccomandazioni streaming, non enciclopedia celebrità — attirerebbe intento sbagliato (stesso pattern di "stasera in TV" su Ads).
  3. Pagine attore = copia povera di TMDb in italiano = stesso problema di `/film/{id}` appena chiuso.
  4. Costo opportunità: meglio investire tempo su pagine `/migliori-*` o `/come/*` aggiuntive nel perimetro reale.
- **Approccio alternativo (se mai)**: tra 4-6 settimane (post-digerimento PR-3), guardare GSC Query → identificare 5-10 nomi che portano impressioni REALI con angolo "alla CosaGuardo" sensato → fare pagine curate (es. "film romantici come quelli di X", "migliori film con X su Netflix") con keyword attore come trigger ma prodotto = raccomandazione. Mai 200 automatiche.

---

## 6-quinquies. Home ottimizzata per first-impression utenti anon (05/06/2026 sessione 4)

### Contesto del problema
- **Diagnosi via Clarity** (owner attivo): utenti che arrivano da IG ads bouncano in 4-5 secondi dalla homepage. Scrollano brevemente e poi escono. Non c'è tempo di "capire e agire".
- **Dato chiave Clarity successivo**: utenti IG che NON bouncano (quindi sono nel target) scrollano TUTTO il sito, segnale di interesse genuino — quindi il problema non è il contenuto, è la "first impression" prima ancora del contenuto.

### Modifiche applicate
**Copy hero**:
- H1 invariato: "Cosa guardo stasera?"
- Sottotitolo PRIMA: "Inserisci 2-3 titoli che ami, ti diamo consigli su misura."
- Sottotitolo DOPO: **"Il tuo consigliere streaming personale."** (37 caratteri, 1 riga garantita su mobile)
- Rationale: il vecchio sottotitolo era ridondante coi 3 step sotto ("Dicci 2-3 titoli che ami" già nello step 1). Il nuovo invece *disambigua* il sito (consigliere=ruolo umano, streaming=non palinsesto TV, personale=personalizzazione). Lavoro complementare ai 3 step, non sovrapposto.

**Mini-strip "Come funziona" (cg-howit-strip)**:
- Icone Lucide (matita, sparkle, TV) sostituite da **cerchi numerati 1/2/3** in viola accent (classe `cg-howit-icon--num`)
- Step 1: "Dicci 2-3 titoli che ami" (invariato)
- Step 2: PRIMA "L'algoritmo trova i simili" → DOPO **"Trova titoli pensati per te"** (focus su benefit utente, non meccanica algoritmo)
- Step 3: PRIMA "Scopri dove vederli" → DOPO **"Scopri dove guardarli ora"** (aggiunto "ora" per urgenza, richiama "stasera" del titolo)
- **Linea verticale tratteggiata** che connette i 3 cerchi su mobile (visibile SOLO per utenti anon dove la strip è SOPRA al box algoritmo; per loggati la strip è in fondo come riferimento didascalico, la timeline confonderebbe)
- Implementazione: pseudo-elemento `::before` su `.cg-howit-icon--num`, `border-left: 1.5px dashed rgba(167,139,250,0.45)`. Gap verticale steps aumentato 9→14px per ospitare la linea senza sovrapposizioni

**Riordino sezioni mobile (≤600px) condizionale per stato login**:
- Classe modificatrice `page-home--anon` o `page-home--logged` su `.page-home` (Jinja conditional su `is_logged_in`)
- **Ordine ANON**: hero → platform-strip → cg-howit-strip → search-panel → hero-trending-strip → resto
- **Ordine LOGGED**: hero → platform-strip → hero-trending-strip (subito i film, sa già usare il sito) → ... → search-panel → cg-howit-strip → resto (invariato rispetto a pre-modifica)
- **Alleggerimento visivo cg-howit-strip per anon**: `background: transparent; border: none; box-shadow: none; padding: 12px 4px` — diventa etichetta visiva leggera invece di card che competerebbe col box algoritmo subito sotto

**Razionale strategico**:
- Utente anon ha bisogno di capire (3 step esplicativi PRIMA del box) poi può agire (box algoritmo)
- Utente loggato ha bisogno di valore subito (raccomandazioni) — sa già usare il sito
- La differenziazione anon vs logged è il design pattern più importante della modifica: trattamenti diversi per esigenze diverse

### File modificato
- `app/templates/index.html` (solo questo file, no Python)

### Validazione
- ast.parse: non necessario (solo template)
- Render Jinja per scenari anon + logged: classi `page-home--anon` / `page-home--logged` applicate correttamente, hero subtitle correto, 3 step numerati con copy aggiornati
- Spot check visivo desktop: layout invariato (le modifiche scattano solo `max-width: 600px`)

### Cosa monitorare nei 7-10 giorni post-deploy
- **Bounce rate sotto 5s utenti IG**: deve scendere (era ~50% pre-deploy)
- **Scroll depth fino al box algoritmo**: deve salire (gli anon ora lo vedono al 3° viewport scroll)
- **Click su campi "Titolo 1/2/3"** dal traffico IG: deve salire (= utenti che provano a compilare)
- **Conversion rate sign_up da IG**: prima del fix era 4% (2 su 50), post-fix ci aspettiamo >5%
- **Tasso "I più apprezzati" click**: può scendere e va bene (era il "bounce alternativo" della home pre-modifica)

---

## 6-sexies. Hero `/dove-vedere` compatto su mobile (05/06/2026 sessione 4)

### Problema
Su mobile l'hero `dvh-hero` (titolo + paragrafo lungo + gradient + padding 28px) occupava ~280px / metà schermata al primo caricamento, costringendo l'utente a scrollare prima di vedere i card film/serie. Su desktop la dimensione è giustificata, su mobile è solo spreco.

### Soluzione: Approccio A — hero compatto su mobile, decorativo su desktop
- Variante mobile del sottotitolo introdotta con classe `dvh-hero-sub-mobile` (versione corta: "Netflix, Disney+, Prime, NOW e altre piattaforme italiane.")
- Variante desktop con `dvh-hero-sub-desktop` (testo lungo originale con tutte le piattaforme + "Aggiornato regolarmente")
- CSS @media (max-width: 600px): hero `background: transparent`, `border: none`, padding ridotto, allineato a sinistra. Nasconde `dvh-hero-sub-desktop`, mostra `dvh-hero-sub-mobile`
- Risultato: hero passa da ~280px a ~70px di altezza, le card film/serie ora visibili nel primo viewport

### File modificato
- `app/templates/dove_vedere_hub.html`

### Pattern riusabile
Lo stesso pattern di "hero compatto mobile" può essere applicato a tutte le pagine hub con hero pesante. Verificato che `/scopri` e `/migliori` sono già OK (hero non pesante). Da riconsiderare se in futuro aggiungiamo altre pagine hub stile `/dove-vedere`.

### Nota strategica più ampia (annotata per refactor futuro)
La maggior parte del traffico CosaGuardo è mobile (IG ads, mobile-first per definizione). Stiamo iterando spesso con media queries di "rimedio" su CSS desktop-first. In futuro vale la pena considerare refactor mobile-first del CSS principale. Non urgente, non rotto.

---

## 6-septies. Strategia D — Filtro titoli TMDb non leggibili (05/06/2026 sessione 4) ⭐

### Problema
TMDb restituisce titoli in lingua originale (devanagari per hindi/tamil/telugu, hangul per coreano, kanji/kana per giapponese, arabo, cirillico, tailandese, ecc.) quando manca la traduzione italiana. Per un utente italiano questi titoli sembrano "bug" perché illeggibili.

**Screenshot scatenante**: `/migliori-thriller-su-netflix` mostrava al primo posto `धुरंधर: द रिवेंज` (devanagari, thriller indiano "Dhurandhar: The Revenge"). Effetto: l'utente vede un bug grafico evidente, percezione di sito poco curato.

**Stima impatto pre-fix**: ~5% del catalogo TMDb (titoli stranieri non tradotti in italiano).

### Strategia scartate
- **A — Filtro hard per `original_language`** in lista nera (hi, ta, te, ko salvo big...): esclude anche capolavori asiatici con traduzione italiana disponibile (Parasite, Squid Game). No.
- **B — Filtro soft per popolarità + whitelist big**: richiede maintenance whitelist, scarta titoli minori asiatici di nicchia.
- **C — Mostra titolo originale + transliterazione/inglese**: complesso lato codice (chiamata API extra), comunque "Dhurandhar" resta un nome strano per italiano.

### Strategia scelta: D — Filtro per LEGGIBILITÀ effettiva del display title
**Logica chirurgica**: NON filtriamo per lingua, ma per **leggibilità del titolo finale**:
1. Se `title`/`name` (TMDb localizzato it-IT) è in alfabeto latino → usalo
2. Altrimenti, prova `original_title`/`original_name`: se latino → fallback a quello
3. Se nessuno dei due è latino → escludi l'item dal feed

**Vantaggio**: passa "Dhurandhar" se TMDb ha l'`original_title` in latino, scarta solo titoli completamente non leggibili. Niente whitelist da mantenere, niente esclusione categoriale di un'intera nazionalità.

### Implementazione tecnica
**3 helper functions aggiunte** in `recommendation_api.py` dopo `filter_adult_results`:
- `_is_latin_readable(text)`: regex `^[A-Za-z0-9\s\-:.,'!?&()\[\]/+*\u00C0-\u017F\u2013\u2014\u2018\u2019\u201C\u201D]+$` — copre latino + accenti europei estesi (italiano/francese/spagnolo/tedesco/portoghese) + dash/quote tipografici
- `pick_readable_title(item, content_type)`: implementa la logica 1-2-3 sopra. Ritorna stringa o None
- `filter_unreadable_titles(items, content_type)`: versione "lista intera" (disponibile ma non usata direttamente nei feed attuali)

**17 punti di applicazione** in `recommendation_api.py`:
1. `get_top_rated_recent()` movie (riga 1222) + tv (1256) — alimenta "I più apprezzati"
2. `get_trending_tmdb()` movie+tv (1316/1319) — alimenta hero strip
3. `get_now_playing()` (1360) — alimenta "In sala adesso"
4. `get_upcoming()` (1399) — alimenta "Prossimamente"
5. News strip homepage: cinema (1771) + popular TV (1802)
6. `get_scopri_results()` (2237) — `/scopri?tipo=...` (compreso filtro `piattaforma=`)
7. `_fetch_strip()` (2307) — strip "Thriller/Azione/Commedia..." su `/scopri`
8. `get_similar_movies_tmdb()` (2395) — fallback similar nelle schede detail
9. `get_popular_by_genre_tmdb()` (2445) — fallback `/migliori-*`
10. Discover `/scopri` con filtri base (2884/2887)
11. Discover `/migliori-*-su-{piattaforma}` programmatic SEO (3080/3083)

**Edge case gestiti**:
- Funzioni con parametro `limit`: cambiato pattern da `[:limit]` a `while len(results) < limit` per garantire `limit` titoli leggibili (senza, perdevamo titoli quando i primi N erano non-latini)
- Sostituito vecchio filtro inline `_non_latin` regex in `get_scopri_results()`: catturava solo cirillico/arabo/cinese/giapponese, NON copriva devanagari/hangul/tamil — sostituito con pattern allow-list (latino+accenti) invece di deny-list incompleta

**Cosa NON è stato modificato**:
- Tabella `seo_titles` (~1400 titoli curati manualmente, già leggibili)
- `_is_adult_content()` keyword matching (intenzionalmente accetta qualsiasi titolo per controllare blacklist sostantivi adult)

### File modificato
- `core/recommendation_api.py` (un solo file, no main.py, no template)

### Validazione
- ast.parse OK
- Test funzionale 16 input: inglese normale, italiano con accenti, francese con É, em-dash, devanagari, hangul, kanji+kana, arabo, cirillico, stringa vuota, None, apostrofi dritti e tipografici → tutti pass
- Test fallback `pick_readable_title`: caso "title devanagari + original_title latino" → ritorna l'inglese correttamente
- Spot-check produzione post-deploy: `/scopri?tipo=film&piattaforma=netflix` → "Dhurandhar" appare in alfabeto latino (non più devanagari) ✅

### Note
- Il fallback a `original_title` significa che titoli come "Dhurandhar" appaiono col nome latino anche se per l'utente italiano è un nome strano. Va bene: meglio "strano" che "illeggibile". Se in futuro vorremo nascondere anche questi, basta cambiare la logica `pick_readable_title` per richiedere `original_language in {it, en, fr, es, de, pt}`.
- Stima impatto: ~5% del catalogo TMDb filtrato/sostituito, ma feed garantiti del numero richiesto grazie al pattern "itera finché trovi `limit` validi".

---

## 7. Stato Attuale Deployato

Tutti i file consegnati nelle ultime sessioni sono stati deployati dall'owner via:
```
git add ...
git commit -m "..."
git push origin main
```
e Render auto-deploya entro 1-2 minuti.

**Ultimo deploy noto** (21/05/2026, sessione precedente):
1. Pagina `/come-funziona` + mini-strip home + link footer
2. Riordino home mobile con CSS `order`
3. Refresh SEO settimanale completo (5 moduli)
4. Admin report `/admin/report`
5. Strip mobile riformulata con icone Lucide (layout verticale)
6. Performance PR-A: parallelizzazione film_detail similar + progress bar nav + Lucide CLS
7. Middleware HEAD→GET fallback per uptime monitors
8. Admin home `/admin/home` con menu navigazione + widget status

**Deploy 28/05/2026** (sessione corrente — VERIFICARE che owner abbia pushato):
9. SEO PR-1: noindex `/persona/{id}` (`persona.html`)
10. SEO PR-2: noindex condizionale/statico su login, register, la-mia-raccolta, scopri-filtrato, dove-vedere-paginate, piattaforma-subversion, come-404 (`main.py` + 7 template)
- File consegnati: `app/main.py` + `app/templates/`: persona, login, register, la_mia_raccolta, scopri, dove_vedere_hub, platform

---

## 8. Monitoring (NUOVO)

### UptimeRobot (free tier)
- Account creato 21/05/2026
- **3 monitor attivi**, check ogni 5 min, alert email su tutti:
  - `https://cosaguardo.com/` — Home
  - `https://cosaguardo.com/dove-vedere` — Hub SEO
  - `https://cosaguardo.com/film/693134` — Scheda film (Dune Part Two, stabile)
- **Status page pubblica**: `https://stats.uptimerobot.com/rfvarmexRH`
- **Telegram bot non configurato** (owner non vede notifiche su Telegram, eventualmente aggiungere)

### Performance baseline (PageSpeed Insights 21/05/2026)
- **Scheda film** mobile: 93/100 Prestazioni ✓, LCP 2.2s ✓, CLS 0 ✓, TTFB 1.2s 🟡, Core Web Vitals **superata**
- **Home** mobile: punteggio basso (`!`) ma LCP 2.2s ✓, CLS 0 ✓, CWV **superata**. Issue Lighthouse-specific (NO_LCP/TBT Error) probabilmente edge case di simulazione, non realtà utente
- **Diagnosi finale**: sito NON è strutturalmente lento. I "5-6s mobile" segnalati dall'owner erano caso isolato (5G fluttuante + iPhone in stress). Risposta server mediana: 175ms per `/film/`
- **CrUX data** ultimi 28gg (utenti reali): LCP 2.2s, CLS 0 → tutto verde

### Render Logs
- Filtra `GET /film/` o `GET /dove-vedere/` per audit performance
- Tempi tipici 150-225ms su cache TMDb calda
- Restart programmato ogni 6h: mail "Exited with status 143", owner ha filtro Gmail per auto-archive

---

## 9. Stato Affiliazioni Streaming

| Piattaforma | Stato | Network | Note |
|---|---|---|---|
| **Prime Video** | ✅ Attivo | Amazon Associates | In produzione |
| **Sky TV 2023** | 🟡 In approvazione | Tradedoubler | Mail follow-up inviata a Laura Izzo. Commissioni: AOL_tv 100€, CMN_tv 10€, CTC_tv 10€. DeepLinking autorizzato. |
| **Disney+** | 🟡 In approvazione | Tradedoubler | Mail follow-up inviata a Benedetta Dellepiane |
| **TicketOne** | 🟡 In approvazione | Tradedoubler | Lucio Abatemarco. 1,10€/sale. Future use (concerti/eventi tematici) |
| **IBS** | 🟡 In approvazione | Tradedoubler | Libri/DVD per sinergia adattamenti |
| **NOW TV** | ❌ Non trovato standalone | — | Incluso in Sky |
| **Paramount+** | ❌ Non trovato standalone | — | Bundle con Sky Cinema |
| **TIMVision** | ⏳ Awin pending da 3 settimane | Awin | Da contattare TIM direct |
| **Apple TV+** | ⏳ Da fare in 2-3 mesi | Partnerize | Selettivo |
| **Netflix** | ❌ Impossibile | — | Programma chiuso dal 2014 |

### IDs Tradedoubler
- **Publisher ID**: `2470342`
- **Site ID CosaGuardo**: `3484363`
- **Account Manager**: Maria Colamonaco (team Pam.it)

### Regola d'oro affiliazioni
**Solo partner entertainment/streaming/intrattenimento**. Niente merchant generalisti (es. Doctor Quality megastore che ha mandato invito 19/05 — ignorato). Coerenza editoriale aiuta anche le candidature premium (Sky/Disney AM vedono profilo).

---

## 10. Setup Meta (Facebook/Instagram) Ads — RESTRICTED, in attesa review

**Account usato**: account FB della **madre dell'owner** (Albertina Fantini), account personale attivo da anni, mai usato prima per ads/business. Era la scelta ideale (account "vecchio" = sembra umano a Meta).

### Cosa è stato completato (28/05)
- ✅ Business Manager creato — nome `CosaGuardo`, **Business ID `4411482915805955`**
- ✅ Pagina Facebook `CosaGuardo` creata (categoria Website, bio: "Trova film e serie simili a quelli che ami. Scopri dove vederli in streaming in Italia.")
- ✅ Account Instagram `@cosaguardoapp` creato (passato a Business). NB: username scelto dopo aver scartato `cosaguardo6` (numero = sembra fake) e `cosaguardo.it` (incoerente con dominio .com). `cosaguardoapp` è pulito e coerente.
- ✅ Instagram personale mamma collegato al BM (per legittimità) — checkbox "Manage users by adding this profile to People list" NON spuntata (sicurezza)
- ✅ Ad Account creato (EUR, Europe/Rome), self assegnato Admin via "Assign access"

### IL BLOCCO
Al tentativo di aggiungere la carta di credito → errore "Unable to update permissions" (risolto con Assign access) → poi **email Meta: "We restricted your business"**.
- Motivo dichiarato: *"This account was created or used with an automation that doesn't follow our rules. This goes against our Advertising Standards on account integrity."*
- **Causa reale probabile**: setup troppo rapido (BM + Pagina + IG + Ad Account + carta in 1-2 ore) da browser **incognito** + Pagina/IG nuovissimi 0 follower. Pattern che assomiglia a bot automation, anche se fatto da umano. Falso positivo del sistema anti-fraud Meta.
- **Lezione**: per il prossimo tentativo, distribuire setup su 2-3 giorni con "vita umana" sull'account, NO incognito.

### PROSSIMO STEP (bloccato su documento)
- L'owner deve cliccare **"Request review"** sull'email/notifica Meta
- ⚠️ La review richiede **caricare ID della mamma (Albertina Fantini)** — l'owner NON ce l'ha avuto con sé al momento → rimandato
- **Testo review già pronto** (da inviare firmato come Albertina, vedi sotto). Quando l'owner ha l'ID, apre review e incolla.

**Testo Request Review (firma Albertina, lei è titolare account)**:
```
Buongiorno,
Sono Albertina Fantini. Ho creato questo Business Manager per il
progetto di mio figlio Marco, CosaGuardo (https://cosaguardo.com),
un sito italiano di consigli su film e serie TV in streaming.
Marco si occupa del sito ma non poteva creare un BM dal suo account,
quindi l'ho creato io tramite il mio account Facebook personale che
uso da anni. È il mio primo setup di Business Manager: ho seguito la
procedura standard mostrata da Facebook, senza alcuna automazione.
Ho creato BM, Pagina CosaGuardo, account Instagram (@cosaguardoapp)
e Ad Account passo passo. CosaGuardo è un sito reale e funzionante,
visitabile a https://cosaguardo.com. Mio figlio è disponibile a
fornire qualsiasi documentazione (proprietà del sito, documento
d'identità, ecc.). Vi chiedo gentilmente di rivedere la restrizione:
non c'è stata alcuna violazione delle policy, solo un setup molto
rapido fatto in un'unica sessione. Grazie per l'attenzione.
Albertina Fantini
```

### Regole per dopo (se review APPROVATA)
- Lanciare campagna Meta solo dopo aver "warmato" Pagina/IG con qualche post + giorni di vita
- Budget €5/giorno × 6 giorni (€30)
- Format: Reel/Story 9:16, video 15s già pronto
- UTM: `?utm_source=instagram&utm_medium=ads&utm_campaign=lancio_maggio2026`

### Se review NEGATA
- Aspettare 30 giorni, NON creare BM a raffica (Meta vede pattern → ban a cascata)
- NON comprare account "verificati" da terzi, NO VPN per ingannare Meta

### Materiale Meta pronto
- ✅ Video MP4 verticale 1080×1920, 15s
- ✅ Asset loghi (icon-192, icon-512, maskable)
- ✅ Immagine cover Facebook 1640×859
- ✅ Copy in 3 varianti (A pain point preferita)
- ⏳ Email `info@cosaguardo.com` (Namecheap Email Forwarding gratuito, da configurare)

---

## 10-bis. Google Ads — LANCIATO 28/05/2026 (con problema da diagnosticare)

**Strategia**: cluster keyword #1 "film simili a" (USP unica di CosaGuardo + landing `/come/[slug]` già pronte). Espansione ad altri cluster (cosa guardare su X, migliori X, dove vedere X) SOLO dopo 14gg di dati.

### Account
- Nuovo account ads sotto Gmail personale owner **`mfantini84@gmail.com`** (altri account ads erano legati ad altra società, separati)
- Campagna: **`Search-1-cosaguardo`**
- **Bonus €400 credito Google** attivato (matched: spendi €400 in 60gg → ricevi €400). Owner ha scelto €200/mese × 2 mesi = raggiunge threshold.

### Configurazione finale (dopo aver evitato ~8 trappole del wizard)
- **Tipo**: Rete di Ricerca pura (Search) — NO Performance Max, NO partner ricerca, NO Display
- **Obiettivo**: Visualizzazioni di pagina
- **Bid strategy**: **Massimizza i clic** (NON Conversioni — account nuovo senza storia conversioni)
- **Conversioni tracciate** (importate da GA4, solo per misura non per bidding): `episode_click`, `sign_up`, `trailer_play`
- **Località**: Italia, opzione "**Presenza**" (no "Presenza o interesse")
- **Lingue**: solo Italiano (rifiutato suggerimento Inglese)
- **AI Max**: DISATTIVATO
- **Keyword**: 34 totali, phrase match `"film simili a"` + exact match `[film simili a]` su top + titoli specifici (`"film come dune/inception/matrix"`, `"serie come breaking bad"`, ecc.). MAI broad match.
- **1 annuncio responsive**: 15 titoli + 5 descrizioni + 4 sitelinks + callout. Display path `film-simili/consigli`
- **URL finale**: `https://cosaguardo.com/?utm_source=googleads&utm_medium=cpc&utm_campaign=lancio_simili_a`
  - ⚠️ **Landing = homepage, NON /come-funziona**. Decisione owner (corretta): /come-funziona su mobile genera dead-click (3 step non cliccabili, troppo scroll prima del CTA); la homepage ha campo search + CTA immediato = converte meglio il traffico ads.
- **Budget**: €6,50/giorno (€200/mese ÷ 30)
- Vecchia campagna PMax accidentale ("Campaign #1") messa in PAUSA / da rimuovere.

### ⚠️ PROBLEMA APERTO da diagnosticare (motivo nuova chat)
Dopo poche ore: **€11 spesi, 12 click → ~€0,92/click** (molto sopra il €0,10 stimato da Google). E **ZERO registrazioni/sessioni visibili su Clarity** (né ieri né oggi, nessun filtro attivo).

**Ipotesi (da verificare con screenshot)**:
1. Clarity perde le sessioni da ads (bounce <2s sotto soglia registrazione, sampling free tier, script carica dopo il rimbalzo). Clarity è qualitativo, NON affidabile per contare → **GA4 è la fonte di verità**.
2. Click fraudolenti/bot (€0,92/click alto + zero engagement = sospetto). Google dovrebbe filtrare e rimborsare.
3. Tracking rotto su landing con UTM.

**Screenshot richiesti per la diagnosi (owner li porta nella nuova chat)**:
- **GA4 → Reports → Acquisition → Traffic acquisition** filtrato `session source/medium = google / cpc` (o `googleads`): quante sessioni? bounce rate? durata media? pagine/sessione? → se GA4 vede ~12 sessioni = Clarity semplicemente cieca (ipotesi 1, ok ma traffico forse low-quality). Se GA4 vede 0-2 = scollamento serio (ipotesi 2/3).
- **Google Ads → campagna → Parole chiave e contenuti → Termini di ricerca**: le query REALI su cui sono apparsi gli annunci → pertinenti ("film simili a dune") o spazzatura ("film gratis streaming")?

**Decisione intanto**: owner ha scelto di lasciar girare (budget basso €6,50/g). Alternativa era pausare fino a diagnosi. Da rivalutare con i dati.

#### ESITO DIAGNOSI (28/05 sessione 2) — RISOLTO
Analizzati screenshot GA4 (Pagine e schermate) + Google Ads (Termini di ricerca). Conclusione: **NESSUN bug di tracking**. GA4 e Clarity concordano: 12 sessioni, 0 registrazioni. Clarity non mostrava sessioni solo per sampling free-tier + bounce immediato (sessioni 0s scartate). GA4 NON mostrava conversioni (colonne "Eventi chiave" ed "Entrate" = 0): i 45 "eventi" sulla home erano page_view/scroll/session_start automatici, non sign_up.
- **Vera causa**: traffico di bassa qualità. La keyword `"cosa guardare stasera"` (phrase match) pescava query da palinsesto TV lineare ("programmi tv stasera", "che cosa c'è stasera in televisione", "canale 5 stasera", "stasera su italia 1") — pubblico che cerca la guida TV, NON il motore di raccomandazione streaming. Bounce 0s, €9,67 su 10 click inutili.
- **Azione**: keyword `"cosa guardare stasera"` MESSA IN PAUSA dall'owner.
- **Termine pertinente confermato**: `film da vedere assolutamente` già aggiunto come keyword; `"film simili a"` aveva CTR 16,67% (1 click) = il cluster giusto su cui spingere.
- **TODO follow-up**: aggiungere negative keyword di blocco cluster "stasera in TV" (vedi sez. 14); ricontrollare Termini di ricerca dopo 3-4gg per verificare che il budget si sposti su query "film simili a [titolo]".

### Tag Google — ATTENZIONE
Durante setup, Google Ads ha proposto di configurare conversion tracking sovrascrivendo il tag esistente `GT-K4CPRGGN`/`G-HMNFXJ98H1`. **EVITATO** (avrebbe rotto eventi GA4 custom). Conversion tracking funziona già via GA4↔Google Ads linking fatto negli step iniziali. Non serve installare snippet `gtag('event',...)` aggiuntivi (sarebbero doppioni che inquinano GA4). Se il wizard ripropone "Utilizza il tag Google trovato nel tuo sito" → NON confermare, uscire/saltare.

---

## 10-ter. Amazon Associates — attivo 28/05/2026

### Account
- **Tag affiliato**: `cosaguardo-21` (marketplace IT, suffisso `-21`). Usato nella pagina `/cosa-serve`.
- **Commissione dispositivi Fire TV**: 2,50% (vista in SiteStripe). Elettronica in generale = % bassa ma su importi alti (TV 70€+) fa cifra.

### ⚠️ SCADENZA CRITICA — 3 vendite in 180 giorni
- **Amazon chiude automaticamente l'account** (senza email di preavviso) se NON si raggiungono **3 vendite qualificate entro 180 giorni** dall'iscrizione. Le commissioni vengono pagate dal primo ordine, ma l'approvazione DEFINITIVA dell'account richiede le 3 vendite.
- Se scade: account chiuso, va riaperto con NUOVO tracking ID e tutti i link pubblicati vanno aggiornati.
- **Le vendite devono essere da visitatori INDIPENDENTI** generati dai contenuti del sito. Ordini dell'owner o di persone vicine NON contano e sono violazione policy.
- **Valore**: conta che siano 3 ordini reali spediti e NON rimborsati, non l'importo. (Prodotti da 1€ praticamente inesistenti su Amazon comunque.)

### ❌ Cosa NON fare (rischio ban discusso e SCARTATO)
- NO auto-acquisti tramite propri link (vietato, confisca commissioni + ban).
- NO chiedere ad amici di comprare prodotti simbolici "per validare" l'account → pattern che Amazon anti-frode punisce (incrocia indirizzi/pagamenti/IP/device, pattern temporale, importi anomali). Rischio chiusura permanente di `cosaguardo-21`.
- Unica via legittima con persone reali: chi **stava già per comprare** un prodotto vero per sé passa dal link. Ma il modo sicuro e sostenibile = **vendite da traffico reale del sito** (pagine /come/, /dove-vedere/, /cosa-serve).

### Strategia immagini prodotto (futuro)
- Oggi: icone illustrative + link generici (vedi sez. 6-bis).
- Futuro (quando vendite costanti): PA-API per foto reali automatiche + prezzi aggiornati. Richiede vendite attribuite API negli ultimi 30gg per mantenere accesso.

---

## 10-quater. Instagram Ads SCALATO + JustWatch Affiliate richiesto (05/06/2026 sessione 4)

### Instagram Ads — risultati primi 1-2 giorni
- **Campaign**: "CosaGuardo - Traffic", obiettivo Traffic, placement Instagram Reels
- **Creativa**: video 15s freelance già pronto (`CosaGuardo_Ad_Brief.docx`, sessione precedente)
- **Account ads**: di mamma Albertina Fantini (ID Business Manager `4411482915805955`, ora sbloccato post review document upload)
- **URL destination**: `https://cosaguardo.com/?utm_source=instagram&utm_medium=cpc&utm_campaign=first_ig_ad&utm_content=reel_15s`
- **Audience**: Italia, 22-45, italiano, interessi {Netflix, Streaming television, Movies, Television, Amazon Prime Video} — escluso `House (TV series)` come troppo specifico, escluso `Action movies` perché troppo restrittivo
- **Placement**: SOLO Instagram Reels (rimosso Instagram Feed perché video 9:16 non compatibile con aspect ratio Feed)
- **Errore "Advertising currently limited"** durante Publish: risolto facendo login `@cosaguardoapp` su app IG e accettando "Usa gratuitamente con annunci" (richiesta DMA EU). Account sbloccato in 10 min, ad ri-publicata OK.

### Dati primi 1-2 giorni (aggiornati al 05/06)
| Metrica | Valore |
|---|---|
| Spesa | ~€6-10 totali |
| Landing page view | 50 |
| Sign_up reali da utenti SCONOSCIUTI | 3 (verificati in admin) |
| CPC effettivo | €0,12 |
| Costo per registrazione | €2-3 |
| Conversion rate sign_up | ~6% |

**Conversion rate da segnalare**: 6% sign_up rate su traffico IG è altissimo per cold audience. Da verificare se regge su volumi maggiori.

### Confronto con Google Ads (stesso periodo)
| Canale | Spesa 7gg | Click | Sign_up | Cost/click | Cost/sign_up |
|---|---|---|---|---|---|
| **Google Ads** | €46 | 77 | **0** | €0,60 | ∞ |
| **IG Ads** | €6-10 | 50 | 3 | €0,12 | €2-3 |

### Decisione 05/06: PAUSA Google Ads + SCALING IG
- **Diagnosi Google Ads**: top keyword performanti per click ("film da vedere assolutamente" 50 click 0 conv, "serie da vedere assolutamente" 20 click 0 conv) hanno intent generico tipo "cosa guardare ORA in TV". Cercano titoli specifici al cinema (spesso non in streaming), si trovano sul nostro sito che non li ha, vanno via.
- **Decisione**: Google in pausa (status changed da Active → Paused, no eliminazione, keyword e setup preservati per futura riattivazione)
- **Scaling IG progressivo**: €7/g (start) → €10/g (giorno 2) → €13/g (giorno 3, attuale). Step graduali per non shockare learning phase Meta.

### Cosa monitorare prossima settimana
- **Volume sign_up**: con €13/g × 7gg = €91, ci aspettiamo ~400-700 click. Se 6% sign_up rate regge → 24-42 sign_up nuove. Se cala al 2-3% (più realistico su volume) → 8-21 sign_up. Entrambi i casi sono un buon dataset per decidere.
- **Engagement time sito**: utenti IG già scrollano molto (Clarity), verificare se trend continua con audience più ampia
- **Frequency Meta**: <2.5 = audience non saturata, >3 = stai bruciando l'audience iniziale
- **Variante creativa**: se i numeri reggono, valutare seconda creativa per A/B (ma non finché non abbiamo dato stabile sulla prima)

### Cosa NON fare nei prossimi 7 giorni
- ❌ Riattivare Google Ads senza prima cambiare keyword (cluster sbagliato sostanzialmente)
- ❌ Scalare IG oltre €15-20/giorno (saturazione audience precoce)
- ❌ Cambiare audience IG (la learning ha appena iniziato a calibrare)
- ❌ Modificare home/sito drasticamente (CTRL+F "stabilità" — vogliamo isolare il segnale ads dal segnale prodotto)

### JustWatch Affiliate Program — RISPOSTA RICEVUTA 11/06, canone non sostenibile
- **Background**: il fallback dei link "Dove vedere" punta a JustWatch (vedi `_build_affiliate_link()` in `recommendation_api.py`). Su Netflix/Disney+/etc. senza affiliazione diretta, regaliamo traffico a JustWatch.
- **Risposta JustWatch (11/06)**: NON esiste un affiliate puro semplice. Offrono solo due strade: (1) **API a pagamento** (data license) **~€1.500/mese** (variabile per paesi/servizi) + attribuzione affiliate **divisa 50/50** con loro + obbligo di logo JustWatch e link alla pagina titolo su justwatch.com su ogni pagina che mostra i loro dati; (2) **Widget gratuito** ma **non personalizzabile**.
- **Decisione (11/06)**: entrambe scartate per ora. L'API è fuori scala (€1.500/mese fissi su ricavi ~zero, + 50% di commissione ceduta, + i dati streaming li abbiamo già via TMDb); il widget non si incastra con la UI custom del sito. La monetizzazione vera resta sulle **affiliazioni dirette** (Amazon attiva = unica per ora; Sky/Disney/Awin da attivare) → mandare l'utente direttamente sulla piattaforma batte JustWatch (100% commissione, €0 fissi).
- **Azione in corso**: inviata (11/06) mail di chiarimento per chiedere se esiste un **affiliate solo-link senza il canone API**. In attesa risposta. Se no → si tiene JustWatch solo come fallback gratuito non monetizzato (o si toglie), e si spinge sulle affiliazioni dirette.
- **Follow-up**: se silenzio per 3 settimane (entro **25/06**), mandare follow-up educato.
- **❌ CHIUSO 25/06**: inviato sollecito a Maaz Ahmed. Risposta definitiva: **non esiste alcun affiliate solo-link**. Le uniche opzioni restano Widget gratuito non personalizzabile (scartato) o API a pagamento ~€1.500/mese (fuori budget). JustWatch archiviato — nessuna azione ulteriore. Monetizzazione interamente sulle affiliazioni dirette. Eventualmente resta solo come fallback gratuito non monetizzato nei link "Dove vedere".

---

## 11. Decisioni di Design Importanti

### Visibilità del prompt PWA install
**Decisione**: NON intrusivo per nuovi visitatori. Banner solo per utenti già investiti.

### App native su store
**Decisione**: rimandata. Validare prima con dati reali.

### Account FB fake
**Decisione**: NO assoluto. Meta detection ban quasi certo. Account madre (vera persona) è scelta migliore.

### Tracking Pixel Meta
**Decisione**: DISATTIVATO completamente, placeholder no-op. Da riattivare quando BM funzionante.

### Sezione episodi sempre o solo per serie con stagioni multiple?
**Decisione**: sempre per serie TV, anche 1 sola stagione. Episodi = valore SEO.

### Episodi futuri
**Decisione**: SHOW con badge "In uscita" + data, ma NIENTE overview (spoiler).

### Click sui meta pill
**Decisione**: "X stagioni" e "X episodi" → aprono Episodi sulla **prima** stagione.

### Pagina `/come-funziona` — uso parola "AI"
**Decisione**: opzione C — "algoritmo intelligente" nel copy visibile, "AI" nei meta description + ultima FAQ. Onesto e SEO-friendly, evita esposizione a critiche dirette/regolamento AI Act.

### Riordino home mobile vs desktop
**Decisione**: mobile (≤600px) cambia ordine visivo con CSS `order` su `.page-home` (DOM invariato). Ordine mobile: hero → platform → trending → personal (se loggati) → search → strip "Come funziona" → resto. Desktop INVARIATO. Strip è didascalica/onboarding, ha senso DOPO il search su mobile (= "spiegazione di cosa ho appena visto").

### Refresh SEO — automatico vs manuale
**Decisione**: automatico settimanale (lunedì 03:00) via threading.Timer. Owner inizialmente voleva solo manuale ("ho paura di dimenticarmi") ma anche per quel motivo abbiamo scelto automatico = "non rischio di dimenticare". Bottone manuale comunque presente in `/admin/seo`. Endpoint HTTP token per cron esterni futuri.

### GA4 nella admin
**Decisione**: NO integrazione GA4 Data API. Org policy GCP owner blocca chiavi service account. Soluzione: 3 deep-link diretti a GA4 nativo (Home / Acquisizione / Pagine) + tutti i dati prodotto dal DB. Pratico e robusto.

### Admin landing
**Decisione**: `/admin/home` dedicata invece di redirect a `/admin/utenti`. Separation of concerns: home = hub navigazione, utenti = pagina specifica.

### Status widget admin
**Decisione**: statico (verde sempre). Se l'utente vede il widget → il sito è up (l'admin è hostato sullo stesso server). Link a UptimeRobot status page per dettaglio cronologico.

### Affiliazioni generaliste
**Decisione**: NO. Solo partner entertainment/streaming/intrattenimento. Invito Doctor Quality (megastore) ricevuto 19/05 ignorato.

---

## 12. Dati Analytics Noti

**Snapshot Clarity** (dati sporcati da test owner):
- 70 sessioni reali, 0 bot
- Score 88/100, LCP 1.44s, INP 200ms borderline, CLS 0
- 0 errori JS
- 2 sign_up su 70 = 2.86% conversion
- 4 visite da Google organic
- Dead clicks 8.57% (mitigato con lightbox modal)
- 1.07 pagine/sessione (basso → mitigato con sezione Episodi)

**PageSpeed Insights 21/05/2026** (con dati CrUX reali ultimi 28gg):
- Scheda film mobile: 93/100, Core Web Vitals superate
- Home mobile: CWV superate ma punteggio Lighthouse basso (edge case NO_LCP)
- TTFB 1.2s (giallo, server Render fa 175ms, latency rete US/EU→Italia)

**Render Logs** (campionamento 21/05):
- `/film/{id}`: mediana 169ms, range 147-225ms (cache calda, ua=bot)
- `/dove-vedere/{slug}`: 151-184ms

**Search Console errors 21/05**:
- "Esclusa per noindex": pagine admin/login/register, normale
- "Errore server 5xx": ricevuti 2 alert, hard refresh occasionali durante restart 6h. Da monitorare.

**Da fare**: owner deve aggiungere proprio IP a Clarity exclusions.

### Aggiornamento dati 28/05/2026
- **Search Console — pagine indicizzate**: ~33K (18/05) → ~39,9K (qualche giorno dopo). SALITE nonostante PR-1+PR-2 = lag normale di deindex (Google scopre nuove `/film/{id}` `/serie/{id}` `/persona/{id}` prima di ricrawlare le noindex-ate). Esempi URL indicizzati visti: tanti `/film/{id}` e `/serie/{id}` con id alti (film TMDb random mai cercati) + `/persona/{id}`.
- **`/come/` indicizzate**: solo 6 su ~750 in sitemap (crawl budget esaurito dalle junk). Query reali su /come/: "film simili a send help", "film simili a war machine". 16 click / 856 impressioni / posizione media 9 negli ultimi 3 mesi.
- **Calo visite**: owner ha notato 6/giorno → 1 o 0/giorno da Google negli ultimi giorni. Diagnosi probabile: combinazione di (a) volatilità naturale sample minuscolo, (b) early lag del cleanup SEO. NON allarmante di per sé ma da monitorare. Recupero atteso in 1-2 mesi col redistribuirsi del crawl budget.
- **Clarity non registra sessioni ads**: vedi sez. 10-bis (problema aperto Google Ads).
- **Verifiche View Source confermate 28/05**: `/persona/X` = `noindex, follow` ✓, `/scopri?genere=thriller` = `noindex` ✓.

---

## 13. Convenzioni di Recupero Contesto

Per file che NON sono in questo handover, posso essere chiesto di:
1. **Leggere file inviato dall'owner**: l'owner può sempre rimandare un file specifico (`db.py`, `main.py`, ecc.). Più affidabile della mia memoria delle 100+ uploads precedenti.
2. **Repository structure**: sopra in sezione 3.
3. **Schema DB completo**: principali tabelle:
   - `users` (id, email, password_hash, first_name, last_name, birth_date, created_at)
   - `searches` (id, user_id, seed_titles, content_type, created_at)
   - `daily_recommendations` (user_id, rec_date, recommendations TEXT JSON)
   - `user_feedback` (user_id, title, content_type, feedback_type, created_at)
   - `user_title_state` (user_id, tmdb_id, content_type, status)
   - `home_picks` (user_id, picks_json, created_at)
   - `poster_cache` (tmdb_id, content_type, poster_url, cached_at)
   - `search_cache` (query, results_json, cached_at)
   - `user_series_tracking` (user_id, tmdb_id, status, current_season, total_seasons_at_save, updated_at)
   - `series_seasons_cache` (tmdb_id, title, total_seasons, status, last_air_date, cached_at)
   - `series_episodes_cache` (tmdb_id, season_number, episodes_json, cached_at)
   - `streaming_alerts` (id, email, tmdb_id, content_type, title, user_id, notified_at, created_at)
   - **`seo_titles`** (slug, tmdb_id, content_type, title, year, popularity, vote_average, poster_path, overview, source, seasons_count, seasons_bumped_at, release_date, updated_at)
   - **`seo_refresh_log`** (id, started_at, finished_at, trigger, evergreen_*, new_*, seasons_detected, duration_seconds, error)

---

## 14. TODO List Futuro (in ordine di priorità)

### ✅ CHIUSO 08/06 — Cloudflare edge cache HTML NON FATTIBILE (era il 🔴 URGENTE)
- ✅ **Cloudflare cache HTML edge — CHIUSA come NON fattibile.** Causa root identificata: **Render mette il PROPRIO Cloudflare davanti all'app** (Render è un provider Cloudflare-for-SaaS). Il tuo zone Cloudflare + quello di Render formano una configurazione **orange-to-orange (O2O)**, e il controllo della cache O2O è **disponibile solo per clienti Enterprise**. Sul piano Free le tue Cache Rules sull'HTML non hanno alcun effetto: il Cloudflare di Render tratta le pagine come contenuto dinamico, le **re-chunka rimuovendo il Content-Length** PRIMA che arrivino al tuo zone → `cf-cache-status: DYNAMIC` su tutto, sempre.
  - **Diagnosi completa (08/06)**: il middleware backend funzionava perfettamente (provato con header debug `x-cg-clen` → l'app emetteva Content-Length corretto, 121671 byte). Il chunking che azzera la cachabilità avviene **a valle dell'app e a monte del tuo CDN**, in un layer che non controlli. Nessuna correzione app-side o config-side può aggirarlo. Confermato anche via grey-cloud + `nslookup` (cosaguardo.com → 216.24.57.1 Render, risponde comunque `Server: cloudflare`).
  - **Nota strategica importante**: l'overage di banda era comunque **traffico BOT su URL diversificate** → quasi tutto sarebbe stato cache MISS anche con edge cache funzionante. La cache edge non avrebbe risolto il problema reale. Lezione: per la banda da scraper, la leva non è il CDN.
  - **Cleanup fatto 08/06**: revert del middleware ASGI + rimozione header debug `x-cg-*` in `main.py` (deployato, verificato pulito); cancellate TUTTE le Cache Rules / Page Rules / regola TEST su Cloudflare. **Restano (corretti, non toccare)**: DNS Proxied (arancione), SSL Full strict, Always Use HTTPS, HTTP/3. Resta attivo il middleware header-only `cloudflare_cache_headers` → `Cache-Control: max-age=7200` per la **browser cache** (piccolo risparmio reale su utenti umani che ricaricano entro 2h).

### 🟡 Banda Render — vere leve (post-Cloudflare, in ordine di impatto)
- ✅ **Blocco bot al CDN edge — FATTO 08/06** ⭐ (leva banda PRINCIPALE, sostituisce il rate limiting app-side previsto). Risolto al bordo Cloudflare invece che nell'app → i bot bloccati NON raggiungono Render = banda risparmiata diretta. Tre livelli attivi:
    1. **Block AI bots** (Security → Bots) ON, scope "Block on all pages" + **AI Labyrinth** ON. Blocca i crawler di training AI (GPTBot, ClaudeBot, Bytespider). NON tocca Googlebot/Bingbot. Era già di default su "Block all pages".
    2. **WAF custom rule** "Block scraper SEO commerciali" (Security → WAF → Custom rules, 1/5 usate, Active). Espressione: `http.user_agent contains` SemrushBot / AhrefsBot / MJ12bot / DotBot / DataForSeoBot / PetalBot / BLEXBot → action Block.
    3. **Rate limiting rule** "Rate limit pagine pesanti" (0/1→1/1 usata, Active). Match: path `/film/` o `/serie/` o `/persona/`. Soglia: **20 requests / 10 secondi per IP** → Block per 10 secondi. NB Free plan: solo Block (no Managed Challenge), period e duration fissi a 10s. Soglia 20 scelta prudente per non colpire utenti mobile dietro IP condiviso operatore.
  - **Bot Fight Mode**: LASCIATO SPENTO di proposito (rischio falsi positivi su Googlebot su Free, no eccezioni granulari). Riconsiderare solo se 1-3 non bastano.
  - **DA MONITORARE 3-5 giorni**: (a) Security → Events → contatori regole devono salire (= spazzatura intercettata); (b) Render → Metrics → Bandwidth deve scendere. **Falso positivo da controllare**: se in Events vedi bloccati che sembrano utenti veri (IP residenziali/mobile IT, pochi hit), ALZARE soglia rate limit da 20 a 30-40.
- ✅ **Riduzione peso pagina — FATTO 09/06** ⭐ (leva banda diretta). Pagina `/migliori-*` da **121 KB → 53 KB (-56%)**. `base.html` (guscio comune a ogni pagina) da 96.9 KB → 30.0 KB estraendo:
    1. **CSS inline → `app/static/css/components.css?v=1`** (26.9 KB). `<link>` messo nella stessa posizione del vecchio `<style>` (prima di `style.css`) → cascade preservata, zero rischio. Validato graffe bilanciate. Verificato: sito identico, servito da ServiceWorker in 1ms.
    2. **JS body inline → `app/static/js/base.js?v=1`** (42 KB), caricato `defer`. Blocco analytics/consent resta inline in `<head>` (deve girare per primo). Flag `IS_LOGGED_IN` ora da bootstrap inline `window.__CG_LOGGED_IN`. Validato `node --check`. Verificato post-deploy: console pulita, ricerca/menu/banner/icone/login OK.
  - **Perché conta**: i 69 KB di CSS+JS ora escono dalla banda Render (li serve cache browser/edge, non uvicorn) e si scaricano UNA volta. Ogni pagina successiva e ogni hit bot sull'HTML: ~53 KB invece di 121. Funziona anche nel setup O2O perché i file statici per estensione vengono cachati (è solo l'HTML che non si poteva cachare).
  - **CACHE-BUSTING**: ad ogni modifica di `components.css` o `base.js`, BUMPARE `?v=N` nel rispettivo tag in `base.html` (come `style.css?v=18`). Dimenticarlo = utenti con versione vecchia in cache.
  - **Lavoro futuro possibile** (NON urgente): `detail.html` ha ~37 KB di JS inline proprio, `index.html` ~29 KB inline + carica già un suo `/static/app.js`. Stessa tecnica applicabile per pagina, ma una alla volta e solo se serve.
- ☐ **PR-3 noindex** — GIÀ LIVE, riduce il crawl budget speso dai bot legittimi sulle ~50K pagine junk. Effetto in arrivo (vedi monitoraggio GSC sotto).
- ☐ **(backstop) Rate limiting app-side** — solo SE il blocco edge non basta. slowapi / limiter in-memory / tabella SQLite. Con i 3 livelli edge sopra probabilmente non necessario.

- ☐ **Monitorare Clarity post-modifiche home anon** (7-10 giorni): confronto bounce <5s utenti IG prima/dopo, scroll depth fino a box algoritmo, click su campi titoli. Target: bounce <5s deve scendere, click box algoritmo deve salire. Vedi sez. 6-quinquies.
- ☐ **Monitorare scaling IG Ads** (€13/g per 7gg): target ~400-700 click totali, sign_up reali da utenti sconosciuti, conversion rate (era 6% su 50 click, da vedere se regge). Vedi sez. 10-quater. **NON modificare audience/creativa/budget nei 7gg** finché non c'è dato stabile.
- ⏳ **Motore consigli — RICOSTRUITO 15/06, DA VALIDARE LIVE** ⭐ (era: "Rivedere logica accordo tra seed"). DIAGNOSI confermata coi dati: la causa NON era una soglia, ma la **sorgente candidati** = solo grafo locale MovieLens (9.742 film, ~2018, no serie, no tmdb_id). Conseguenze: (a) seed risolto sul film SBAGLIATO ("Joker"→"Batman Beyond: Return of the Joker"; "Parasite"→id 2256 horror anni '80), (b) similarità per co-visione, non tematica (Inception+Interstellar dava action a caso). RISCRITTO `recommendation_api.py` su modello TMDb-primario (= `recommendation_tv.py`). DA FARE: validare live (Inception+Interstellar deve dare sci-fi; Joker→2019; combo Parasite+Joker/Her+Eternal Sunshine devono dare >3 risultati pertinenti). Vedi **sez. 15**.
- ✅ **Bottoni "ricerche preimpostate" — FATTO 11/06** (richiesti owner). In `index.html`, sotto i campi input e sopra "Trova consigli". **2 bottoni** (lo spazio mobile non reggeva 3 senza spingere giù la CTA): 🎬 **Inception + Interstellar** (type=movie) e 📺 **Breaking Bad + I Soprano** (type=tv). Funzione `fillExample(type,t1,t2)`: imposta il radio Film/Serie giusto + `updatePlaceholders`, riempie Titolo 1 e 2 (svuota gli altri), NON invia il form (scrolla su "Trova consigli" → l'utente impara il meccanismo). Label spiega i 2 passaggi ("Tocca un esempio, poi premi «Trova consigli»"). Stile a contorno azzurro scoped `#recommend-form .example-combo-btn` con `!important` (necessario per battere una regola globale dei bottoni in style.css che da DESKTOP li dipingeva blu pieno; da mobile invece era già ok → la regola globale è desktop-only). **Combo Chernobyl+Band of Brothers**: testata e valida ma esclusa dai bottoni per spazio — tenuta come riserva / utile per la revisione algoritmo. **NB**: le combo 2-6 film e 8/10 serie davano solo 3 risultati "potrebbe piacerti" → vedi task revisione logica accordo seed (sopra).
- ☐ **Monitorare GSC post-PR-3** (settimanale, NON giornaliero): "Esclusa noindex" deve SALIRE, "Indicizzate" deve SCENDERE da 50K verso ~5-8K. Tempistica 4-8 settimane. NON rollback nel frattempo. NON altre PR SEO finché non stabilizzato. **NB (08/06)**: il report "Indicizzazione pagine" di GSC è fermo al **29/05** = PRE PR-3 (live 01/06), normale ritardo di GSC (non è un bug). Quindi i 50,5K indicizzate / 18,2K no NON riflettono ancora il noindex. Il calo di "Indicizzate" è ATTESO e = SUCCESSO, non allarme.
- ☐ **Monitorare scadenza Amazon Associates 180gg** (sez. 10-ter): 3 vendite qualificate da traffico reale. NO acquisti pilotati.
- ☐ **Follow-up JustWatch**: inviata mail chiarimento 11/06 (esiste affiliate solo-link senza canone API?). In attesa risposta. Se silenzio entro ~25/06, sollecito o si chiude. Vedi sez. 10-quater.
- ☐ **Aggiungere negative keyword Google Ads** (per quando si riapre): phrase match `"stasera in tv"`, `"programmi tv"`, `"palinsesto"`, `"canale 5"`, `"italia 1"`, `"rai"`, `"guida tv"`, `"film cinema"`, `"al cinema"`. *Nota*: Google ora in pausa (sez. 10-quater), questo step solo se decidiamo di riattivare.
- ✅ **Cloudflare DNS + SSL + Proxy setup** — FATTO 06/06 (sito ora via Cloudflare, SSL Full strict, HTTPS forzato). RESTA valido.
- ✅ **Bing Webmaster verification meta tag** — FATTO 06/06
- ↩️ **Middleware cloudflare_cache_headers** — deployato 06/06, poi **revertito/ripulito 08/06** (edge cache non fattibile, vedi blocco CHIUSO in cima). Resta la versione header-only per browser cache. main.py pulito da middleware ASGI e debug.
- ✅ **Deploy PR-3 (SEO indicizzazione)** — FATTO sessione 3, **VERIFICATO end-to-end in prod 08/06**: ramo INDEX (titolo curato → `index, follow` + canonical `/dove-vedere/{slug}`, testato su Dune Parte Due / In the Grey / Addicted); ramo NOINDEX (titolo non curato → `noindex, follow`, nessun canonical, testato su /film/2 e /film/3). Template renderizza correttamente.
- ✅ **Fix frammentazione sessioni Clarity — FATTO 08/06** (`base.html`). Causa: dal 31/10/2025 Clarity richiede un SEGNALE di consenso esplicito per il traffico EEA (utenti IT); caricare lo script non basta → senza segnale ogni pageview = sessione nuova (1 sessione per pagina). Fix: aggiunta chiamata `window.clarity('consentv2', { ad_Storage: "denied", analytics_Storage: "granted" })` DENTRO `loadClarity()` (chiamato solo in contesto già consentito). `analytics_Storage: granted` abilita `_clck`/`_clsk` = stitching sessioni; `ad_Storage: denied` coerente con privacy policy (no cookie pubblicitari). Validato `node --check`. **Verifica post-deploy**: in incognito, accetta cookie, naviga 2-3 pagine, il valore di `_clsk` deve restare STABILE (prima cambiava ad ogni pagina). Vale solo in avanti e solo per chi accetta. NB se mai continuasse a frammentare con `_clsk` stabile, unica variabile da rivedere = provare `ad_Storage: "granted"`.
- ✅ **Privacy policy aggiornata — FATTO 08/06** (`privacy.html`): aggiunto Microsoft Clarity nelle sezioni Cookie (registrazione sessioni anonime + heatmap, no cookie pubblicitari) e Condivisione dati; aggiornata data a giugno 2026. Validato Jinja2 parse.
- ✅ **Deploy bug fix /scopri + chip generi** — FATTO sessione 3
- ✅ **Diagnosi Google Ads** — FATTO sessione 2-4. Ora in pausa con motivazione completa (sez. 10-quater).
- ✅ **Diagnosi crollo SEO 19-20/05** — FATTO sessione 3
- ✅ **Strategia ads ottimizzata** — FATTO sessione 4 (Google pausato, IG scalato a €13/g)
- ✅ **Home ottimizzata per first-impression anon** — FATTO sessione 4 (sez. 6-quinquies)
- ✅ **Hero /dove-vedere compatto mobile** — FATTO sessione 4 (sez. 6-sexies)
- ✅ **Strategia D filtro titoli leggibili** — FATTO sessione 4 (sez. 6-septies)
- ✅ **Strategia D estensione /scopri+piattaforma** — FATTO 06/06 (fix screenshot Dhurandhar su scopri Netflix)
- ✅ **Meta Request Review** — FATTO sessione 4 (account sbloccato dopo upload ID)
- ✅ **JustWatch Affiliate request** — INVIATA sessione 4 (mail partnership)

### Bloccato (in attesa di evento esterno)
- ⏳ Approvazione programmi Tradedoubler (Sky, Disney+, IBS, TicketOne) — 1-14 giorni
- ⏳ Follow-up Awin (TIM, NOW) o decisione di abbandono
- ⏳ Risposta Maria Colamonaco / account manager Tradedoubler
- ✅ **JustWatch — CHIUSO 25/06**: confermato che non esiste affiliate solo-link (solo API ~€1.500/mese o widget non personalizzabile). Archiviato, vedi sez. 10-quater e 20.2.
- ⏳ **PR-3 in digerimento** (4-8 settimane): **verificata attiva 08/06**; **al 25/06 confermato funzionamento** via GSC (~113K pagine "Escluse con noindex"). Ora si aspetta solo il calo di "Indicizzate" (da 54K). Aspettare prima di altre PR SEO.
- ✅ **Dati IG settimana scaling — OK (25/06)**: a regime €13/g, costo per visita reale ~15 cent (sotto soglia saturazione). Si mantiene così, nessuna modifica.

### Pronti da fare (dopo verifiche correnti)
- ✅ **Sistema feedback rating raccomandazioni — FATTO 11/06** ⭐ (idea owner 05/06). Microform "Questi consigli ti convincono?" (scala 1-10, 😞→😍) sotto i risultati dell'algoritmo, con reazione condizionale: 8-10 = grazie + CTA registrazione (solo se anonimo); 6-7 = "Cosa avresti voluto vedere?" + quick-select; 1-5 = "Ci dispiace. Cosa non ha funzionato?" + quick-select. Quick-select: Titoli troppo conosciuti / troppo sconosciuti / Lontani dal mio gusto / Niente di nuovo (già visti tutti) / Manca il titolo che cercavo + campo libero "Altro".
  - **Costruito in 4 step** (DB → API → frontend → admin):
    - **DB** (`app/db.py`): tabella `recommendation_feedback` (id, session_id, user_id, rating, seed_titles, recommended_titles, complaint_buttons, free_text, created_at) — liste salvate in JSON. Auto-creata da `init_db` (`IF NOT EXISTS`), nessuna migrazione. Funzioni `save_recommendation_feedback(...)` e `get_recommendation_feedback_stats(limit_comments=50)` (ritorna total, avg_rating, buckets 1-5/6-7/8-10, distribution 1-10, complaint_counts ordinati, recent_comments).
    - **API** (`main.py`): `POST /api/recommendation-feedback` — **senza login**, funziona per anonimi (session_id stabile in `request.session["cg_sid"]`), user_id solo se loggato. Valida rating 1-10, sanitizza/cap liste e testo.
    - **Frontend** (`results.html`): microform vanilla fail-silent. Legge seed da `.seed-tag` e titoli consigliati da `.result-scroll-title`, usa `window.__CG_LOGGED_IN` per la CTA. CSS in tema appeso allo `<style>` di results.
    - **Admin** (`admin_feedback_stats.html` nuova + route `/admin/feedback-stats`): nota selection-bias in cima, stats (totale/media/fasce), distribuzione 1-10 a barre, classifica motivi, ultimi commenti. Stato vuoto finché total<1. Link dalla card "⭐ Feedback consigli" in `admin_home.html`.
  - **Dove si legge cosa**: testo libero "Altro" → sezione "Ultimi commenti" (individuale, voto+data); bottoni quick-select → sezione "Motivi più segnalati" (aggregato, conteggio). NB: nella dashboard bottoni e commenti sono in sezioni separate (il dato però è sulla stessa riga DB) — possibile micro-miglioramento futuro: mostrare per ogni commento anche i bottoni di quella risposta.
  - **Lettura dati**: il rating misura *qualità della lista presentata*, non qualità post-visione. Selection bias (rispondono molto contenti/delusi) → **non interpretare medie sotto le 30 risposte**.
- ✅ **Fix navigazione admin — FATTO 11/06**: aggiunto tasto "← Home Admin" su `/admin/utenti` (mancava) e cambiato quello di `/admin/streaming-alerts` da "← Utenti" a "← Home Admin" (punta a `/admin/home`). Stesso tasto anche sulla nuova dashboard feedback.
  
- ☐ **Decisione pagine attore curate** (sez. 6-quater): guardare GSC Query post-stabilizzazione → se 5-10 nomi specifici hanno volumi reali con angolo "alla CosaGuardo" → pagine curate manuali (es. "film romantici come quelli di X"). MAI 200 automatiche.
- ☐ **Google Ads ricontrollo Termini di ricerca**: solo se decidiamo di riattivare il canale.
- ☐ **Amazon /cosa-serve upgrade**: prodotti specifici + foto reali via PA-API quando vendite costanti.
- ☐ **Audit slug generici** in `seo_titles`: `/come/michael` e simili (bug slugify o titoli ambigui).
- ☐ **Verificare `form_submit` duplicato in GA4** (doppio fire sospetto durante setup Ads).
- ✅ Configurare `info@cosaguardo.com` via Namecheap Email Forwarding — FATTO 11/06
- ✅ Rinominare conversioni Google Ads (erano auto-generate "Obiettivo consigliato") — FATTO 11/06
- ✅ Aggiungere IP owner a Clarity exclusions — FATTO 11/06
- ✅ Audit mobile fisico — DECISO 11/06 di NON farlo: coperto di fatto da Clarity (registrazioni quasi tutte mobile → dead click e 404 visibili in contesto reale) + uso diretto dell'owner da telefono. Unico punto cieco: problemi puramente estetici che non causano misclick (Clarity non li segnala) — minori, si sistemano se notati.
- ✅ Link admin → status page UptimeRobot — FATTO 11/06 (coperto: la home admin ha già widget status + link UptimeRobot, e streaming-alerts ora rimanda alla home)
- ☐ **Refactor CSS mobile-first** (annotato sez. 6-sexies): non urgente, da fare quando si ferma il flusso di micro-modifiche UI

### Idee in standby (non ora)
- ☐ **Reel IG/TikTok con consigli legati a una serie/film** (content marketing, idea owner 12/06): video brevi tipo "Se ti è piaciuta [serie], guarda [X]". Per restare coerenti col prodotto: usare i **risultati del nostro algoritmo** come fonte dei consigli del reel (anche partendo da più serie/film in input → output), così il contenuto social rispecchia ciò che fa il sito e rimanda lì. Canale di crescita organica oltre agli ads IG.
- ☐ **Test/quiz "Sei un cinefilo vero"** come content marketing — VALUTATA E SCARTATA per ora (sessione 4): rischio diluire il prodotto, content marketing prematuro. Riconsiderare dopo 1-2 mesi di traffico stabile.
- ☐ **Riduzione box algoritmo a 1 solo titolo** per ridurre friction — VALUTATA E SCARTATA per ora (sessione 4): con 1 titolo perdiamo USP (= TMDb/JustWatch). Manteniamo min 2-3 titoli. Risolviamo friction con bottoni esempio preimpostati invece (vedi sopra in URGENTE).
- ☐ **Lista raccomandazioni condivisibile senza registrazione** (cookie/localStorage): "Continua dove avevi lasciato". Buona idea ma da considerare solo dopo dati box algoritmo.

### Da pianificare quando affiliazioni approvate
- ☐ Integrare link affiliati in sezione "Dove vederlo in Italia" delle schede
- ☐ Modificare `_build_affiliate_link()` in `recommendation_api.py` per Sky/Disney/etc
- ☐ Hub editoriali per affiliati: "Le migliori serie su Sky", "Cosa guardare su Disney+ questo mese"
- ☐ Aggiungere parametro affiliato JustWatch a `jw_link` se richiesta approvata

### Performance / Tech debt (basso impatto)
- ☐ Critical CSS inline + style.css async (Strategia 1 PR-B performance) — solo SE PageSpeed home rimane problematica dopo qualche settimana di dati reali
- ☐ Audit fetch async in detail.html (4 fetch al load: track, alert, feedback, seasons) — defer non critici dopo `window.load`
- ☐ Compressione immagini TMDb (95KB risparmiabili) — richiede Cloudflare Image Resizing o self-host

### Phase 3 futura (mesi)
- ☐ Cron job per notifiche email "X è ora in streaming!" (Resend/Postmark)
- ☐ Newsletter settimanale (se vorrai)
- ☐ Pulizia codice morto in `style.css` (vecchie classi signup-card/signup-prompt v1)
- ☐ Export CSV admin streaming alerts
- ☐ Filtri admin (data, content_type, pending only)
- ☐ Apple TV+ affiliation via Partnerize (post-2-3 mesi traffico)
- ☐ Capacitor + app store (validare prima la domanda)
- ☐ Tracking "ho visto questo episodio" per serie
- ☐ Pagine hub mensili `/nuove-uscite-novembre-2026` (after validation new_releases settimanali)
- ☐ Auto-promozione esplicita new_release → evergreen (oggi è implicita, sufficiente)
- ☐ UptimeRobot Telegram bot (quando owner pronto a ricevere notifiche)
- ☐ Widget live status (non statico) in admin home — richiede API key UptimeRobot

### Bug noti / Tech debt
- Render manda mail "Exited with status 143" ogni restart programmato (filtro Gmail attivo)
- Classi CSS dead in `style.css`: `signup-card-*`, `signup-prompt-*` (sostituite da `cg-signup-card-*` inline)
- Search Console "Errore server 5xx" occasionali (probabilmente durante restart 6h, da monitorare se ricapita >1x/settimana)

---

## 15. Comportamenti da Mantenere

Quando rispondo all'owner in nuova chat, devo:

1. **NON ricaricare contesto storico**: dare per acquisito il contenuto di questo handover
2. **Chiedere il file specifico** se serve guardare codice esatto (più affidabile della memoria)
3. **Usare le convenzioni** stabilite in sezione 4-5
4. **Validare prima di consegnare**: ast.parse, Jinja render, node --check, balanced braces
5. **Output struttura**: `/mnt/user-data/outputs/app/templates/`, `/mnt/user-data/outputs/app/main.py`, `/mnt/user-data/outputs/core/...` (NOTA: `core/` al root, non in `app/`. `templates/` è dentro `app/`)
6. **Tono**: pratico, decisioni chiare, raccomandazione finale netta
7. **Lingua**: italiano
8. **Comandi git**: brevi, formato `git add ...` / `git commit -m "..." -m "..."` / `git push origin main` in code-blocks separati
9. **Performance work**: misurare PRIMA di ottimizzare. Mai assumere il collo di bottiglia senza dati (Render Logs, PageSpeed, Search Console).
10. **Quando sovrascrivo file**: dire all'owner di rimandare gli ultimi se non sono sicuro di partire dalla versione corrente in prod.

---

## 16. Note Finali

**Punto di forza di CosaGuardo**: italiano-first, contenuti SEO-ottimizzati (`/come/`, `/dove-vedere/`, `/come-funziona`), nicchia ben definita, motore di raccomandazione (non catalogo navigabile come JustWatch), monetizzazione via affiliazioni native pertinenti (no banner ads invasivi).

**Punto debole attuale**: nessuno conosce il sito ancora. ~70 utenti reali, niente brand awareness. **Stato lancio ads (28/05)**: Google Ads LANCIATO (campagna Search "film simili a", €6,50/g) ma con problema da diagnosticare (€0,92/click, 0 engagement visibile su Clarity — vedi sez. 10-bis). Meta Ads BLOCCATO (Business restricted, review in attesa di ID — sez. 10). In parallelo, cleanup SEO in corso (PR-1+PR-2) per liberare crawl budget e far ranking le pagine `/come/` che ricevono già traffico organico.

**Filosofia condivisa Owner ↔ Claude**: build solido, valida prima di scalare, no shortcut rischiosi (es. account fake, app native premature, ottimizzazioni performance senza dati).

**Quando in dubbio**: priorità a contenuti SEO + raccolta lead + onestà UX. Crescita organica + affiliazioni native è il modello sostenibile per CosaGuardo.

---

---

## 15. Sessione 15/06 — Motore consigli TMDb + Incidente bot/origin-IP

### 15.1 INCIDENTE SICUREZZA — scraper da datacenter sull'IP di origine (RISOLTO)

**Sintomo**: durante la sessione il sito è andato giù più volte; CPU costantemente al 100% (anche di notte, prima di ogni deploy), memoria in salita, risposte sulle pagine `/film` `/serie` `/persona` da **200 fino a 585 secondi**.

**Diagnosi (dai log Render)**: decine di GET/secondo su `/film/{id}`, `/serie/{id}`, `/persona/{id}` con ID sequenziali, da IP **datacenter** — soprattutto **Apple `17.0.0.0/8`** e **AWS** (`3.x`, `18.x`, `34.x`, `54.x`, `98.80-98.95`). User-agent in gran parte falsificati (`ua=usr`). Visto anche `GET /?prefill=<cinese>` = injection di titoli non-latini.

**Perché Cloudflare non serviva**: i bot avevano scoperto l'**IP di origine di Render** (`216.24.57.1`, il record A) e lo colpivano **direttamente** mettendo `Host: cosaguardo.com`, **saltando del tutto la Cloudflare dell'utente**. Prova: le WAF custom rules, il rate-limit (0 eventi) e perfino la **modalità "Under Attack"** non hanno cambiato nulla; nei log Uvicorn comparivano gli IP AWS/Apple come peer.

**Tentativi falliti (e perché)**:
- WAF / rate-limit / Under Attack su Cloudflare → inutili: il traffico non passa da CF.
- Transform Rule che aggiunge header segreto `X-CG-Edge` + check nell'app → **l'header viene STRIPPATO** dall'infrastruttura di Render (architettura O2O / Cloudflare-for-SaaS). Verificato con rotta diagnostica `/__debug-edge`: `x_cg_edge_ricevuto=false`.
- Check su presenza di `cf-ray` → **fallito**: Render aggiunge `cf-ray` **anche ai colpi diretti**, quindi non distingue.

**SOLUZIONE (funziona)**: blocco lato app. `cf-connecting-ip` riporta in modo affidabile l'IP **reale** del cliente (verificato: utente vero = `93.37.x` residenziale; bot = `17.x`/AWS). Middleware `block_direct_origin` in `main.py`: respinge con 403 immediato (0ms, prima di ogni lavoro TMDb/DB) le richieste a `/film/` `/serie/` `/persona/` il cui IP reale è in `_DATACENTER_CIDRS` (Apple 17/8 + range AWS osservati). Risultato confermato: bot → `403 0ms`, utente vero e **Googlebot** (`66.249.x`, NON nei range bloccati) → `200`.
- Toggle d'emergenza: env **`EDGE_GUARD=0`** disattiva il blocco all'istante.
- **È una blocklist, va estesa** se i bot cambiano rete: nuovi IP datacenter nei log → aggiungere CIDR in `_DATACENTER_CIDRS`. Applebot (17.x) viene bloccato su queste pagine: ok, sono `noindex`.
- La home (`/`) NON è protetta (lì atterrano gli ads): qualche `GET /?prefill=...` da AWS passa ma è leggera (2ms), innocua.

**Learning chiave (architettura)**: con Render (O2O Cloudflare-for-SaaS) **non si può distinguere CF-vs-diretto via header** (header custom strippati, `cf-ray` aggiunto sempre). L'unico segnale affidabile dell'origine reale è **`cf-connecting-ip`**. Stesso muro architetturale già incontrato l'08/06 per l'edge caching.

**2ª ONDATA (stessa giornata) — botnet residenziale + geo-block**: dopo il blocco datacenter, l'attacco è passato a **IP residenziali esteri** (soprattutto ISP vietnamiti: `14.x`, `113.x`, `1.54.x` = VNPT/Viettel/FPT) + qualche range AWS sfuggito (`54.224`, `44.x`, `100.24`). La blocklist datacenter NON prende i residenziali → sito di nuovo giù. SOLUZIONE: **geo-restrizione**. CosaGuardo serve l'Italia → su `/film` `/serie` `/persona` il traffico **non italiano è ~100% scraper**. Header `cf-ipcountry` (affidabile, arriva all'app) → blocco se paese ∉ `_EDGE_ALLOWED_COUNTRIES` (default `IT,SM,VA`, env-override). Verificato post-deploy: VN/US → `403 0ms`, utente IT → 200 in pochi ms, CPU crollata. Queste 3 pagine sono noindex → bloccare Googlebot (US) qui NON costa SEO; `/dove-vedere` (SEO) NON è geo-bloccata. La geo-restrizione di fatto **supera** la blocklist datacenter per il traffico estero (un AWS USA è comunque country US → bloccato). Manopole: env `EDGE_ALLOWED_COUNTRIES` (aggiungi paesi), `EDGE_GUARD=0` (spegne tutto).

**Superfici ancora aperte (sorvegliare)**: la **home `/`** (deve restare per gli ads; qualche `/?prefill=` da AWS passa ma è leggera ~3ms) e **`/dove-vedere`** (deve restare per Googlebot). Se la botnet ci si sposta in forze: limite mirato su `prefill` da IP datacenter, o geo-block di `/dove-vedere` con eccezione crawler verificati.

**Causa di fondo — FATTO (15/06)**: le pagine `/film` `/serie` `/persona` ci mettevano **4-5 secondi** a renderizzare (il pezzo costoso era la sezione "simili", che lanciava il motore consigli a ogni vista). Risolto con **cache della sezione simili** per `tmdb_id` (helper `_cached_similar_movies`/`_cached_similar_tv` in `main.py`, via `cached_call`) + **cache di `get_person_detail`** (`person:detail:v1:{id}` in `recommendation_api.py`, prima era 1 chiamata TMDb a vista). Lo stato utente (preferiti/visto/tracking) resta calcolato al volo, fuori dalla cache. Caveat: il cold-build avviene ancora 1 volta per pagina unica; la cache aiuta utenti veri + hit ripetute, e il blocco bot copre i flood di crawl.

**Strascichi — CHIUSI (15/06)**:
- Rotta diagnostica **`/__debug-edge` RIMOSSA** da `main.py`. ✓
- Transform Rule `X-CG-Edge` ed env `EDGE_SECRET` su Cloudflare/Render: **cancellate**. ✓
- WAF rules Cloudflare "Block datacenter / Challenge scraper / Block prefill": tenute ma **inefficaci** (i bot bypassano CF); il blocco vero è lato app.
- **Under Attack Mode**: era stata attivata, ora **disattivata**. Il selettore "Security Level" Medium/High non è più presente nella dashboard nuova: normale, non serve.

### 15.2 MOTORE CONSIGLI FILM — ricostruzione TMDb-primaria (CONFERMATO LIVE + tarato)

**Diagnosi definitiva (dati dal DB motore)**: il motore film generava i candidati SOLO dal grafo locale MovieLens (`db/cosaguardo.db` nel repo, tabelle `titles`+`title_relations`). Quel DB ha **solo 9.742 film, fermo ~2018, zero serie, nessuna colonna `tmdb_id`**. Conseguenze:
- `find_movie_by_title` risolveva il seed sul DB locale con match parziale → film SBAGLIATO: "Joker" → "Batman Beyond: Return of the Joker" (cartone); "Parasite" → id 2256 (horror anni '80, non Bong Joon-ho).
- similarità per **co-visione** (chi ha votato cosa), non **tematica** → Inception+Interstellar dava action a caso.
- copertura scarsa → fallback "potrebbe piacerti" (3 titoli) frequente.

**Due DB distinti (importante)**: APP DB = `/data/cosaguardo.db` (env `DATABASE_PATH`, disco persistente: utenti, `seo_titles`, feedback, `search_cache`...). MOTORE DB = `db/cosaguardo.db` dentro il repo (statico: `titles`, `title_relations` MovieLens; `recommendation_api.py` ha la SUA `get_connection` → `BASE_DIR/db/cosaguardo.db`).

**Ricostruzione** (`core/recommendation_api.py`, funzione `recommend_from_seed_titles` riscritta): rispecchia il lato serie `recommendation_tv.py` (che era già TMDb-primario e funzionava):
- FASE 1: risoluzione seed via TMDb (`_resolve_movie_seed` → `get_movie_tmdb_match`) = film corretto + generi + keywords.
- FASE 2: candidati da TMDb `/movie/{id}/similar` + `/recommendations` (nuovi fetcher `get_similar_movies_rich`, `get_recommended_movies`, `get_movie_keywords_by_tmdb`).
- SCORING: sovrapposizione **keyword/temi** col profilo dei seed (riusa gli helper di `recommendation_tv.py` via import lazy: `build_seed_keyword_profile`, `keyword_overlap_score`, `get_top_matching_seeds`, ecc.) + bonus `seed_coverage` (titolo simile a più seed).
- Output mappato negli **stessi campi interni** di prima (components/avg_score/why_titles/badge/ui_signals) → `main.py`/`results.html` invariati.
- **Idea X/Y di Marco integrata**: i match di un solo seed restano visibili, etichettati "richiama X" via `why_titles` (niente filtro che li escluda; le keyword ordinano, non escludono).
- **MovieLens resta nel file ma INUTILIZZATO** (`recommend_from_seed_ids`, `get_candidates_for_movie`, `find_movie_by_title` ancora presenti) → futuro reintegro come **segnale bonus** dove il film esiste nel grafo.
- Manopole di taratura: peso keyword `* 14.0`, soglia `final_score < 2.0`, contributo genere.

**Validazione**: solo offline (ast.parse + test logica scoring/dedup/X-Y con dati finti). **NON ancora provato live** perché prima la `search_cache` mascherava i risultati (fix sotto), poi è scoppiato l'incendio bot. **PROSSIMO STEP**: con sito stabile, testare Inception+Interstellar (deve dare sci-fi), Joker (→2019), Parasite+Joker / Her+Eternal Sunshine (>3 risultati pertinenti). Joker risolto correttamente è GIÀ stato verificato in un test parziale.

**Lato serie `recommendation_tv.py`**: NON toccato (era già il modello corretto).

### 15.3 Cache versione algoritmo (`main.py`)
La chiave di `search_cache` includeva solo `seed + content_type` → i cambi di algoritmo non invalidavano le liste in cache (TTL 24h) e si vedevano risultati vecchi. Aggiunto `_ALGO_VERSION = "movie-tmdb-v2"` nella chiave. **A ogni revisione dell'algoritmo: bump a "v3", "v4"...** per invalidare da soli. (In alternativa: `DELETE FROM search_cache` via Render Shell.)

### 15.4 Filtro slug SEO non-latini + denylist (`seo_pages.py`)
Audit `seo_titles`: ~25 slug non-latini (CJK/hangul/devanagari/tamil — `\w` di slugify li teneva in ideogrammi) + parole generiche. Aggiunto `_seo_slug_ok()` (regex non-latina `_NON_LATIN_SLUG_RE` + `_SEO_SLUG_DENYLIST`) agganciato in `populate_seo_titles_db` e `populate_new_releases`. Chiude il buco di Strategia D sul lato generazione SEO. Pulizia una-tantum delle righe vecchie via Render Shell (script `DELETE`).

### 15.5 TODO aperti dopo questa sessione
- ✅ **Motore film validato live** (Inception+Interstellar → sci-fi; Parasite+Joker → thriller cupi; Her+Eternal Sunshine → sci-fi intimo). Manopole "equilibrato" confermate.
- ✅ **Alleggerite `/film` `/serie` `/persona`**: cache della sezione "simili" (`_cached_similar_movies`/`_cached_similar_tv`) + cache `get_person_detail`. Vedi 15.6.
- ✅ **Rimossa** rotta `/__debug-edge`; **cancellati** Transform Rule `X-CG-Edge` + env `EDGE_SECRET`.
- ✅ **2ª ondata bot** (botnet residenziale estera) chiusa con geo-restrizione. Vedi 15.1.
- ☐ **Estendere `_DATACENTER_CIDRS` / `EDGE_ALLOWED_COUNTRIES`** se i bot si spostano (nuovi IP/paesi nei log).
- ☐ (Futuro) Reintrodurre **MovieLens come segnale bonus** nel motore film (map titolo+anno).
- ☐ (Futuro) **Service worker auto-update**: far sì che `sw.js` rinfreschi gli asset a ogni deploy, così gli utenti non restino su versioni vecchie (vedi lezione in 15.6).

### 15.6 SESSIONE 16/06 — Affinamento motori film+serie, saghe, autocomplete

**Film — dedup saghe (`recommendation_api.py`)**. Uscivano sequel numerati (Padrino II/III, John Wick 4, Matrix Revolutions). La vecchia `is_franchise_duplicate` (presa dal motore TV) scattava solo con seed ≥2 token o franchise hardcoded → "Il Padrino" (1 token) non la attivava. Aggiunti `_saga_core()` (toglie sottotitolo dopo `:`/` - ` e marcatori finali numerici/romani/parte/capitolo) e `_same_saga()` (alias franchise noti `_KNOWN_FRANCHISE_KEYS` + radice uguale). Applicati: (a) contro i seed (no sequel del film cercato), (b) candidato-vs-candidato (max 1 per saga). NB: titoli **diversi** della stessa saga ma non in `_KNOWN_FRANCHISE_KEYS` (es. Mad Max → Furiosa, Interceptor) NON vengono raggruppati — scelta OK (sono film diversi che un fan può non aver visto).

**Serie — portato al livello film (`recommendation_tv.py`)**. Tre problemi risolti: (1) **filtro lingua rigido** `original_language in {en,it}` → sostituito con `_is_readable_title()` (titolo prevalentemente latino) → serie estere leggibili (Dark, La casa di carta) restano, non-latine (悪霊病棟) escono; (2) **tagli single-seed troppo aggressivi** (azzeravano il seed debole) → ammorbiditi (soglia 0.045→0.02, penalità -3→-1, moltiplicatori 0.70/0.55→0.85/0.72, floor 6→4); (3) **sbilanciamento** (Friends+Chernobyl tutto Friends) → aggiunto **bilanciamento X/Y** in selezione finale (prima i multi-seed, poi round-robin tra i seed) + `best_seed_title` accurato dal seed che ha prodotto il candidato.

**Serie — generi animazione/kids condizionati al seed**. `EXCLUDED_GENRE_IDS={16,10751}` escludeva sempre Animazione/Family → Peppa+Breaking Bad dava solo BB, e due animate adulte (Rick&Morty+BoJack) davano ZERO. Fix: escludere quei generi **solo se nessun seed** li ha. ⚠️ **Bug nomi/ID**: i generi del SEED sono **nomi** (`genre_ids_to_names` in `find_tv_by_title` → "Animation"), i candidati hanno **ID** (16). Il primo tentativo confrontava nomi vs ID → non scattava mai. Risolto confrontando sui nomi via `TV_GENRE_NAMES`, **includendo anche "Kids" (10762)** perché TMDb etichetta i preschool come Kids, non Animation (es. Peppa). Rick&Morty+BoJack ora ok. Peppa+Breaking Bad resta "quasi solo BB" ed è CORRETTO (coppia degenere, zero affinità — non forzare).

**Autocomplete serie — `search_tv_series` + `app.js`**. Sintomo: titoli famosi non in cima ("walk" non dava The Walking Dead). Due cause in cascata: (a) lato server, ranking per `popularity` (metrica volatile) → cambiato a **`vote_count`** (fama storica, `log10(vc+1)*300`) + **ignora articolo iniziale** (`_strip_article`, "The Walking Dead"→"walking dead" per lo startsWith) + **fetch 2 pagine** TMDb (il titolo famoso era oltre pagina 1); (b) **CAUSA VERA**: `app.js` (`renderSuggestions`) **ricalcolava `_score` lato client** con la vecchia `scoreMovie`, scavalcando il ranking del server. Fix in `app.js`: usa il `_score` del **server** quando presente (serie TV), fallback a `scoreMovie` per i film (che non espongono `_score`). `index.html`: `app.js` ora con `?v=2` (prima senza versione → cache immutable).

**⚠️ LEZIONI CHIAVE (ci sono costate molto tempo)**:
1. **Service worker (`sw.js`) + cache immutable**: il sito è una PWA. Dopo un deploy, il service worker continua a servire i vecchi asset (template/JS) **anche in incognito** → sembrava che i deploy non avessero effetto ("tutto identico"). I file `.py` invece si aggiornavano (provato via `/search-fast` diretto). Diagnosi vincente: aprire l'**asset diretto** (`/static/app.js?v=2`) e il **view-source** della pagina per confermare cosa serve davvero il server, poi `Clear site data` + Unregister service worker. TODO futuro: auto-update del service worker.
2. **Ranking lato client che scavalca il server**: per l'autocomplete, qualsiasi modifica server è inutile se `app.js` riordina i risultati. Controllare SEMPRE il JS prima di tarare il server.
3. **Generi seed = NOMI, candidati = ID** in `recommendation_tv.py`. Mai confrontarli direttamente.
4. **`_score` esposto dal server** è il punteggio autoritativo per l'autocomplete: `app.js` deve usarlo, non ricalcolare.

**Versione cache algoritmo (`main.py`)**: `_ALGO_VERSION` portata fino a **"movie-tmdb-v7"** durante la sessione (ogni cambio motore → bump per invalidare `search_cache`). Prossimo cambio motore: bump a v8.

**Bug noto residuo (minore)**: l'etichetta "Simile a X" sul lato FILM era apparsa sbagliata una volta (2001+American Pie mostrava "Simile a Old Boy"). Non ri-osservato dopo; da verificare se ricapita.

---

## 17. STRATEGIA SOCIAL — Reel/Short (Sessione 17-19/06)

**Obiettivo**: crescita organica via video verticali su IG Reels, TikTok, YouTube Shorts, Facebook. Owner monta da **PC Windows**, pubblica/programma in blocco.

### 17.1 Format del contenuto
Schema base: **"2 titoli di partenza → 5 consigli (≥2 di nicchia)"** — rispecchia il motore del sito, crea curiosità ("il 5°/4° non lo conosce nessuno") e spinge i commenti ("la tua coppia?"). Si fa sia su **film** sia su **serie** (testa anche il toggle Serie del sito).

**Struttura card (7 card classiche, 8 con domanda):**
- Card apertura: i 2 poster di partenza + gancio (es. "5 FILM DA VEDERE SUBITO").
- 5 card consiglio: numero + titolo + anno + (bandiera/etichetta chicca). La trama vive nei **sottotitoli automatici**, non sulla card.
- Card CTA: "LA TUA COPPIA?" → cosaguardo.com.

**Selezione dei 5**: 3 "sicuri" che fanno annuire + 2 chicche con gancio forte (budget, storia vera, premio, "stesso regista di…", premessa assurda). I dati delle chicche vanno **sempre verificati con web search** prima di produrre.

### 17.2 Regole fisse della "ricetta" (DA RISPETTARE nei prossimi pacchetti)
- **Parlato (TTS)**: mai solo il titolo, **sempre almeno una micro-informazione**; alternare frasi **corte e lunghe/descrittive** per non appiattire il ritmo.
- **Voce TTS sempre a 1,1x** (impercettibile, accorcia i video, permette più testo).
- **CTA di chiusura sempre col dominio**: "La tua coppia? Te ne trovo altri/e cinque **su cosaguardo.com**" (a voce + a schermo). NON mettere il link cliccabile nella **didascalia** di TikTok/IG (penalizza la portata); su **YouTube** invece il link in descrizione è OK e va messo.
- **Zone di sicurezza** (UI piattaforme): tenere libero ~15% alto, ~15% destra (pulsanti like/commenti), ~28% basso (nome+didascalia). Contenuto e **sottotitoli a ~35% dal fondo**, font grande + **contorno nero spesso**. (Il primo video aveva titolo/logo/sottotitoli troppo in basso → coperti.)
- **Grammatica domanda**: film → "QUANTI NE HAI VISTI?"; serie → "QUANTE NE HAI VISTE?".
- **Genere**: la card di apertura concorda con film (maschile) / serie (femminile).

### 17.3 Pipeline di produzione (tutto da PC, no costi)
1. **Canva** (1080×1920, sfondo #0B1020): template card riutilizzabile. Usare **cornici (frame)** per i poster → ci si trascina dentro l'immagine e prende misura/posizione automatiche (no resize manuale). Duplicare la card e **sostituire poster/numero/titolo**. Poster da **TMDb** (size **w500**; se il browser salva .webp → convertire con Paint in JPEG, oppure cambiare `original`→`w500` nell'URL).
2. **CapCut desktop**: **Sintesi vocale** (voce italiana energica, fissa per il brand, a 1,1x) → genera audio, poi cancella la casella di testo (resta solo l'audio). **Sottotitoli automatici** dalle voci. Musica royalty-free per **mood** (vedi 17.5) a volume 15-20%. Animazione "Zoom in" sulle chicche per far "sbattere" i ganci. Export **MP4 1080p 30fps** (verificare che il progetto sia **9:16**, non 16:9 — un export uscì 1920×1080 perché il rapporto era orizzontale; si corregge col pulsante "Rapporto" → 9:16 e riesporto).
3. **Riuso progetto CapCut**: NON "Nuovo progetto" (azzera tutto) ma **Duplica** dalla lista progetti → sostituisci le 7-8 clip (trascina il nuovo PNG sopra la vecchia → "Sostituisci", mantiene durata e zoom) → rigenera voci e sottotitoli. Da ~40 min a ~10-15 min a video.
4. **File PC→iPhone** (se serve): Google Drive (NON WhatsApp, comprime). Ma programmando da PC non serve più il telefono.

### 17.4 Apertura "a due tempi" (esperimento hook)
Variante per testare la ritenzione dei **primi 1-2 secondi** (punto debole su TikTok, dove lo stacco è all'inizio = problema di "copertina"):
- **Card A (0-1,5s)**: domanda a tutto schermo su **sfondo scuro pieno** + scritta GIGANTE (es. "QUANTE NE HAI VISTE?", con una parola in azzurro #6EA8FE). Sfondo scuro minimal > collage poster sfocati (più leggibile, "buca" di più). Animazione testo "Digita"/"Pop" + zoom in lento.
- **Card B (1,5-3s)**: i 2 poster + **affermazione** "SE TI PIACCIONO QUESTI 2" (NON una seconda domanda → due domande di fila si annullano) + tema/numero + tease.
- Cut secco/flash (≤0,2s) tra A e B. Voce unica sopra A+B.

### 17.5 Musica per mood (basi fisse royalty-free da CapCut)
Tenere una base per mood, per coerenza: **thriller/teso** (tension, dark, trap cinematic) · **emotivo** (piano, emotional, nostalgic) · **azione** (epic, driving beat, intense) · **fantasy/epico** (orchestral, cinematic trailer) · **crime** (dark, hip-hop/trap). Su TikTok/IG si può anche montare senza musica e aggiungere un **audio di tendenza** dall'app (più portata + zero copyright).

### 17.6 Pubblicazione & programmazione
- **Metricool** (free) = hub unico per programmare e auto-pubblicare su tutte e 4 (IG/FB/TikTok/YouTube). **Auto-pubblicazione confermata attiva su tutte e 4.** Testo personalizzato per piattaforma (caption corta+hashtag per IG/TikTok/FB; titolo #shorts + descrizione lunga col link per YouTube).
- Se una piattaforma non auto-pubblica nel piano free → ripiego con scheduler nativo (TikTok web, YouTube Studio, Meta Business Suite per IG/FB).
- **YouTube "destinato ai bambini" = NO** (sempre): "Sì" disattiva i commenti e taglia la portata.
- Cadenza: **3-4 uscite/settimana costanti** > tante in un giorno. Sera 19-22, orari sfasati tra piattaforme.

### 17.7 Analytics — dove guardare
Il dato profondo (ritenzione, durata media, completamento) sta nelle **native**, non in Metricool (che dà solo overview/orari):
- **TikTok**: account in **TikTok Studio** (il toggle "Creator" non esiste più, gli account personali hanno già Studio). Da app o **business.tiktok.com** → Analitiche → curva di ritenzione secondo-per-secondo (il migliore). NB: "TikTok for Business"/Ads Manager è un'altra cosa (solo per pubblicità a pagamento).
- **YouTube**: Studio → Analytics → "visualizzati vs ignorati", % vista, ritenzione.
- **Instagram**: account Pro → "Visualizza insights" per Reel (views, watch time medio, **salvataggi**).
- **Facebook**: Meta Business Suite → Insights (% visione).
Confrontare ogni video con la **media dei propri precedenti**, non con altri creator. Servono volume + 1-2 settimane per dati affidabili.

### 17.8 Esperimenti in corso (da leggere quando tornano i dati)
- **Apertura classica vs domanda a due tempi**: 3 classici (Forrest Gump, John Wick, Trono di Spade) + 2 con domanda (Il Padrino, Breaking Bad). Guardare ritenzione primi 2-3s + completamento.
- **Formato parlato**: corto-secco (solo titoli) vs lungo-con-trama (micro-info). Capire quale tiene di più.
- **Genere**: sci-fi/mystery vs thriller cupo vs azione vs fantasy vs emotivo vs crime → su quale puntare.
- **Dato già emerso**: su TikTok lo stacco è nei **primi 1-2s** (copertina debole), mentre su IG/FB la gente resta di più; tra video 1 e 2 la % vista è migliorata. Su TikTok lavorare sulla copertina/primo frame più che altrove.

### 17.9 Asset prodotti
- **Cover Facebook**: `cosaguardo_cover_facebook.png` (1702×630), navy + wordmark CosaGuardo + play-button + cosaguardo.com, angolo basso-sx libero per la foto profilo. SVG sorgente disponibile.
- **5 pacchetti video pronti** (copione TTS + testo card + caption + descrizione YouTube + poster list, chicche verificate): Forrest Gump+Le ali della libertà · John Wick+Mad Max · Trono di Spade+The Witcher · Il Padrino+Quei bravi ragazzi (domanda) · Breaking Bad+Peaky Blinders (domanda). Già prodotti in precedenza: Inception+Interstellar, Parasite+Joker, Dark+Stranger Things, Squid Game+La casa di carta, Se7en+Shutter Island.

### 17.10 Prossimi passi social
- Pubblicare/programmare i 5 in blocco su Metricool (settimana di assenza owner).
- Al ritorno: leggere i dati TikTok e tarare formato + genere + tipo di apertura.
- **Caroselli** → ora attivi, vedi sezione 18. **Report metriche** → vedi sezione 19.

### 17.11 Aggiornamenti finali (sessione 17-19/06) — DA RECEPIRE
- **Tutti e 5 i video della settimana COMPLETATI e montati** (3 classici + 2 con domanda).
- **Voce TTS a 1,1x = standard fisso** d'ora in poi (confermato: impercettibile, accorcia, tiene più testo).
- **CTA chiusura = "...su cosaguardo.com" fisso** (voce + schermo). Mai link cliccabile nelle caption IG/TikTok; su YouTube sì.
- **Apertura a due tempi rifinita**: Card A = sfondo scuro PIENO (#0B1020) + domanda GIGANTE con UNA parola in azzurro #6EA8FE (es. "QUANTE NE HAI **VISTE?**"); collage poster sfocati scartato (meno leggibile). Card B = **affermazione** "SE TI PIACCIONO QUESTI 2" (NON una seconda domanda) + coppia + tema/numero + tease. La frase voce dell'apertura va accorciata per stare in ~3-4s su A+B (a 1,1x).
- **Grammatica**: film "QUANTI NE HAI VISTI?" · serie "QUANTE NE HAI VISTE?".
- **CapCut export gotcha**: un progetto può uscire 1920×1080 (orizzontale) se il "Rapporto" è 16:9. Fix: pulsante **Rapporto → 9:16**, ricontrolla che le card riempiano, riesporta. Verifica sempre in Proprietà → Dettagli che sia **1080×1920**.
- **Dato osservato**: su TikTok lo stacco è nei **primi 1-2s** (copertina debole) mentre IG/FB trattengono di più; tra video 1 e 2 la % vista è migliorata. → lavorare sul primo frame su TikTok.

---

## 18. CAROSELLI (Sessione 17-19/06) — NUOVO formato attivo

### 18.1 Cosa sono e dove
Post a schede scorrevoli (immagini). Puntano su **permanenza** (lo swipe) e soprattutto **salvataggi** (su IG spingono molto la portata). **Solo IG + TikTok** (NO Facebook: rende male; NO YouTube: non esistono). Contenuti perfetti da salvare (liste film/serie + dove vederle).

### 18.2 Formato
- **Instagram**: 1080×1350 (4:5 verticale) = ideale.
- **TikTok modalità foto**: vuole 1080×1920 (9:16) a tutto schermo. Per ora si usa **un solo file 4:5 (1080×1350) su entrambe** (compromesso, su TikTok lascia bande/ritaglio ma si vede tutto). Ottimizzazione 9:16 dedicata = rimandata.

### 18.3 Struttura (7 schede)
1. **Cover**: hook grande su sfondo #0B1020 + USP "+ dove vederle in streaming" + pulsante **SCORRI →**. (Generate come PNG di brand, vedi 18.6.)
2-6. **Una scheda per titolo**: **screenshot DESKTOP** della scheda del sito (poster, voto, generi, trama, cast scorribile e soprattutto **DOVE VEDERLO** con le piattaforme). In alto **"N. NOME"** (es. "1. BREAKING BAD"), logo piccolo **in basso a destra**. Sulla chicca: etichetta **"💎 LA CHICCA"**.
7. **Chiusura**: screenshot del **motore di ricerca** del sito coi titoli del carosello già inseriti + **"Ti sono piaciute? Te ne trovo altre"** + cosaguardo.com. (Allineare testo allo screenshot: il motore = funzione "consiglia", non "trama/voto", quindi il claim è "te ne trovo altre".)

### 18.4 Come si costruisce in Canva
- Tela **Dimensioni personalizzate 1080×1350**.
- Screenshot desktop (NON mobile: mobile perde la sezione "dove vederlo"). Rimpicciolire la pagina del browser (Ctrl -) per far entrare tutta la colonna destra "DOVE VEDERLO" senza tagli.
- **Cornice (frame)** della **stessa proporzione** dello screenshot (orizzontale) → l'immagine entra **intera senza tagli ai lati**; se la cornice ha proporzione diversa, Canva taglia. Fare gli screenshot **tutti con lo stesso ritaglio** così entrano uguali.
- **Duplica pagina** (Ctrl+D) per le altre: cambi solo numero+nome + screenshot (trascinato sopra il vecchio dentro la cornice → "Sostituisci").
- **Font**: titoli **Anton** o **Bebas Neue** (impatto da poster), resto **Montserrat SemiBold**. Stesso font/posizioni su TUTTE le schede = effetto "collana".
- Export: Condividi → Scarica → PNG → "Seleziona pagine" → tutte, numerate nell'ordine del carosello.

### 18.5 Pubblicazione & musica (Metricool)
- Si programma da Metricool come i Reel (solo IG + TikTok). Caricare le immagini **nell'ordine** di scorrimento. Controllare nell'anteprima che sia un carosello vero e in ordine. Se il piano free lo spezza → pubblicare a mano (IG: nuovo post multi-immagine; TikTok: + → Carica → più foto).
- **Musica TikTok su Metricool**: dipende dall'account. **Personal** (quello dell'owner) = solo **musica random** (TikTok sceglie, non puoi indicare il brano), e solo su foto/caroselli. **Business** = Top 100 di tendenza selezionabile, MA ha limitazioni (TikTok restringe i suoni non-commerciali, può mutare audio) → non conviene passare a Business solo per la musica. **Per scegliere un audio di tendenda preciso → pubblicazione MANUALE via notifica**: Metricool manda notifica al telefono, si finisce in app TikTok con libreria musicale completa (un tap in più, ma controllo totale).
- L'opzione musica in Metricool compare nelle **opzioni specifiche di TikTok** (icona/linguetta TikTok sotto la caption) e **solo con auto-publish attivo**. Se non si vede: aprire opzioni TikTok, attivare auto-publish, eventualmente **ricollegare** l'account.
- Campo **"Title"** del post TikTok = **titolo/hook breve del post foto** (NON il titolo del carosello, NON influenza la musica). Mettere un gancio corto; dettagli+hashtag+"salva" nella caption.

### 18.6 Caption caroselli (ottimizzata salvataggi)
Struttura fissa: hook + elenco numerato dei 5 titoli (con 💎 sulla chicca) + "Su CosaGuardo trovi trama, voto e DOVE vederle in streaming 📲" + **"🔖 Salva il post"** + **"💬 [domanda]"** + hashtag. NIENTE link cliccabile nella caption.

### 18.7 Spinta organica (per ogni carosello)
1. Pubblica → 2. **Primo commento** tuo che innesca conversazione → 3. **Ricondividi in Storia** con freccia/sticker → 4. **Rispondi ai commenti** nelle prime ore. Eventuale **riuso come Reel** (schede che scorrono + audio di tendenza): Reel e carosello pescano da bacini diversi. La **costanza** batte la spinta del singolo post.

### 18.8 Caroselli prodotti (5) + asset
Cover PNG di brand generate in `/mnt/user-data/outputs/` (Python+cairosvg+Montserrat, 1080×1350): `carosello_cover_apertura.png` (template "5 SERIE CRIME"), `carosello_cover_1999.png`, `carosello_cover_finale_shock.png`, `carosello_cover_1994.png`, `carosello_cover_weekend.png`.
- **C01 — Serie crime da non perdere**: Breaking Bad, Gomorra, Peaky Blinders, Narcos, ZeroZeroZero💎
- **C02 — 1999, l'anno che ha cambiato il cinema**: Matrix, Fight Club, Il sesto senso, American Beauty, Essere John Malkovich💎
- **C03 — 5 film dal finale shock**: The Prestige, (Memento→sostituire, non in streaming) Shutter Island, I soliti sospetti, Gli altri, Oldboy💎
- **C04 — 1994, l'anno d'oro del cinema**: Pulp Fiction, Le ali della libertà, Forrest Gump, Il re leone, Léon💎
- **C05 — 5 serie da finire in un weekend (miniserie 1 stagione)**: Chernobyl, La regina degli scacchi, Mare of Easttown, When They See Us, The Night Of💎
- **Verifica disponibilità**: l'owner la vede da sé facendo lo screenshot (la scheda mostra "dove vederlo"); se un titolo è "buco" lo sostituisce.
- Idee prossimi caroselli: anno recente (2019), decennio, tema emotivo ("film che ti fanno piangere"), italiani, "serie sottovalutate".

---

## 19. REPORT METRICHE social — `cosaguardo_report_social.xlsx`

File Excel in `/mnt/user-data/outputs/cosaguardo_report_social.xlsx` per valutare cosa funziona (vs intuito). **4 fogli**:
- **Legenda**: istruzioni + legenda valori.
- **Contenuti**: 1 riga per contenuto, **già precompilato** con R01-R10 (Reel) e C01-C05 (caroselli), incluse note sugli esperimenti. Owner aggiunge la **Data** alla pubblicazione e i nuovi contenuti. Menù a tendina su Formato/Tipo/Apertura/Stile parlato.
- **Metriche**: 1 riga per contenuto × piattaforma (stesso ID di Contenuti). Colonne: Visualizz., Ritenz.3s%, Completam.%, Like, Commenti, Condivis., Salvataggi, Follow+, Fonte FYP%, Note + 4 colonne "(auto)" che recuperano Formato/Tipo/Apertura/Stile via VLOOKUP sull'ID (non toccarle).
- **Riepilogo**: si calcola da solo (AVERAGEIFS/COUNTIFS, IFERROR→"—"): ritenzione/completamento per **Apertura** (Classica vs Domanda 2 tempi, TikTok), per **Stile parlato**, salvataggi per **Formato** (IG), per **Tipo** (Film/Serie).

**Uso**: a ogni lettura dati (TikTok Studio / IG insights) aggiungere riga in Metriche con ID+piattaforma+numeri+**Data rilevazione** (i numeri crescono: confrontare a parità di giorni dalla pubblicazione). Costruito con openpyxl, ricalcolato con `scripts/recalc.py`, **0 errori formula**. Per modifiche future: rigenerare con build script analogo, sempre recalc + verifica errori.

**Domande a cui risponderà coi dati**: l'apertura a domanda tiene meglio della classica nei primi secondi? Il parlato lungo-con-trama batte il corto-secco? I caroselli meritano spazio fisso accanto ai Reel (salvataggi)? Quale genere/tipo rende di più? → da lì smettere di variare a intuito e puntare sui filoni forti.

---

## 20. Sessione 25/06/2026 — Filtri pagine piattaforma + chiusure JustWatch/TikTok

### 20.1 Filtri su pagine `/piattaforma/{slug}` (NUOVA feature) ⭐
- **Contesto**: dai dati Clarity, parecchi utenti entrano direttamente nelle sezioni piattaforma (Netflix, Prime, ecc.). Mancava lì la possibilità di filtrare come su `/scopri`.
- **Cosa fa**: aggiunge alle pagine piattaforma gli **stessi filtri di `/scopri`** — Genere, Mood, Periodo, Voto — **senza il selettore Piattaforma** (già fissa dall'URL). I filtri agiscono *dentro* quella piattaforma.
- **Riuso, zero duplicazione**: la route `platform_page` chiama `get_scopri_results()` passando il `provider_id` **diretto** (preso da `PLATFORM_SLUGS`), aggiungendo a quella funzione un nuovo parametro opzionale `provider_id` (retrocompatibile — `/scopri` non cambia).
  - ⚠️ **Perché provider_id diretto e non lo slug**: i due sistemi usavano slug diversi → `PLATFORM_SLUGS` (pagina piattaforma) ha `prime-video`/`disney-plus`/10 piattaforme con ID region-aware (Prime=119); `PLATFORM_MAP` (filtro `/scopri`) ha `prime`/`disney`/solo 6 con ID diversi (Prime=9). Passare lo slug lungo dentro `PLATFORM_MAP` non avrebbe trovato corrispondenza. Bypassato passando l'ID corretto direttamente.
- **Film e Serie sempre separati con filtri attivi**: niente "Tutti" misto (decisione owner). Se l'utente applica un filtro stando su "Tutti", la pagina si ripiega su Film e il tab "Tutti" sparisce finché ci sono filtri. Su Film/Serie la griglia è pulita e paginata (20/pagina).
- **Filtri collassabili** (richiesta owner, soprattutto per mobile): pannello **chiuso di default** dietro pulsante "**Filtri**" (stesso pattern di `/scopri`, funzione JS `togglePfFilters()`). Si apre da solo solo se c'è già un filtro attivo, con indicatore "Filtri •" azzurro.
- **🔒 Guardrail SEO (coerente con PR-1/PR-2/PR-3)**:
  - `/piattaforma/{slug}` PULITA (tipo=tutti, nessun filtro, pagina 1) → **INDEX**
  - qualsiasi stato filtrato, `?tipo=film/serie`, o `page>1` → **NOINDEX, follow** + **canonical** verso la versione pulita.
  - Così non si ricrea l'esplosione di URL indicizzati appena spenta.
- **Adattamento item**: `get_scopri_results` restituisce `vote_average`/`release_date`; la route li mappa in `rating`/`year` per riusare invariato il markup `pf-card` di `platform.html`.
- **File modificati**: `core/recommendation_api.py` (param `provider_id`), `app/main.py` (route `platform_page`), `app/templates/platform.html` (controlli + pannello collassabile + griglia filtrata + paginazione + CSS inline + JS toggle).
- **Validazione**: `ast.parse` OK, compile Jinja2 OK, CSS brace-balanced, test logico tipo/robots/adattamento item tutti verdi.

### 20.2 JustWatch — CHIUSO definitivamente (25/06)
Vedi dettaglio in §10-quater. In sintesi: sollecitato Maaz Ahmed, confermato che **non esiste affiliate solo-link**; solo Widget non personalizzabile o API ~€1.500/mese. Pista archiviata. Tutta la monetizzazione resta sulle affiliazioni dirette (Amazon attiva; Sky/Disney via Tradedoubler e TIMVision/NOW via Awin come target futuri).

### 20.3 GSC post-PR-3 — verifica al 25/06 (PR-3 FUNZIONA) ⭐
- Screenshot GSC (ultimo aggiornamento 12/06, ~11gg dopo PR-3 live 01/06):
  - **Indicizzate: 54.221** — non ancora scese (era ~50,5K il 29/05). Normale: è la coda lenta (Google deve ri-passare sulle già-indicizzate per toglierle).
  - **Non indicizzate: 163.683**, di cui **~113K "Escluse con noindex"** + **~33K "Escluse per reindirizzamento"**.
- **Lettura**: i 113K noindex **confermano che Google vede e applica il noindex** sulle pagine junk → PR-3 opera correttamente. Il calo di "Indicizzate" da 54K è solo questione di tempo (prossime 2-4 settimane).
- I 33K "reindirizzamento" sono benigni (http→https / canonical curati). Nessuna azione.
- **Standing**: continuare monitoraggio settimanale. Nessun rollback, nessuna nuova PR finché "Indicizzate" non inizia a scendere. Se tra ~2 settimane fosse ancora ferma a 54K, rivalutare.

### 20.4 TikTok — blocco e sblocco (25/06) + accorgimento operativo
- **Episodio**: account TikTok `@cosaguardoapp` (account personale vecchio mai usato, **rinominato per il brand**, che pubblicava via **Metricool** partendo da zero follower) → bloccato "per ripetute violazioni Linee Guida". Causa probabile: **cambio identità improvviso + pubblicazione automatizzata su account nuovo + CTA verso sito esterno** = pattern che gli anti-spam TikTok leggono come bot/promozione (stessa famiglia di trappole del sistema anti-frode Meta).
- **Esito**: ricorso ("Chiedi un'altra analisi") accettato, mail "Appeal Update" di ripristino ricevuta. Nota: c'è stato un disallineamento temporaneo (mail di ripristino vs app ancora bloccata per ore) — gestito via ripresentazione ricorso con screenshot della mail di ripristino come prova.
- **🔁 Accorgimento da rispettare quando si rientra (e in generale)**:
  - Prime pubblicazioni **a mano dall'app** per qualche giorno, non da Metricool.
  - **Warm-up** dell'account prima di postare: guardare video, like, follow (comportamento "umano").
  - Reintrodurre Metricool **gradualmente** quando l'account ha un po' di storia.
  - **MAI** creare un secondo account brand mentre uno è bloccato (= elusione ban → chiude anche il nuovo).
- Instagram non coinvolto, prosegue normale.

---

## 21. Sessione 25/06/2026 (parte 2) — Analytics: conversioni, opt-out, filtro interno + chip toggle

### 21.1 Diagnosi GA4 (ultimi 30gg, 27 mag–25 giu) ⭐
Esportati 6 PDF GA4 e analizzati. Quadro:
- **436 utenti attivi, 427 nuovi, 49 di ritorno.** Volume ancora basso (SEO in fase noindex, traffico quasi tutto paid).
- **Tempo medio coinvolgimento: 1m 52s** → OTTIMO (benchmark ~44s). Chi atterra **si coinvolge**: la qualità del traffico NON è il problema.
- **Qualità per canale molto diversa**: Paid Social (IG, 53%) = 1m09s, 0,87 sess/utente, 5,2% → superficiale ma normale per traffico freddo. **Direct (25%) = 4m01s, 1,51 sess/utente, 15,4%** → utenti "oro" che tornano. Organic Search ancora piccolo (58 utenti).
- **82% mobile, 93% Italia** → targeting perfetto, mobile dominante.
- **⚠️ PROBLEMA MACRO: conversioni = 0.** Non per scarso traffico ma perché **non c'era tracciamento conversioni** (vedi 21.2). Volavamo alla cieca sulla metrica-ricavo.
- Igiene dati: pagine admin/profilo tra le top viste (traffico interno non escluso); demografia vuota (Signals off).
- Benchmark di riferimento salvati: engagement rate mediano ~56% (range reale 40-90%, paid social più basso); tempo medio ~44s; GA4 ha un **benchmarking integrato** (Home → clic su nome metrica → Benchmarking).

### 21.2 Tracciamento conversioni — implementato ⭐
**Scoperta chiave**: l'helper `cgTrack()` in `base.html` (GA4 + Meta Pixel, gated da consenso cookie) **esisteva ma non veniva mai chiamato** = codice morto. L'unico evento custom attivo era `trailer_play`. I **link provider/affiliato erano `<a>` senza alcun tracking** → il clic-ricavo era invisibile. Ecco perché "lead = 0".

Eventi cablati (chiamano `cgTrack`, rispettano consenso, inviano anche al Pixel):
- **`select_provider`** ⭐ — clic su link provider/affiliato. Via **event delegation** in `base.html` (listener capture) + `data-cg-track/-provider/-title/-group` sui link in `detail.html` (provider-item, streaming+noleggio) e `dove_vedere.html` (dv-provider, streaming+noleggio). Manda provider, titolo, gruppo.
- **`sign_up`** — usa il flag `registered=1` già presente sul redirect `/profilo`. base.html legge il flag, spara l'evento una volta, pulisce l'URL.
- **`login`** — aggiunto flag `logged_in=1` al redirect login in `main.py`; stessa logica di firing+cleanup.
- **`pwa_install`** — DEFERITO: il flusso install (`install_prompt_available`) sta in un JS esterno/SW non in mano; fast-follow.
- **File**: `app/templates/base.html`, `app/templates/detail.html`, `app/templates/dove_vedere.html`, `app/main.py`. Validati (AST, Jinja2, node --check).

### 21.3 GA4 — Eventi chiave (config UI)
- Gli eventi `select_provider`/`sign_up`/`login` vengono **dal codice** → NON vanno creati con "Crea evento" (quello li genera da trigger tipo page_view = sbagliato).
- Si marcano come conversione mettendo la **stella** accanto al nome in **Amministrazione → Eventi → (scheda "Eventi recenti")**. In questa versione GA4 NON c'è una voce di menù "Eventi chiave" separata: è dentro "Eventi".
- L'evento compare solo dopo essere partito almeno una volta (+ ritardo elaborazione). `sign_up` già visto attivo. `select_provider`/`login` in attesa di prima occorrenza al momento dello screenshot.
- Dati NON retroattivi: partono da ora.

### 21.4 Esclusione traffico interno — DOPPIA protezione ✅
- **A) Opt-out via codice (cross-device)** — in `base.html`: visitando `cosaguardo.com/?optout=1` si setta `localStorage.cg_optout='1'` e GA4/Clarity/cgTrack non partono più su quel device (a prescindere dall'IP). `?optout=0` riattiva. Guard messo **dentro** `loadGA4`/`loadMetaPixel`/`loadClarity`/`cgTrack`, così copre ogni call site (incluso il pulsante Accetta). Va rifatto su nuovo browser/incognito/cancellazione dati. **Da fare su PC + iPhone dopo deploy.**
- **B) Filtro IP in GA4 (rete di casa)** — IP definito in *Stream di dati → Configura impostazioni tag → Mostra tutto → Definisci traffico interno* (regola "Casa", `traffic_type=internal`, IP uguale a casa); poi filtro **"Internal Traffic"** in *Impostazioni dati → Filtri dati* portato da Test → **Attivo**. Non retroattivo; da aggiornare se cambia l'IP.
- Nota architetturale GA4: l'IP si mette nello *Stream*, non nel filtro (il filtro esclude solo ciò che è già marcato `internal`).

### 21.5 Google Signals (demografia) — opzionale, con caveat
- Percorso: *Amministrazione → Raccolta e modifica dei dati → Raccolta dei dati → Google Signals → attiva*.
- Sblocca età/genere/interessi, MA: non retroattivo, 24-48h per i dati, e **col volume attuale (~436/mese) i dati restano scarsi/"unknown"** finché il traffico non cresce.
- ⚠️ Compliance EU: attivarlo aggiunge **cookie pubblicitari** (DoubleClick); la `privacy.html` cita solo i cookie analitici → andrebbe aggiunta una riga. La modifica Google del 15/06/2026 ha solo scollegato Signals dal flusso verso Google Ads; per la demografia dentro GA4 resta il modo per averla.
- Stato: lasciato alla decisione di Marco (priorità più bassa vista la scarsità di dati a basso volume).

### 21.6 Chip filtro deselezionabili (toggle-off) — scopri + piattaforma
Su richiesta: cliccando un chip filtro già attivo ora lo si **deseleziona** lasciando gli altri (prima costringeva ad "Azzera filtri"). I radio nativi non si deselezionano → gestito via `data-was-checked` + handler `onclick` (`pfChipClick`/`scopriChipClick`) che inizializza lo stato sui chip resi `checked` dal server. Applicato a `platform.html` e `scopri.html`.

### 21.7 TODO analytics aperti
- [ ] Deploy `base.html` (opt-out) + fare `?optout=1` su PC e iPhone.
- [ ] Mettere la stella su `select_provider` e `login` quando compaiono in "Eventi recenti".
- [ ] (opz.) Attivare Google Signals + aggiungere riga cookie pubblicitari alla privacy.
- [ ] Fast-follow: evento `pwa_install`.
- [ ] Rileggere il report "Generazione lead" tra 2-4 settimane con i dati di conversione veri (capire se le IG ads convertono o solo portano traffico).

### 21.8 Eventi PWA install + fix base.js (sessione 25/06 parte 3)
- **Scoperta**: il flusso PWA in `app/static/js/base.js` era già strumentato con una funzione locale `track()` → eventi già esistenti: `install_prompt_available` (eligibilità, i 512 visti), `install_banner_shown`, `install_banner_clicked` (clic tasto = intenzione), `install_prompt_outcome` (accetta/annulla nel popup nativo), **`app_installed`** (su `appinstalled` = installazione REALE completata), `install_fallback_shown`. Nessun codice nuovo necessario.
- **Eventi chiave GA4 marcati**: `app_installed` (install reale) + `install_banner_clicked` (clic) → per leggere il tasso clic→install.
- ⚠️ **Limite iOS**: Safari non supporta `beforeinstallprompt`/`appinstalled` → `app_installed` cattura solo **Android/desktop**. Da iPhone ("Aggiungi a Home") l'install NON è rilevabile via JS. Con ~82% mobile e forte quota iOS, le install reali da iPhone restano invisibili: nel rapporto clic/install segmentare per OS ed escludere iOS per un dato pulito.
- **2 bug sistemati in `base.js`**:
  1. **Doppione `sign_up`**: `base.js` aveva già `registered=1 → cgTrack('sign_up')`, identico a quello che era stato messo in `base.html`. Consolidato tutto in `base.js` (sign_up **e** login lì, con cleanup URL unico) e rimosso il doppione da `base.html`. `base.html` ora gestisce solo il listener `select_provider`.
  2. **`track()` ignorava l'opt-out**: gli eventi PWA partivano anche con `?optout=1`. Aggiunto guard `if (localStorage.getItem('cg_optout')==='1') return;` in `track()`. Ora l'opt-out interno copre anche gli eventi PWA.
- **Architettura conversioni (stato finale)**: `base.html` → opt-out helper + guard in loadGA4/Pixel/Clarity/cgTrack + listener `select_provider`. `base.js` → sign_up/login post-redirect (via cgTrack) + tutti gli eventi PWA (via track()). I link provider in `detail.html`/`dove_vedere.html` hanno i `data-cg-*`.
- **File**: `app/static/js/base.js`, `app/templates/base.html`.

### 21.9 BACKLOG — Edge bypass per utenti loggati + pagina "estero" (feature futura)
**Contesto**: il middleware `block_direct_origin` (main.py, ~riga 437) blocca con 403 le pagine profonde `/film//serie//persona/` quando il traffico è (a) da IP datacenter noti o (b) da paese non in `EDGE_ALLOWED_COUNTRIES` (default `IT,SM,VA`, override via env). Protegge da scraper/botnet residenziali estere e da Render bandwidth overage. Le pagine sono noindex → non costa SEO; `/dove-vedere` (SEO) NON è toccata. Emerso il 27/06: Marco in vacanza in Grecia (cf-ipcountry=GR) prendeva 403 sulle pagine profonde — comportamento corretto, non bug.

**Valutazione**: il collaterale su umani veri è BASSO (pubblico 93% IT; traffico estero sulle pagine profonde ~100% scraper). Nessuna emergenza. Fix immediato personale disponibile: env `EDGE_ALLOWED_COUNTRIES=IT,SM,VA,GR` su Render (temporaneo), o VPN.

**Feature futura (2 parti), da fare QUANDO le registrazioni iniziano a contare** (ora valore basso: pochi loggati, ~49 di ritorno):
1. **Edge bypass per utenti loggati** — nel middleware `block_direct_origin`, se esiste una **sessione loggata valida** (cookie di sessione già firmato con SECRET_KEY, non falsificabile), lasciar passare a prescindere da paese/datacenter. Così expat/viaggiatori italiani con account navigano ovunque. ⚠️ Il coupling DEVE essere sull'**utente loggato**, NON su "N pagine di navigazione anonima" (aggirabile: gli scraper fanno le stesse N pagine). Se un account viene abusato per scraping → si revoca. Implementazione ~15 min, nessun token custom (riusa la sessione firmata esistente).
2. **Pagina "estero" ad-hoc** — invece del 403 nudo su paese non consentito, servire una pagina che spiega "contenuto disponibile dall'Italia; **accedi** per navigare da qui" con CTA login/registrazione. Diventa un **punto di conversione** verso il login (sinergico con la parte 1: chi si logga poi passa). Da NON applicare al blocco datacenter (quelli sono bot, 403 secco basta) — solo al ramo geografico.

**Priorità**: bassa finché la base registrati non cresce. Personale: VPN nel frattempo.

### 21.10 GSC (30/06) — PR-3 conferma il calo di "Indicizzate" ✅
- Ultimo aggiornamento GSC 30/06: **Indicizzate = 42,1K** (era 54,2K il 12/06 → **-12K in ~18gg**). "Non indicizzate" = 179K (era 164K). Il grafico mostra la fascia verde che si assottiglia.
- **PR-3 sta digerendo come previsto.** Nessuna azione, monitoraggio settimanale. Quando "Indicizzate" si stabilizza (~5-10K stimati, pagine di reale valore) si potrà valutare la PR-3-next (noindex su `/film//serie/{id}` non curati).
- Traiettoria storica per riferimento: ~50,5K (29/05) → 54,2K (12/06, picco) → 42,1K (30/06, in calo).

### 21.11 ⭐ PRIORITÀ ALTA — Outreach influencer/creator per ads diretta
**Obiettivo**: contattare creator per reel/post che mandino traffico al sito, misurare quante persone portano davvero (via link tracciati). Fase esplorativa iniziata (Marco in vacanza) mandando DM a piccoli creator per capire i costi.
- **Messaggi pronti**: versione DM breve + versione email strutturata (nel thread chat 30/06). Riempire sempre nome + **aggancio specifico** al profilo (altrimenti = spam ignorato).
- **Strategia a 2 secchi** (per livello di intento, non per genere):
  1. **Nicchia cinema/serie** = alta intenzione, converte meglio, punto di partenza sicuro.
  2. **Lifestyle/femminile grande pubblico** = reach economica DA TESTARE come esperimento. CosaGuardo è prodotto di massa (streaming = mass-appeal), quindi NON è pubblico sbagliato; ma "costa poco per follower" è spesso trappola (engagement morto/follower gonfiati). Va bene solo se engagement reale + contenuto nativo + misurato.
- **Metrica di valutazione**: NON follower né prezzo, ma **costo per click reale al sito** (UTM) e poi costo per conversione (`select_provider`/`sign_up`). Baseline di confronto: **IG ads ~15 cent/visita**. Esempio: reel 50€ → 500 click = 10c/click (meglio delle ads, buono); 80 click = 62c/click (scarta).
- **Prima di dire sì a un creator**: chiedere viste medie storie/reel (non follower) + screenshot metriche recenti; proporre prima una **storia con link** (test economico) prima del reel; usare sempre **link UTM** (es. `?utm_source=nomepagina&utm_medium=influencer`). TODO: preparare i link UTM al primo "sì".
- **Nota**: template micro/nano-influencer (DM + email) erano già menzionati come in preparazione nei materiali storici; ora formalizzati e in outreach attivo.

### 21.12 Setup conversioni COMPLETATO + primo confronto settimanale (04/07)
**Setup conversioni chiuso** ✅: `select_provider` verificato in Tempo reale (parte cliccando un provider da device non escluso) e **stellato come evento chiave**. Ora tutti gli eventi chiave attivi: `select_provider`, `sign_up`, `login`, `app_installed`, `install_banner_clicked` (+ `trailer_play`, `episode_click` preesistenti). Tracking conversioni end-to-end operativo. Nota: `select_provider` scatta solo dai clic provider su schede `/film//serie/` → se il conteggio è basso è perché pochi utenti arrivano ancora fin lì (home/scopri/recommend sono le pagine più viste), non perché è rotto.

**Primo confronto settimanale** (27/6–3/7 vs 20–26/6, via selettore date → Confronta → Periodo precedente):
- **⚠️ ASTERISCO METODOLOGICO**: l'esclusione traffico interno (filtro IP + opt-out) è stata attivata ~25-27/6, quindi cade A METÀ del confronto. La settimana attuale ha Marco escluso, la precedente no → il "-15% utenti" e il "+15% durata" sono **in parte artefatti** (meno visite sue = volume più basso, niente sue navigazioni admin veloci = engagement più alto). Il confronto 30gg vs 30gg pulito arriva **a fine luglio** (esclusione attiva su entrambe le finestre).
- **Segnali VERI positivi** (indipendenti dal traffico interno): Paid Social durata 1m45s vs 1m13s = **+44%** (le IG ads portano traffico più coinvolto); uso funzioni core in forte crescita — `/scopri` durata +230% e views +126%, `/la-mia-raccolta` +170%, `/profilo` +116% (le persone sfogliano/filtrano/salvano, coerente col lavoro filtri); Direct nuovi utenti +15,6%.
- **Cali spiegati**: utenti -15% (in gran parte esclusione interna) + Paid Social nuovi -31,5% → **spiegato: le IG ads erano ferme ~2 giorni** (budget a fine data, poi riattivate) da cui anche 14 vs 20 registrati. Nessun problema reale.
- **Eventi chiave 61 vs 22 (+177%)**: soprattutto effetto tracking appena acceso (login/select_provider prima invisibili), non +177% reale. Il punto è che ORA si misura.
- Per conteggio registrati REALE usare `/admin/utenti` (DB), non GA4 (sign_up non tracciato nella settimana precedente).
- Geo/pubblico stabile: ~93% Italia, ~82% mobile, italiano dominante.

**Prossimo appuntamento analytics**: confronto 30gg vs 30gg a fine luglio (lettura pulita). Fino ad allora: lasciar accumulare dati.

---

## 22. Sessione ~08/07/2026 — Fix UX/dati, affiliazione NOW/Awin, outreach influencer

### 22.1 Fix titoli non-latini su hub /dove-vedere ✅
- **Problema**: la pagina hub `/dove-vedere` mostrava titoli in cirillico/tamil/telugu/hindi (es. "Твоё сердце", "कॉकटेल"). L'hub NON fa query live: pesca da tabella DB `seo_titles` via `list_seo_titles()`, e quei titoli erano già dentro (inseriti prima del filtro leggibilità).
- **Fix in `core/seo_pages.py`** (3 livelli, riusa `_is_latin_readable`/`pick_readable_title` già esistenti in recommendation_api.py):
  1. **Pulizia una-tantum**: in `_ensure_db`, guard modulo `_UNREADABLE_CLEANUP_DONE`, al primo accesso dopo deploy cancella dal DB le righe con titolo non-latino (log: "seo_titles: rimossi N titoli illeggibili").
  2. **Prevenzione in scrittura**: `populate_seo_titles_db` (evergreen) e `populate_new_releases` (uscite, 2 punti) usano `pick_readable_title` → item illeggibile = skippato, non entra più.
- Validato con ast.parse + test regex sui titoli reali. Il conteggio hub cala un po' (voluto). File: `core/seo_pages.py`.

### 22.2 Fix barra di caricamento su card /recommend ✅
- **Problema**: cliccando un titolo nei risultati `/recommend`, la scheda impiega ~2s a caricare ma NON compariva la top progress bar → utente pensa a dead-click e ri-clicca.
- **Causa**: la barra (`#cg-nav-progress` in base.html + logica in base.js) esisteva e funziona su tutti i link `<a>`, ma le card risultati sono `<article>` che navigano via `window.location.href` (nav programmatica) → invisibile al click-listener.
- **Fix**: esposta `window.cgStartNavProgress` in `base.js`; chiamata in `results.html` prima di `window.location.href`. File: `app/static/js/base.js`, `app/templates/results.html`. (base.html invariato.)
- Bonus: per future nav via JS basta chiamare `window.cgStartNavProgress()` prima.

### 22.3 Affiliazione NOW / Awin — ATTIVATA (solo env, zero codice) ⭐
- NOW ora iscritto su Awin → tracking attivabile. **Il codice era GIÀ predisposto** in `recommendation_api.py` (`_build_affiliate_link` + dict `awin_programs` con chiavi "NOW"/"NOW TV" → env `AWIN_MID_NOW`; provider id 39 → nome "NOW").
- **Attivazione = 2 env var su Render** (nessun deploy codice):
  - `AFFILIATE_AWIN_ID = 2879325` (publisher ID Cosaguardo)
  - `AWIN_MID_NOW = 9535` (merchant ID NOW IT)
- Risultato: bottone NOW → `https://www.awin1.com/cread.php?awinmid=9535&awinaffid=2879325&ued=https%3A%2F%2Fwww.nowtv.it%2F`. Destinazione = homepage nowtv.it. Il tracking `select_provider` (già cablato) misura anche questi clic.
- **TIMVision già predisposto**: quando approvato, aggiungere env `AWIN_MID_TIMVISION` e si accende (stesso meccanismo). Disney+/Paramount+/Netflix NON hanno programma Awin IT → restano su fallback JustWatch.
- Meccanismo coerente con Amazon (`AFFILIATE_AMAZON=cosaguardo-21`) e Apple (`AFFILIATE_APPLE`, Partnerize).

### 22.4 Outreach influencer — strategia + primi test ⭐
- **Impostazione definitiva**: in questa fase SOLO **storie con link** (unico formato IG con link cliccabile → traffico misurabile; reel/post non linkano). Metrica di giudizio: **costo per click reale su GA4** (non tap dichiarati, non follower) vs baseline **IG ads ~15 cent/visita**. Ogni creator = link UTM dedicato (`utm_source=<creator>&utm_medium=influencer&utm_campaign=test_storie_lug25`), approvazione storia prima della pubblicazione, richiesta screenshot statistiche a fine test per calibrare rapporto **tap→visita GA4** (dato che serve per tutti i creator futuri).
- **Test avviati** (tutti a performance, budget da test):
  - **cinesocialclub**: 2 storie 20€ + 5€ bonus a 400 click. (Il profilo ha 8,5M views ma tutto reel non-linkabili; storie solo 2-3k viste.)
  - **ludovicaledger** (~100k): 25€ + 10€ bonus a 200 click. Profilo motivato/autentico (voleva visitare il sito prima). Nel brief spinto il differenziale "più aggiungi titoli, più ti consiglia altro".
  - **cinefilomalefico**: 2 storie 10€/cad + 5€ bonus a target click.
- **Scartati/rinviati**: Enrica Ilari (200k, verticale libri+cinema, ottimo profilo MA storie a 500€ e collab analoga fece solo 176 click → ~2,8€/click, ~19× le ads; rinviata a fase awareness futura). JustWatch resta chiuso.
- **Principio appreso**: costo-per-click CRESCE coi follower (i grandi vendono reach/awareness non-linkabile). Sweet spot per traffico misurabile = micro/nano-creator 20-30€. I grandi si tengono per quando l'obiettivo sarà notorietà, non traffico.
- **Lettura risultati**: GA4 → Acquisizione traffico → dimensione "Sorgente/mezzo sessione" → filtra per nome creator. Guardare volume, durata, e `select_provider`/`sign_up` da quella sorgente (qualità, non solo click).

### 22.5 TikTok — follow-up inviato (oltre i 7 giorni)
- Passati i 7+ giorni indicati da Elina senza ripristino → inviato sollecito cortese-ma-fermo sullo stesso thread (feedback_eu@tiktok.com), ribadendo che il ripristino è già approvato (mail "Appeal Update") e chiedendo aggiornamento concreto + tempi. Motivo ban confermato da TikTok: "Comportamenti di interazione anomali" (= automazione Metricool su account nuovo, come sospettato). Prossimo passo se vago: chiedere escalation a team specializzato. Sempre: nessun account nuovo.

---

## 23. Sessione ~10/07/2026 — Sistema notifiche PUSH (app), Sky Go, admin visite, risultati influencer

### 23.1 ⭐ SISTEMA NOTIFICHE WEB PUSH (Fasi 1-3) — COMPLETO end-to-end
Costruito il sistema di notifiche automatiche, base per l'app Android. Flusso: utente loggato apre un titolo non in streaming → "Avvisami" → attiva push → quando il titolo arriva in streaming, riceve un push che apre la scheda. **Zero gestione manuale.**

**Architettura (3 parti):**
- **Fase 1 — Trasporto push**: nuovo modulo **`core/push.py`** (autonomo, come seo_pages.py): tabella `push_subscriptions`, `save_subscription()`, `get_subscriptions_for_user()`, `send_push_to_user()` (usa `pywebpush`). Handler `push`+`notificationclick` in `app/static/sw.js`. Funzione globale **`window.cgEnablePush()`** in `base.js` (chiede permesso, sottoscrive, invia a server; rispetta l'opt-out). Route `GET /api/push/public-key` e `POST /api/push/subscribe` in main.py. `init_push_db()` allo startup. Bumpati `base.js?v=3` e `CACHE_VERSION` del SW.
- **Fase 2 — Job rilevamento arrivi**: nuovo **`core/alerts_job.py`** → `check_and_notify_alerts()`: legge alert pending (`list_pending_streaming_alerts` in db.py), raggruppa per titolo, controlla `get_watch_providers(title, ct)` (flatrate non vuoto = arrivato), invia push + marca notificato (`mark_streaming_alert_notified`). Scheduler giornaliero **04:00 Europe/Rome** in main.py (`_start_alerts_scheduler`, thread; disattiva con env `DISABLE_ALERTS_JOB=1`). Route test manuale **`GET /admin/run-alerts`** (solo admin).
- **Fase 3 — Attivazione UI**: in `detail.html`, il form "Avvisami" ora: se **loggato** → attiva `cgEnablePush()` (msg "🔔 Ti mandiamo una notifica…"); se **anonimo** → salva email + mostra incentivo "**Accedi o registrati** per la notifica 🔔" (link a /register). Onesto (l'email non riceve ancora nulla) + spinge le registrazioni.

**ENV VAPID su Render** (già impostati):
- `VAPID_PUBLIC_KEY` = `BHMiQG3jUL8X4kGeXDtdMmng9B7wKa6lZLLkQc2q9YALC8gpbWZWnuwBi7NoxAuZwM4WAViSa2Zmr3K3pMFsXlU`
- `VAPID_PRIVATE_KEY` = `v0uTau_Z9LipFfVVnyY0UjlwhXAgFAzdOrB3OlhxFr0` (SEGRETA)
- `VAPID_SUBJECT` = `mailto:privacy@cosaguardo.com`
- **`requirements.txt`**: aggiunto `pywebpush`.

**Come testare**: `cosaguardo.com/api/push/public-key` → deve dare `{"key":...,"enabled":true}`. Console (loggato, non-optout): `await cgEnablePush()` → `{ok:true}`. Job: `/admin/run-alerts` (da admin) → JSON `{pending,arrived,notified}`. ⚠️ L'opt-out (`?optout=1`) blocca anche cgEnablePush (per questo Marco vedeva `reason:'optout'` → risolto con `?optout=0`).

**PENDING sistema notifiche**:
- **Invio EMAIL** per gli alert anonimi (ora ricevono solo l'incentivo a registrarsi, nessuna email) → richiede servizio tipo Resend/SES. Fase futura.
- **Contenitore Android** vero (PWABuilder → .aab → Google Play 25$). La PWA + push sono pronti; il push è anche la "funzione nativa" che servirà per la revisione iOS futura.

### 23.2 App CG — piano store (deciso)
- **Solo Android per ora** (iOS dopo). Android: 25$ una-tantum, no Mac, via **PWABuilder** (genera .aab dalla PWA, carica il sito live → aggiornamenti automatici). iOS: 99$/anno + Mac + revisione ostica (serve funzione nativa = le push, già pronte).
- Notifiche push automatiche = valore nativo + incentivo registrazione. Contenitore carica sito live (no ricompilazione per ogni modifica).

### 23.3 Sky Go → affiliato NOW/Awin
Aggiunto "Sky Go" alla mappa `awin_programs` in `recommendation_api.py`: i click su Sky Go (provider TMDb id 29) vanno all'affiliato NOW (stesso merchant `AWIN_MID_NOW`, dest. nowtv.it). Razionale: Sky Go non è acquistabile a sé, per chi non è cliente Sky il modo di guardare è NOW. Riusa le env NOW già impostate.

### 23.4 Admin /utenti — colonne visite (user_activity)
Aggiunta tabella `user_activity` (1 riga/utente/giorno) + `last_seen` + `record_user_activity()` in db.py; hook in `_patched_TemplateResponse` (main.py) che registra 1x/giorno/sessione (solo render pagine). Colonne in admin_utenti.html: **Ultimo accesso · Giorni attivi · Attivo 30gg**. NON retroattivo. "Ultimo accesso" = segnale-chiave retention.

### 23.5 ⭐ Risultati primi test influencer (dati GA4 reali)
Letti in GA4 (Acquisizione traffico → Sorgente/mezzo sessione, campagna `test_storie_lug25`):
- **ludovicaledger** (25€): **41 sessioni**, durata **1m05s**, coinvolgimento **70,7%** → costo/visita **61 cent**. Eventi chiave: **2 install_banner_clicked + 1 sign_up**. Il sign_up (2,4% conv su traffico freddo) è ottimo → traffico DI QUALITÀ che converte.
- **cinesocialclub** (10€, 1ª storia): **23 sessioni**, durata **52s**, coinvolgimento 47,8% → costo/visita **43 cent**. **0 eventi chiave**. Traffico superficiale.
- **Baseline IG ads**: ~15 cent/visita. **Entrambi ~3-4× più cari a visita.**
- **VERDETTO**: sul costo-per-visita gli influencer NON battono le ads. MA la metrica giusta è **costo-per-REGISTRATO/conversione**: lì Ludovica (1 sign_up) ha dato valore, cinesocialclub no. **cinesocialclub → non ripetere. Ludovica → profilo giusto (autentica, in target), da riprovare.** cinesocialclub deve ancora fare la **2ª storia** → test in sospeso fino a quella (segnarsi baseline 23 sessioni/0 eventi per isolare la 2ª). Enrica Ilari rinviata (troppo cara). unchained_cinema + cinefilomalefico test avviati (2 storie 20€+5€).
- **Principio confermato**: costo/click cresce coi follower; il costo-per-visita da solo inganna, conta cosa fanno DOPO (conversioni).

### 23.6 Contenuti social — tracking Excel aggiornato + apprendimenti
- **`cosaguardo_report_social.xlsx`** popolato: foglio Metriche con 28 rilevazioni (IG+YouTube per R01-R10 coppie, C01-C05 caroselli, +TL01/C06/Q01/Q02), colonna Categoria (mainstream/nicchia), nuovo foglio **Analisi**. 0 errori formula.
- **Numeri chiave**: media viste **YouTube 473 vs IG 260**. Coppie **mainstream YT 851 vs nicchia YT 100 (8,5×!)**. Su IG il divario si schiaccia (316 vs 250, IG è piatto).
- **STRATEGIA CONTENUTI**: raddoppiare su **YouTube Shorts** (soffitto alto, breakout fino 2.300; titoli ricercabili + link cliccabile in descrizione), **coppie universali/mainstream** (le nicchie solo per variare), **lavorare sull'engagement** (domanda + primo commento tuo) per sbloccare la reach IG. Formato **tier list** = macchina da commenti (template brandizzati creati: finali, "quando smettere"). Video "coppie" format: hook "se ti sono piaciuti X e Y" + 5 titoli con 1-2 chicche (verificate in streaming IT) + CTA "la tua coppia? …su cosaguardo.com".
- **Copertine/template generati** (in /mnt/user-data/outputs/caroselli/): stile "testuale" (titolo grande + card laterali) e "coppia" (2 slot poster + "5 FILM DA VEDERE"); tier list 1080×1920 con celle poster vuote. Env generazione: `pip install cairosvg pillow --break-system-packages; apt-get install -y fonts-montserrat`.

### 23.7 Bit.ly per link influencer
I link UTM lunghi vengono rifiutati dallo sticker link IG → accorciati con **bit.ly** (mantiene gli UTM, redirect trasparente). Feature futura possibile: redirect on-brand `cosaguardo.com/go/<creator>` (route redirect) invece di bit.ly.

---

## 24. Sessione ~14–17/07/2026 — APP ANDROID PUBBLICATA (Play, test chiuso), APP iOS in preparazione, fix UX, apprendimenti social, nuovo TikTok

### 24.1 APP ANDROID — pubblicata in TEST CHIUSO su Google Play ✅
Via PWABuilder (TWA che carica il sito live → aggiornamenti automatici). **Package ID PERMANENTE: `com.cosaguardo.app`**.
- **Account Play Console**: ID `7306521630854481240`, App ID `4974417119108202241`. Tipo Personale, dev name `CosaGuardo`, login `mfantini84@gmail.com`, email pubblica `info@cosaguardo.com`. Identità + telefono verificati.
- **Scheda Store** completa: titolo `CosaGuardo`, descr. breve 74char, descr. lunga, categoria **Intrattenimento**, tag (streaming/film/serie tv/guida tv/intrattenimento — NO "biglietti cinema"). Contatti: email + sito, no telefono. Video store saltato (i reel IG sono 9:16).
- **Questionario/dichiarazioni** (risposte chiave): monetizzazione=Sì (affiliati); no bambini; dettagli accesso=Sì (account test sul sito + nota EN); **annunci=No** (affiliati non contano → CAMBIARE se attivi AdSense); classificazione contenuti tutta **No** tranne "Contenuto online=Sì" → **PEGI 3/Per tutti ovunque**; pubblico 13+; no posizione (IP non conta); Sicurezza dati: HTTPS sì, account nome+password, criptati in transito=Sì, dati dichiarati = email + ID utente (non condivisi) + Interazioni app (GA4+Clarity, CONDIVISI) + Cronologia ricerche + ID dispositivo (GA4, CONDIVISO); no finanziarie/salute/governative; **ID pubblicità=No** (TWA, no SDK pubblicitari nativi; GA4 web NON usa l'AAID).
- **URL eliminazione account**: `cosaguardo.com/elimina-account`.
- **Materiali store** (cartella `/mnt/user-data/outputs/store/`): `icon-512.png`, `feature_graphic.png` (1024×500), `store_1..5.png` (1080×1920).
- **Release test chiuso (Alpha) PUBBLICATA E LIVE (14/07)**: stato "Pubblicata", app bundle 1.0.0.0, 659 kB, **solo Italia**. Link opt-in: `https://play.google.com/apps/testing/com.cosaguardo.app`. Testata su Android: **si apre a schermo intero + push OK**.
- **⚠️ REGOLA account nuovo**: serve **test chiuso con ≥12 tester per ≥14 giorni** → poi si può **richiedere la Produzione**. I tester devono dare la loro **email Google reale** (match con l'account del loro telefono); non basta metterne una qualsiasi. Meglio 15-18 per margine.
- **Verifica sviluppatori Android**: registrazione automatica già fatta (nessuna azione; riguarda solo chi distribuisce fuori Play).
- **PENDING**: completare 12 tester → attendere 14gg → richiedere Produzione → poi link Play Store su sito+social.

### 24.2 assetlinks — impronta Google Play aggiunta (verifica TWA)
Route `GET /.well-known/assetlinks.json` in `main.py` con `_ASSETLINKS_FINGERPRINTS` = **DUE impronte SHA-256**:
- Chiave di firma dell'app di **GOOGLE** (Play App Signing, firma le release consegnate): `17:35:4B:A6:C3:4A:85:77:57:03:5C:15:67:63:85:B7:2E:36:24:62:5B:F6:4E:AE:18:BA:DD:54:6D:48:56:EE`
- Chiave di caricamento PWABuilder: `84:BE:C5:9C:3C:C2:0D:1A:A3:50:A2:78:74:25:C9:92:1D:C0:92:EF:26:D4:6F:A2:20:CB:B9:82:82:C8:81:85`

Impronta Google trovata (nuova UI 2026): **Protetto con Play → Protezione del Play Store → Vai a Play app signing** → Certificato della chiave di firma dell'app → SHA-256. **Verificato online**: l'endpoint restituisce entrambe → app a schermo intero senza barra browser.

### 24.3 APP iOS — in preparazione (in PAUSA tecnica)
Via PWABuilder (progetto iOS/TWA). Rischio revisione Apple **4.2** (siti impacchettati) → le **push** sono il valore nativo anti-rifiuto.
- **Account Apple Developer PAGATO** (99$/anno, ordine `W1815035906`, 14/07) ma **IN ELABORAZIONE**. Attendere email *"Welcome to the Apple Developer Program"* + Membership attiva su `developer.apple.com/account`. La ricevuta di pagamento NON è l'attivazione.
- **Mac fisso in ufficio** (Sequoia 15.7.7). **Xcode 26.3 installato** (scaricato `Universal.xip` da `developer.apple.com/download/all`, NON dal Mac App Store che pretendeva macOS 26.2/Tahoe). Compatibilità: Sequoia 15 → Xcode 16+; Xcode 26 → richiede 15.6+.
- Progetto iOS aperto in Xcode (`.xcworkspace`): contiene AppDelegate/PushNotifications/WebView/GoogleService-Info (push predisposte). Target: **"Any iOS Device (arm64)"**. Signing: "Automatically manage" ON, Team **Marco Fantini**, **Bundle ID `com.cosaguardo`** (senza .app, va bene, diverso da Android). Errori firma "Communication with Apple failed / No profiles" = **NORMALI** finché l'account non è attivo.
- **Materiali iOS pronti** (`/mnt/user-data/outputs/ios/`): `AppIcon_1024.png` (1024×1024 quadrato pieno, no trasparenza/angoli, RGB), `ios_1..5.png` (iPhone 6.7" **1290×2796**), `testi_app_store.md` (nome, sottotitolo "Cosa guardare e dove vederlo", **keyword SENZA marchi** Netflix/Disney, descrizione, note revisore EN sul valore nativo).
- **PENDING**: attesa attivazione account → in Xcode "Try Again" firma → **Product > Archive → Distribute → App Store Connect** (upload) → creare scheda su App Store Connect → superare revisione. Ignorare/disattivare target **macOS Catalyst**.

### 24.4 FIX DEPLOYATI
- **POST→Redirect→GET su `/recommend`** (`main.py`): la POST ora reindirizza (303) a **GET** `/recommend?content_type=...&movie1=...` → elimina l'avviso **"Conferma reinvio modulo"** al refresh (segnalato dalla tester Sharon, fastidioso in-app). Aggiunto import `Query`. Bonus: risultati bookmarkabili/condivisibili.
- **Fix raccolta poster / dead-click** (`main.py`, `_enrich_titles_with_posters`): fallback TMDb live per item non in cache + pulizia titolo malformato (regex rimuove "(...)" finale) + write-back cache (self-healing).
- **Pagina eliminazione account** (`app/templates/elimina_account.html` self-contained + route `GET /elimina-account` in `main.py` che legge il template): requisito Play. Eliminazione **via email** a `info@cosaguardo.com` entro 30gg (metodo accettato da Google). **[DECISIONE Marco]**: NON mettere il bottone nel profilo — basta la pagina sul sito (risulta comunque per Play).

### 24.5 NOW TV / Awin — ATTIVATA + sul sito ✅ FATTA
NOW TV (Awin) **attivata e aggiunta al sito** (non più pending). Env: `AFFILIATE_AWIN_ID=2879325`, `AWIN_MID_NOW=9535`. Sky Go (TMDb id 29) → mappato su affiliato NOW in `core/recommendation_api.py`. **Restano pending**: TIMVision (Awin), Apple TV+ (2-3 mesi).

### 24.6 BANDA / RENDER — OTTIMA ✅
Dopo **17 giorni di luglio: banda < 1 GB** → il Cloudflare proxy contiene i bot scraper, tutto sotto controllo. Upgrade a Render Standard (~$25/mese) rinviato finché non arriva traffico organico (strategia conservativa confermata).

### 24.7 APPRENDIMENTI SOCIAL (chiave)
- **TIER LIST = formato regina su IG**: "Quando smettere di guardarla" **57,6k** (virale), "Finali che non dimentichi" **2.213**, "Capolavori" **878**. Coppie solide (Se7en+Shutter 488, Forrest Gump+Ali libertà 462, White Lotus+BLL 311). Caroselli **testuali** rendono meno (90-249) → contenuti **CON locandine/volti** battono i solo-testo.
- **⚠️ YouTube — LEZIONE TITOLI**: "Capolavori" ha fatto **8 views** su YT (pubblicato come Short, nessuna rivendicazione) perché il **titolo** partiva da "Obsession" (nicchia, nessuno lo cerca). Quella da **2.800** aveva titolo "Serie TV: quando dovevi smettere di guardarla" (keyword "Serie TV" davanti + gancio). **REGOLA YouTube** (è un motore di ricerca, ≠ IG): keyword ricercabile all'inizio (Film/Serie TV/titolo famoso) + gancio provocatorio; **MAI partire da un titolo di nicchia**. Su IG il titolo conta poco (contano immagine + pubblico).
- **Statici crollano su YT Shorts** anche se pubblicati come Short (l'algoritmo Shorts vuole movimento); i **video parlati reggono** (coppie fino 2.300, White Lotus 183). STRATEGIA: IG = tier list + coppie con locandine; YouTube = **solo video parlati con titolo ricercabile**.
- **Formato "coppia + chicche"**: coppia MAINSTREAM come amo (aggancia il pubblico largo) + 5 consigli con 1-2 **chicche di nicchia** (verificate in streaming IT). L'esca in una tier list = titolo **riconoscibile** (Stranger Things sì, I Soprano no).
- **Excel report aggiornato** (snapshot 15/07): `/mnt/user-data/outputs/cosaguardo_report_social.xlsx` (fogli Contenuti/Metriche/Riepilogo/Analisi; +6 contenuti, +14 rilevazioni). N.B.: media IG gonfiata dal virale 57,6k, tipico ~200-500.

### 24.8 CONTENUTI PRONTI (in canna)
- **Video coppie** (script a blocchi TTS 1.1x, hook "se ti sono piaciuti X+Y" + 5 titoli + CTA "su cosaguardo.com", caption + titolo YT ricercabile + musica forniti):
  1. Inception+Interstellar → chicca **Predestination** (Netflix/Prime). Musica epico/cinematic.
  2. Breaking Bad+Peaky Blinders → chicca **Snowfall** (Disney+). Musica dark/crime.
  3. Fight Club+Se7en (coppia famosa → CHICCHE dark) → Lo Sciacallo · Prisoners · L'uomo senza sonno · Oldboy · **Enemy** (verificato Prime/TIMVision, "erede di Fight Club"). Musica dark/inquietante.
- **Tier list** (template 1080×1920 in `/mnt/user-data/outputs/caroselli/`): "Capolavori" (con Obsession — horror 2026 record incassi <1M$), "Finali di serie" (esca The Walking Dead, non I Soprano), "Serie partite bene, poi..." (MAI ROVINATA/CALO LIEVE/CROLLATA/DISASTRO).
- **Principi caption**: alternare stile "dichiarato" (Ho messo X tra…) vs "sincero/non dichiarato" (così l'esca sembra opinione genuina). Musica: royalty-free per YT (Content ID), audio trend IG solo su Instagram.

### 24.9 NUOVO TIKTOK (vecchio bannato per Metricool)
Vecchio account `@cosaguardoapp` sospeso per **automazione (Metricool)**; reclamo fermo senza tempistica ("qualcuno lo controlla") → aperto **NUOVO account** (altro cel, altro numero, altra connessione).
- **PROTOCOLLO**: email+telefono NUOVI; **zero automazione/scheduler esterni (MAI Metricool)** — MA la **programmazione NATIVA di TikTok è OK** (da tiktok.com desktop / TikTok Studio). Username variante (`@cosaguardo.app`/`.it`); no file identici ai vecchi; **no watermark IG/CapCut**; ritmo umano 1 video/1-2gg; "riscaldare" l'account prima di postare; tier list ideali per pubblico giovane.
- **Bio** (max 80 char; link cliccabile solo da ~1000 follower → mettere `cosaguardo.com` in chiaro): scelta stile "POV: non sai cosa guardare 🍿 ci pensiamo noi → cosaguardo.com". Account **Creator** meglio di Business all'inizio (audio trend disponibili).

### 24.10 INFLUENCER / MARKETING
- **cinesocialclub — 2ª storia: ESITO (bocciata sul dato-chiave)**. Lato storia IG: 900 account raggiunti, 20 clic sul link (CTR ~2,2%, discreto), 4 like. Lato sito GA4: 10 sessioni, 9 utenti, **90% engagement, 2m00s permanenza, 13,7 eventi/sessione, 0 conversioni**. Lettura: nettamente meglio della 1ª storia (23 sess, 0 eventi) — il **traffico è di qualità** e il **sito converte l'engagement** (esplorano 2 minuti, la landing NON è il problema), MA dopo 2 storie **0 registrati**. Confronto: ludovicaledger (25€) aveva portato 1 sign_up + 2 install_click. VERDETTO: **non ripetere cinesocialclub**; il pubblico si diverte ma non è in target-registrazione e i volumi sono troppo piccoli. APPRENDIMENTO: il collo di bottiglia è **portare il pubblico giusto in volume**, non la landing.
- **ludovicaledger** (25€): 41 sessioni, **2 install_banner_clicked + 1 sign_up** (promettente). Metrica vera = **costo-per-registrato**.
- **unchained_cinema (10€) — INUTILE**: ha ricondiviso il reel nella storia SENZA mettere il link al sito → 1.600 views, 40 visite profilo (2,5%), **0 traffico sito, 0 conversioni**. Errore di esecuzione (niente link) + reach bassa.
- **CONCLUSIONE STRATEGICA (dopo 3 test influencer)**: le **storie "spot" da pochi euro con creator random sono CHIUSE** — reach bassa/incostante, raramente convertono, contro le Ads (IG/Google) partono in svantaggio (le Ads sono misurabili, scalabili, con link tracciabile). **Priorità: Ads (traffico/download) + contenuti organici (tier list, costo zero).** Influencer SOLO se: (a) davvero in target, (b) obiettivo download, (c) link tracciabile + brief rispettato (filtro da applicare alla proposta Benedetta/Eleonora).
- **Test IG boost 10€ — NON fatto** (Marco ha deciso di non procedere). [Nota strategica per il futuro: sponsorizzare per follower è vanity; se si sponsorizza, meglio puntare a traffico sito o, quando l'app è live, a "scarica l'app".]

### 24.12 ADS — efficienza per canale + strategia (dato chiave 17/07)
Spesa Ads finora: **IG 562€, Google 58€** (tot 620€). Efficienza:
- **Instagram**: ~0,15€/visita → ~3.700 visite, **~90 registrazioni** → **~6,2€/registrazione**.
- **Google**: ~0,60€/visita → ~97 visite, **2-3 registrazioni** → **~19-29€/registrazione**.
- **CONCLUSIONE: Instagram vince nettamente** (visita 4× più economica, registrazione 3-5× più economica). **DECISIONE: spostare il grosso del budget su IG**; Google solo se utile per awareness/intercettare ricerche.
- **Costo-per-registrazione IG ~6€ = numero-guida** per valutare ogni canale futuro.
- **[Marco] Nessun ritorno economico atteso in early stage (per scelta)**: focus ora = **crescita del sito**, monetizzazione più avanti. Il business plan post-lancio dovrà chiarire quanto vale un utente registrato nel tempo (per capire se ~6€ di acquisizione è sostenibile).
- Dati tracciati nel file bilancio, foglio **"Ritorno Ads"**.
- **Video freelance per Ads**: costa solo **31,44€** → conveniente; per la campagna app conviene farne **2-3 varianti** da testare A/B su Meta.

### 24.13 STRATEGIA CAMPAGNA APP (post-lancio, da ricordare)
- Creare una **NUOVA campagna dedicata all'app** (non riusare quella traffico-sito): obiettivo diverso, ottimizzazione e misurazione separate. Tenere viva anche quella verso il sito.
- **Tracciamento Livello 1 (semplice, consigliato all'avvio)**: link Play Store con **UTM/referrer** → Play Console mostra installazioni per sorgente. Nessuna modifica al codice app.
- **Tracciamento Livello 2 (complesso)**: Meta SDK/MMP dentro l'app per far ottimizzare Meta sui download — con la TWA è laborioso → solo se si scala forte.

### 24.14 ⚠️ SCADENZA AGOSTO 2026 — target API 36 (Google Play)
Mail da Play Console (22/07): l'app targeta **API 35**, ma **dal 31 agosto 2026** nuove app e **aggiornamenti** devono targetare **Android 16 (API 36)**. Le app esistenti restano disponibili ai nuovi utenti se targetano almeno API 35 → **oggi sei conforme**, non è bloccante.
- **[DECISIONE Marco]**: si fa **DOPO** che l'app è online in produzione (non durante i 14 giorni di test).
- **Perché non ora**: (1) PWABuilder storicamente è in ritardo sulle scadenze Google (nel 2024 generava ancora API 33 quando serviva 34) → si rischia di rifare tutto per nulla; (2) caricare release durante il conteggio dei 14gg è un rischio inutile; (3) Android 16 cambia comportamenti (notifiche full-screen richiedono permesso esplicito) → l'app va ritestata con calma.
- **Come si farà (inizio agosto)**: rigenerare il pacchetto su PWABuilder → ⚠️ **usare LO STESSO `signing.keystore`** dello zip originale (altrimenti Play rifiuta per upload key mismatch) → alzare **version code a 2** → caricare come aggiornamento → ritestare push e schermo intero.
- Se serve più tempo: si può richiedere **proroga fino al 1° novembre 2026** (form nella Play Console, sezione stato norme).

### 24.11 PENDING (todolist aggiornata al 17/07)
- **Android**: completare 12 tester → 14gg → richiedere Produzione → link Play su sito/social.
- **Android (agosto)**: dopo la produzione, **bump a target API 36** entro il 31/08/2026 (vedi 24.14).
- **iOS**: attesa attivazione account Apple → Archive + upload in Xcode → scheda App Store Connect → revisione (4.2) → disattivare target macOS.
- **Verificare deploy online**: PRG /recommend, raccolta poster, /elimina-account, assetlinks (2 impronte), push (Fasi 1-3), Sky Go/NOW.
- **Misurare**: GA4 30gg vs 30gg, GSC.
- **Social**: ramp-up nuovo TikTok (manuale/nativo), continuare tier list (IG) + video-coppie (YouTube con titolo ricercabile).
- **Futuro**: invio EMAIL alert anonimi (Resend); redirect on-brand `/go/<creator>` vs bit.ly; quando attivi AdSense → cambiare dichiarazione annunci su Play; Awin TIMVision; monitorare Render bandwidth.
- **File bilancio uscite creato**: `/mnt/user-data/outputs/cosaguardo_bilancio_uscite.xlsx` (fogli Uscite + Riepilogo, totali automatici + costo mensile/annuale equivalente). Spese registrate: influencer ludovicaledger 25€ / cinesocialclub 20€ / unchained_cinema 10€, Google Play 15€ una tantum, Apple 99€/anno; celle gialle DA COMPILARE: Render, CapCut, Google Ads. (Boost IG 10€ NON fatto.)
- **Bilancio / Business plan**: [Marco] preparare un **bilancio delle uscite attuali** (Render, dominio, Apple 99$/anno, ads, influencer, ecc.) per tenere i costi monitorati; **dopo il lancio dell'app**, costruire un **business plan da seguire** (proiezioni costi/ricavi, break-even, obiettivi).

---

**Fine documento.** In nuova chat, carica questo file come primo upload e ripartiamo da qui.

---

## 25. Sessione 23/07/2026 — iOS build caricata su App Store Connect, affiliazione Amazon (bounty Prime), fix filtro Prime, template tier list, Email Routing

### 25.1 STATO GENERALE A INIZIO SESSIONE (22/07)
- **Android**: test chiuso, giorno 7 di 14, 12 tester ok → poi richiesta Produzione.
- **iOS**: account Apple ancora "in elaborazione", mail al supporto inviata.
- **Bump target API 36**: confermato rimandato a dopo il lancio (scadenza 31/08).
- **TikTok**: nuovo account in ramp-up.
- **Contenuti in canna (conteggio reale, aggiorna 24.x)**: ~10 tier list, ~10 video, 6-7 caroselli.

---

### 25.2 TEMPLATE TIER LIST — versione definitiva con ZONE DI SICUREZZA
**Problema**: su TikTok le tier list venivano coperte in 3 punti — barra ricerca (alto), colonna icone (destra), bolla profilo (basso sinistra). Su IG il problema non esiste.

**Soluzione (una sola versione valida per IG + TikTok), tela 1080×1920:**
| Zona | Riserva |
|---|---|
| Alto | 220 px (barra ricerca TikTok) |
| Destra | 300 px (colonna icone) |
| Basso | 260 px (bolla profilo + caption) |

**Layout misurato (file `tier_da_quando_v3_*`):**
- Margine sinistro x=60; nulla oltre **x=780**
- Logo CosaGuardo (76px) + kicker "TIER LIST" da y=266
- Titolo Montserrat ExtraBold, base a y=554
- **Gap titolo→sottotitolo = 86 px** (era 18 → causa della sensazione "incasinata")
- Sottotitolo y 640→678; griglia y 742→1642
- Colonna etichette 195 px, poster **140×210**, gap 20 px, **3 poster per riga** (12 titoli, non 16)
- Colori blocchi ripresi dai reel: blu → verde acqua → ambra → rosso/rosa

**File generati**: `/mnt/user-data/outputs/caroselli/tier_da_quando_v3_BASE.png` (sfondo da caricare in Canva) + `_GUIDA.png` (bande rosse + nomi serie, mai esportare).

**Regola operativa**: in Canva salvare come **modello di brand**, con livello guida bloccato e nascosto prima dell'export. Per le prossime tier list: rigenerare da script cambiando titolo/etichette/titoli.

**Caption TikTok**: sempre **una riga**, il resto in commento fissato (una caption lunga allunga il blocco basso e mangia la griglia).

---

### 25.3 CONTENUTO PRODOTTO IN SESSIONE

**Tier list "Da quando diventa bella"** (approvata, template v3):
- TI PRENDE SUBITO: Chernobyl · Squid Game · Mare Fuori
- DOPO 2-3 EPISODI: Stranger Things · The Last of Us · Mercoledì
- DA METÀ STAGIONE: **Breaking Bad** (provocazione) · Dark · Better Call Saul
- SOLO DALLA SECONDA: **Il Trono di Spade** (provocazione forte) · Peaky Blinders · Ted Lasso
- Caption IG nello stile consolidato ("La mia tier list onesta…"), TikTok una riga, commento fissato con lista + domanda.
- Titolo YouTube: "La mia tier list onesta: da quando le serie TV diventano belle davvero" (alternativa più SEO: "Serie TV: da quando diventano belle davvero (Il Trono di Spade in fondo)").

**VIDEO 4 — coppie: "Se ti sono piaciute CHERNOBYL + LA REGINA DEGLI SCACCHI"**
Filo: miniserie chiuse, ricostruzione d'epoca, nessuna stagione di troppo.
1. Unbelievable · 2. Dopesick · 3. Mare of Easttown · 4. When They See Us · 5. **The Terror** (chicca; riserva: Il Miracolo)
- Musica: "ambient tension", "cold war", "minimal piano", "drone" — registro gelido, non epico.
- Titolo YT: "5 miniserie come Chernobyl e La regina degli scacchi 💎 (l'ultima non la conosce nessuno)"
- ⚠️ Già fatti in passato: Stranger Things+Dark, **Breaking Bad+Peaky Blinders**.
- Prossime coppie pronte: La Casa di Carta+Lupin, The Last of Us+The Walking Dead.

**⚠️ NUOVA REGOLA — LUNGHEZZA VIDEO (dal video 5 in poi)**
Target: **45-50 secondi** (il video 4 era ~63s / 178 parole).
- TTS 1.1x ≈ **170 parole/minuto ≈ 2,8 parole/secondo**
- **Budget: 130-140 parole totali** → Hook 20 · ogni titolo 17 · chicca 24 · CTA 10
- Tre regole di taglio: (1) una frase sola per titolo; (2) via i richiami alla coppia di partenza (l'hook li ha già stabiliti); (3) un solo dettaglio concreto per titolo.
- **DA FARE**: calibrare una volta con un blocco reale in CapCut per verificare le 170 wpm.

---

### 25.4 AMAZON ASSOCIATES — approvazione, diagnosi e patch

**Approvazione**: mail 14/07, account `cosaguardo-21` approvato. Le 3 vendite qualificate erano già state fatte.
**Dati primo mese (23/06-22/07)**: 85 clic · 4 articoli · 37€ ordinato · **1,11€ commissione** · conversione 4,71%.
- Il filtro "Tracking ID" nella schermata Riepilogo **è buggato** (non mostra nulla filtrando su cosaguardo-21); il CSV del Report guadagni conferma che tutto è sotto quel tag.
- Le vendite sono **acquisti accidentali**: cookie 24h → l'utente clicca Prime Video, arriva su Amazon e compra altro. ~3% = aliquota elettronica.

**Diagnosi architettura (importante)**:
- `/cosa-serve` ha solo **4 visualizzazioni/mese** → NON è la fonte dei clic. (Attenzione: in GA4 usare **Pagine e schermate**, non "Pagina di destinazione", che conta solo le sessioni che *iniziano* lì.)
- I clic vengono dalle **schede film** e da `/piattaforma/prime-video` (111 viste).
- Il link affiliato NON è nei template: è iniettato a runtime da **`core/recommendation_api.py` → `_build_affiliate_link()`**, che arriva a `detail.html` come `p.link` + flag `p.is_affiliate`.
- Formato attuale per Amazon: `https://www.amazon.it/s?k={titolo}&i=instant-video&tag=cosaguardo-21` → **ricerca su Amazon**, non pagina del titolo né iscrizione Prime.
- ⚠️ **Non "migliorare" il link puntando alla pagina del titolo**: oggi si guadagna proprio perché la gente esce dalla ricerca e curiosa. Dati insufficienti per decidere (4 vendite).

**BOUNTY PRIME — la leva vera**
- Link ufficiale (**unica landing che genera bounty**): `https://www.amazon.it/provaprime?tag=cosaguardo-21`
- Valore **confermato: 3€ per prova gratuita attivata** (vs 0,03€ medi attuali).
- Vale solo per **clienti nuovi** idonei ai 30 giorni gratis.
- Bug trovato: `get_platform_subscribe_link()` chiamava `_build_affiliate_link(name, title="")` → CTA "Abbonati a Prime Video" portava a una **ricerca vuota**. Corretto: ora restituisce il link provaprime.

**PATCH DEPLOYATA (9 file)** — `/mnt/user-data/outputs/patch_affiliati/`
| File | Modifica |
|---|---|
| `core/recommendation_api.py` | bounty in `get_platform_subscribe_link()`; nuova `get_amazon_bounty_link()`; **fix PLATFORM_MAP prime 9→119** |
| `app/main.py` | import + `amazon_bounty_link` nelle 3 route (detail film, detail serie, dove_vedere) |
| `detail.html` | rel condizionale, CTA bounty sotto i provider, disclosure in fondo |
| `dove_vedere.html` | idem (pagina più importante: ~753 URL indicizzati) |
| `come.html`, `best.html`, `platform.html` | rel sponsored + disclosure |
| `index.html`, `results.html` | rel in JS: `${p.is_affiliate ? 'sponsored' : 'nofollow'}` |

- **Disclosure**: "Alcuni link verso le piattaforme sono affiliati: in qualità di Affiliato Amazon, CosaGuardo riceve un guadagno dagli acquisti idonei. Per te il prezzo non cambia." — 12px, grigia, mostrata **solo se c'è almeno un link affiliato**.
- ⚠️ **Debito tecnico**: la disclosure è replicata in 6 template. Soluzione pulita: una riga sola nel footer di `base.html`.

**DA FARE su Amazon**:
- Verificare periodicamente il bounty (le cifre cambiano con le promozioni).
- ⚠️ **Policy applicazioni mobili**: l'app Android/iOS serve le stesse pagine con gli stessi link affiliati. Da verificare nel contratto operativo Associates o col supporto — rischio chiusura account.
- Valutare **Tracking ID separati** per fonte (es. `cosaguardo-cs-21` per /cosa-serve, `cosaguardo-fi-21` per le schede).

---

### 25.5 BUG RISOLTO — filtro Prime su /scopri (schermata vuota)
**Causa**: due mappe piattaforme con ID diversi.
- `PLATFORM_MAP` (usata da `/scopri`) aveva `"prime": 9`
- `PLATFORM_SLUGS` (usata da `/piattaforma/`) ha `"prime-video": 119`
Con `watch_region=IT` il provider Prime Video su TMDb è **119**; l'ID 9 è di altri mercati → zero risultati.
**Fix**: `PLATFORM_MAP["prime"] = 119` + commento di avviso. Verificati tutti e 6 gli ID: ora allineati (Netflix 8, Prime 119, Disney 337, Apple 350, Paramount 531, NOW 39).
⚠️ **Debito tecnico**: due mappe per la stessa cosa è fragile — da unificare.

---

### 25.6 iOS — DA ACCOUNT BLOCCATO A BUILD CARICATA (percorso completo, molti ostacoli)

**Account**: attivo, Team ID **`JJKPDS552T`**, rinnovo 14/07/2027. ⚠️ **Nessuna carta associata per il rinnovo automatico** → da sistemare (senza carta le app vengono rimosse).

**Ostacoli risolti, in ordine:**
1. **Xcode non vedeva il team** → rimuovere e riaggiungere l'Apple ID in Settings → Accounts.
2. **Xcode era in `~/Downloads`** invece che `/Applications` → spostato. Versione Xcode 26.3, SDK MacOSX26.2 (aggiornato, nessun problema di versione minima).
3. **CocoaPods non installabile** con `sudo gem install`: il Ruby di sistema (2.6) non compila `nkf` perché gli SDK recenti **non includono più gli header di Ruby**. → **Soluzione: Homebrew** (`brew install cocoapods`, v1.17.0). Non insistere con gem.
4. **Pods mancanti** → `cd ~/Downloads/CosaGuardo/src` → `pod install` (Firebase 12.16.0 + 8 dipendenze). ⚠️ **Aprire sempre `CosaGuardo.xcworkspace`, mai il `.xcodeproj`**.
5. **"Your team has no devices"** → nessun iPhone registrato; il telefono non veniva riconosciuto dal Mac.
6. ⚠️ **ERRORE COMMESSO DA CLAUDE, da non ripetere**: aver suggerito di forzare `Code Signing Identity = Apple Distribution` nelle Build Settings → genera "conflicting provisioning settings". **Con firma automatica "Apple Development" nella scheda Release è CORRETTO.**
7. **Soluzione definitiva: FIRMA MANUALE**
   - Xcode → Settings → Accounts → Manage Certificates → + → **Apple Distribution**
   - developer.apple.com → Identifiers: `com.cosaguardo` con **Push Notifications + Associated Domains** ✅
   - Profiles → + → **App Store Connect** → profilo `CosaGuardo AppStore` → Download → doppio clic
   - Signing & Capabilities → **togliere** "Automatically manage signing" → scheda Release → selezionare il profilo
8. **Product → Archive** → OK → **Distribute App → App Store Connect → Upload** → **"Uploaded to Apple"** ✅ (Build 1.0 (1), arm64, 23/07 ore 06:01)

**⚠️ Xcode Cloud**: se compare "The workspace is not using Git Source Control" → **Cancel**. È un servizio a pagamento che non serve (build in locale).

**Supported Destinations**: tolte le voci Mac/Catalyst. **iPhone + iPad rimasti** → obbligatori anche gli screenshot iPad.

---

### 25.7 APP STORE CONNECT — stato compilazione

**Fatto:**
- Record app `com.cosaguardo` esistente; build associata; account di test fornito ai revisori
- **Contratti**: "Contratto per le app gratuite" **Attivo** (quello a pagamento resta "Nuovo", non serve)
- **DSA — dichiarato OPERATORE COMMERCIALE**. Motivazione: link affiliati attivi, Google Ads, promozione social, intento di monetizzazione. (Apple accetta **casella postale** al posto dell'indirizzo di casa; email pubblica consigliata `info@cosaguardo.com`.) Documento identità inviato per verifica.
- **Copertura geografica (.geojson)**: campo **facoltativo**, lasciato vuoto (serve solo ad app geo-limitate).
- **Screenshot iPhone**: 5 pronti a **1284×2778** in `/mnt/user-data/outputs/ios_screenshot/`. Erano 1290×2796 (nativi iPhone 15/16 Pro Max) → **non accettati**: ridimensionati mantenendo proporzioni + padding navy.
  - Ordine consigliato (solo i primi 3 appaiono nella scheda di installazione): **ios_4** (home) · **ios_5** (consigli su misura) · **ios_2** (scheda titolo) · ios_1 · ios_3

**Questionario privacy — risposte date:**
| Dato | Finalità | Collegato | Monitoraggio |
|---|---|---|---|
| Indirizzo email | Funzionalità app | Sì | No |
| ID utente | Funzionalità · Personalizzazione (+Analisi se user-id passa a GA4) | Sì | No |
| ID dispositivo (token FCM) | Funzionalità app | Sì | No |
| Interazione con il prodotto | Analisi · Personalizzazione | No | No |

⚠️ **Nodo chiave risolto**: la risposta "No" al monitoraggio era valida **solo disattivando i Segnali Google in GA4** (erano attivi, come documentato nella vecchia privacy policy). Dichiarare monitoraggio = obbligo di implementare **ATT**, non presente nell'app → rifiuto sicuro. **Segnali Google DISATTIVATI** (GA4 → Amministrazione → Impostazioni relative ai dati → Raccolta dei dati).

**MANCA ANCORA:**
- **Screenshot iPad** (l'app è universale) — Marco li farà dall'iPad, Claude ci metterà la cornice
- Invio in revisione (24-48h di attesa media)

**⚠️ RISCHIO RIFIUTO PRINCIPALE — cancellazione account**
Apple richiede che la cancellazione sia **avviabile da dentro l'app**. La pagina `/elimina-account` descrive una procedura via email (accettata da Google Play). Mitigazione applicata: link ora presente **nel menu drawer** (sotto Profilo, se loggato) **e nel footer**. Se Apple contesta comunque → **piano B: pulsante che avvia realmente la cancellazione dal profilo** (richiede Mac per nuova build).

**Altro rischio minore**: negli screenshot compaiono loghi Netflix/Prime/Disney+ e locandine. Uso standard (JustWatch fa uguale); se contestato, rispondere che l'app indica dove i contenuti sono disponibili e non li distribuisce.

---

### 25.8 PRIVACY POLICY — aggiornata (deployata)
File: `privacy.html` + `base.html` in `/mnt/user-data/outputs/patch_affiliati/app/templates/`
- **Rimossi Google Signals** da sez. 2, 4, 5 (coerenza con la disattivazione in GA4 e con la dichiarazione Apple). Resta una sola menzione, nella frase che dichiara di NON usarli.
- **Aggiunto Firebase Cloud Messaging**: token dispositivo tra i dati raccolti, tra le finalità e tra i soggetti con cui si condivide.
- **Sez. 6 riscritta**: link diretto a `/elimina-account` + "entro 30 giorni".
- Data → luglio 2026.

---

### 25.9 EMAIL — Cloudflare Email Routing configurato
**Problema**: `info@cosaguardo.com` non arrivava. Causa: il forwarding era configurato su **Namecheap**, ma i nameserver puntano a **Cloudflare** → inattivo.

**Fatto:**
- Cancellati i **5 record MX** `eforward*.registrar-servers.com` e il TXT SPF di Namecheap (gli MX non possono convivere)
- Cloudflare Email Routing → Settings → **Add missing records** (3 MX `route1/2/3.mx.cloudflare.net` + TXT DKIM + TXT SPF `include:_spf.mx.cloudflare.net`)
- Destination address verificata + regola per `info`

⚠️ **`privacy@cosaguardo.com` è citato 4 volte nella privacy policy** come unico canale GDPR → serve la regola dedicata **o il catch-all** (consigliato: copre anche supporto@, contatti@, ecc.).

**Regola generale**: un solo TXT `v=spf1` per dominio (due sono invalidi). Namecheap gestisce solo il dominio; DNS ed email sono su Cloudflare.

---

### 25.10 TODO AGGIORNATA (fine sessione 23/07)

**iOS — imminente (fattibile senza Mac, da PC/iPad):**
1. Screenshot iPad → Claude aggiunge la cornice → caricare
2. Completare gli ultimi campi scheda → **Aggiungi alla verifica**
3. Attendere esito (24-48h). Se rifiuto per cancellazione account → serve il Mac (Marco lo riavrà tra 4 giorni)
4. Aggiungere **carta di credito** per il rinnovo Apple

**Android:**
5. Completare i 14 giorni (scadenza ~28-29/07) → richiedere Produzione. ⚠️ Non caricare nuove release nel frattempo; verificare intorno al giorno 10 che i 12 tester siano ancora iscritti
6. **Agosto**: bump target API 36 entro il 31/08 (vedi 24.14)

**Amazon:**
7. Verificare la **policy applicazioni mobili** (rischio account)
8. Valutare Tracking ID separati per fonte
9. Dopo il deploy: verificare che la CTA bounty e la disclosure compaiano (Ctrl+Shift+R)

**Tecnico / debito:**
10. Disclosure affiliazione → spostare nel footer di `base.html` (togliere le 6 copie)
11. Unificare `PLATFORM_MAP` e `PLATFORM_SLUGS`
12. `privacy@` su Cloudflare (regola o catch-all)

**Contenuti:**
13. Smaltire l'arretrato (~10 tier list, ~10 video, 6-7 caroselli). TikTok: **i post foto NON sono programmabili nativamente** → pubblicazione manuale; i video sì (fino a 10 giorni, solo da desktop, account Creator/Business, non modificabili dopo)
14. Video 5 con il nuovo budget di 130-140 parole
15. Calibrare la velocità TTS reale in CapCut

**Analytics:**
16. Verificare in GA4 i **clic in uscita** verso Amazon per pagina di origine (Esplorazioni: dimensioni `Percorso pagina` + `Link URL`, metrica `Conteggio eventi`, filtro `Nome evento = click`). Prima controllare che "Clic in uscita" sia attivo in Misurazione avanzata
17. Con 945 sessioni/mese ogni analisi di conversione è fragile: **il collo di bottiglia è il traffico**, non l'ottimizzazione

---

**Fine sezione 25.**

---

## 26. Metriche social al 24/07/2026 + screenshot iPad + invio App Store

### 26.1 iOS — INVIATA IN REVISIONE ✅
- Stato: **"1.0 In attesa di verifica"** (inviata 24/07). Esito entro 48h via mail su mfantini84@gmail.com.
- **Screenshot iPad**: 5 file **2048×2732** in `/mnt/user-data/outputs/ios_screenshot/` (`ipad_1..5_*`). Sorgenti 1640×2360 (iPad Air 11"), **ritagliata via la chrome di Safari** (URL bar + barra di stato) — lasciarla avrebbe dichiarato apertamente il webview → rischio linea guida 4.2. Cornice ricostruita nello stile degli iPhone (navy radiale, logo + wordmark, titolo Montserrat ExtraBold).
  - Ordine: 1. Consigli fatti su misura · 2. Esplora per genere e umore · 3. Il catalogo di ogni piattaforma · 4. Il meglio dello streaming, ogni giorno · 5. Le classifiche dei migliori
- **App ID App Store Connect: 6793943636**
- **Prezzo**: Gratuita. **Privacy dell'app**: questionario **pubblicato** (senza "Pubblica" risulta mancante).
- **Diritti sui contenuti**: dichiarato **Sì** (contenuti TMDb + loghi piattaforme). → Aggiunta **attribuzione TMDb nel footer** di `base.html`: "Questo prodotto utilizza l'API di TMDB ma non è approvato o certificato da TMDB" con link a themoviedb.org. ⚠️ **Manca il logo TMDb**, richiesto dai loro termini: da scaricare dalle risorse di brand e affiancare al testo.
- **Classificazione età: 13+**. Risposte date:
  - Controlli in-app: tutti No · Contenuti generati utenti / social / chat / gioco d'azzardo / loot box: No
  - **Pubblicità: Sì** (coerenza con la dichiarazione DSA da operatore commerciale: link affiliati = promozione a pagamento)
  - Horror, alcol/sostanze, violenza cartoon, violenza realistica, armi, suggestivo, nudità non esplicita → **Poco frequente**
  - Violenza sadica prolungata, sessuale esplicito → **Nessuno** (regge grazie al filtro adult già implementato)
  - ⚠️ **Accesso al web senza limitazioni → No**: da VERIFICARE su TestFlight che i link esterni (Amazon/Netflix/Prime) si aprano in Safari e non dentro l'app.
- **TestFlight**: build installabile senza attendere la revisione (test interni). Primo tentativo → "L'app richiesta non è disponibile": molto probabilmente propagazione del prezzo appena impostato. Se persiste, controllare **conformità all'esportazione** sulla build.
- **Email**: `privacy@` e `supporto@` creati su Cloudflare ✅ (chiude il punto aperto della privacy policy)

---

### 26.2 METRICHE SOCIAL — il dato che cambia le priorità

**Instagram**
| Contenuto | Views |
|---|---|
| Quando smettere di guardarla (tier) | **62.500** (era 57.600, cresce ancora) |
| Film di supereroi (tier) | 18.400 |
| Film da vedere prima di morire (tier) | 10.700 |
| I film che tutti chiamano capolavori (tier) | 1.788 |
| I finali di serie più discussi (tier) | 1.503 |
| Interstellar+Inception (coppia) | 850 |
| Breaking Bad+Peaky Blinders (coppia) | 824 |
| Serie partite bene, poi… (tier) | 440 |
| Almeno una volta (carosello) | 210 |
| Odissea+Il Gladiatore (coppia) | **91** |

**YouTube Shorts**
| Contenuto | Views |
|---|---|
| Serie partite bene, poi… (tier) | 2.500 |
| Film da vedere prima di morire (tier) | 1.700 |
| Film di supereroi (tier) | 570 |
| Braveheart / Odissea+Gladiatore (coppia) | 530 |
| Shutter Island / Interstellar+Inception (coppia) | 264 |
| Narcos / Breaking Bad+Peaky (coppia) | 212 |

**TikTok (account nuovo, 4 post, 2 follower, 206 like)**
| Contenuto | Views |
|---|---|
| I finali di serie più discussi (tier) | **10.818** |
| Quando smettere di guardarla (tier) | 3.439 |
| Troy / Odissea+Il Gladiatore (coppia) | 609 |
| Da quando diventa bella (tier, template v3, online da 2h) | 140 |

**LETTURA:**
- **Le tier list battono le coppie di 1-2 ordini di grandezza su IG** (62,5k vs 850). Anche la tier list peggiore (440) è in linea con la coppia migliore (850).
- **Su YouTube il divario si stringe** (2,5k vs 530): il formato parlato funziona meglio dove la gente cerca. Odissea+Gladiatore: 91 su IG, 530 su YT.
- **Su TikTok** le tier list dominano (10,8k vs 609).
- **Costo/beneficio**: montare una coppia costa molto più di una tier list e su IG rende una frazione.

**DECISIONE DI MARCO (editoriale, non solo metrica):** continuare comunque con le coppie — evitano il mono-argomento e **mostrano cosa fa davvero il sito** (il motore di raccomandazione). Ipotesi da valutare: **riservare le coppie a YouTube/Shorts** e tenere IG prevalentemente per le tier list.

---

### 26.3 PROBLEMA APERTO — TikTok: view alte, follower ~0
10.818 view → **2 follower**. Conversione praticamente nulla.

**Diagnosi**: con **4 post** il profilo non dà motivo di seguire. Chi arriva da un video virale controlla il profilo, vede una griglia vuota e non torna. Le metriche di conversione a questo volume sono premature.

**Leve, in ordine di impatto:**
1. **Volume**: pubblicare l'arretrato (~10 tier list, ~10 video, 6-7 caroselli). È di gran lunga la leva principale.
2. **Serializzazione**: dare un nome ricorrente al format e una cadenza dichiarata ("ogni giovedì una tier list") → motivo esplicito per seguire.
3. **CTA verbale dentro il video**, non solo in caption.
4. **Commento fissato** con la domanda + invito a seguire.
5. **Rispondere ai commenti**: genera visite al profilo, che è dove avviene la conversione.

---

### 26.4 TODO AGGIORNATA (24/07)
1. ⏳ Esito revisione Apple (entro 48h)
2. ⏳ Android: 14 giorni completi ~28-29/07 → richiedere Produzione (non caricare release nel frattempo; check tester al giorno 10)
3. TestFlight: installare e **verificare che i link esterni escano in Safari**
4. Aggiungere **logo TMDb** nel footer
5. Aggiungere **carta di credito** per il rinnovo Apple Developer
6. Pubblicare l'arretrato social (leva principale per i follower TikTok)
7. Video 5 (coppie candidate: **La Casa di Carta + Lupin**, The Last of Us + The Walking Dead) con budget 130-140 parole
8. Calibrare la velocità TTS reale in CapCut
9. Debito tecnico: disclosure nel footer di `base.html`; unificare PLATFORM_MAP/PLATFORM_SLUGS
10. Amazon: verificare policy applicazioni mobili; valutare Tracking ID per fonte

**Fine sezione 26.**

---

## 27. Sessione 27/07/2026 — iOS rifiuto+reinvio, generatore tier list (agente), accredito Venezia

### 27.1 iOS — RIFIUTO AUTOMATICO E REINVIO (build 2)

**Primo esito revisione: RIFIUTATA** (rifiuto automatico, non di merito).
Motivo: **purpose string segnaposto/insufficienti** nell'`Info.plist`. L'analisi automatica ha trovato 3 permessi con descrizioni generiche che l'app NON usa:
- `NSMicrophoneUsageDescription` → "Capture Audio by user request"
- `NSLocationWhenInUseUsageDescription` → "Track current location by user request"
- `NSCameraUsageDescription` → "Capture Video by user request"

**Causa**: residui del template Xcode (erano finiti anche in App Sandbox lato macOS, già tolto). L'app è una webapp che mostra film: microfono, posizione e fotocamera non servono → la strada giusta era RIMUOVERLI, non riscrivere le descrizioni.

**FIX applicato sul Mac** (percorso `~/Downloads/CosaGuardo/src`):
1. Aperto `CosaGuardo/Info.plist` in Xcode → eliminate le 3 voci UsageDescription (verificato con `grep -A1 "UsageDescription" CosaGuardo/Info.plist` → non stampa più nulla)
2. Signing & Capabilities: nessun App Sandbox residuo da sistemare (le capability rimaste — Associated Domains, Background Modes/Remote notifications, Push Notifications — sono legittime)
3. **General → Build da 1 a 2** (Version resta 1.0). ⚠️ Obbligatorio: senza cambio numero l'upload viene rifiutato
4. Product → Archive → Distribute App → App Store Connect → Upload → Manually manage signing → profilo `CosaGuardo AppStore`
5. Build 2 elaborata (verde). Riassociata alla versione 1.0 → **Aggiorna la verifica**

**Stato**: build 2 in revisione (inviata 27/07). Attesa esito 24-48h.

**⚠️ Procedura reinvio build (per il futuro)**: i metadati (scheda, screenshot, privacy, prezzo, età, DSA) sono legati alla VERSIONE 1.0, non alla build → sostituendo la build NON si ricompila nulla. Solo: incrementa Build, Archive, Upload, riassocia, Aggiorna la verifica.

**TestFlight — problema NON risolto**: installazione continua a fallire con "L'app richiesta non è disponibile o non esiste", anche con la build 2 (numero diverso, che era il rimedio più citato). **È un bug ricorrente lato Apple** (confermato da molte segnalazioni sui forum, anni di storia). NON blocca nulla: TestFlight e revisione App Store sono binari separati. Se Apple approva, l'app si scarica dallo Store e TestFlight diventa irrilevante. Tentativi lato device: disinstallare/reinstallare TestFlight, pull-to-refresh.

**App ID App Store Connect: 6793943636** · Team ID: JJKPDS552T

---

### 27.2 GENERATORE TIER LIST — "agente" semi-autonomo (NUOVO STRUMENTO)

Cartella consegnata: `/mnt/user-data/outputs/agente_tierlist/`. Gira in **locale su Windows**.
Da tenere in `C:\Users\m.fantini\Desktop\tierlist\`.

**File:**
| File | Funzione |
|---|---|
| `tierlist.py` | Fase 1 — grafica: legge JSON, scarica poster TMDb, compone PNG 1080×1920 |
| `genera.py` | Fase 2 — contenuti: chiede a Claude argomento/fasce/16 titoli, poi crea il PNG |
| `esempio_tierlist.json` | modello del file di input |
| `storico.json` | memoria delle tier list già fatte (precompilato con le 7+2 pubblicate) |
| `.env.esempio` | modello per le chiavi (rinominare in `.env`) |
| `ISTRUZIONI.md` | guida completa |

**Uso quotidiano**: `python genera.py` → propone → `[s=va bene · m=modifica · r=rifai · n=annulla]` → a `s` crea il PNG in `output\`.
- **`m` = modifica mirata**: cambia solo ciò che chiedi, il resto resta identico. Ripetibile.
- **`r` = rifai da capo**: proposta nuova (si perde il resto).

**Chiavi (`.env`)**: file `.env` nella cartella con `TMDB_API_KEY=...` e `ANTHROPIC_API_KEY=...`. Gli script lo leggono da soli. NON va su GitHub. Path alternativo via `set CG_ENV=...`. ⚠️ Serve account API Anthropic separato (console.anthropic.com) con credito — pochi centesimi a tier list. Chiave TMDb: quella già su Render.

**Preset**: `--titoli 12` (rispetta zone TikTok, griglia entro x=780) · `--titoli 16` (default; 4ª colonna sotto le icone TikTok, x=895 — deciso accettabile). Poster 140×210 identici nei due preset (l'altezza è vincolata dalle 4 fasce).

**LEZIONI IMPARATE (già codificate nel prompt/script):**
- **Poster sbagliati su titoli generici**: TMDb ordina per popolarità e sbaglia (es. "Dark" → prendeva "Dark Shadows"; "Unbelievable" → serie coreana). Soluzione: campi opzionali `"anno"` e `"tmdb_id"` nel JSON; regola nel prompt di scrivere sempre l'anno.
- **Titoli compositi**: "Il Trono di Spade: House of the Dragon" non trova il poster. Lo script ora prova varianti (parti dopo i due punti, poi inglese); il prompt chiede il titolo esatto senza prefissi di franchise.
- **Ordine fasce = colore**: la 1ª fascia dev'essere sempre la più desiderabile (blu), l'ultima la meno (rosso). Prima capitava il rosso sui titoli migliori. Regola aggiunta al prompt.
- **Etichette illeggibili**: erano troppo piccole. Fix: colonna etichette più larga nel preset 16 (260px), algoritmo che sceglie la miglior spezzatura su 1-3 righe, tetto a 34px, penalità sulle righe extra. Regola nel prompt: **max 16 caratteri, max 3 parole**. Supporto opzionale a un font condensato (`fonts/Etichette.ttf`).
- **max_token**: i modelli recenti "ragionano" prima di rispondere; budget alzato a 12000 (troncava a 3000). Il prefill `{` NON è supportato da alcuni modelli → rimosso.

**JSON salvati** in sottocartella `json_salvati\` (con suffisso `_2` se stesso titolo). ⚠️ Se una tier list generata non viene pubblicata, cancellare la sua voce da `storico.json`, altrimenti il criterio resta "bruciato".

**Tier list generate finora con l'agente (già in storico, DA PUBBLICARE o SCARTARE)**: "Quanto puoi guardare il telefono", "I cattivi che tifiamo davvero".

**Prossimi step possibili dell'agente**: Fase 4 (caption IG/TikTok + titolo YT + commento fissato in un .txt) e MP4 muto da 8s (già progettati, non implementati).

---

### 27.3 ACCREDITO MOSTRA DEL CINEMA DI VENEZIA 2026 (tentativo esplorativo)

**Contesto**: valutata la richiesta di accredito per la 83. Mostra (2-12 settembre 2026). Analisi delle opzioni:
- **Accredito Stampa** (scad. 5 ago): riservato a testate/operatori dell'informazione. Profilo CosaGuardo debole → improbabile.
- **Pass photo** (gratuito): solo area Mostra, no proiezioni, ma dà accesso a photocall/red carpet → **ottimo per contenuti social**. Alternativa consigliata se Marco decide di andarci.
- **Accredito Cinema** (€95 tariffa full, scad. 12 ago): "profilo culturale". Scelto come tentativo. Requisito 2 anni di attività vale solo per la 3ª opzione (piattaforma web); si è puntato sulla 2ª (documentazione attività di progetto culturale).

**Documenti preparati** in `/mnt/user-data/outputs/venezia_accredito/`:
- `CosaGuardo_accredito_cinema_completo.pdf` (3 pagine: lettera su carta intestata + allegato dati) ← **file unico da caricare** (il modulo accetta 1 file)
- Anche separati in docx/pdf per modifiche

**Dati usati**: Marco Fantini, CEO e fondatore, CF FNTMRC84M11B819Q, Via Carso 22 Fabbrico (RE), info@cosaguardo.com. 1 accredito solo per lui.
**Compilazione modulo**: Ruolo = **Blogger**; Tipologia = **Altro** → "Piattaforma editoriale digitale sul cinema e le serie".
**Dati social nell'allegato** (solo visualizzazioni, NO follower per scelta — TikTok nuovo): tabella con i numeri reali (62,5k IG, ecc.), handle @cosaguardoapp (IG/YT) e @cosaguardo.app (TikTok). ⚠️ Dati serie TV riportati onestamente col loro titolo, non camuffati da film.

**⚠️ Il formulario scade dopo 20 minuti** — tenere il PDF pronto prima di iniziare.
**Nota**: esito a insindacabile giudizio della Biennale; progetto recente → aspettative caute. Marco procede senza mail interlocutoria (è un test).

---

### 27.4 CONTENUTI — caption prodotte in sessione
- **Tier list supereroi (TikTok)**: "La mia tier list onesta sui film di supereroi 🦸 Qualcuno si offenderà per la fascia più bassa 😏 #supereroi #tierlist"
- **Carosello "5 film che ti fanno piangere" (TikTok, primo test carosello)**: "5 film che ti faranno piangere 😭 L'ultimo quasi nessuno l'ha visto 💎 #film #filmdavedere" + commento fissato. ⚠️ Su TikTok i caroselli: prima immagine deve reggere da sola; pubblicazione manuale (non programmabili).

---

### 27.5 TODO AGGIORNATA (27/07)
1. ⏳ Esito revisione iOS build 2 (24-48h). Se rifiuto per cancellazione account → serve Mac
2. ⏳ **Android: 14 giorni scadono 28-29/07 → richiedere PRODUZIONE** (imminente!)
3. Carta di credito per rinnovo Apple Developer (scad. 14/07/2027)
4. Agosto: bump target API 36 entro 31/08 (Android)
5. Pubblicare arretrato social (leva principale follower TikTok: 4 post → 2 follower)
6. Tier list: pubblicare o scartare le 2 generate dall'agente
7. Deploy patch affiliati + privacy + fix Prime + TMDb footer (se non già fatto): `/mnt/user-data/outputs/patch_affiliati/` — git add/commit/push
8. Debito tecnico: disclosure nel footer base.html; unificare PLATFORM_MAP/PLATFORM_SLUGS
9. Amazon: policy applicazioni mobili; Tracking ID per fonte
10. Logo TMDb nel footer (attribuzione richiesta dai termini API)
11. Venezia: caricare domanda entro 12/08 (se Marco decide di procedere)

**Fine sezione 27.**

---

## 28. Sistema completo di produzione contenuti (3 formati) + Android + fix reel iOS

### 28.1 iOS — STATO (aggiornamento fine 27/07)
- Build 2 (permessi Info.plist rimossi) **inviata, in revisione**. Attesa esito.
- TestFlight continua a dare "app non disponibile" anche con build 2 — bug lato Apple, NON blocca. Se approvata, l'app si scarica dallo Store.
- ⚠️ Se rifiuto per cancellazione account → serve pulsante in-app (richiede Mac).

### 28.2 Android — IMMINENTE
- 14 giorni di closed test in scadenza **28-29/07** → **richiedere PRODUZIONE** in Play Console (Rilasci → Produzione). È lo sblocco della pubblicazione Android, ferma da settimane.
- Non caricare release fino ad allora. Poi: bump target API 36 entro 31/08.

---

### 28.3 SISTEMA CONTENUTI — 3 FORMATI, stesso schema (NUOVO, tutto in `/mnt/user-data/outputs/agente_tierlist/`)

Cartella unica su Windows: `C:\Users\m.fantini\Desktop\tierlist\`. Tutti gli script condividono:
- **`.env`** con `TMDB_API_KEY` e `ANTHROPIC_API_KEY` (loader comune; path alt. via `set CG_ENV=...`). NON su GitHub.
- **`cache_poster\`** condivisa (un poster scaricato vale per tutti i formati).
- **`fonts\`** con Montserrat. Pesi necessari: ExtraBold, Bold (tier list) + Black, SemiBold, MediumItalic (caroselli) + Medium (reel). Da fonts.google.com, cartella `static/`.
- **`icon-512.png`** (logo) nella cartella.

**Schema comune**: `python genera_X.py` → Claude propone → `[s=va bene · m=modifica mirata · r=rifai da capo · n=annulla]` → a `s` crea gli asset. Ogni formato ha storico separato (no ripetizioni). Modelli: `claude-sonnet-5`, max_token alto (i modelli ragionano prima; il prefill `{` NON è supportato → non usarlo).

**FORMATO 1 — TIER LIST** (`genera.py` + `tierlist.py`)
- 1 immagine 1080×1920, 4 fasce × 4 titoli (16) o × 3 (12). `--titoli 12/16`.
- Preset 12 rispetta zone TikTok (griglia ≤ x=780); preset 16 usa tutta la larghezza (4ª colonna sotto le icone TikTok). Poster 140×210 in entrambi.
- Regole: criterio originale (non bello/brutto), ≥1 collocazione provocatoria, prima fascia = più desiderabile (blu) / ultima = meno (rosso), etichette max 16 caratteri.
- Storico precompilato con 7+ tier list pubblicate.

**FORMATO 2 — CAROSELLO "collezione"** (`genera_carosello.py` + `carosello.py`)
- 7 slide 1080×1920: cover (poster più iconico, sfocato blur 15) + 5 schede (mood + 3 poster + frase) + chiusura CTA.
- Cover: etichetta "15 SERIE/FILM/TITOLI" dedotta dai contenuti. Opzioni `--cover`, `--no-cover-blur`, `--tipo`.
- Sottocategorie = mood/occasioni ("quando sei giù"), NON giudizi. Mono-tipo. Frase descrittiva per scheda (dà carattere).
- Genera anche **`_testo_tiktok.txt`**: titolo TikTok + caption + commento fissato.
- Contenuto slide 2-6 abbassato (parte y≈440) per non finire sotto la barra ricerca TikTok. Niente puntini (TikTok li mette da sé).
- ⚠️ IG: i caroselli vanno come **POST**, non reel. Ma rendono poco su IG (210 view) → formato pensato per TikTok. Su TikTok: pubblicazione manuale.

**FORMATO 3 — REEL "coppie"** (`genera_reel.py` + `reel.py`)
- 7 schermate 1080×1920: hook (2 poster coppia, bordo verde smeraldo) + 5 consigli (poster grande + titolo + anno) + CTA. Il montaggio VIDEO resta in CapCut.
- Concordanza auto: "il quinto" (film) / "la quinta" (serie); etichetta "5 FILM/5 SERIE" dal tipo. Chicca = 5° consiglio.
- **Budget voiceover 130-140 parole ≈ 45-50s**, verificato dalla validazione. Ripartizione: hook ~20, consigli ~17, chicca ~24, CTA ~10.
- **CTA parlata FISSA**: "Ti consiglio altri cinque titoli su cosaguardo punto com." (forzata in reel.py, non modificabile dal JSON).
- File generati: 7 PNG + `_voiceover.txt` (con durate) + **`_voiceover_pulito.txt`** (solo parlato, da incollare in TTS) + `_montaggio.txt` (scaletta) + **`_pubblicazione.txt`** (musica suggerita, titolo YouTube, titolo TikTok, caption, commento).
- Titolo YouTube: apre con parola cercabile + gancio (mai titolo di nicchia in apertura).

**LEZIONI VALIDE PER TUTTI (già nel codice/prompt):**
- Poster sbagliati su titoli generici → campi opzionali `"anno"` e `"tmdb_id"` nel JSON; anno sempre richiesto nel prompt.
- Titoli compositi ("Il Trono di Spade: House of the Dragon") → lo script prova varianti (parti dopo i due punti, poi inglese); il prompt chiede il titolo esatto senza prefissi.
- Emoji: Montserrat non le ha → rese come rettangoli. Rimosse da tutti i testi disegnati (restano ok nelle caption/testi copiabili).
- Se un contenuto generato non viene pubblicato → cancellare la voce dal rispettivo storico (`storico.json`, `storico_caroselli.json`, `storico_reel.json`).

**File di esempio pronti**: `esempio_tierlist.json`, `esempio_carosello.json`, `esempio_reel.json`. Guida completa in `ISTRUZIONI.md`.

⚠️ **Le chiavi API Anthropic esposte in chat in sessioni precedenti vanno revocate su console.anthropic.com.** Nel `.env` solo la chiave nuova.

---

### 28.4 CONTENUTI PRODOTTI IN SESSIONE (caption/titoli)
- Tier list "Saghe e trilogie": caption + titolo TikTok "Le saghe che tutti amano, ordinate senza pietà" (curiosità, non rage-bait: il trigger sta nella caption, non nel titolo).
- Carosello "15 film per ogni tipo di serata" (primo carosello pubblicato, generato dallo strumento).

---

### 28.5 TODO AGGIORNATA (fine 27/07)
1. ⏳ **Android: richiedere PRODUZIONE (28-29/07, imminente)**
2. ⏳ Esito revisione iOS build 2
3. Provare i 3 generatori con contenuti reali; pubblicare l'arretrato (leva #1 per follower TikTok)
4. Carta di credito rinnovo Apple; Venezia entro 12/08 (se Marco procede)
5. Revocare chiavi API esposte
6. Deploy patch affiliati+privacy+Prime+TMDb se non fatto
7. Debito tecnico: disclosure nel footer base.html; unificare PLATFORM_MAP/PLATFORM_SLUGS
8. Bump target API 36 Android entro 31/08

**Fine sezione 28.**

---

## 29. Fix pagine "migliori", testi pubblicazione su tutti i formati, rifiniture layout

### 29.1 FIX PAGINE "/migliori-..." — [PATCH PRONTA, deploy da rifare]
[DOCUMENT] `/mnt/user-data/outputs/patch_migliori/core/recommendation_api.py`

**Problema segnalato da Marco**: nelle classifiche "I migliori {genere}" comparivano titoli fuori categoria (es. The Wolf of Wall Street in "commedia") e l'ordine non era chiaro.
**Diagnosi** (funzione `get_best_content`, core/recommendation_api.py):
1. Ordinamento era `popularity.desc`, NON i voti → la pagina dice "in base a popolarità e voti" ma di fatto ordinava solo per popolarità (per questo un 7.9 stava sopra un 8.3).
2. `with_genres=genre_id` su TMDb significa "ha quel genere TRA i suoi", non "genere principale". Wolf of Wall Street è Crime+Dramma+Commedia → rientrava in commedia. Non è un bug del codice, è il tagging multi-genere di TMDb.

**[DECISION] Marco ha scelto**: filtrare per GENERE PRINCIPALE + mix popolarità/voti.
**[CODE] Patch applicata a get_best_content**:
- Filtro `genre_ids[0] == genre_id` (TMDb ordina i generi per rilevanza → il primo è il principale).
- Fetch fino a 6 pagine (il filtro scarta molto).
- Punteggio = VOTO_W(0.70)·voto_bayesiano_normalizzato + POP_W(0.30)·popolarità_normalizzata. BAYES_M=500 (media bayesiana come IMDb Top 250: impedisce che un titolo con pochi voti altissimi domini). Pesi configurabili in cima alla funzione.

**⚠️ ERRORE COMMESSO E RECUPERATO (importante come lezione)**:
- Primo tentativo: la patch è stata applicata a una versione VECCHIA di recommendation_api.py (pre-patch-Amazon), che NON aveva `get_amazon_bounty_link`.
- Deploy Render FALLITO: `ImportError: cannot import name 'get_amazon_bounty_link'` (main.py lo importa). Render è tornato in auto all'ultima versione buona ("Service recovered").
- Recupero: Marco ha fatto `git checkout HEAD core/recommendation_api.py`, ha rimandato il file CORRENTE completo (3497 righe, con get_amazon_bounty_link + tutte le funzioni Amazon + fix Prime 119). La patch migliori è stata riapplicata su QUEL file, preservando CRLF. Verificato che tutti i nomi importati da main.py siano presenti. File finale 3535 righe.
- **LEZIONE**: prima di patchare recommendation_api.py, verificare SEMPRE che `get_amazon_bounty_link` e le funzioni Amazon ci siano (il file "giusto" è quello con la patch affiliati già dentro).

**[PENDING] Marco deve**: sovrascrivere core/recommendation_api.py con la versione in patch_migliori/, git add/commit/push. Cache TTL 6h. Verificare commedia (Wolf of Wall Street sparito). ⚠️ Se qualche lista /migliori- di genere di nicchia scende sotto 5-6 titoli per via del filtro principale → allentare accettando anche genre_ids[1].

---

### 29.2 TESTI DI PUBBLICAZIONE su TUTTI E TRE i formati (aggiornamento sistema contenuti)

Ora ogni generatore produce, oltre agli asset, il testo pronto da pubblicare:
- **Tier list** (`genera.py`): campi `titolo_tiktok`, `caption`, `commento`, `musica` → salvati in `output/<slug>/<slug>_testo_tiktok.txt`. ⚠️ Ogni tier list ora ha la SUA SOTTOCARTELLA in `output/` (prima erano tutte sciolte insieme).
- **Carosello** (`genera_carosello.py`): `_testo_tiktok.txt` (titolo TikTok + caption + commento).
- **Reel** (`genera_reel.py`): `_pubblicazione.txt` (musica, titolo YouTube, titolo TikTok, caption, commento) + `_voiceover_pulito.txt` (solo parlato per TTS).

Regole comuni codificate nei prompt:
- Titolo TikTok: incuriosisce senza rage-bait, il trigger sta nel contenuto non nel titolo.
- Titolo YouTube (reel): apre con parola cercabile (Film/Serie/titolo famoso) + gancio, mai nicchia in apertura.
- Musica: descrive il REGISTRO non un brano ("trend TikTok ritmato", "orchestrale epico"...); nota che su TikTok l'audio in trend spinge più della musica tematica.
- Emoji ok SOLO nei testi copiabili (caption/commento), MAI nei testi disegnati sulle immagini (Montserrat non le ha → rettangoli).

### 29.3 RIFINITURE LAYOUT (dai test di pubblicazione reali)
- **Carosello**: tagline cover con auto-fit + wrap (prima sforava dai bordi se lunga). Logo abbassato a y=300 su tutte le slide (usciva da sotto la barra ricerca TikTok). Rimosso indice "3/7" (TikTok lo mette da sé, come i puntini già tolti).
- **Reel schermate consiglio**: occhiello "Se ti è piaciuto X+Y" spostato ACCANTO al logo in alto (stessa riga), poster e titolo alzati (poster y=500, titolo finisce ~y=1513). Motivo: lascia ~400px liberi in basso per i sottotitoli del montaggio, che prima finivano troppo in basso/sotto la UI social.

### 29.4 SOCIAL — nota dai commenti
Equivoco ricorrente sotto i post: utenti pensano che CosaGuardo sia "a pagamento". Chiarire sempre con garbo: CosaGuardo è GRATIS, sono le piattaforme (Netflix/Prime) ad avere abbonamenti — vale per qualsiasi guida. Valutare di mettere "gratis" bene in evidenza su sito/bio.

---

### 29.5 TODO AGGIORNATA
1. ⏳ **Android: richiedere PRODUZIONE (28-29/07, SCADE ORA)**
2. ⏳ **Deploy patch migliori** (patch_migliori/core/recommendation_api.py) → git add/commit/push. Verificare che il deploy passi (get_amazon_bounty_link presente) e commedia pulita.
3. ⏳ Esito revisione iOS build 2
4. Provare i 3 generatori con contenuti reali; pubblicare arretrato social
5. Carta di credito rinnovo Apple; Venezia entro 12/08 (se procede)
6. Revocare chiavi API Anthropic esposte in chat
7. Deploy patch affiliati+privacy+Prime+TMDb se non già fatto
8. Debito tecnico: disclosure footer base.html; unificare PLATFORM_MAP/PLATFORM_SLUGS; logo TMDb footer
9. Bump target API 36 Android entro 31/08

**Fine sezione 29.**
