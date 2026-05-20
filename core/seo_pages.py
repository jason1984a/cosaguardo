"""
Gestione delle pagine SEO `/dove-vedere/{slug}`.

Architettura:
- Tabella `seo_titles` in DB: mappa slug → tmdb_id + metadati cached
  Colonne dinamiche aggiunte via _ensure_db migrations:
    - source         : 'evergreen' | 'new_release'  (default 'evergreen')
    - seasons_count  : INTEGER NULL (solo per TV; per detection nuove stagioni)
    - seasons_bumped_at : INTEGER NULL (timestamp ultimo incremento stagioni)
- Funzione `populate_seo_titles_db()`: refresh top evergreen (~700) — diff-only
- Funzione `populate_new_releases()`: aggiunge uscite recenti IT (90gg storia + 60gg futuro), cap 300
- Funzione `weekly_seo_refresh()`: entrypoint del job settimanale (chiamata da scheduler + admin)
- Funzione `get_title_by_slug()`: lookup veloce slug → tmdb_id
- Funzione `slugify()`: converte titolo in slug SEO-friendly
- Funzione `list_seo_titles()`: paginazione per la pagina hub

Strategia anti-spam Google:
- Pagine generate solo per titoli con dati reali completi (poster, trama, anno)
- Cap totale 1000 (700 evergreen + 300 new_release) per evitare red flag thin content
- Refresh settimanale (job + admin manuale), diff-only su updated_at:
  toccato SOLO se i dati cambiano davvero → segnale lastmod sitemap pulito
- Schema.org JSON-LD per rich snippets
"""
import os
import re
import sqlite3
import time
import unicodedata
import logging
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

log = logging.getLogger("cosaguardo")

# DB path: stesso DB utenti (Render persistent disk)
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_DB = os.path.join(_BASE_DIR, "app", "cosaguardo.db")
SEO_DB_PATH = os.environ.get("DATABASE_PATH") or _DEFAULT_DB

# Cap totali (sotto 1000 per evitare flag "thin content scaling")
EVERGREEN_PER_TYPE = 350   # 350 movies + 350 tv = 700 evergreen
NEW_RELEASES_CAP   = 300   # uscite recenti IT (movies+tv mescolati, ranked per release_date desc)
MAX_TOTAL_TARGET   = EVERGREEN_PER_TYPE * 2 + NEW_RELEASES_CAP  # 1000

# Backwards-compat: alias per chi importa le costanti vecchie
MAX_TITLES_TARGET = EVERGREEN_PER_TYPE * 2  # 700
TITLES_PER_TYPE   = EVERGREEN_PER_TYPE      # 350


# ─── SLUGIFY ─────────────────────────────────────────────────────────────
def slugify(text: str, year: Optional[int] = None) -> str:
    """
    Converte un titolo in uno slug SEO-friendly.
    'Il padrino' → 'il-padrino'
    'It (2017)' → 'it-2017' (se year fornito altrimenti 'it')

    Le collisioni (es. due film con lo stesso titolo) vengono risolte
    aggiungendo l'anno automaticamente in populate_seo_titles_db.
    """
    if not text:
        return ""

    # Rimuovi anno tra parentesi alla fine (es "(2017)" o "(1994)")
    # così evitiamo duplicazione di anno se passiamo year esplicitamente
    text = re.sub(r"\s*\(\d{4}\)\s*$", "", text.strip())

    # Rimuovi diacritici (è → e, ñ → n)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))

    # Rimuovi caratteri non alfanumerici (mantiene spazi e -)
    text = re.sub(r"[^\w\s-]", " ", text.lower())

    # Spazi/underscores → singoli trattini
    text = re.sub(r"[\s_]+", "-", text.strip())

    # Trattini multipli → singolo trattino
    text = re.sub(r"-+", "-", text)

    text = text.strip("-")

    if year:
        text = f"{text}-{year}"

    # Limita lunghezza (max 80 char è già abbondante per SEO)
    return text[:80]


# ─── DB MANAGEMENT ───────────────────────────────────────────────────────
def _ensure_db():
    """Crea tabella seo_titles + seo_refresh_log se non esistono, applica migration.

    Migration idempotente: ALTER TABLE ADD COLUMN viene chiamato dentro try/except,
    su SQLite fallisce con OperationalError se la colonna esiste già — ignoriamo.
    """
    conn = sqlite3.connect(SEO_DB_PATH, timeout=5)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS seo_titles (
                slug         TEXT PRIMARY KEY,
                tmdb_id      INTEGER NOT NULL,
                content_type TEXT NOT NULL,
                title        TEXT NOT NULL,
                year         INTEGER,
                popularity   REAL,
                vote_average REAL,
                poster_path  TEXT,
                overview     TEXT,
                updated_at   INTEGER NOT NULL
            )
        """)
        # Migrations soft: ogni ALTER è isolato in try/except (idempotente)
        for ddl in [
            "ALTER TABLE seo_titles ADD COLUMN source TEXT DEFAULT 'evergreen'",
            "ALTER TABLE seo_titles ADD COLUMN seasons_count INTEGER",
            "ALTER TABLE seo_titles ADD COLUMN seasons_bumped_at INTEGER",
            "ALTER TABLE seo_titles ADD COLUMN release_date TEXT",
        ]:
            try:
                conn.execute(ddl)
            except sqlite3.OperationalError:
                pass  # colonna già esistente

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_seo_titles_pop
            ON seo_titles (popularity DESC)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_seo_titles_type_pop
            ON seo_titles (content_type, popularity DESC)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_seo_titles_source
            ON seo_titles (source)
        """)

        # Log delle esecuzioni del refresh (per dashboard admin + check "ultimo run")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS seo_refresh_log (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at          INTEGER NOT NULL,
                finished_at         INTEGER,
                trigger             TEXT,         -- 'scheduler' | 'admin_manual' | 'http_token' | 'startup_catchup'
                evergreen_updated   INTEGER DEFAULT 0,
                evergreen_diff      INTEGER DEFAULT 0,
                evergreen_added     INTEGER DEFAULT 0,
                evergreen_removed   INTEGER DEFAULT 0,
                new_added           INTEGER DEFAULT 0,
                new_promoted        INTEGER DEFAULT 0,
                new_removed         INTEGER DEFAULT 0,
                seasons_detected    INTEGER DEFAULT 0,
                duration_seconds    REAL,
                error               TEXT
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_refresh_log_started
            ON seo_refresh_log (started_at DESC)
        """)

        conn.commit()
    finally:
        conn.close()


def get_title_by_slug(slug: str) -> Optional[dict]:
    """Lookup slug → tmdb_id + metadata. None se slug non trovato."""
    if not slug:
        return None
    _ensure_db()
    try:
        conn = sqlite3.connect(SEO_DB_PATH, timeout=3)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM seo_titles WHERE slug = ? LIMIT 1",
            (slug,)
        )
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception:
        return None


def get_slug_by_tmdb_id(tmdb_id: int, content_type: str = "movie") -> Optional[str]:
    """
    Lookup tmdb_id → slug (None se il titolo non è nel DB SEO).
    Usato per linking interno verso /come/{slug} e /dove-vedere/{slug}
    dalle pagine /film/{id} e /serie/{id}.
    """
    if not tmdb_id:
        return None
    _ensure_db()
    try:
        conn = sqlite3.connect(SEO_DB_PATH, timeout=3)
        cur = conn.execute(
            "SELECT slug FROM seo_titles WHERE tmdb_id = ? AND content_type = ? LIMIT 1",
            (tmdb_id, content_type),
        )
        row = cur.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


def list_seo_titles(content_type: Optional[str] = None,
                    page: int = 1,
                    per_page: int = 50) -> tuple[list[dict], int]:
    """
    Restituisce (lista_titoli, total_count) per la pagina hub paginata.
    Ordina per popularity DESC.
    """
    _ensure_db()
    offset = max(0, (page - 1) * per_page)

    try:
        conn = sqlite3.connect(SEO_DB_PATH, timeout=3)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        if content_type in ("movie", "tv"):
            cur.execute("SELECT COUNT(*) FROM seo_titles WHERE content_type = ?", (content_type,))
            total = cur.fetchone()[0]
            cur.execute("""
                SELECT * FROM seo_titles
                WHERE content_type = ?
                ORDER BY popularity DESC
                LIMIT ? OFFSET ?
            """, (content_type, per_page, offset))
        else:
            cur.execute("SELECT COUNT(*) FROM seo_titles")
            total = cur.fetchone()[0]
            cur.execute("""
                SELECT * FROM seo_titles
                ORDER BY popularity DESC
                LIMIT ? OFFSET ?
            """, (per_page, offset))

        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows, total
    except Exception:
        return [], 0


def list_all_slugs_for_sitemap() -> list[tuple[str, str, int]]:
    """Restituisce [(slug, content_type, updated_at), ...] per tutti i titoli SEO.
    Usato dalla sitemap.xml."""
    _ensure_db()
    try:
        conn = sqlite3.connect(SEO_DB_PATH, timeout=3)
        cur = conn.cursor()
        cur.execute("SELECT slug, content_type, updated_at FROM seo_titles ORDER BY popularity DESC")
        rows = cur.fetchall()
        conn.close()
        return [(r[0], r[1], r[2]) for r in rows]
    except Exception:
        return []


# ─── POPOLAMENTO DA TMDb ─────────────────────────────────────────────────
def _fetch_tmdb_page(endpoint: str, page: int) -> list:
    """Fetch una pagina TMDb. Ritorna la lista 'results' o []."""
    import requests
    api_key = os.environ.get("TMDB_API_KEY", "")
    if not api_key:
        return []
    try:
        r = requests.get(
            f"https://api.themoviedb.org/3{endpoint}",
            params={"api_key": api_key, "language": "it-IT", "page": page},
            timeout=8
        )
        if r.status_code != 200:
            return []
        return r.json().get("results", [])
    except Exception:
        return []


def populate_seo_titles_db(_log_ctx: Optional[dict] = None) -> dict:
    """
    Refresh evergreen: pesca i top titoli da TMDb (popular + top_rated) e popola/aggiorna seo_titles.

    DIFF-ONLY su `updated_at`: il timestamp viene aggiornato SOLO se cambia
    almeno un campo significativo (popularity, vote_average, poster_path, overview,
    seasons_count per TV). Questo è cruciale per la freshness SEO: la sitemap
    usa updated_at come <lastmod> e Google penalizza i "refresh fake".

    Ritorna stats {"movies": N, "tv": M, "total": T, "updated": U, "added": A,
                   "removed": R, "seasons_detected": S}.

    Strategia: 20 pagine x 20 = 400 per movie + 400 per tv → top EVERGREEN_PER_TYPE
    (350 ognuno per lasciar spazio ai 300 new_release nel cap totale 1000).
    """
    _ensure_db()

    movie_sources = [
        ("/movie/popular",    list(range(1, 11))),
        ("/movie/top_rated",  list(range(1, 11))),
    ]
    tv_sources = [
        ("/tv/popular",       list(range(1, 11))),
        ("/tv/top_rated",     list(range(1, 11))),
    ]

    # Fetch movies in parallel
    movies_by_id = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = []
        for endpoint, pages in movie_sources:
            for p in pages:
                futures.append(ex.submit(_fetch_tmdb_page, endpoint, p))
        for fut in futures:
            for item in fut.result():
                if not item.get("id") or not item.get("title"):
                    continue
                if not item.get("poster_path"):
                    continue
                movies_by_id[item["id"]] = item

    # Fetch tv in parallel
    tv_by_id = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = []
        for endpoint, pages in tv_sources:
            for p in pages:
                futures.append(ex.submit(_fetch_tmdb_page, endpoint, p))
        for fut in futures:
            for item in fut.result():
                if not item.get("id") or not item.get("name"):
                    continue
                if not item.get("poster_path"):
                    continue
                tv_by_id[item["id"]] = item

    # Tieni solo i top per popularity
    movies_sorted = sorted(movies_by_id.values(),
                           key=lambda x: x.get("popularity", 0), reverse=True)[:EVERGREEN_PER_TYPE]
    tv_sorted = sorted(tv_by_id.values(),
                       key=lambda x: x.get("popularity", 0), reverse=True)[:EVERGREEN_PER_TYPE]

    now = int(time.time())
    conn = sqlite3.connect(SEO_DB_PATH, timeout=10)
    try:
        cur = conn.cursor()

        # Carica righe esistenti per fare diff-only
        # (chiave: tmdb_id + content_type — più stabile dello slug)
        cur.execute("""
            SELECT slug, tmdb_id, content_type, title, year, popularity, vote_average,
                   poster_path, overview, source, seasons_count
            FROM seo_titles
        """)
        existing_by_key = {}
        existing_slugs_by_key = {}
        for r in cur.fetchall():
            key = (r[1], r[2])  # (tmdb_id, content_type)
            existing_by_key[key] = {
                "slug": r[0], "title": r[3], "year": r[4],
                "popularity": r[5], "vote_average": r[6],
                "poster_path": r[7], "overview": r[8],
                "source": r[9], "seasons_count": r[10],
            }
            existing_slugs_by_key[key] = r[0]

        used_slugs = set()
        seen_evergreen_keys = set()  # le chiavi che vediamo in questo run come evergreen
        counters = {
            "movies": 0, "tv": 0,
            "updated": 0, "diff_real": 0,
            "added": 0,
        }

        def _insert_or_update(item, content_type):
            title = item.get("title") if content_type == "movie" else item.get("name")
            date_field = item.get("release_date") if content_type == "movie" else item.get("first_air_date")
            year = None
            if date_field and len(date_field) >= 4:
                try:
                    year = int(date_field[:4])
                except ValueError:
                    year = None

            key = (item["id"], content_type)
            seen_evergreen_keys.add(key)
            existing = existing_by_key.get(key)

            # Mantieni lo slug esistente per continuità SEO (rinomini = URL nuovo = SEO perso)
            if existing:
                slug = existing["slug"]
            else:
                slug = slugify(title)
                if not slug:
                    return
                if slug in used_slugs and year:
                    slug = slugify(title, year)
                if slug in used_slugs:
                    slug = f"{slug}-{item['id']}"

            used_slugs.add(slug)

            new_popularity   = item.get("popularity", 0) or 0
            new_vote_average = item.get("vote_average", 0) or 0
            new_poster_path  = item.get("poster_path")
            new_overview     = (item.get("overview") or "")[:500]

            if existing:
                # DIFF: confronta SOLO i campi significativi. Cambi piccoli su
                # popularity (< 0.5) sono noise di TMDb — non li consideriamo "diff vera".
                pop_changed = abs((existing["popularity"] or 0) - new_popularity) >= 0.5
                vote_changed = abs((existing["vote_average"] or 0) - new_vote_average) >= 0.1
                poster_changed = (existing["poster_path"] or "") != (new_poster_path or "")
                overview_changed = (existing["overview"] or "") != new_overview

                is_diff = pop_changed or vote_changed or poster_changed or overview_changed
                new_updated_at = now if is_diff else None

                if is_diff:
                    counters["diff_real"] += 1
                    cur.execute("""
                        UPDATE seo_titles
                        SET title=?, year=?, popularity=?, vote_average=?,
                            poster_path=?, overview=?, source='evergreen',
                            release_date=?, updated_at=?
                        WHERE slug=?
                    """, (title, year, new_popularity, new_vote_average,
                          new_poster_path, new_overview, date_field, new_updated_at, slug))
                else:
                    # Aggiorna popularity/vote silenziosamente (per ranking interno) ma NON
                    # tocchiamo updated_at. source='evergreen' (rinforzo, magari era new_release prima).
                    cur.execute("""
                        UPDATE seo_titles
                        SET popularity=?, vote_average=?, source='evergreen',
                            release_date=COALESCE(release_date, ?)
                        WHERE slug=?
                    """, (new_popularity, new_vote_average, date_field, slug))
                counters["updated"] += 1
            else:
                # Nuovo titolo evergreen
                cur.execute("""
                    INSERT INTO seo_titles
                    (slug, tmdb_id, content_type, title, year, popularity, vote_average,
                     poster_path, overview, source, release_date, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'evergreen', ?, ?)
                """, (slug, item["id"], content_type, title, year, new_popularity,
                      new_vote_average, new_poster_path, new_overview, date_field, now))
                counters["added"] += 1

            if content_type == "movie":
                counters["movies"] += 1
            else:
                counters["tv"] += 1

        for m in movies_sorted:
            _insert_or_update(m, "movie")
        for t in tv_sorted:
            _insert_or_update(t, "tv")

        # Pulizia stale evergreen: titoli che NON sono più nella top e
        # NON sono di source='new_release' (i new_release li gestisce populate_new_releases).
        # Safety: cancella solo se l'aggiornamento ha avuto successo significativo (>= 100).
        removed = 0
        if counters["movies"] + counters["tv"] >= 100:
            # Trova evergreen non visti in questo run
            cur.execute("""
                SELECT slug, tmdb_id, content_type FROM seo_titles
                WHERE source = 'evergreen' OR source IS NULL
            """)
            stale_slugs = []
            for r in cur.fetchall():
                if (r[1], r[2]) not in seen_evergreen_keys:
                    stale_slugs.append(r[0])
            if stale_slugs:
                cur.executemany(
                    "DELETE FROM seo_titles WHERE slug = ?",
                    [(s,) for s in stale_slugs]
                )
                removed = len(stale_slugs)

        conn.commit()
    finally:
        conn.close()

    result = {
        "movies": counters["movies"],
        "tv": counters["tv"],
        "total": counters["movies"] + counters["tv"],
        "updated": counters["updated"],
        "diff_real": counters["diff_real"],
        "added": counters["added"],
        "removed_stale": removed,
    }

    # Se chiamato dal weekly_seo_refresh, popola il context per il log riga
    if _log_ctx is not None:
        _log_ctx["evergreen_updated"] = counters["updated"]
        _log_ctx["evergreen_diff"]    = counters["diff_real"]
        _log_ctx["evergreen_added"]   = counters["added"]
        _log_ctx["evergreen_removed"] = removed

    return result


def seo_titles_count() -> int:
    """Quanti titoli SEO ci sono in DB ora."""
    _ensure_db()
    try:
        conn = sqlite3.connect(SEO_DB_PATH, timeout=3)
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM seo_titles")
            n = cur.fetchone()[0]
            return n
        finally:
            conn.close()
    except Exception:
        return 0


# ─── NEW RELEASES (Modulo 3) ──────────────────────────────────────────────
def populate_new_releases(_log_ctx: Optional[dict] = None) -> dict:
    """
    Aggiunge in seo_titles le uscite recenti italiane: ultimi 90gg + prossimi 60gg.

    Endpoint TMDb:
    - /movie/now_playing?region=IT  → in sala Italia (ultimi ~30-45 gg)
    - /movie/upcoming?region=IT     → prossimamente Italia (prossimi ~30-60 gg)
    - /tv/airing_today              → episodi che vanno in onda oggi
    - /tv/on_the_air                → serie con episodi nelle prossime 7gg

    Strategia:
    - Cap totale NEW_RELEASES_CAP (300). Se ne arrivano di più, taglia i più vecchi.
    - source='new_release' per distinguerli dagli evergreen.
    - Auto-promozione: se la entry diventa molto popular (top EVERGREEN_PER_TYPE)
      verrà ri-assorbita dal prossimo refresh evergreen che la marcherà come 'evergreen'.
    - Auto-decadimento gestito da _cleanup_old_new_releases() (>180gg dopo release_date,
      se non promossi).

    Ritorna stats {"new_added": N, "new_promoted": P, "new_removed": R}.
    """
    _ensure_db()
    import requests
    api_key = os.environ.get("TMDB_API_KEY", "")
    if not api_key:
        if _log_ctx is not None:
            _log_ctx["new_added"] = 0
            _log_ctx["new_promoted"] = 0
            _log_ctx["new_removed"] = 0
        return {"new_added": 0, "new_promoted": 0, "new_removed": 0, "error": "no_api_key"}

    # Endpoint da scrapare per new releases (region=IT dove rilevante)
    sources = [
        ("/movie/now_playing", "movie", "release_date",    {"region": "IT", "page": 1}),
        ("/movie/now_playing", "movie", "release_date",    {"region": "IT", "page": 2}),
        ("/movie/upcoming",    "movie", "release_date",    {"region": "IT", "page": 1}),
        ("/movie/upcoming",    "movie", "release_date",    {"region": "IT", "page": 2}),
        ("/tv/airing_today",   "tv",    "first_air_date",  {"page": 1}),
        ("/tv/on_the_air",     "tv",    "first_air_date",  {"page": 1}),
        ("/tv/on_the_air",     "tv",    "first_air_date",  {"page": 2}),
    ]

    candidates = []  # list of (item_dict, content_type, release_date_str)

    def _fetch(endpoint, content_type, date_field, params):
        try:
            r = requests.get(
                f"https://api.themoviedb.org/3{endpoint}",
                params={"api_key": api_key, "language": "it-IT", **params},
                timeout=8
            )
            if r.status_code != 200:
                return []
            results = r.json().get("results", [])
            out = []
            for item in results:
                title = item.get("title") if content_type == "movie" else item.get("name")
                if not title or not item.get("id") or not item.get("poster_path"):
                    continue
                rd = item.get(date_field)
                if not rd:
                    continue
                out.append((item, content_type, rd))
            return out
        except Exception as e:
            log.warning("populate_new_releases: fetch %s failed: %s", endpoint, e)
            return []

    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = [ex.submit(_fetch, ep, ct, df, p) for ep, ct, df, p in sources]
        for fut in futures:
            candidates.extend(fut.result())

    # Dedup per (tmdb_id, content_type) — alcuni titoli appaiono in più endpoint
    dedup = {}
    for item, ct, rd in candidates:
        key = (item["id"], ct)
        # Prefer la entry con popularity più alta se duplicata
        if key not in dedup or (item.get("popularity", 0) or 0) > (dedup[key][0].get("popularity", 0) or 0):
            dedup[key] = (item, ct, rd)

    # Filtra finestra temporale: -90gg ÷ +60gg da oggi
    from datetime import datetime, timedelta
    today = datetime.utcnow().date()
    lower_bound = today - timedelta(days=90)
    upper_bound = today + timedelta(days=60)

    in_window = []
    for item, ct, rd in dedup.values():
        try:
            rd_date = datetime.strptime(rd[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            continue
        if lower_bound <= rd_date <= upper_bound:
            in_window.append((item, ct, rd, rd_date))

    # Ordina per release_date DESC (più recenti prima) e taglia al cap
    in_window.sort(key=lambda x: x[3], reverse=True)
    in_window = in_window[:NEW_RELEASES_CAP]

    # Inserisci/aggiorna come source='new_release'
    now = int(time.time())
    new_added = 0
    new_promoted = 0

    conn = sqlite3.connect(SEO_DB_PATH, timeout=10)
    try:
        cur = conn.cursor()

        # Pre-carica slug esistenti per evitare collisioni
        cur.execute("SELECT slug, tmdb_id, content_type, source FROM seo_titles")
        existing = {}
        used_slugs_db = set()
        for r in cur.fetchall():
            existing[(r[1], r[2])] = {"slug": r[0], "source": r[3]}
            used_slugs_db.add(r[0])

        for item, ct, rd, rd_date in in_window:
            title = item.get("title") if ct == "movie" else item.get("name")
            year = rd_date.year
            key = (item["id"], ct)

            if key in existing:
                # Già in DB: se era evergreen, NON degradare a new_release (manteniamo evergreen).
                # Se era new_release, aggiorna i campi mobili (popularity/vote/poster/overview).
                if existing[key]["source"] in (None, "evergreen"):
                    new_promoted += 1  # contiamo come "già promosso/stabile"
                    continue
                # Refresh i dati ma diff-only: se cambia qualcosa updated_at = now
                cur.execute("""
                    SELECT popularity, vote_average, poster_path, overview
                    FROM seo_titles WHERE slug = ?
                """, (existing[key]["slug"],))
                old = cur.fetchone()
                new_pop = item.get("popularity", 0) or 0
                new_vote = item.get("vote_average", 0) or 0
                new_poster = item.get("poster_path")
                new_overview = (item.get("overview") or "")[:500]
                is_diff = (
                    abs((old[0] or 0) - new_pop) >= 0.5 or
                    abs((old[1] or 0) - new_vote) >= 0.1 or
                    (old[2] or "") != (new_poster or "") or
                    (old[3] or "") != new_overview
                )
                if is_diff:
                    cur.execute("""
                        UPDATE seo_titles
                        SET popularity=?, vote_average=?, poster_path=?,
                            overview=?, release_date=?, updated_at=?
                        WHERE slug=?
                    """, (new_pop, new_vote, new_poster, new_overview, rd, now, existing[key]["slug"]))
                else:
                    cur.execute("""
                        UPDATE seo_titles SET popularity=?, vote_average=?
                        WHERE slug=?
                    """, (new_pop, new_vote, existing[key]["slug"]))
                continue

            # Nuovo: genera slug
            slug = slugify(title)
            if not slug:
                continue
            if slug in used_slugs_db and year:
                slug = slugify(title, year)
            if slug in used_slugs_db:
                slug = f"{slug}-{item['id']}"
            used_slugs_db.add(slug)

            cur.execute("""
                INSERT INTO seo_titles
                (slug, tmdb_id, content_type, title, year, popularity, vote_average,
                 poster_path, overview, source, release_date, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'new_release', ?, ?)
            """, (slug, item["id"], ct, title, year,
                  item.get("popularity", 0) or 0,
                  item.get("vote_average", 0) or 0,
                  item.get("poster_path"),
                  (item.get("overview") or "")[:500],
                  rd, now))
            new_added += 1

        # Cleanup: cancella new_release vecchi (>180gg da release_date) E non promossi
        cur.execute("""
            DELETE FROM seo_titles
            WHERE source = 'new_release'
              AND release_date IS NOT NULL
              AND date(release_date) < date('now', '-180 days')
        """)
        new_removed = cur.rowcount or 0

        conn.commit()
    finally:
        conn.close()

    if _log_ctx is not None:
        _log_ctx["new_added"]    = new_added
        _log_ctx["new_promoted"] = new_promoted
        _log_ctx["new_removed"]  = new_removed

    return {
        "new_added": new_added,
        "new_promoted": new_promoted,
        "new_removed": new_removed,
    }


# ─── DETECTION NUOVE STAGIONI (Modulo 4) ──────────────────────────────────
def detect_new_seasons(_log_ctx: Optional[dict] = None) -> dict:
    """
    Per ogni serie TV in seo_titles fetcha TMDb /tv/{id} e legge number_of_seasons.
    Se incrementato rispetto a seasons_count salvato → marca con seasons_bumped_at=now
    e aggiorna updated_at=now (freshness vera: la pagina mostrerà banner "Nuova stagione").

    Per ridurre carico TMDb: limita a 200 serie per esecuzione, prioritizzando
    quelle con seasons_count NULL (mai checkate) e poi quelle più popolari.

    Ritorna {"checked": N, "bumped": M}.
    """
    _ensure_db()
    import requests
    api_key = os.environ.get("TMDB_API_KEY", "")
    if not api_key:
        if _log_ctx is not None:
            _log_ctx["seasons_detected"] = 0
        return {"checked": 0, "bumped": 0, "error": "no_api_key"}

    conn = sqlite3.connect(SEO_DB_PATH, timeout=10)
    try:
        cur = conn.cursor()
        # Seleziona TV: prima i NULL (mai checkate), poi ordina per popularity
        cur.execute("""
            SELECT slug, tmdb_id, seasons_count
            FROM seo_titles
            WHERE content_type = 'tv'
            ORDER BY (seasons_count IS NOT NULL) ASC, popularity DESC
            LIMIT 200
        """)
        tv_rows = cur.fetchall()
    finally:
        conn.close()

    def _fetch_seasons(tmdb_id):
        try:
            r = requests.get(
                f"https://api.themoviedb.org/3/tv/{tmdb_id}",
                params={"api_key": api_key, "language": "it-IT"},
                timeout=6
            )
            if r.status_code != 200:
                return None
            return r.json().get("number_of_seasons")
        except Exception:
            return None

    now = int(time.time())
    bumped = 0
    checked = 0

    # Fetch in parallel ma write seriale (SQLite single-writer)
    updates_to_write = []  # (slug, new_count, is_bump)
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(_fetch_seasons, row[1]): row for row in tv_rows}
        for fut, row in futures.items():
            slug, tmdb_id, old_count = row
            new_count = fut.result()
            checked += 1
            if new_count is None:
                continue
            if old_count is None:
                # Prima volta: salva il count senza bump (baseline)
                updates_to_write.append((slug, new_count, False))
            elif new_count > old_count:
                # BUMP vero
                updates_to_write.append((slug, new_count, True))
                bumped += 1
            # else: count uguale o diminuito (raro), no-op

    if updates_to_write:
        conn = sqlite3.connect(SEO_DB_PATH, timeout=10)
        try:
            cur = conn.cursor()
            for slug, count, is_bump in updates_to_write:
                if is_bump:
                    cur.execute("""
                        UPDATE seo_titles
                        SET seasons_count=?, seasons_bumped_at=?, updated_at=?
                        WHERE slug=?
                    """, (count, now, now, slug))
                else:
                    # Baseline: NON tocchiamo updated_at (non è una "freshness vera",
                    # è solo che prima non sapevamo).
                    cur.execute("""
                        UPDATE seo_titles SET seasons_count=?
                        WHERE slug=?
                    """, (count, slug))
            conn.commit()
        finally:
            conn.close()

    if _log_ctx is not None:
        _log_ctx["seasons_detected"] = bumped

    return {"checked": checked, "bumped": bumped}


# ─── WEEKLY ORCHESTRATOR + LOG ────────────────────────────────────────────
def weekly_seo_refresh(trigger: str = "scheduler") -> dict:
    """
    Esegue tutti i moduli del refresh settimanale in ordine e logga in seo_refresh_log.
    trigger: 'scheduler' | 'admin_manual' | 'http_token' | 'startup_catchup'

    Ordine:
    1. populate_seo_titles_db() — refresh evergreen, diff-only updated_at
    2. populate_new_releases()  — aggiunge uscite IT recenti, cleanup vecchie
    3. detect_new_seasons()     — check nuove stagioni serie TV

    Cattura errori in modo soft: ogni modulo è isolato in try/except, fallisce uno
    gli altri continuano. Errori loggati nella riga seo_refresh_log.

    Ritorna dict con stats aggregati.
    """
    _ensure_db()
    started_at = int(time.time())
    log_ctx = {
        "evergreen_updated": 0, "evergreen_diff": 0,
        "evergreen_added": 0, "evergreen_removed": 0,
        "new_added": 0, "new_promoted": 0, "new_removed": 0,
        "seasons_detected": 0,
    }
    errors = []

    # Crea riga log (started)
    conn = sqlite3.connect(SEO_DB_PATH, timeout=5)
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO seo_refresh_log (started_at, trigger)
            VALUES (?, ?)
        """, (started_at, trigger))
        log_id = cur.lastrowid
        conn.commit()
    finally:
        conn.close()

    # Modulo 2: evergreen refresh
    try:
        log.info("weekly_seo_refresh: avvio populate_seo_titles_db (trigger=%s)", trigger)
        populate_seo_titles_db(_log_ctx=log_ctx)
    except Exception as e:
        log.exception("weekly_seo_refresh: errore in populate_seo_titles_db")
        errors.append(f"evergreen: {e}")

    # Modulo 3: new releases
    try:
        log.info("weekly_seo_refresh: avvio populate_new_releases")
        populate_new_releases(_log_ctx=log_ctx)
    except Exception as e:
        log.exception("weekly_seo_refresh: errore in populate_new_releases")
        errors.append(f"new_releases: {e}")

    # Modulo 4: detect nuove stagioni
    try:
        log.info("weekly_seo_refresh: avvio detect_new_seasons")
        detect_new_seasons(_log_ctx=log_ctx)
    except Exception as e:
        log.exception("weekly_seo_refresh: errore in detect_new_seasons")
        errors.append(f"seasons: {e}")

    finished_at = int(time.time())
    duration = finished_at - started_at

    # Chiudi riga log con stats finali
    conn = sqlite3.connect(SEO_DB_PATH, timeout=5)
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE seo_refresh_log
            SET finished_at=?,
                evergreen_updated=?, evergreen_diff=?,
                evergreen_added=?, evergreen_removed=?,
                new_added=?, new_promoted=?, new_removed=?,
                seasons_detected=?,
                duration_seconds=?, error=?
            WHERE id=?
        """, (
            finished_at,
            log_ctx["evergreen_updated"], log_ctx["evergreen_diff"],
            log_ctx["evergreen_added"], log_ctx["evergreen_removed"],
            log_ctx["new_added"], log_ctx["new_promoted"], log_ctx["new_removed"],
            log_ctx["seasons_detected"],
            duration, ("; ".join(errors) if errors else None),
            log_id
        ))
        conn.commit()
    finally:
        conn.close()

    log.info("weekly_seo_refresh: done in %.1fs — %s", duration, log_ctx)

    return {
        **log_ctx,
        "duration_seconds": duration,
        "trigger": trigger,
        "errors": errors,
        "ok": not errors,
    }


def get_last_refresh_info() -> Optional[dict]:
    """Ultima riga di seo_refresh_log (finished o in corso). Per dashboard admin."""
    _ensure_db()
    try:
        conn = sqlite3.connect(SEO_DB_PATH, timeout=3)
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("""
                SELECT * FROM seo_refresh_log
                ORDER BY started_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    except Exception:
        return None


def list_recent_refresh_log(limit: int = 10) -> list[dict]:
    """Ultime N righe di seo_refresh_log per dashboard admin."""
    _ensure_db()
    try:
        conn = sqlite3.connect(SEO_DB_PATH, timeout=3)
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("""
                SELECT * FROM seo_refresh_log
                ORDER BY started_at DESC
                LIMIT ?
            """, (limit,))
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception:
        return []


def get_seo_stats() -> dict:
    """Stats aggregate per dashboard admin: count totale, breakdown source/type."""
    _ensure_db()
    try:
        conn = sqlite3.connect(SEO_DB_PATH, timeout=3)
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM seo_titles")
            total = cur.fetchone()[0]
            cur.execute("SELECT content_type, COUNT(*) FROM seo_titles GROUP BY content_type")
            by_type = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("""
                SELECT COALESCE(source, 'evergreen'), COUNT(*)
                FROM seo_titles GROUP BY COALESCE(source, 'evergreen')
            """)
            by_source = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("""
                SELECT COUNT(*) FROM seo_titles
                WHERE seasons_bumped_at IS NOT NULL
                  AND seasons_bumped_at > strftime('%s', 'now', '-30 days')
            """)
            recent_season_bumps = cur.fetchone()[0]
            return {
                "total": total,
                "by_type": by_type,
                "by_source": by_source,
                "recent_season_bumps": recent_season_bumps,
                "cap_total": MAX_TOTAL_TARGET,
                "cap_evergreen_per_type": EVERGREEN_PER_TYPE,
                "cap_new_releases": NEW_RELEASES_CAP,
            }
        finally:
            conn.close()
    except Exception as e:
        return {"error": str(e)}


def get_seasons_bump_info(slug: str) -> Optional[dict]:
    """Per il template dove_vedere.html: se il titolo ha avuto un season bump recente
    (ultimi 60 giorni), restituisce {"seasons_count": N, "bumped_at": ts} per
    mostrare il banner "🆕 Stagione N disponibile". None altrimenti.
    """
    if not slug:
        return None
    try:
        conn = sqlite3.connect(SEO_DB_PATH, timeout=2)
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT seasons_count, seasons_bumped_at FROM seo_titles
                WHERE slug = ? AND content_type = 'tv'
                  AND seasons_bumped_at IS NOT NULL
                  AND seasons_bumped_at > strftime('%s', 'now', '-60 days')
            """, (slug,))
            row = cur.fetchone()
            if not row:
                return None
            return {"seasons_count": row[0], "bumped_at": row[1]}
        finally:
            conn.close()
    except Exception:
        return None


# ─── SIMILAR-FOR-SEO ──────────────────────────────────────────────────────
# Per le pagine /come/{slug}: data una slug nel DB seo_titles, chiama
# l'algoritmo di raccomandazione e restituisce 12 titoli simili.
# Risultato cachato 7gg per non ricalcolare ogni hit (le similar non cambiano spesso).

def get_similar_for_seo(slug: str, limit: int = 12) -> list[dict]:
    """
    Restituisce N titoli simili per una pagina /come/{slug}.
    Ogni risultato include slug per linking interno SEO.

    Cache: 24h memoria + 7gg DB (i simili sono quasi statici).
    """
    item = get_title_by_slug(slug)
    if not item:
        return []

    from core.tmdb_cache import cached_call
    cache_key = f"seo:similar:v5:{item['content_type']}:{item['tmdb_id']}:{limit}"

    return cached_call(
        cache_key,
        lambda: _compute_similar_for_seo(item, limit)
    ) or []


def _compute_similar_for_seo(item: dict, limit: int = 12) -> list[dict]:
    """
    Usa l'algoritmo motore esistente per trovare i simili,
    poi arricchisce ogni risultato con poster, anno, e slug SEO.

    Strategia robusta:
    - Se il motore restituisce rec con poster_path mancanti, faccio lookup TMDb
      via find_tv_by_title o get_movie_tmdb_info.
    - Logging diagnostico (visibile su Render Logs) per capire cosa va storto.
    """
    title = item.get("title")
    content_type = item.get("content_type", "movie")
    tmdb_id_seed = item.get("tmdb_id")

    if not title:
        return []

    raw_similar = []

    if content_type == "tv":
        try:
            from core.recommendation_tv import recommend_tv_from_seed_titles
            result = recommend_tv_from_seed_titles([title], top_k=limit + 5)
            raw_similar = result.get("recommendations", []) or []
            print(f"[seo /come] tv '{title}' (id={tmdb_id_seed}): "
                  f"motore ha restituito {len(raw_similar)} candidati")
        except Exception as e:
            print(f"[seo /come] ERROR recommend_tv per '{title}': {e}")
            return []
    else:
        try:
            from core.recommendation_api import recommend_from_seed_titles
            result = recommend_from_seed_titles([title], top_k=limit + 5)
            raw_similar = result.get("recommendations", []) or []
            print(f"[seo /come] movie '{title}' (id={tmdb_id_seed}): "
                  f"motore ha restituito {len(raw_similar)} candidati")
        except Exception as e:
            print(f"[seo /come] ERROR recommend_movie per '{title}': {e}")
            return []

    # NOTA: NON facciamo return early se raw_similar è vuoto.
    # Il motore CosaGuardo con singolo seed può restituire 0 candidati,
    # ma il fallback TMDb (Fase 4) può comunque produrre risultati validi.
    # Quindi continuiamo il flusso anche con raw_similar vuoto.

    # ── FASE 1: arricchimento poster + tmdb_id (in parallelo) ────────
    from concurrent.futures import ThreadPoolExecutor

    if content_type == "movie":
        from core.recommendation_api import get_movie_tmdb_info

        def _resolve_movie(rec):
            t = rec.get("title", "")
            if not t:
                return rec
            tmdb_info = get_movie_tmdb_info(t)
            if tmdb_info:
                rec["_tmdb_id"]    = tmdb_info.get("tmdb_id")
                rec["_poster_url"] = tmdb_info.get("poster_url")
                rec["_year"]       = tmdb_info.get("year")
            return rec

        with ThreadPoolExecutor(max_workers=8) as ex:
            raw_similar = list(ex.map(_resolve_movie, raw_similar))

    else:  # tv
        # Le rec TV potrebbero avere poster_path mancante per alcuni titoli.
        # Se manca, faccio fallback con find_tv_by_title (cached).
        from core.recommendation_tv import find_tv_by_title

        def _resolve_tv(rec):
            # Se ha già tv_id e poster_path, non serve nulla
            if rec.get("tv_id") and rec.get("poster_path"):
                return rec
            # Altrimenti, fallback lookup
            t = rec.get("title", "")
            if not t:
                return rec
            tv_info = find_tv_by_title(t)
            if tv_info:
                if not rec.get("tv_id"):
                    rec["tv_id"] = tv_info.get("tv_id")
                if not rec.get("poster_path"):
                    rec["poster_path"] = tv_info.get("poster_path")
            return rec

        with ThreadPoolExecutor(max_workers=8) as ex:
            raw_similar = list(ex.map(_resolve_tv, raw_similar))

    # ── FASE 2: lookup batch slug SEO (per internal linking) ─────────
    ids_in_results = []
    for r in raw_similar:
        tid = (r.get("_tmdb_id") or r.get("tmdb_id")
               or r.get("tv_id") or r.get("movie_id"))
        if tid:
            ids_in_results.append(tid)

    slug_map = {}
    if ids_in_results:
        _ensure_db()
        try:
            conn = sqlite3.connect(SEO_DB_PATH, timeout=3)
            placeholders = ",".join("?" * len(ids_in_results))
            cur = conn.execute(
                f"SELECT tmdb_id, slug, year FROM seo_titles "
                f"WHERE tmdb_id IN ({placeholders}) AND content_type = ?",
                ids_in_results + [content_type]
            )
            for row in cur.fetchall():
                slug_map[row[0]] = {"slug": row[1], "year": row[2]}
            conn.close()
        except Exception:
            pass

    # ── FASE 3: costruzione output finale ────────────────────────────
    enriched = []
    skipped_no_id = 0
    skipped_no_poster = 0

    for r in raw_similar:
        tid = (r.get("_tmdb_id") or r.get("tmdb_id")
               or r.get("tv_id") or r.get("movie_id"))

        if not tid:
            skipped_no_id += 1
            continue

        slug_info = slug_map.get(tid, {})

        # Poster: per film è già URL completo dal lookup, per TV serve costruire
        poster_url = r.get("_poster_url")  # film
        if not poster_url and r.get("poster_path"):
            poster_url = f"https://image.tmdb.org/t/p/w300{r['poster_path']}"

        if not poster_url:
            skipped_no_poster += 1
            continue

        year = (slug_info.get("year")
                or r.get("_year")
                or _extract_year(r.get("release_date")
                                 or r.get("first_air_date") or ""))

        enriched.append({
            "tmdb_id":      tid,
            "title":        r.get("title", ""),
            "poster_url":   poster_url,
            "overview":     (r.get("overview") or "")[:200],
            "vote_average": r.get("vote_average") or r.get("score") or 0,
            "year":         year,
            "slug":         slug_info.get("slug"),  # None se non in DB SEO
            "content_type": content_type,
            "reason":       r.get("reason") or r.get("explanation") or "",
        })

        if len(enriched) >= limit:
            break

    # ── FASE 4: fallback TMDb se l'algoritmo ha prodotto pochi risultati ──
    # Il motore CosaGuardo è ottimizzato per multi-seed (3+ titoli).
    # Con singolo seed (caso /come/X) può restituire pochi candidati.
    # In quei casi usiamo TMDb similar+recommended come complemento.
    if len(enriched) < limit and tmdb_id_seed:
        try:
            existing_ids = {e["tmdb_id"] for e in enriched}
            extra_recs = []
            tmdb_similar = []

            if content_type == "tv":
                from core.recommendation_tv import get_similar_tv, get_recommended_tv
                rec_tv = get_recommended_tv(tmdb_id_seed, limit=20) or []
                sim_tv = get_similar_tv(tmdb_id_seed, limit=20) or []
                tmdb_similar = rec_tv + sim_tv
                print(f"[seo /come] fallback TV pool: {len(rec_tv)} recommended + "
                      f"{len(sim_tv)} similar = {len(tmdb_similar)} candidati totali")
            else:
                from core.recommendation_api import get_similar_movies_tmdb
                tmdb_similar = get_similar_movies_tmdb(tmdb_id_seed, limit=30) or []
                print(f"[seo /come] fallback movie pool: {len(tmdb_similar)} candidati")

            # Dedup vs gli enriched già presenti
            seen_ids = set(existing_ids)
            skip_no_id = skip_dup = skip_no_poster = 0

            for r in tmdb_similar:
                tid = r.get("tv_id") or r.get("tmdb_id") or r.get("movie_id")
                if not tid:
                    skip_no_id += 1
                    continue
                if tid in seen_ids:
                    skip_dup += 1
                    continue
                seen_ids.add(tid)

                # Poster
                poster_url = r.get("poster_url")
                if not poster_url and r.get("poster_path"):
                    poster_url = f"https://image.tmdb.org/t/p/w300{r['poster_path']}"
                if not poster_url:
                    skip_no_poster += 1
                    continue

                # Slug SEO se disponibile
                slug_info = slug_map.get(tid, {})
                if not slug_info:
                    # Lookup singolo (fallback, non batch perché siamo già in coda)
                    try:
                        conn2 = sqlite3.connect(SEO_DB_PATH, timeout=2)
                        cur2 = conn2.execute(
                            "SELECT slug, year FROM seo_titles WHERE tmdb_id = ? AND content_type = ? LIMIT 1",
                            (tid, content_type)
                        )
                        row2 = cur2.fetchone()
                        conn2.close()
                        if row2:
                            slug_info = {"slug": row2[0], "year": row2[1]}
                    except Exception:
                        pass

                year = (slug_info.get("year")
                        or _extract_year(r.get("release_date")
                                         or r.get("first_air_date") or ""))

                extra_recs.append({
                    "tmdb_id":      tid,
                    "title":        r.get("title", "") or r.get("name", ""),
                    "poster_url":   poster_url,
                    "overview":     (r.get("overview") or "")[:200],
                    "vote_average": r.get("vote_average") or 0,
                    "year":         year,
                    "slug":         slug_info.get("slug"),
                    "content_type": content_type,
                    "reason":       "Simile per genere e tematiche",
                })

                if len(enriched) + len(extra_recs) >= limit:
                    break

            enriched.extend(extra_recs)
            print(f"[seo /come] '{title}': fallback aggiunti={len(extra_recs)} "
                  f"(skip_no_id={skip_no_id}, skip_dup={skip_dup}, "
                  f"skip_no_poster={skip_no_poster}), totale={len(enriched)}")
        except Exception as e:
            import traceback
            print(f"[seo /come] ERROR fallback TMDb per '{title}': {e}")
            print(f"[seo /come] Traceback: {traceback.format_exc()}")

    return enriched[:limit]


def _extract_year(date_str: str) -> Optional[int]:
    """Estrae l'anno da una data ISO (YYYY-MM-DD)."""
    if date_str and len(date_str) >= 4:
        try:
            return int(date_str[:4])
        except ValueError:
            pass
    return None
