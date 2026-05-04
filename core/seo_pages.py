"""
Gestione delle pagine SEO `/dove-vedere/{slug}`.

Architettura:
- Tabella `seo_titles` in DB: mappa slug → tmdb_id + metadati cached
- Funzione `populate_seo_titles_db()`: pesca top ~800 titoli da TMDb e li salva
- Funzione `get_title_by_slug()`: lookup veloce slug → tmdb_id
- Funzione `slugify()`: converte titolo in slug SEO-friendly
- Funzione `list_seo_titles()`: paginazione per la pagina hub

Strategia anti-spam Google:
- Pagine generate solo per titoli con dati reali completi (poster, trama, anno)
- ~800 titoli (sotto 1000 per evitare red flag thin content)
- Refresh settimanale via job admin (NON ad ogni richiesta)
- Schema.org JSON-LD per rich snippets
"""
import os
import re
import sqlite3
import time
import unicodedata
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

# DB path: stesso DB utenti (Render persistent disk)
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_DB = os.path.join(_BASE_DIR, "app", "cosaguardo.db")
SEO_DB_PATH = os.environ.get("DATABASE_PATH") or _DEFAULT_DB

# Limite pagine indicizzabili — sotto 1000 per evitare flag "thin content scaling"
MAX_TITLES_TARGET = 800
TITLES_PER_TYPE = 400  # 400 movies + 400 series = 800 totali


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
    """Crea tabella seo_titles se non esiste."""
    conn = sqlite3.connect(SEO_DB_PATH, timeout=5)
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
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_seo_titles_pop
        ON seo_titles (popularity DESC)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_seo_titles_type_pop
        ON seo_titles (content_type, popularity DESC)
    """)
    conn.commit()
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


def populate_seo_titles_db() -> dict:
    """
    Pesca i top titoli da TMDb (popular + top_rated) e popola/aggiorna seo_titles.
    Ritorna stats {"movies": N, "tv": M, "total": T}.

    Strategia: prendiamo 20 pagine × 20 risultati = 400 per movie + 400 per tv.
    Pagine TMDb in parallelo per velocità.
    """
    _ensure_db()

    # Endpoint da scrapare (4 endpoint x 5 pagine = 100 risultati ognuno; * 4 fonti = 400 per type)
    movie_sources = [
        ("/movie/popular",    list(range(1, 11))),  # popular pages 1-10
        ("/movie/top_rated",  list(range(1, 11))),  # top_rated pages 1-10
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
                # Salta se non ha poster (pagina seo senza poster è terribile)
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

    # Tieni solo i top per popularity (ordina + tronca)
    movies_sorted = sorted(movies_by_id.values(),
                           key=lambda x: x.get("popularity", 0), reverse=True)[:TITLES_PER_TYPE]
    tv_sorted = sorted(tv_by_id.values(),
                       key=lambda x: x.get("popularity", 0), reverse=True)[:TITLES_PER_TYPE]

    # Inserisci in DB con gestione collisioni slug
    now = int(time.time())
    conn = sqlite3.connect(SEO_DB_PATH, timeout=10)
    cur = conn.cursor()

    # Reset cache slug usati in questa run per evitare collisioni
    used_slugs = set()
    inserted_movies = 0
    inserted_tv = 0

    # Carica slug esistenti per non perderli
    cur.execute("SELECT slug FROM seo_titles")
    existing_slugs = {r[0] for r in cur.fetchall()}

    def _insert(item, content_type):
        nonlocal inserted_movies, inserted_tv

        title = item.get("title") if content_type == "movie" else item.get("name")
        date_field = item.get("release_date") if content_type == "movie" else item.get("first_air_date")
        year = None
        if date_field and len(date_field) >= 4:
            try:
                year = int(date_field[:4])
            except ValueError:
                year = None

        # Slug iniziale senza anno
        slug = slugify(title)
        if not slug:
            return

        # Se collisione → aggiungi anno
        if slug in used_slugs and year:
            slug = slugify(title, year)

        # Se ancora collisione (rarissimo) → aggiungi tmdb_id
        if slug in used_slugs:
            slug = f"{slug}-{item['id']}"

        used_slugs.add(slug)

        cur.execute("""
            INSERT OR REPLACE INTO seo_titles
            (slug, tmdb_id, content_type, title, year, popularity, vote_average, poster_path, overview, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            slug,
            item["id"],
            content_type,
            title,
            year,
            item.get("popularity", 0),
            item.get("vote_average", 0),
            item.get("poster_path"),
            item.get("overview", "")[:500],  # cap a 500 char
            now
        ))

        if content_type == "movie":
            inserted_movies += 1
        else:
            inserted_tv += 1

    for m in movies_sorted:
        _insert(m, "movie")
    for t in tv_sorted:
        _insert(t, "tv")

    # Pulizia: rimuovi gli slug vecchi che non sono più nel TOP 400
    # (così se un film perde popolarità non ha più la sua pagina SEO)
    cur.execute("SELECT slug, updated_at FROM seo_titles WHERE updated_at < ?", (now,))
    stale = [r[0] for r in cur.fetchall()]
    if stale:
        # Cancella stale solo se l'aggiornamento ha avuto successo (non di poco)
        if inserted_movies + inserted_tv >= 100:
            cur.executemany("DELETE FROM seo_titles WHERE slug = ?", [(s,) for s in stale])

    conn.commit()
    conn.close()

    return {
        "movies": inserted_movies,
        "tv": inserted_tv,
        "total": inserted_movies + inserted_tv,
        "removed_stale": len(stale) if (inserted_movies + inserted_tv >= 100) else 0,
    }


def seo_titles_count() -> int:
    """Quanti titoli SEO ci sono in DB ora."""
    _ensure_db()
    try:
        conn = sqlite3.connect(SEO_DB_PATH, timeout=3)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM seo_titles")
        n = cur.fetchone()[0]
        conn.close()
        return n
    except Exception:
        return 0
