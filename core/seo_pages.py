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
