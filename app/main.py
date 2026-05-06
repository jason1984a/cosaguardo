import os
import sys
from fastapi import FastAPI, Form, Request
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Body
from starlette.middleware.sessions import SessionMiddleware
from app.taste_profile import build_taste_profile
from app.dashboard_recommendations import build_dashboard_recommendations
from app.db import (
    init_db,
    get_user_by_email,
    create_user,
    verify_user,
    get_user_by_id,
    create_search,
    get_searches_by_user,
    get_daily_recommendations,
    save_daily_recommendations,
    get_liked_states_by_user,
    get_seen_titles_by_user,
    get_disliked_titles_by_user,
    get_title_states_map,
    upsert_title_state,
    get_user_stats,
    get_home_picks,
    save_home_picks,
    save_user_onboarding,
    get_admin_stats,
    get_poster_cache,
    save_poster_cache,
    get_search_cache,
    save_search_cache,
)
from datetime import datetime
from core.recommendation_api import (
    recommend_from_seed_titles,
    search_movies,
    get_movie_tmdb_info,
    get_trending_tmdb,
    get_watch_providers,
    get_now_playing,
    get_upcoming,
    get_top_rated_recent,
    get_detail_movie,
    get_detail_tv,
    get_cinema_news,
    search_movies_fast,
    get_person_detail,
    get_scopri_results,
    get_scopri_strips,
    get_similar_movies_tmdb,
    get_popular_by_genre_tmdb,
    get_franchise_key,
    is_same_franchise,
)

from core.recommendation_tv import recommend_tv_from_seed_titles, search_tv_series, find_tv_by_title
from core.seo_pages import (
    get_title_by_slug, list_seo_titles, list_all_slugs_for_sitemap,
    populate_seo_titles_db, seo_titles_count, slugify, get_similar_for_seo,
    get_slug_by_tmdb_id,
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

# ─── Bot detection (anti-OOM su crawl massiccio) ────────────────────────
_BOT_UA_PATTERNS = (
    "bot", "crawler", "spider", "googlebot", "bingbot", "yandex",
    "duckduckbot", "baiduspider", "slurp", "facebookexternalhit",
    "twitterbot", "linkedinbot", "applebot", "semrushbot", "ahrefsbot",
    "mj12bot", "petalbot", "pinterestbot",
)


def _is_bot(request: Request) -> bool:
    """True se l'User-Agent sembra un bot/crawler. Case-insensitive."""
    ua = (request.headers.get("user-agent") or "").lower()
    if not ua:
        return True  # UA vuoto è quasi sempre bot/script
    return any(p in ua for p in _BOT_UA_PATTERNS)


# ─── Cache trending home (TTL 10 minuti) ───────────────────────────────────
import time as _time
_trending_cache: dict = {"data": None, "ts": 0.0}
_TRENDING_TTL = 600  # secondi


def get_trending_cached(limit: int = 12) -> list:
    now = _time.time()
    if _trending_cache["data"] is not None and (now - _trending_cache["ts"]) < _TRENDING_TTL:
        return _trending_cache["data"]
    fresh = get_trending_tmdb(limit=limit)
    if fresh:
        _trending_cache["data"] = fresh
        _trending_cache["ts"] = now
    return fresh or _trending_cache.get("data") or []
# ───────────────────────────────────────────────────────────────────────────

# ─── Cache now_playing / upcoming (TTL 6 ore) ─────────────────────────────
_cinema_cache: dict = {"now_playing": None, "upcoming": None, "ts": 0.0}
_CINEMA_TTL = 21600  # 6 ore — le uscite cambiano lentamente


def get_cinema_cached() -> dict:
    now = _time.time()
    if _cinema_cache["now_playing"] is not None and (now - _cinema_cache["ts"]) < _CINEMA_TTL:
        return _cinema_cache
    np = get_now_playing(limit=10)
    up = get_upcoming(limit=10)
    if np or up:
        _cinema_cache["now_playing"] = np
        _cinema_cache["upcoming"] = up
        _cinema_cache["ts"] = now
    return _cinema_cache
# ──────────────────────────────────────────────────────────────────────────

# ─── Cache top rated recent (TTL 24 ore) ──────────────────────────────────
_toprated_cache: dict = {"data": None, "ts": 0.0}
_TOPRATED_TTL = 86400  # 24 ore — shufflato con seed giornaliero


def get_toprated_cached(limit: int = 10) -> list:
    now = _time.time()
    if _toprated_cache["data"] is not None and (now - _toprated_cache["ts"]) < _TOPRATED_TTL:
        return _toprated_cache["data"]
    fresh = get_top_rated_recent(limit=limit)
    if fresh:
        _toprated_cache["data"] = fresh
        _toprated_cache["ts"] = now
    return fresh or _toprated_cache.get("data") or []
# ──────────────────────────────────────────────────────────────────────────

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=os.environ.get("SECRET_KEY", "cosaguardo-secret-key"))


# ─── Perf logging middleware ────────────────────────────────────────────
# Logga durata e status di ogni request. Filtrabile su Render Logs con grep "[perf]".
# Format: [perf] METHOD /route status=200 dur=123ms ua=browser
# Per disattivare: set env var PERF_LOG=0 (default = attivo)
import time as _perf_time
import threading as _perf_threading
import re as _perf_re

_PERF_LOG_ENABLED = os.environ.get("PERF_LOG", "1") != "0"
_PERF_SLOW_MS = int(os.environ.get("PERF_SLOW_MS", "500"))  # marca [SLOW] sopra questa soglia
# Path da non loggare (statici, healthcheck, ecc. — riducono rumore)
_PERF_SKIP_PREFIXES = ("/static/", "/favicon", "/sw.js", "/manifest")

# Accumulator in-memory per /admin/perf-stats — resetta al restart processo.
# Per ogni "route pattern" (parametri normalizzati) tiene: count, total_ms,
# max_ms, ultimo_status. Cap totale 200 pattern per evitare memory bloat.
_PERF_STATS_MAX_PATTERNS = 200
_perf_stats: dict = {}
_perf_stats_lock = _perf_threading.Lock()


def _perf_normalize_path(path: str) -> str:
    """Sostituisce parametri variabili nei path per raggruppare statistiche.
    /film/27205 → /film/{id}, /come/inception → /come/{slug}"""
    # /film/{numero}, /serie/{numero}, /persona/{numero}
    path = _perf_re.sub(r"^(/film|/serie|/persona)/\d+", r"\1/{id}", path)
    # /come/{qualcosa}, /dove-vedere/{qualcosa} (non l'hub /dove-vedere/ stesso)
    path = _perf_re.sub(r"^(/come|/dove-vedere)/[^/]+$", r"\1/{slug}", path)
    return path


def _perf_record(path: str, dur_ms: int, status, ua_kind: str) -> None:
    norm = _perf_normalize_path(path)
    with _perf_stats_lock:
        if norm not in _perf_stats and len(_perf_stats) >= _PERF_STATS_MAX_PATTERNS:
            return  # cap raggiunto, non aggiungiamo nuovi pattern
        s = _perf_stats.setdefault(norm, {
            "count": 0, "total_ms": 0, "max_ms": 0, "slow_count": 0,
            "last_status": None, "bot_count": 0, "usr_count": 0,
        })
        s["count"] += 1
        s["total_ms"] += dur_ms
        if dur_ms > s["max_ms"]:
            s["max_ms"] = dur_ms
        if dur_ms >= _PERF_SLOW_MS:
            s["slow_count"] += 1
        s["last_status"] = status
        if ua_kind == "bot":
            s["bot_count"] += 1
        else:
            s["usr_count"] += 1


@app.middleware("http")
async def perf_logger(request: Request, call_next):
    if not _PERF_LOG_ENABLED:
        return await call_next(request)

    path = request.url.path
    if any(path.startswith(p) for p in _PERF_SKIP_PREFIXES):
        return await call_next(request)

    t0 = _perf_time.perf_counter()
    status = "?"
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    except Exception:
        status = "EXC"
        raise
    finally:
        dur_ms = int((_perf_time.perf_counter() - t0) * 1000)
        slow_marker = " [SLOW]" if dur_ms >= _PERF_SLOW_MS else ""
        ua = (request.headers.get("user-agent") or "").lower()
        ua_kind = "bot" if any(p in ua for p in ("bot", "crawler", "spider", "googlebot")) else "usr"
        try:
            _perf_record(path, dur_ms, status, ua_kind)
        except Exception:
            pass  # mai rompere la richiesta per il logger
        print(f"[perf] {request.method} {path} status={status} dur={dur_ms}ms ua={ua_kind}{slow_marker}")


init_db()

app.mount(
    "/static",
    StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
    name="static",
)
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

# Inietta is_logged_in in ogni template automaticamente
_original_TemplateResponse = templates.TemplateResponse

def _patched_TemplateResponse(*args, **kwargs):
    # Supporta sia chiamata positional che keyword
    if args and isinstance(args[0], str):
        name, context = args[0], args[1] if len(args) > 1 else kwargs.get("context", {})
        request = context.get("request")
    else:
        request = kwargs.get("request") or (args[0] if args else None)
        name = kwargs.get("name") or (args[1] if len(args) > 1 else "")
        context = kwargs.get("context", {})

    if request and "is_logged_in" not in context:
        context["is_logged_in"] = bool(request.session.get("user_id"))
    if request and "user_email" not in context:
        context["user_email"] = request.session.get("user_email", "")

    return _original_TemplateResponse(*args, **kwargs)

templates.TemplateResponse = _patched_TemplateResponse


def prettify_title(title: str) -> str:
    """
    Pulisce i titoli stile MovieLens, gestendo:
    - articoli in coda: 'Matrix, The' → 'The Matrix'
    - alias 'a.k.a.': rimossi
    - titoli localizzati IT in parentesi: preferiti se presenti
    - anno '(YYYY)' finale: rimosso
    """
    if not title:
        return title

    import re
    t = title.strip()

    # 1. Rimuovi anno finale "(YYYY)"
    t = re.sub(r"\s*\(\d{4}\)\s*$", "", t)

    # 2. Estrai tutte le parentesi alla fine, una alla volta da destra a sinistra
    parens_at_end = []
    while True:
        m = re.search(r"\s*\(([^()]+)\)\s*$", t)
        if not m:
            break
        parens_at_end.insert(0, m.group(1).strip())
        t = t[: m.start()].rstrip()

    main_title = t

    # 3. Filtra le parentesi: scarta a.k.a. e anni soli
    candidates_loc = []
    for paren in parens_at_end:
        if re.match(r"a\.?k\.?a\.?\b", paren, flags=re.IGNORECASE):
            continue
        if re.fullmatch(r"\d{4}", paren):
            continue
        candidates_loc.append(paren)

    # 4. Se c'è un titolo localizzato, preferiscilo (in MovieLens è sempre l'ultima paren)
    if candidates_loc:
        chosen = candidates_loc[-1]
        if any(c.isalpha() for c in chosen):
            main_title = chosen

    # 5. Articolo in coda: "Matrix, The" → "The Matrix"
    suffixes = [", The", ", A", ", An", ", La", ", Le", ", Les", ", Il", ", Lo", ", L'", ", Gli", ", Una", ", Un"]
    for suffix in suffixes:
        if main_title.endswith(suffix):
            base = main_title[: -len(suffix)].strip()
            article = suffix[2:].strip()
            main_title = f"{article} {base}"
            break

    return main_title.strip()



# ─── Cache strips /scopri (TTL 2 ore) ────────────────────────────────────
_strips_cache: dict = {}  # {tipo: {"data": [...], "ts": float}}
_STRIPS_TTL = 7200


def get_strips_cached(tipo: str) -> list:
    now   = _time.time()
    entry = _strips_cache.get(tipo)
    if entry and (now - entry["ts"]) < _STRIPS_TTL:
        return entry["data"]
    fresh = get_scopri_strips(tipo=tipo)
    if fresh:
        _strips_cache[tipo] = {"data": fresh, "ts": now}
    return fresh or (entry["data"] if entry else [])
# ──────────────────────────────────────────────────────────────────────────

# ─── Cache RSS news (TTL 1 ora) ───────────────────────────────────────────
_news_cache: dict = {"data": None, "ts": 0.0}
_NEWS_TTL = 1800  # 30 minuti


def get_news_cached(limit: int = 8) -> list:
    now = _time.time()
    if _news_cache["data"] is not None and (now - _news_cache["ts"]) < _NEWS_TTL:
        return _news_cache["data"]
    fresh = get_cinema_news(limit=limit)
    if fresh:
        _news_cache["data"] = fresh
        _news_cache["ts"] = now
    return fresh or _news_cache.get("data") or []
# ──────────────────────────────────────────────────────────────────────────

@app.get("/")
def home(request: Request):
    trending = get_trending_cached(limit=12)
    user_id  = request.session.get("user_id")
    cinema   = get_cinema_cached()

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "request": request,
            "trending": trending,
            "is_logged_in": bool(user_id),
            "now_playing": cinema.get("now_playing") or [],
            "upcoming": cinema.get("upcoming") or [],
            "top_rated": get_toprated_cached(limit=10),
            "news": get_news_cached(limit=8),
        },
    )



@app.get("/cinema-news", response_class=JSONResponse)
def cinema_news():
    """
    Dati cinema aggiornati: film in sala + prossime uscite (IT).
    Cached 6h lato server, può essere richiamato dal frontend.
    """
    cinema = get_cinema_cached()
    return {
        "now_playing": cinema.get("now_playing") or [],
        "upcoming":    cinema.get("upcoming") or [],
    }

@app.get("/home-picks", response_class=JSONResponse)
def home_picks(request: Request):
    """
    Consigli personalizzati per la home — calcolati una volta al giorno e salvati in DB.
    Prima chiamata del giorno: ~2-3s. Tutte le successive: istantanee (lettura DB).
    """
    user_id = request.session.get("user_id")
    if not user_id:
        from fastapi.responses import JSONResponse as _JSONResponse
        return _JSONResponse(status_code=401, content={"error": "not_logged_in"})

    today = datetime.now().strftime("%Y-%m-%d")

    # 1. Prova a leggere dal DB (già calcolato oggi)
    cached = get_home_picks(user_id, today)
    if cached:
        return cached

    # 2. Prima chiamata del giorno — calcola
    from concurrent.futures import ThreadPoolExecutor, as_completed

    searches     = get_searches_by_user(user_id, limit=10)
    liked_titles = [dict(row) for row in get_liked_states_by_user(user_id)]

    # Poster in parallelo
    def fetch_poster(item):
        if item["content_type"] == "movie":
            info = get_movie_tmdb_info(item["title"])
            item["poster_url"] = info.get("poster_url", "") if info else ""
            item["tmdb_id"]    = info.get("tmdb_id") if info else None
        else:
            tv = find_tv_by_title(item["title"])
            if tv and tv.get("poster_path"):
                item["poster_url"] = f"https://image.tmdb.org/t/p/w342{tv['poster_path']}"
                item["tmdb_id"]    = tv.get("id") or tv.get("tv_id")
            else:
                item["poster_url"] = ""
                item["tmdb_id"]    = None
        return item

    needs = [i for i in liked_titles if not i.get("poster_url")]
    if needs:
        with ThreadPoolExecutor(max_workers=8) as ex:
            for fut in as_completed({ex.submit(fetch_poster, i): i for i in needs}):
                try: fut.result()
                except Exception: pass

    picks = build_dashboard_recommendations(
        user_id=user_id,
        searches=searches,
        liked_titles=liked_titles,
        per_type_pool=18,
        final_count=12,
    )

    # 3. Salva in DB per tutto il resto della giornata
    if picks:
        save_home_picks(user_id, today, picks)

    return picks


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if request.session.get("user_id"):
        return RedirectResponse(url="/profilo", status_code=303)

    return templates.TemplateResponse(
        request=request,
        name="login.html",
        context={
            "request": request,
            "error": None
        },
    )

@app.get("/register", response_class=HTMLResponse)
def register_page(request: Request):
    if request.session.get("user_id"):
        return RedirectResponse(url="/profilo", status_code=303)

    return templates.TemplateResponse(
        request=request,
        name="register.html",
        context={
            "request": request,
            "error": None,
            "email": ""
        },
    )

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    """Legacy redirect — ora tutto è in /profilo."""
    user_id = request.session.get("user_id")
    if not user_id:
        return RedirectResponse(url="/login", status_code=303)
    return RedirectResponse(url="/profilo", status_code=302)


@app.post("/register", response_class=HTMLResponse)
def register_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
    first_name: str = Form(default=""),
    last_name: str = Form(default=""),
    birth_date: str = Form(default=""),
    accept_privacy: str = Form(default=""),
    accept_terms: str = Form(default=""),
    accept_age: str = Form(default=""),
    content_pref: str = Form(default="both"),
    platforms: list = Form(default=[]),
):
    import re
    email = email.strip().lower()

    def err(msg):
        return templates.TemplateResponse(
            request=request,
            name="register.html",
            context={
                "request": request, "error": msg, "email": email,
                "first_name": first_name, "last_name": last_name,
                "birth_date": birth_date,
                "content_pref": content_pref, "platforms": platforms,
                "now": datetime.now().isoformat(),
            },
        )

    if not email:
        return err("Inserisci una email valida.")
    if not first_name.strip():
        return err("Inserisci il tuo nome.")
    if not last_name.strip():
        return err("Inserisci il tuo cognome.")

    # Validazione età 16+
    if birth_date.strip():
        from datetime import date
        try:
            bd = date.fromisoformat(birth_date.strip())
            today = date.today()
            age = today.year - bd.year - ((today.month, today.day) < (bd.month, bd.day))
            if age < 16:
                return err("Devi avere almeno 16 anni per registrarti.")
        except ValueError:
            return err("Data di nascita non valida.")

    # Password: min 8 char + almeno 1 numero o simbolo
    if len(password) < 8:
        return err("La password deve avere almeno 8 caratteri.")
    if not re.search(r"[0-9!@#$%^&*()+_=;:,.<>?@]", password):
        return err("La password deve contenere almeno un numero o un carattere speciale.")

    if password != confirm_password:
        return err("Le password non coincidono.")

    # Spunte legali obbligatorie
    if not accept_privacy:
        return err("Devi accettare la Privacy Policy per registrarti.")
    if not accept_terms:
        return err("Devi accettare i Termini di Servizio per registrarti.")
    if not accept_age:
        return err("Devi dichiarare di avere almeno 16 anni.")

    existing_user = get_user_by_email(email)
    if existing_user:
        return err("Esiste già un account con questa email.")

    user_id = create_user(
        email, password,
        first_name=first_name.strip(),
        last_name=last_name.strip(),
        birth_date=birth_date.strip()
    )
    request.session["user_id"]    = user_id
    request.session["user_email"] = email

    # Salva preferenze onboarding
    if content_pref or platforms:
        save_user_onboarding(
            user_id, content_pref,
            platforms if isinstance(platforms, list) else [platforms]
        )

    return RedirectResponse(url="/profilo", status_code=303)

@app.post("/feedback")
def save_feedback(request: Request, data: dict = Body(...)):
    user_id = request.session.get("user_id")

    if not user_id:
        return {"status": "error", "message": "not logged"}

    title = (data.get("title") or "").strip()
    content_type = (data.get("content_type") or "").strip().lower()
    feedback_type = (data.get("feedback_type") or "").strip().lower()

    if not title or not content_type or not feedback_type:
        return {"status": "error", "message": "missing data"}

    if feedback_type == "liked":
        upsert_title_state(
            user_id=user_id,
            title=title,
            content_type=content_type,
            preference="liked"
        )

    elif feedback_type == "disliked":
        upsert_title_state(
            user_id=user_id,
            title=title,
            content_type=content_type,
            preference="disliked"
        )

    elif feedback_type == "seen":
        current_state = None
        try:
            from app.db import get_title_state
            current_state = get_title_state(user_id, title, content_type)
        except Exception:
            current_state = None

        current_seen = current_state["seen"] if current_state else 0
        new_seen = 0 if current_seen == 1 else 1

        upsert_title_state(
            user_id=user_id,
            title=title,
            content_type=content_type,
            seen=new_seen
        )

        return {"status": "ok", "seen": new_seen}

    else:
        return {"status": "error", "message": "invalid feedback type"}

    return {"status": "ok"}

@app.post("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)

@app.post("/login")
def login_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...)
):
    email = email.strip().lower()
    user = verify_user(email, password)

    if not user:
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context={
                "request": request,
                "error": "Email o password non corrette."
            },
        )

    request.session["user_id"] = user["id"]
    request.session["user_email"] = user["email"]

    return RedirectResponse(url="/profilo", status_code=303)

@app.post("/recommend")
def recommend(
    request: Request,
    content_type: str = Form("movie"),
    movie1: str = Form(""),
    movie2: str = Form(""),
    movie3: str = Form(""),
    movie4: str = Form(""),
    movie5: str = Form(""),
    movie6: str = Form(""),
):
    seed_titles = [
        m.strip()
        for m in [movie1, movie2, movie3, movie4, movie5, movie6]
        if m.strip()
    ]

    # ── Cache check ────────────────────────────────────────────────────────
    import hashlib as _hl
    _cache_key = _hl.md5(
        ("|".join(sorted(t.lower() for t in seed_titles)) + content_type).encode()
    ).hexdigest()
    _cached = get_search_cache(_cache_key)
    if _cached:
        # Hit cache — risposta istantanea
        user_id = request.session.get("user_id")
        if user_id and seed_titles:
            create_search(user_id=user_id, seed_titles=", ".join(seed_titles), content_type=content_type)
        # Aggiorna is_seen/is_liked in base allo stato utente corrente
        if user_id:
            title_states = get_title_states_map(user_id, content_type)
            for rec in _cached.get("recommendations", []):
                state_key = rec.get("title","").strip().lower()
                rec_state = title_states.get(state_key, {})
                rec["is_seen"]     = rec_state.get("seen", 0) == 1
                rec["is_liked"]    = rec_state.get("preference") == "liked"
                rec["is_disliked"] = rec_state.get("preference") == "disliked"
        return templates.TemplateResponse(
            request=request, name="results.html",
            context={**_cached, "request": request},
        )
    # ──────────────────────────────────────────────────────────────────────

    user_id = request.session.get("user_id")
    if user_id and seed_titles:
        create_search(
            user_id=user_id,
            seed_titles=", ".join(seed_titles),
            content_type=content_type,
        )

    if content_type == "movie":
        result = recommend_from_seed_titles(seed_titles, top_k=10, per_seed_limit=30)
    elif content_type == "tv":
        result = recommend_tv_from_seed_titles(seed_titles)
    else:
        result = {
            "resolved_seeds": [],
            "missing_titles": seed_titles,
            "recommendations": [],
        }

    resolved_seeds = result["resolved_seeds"]
    missing_titles = result["missing_titles"]
    recommendations = result["recommendations"]

    user_id = request.session.get("user_id")
    excluded_titles = []
    title_states = {}

    if user_id:
        seen_titles = get_seen_titles_by_user(user_id, content_type)
        disliked_titles = get_disliked_titles_by_user(user_id, content_type)
        excluded_titles = list(set(seen_titles + disliked_titles))
        title_states = get_title_states_map(user_id, content_type)

    if excluded_titles:
        recommendations = [
            rec for rec in recommendations
            if rec.get("title", "").strip().lower() not in excluded_titles
        ]

    pretty_resolved_seeds = []
    for seed in resolved_seeds:
        pretty_resolved_seeds.append({
            **seed,
            "title": prettify_title(seed["title"]),
        })

    enriched_recommendations = []
    if content_type == "movie":
        # Fetch TMDb in parallelo per tutti i risultati
        from concurrent.futures import ThreadPoolExecutor, as_completed
        def _fetch_tmdb(rec):
            return rec, get_movie_tmdb_info(rec["title"])
        tmdb_results = {}
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(_fetch_tmdb, rec): rec for rec in recommendations}
            for fut in as_completed(futures):
                try:
                    rec, tmdb_info = fut.result()
                    tmdb_results[rec["title"]] = tmdb_info
                except Exception:
                    pass

    for rec in recommendations:
        if content_type == "movie":
            tmdb_info = tmdb_results.get(rec["title"]) or {}
        else:
            tmdb_info = {
                "display_title": rec["title"],
                "poster_url": (
                    f"https://image.tmdb.org/t/p/w500{rec.get('poster_path')}"
                    if rec.get("poster_path")
                    else ""
                ),
                "overview": rec.get("overview", ""),
            }

        state_key = rec.get("title", "").strip().lower()
        rec_state = title_states.get(state_key, {})
        is_seen = rec_state.get("seen", 0) == 1
        preference = rec_state.get("preference")

        why_titles = [prettify_title(t) for t in rec.get("why_titles", [])]

        if len(why_titles) == 1:
            why_text = f"Ti potrebbe piacere perché richiama {why_titles[0]}."
        elif len(why_titles) == 2:
            why_text = (
                f"Ti potrebbe piacere perché ha affinità con "
                f"{why_titles[0]} e {why_titles[1]}."
            )
        elif len(why_titles) >= 3:
            why_text = (
                f"Ti potrebbe piacere perché combina elementi vicini a "
                f"{why_titles[0]}, {why_titles[1]} e {why_titles[2]}."
            )
        else:
            why_text = "Ti potrebbe piacere per affinità con i titoli che hai inserito."

        if content_type == "movie":
            enriched_recommendations.append({
                "title": tmdb_info["display_title"] or prettify_title(rec["title"]),
                "poster_url": tmdb_info["poster_url"] or "",
                "overview": tmdb_info["overview"] or "",
                "appearances": rec.get("appearances", 1),
                "avg_score": round(rec.get("avg_score", rec.get("score", 0)), 3),
                "why_recommended": why_text,
                "explanation": rec.get("explanation", ""),
                "badge": rec.get("badge", ""),
                "ui_signals": rec.get("ui_signals", []),
                "match_score": rec.get("match_score", 0),
                "genre_score_ui": rec.get("genre_score_ui", 0),
                "vibe_score_ui": rec.get("vibe_score_ui", 0),
                "genre_score": round(rec.get("components", {}).get("genre_score", 0), 3),
                "tag_score": round(rec.get("components", {}).get("tag_score", 0), 3),
                "collab_score": round(rec.get("components", {}).get("collab_score", 0), 3),
                "keyword_score": 0,
                "matched_keywords": [],
                "tmdb_id": tmdb_info.get("tmdb_id") if tmdb_info else None,
                "is_seen": is_seen,
                "is_liked": preference == "liked",
                "is_disliked": preference == "disliked",
            })
        else:
            enriched_recommendations.append({
                "title": tmdb_info["display_title"] or prettify_title(rec["title"]),
                "poster_url": tmdb_info["poster_url"] or "",
                "overview": tmdb_info["overview"] or "",
                "appearances": rec.get("appearances", 1),
                "avg_score": round(rec.get("avg_score", rec.get("score", 0)), 3),
                "why_recommended": why_text,
                "explanation": rec.get("explanation", ""),
                "badge": rec.get("badge", ""),
                "ui_signals": rec.get("ui_signals", []),
                "match_score": rec.get("match_score", 0),
                "genre_score_ui": rec.get("genre_score_ui", 0),
                "vibe_score_ui": rec.get("vibe_score_ui", 0),
                "genre_score": 0,
                "tag_score": round(rec.get("keyword_score", 0), 3),
                "collab_score": 0,
                "keyword_score": round(rec.get("keyword_score", 0), 3),
                "matched_keywords": rec.get("matched_keywords", []),
                "tmdb_id": rec.get("tv_id"),
            })

    # ── Fallback garantito: minimo 3 risultati ───────────────────────────
    MIN_RESULTS = 3
    if len(enriched_recommendations) < MIN_RESULTS:
        needed = MIN_RESULTS - len(enriched_recommendations)
        existing_titles = {r["title"].lower() for r in enriched_recommendations}

        # Franchise keys già usati — per evitare sequel/prequel dello stesso seed
        used_franchise_keys = set()
        for r in enriched_recommendations:
            fk = get_franchise_key(r["title"])
            if fk: used_franchise_keys.add(fk)
        # Aggiungi anche i seed come franchise esclusi
        for seed in seed_titles:
            fk = get_franchise_key(seed)
            if fk: used_franchise_keys.add(fk)

        def _is_franchise_dup(title):
            """
            True se il titolo è franchise-dup rispetto a:
            - candidati già accettati (evita sequel tra i fallback)
            - seed originali
            """
            fk = get_franchise_key(title)
            # 1. Franchise key già visto tra i candidati accettati
            if fk and fk in used_franchise_keys:
                return True
            # 2. Overlap diretto con i seed
            for seed in seed_titles:
                if is_same_franchise(seed, title):
                    return True
            # 3. Overlap diretto con titoli già accettati nel fallback
            for accepted in existing_titles:
                if is_same_franchise(accepted, title):
                    return True
            return False

        fallback_recs = []

        # Livello 1: film simili al primo seed riconosciuto via TMDb
        first_resolved = resolved_seeds[0] if resolved_seeds else None
        if first_resolved and content_type == "movie":
            first_tmdb = get_movie_tmdb_info(first_resolved.get("title",""))
            if first_tmdb and first_tmdb.get("tmdb_id"):
                similars = get_similar_movies_tmdb(first_tmdb["tmdb_id"], limit=needed + 3)
                for s in similars:
                    t = s["title"]
                    if t.lower() in existing_titles: continue
                    if _is_franchise_dup(t): continue
                    existing_titles.add(t.lower())
                    fk = get_franchise_key(t)
                    if fk: used_franchise_keys.add(fk)
                    fallback_recs.append(s)
                    if len(fallback_recs) >= needed: break

        # Livello 2: per TV — usa get_similar_tv sul primo seed
        if content_type == "tv" and len(fallback_recs) < needed:
            from core.recommendation_tv import find_tv_by_title, get_similar_tv
            if seed_titles:
                tv = find_tv_by_title(seed_titles[0])
                if tv and tv.get("tv_id"):
                    similars = get_similar_tv(tv["tv_id"], limit=needed + 3)
                    for s in similars:
                        t = s.get("title","")
                        if not t or t.lower() in existing_titles: continue
                        if _is_franchise_dup(t): continue
                        existing_titles.add(t.lower())
                        fk = get_franchise_key(t)
                        if fk: used_franchise_keys.add(fk)
                        pp = s.get("poster_path","")
                        poster = f"https://image.tmdb.org/t/p/w342{pp}" if pp else s.get("poster_url","")
                        fallback_recs.append({
                            "title":       t,
                            "poster_url":  poster,
                            "tmdb_id":     s.get("tv_id"),
                            "overview":    (s.get("overview","") or "")[:200],
                            "is_fallback": True,
                        })
                        if len(fallback_recs) >= needed: break

        # Livello 3 (garantito): popolari del genere del primo seed — sempre funziona
        if len(fallback_recs) < needed:
            # Prova a ricavare il genere dal primo seed riconosciuto
            genre_id = 18  # dramma come default
            if resolved_seeds and content_type == "movie":
                try:
                    from core.recommendation_api import get_movie_genres
                    genres = get_movie_genres(resolved_seeds[0].get("title",""))
                    if genres:
                        # Mappa nome genere → ID TMDb
                        g_map = {"azione":28,"commedia":35,"thriller":53,"horror":27,
                                 "dramma":18,"fantascienza":878,"animazione":16,"crimine":80}
                        for g in genres:
                            gid = g_map.get(g.lower())
                            if gid: genre_id = gid; break
                except Exception:
                    pass
            elif content_type == "tv":
                genre_id = 18  # dramma TV

            pops = get_popular_by_genre_tmdb(genre_id, content_type, limit=needed + 5)
            for p in pops:
                t = p["title"]
                if t.lower() in existing_titles: continue
                if _is_franchise_dup(t): continue
                existing_titles.add(t.lower())
                fk = get_franchise_key(t)
                if fk: used_franchise_keys.add(fk)
                fallback_recs.append(p)
                if len(fallback_recs) >= needed: break

        # Aggiungi i fallback con badge dedicato
        for fb in fallback_recs[:needed]:
            enriched_recommendations.append({
                "title":           fb.get("title",""),
                "poster_url":      fb.get("poster_url",""),
                "overview":        fb.get("overview",""),
                "tmdb_id":         fb.get("tmdb_id"),
                "appearances":     1,
                "avg_score":       0,
                "why_recommended": "",
                "explanation":     "Potrebbe piacerti in base al tuo stile.",
                "badge":           "💡 Potrebbe piacerti",
                "ui_signals":      [],
                "match_score":     0,
                "genre_score_ui":  0,
                "vibe_score_ui":   0,
                "genre_score":     0,
                "tag_score":       0,
                "collab_score":    0,
                "keyword_score":   0,
                "matched_keywords": [],
                "is_seen":         False,
                "is_liked":        False,
                "is_disliked":     False,
            })
    # ──────────────────────────────────────────────────────────────────────

    # Salva in cache per ricerche future identiche
    _cache_data = {
        "resolved_seeds":  pretty_resolved_seeds,
        "missing_titles":  missing_titles,
        "recommendations": enriched_recommendations,
        "content_type":    content_type,
    }
    if enriched_recommendations:
        save_search_cache(_cache_key, _cache_data)

    return templates.TemplateResponse(
        request=request,
        name="results.html",
        context={
            "resolved_seeds": pretty_resolved_seeds,
            "missing_titles": missing_titles,
            "recommendations": enriched_recommendations,
            "content_type": content_type,
        },
    )


@app.get("/search-fast", response_class=JSONResponse)
def search_fast(q: str = "", content_type: str = "movie"):
    """
    Autocomplete veloce:
    - Film: solo DB locale (<10ms)
    - TV: TMDb con cache server-side (prima call ~200ms, successive <1ms)
    Il client ha anche una cache propria in app.js.
    """
    query = q.strip()
    if len(query) < 2:
        return []
    if content_type == "tv":
        return search_tv_series(query, limit=8)
    return search_movies_fast(query, limit=8)


@app.get("/search", response_class=JSONResponse)
def search(q: str = "", content_type: str = "movie"):
    query = q.strip()

    print("SEARCH DEBUG -> q:", query, "| content_type:", content_type)

    if len(query) < 2:
        return []

    if content_type == "movie":
        results = search_movies(query, limit=8)
    elif content_type == "tv":
        results = search_tv_series(query, limit=8)
    else:
        results = []

    return results


@app.get("/watch-providers", response_class=JSONResponse)
def watch_providers(title: str = "", content_type: str = "movie"):
    if not title.strip():
        return {}
    return get_watch_providers(title.strip(), content_type=content_type)


@app.get("/film/{tmdb_id}", response_class=HTMLResponse)
def film_detail(request: Request, tmdb_id: int):
    detail = get_detail_movie(tmdb_id)
    if not detail:
        return RedirectResponse(url="/", status_code=302)

    user_id = request.session.get("user_id")
    title_state = {}
    if user_id and detail.get("title"):
        title_state = get_title_states_map(user_id, "movie").get(
            detail["title"].strip().lower(), {}
        )

    # Raccomandazioni simili dal motore interno
    similar = []
    if detail.get("title"):
        try:
            res = recommend_from_seed_titles([detail["title"]], top_k=6, per_seed_limit=20)
            for rec in res.get("recommendations", [])[:6]:
                tmdb_info = get_movie_tmdb_info(rec["title"])
                if tmdb_info and tmdb_info.get("poster_url"):
                    similar.append({
                        "title":      tmdb_info.get("display_title") or rec["title"],
                        "poster_url": tmdb_info["poster_url"],
                        "tmdb_id":    tmdb_info.get("tmdb_id"),
                        "content_type": "movie",
                    })
        except Exception:
            pass

    return templates.TemplateResponse(
        request=request,
        name="detail.html",
        context={
            "request":    request,
            "detail":     detail,
            "similar":    similar,
            "is_logged_in": bool(user_id),
            "is_liked":   title_state.get("preference") == "liked",
            "is_seen":    title_state.get("seen", 0) == 1,
            "seo_slug":   get_slug_by_tmdb_id(tmdb_id, "movie"),
        },
    )


@app.get("/serie/{tmdb_id}", response_class=HTMLResponse)
def serie_detail(request: Request, tmdb_id: int):
    detail = get_detail_tv(tmdb_id)
    if not detail:
        return RedirectResponse(url="/", status_code=302)

    user_id = request.session.get("user_id")
    title_state = {}
    if user_id and detail.get("title"):
        title_state = get_title_states_map(user_id, "tv").get(
            detail["title"].strip().lower(), {}
        )

    # Raccomandazioni simili
    similar = []
    if detail.get("title"):
        try:
            res = recommend_tv_from_seed_titles([detail["title"]])
            for rec in res.get("recommendations", [])[:6]:
                pp = rec.get("poster_path", "")
                similar.append({
                    "title":        rec.get("title", ""),
                    "poster_url":   f"https://image.tmdb.org/t/p/w342{pp}" if pp else "",
                    "tmdb_id":      rec.get("tv_id"),
                    "content_type": "tv",
                })
        except Exception:
            pass

    return templates.TemplateResponse(
        request=request,
        name="detail.html",
        context={
            "request":    request,
            "detail":     detail,
            "similar":    similar,
            "is_logged_in": bool(user_id),
            "is_liked":   title_state.get("preference") == "liked",
            "is_seen":    title_state.get("seen", 0) == 1,
            "seo_slug":   get_slug_by_tmdb_id(tmdb_id, "tv"),
        },
    )


@app.get("/news", response_class=JSONResponse)
def news_endpoint():
    """Feed RSS news cinema aggregato — cached 1h."""
    return get_news_cached(limit=8)


@app.get("/profilo", response_class=HTMLResponse)
def profilo(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return RedirectResponse(url="/login", status_code=303)

    user = get_user_by_id(user_id)
    if not user:
        request.session.clear()
        return RedirectResponse(url="/login", status_code=303)

    stats      = get_user_stats(user_id)
    searches   = get_searches_by_user(user_id, limit=10)
    liked_titles = [dict(row) for row in get_liked_states_by_user(user_id)]
    taste_profile = build_taste_profile(searches)

    # Poster e tmdb_id — prima dalla cache DB, poi TMDb solo se mancanti
    seen_titles = stats.get("seen", [])
    all_items   = liked_titles + seen_titles

    # 1. Controlla cache DB
    keys        = [(item["title"], item["content_type"]) for item in all_items]
    cached_map  = get_poster_cache(keys)

    needs_fetch = []
    for item in all_items:
        key = (item["title"], item["content_type"])
        if key in cached_map:
            item["poster_url"] = cached_map[key]["poster_url"]
            item["tmdb_id"]    = cached_map[key]["tmdb_id"]
        else:
            item["poster_url"] = ""
            item["tmdb_id"]    = None
            needs_fetch.append(item)

    # 2. TMDb solo per quelli non in cache
    if needs_fetch:
        def _enrich_item(item):
            if item["content_type"] == "movie":
                tmdb_info = get_movie_tmdb_info(item["title"])
                if tmdb_info:
                    item["poster_url"] = tmdb_info.get("poster_url", "")
                    item["tmdb_id"]    = tmdb_info.get("tmdb_id")
            else:
                tv_info = find_tv_by_title(item["title"])
                if tv_info and tv_info.get("poster_path"):
                    item["poster_url"] = f"https://image.tmdb.org/t/p/w342{tv_info['poster_path']}"
                    item["tmdb_id"]    = tv_info.get("id") or tv_info.get("tv_id")
            return item

        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {ex.submit(_enrich_item, item): item for item in needs_fetch}
            for fut in as_completed(futures):
                try: fut.result()
                except Exception: pass

        # 3. Salva nuovi risultati in cache
        save_poster_cache([
            {"title": i["title"], "content_type": i["content_type"],
             "poster_url": i.get("poster_url",""), "tmdb_id": i.get("tmdb_id")}
            for i in needs_fetch
        ])

    # Consigli del giorno (stessa logica del vecchio dashboard)
    today_key  = datetime.now().strftime("%Y-%m-%d")
    daily_recs = get_daily_recommendations(user_id, today_key)

    if daily_recs and len(daily_recs) > 0:
        recommendations = [dict(rec) for rec in daily_recs]
    else:
        recommendations = build_dashboard_recommendations(
            user_id=user_id,
            searches=searches,
            liked_titles=liked_titles,
            taste_profile=taste_profile,
        )
        if recommendations:
            save_daily_recommendations(user_id, today_key, recommendations)

    return templates.TemplateResponse(
        request=request,
        name="profilo.html",
        context={
            "request":         request,
            "user_email":      user["email"],
            "user_name":       f"{user['first_name'] or ''} {user['last_name'] or ''}".strip() or user["email"],
            "stats":           stats,
            "taste_profile":   taste_profile,
            "liked_titles":    liked_titles,
            "seen_titles":     seen_titles,
            "recommendations": recommendations,
            "searches":        searches,
        },
    )


@app.get("/tmdb-id", response_class=JSONResponse)
def get_tmdb_id(title: str = "", content_type: str = "movie"):
    """
    Restituisce il tmdb_id per un titolo — usato dal modal del profilo
    per costruire il link /film/{id} o /serie/{id}.
    """
    title = title.strip()
    if not title:
        return {"tmdb_id": None}

    if content_type == "tv":
        try:
            result = find_tv_by_title(title)
            tmdb_id = result.get("id") or result.get("tv_id") if result else None
        except Exception:
            tmdb_id = None
    else:
        try:
            info = get_movie_tmdb_info(title)
            tmdb_id = info.get("tmdb_id") if info else None
        except Exception:
            tmdb_id = None

    return {"tmdb_id": tmdb_id}

# ─── Google OAuth ─────────────────────────────────────────────────────────
import httpx
import secrets
import logging

logger = logging.getLogger("cosaguardo.oauth")

GOOGLE_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI  = os.environ.get("GOOGLE_REDIRECT_URI", "https://cosaguardo.com/auth/google/callback")


@app.get("/auth/google")
def google_login(request: Request):
    """Redirect a Google per il login OAuth.
    Genera uno state CSRF token e lo salva in sessione per verifica al callback."""
    import urllib.parse

    # CSRF protection — genera state token e salva in sessione
    state = secrets.token_urlsafe(32)
    request.session["oauth_state"] = state

    params = {
        "client_id":     GOOGLE_CLIENT_ID,
        "redirect_uri":  GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope":         "openid email profile",
        "access_type":   "online",
        "prompt":        "select_account",
        "state":         state,
    }
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
    return RedirectResponse(url=url)


@app.get("/auth/google/callback")
def google_callback(request: Request, code: str = "", state: str = "", error: str = ""):
    """Callback Google OAuth — crea o logga l'utente."""
    if error or not code:
        return RedirectResponse(url="/login?error=google_cancelled", status_code=302)

    # CSRF check — verifica state e consuma il token (one-shot)
    expected_state = request.session.pop("oauth_state", None)
    if not expected_state or not state or not secrets.compare_digest(state, expected_state):
        logger.warning("OAuth state mismatch o assente — possibile CSRF attempt")
        return RedirectResponse(url="/login?error=google_state", status_code=302)

    try:
        # 1. Scambia il code con il token
        token_resp = httpx.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code":          code,
                "client_id":     GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "redirect_uri":  GOOGLE_REDIRECT_URI,
                "grant_type":    "authorization_code",
            },
            timeout=10,
        )
        token_data = token_resp.json()
        access_token = token_data.get("access_token")

        if not access_token:
            logger.warning("OAuth token exchange failed: %s", token_data.get("error", "unknown"))
            return RedirectResponse(url="/login?error=google_failed", status_code=302)

        # 2. Recupera info utente da Google
        userinfo_resp = httpx.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
        userinfo = userinfo_resp.json()
        email = userinfo.get("email", "").strip().lower()
        email_verified = userinfo.get("verified_email", False)

        if not email:
            return RedirectResponse(url="/login?error=google_no_email", status_code=302)

        # Account takeover protection: rifiuta email non verificate da Google.
        # Senza questo check, chiunque potrebbe rivendicare un account altrui
        # registrando una Google identity non-verificata con la stessa email.
        if not email_verified:
            logger.warning("OAuth: email %s rifiutata, non verificata da Google", email)
            return RedirectResponse(url="/login?error=google_unverified", status_code=302)

        # 3. Crea o recupera l'utente
        user = get_user_by_email(email)
        if not user:
            # Nuovo utente — crea con password casuale (non usata per login Google)
            user_id = create_user(email, secrets.token_hex(32))
        else:
            user_id = user["id"]

        # 4. Setta la sessione
        request.session["user_id"]    = user_id
        request.session["user_email"] = email

        return RedirectResponse(url="/profilo", status_code=302)

    except Exception as e:
        logger.exception("OAuth callback error: %s", e)
        return RedirectResponse(url="/login?error=google_error", status_code=302)
# ──────────────────────────────────────────────────────────────────────────

# ─── Sitemap.xml ──────────────────────────────────────────────────────────
from fastapi.responses import Response, FileResponse


@app.get("/sw.js")
def service_worker():
    """
    Serve il service worker dalla root del sito.
    DEVE essere servito da / e non da /static/ perché il suo scope
    altrimenti sarebbe limitato a /static/*. Richiesto per le PWA.
    """
    sw_path = os.path.join(os.path.dirname(__file__), "static", "sw.js")
    return FileResponse(
        sw_path,
        media_type="application/javascript",
        headers={
            # No-cache sul SW stesso, così quando deployi il browser scarica subito la nuova versione
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Service-Worker-Allowed": "/",
        },
    )


@app.get("/installa", response_class=HTMLResponse)
def installa(request: Request):
    """Pagina dedicata per installare la PWA con istruzioni multi-piattaforma."""
    return templates.TemplateResponse(
        request=request,
        name="installa.html",
        context={"request": request},
    )


@app.get("/dove-vedere", response_class=HTMLResponse)
def dove_vedere_hub(request: Request, tipo: str = "", p: int = 1):
    """Hub paginato di tutti i titoli con pagine SEO /dove-vedere/{slug}."""
    content_type = None
    if tipo == "film":
        content_type = "movie"
    elif tipo == "serie":
        content_type = "tv"

    page = max(1, p)
    per_page = 36

    items, total = list_seo_titles(content_type=content_type, page=page, per_page=per_page)
    total_pages = max(1, (total + per_page - 1) // per_page)

    return templates.TemplateResponse(
        request=request,
        name="dove_vedere_hub.html",
        context={
            "request": request,
            "items": items,
            "content_type": content_type,
            "page": page,
            "total_pages": total_pages,
            "total": total,
        },
    )


@app.get("/dove-vedere/{slug}", response_class=HTMLResponse)
def dove_vedere_detail(request: Request, slug: str):
    """Pagina SEO dedicata: dove vedere {titolo} in streaming."""
    item = get_title_by_slug(slug)
    if not item:
        # 404 esplicito invece di redirect: meglio per SEO
        return templates.TemplateResponse(
            request=request,
            name="dove_vedere_hub.html",
            context={
                "request": request,
                "items": [],
                "content_type": None,
                "page": 1,
                "total_pages": 1,
                "total": 0,
                "_not_found_slug": slug,
            },
            status_code=404,
        )

    # Costruisci poster URL (se in DB c'è solo il path)
    if item.get("poster_path"):
        poster_url = f"https://image.tmdb.org/t/p/w500{item['poster_path']}"
    else:
        poster_url = None

    # Detail completo da TMDb (cache 24h memoria + 7gg DB già attiva)
    if item["content_type"] == "tv":
        detail = get_detail_tv(item["tmdb_id"]) or {}
    else:
        detail = get_detail_movie(item["tmdb_id"]) or {}

    # Se TMDb ha info migliori per poster, usa quelle
    if not detail.get("poster_url") and poster_url:
        detail["poster_url"] = poster_url

    # Trova titoli simili dal DB SEO (da raccomandare con link interno)
    similar_titles = []
    try:
        # Strategia semplice: stesso content_type, alta popolarità, escluso self
        candidates, _ = list_seo_titles(
            content_type=item["content_type"],
            page=1,
            per_page=20
        )
        # Filtra fuori se stesso e prendi i primi 8 (non è raccomandazione algoritmica
        # ma copre il caso generale ed è utile per internal linking SEO)
        similar_titles = [c for c in candidates if c["slug"] != item["slug"]][:8]
    except Exception:
        pass

    return templates.TemplateResponse(
        request=request,
        name="dove_vedere.html",
        context={
            "request": request,
            "item": item,
            "detail": detail,
            "similar_titles": similar_titles,
        },
    )


@app.get("/come/{slug}", response_class=HTMLResponse)
def come_simili(request: Request, slug: str):
    """
    Pagina SEO 'Film/Serie come X': 12 alternative consigliate dall'algoritmo.
    URL pattern: /come/inception, /come/breaking-bad
    """
    item = get_title_by_slug(slug)
    if not item:
        # 404: titolo non in DB SEO. Reindirizza alla home con messaggio.
        return templates.TemplateResponse(
            request=request,
            name="dove_vedere_hub.html",
            context={
                "request": request,
                "items": [],
                "content_type": None,
                "page": 1,
                "total_pages": 1,
                "total": 0,
                "_not_found_slug": slug,
            },
            status_code=404,
        )

    # Detail TMDb (cached) — solo per il titolo principale
    if item["content_type"] == "tv":
        detail = get_detail_tv(item["tmdb_id"]) or {}
    else:
        detail = get_detail_movie(item["tmdb_id"]) or {}

    # Fallback poster URL
    if not detail.get("poster_url") and item.get("poster_path"):
        detail["poster_url"] = f"https://image.tmdb.org/t/p/w500{item['poster_path']}"

    # 12 simili dall'algoritmo (cached 7gg)
    # Anti-OOM: se è un bot e la cache è miss, ritorna lista vuota invece di
    # triggerare l'engine recommend (che è il pezzo che fa esplodere la RAM).
    # Gli utenti reali continuano a generare la cache normalmente; i bot poi
    # leggeranno dalla cache calda. Pagina resta indicizzabile (titolo+detail ci sono).
    if _is_bot(request):
        from core.tmdb_cache import cache_get
        cache_key = f"seo:similar:v5:{item['content_type']}:{item['tmdb_id']}:12"
        cached = cache_get(cache_key)
        similar_items = cached if cached else []
    else:
        similar_items = get_similar_for_seo(slug, limit=12)

    return templates.TemplateResponse(
        request=request,
        name="come.html",
        context={
            "request": request,
            "item": item,
            "detail": detail,
            "similar_items": similar_items,
        },
    )


@app.get("/admin/refresh-seo")
def admin_refresh_seo(request: Request):
    """Rigenera le 800 pagine SEO da TMDb. Solo admin loggato.
    Da chiamare manualmente o via cron settimanale."""
    if not _check_admin(request):
        return RedirectResponse(url="/admin", status_code=302)

    try:
        result = populate_seo_titles_db()
        return {"status": "ok", **result}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/admin/seo-stats")
def admin_seo_stats(request: Request):
    """Quante pagine SEO sono attualmente in DB."""
    if not _check_admin(request):
        return RedirectResponse(url="/admin", status_code=302)

    return {
        "total": seo_titles_count(),
        "movies": list_seo_titles(content_type="movie", page=1, per_page=1)[1],
        "tv": list_seo_titles(content_type="tv", page=1, per_page=1)[1],
    }


@app.get("/admin/debug-come/{slug}")
def admin_debug_come(request: Request, slug: str):
    """
    Diagnostico /come/{slug}: mostra cosa restituisce ogni step.
    NON cachato — esegue tutto live per debugging.
    """
    if not _check_admin(request):
        return RedirectResponse(url="/admin", status_code=302)

    # Step 1: trovo il titolo
    item = get_title_by_slug(slug)
    if not item:
        return {"error": f"slug '{slug}' non trovato in seo_titles"}

    out = {
        "slug": slug,
        "item": {k: v for k, v in item.items() if k != "overview"},
        "steps": {}
    }

    # Step 2: chiamo l'algoritmo
    title = item["title"]
    content_type = item["content_type"]

    try:
        if content_type == "tv":
            from core.recommendation_tv import recommend_tv_from_seed_titles
            res = recommend_tv_from_seed_titles([title], top_k=17)
        else:
            from core.recommendation_api import recommend_from_seed_titles
            res = recommend_from_seed_titles([title], top_k=17)

        recs = res.get("recommendations", []) or []
        out["steps"]["1_motore"] = {
            "candidati": len(recs),
            "resolved_seeds": len(res.get("resolved_seeds", []) or []),
            "missing_titles": res.get("missing_titles", []) or [],
            "primi_3": [
                {"title": r.get("title"), "tv_id": r.get("tv_id"), "tmdb_id": r.get("tmdb_id"),
                 "poster_path": r.get("poster_path")}
                for r in recs[:3]
            ],
        }
    except Exception as e:
        import traceback
        out["steps"]["1_motore_ERROR"] = {"error": str(e), "traceback": traceback.format_exc()}

    # Step 3: TMDb similar/recommended (per fallback)
    tmdb_id = item.get("tmdb_id")
    try:
        if content_type == "tv":
            from core.recommendation_tv import get_similar_tv, get_recommended_tv
            sim = get_similar_tv(tmdb_id, limit=20) or []
            rec = get_recommended_tv(tmdb_id, limit=20) or []
            out["steps"]["2_tmdb_fallback"] = {
                "similar": len(sim),
                "recommended": len(rec),
                "primi_3_similar": [{"title": x.get("title"), "tv_id": x.get("tv_id"),
                                     "poster_path": x.get("poster_path")} for x in sim[:3]],
                "primi_3_recommended": [{"title": x.get("title"), "tv_id": x.get("tv_id"),
                                         "poster_path": x.get("poster_path")} for x in rec[:3]],
            }
        else:
            from core.recommendation_api import get_similar_movies_tmdb
            sim = get_similar_movies_tmdb(tmdb_id, limit=20) or []
            out["steps"]["2_tmdb_fallback"] = {
                "similar": len(sim),
                "primi_3": [{"title": x.get("title"), "tmdb_id": x.get("tmdb_id"),
                             "poster_path": x.get("poster_path")} for x in sim[:3]],
            }
    except Exception as e:
        import traceback
        out["steps"]["2_tmdb_ERROR"] = {"error": str(e), "traceback": traceback.format_exc()}

    # Step 4: chiamata completa
    try:
        from core.seo_pages import _compute_similar_for_seo
        final = _compute_similar_for_seo(item, limit=12)
        out["steps"]["3_finale"] = {
            "totali": len(final),
            "primi_5": [{"title": x["title"], "tmdb_id": x["tmdb_id"],
                         "has_poster": bool(x["poster_url"])} for x in final[:5]],
        }
    except Exception as e:
        import traceback
        out["steps"]["3_finale_ERROR"] = {"error": str(e), "traceback": traceback.format_exc()}

    # Step 5: cache check — c'è una entry stale?
    try:
        import sqlite3 as _sql
        conn = _sql.connect(os.environ.get("DATABASE_PATH") or "app/cosaguardo.db", timeout=3)
        cur = conn.execute(
            "SELECT cache_key, length(value_json), expires_at FROM tmdb_cache WHERE cache_key LIKE ?",
            (f"seo:similar:%:{content_type}:{tmdb_id}:%",)
        )
        out["steps"]["4_cache"] = [
            {"key": r[0], "size_bytes": r[1], "expires_at": r[2]}
            for r in cur.fetchall()
        ]
        conn.close()
    except Exception as e:
        out["steps"]["4_cache_ERROR"] = str(e)

    return out


@app.get("/admin/cache-purge-come/{slug}")
def admin_cache_purge_come(request: Request, slug: str):
    """Cancella TUTTE le entry cache /come per uno slug specifico (tutte le versioni v1, v2, ...)."""
    if not _check_admin(request):
        return RedirectResponse(url="/admin", status_code=302)

    item = get_title_by_slug(slug)
    if not item:
        return {"error": f"slug '{slug}' non trovato"}

    try:
        import sqlite3 as _sql
        conn = _sql.connect(os.environ.get("DATABASE_PATH") or "app/cosaguardo.db", timeout=3)
        cur = conn.execute(
            "DELETE FROM tmdb_cache WHERE cache_key LIKE ?",
            (f"seo:similar:%:{item['content_type']}:{item['tmdb_id']}:%",)
        )
        deleted = cur.rowcount
        conn.commit()
        conn.close()

        # Pulisco anche cache memoria L1
        from core.tmdb_cache import _mem_cache, _mem_lock
        with _mem_lock:
            keys_to_remove = [k for k in _mem_cache.keys()
                              if k.startswith("seo:similar:") and f":{item['tmdb_id']}:" in k]
            for k in keys_to_remove:
                _mem_cache.pop(k, None)

        return {
            "status": "ok",
            "slug": slug,
            "tmdb_id": item["tmdb_id"],
            "deleted_db_rows": deleted,
            "cleared_memory_keys": len(keys_to_remove),
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/admin/memory-stats")
def admin_memory_stats(request: Request):
    """
    Statistiche memoria del processo Python.
    Utile per diagnosticare OOM crashes su Render.
    """
    if not _check_admin(request):
        return RedirectResponse(url="/admin", status_code=302)

    try:
        import resource, sys
        # Memoria max usata dal processo (in KB su Linux, bytes su macOS)
        rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        rss_mb = round(rss_kb / 1024, 1)  # Linux: KB → MB

        # Cache memoria
        from core.tmdb_cache import _mem_cache, _mem_lock, MAX_MEMORY_KEYS
        with _mem_lock:
            cache_count = len(_mem_cache)
            # Stima dimensione cache (rough — usa sys.getsizeof sui values)
            try:
                cache_size_bytes = sum(sys.getsizeof(v) for v in _mem_cache.values())
                cache_size_mb = round(cache_size_bytes / (1024*1024), 2)
            except Exception:
                cache_size_mb = -1

        return {
            "process_rss_mb": rss_mb,
            "render_starter_limit_mb": 512,
            "process_pct_used": round(100 * rss_mb / 512, 1),
            "cache_memory": {
                "count": cache_count,
                "max_allowed": MAX_MEMORY_KEYS,
                "estimated_size_mb": cache_size_mb,
            },
            "warning": "Process > 80% del limite" if rss_mb > 410 else None,
        }
    except Exception as e:
        return {"error": str(e)}


# ─── Cache sitemap (1h TTL — non cambia spesso) ────────────────────────
_sitemap_cache: dict = {"xml": None, "ts": 0.0}
_SITEMAP_TTL = 3600  # 1 ora


@app.get("/robots.txt")
def robots_txt():
    """
    robots.txt — dice ai crawler dove trovare la sitemap e cosa NON crawlare.
    Fondamentale per Google: prima cerca questo file, POI la sitemap.
    """
    content = """User-agent: *
Allow: /

# Non indicizzare aree admin/tecniche
Disallow: /admin/
Disallow: /api/
Disallow: /static/sw.js
Disallow: /tmdb-id
Disallow: /watch-providers
Disallow: /feedback

# Non indicizzare pagine utente (private)
Disallow: /profilo
Disallow: /login
Disallow: /register
Disallow: /logout

# Sitemap
Sitemap: https://cosaguardo.com/sitemap.xml
"""
    return Response(content=content, media_type="text/plain")


@app.get("/sitemap.xml")
def sitemap():
    """
    Sitemap dinamica cached 1h.
    Include: pagine statiche + hub + tutti gli slug /dove-vedere/ e /come/.
    NON fa chiamate TMDb live (per affidabilità verso Google crawler).
    """
    # Cache check (1h TTL)
    import time as _t
    now = _t.time()
    if _sitemap_cache["xml"] and (now - _sitemap_cache["ts"]) < _SITEMAP_TTL:
        return Response(content=_sitemap_cache["xml"], media_type="application/xml")

    base = "https://cosaguardo.com"
    today = datetime.now().strftime("%Y-%m-%d")

    # Pagine statiche
    static_urls = [
        ("",                 "daily",   "1.0"),
        ("/dove-vedere",     "daily",   "0.9"),
        ("/dove-vedere?tipo=film",  "daily", "0.7"),
        ("/dove-vedere?tipo=serie", "daily", "0.7"),
        ("/installa",        "monthly", "0.5"),
        ("/privacy",         "yearly",  "0.3"),
        ("/termini",         "yearly",  "0.3"),
    ]

    # Tutti gli slug /dove-vedere/* e /come/* dal DB locale (~750+750)
    seo_entries = []
    try:
        seo_entries = list_all_slugs_for_sitemap()
    except Exception:
        pass

    xml_parts = ['<?xml version="1.0" encoding="UTF-8"?>',
                 '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']

    # 1. Pagine statiche
    for path, freq, priority in static_urls:
        xml_parts.append(
            f"  <url><loc>{base}{path}</loc><lastmod>{today}</lastmod>"
            f"<changefreq>{freq}</changefreq><priority>{priority}</priority></url>"
        )

    # 2. Pagine /dove-vedere/{slug}
    for slug, ctype, updated_at in seo_entries:
        try:
            lastmod = datetime.fromtimestamp(updated_at).strftime("%Y-%m-%d") if updated_at else today
        except Exception:
            lastmod = today
        xml_parts.append(
            f"  <url><loc>{base}/dove-vedere/{slug}</loc><lastmod>{lastmod}</lastmod>"
            f"<changefreq>weekly</changefreq><priority>0.7</priority></url>"
        )

    # 3. Pagine /come/{slug}
    for slug, ctype, updated_at in seo_entries:
        try:
            lastmod = datetime.fromtimestamp(updated_at).strftime("%Y-%m-%d") if updated_at else today
        except Exception:
            lastmod = today
        xml_parts.append(
            f"  <url><loc>{base}/come/{slug}</loc><lastmod>{lastmod}</lastmod>"
            f"<changefreq>monthly</changefreq><priority>0.6</priority></url>"
        )

    xml_parts.append("</urlset>")
    xml = "\n".join(xml_parts)

    # Salva in cache
    _sitemap_cache["xml"] = xml
    _sitemap_cache["ts"]  = now

    return Response(content=xml, media_type="application/xml")
# ──────────────────────────────────────────────────────────────────────────


@app.get("/privacy", response_class=HTMLResponse)
def privacy(request: Request):
    return templates.TemplateResponse(request=request, name="privacy.html", context={"request": request})


@app.get("/termini", response_class=HTMLResponse)
def termini(request: Request):
    return templates.TemplateResponse(request=request, name="termini.html", context={"request": request})


@app.get("/admin/db-cache-stats")
def admin_db_cache_stats(request: Request):
    """Statistiche tabella tmdb_cache (count, size, expired, pct cap)."""
    if not _check_admin(request):
        return RedirectResponse(url="/admin", status_code=302)
    from core.tmdb_cache import cache_db_count_and_size
    return cache_db_count_and_size()


@app.get("/admin/db-cache-trim")
def admin_db_cache_trim(request: Request, keep: int = 30000):
    """
    Trim manuale: cancella entry più vecchie finché ne restano solo `keep`.
    Esegue DELETE a batch da 5000 righe per evitare timeout SQLite.
    Esempio: /admin/db-cache-trim?keep=20000

    Se la risposta ha "partial": true, rilancia la stessa URL per continuare.
    """
    if not _check_admin(request):
        return RedirectResponse(url="/admin", status_code=302)
    if keep < 1000 or keep > 100000:
        return {"error": "keep deve essere tra 1000 e 100000"}
    from core.tmdb_cache import cache_db_trim_to, cache_db_count_and_size
    trim_result = cache_db_trim_to(keep)
    after = cache_db_count_and_size()
    return {"status": "ok", "trim": trim_result, "after": after}


@app.get("/admin/db-vacuum")
def admin_db_vacuum(request: Request):
    """
    Esegue VACUUM sul DB SQLite per recuperare spazio fisico post-trim.
    Operazione bloccante: il sito può rispondere lentamente per 30-90 secondi.
    Lancia in momenti di basso traffico.

    Richiede ~1× la dimensione attuale del DB libera sul disco.
    Verificalo prima con: /admin/db-cache-stats e Render Disk metrics.
    """
    if not _check_admin(request):
        return RedirectResponse(url="/admin", status_code=302)
    from core.tmdb_cache import db_vacuum
    return db_vacuum()


@app.get("/admin/perf-stats")
def admin_perf_stats(request: Request, sort: str = "avg"):
    """
    Riassunto performance per route pattern dall'avvio del processo.
    Resetta a ogni restart Render. Ordinabile via ?sort=avg|max|count|slow.

    Esempio: /admin/perf-stats?sort=avg → route più lente in media (target principale)
             /admin/perf-stats?sort=slow → route che superano più spesso 500ms
             /admin/perf-stats?sort=count → route più chiamate
    """
    if not _check_admin(request):
        return RedirectResponse(url="/admin", status_code=302)

    with _perf_stats_lock:
        snapshot = {k: dict(v) for k, v in _perf_stats.items()}

    rows = []
    for route, s in snapshot.items():
        avg = s["total_ms"] / s["count"] if s["count"] else 0
        rows.append({
            "route": route,
            "count": s["count"],
            "avg_ms": round(avg, 1),
            "max_ms": s["max_ms"],
            "slow_count": s["slow_count"],
            "slow_pct": round(100 * s["slow_count"] / s["count"], 1) if s["count"] else 0,
            "bot": s["bot_count"],
            "usr": s["usr_count"],
            "last_status": s["last_status"],
        })

    sort_keys = {
        "avg":   lambda r: -r["avg_ms"],
        "max":   lambda r: -r["max_ms"],
        "count": lambda r: -r["count"],
        "slow":  lambda r: -r["slow_count"],
    }
    rows.sort(key=sort_keys.get(sort, sort_keys["avg"]))

    return {
        "slow_threshold_ms": _PERF_SLOW_MS,
        "patterns_tracked": len(rows),
        "sorted_by": sort,
        "routes": rows,
    }


@app.get("/admin/perf-reset")
def admin_perf_reset(request: Request):
    """Resetta i contatori perf-stats. Utile per fare misure pulite di una sessione."""
    if not _check_admin(request):
        return RedirectResponse(url="/admin", status_code=302)
    with _perf_stats_lock:
        cleared = len(_perf_stats)
        _perf_stats.clear()
    return {"status": "ok", "cleared_patterns": cleared}


@app.get("/admin/flush-cache")
def admin_flush_cache(request: Request):
    """Svuota tutte le cache in-memory. Solo admin loggato."""
    if not _check_admin(request):
        return RedirectResponse(url="/admin", status_code=302)

    cleared = []

    for cache, label in [
        (_trending_cache, "trending"),
        (_news_cache,     "news"),
        (_toprated_cache, "top_rated"),
    ]:
        cache["data"] = None
        cache["ts"]   = 0.0
        cleared.append(label)

    _cinema_cache["now_playing"] = None
    _cinema_cache["upcoming"]    = None
    _cinema_cache["ts"]          = 0.0
    cleared.append("cinema")

    _strips_cache.clear()
    cleared.append("strips")

    return {"status": "ok", "cleared": cleared}


@app.get("/persona/{person_id}", response_class=HTMLResponse)
def persona_detail(request: Request, person_id: int):
    detail = get_person_detail(person_id)
    if not detail:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        request=request,
        name="persona.html",
        context={"request": request, "detail": detail},
    )


# ── Titoli SEO dinamici per /scopri ──────────────────────────────────────
def _scopri_seo(tipo, genere, mood, piattaforma, anno, voto=""):
    parts = []
    if genere:   parts.append(genere.capitalize())
    if mood:     parts.append(f"mood {mood}")
    if piattaforma: parts.append(piattaforma.capitalize())
    if anno == "recenti": parts.append("recenti")
    elif anno == "classici": parts.append("classici")
    if voto == "7": parts.append("7+")
    elif voto == "8": parts.append("8+")

    tipo_label = "serie TV" if tipo == "serie" else "film"
    if parts:
        title = f"I migliori {tipo_label} {' · '.join(parts)} — CosaGuardo"
        desc  = f"Scopri i migliori {tipo_label} {' '.join(parts)} consigliati dall\'algoritmo di CosaGuardo."
    else:
        title = f"Scopri {tipo_label} — CosaGuardo"
        desc  = f"Esplora {tipo_label} consigliati in base al tuo gusto su CosaGuardo."
    return title, desc


@app.get("/scopri", response_class=HTMLResponse)
def scopri(
    request: Request,
    tipo:        str = "film",
    genere:      str = "",
    mood:        str = "",
    piattaforma: str = "",
    anno:        str = "",
    voto:        str = "",
    page:        int = 1,
):
    has_filters = any([genere, mood, piattaforma, anno, voto])

    if has_filters:
        # Modalità filtrata — griglia paginata
        data = get_scopri_results(
            tipo=tipo, genere=genere, mood=mood,
            piattaforma=piattaforma, anno=anno, voto=voto, page=page
        )
        strips = []
        results = data["results"]
        has_next = (page * 20) < data["total"]
        has_prev = page > 1
    else:
        # Modalità home — strip per genere (cached)
        strips  = get_strips_cached(tipo=tipo)
        results = []
        has_next = has_prev = False

    seo_title, seo_desc = _scopri_seo(tipo, genere, mood, piattaforma, anno, voto)

    return templates.TemplateResponse(
        request=request,
        name="scopri.html",
        context={
            "request":     request,
            "strips":      strips,
            "results":     results,
            "has_next":    has_next,
            "has_prev":    has_prev,
            "page":        page,
            "has_filters": has_filters,
            "tipo":        tipo,
            "genere":      genere,
            "mood":        mood,
            "piattaforma": piattaforma,
            "anno":        anno,
            "voto":        voto,
            "seo_title":   seo_title,
            "seo_desc":    seo_desc,
        },
    )


@app.get("/scopri/json", response_class=JSONResponse)
def scopri_json(
    tipo: str = "film", genere: str = "", mood: str = "",
    piattaforma: str = "", anno: str = "", voto: str = "", page: int = 1,
):
    """AJAX endpoint per caricamento pagine successive."""
    return get_scopri_results(
        tipo=tipo, genere=genere, mood=mood,
        piattaforma=piattaforma, anno=anno, voto=voto, page=page
    )


# ── Admin ─────────────────────────────────────────────────────────────────
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")


def _check_admin(request: Request) -> bool:
    """Verifica sessione admin."""
    return request.session.get("is_admin") is True


@app.get("/admin", response_class=HTMLResponse)
def admin_login_page(request: Request):
    if _check_admin(request):
        return RedirectResponse(url="/admin/utenti", status_code=302)
    return templates.TemplateResponse(
        request=request, name="admin_login.html",
        context={"request": request, "error": ""}
    )


@app.post("/admin", response_class=HTMLResponse)
def admin_login_submit(request: Request, password: str = Form(...)):
    if ADMIN_PASSWORD and password == ADMIN_PASSWORD:
        request.session["is_admin"] = True
        return RedirectResponse(url="/admin/utenti", status_code=303)
    return templates.TemplateResponse(
        request=request, name="admin_login.html",
        context={"request": request, "error": "Password errata."}
    )


@app.get("/admin/utenti", response_class=HTMLResponse)
def admin_utenti(request: Request):
    if not _check_admin(request):
        return RedirectResponse(url="/admin", status_code=302)
    stats = get_admin_stats()
    return templates.TemplateResponse(
        request=request, name="admin_utenti.html",
        context={"request": request, "stats": stats}
    )


@app.post("/admin/logout")
def admin_logout(request: Request):
    request.session.pop("is_admin", None)
    return RedirectResponse(url="/admin", status_code=303)
# ──────────────────────────────────────────────────────────────────────────

