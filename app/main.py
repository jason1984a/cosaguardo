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

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

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
    if not title:
        return title

    suffixes = [", The", ", A", ", An", ", La", ", Le", ", Les", ", Il", ", Lo", ", L'"]

    for suffix in suffixes:
        if title.endswith(suffix):
            base = title[:-len(suffix)].strip()
            article = suffix[2:].strip()
            return f"{article} {base}"

    return title



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


@app.get("/sitemap.xml")
def sitemap():
    """
    Sitemap dinamica con pagine statiche + top film/serie da TMDb.
    Aggiornata ad ogni richiesta (cached dal CDN di Render/browser).
    """
    base = "https://cosaguardo.com"

    # Pagine statiche
    static_urls = [
        ("",        "daily",   "1.0"),
        ("/login",  "monthly", "0.5"),
        ("/register","monthly","0.5"),
    ]

    # Top film popolari da TMDb (per indicizzazione schede)
    movie_ids = []
    tv_ids    = []
    try:
        r = __import__("requests").get(
            "https://api.themoviedb.org/3/movie/popular",
            params={"api_key": os.environ.get("TMDB_API_KEY",""), "language":"it-IT", "page":1},
            timeout=5
        )
        for item in r.json().get("results",[])[:20]:
            if item.get("id"): movie_ids.append(item["id"])
    except Exception:
        pass

    try:
        r = __import__("requests").get(
            "https://api.themoviedb.org/3/tv/popular",
            params={"api_key": os.environ.get("TMDB_API_KEY",""), "language":"it-IT", "page":1},
            timeout=5
        )
        for item in r.json().get("results",[])[:20]:
            if item.get("id"): tv_ids.append(item["id"])
    except Exception:
        pass

    today = datetime.now().strftime("%Y-%m-%d")

    xml = '''<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'''

    for path, freq, priority in static_urls:
        xml += f"""
  <url>
    <loc>{base}{path}</loc>
    <lastmod>{today}</lastmod>
    <changefreq>{freq}</changefreq>
    <priority>{priority}</priority>
  </url>"""

    for mid in movie_ids:
        xml += f"""
  <url>
    <loc>{base}/film/{mid}</loc>
    <lastmod>{today}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.8</priority>
  </url>"""

    for tid in tv_ids:
        xml += f"""
  <url>
    <loc>{base}/serie/{tid}</loc>
    <lastmod>{today}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.8</priority>
  </url>"""

    xml += "\n</urlset>"

    return Response(content=xml, media_type="application/xml")
# ──────────────────────────────────────────────────────────────────────────


@app.get("/privacy", response_class=HTMLResponse)
def privacy(request: Request):
    return templates.TemplateResponse(request=request, name="privacy.html", context={"request": request})


@app.get("/termini", response_class=HTMLResponse)
def termini(request: Request):
    return templates.TemplateResponse(request=request, name="termini.html", context={"request": request})


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

