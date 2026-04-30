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
        return RedirectResponse(url="/dashboard", status_code=303)

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
        return RedirectResponse(url="/dashboard", status_code=303)

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


@app.get("/dashboard-legacy", response_class=HTMLResponse)
def dashboard_legacy(request: Request):
    user_id = request.session.get("user_id")

    if not user_id:
        return RedirectResponse(url="/login", status_code=303)

    user = get_user_by_id(user_id)
    if not user:
        request.session.clear()
        return RedirectResponse(url="/login", status_code=303)

    searches = get_searches_by_user(user_id, limit=10)
    liked_titles = [dict(row) for row in get_liked_states_by_user(user_id)]

    for item in liked_titles:
        item["poster_url"] = item.get("poster_url") or ""

        if item["content_type"] == "movie" and not item["poster_url"]:
            tmdb_info = get_movie_tmdb_info(item["title"])
            item["poster_url"] = tmdb_info.get("poster_url", "") if tmdb_info else ""

        elif item["content_type"] == "tv" and not item["poster_url"]:
            tv_info = find_tv_by_title(item["title"])
            if tv_info and tv_info.get("poster_path"):
                item["poster_url"] = f"https://image.tmdb.org/t/p/w342{tv_info['poster_path']}"

    taste_profile = build_taste_profile(searches)

    today_key = datetime.now().strftime("%Y-%m-%d")
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
        name="dashboard.html",
        context={
            "request": request,
            "user_email": user["email"],
            "searches": searches,
            "liked_titles": liked_titles,
            "taste_profile": taste_profile,
            "recommendations": recommendations,
            "tmdb_api_key": os.environ.get("TMDB_API_KEY", ""),
        },
    )

@app.post("/register", response_class=HTMLResponse)
def register_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...)
):
    email = email.strip().lower()

    if not email:
        return templates.TemplateResponse(
            request=request,
            name="register.html",
            context={
                "request": request,
                "error": "Inserisci una email valida.",
                "email": email
            },
        )

    if len(password) < 6:
        return templates.TemplateResponse(
            request=request,
            name="register.html",
            context={
                "request": request,
                "error": "La password deve avere almeno 6 caratteri.",
                "email": email
            },
        )

    if password != confirm_password:
        return templates.TemplateResponse(
            request=request,
            name="register.html",
            context={
                "request": request,
                "error": "Le password non coincidono.",
                "email": email
            },
        )

    existing_user = get_user_by_email(email)
    if existing_user:
        return templates.TemplateResponse(
            request=request,
            name="register.html",
            context={
                "request": request,
                "error": "Esiste già un account con questa email.",
                "email": email
            },
        )

    user_id = create_user(email, password, first_name=first_name, last_name=last_name, birth_date=birth_date)

    request.session["user_id"] = user_id
    request.session["user_email"] = email

    return RedirectResponse(url="/dashboard", status_code=303)

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

    return RedirectResponse(url="/dashboard", status_code=303)

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
    for rec in recommendations:
        if content_type == "movie":
            tmdb_info = get_movie_tmdb_info(rec["title"])
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

    # Poster e tmdb_id per liked — recupero parallelo
    def _enrich_liked(item):
        item["poster_url"] = item.get("poster_url") or ""
        if item["content_type"] == "movie":
            tmdb_info = get_movie_tmdb_info(item["title"])
            if tmdb_info:
                item["poster_url"] = item["poster_url"] or tmdb_info.get("poster_url", "")
                item["tmdb_id"]    = tmdb_info.get("tmdb_id")
            else:
                item["tmdb_id"] = None
        else:
            tv_info = find_tv_by_title(item["title"])
            if tv_info and tv_info.get("poster_path"):
                item["poster_url"] = item["poster_url"] or f"https://image.tmdb.org/t/p/w342{tv_info['poster_path']}"
                item["tmdb_id"]    = tv_info.get("id") or tv_info.get("tv_id")
            else:
                item["tmdb_id"] = None
        return item

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(_enrich_liked, item): item for item in liked_titles}
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception:
                pass

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
            "recommendations": recommendations,
            "searches":        searches,
            "tmdb_api_key":    os.environ.get("TMDB_API_KEY", ""),
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

GOOGLE_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI  = os.environ.get("GOOGLE_REDIRECT_URI", "https://cosaguardo.com/auth/google/callback")


@app.get("/auth/google")
def google_login(request: Request):
    """Redirect a Google per il login OAuth."""
    import urllib.parse
    params = {
        "client_id":     GOOGLE_CLIENT_ID,
        "redirect_uri":  GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope":         "openid email profile",
        "access_type":   "online",
        "prompt":        "select_account",
    }
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
    return RedirectResponse(url=url)


@app.get("/auth/google/callback")
def google_callback(request: Request, code: str = "", error: str = ""):
    """Callback Google OAuth — crea o logga l'utente."""
    if error or not code:
        return RedirectResponse(url="/login?error=google_cancelled", status_code=302)

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
            return RedirectResponse(url="/login?error=google_failed", status_code=302)

        # 2. Recupera info utente da Google
        userinfo_resp = httpx.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
        userinfo = userinfo_resp.json()
        email = userinfo.get("email", "").strip().lower()

        if not email:
            return RedirectResponse(url="/login?error=google_no_email", status_code=302)

        # 3. Crea o recupera l'utente
        user = get_user_by_email(email)
        if not user:
            # Nuovo utente — crea con password casuale (non usata per login Google)
            import secrets
            user_id = create_user(email, secrets.token_hex(32))
        else:
            user_id = user["id"]

        # 4. Setta la sessione
        request.session["user_id"]    = user_id
        request.session["user_email"] = email

        return RedirectResponse(url="/profilo", status_code=302)

    except Exception as e:
        return RedirectResponse(url="/login?error=google_error", status_code=302)
# ──────────────────────────────────────────────────────────────────────────

# ─── Sitemap.xml ──────────────────────────────────────────────────────────
from fastapi.responses import Response

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

