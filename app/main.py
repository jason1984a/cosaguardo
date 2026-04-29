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


@app.get("/")
def home(request: Request):
    trending = get_trending_cached(limit=12)
    user_id = request.session.get("user_id")
    cinema = get_cinema_cached()
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
    Consigli personalizzati per la home (carosello Netflix-style).
    Restituisce una lista di raccomandazioni basate su preferiti e ricerche recenti.
    Solo per utenti loggati — risponde 401 se non autenticato.
    """
    user_id = request.session.get("user_id")
    if not user_id:
        from fastapi.responses import JSONResponse as _JSONResponse
        return _JSONResponse(status_code=401, content={"error": "not_logged_in"})

    searches = get_searches_by_user(user_id, limit=10)
    liked_titles = [dict(row) for row in get_liked_states_by_user(user_id)]

    # Recupera poster per i liked
    for item in liked_titles:
        item["poster_url"] = item.get("poster_url") or ""
        if item["content_type"] == "movie" and not item["poster_url"]:
            tmdb_info = get_movie_tmdb_info(item["title"])
            item["poster_url"] = tmdb_info.get("poster_url", "") if tmdb_info else ""
        elif item["content_type"] == "tv" and not item["poster_url"]:
            tv_info = find_tv_by_title(item["title"])
            if tv_info and tv_info.get("poster_path"):
                item["poster_url"] = f"https://image.tmdb.org/t/p/w342{tv_info['poster_path']}"

    # Usa lo stesso motore del dashboard ma con pool più ampio
    picks = build_dashboard_recommendations(
        user_id=user_id,
        searches=searches,
        liked_titles=liked_titles,
        per_type_pool=18,
        final_count=12,
    )

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

    user_id = create_user(email, password)

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