import os
import sys
from fastapi import FastAPI, Form, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from core.recommendation_api import (
    recommend_from_seed_titles,
    search_movies,
    get_movie_tmdb_info,
)

from core.recommendation_tv import recommend_tv_from_seed_titles, search_tv_series

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

app = FastAPI()

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
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={},
    )


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

    print("DEBUG TV RESULTS COUNT:", len(result["recommendations"]))
    print("DEBUG TV TITLES:", [r["title"] for r in result["recommendations"]])

    resolved_seeds = result["resolved_seeds"]
    missing_titles = result["missing_titles"]
    recommendations = result["recommendations"]

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