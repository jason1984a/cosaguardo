from datetime import datetime
import hashlib

from core.recommendation_api import recommend_from_seed_titles, get_movie_tmdb_info
from core.recommendation_tv import recommend_tv_from_seed_titles

TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w342"

def parse_seed_titles(seed_titles_raw):
    if not seed_titles_raw:
        return []

    return [x.strip() for x in seed_titles_raw.split(",") if x.strip()]


def dedupe_keep_order(items):
    return list(dict.fromkeys([x for x in items if x]))


def collect_recent_seeds(searches, max_searches=6, max_titles=12):
    movie_titles = []
    tv_titles = []

    for search in searches[:max_searches]:
        try:
            seed_titles_raw = search["seed_titles"] if search["seed_titles"] else ""
            content_type = (search["content_type"] or "").strip().lower()
        except Exception:
            seed_titles_raw = search.get("seed_titles", "")
            content_type = (search.get("content_type") or "").strip().lower()

        titles = dedupe_keep_order(parse_seed_titles(seed_titles_raw))

        if content_type == "movie":
            movie_titles.extend(titles)
        elif content_type == "tv":
            tv_titles.extend(titles)

    movie_titles = dedupe_keep_order(movie_titles)[:max_titles]
    tv_titles = dedupe_keep_order(tv_titles)[:max_titles]

    return movie_titles, tv_titles


def stable_daily_pick(items, user_id, day_key, count=3):
    if not items:
        return []

    scored = []

    for item in items:
        title = (item["title"] or "") if "title" in item.keys() else ""
        seed = f"{user_id}-{day_key}-{title}".encode("utf-8")
        score = hashlib.md5(seed).hexdigest()
        scored.append((score, item))

    scored.sort(key=lambda x: x[0])
    return [item for _, item in scored[:count]]


def normalize_movie_rec(rec, tmdb_info=None):
    poster = rec.get("poster_path") or rec.get("poster_url")

    if poster and isinstance(poster, str) and poster.startswith("/"):
        poster = TMDB_IMAGE_BASE + poster

    tmdb_id = rec.get("tmdb_id")

    if not poster or not tmdb_id:
        if tmdb_info is None:
            try:
                tmdb_info = get_movie_tmdb_info(rec.get("title", ""))
            except Exception:
                tmdb_info = {}
        if not poster:
            poster = (tmdb_info or {}).get("poster_url")
        if not tmdb_id:
            tmdb_id = (tmdb_info or {}).get("tmdb_id")

    return {
        "title": rec.get("title", ""),
        "content_type": "movie",
        "reason": rec.get("explanation") or "Compatibile con i tuoi gusti recenti.",
        "score": round(rec.get("match_score", 0), 1) if rec.get("match_score") else None,
        "poster_url": poster,
        "tmdb_id": tmdb_id,
    }


def normalize_tv_rec(rec):
    poster = rec.get("poster_path") or rec.get("poster_url")

    if poster and poster.startswith("/"):
        poster = TMDB_IMAGE_BASE + poster

    return {
        "title": rec.get("title", ""),
        "content_type": "tv",
        "reason": rec.get("explanation") or "Compatibile con i tuoi gusti recenti.",
        "score": round(rec.get("match_score", 0), 1) if rec.get("match_score") else None,
        "poster_url": poster,
        "tmdb_id": rec.get("tv_id") or rec.get("tmdb_id"),
    }


def build_dashboard_recommendations(
    user_id,
    searches,
    liked_titles=None,
    taste_profile=None,
    per_type_pool=12,
    final_count=3
):
    movie_titles, tv_titles = collect_recent_seeds(searches)

    # ---- NEW: integra liked_titles ----
    if liked_titles:
        for item in liked_titles:
            title = (item["title"] or "").strip() if "title" in item.keys() else ""
            ctype = (item["content_type"] or "").strip().lower() if "content_type" in item.keys() else ""

            if not title:
                continue

            if ctype == "movie" and title not in movie_titles:
                movie_titles.insert(0, title)

            elif ctype == "tv" and title not in tv_titles:
                tv_titles.insert(0, title)

    # limitiamo per sicurezza
    movie_titles = movie_titles[:12]
    tv_titles = tv_titles[:12]

    pool = []

    if len(movie_titles) >= 2:
        try:
            movie_result = recommend_from_seed_titles(
                movie_titles[:6],
                top_k=per_type_pool,
                per_seed_limit=20,
            )
            recs = movie_result.get("recommendations", [])
            # Fetch TMDb info in parallel only for recs missing poster
            needs_fetch = [r for r in recs if not (r.get("poster_path") or r.get("poster_url"))]
            has_poster  = [r for r in recs if r.get("poster_path") or r.get("poster_url")]

            from concurrent.futures import ThreadPoolExecutor, as_completed
            tmdb_map = {}
            if needs_fetch:
                with ThreadPoolExecutor(max_workers=6) as ex:
                    futures = {ex.submit(get_movie_tmdb_info, r["title"]): r["title"]
                               for r in needs_fetch}
                    for fut in as_completed(futures):
                        title = futures[fut]
                        try:
                            tmdb_map[title] = fut.result()
                        except Exception:
                            tmdb_map[title] = {}

            for rec in has_poster:
                pool.append(normalize_movie_rec(rec))
            for rec in needs_fetch:
                pool.append(normalize_movie_rec(rec, tmdb_info=tmdb_map.get(rec["title"])))
        except Exception:
            pass

    if len(tv_titles) >= 2:
        try:
            tv_result = recommend_tv_from_seed_titles(tv_titles[:6])
            for rec in tv_result.get("recommendations", [])[:per_type_pool]:
                pool.append(normalize_tv_rec(rec))
        except Exception:
            pass

    if not pool:
        return []

    # dedup per titolo + tipo
    unique = []
    seen = set()

    for rec in pool:
        key = (rec.get("content_type", ""), rec.get("title", "").strip().lower())
        if key in seen:
            continue
        seen.add(key)
        unique.append(rec)

    day_key = datetime.now().strftime("%Y-%m-%d")
    # ordina per punteggio
    unique.sort(key=lambda x: x.get("score") or 0, reverse=True)

    # poi applica random giornaliero sui top
    top_candidates = unique[:10]

    picks = stable_daily_pick(top_candidates, user_id=user_id, day_key=day_key, count=final_count)

    return picks