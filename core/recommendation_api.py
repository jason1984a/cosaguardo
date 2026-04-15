import os
import sqlite3
import re
import requests
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "db", "coseguardo.db")

def simple_similarity(a, b):
    score = 0

    # generi
    if set(a.get("genres", [])) & set(b.get("genres", [])):
        score += 1

    # keywords
    if set(a.get("keywords", [])) & set(b.get("keywords", [])):
        score += 1

    return score

def normalize_title(title: str) -> str:
    title = title.lower().strip()
    title = re.sub(r"\(\d{4}\)", "", title)
    title = re.sub(r"[^a-z0-9\s]", " ", title)
    title = re.sub(r"\s+", " ", title).strip()

    stop = {"the", "a", "an"}
    tokens = [t for t in title.split() if t not in stop]

    return " ".join(tokens)


def token_overlap(a: str, b: str) -> float:
    sa = set(normalize_title(a).split())
    sb = set(normalize_title(b).split())

    if not sa or not sb:
        return 0.0

    return len(sa & sb) / len(sa | sb)


def get_franchise_key(title: str) -> str:
    t = normalize_title(title)

    franchise_aliases = {
        "lord of the rings": "tolkien",
        "fellowship of the ring": "tolkien",
        "two towers": "tolkien",
        "return of the king": "tolkien",
        "hobbit": "tolkien",

        "harry potter": "wizarding_world",
        "fantastic beasts": "wizarding_world",

        "batman": "batman",
        "dark knight": "batman",

        "spider man": "spiderman",
        "amazing spider man": "spiderman",

        "superman": "superman",
    }

    for pattern, key in franchise_aliases.items():
        if pattern in t:
            return key

    tokens = t.split()

    sequel_markers = {
        "2", "3", "4", "5",
        "ii", "iii", "iv", "v",
        "part", "chapter", "begins", "returns", "rises",
        "robin", "forever", "reload", "resurrections"
    }

    tokens = [tok for tok in tokens if tok not in sequel_markers]

    if not tokens:
        return ""

    return tokens[0]


def is_same_franchise(seed_title: str, candidate_title: str) -> bool:
    ns = normalize_title(seed_title)
    nc = normalize_title(candidate_title)

    if not ns or not nc:
        return False

    if ns in nc or nc in ns:
        return True

    if get_franchise_key(seed_title) == get_franchise_key(candidate_title):
        return True

    overlap = token_overlap(seed_title, candidate_title)
    if overlap >= 0.5:
        return True

    return False

def is_sequel(title: str) -> bool:
    t = normalize_title(title)

    sequel_markers = {
        "2", "3", "4", "5",
        "ii", "iii", "iv", "v",
        "part", "chapter",
        "returns", "rises", "begins",
        "reloaded", "resurrections",
        "again"
    }

    tokens = t.split()

    for tok in tokens:
        if tok in sequel_markers:
            return True

    return False

def get_connection():
    return sqlite3.connect(DB_PATH)

def build_movie_best_seed_title(rec):
    why_titles = rec.get("why_titles", [])
    if why_titles:
        return why_titles[0]
    return None


def build_movie_badge(rec):
    components = rec.get("components", {})

    collab = components.get("collab_score", 0)
    genre = components.get("genre_score", 0)
    tag = components.get("tag_score", 0)

    if collab >= genre and collab >= tag:
        return {"text": "🎯 Match forte", "type": "highlight"}

    if tag >= genre and tag >= collab:
        return {"text": "🧠 Più vicino ai tuoi gusti", "type": "mind"}

    if genre >= tag and genre >= collab:
        return {"text": "🎬 Stesso tipo di film", "type": "light"}

    return {"text": "✨ Consiglio", "type": "default"}


def build_movie_explanation(rec, index=0):
    best_seed_title = rec.get("best_seed_title")
    why_titles = rec.get("why_titles", [])
    components = rec.get("components", {})

    collab = components.get("collab_score", 0)
    genre = components.get("genre_score", 0)
    tag = components.get("tag_score", 0)

    if index == 0:
        return (
            "È il suggerimento più forte del gruppo: combina al meglio affinità, coerenza e potenziale interesse."
        )

    if why_titles:
        if len(why_titles) >= 2:
            return f"Unisce elementi di {why_titles[0]} e {why_titles[1]}, risultando molto coerente con i tuoi gusti."

        best_seed_title = why_titles[0]

        if collab >= genre and collab >= tag:
            return f"Se ti è piaciuto {best_seed_title}, questo è uno dei consigli più vicini alle tue scelte iniziali."

        if tag >= genre and tag >= collab:
            return f"Se ti è piaciuto {best_seed_title}, questo titolo richiama bene temi e atmosfera dei film che hai inserito."

        if genre >= tag and genre >= collab:
            return f"Se ti è piaciuto {best_seed_title}, questo resta molto coerente per stile e tipo di film."

        return f"Se ti è piaciuto {best_seed_title}, questo consiglio ha diversi elementi in comune con i tuoi input."

    return "Consigliato in base alla combinazione dei film che hai inserito."

def build_movie_ui_signals(rec):
    components = rec.get("components", {})

    avg_score = rec.get("avg_score", 0)
    genre_score = components.get("genre_score", 0)
    tag_score = components.get("tag_score", 0)
    collab_score = components.get("collab_score", 0)

    def level_label(value):
        if value >= 0.40:
            return "alto"
        if value >= 0.22:
            return "medio"
        return "basso"

    signals = []

    match_level = level_label(avg_score)
    signals.append({
        "icon": "🎯",
        "label": "Match",
        "value": match_level
    })

    if genre_score >= max(tag_score, collab_score):
        signals.append({
            "icon": "🎬",
            "label": "Genere",
            "value": "coerente" if genre_score >= 0.30 else "vicino"
        })
    elif tag_score >= max(genre_score, collab_score):
        signals.append({
            "icon": "🧠",
            "label": "Temi",
            "value": "simili" if tag_score >= 0.08 else "affini"
        })
    else:
        signals.append({
            "icon": "👥",
            "label": "Pubblico",
            "value": "simile" if collab_score >= 0.05 else "vicino"
        })

    if avg_score >= 0.30:
        vibe_value = "forte"
    elif avg_score >= 0.22:
        vibe_value = "buona"
    else:
        vibe_value = "soft"

    signals.append({
        "icon": "✨",
        "label": "Vibe",
        "value": vibe_value
    })

    return signals[:3]

def find_movie_by_title(title_query: str):
    conn = get_connection()
    cursor = conn.cursor()

    normalized = title_query.strip()

    # 1. match esatto
    cursor.execute("""
    SELECT movielens_movie_id, title
    FROM titles
    WHERE LOWER(title) = LOWER(?)
    LIMIT 1
    """, (normalized,))
    row = cursor.fetchone()

    # 2. match parziale
    if not row:
        cursor.execute("""
        SELECT movielens_movie_id, title
        FROM titles
        WHERE LOWER(title) LIKE LOWER(?)
        ORDER BY LENGTH(title) ASC
        LIMIT 1
        """, (f"%{normalized}%",))
        row = cursor.fetchone()

    # 3. match senza articolo finale ", The"
    if not row and normalized.lower().startswith("the "):
        alt_title = normalized[4:] + ", The"
        cursor.execute("""
        SELECT movielens_movie_id, title
        FROM titles
        WHERE LOWER(title) = LOWER(?)
        LIMIT 1
        """, (alt_title,))
        row = cursor.fetchone()

    conn.close()

    if not row:
        return None

    return {
        "movie_id": row[0],
        "title": row[1]
    }


def get_candidates_for_movie(source_movie_id: int, limit: int = 50):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        tr.target_movie_id,
        t.title,
        tr.score_raw,
        tr.shared_users,
        tr.collab_score,
        tr.genre_score,
        tr.tag_score,
        tr.quality_score_norm,
        tr.content_score,
        tr.pop_penalty_norm
    FROM title_relations tr
    JOIN titles t
        ON tr.target_movie_id = t.movielens_movie_id
    WHERE tr.source_movie_id = ?
    ORDER BY tr.score_raw DESC
    LIMIT ?
    """, (source_movie_id, limit))

    rows = cursor.fetchall()
    conn.close()

    results = []
    for row in rows:
        results.append({
            "movie_id": row[0],
            "title": row[1],
            "score_raw": row[2],
            "shared_users": row[3],
            "collab_score": row[4],
            "genre_score": row[5],
            "tag_score": row[6],
            "quality_score_norm": row[7],
            "content_score": row[8],
            "pop_penalty_norm": row[9],
        })

    return results


def recommend_from_seed_ids(seed_ids: list[int], top_k: int = 20, per_seed_limit: int = 50):
    aggregated = defaultdict(lambda: {
        "movie_id": None,
        "title": None,
        "total_score": 0.0,
        "appearances": 0,
        "best_score": 0.0,
        "from_seed_ids": [],
        "components": {
            "score_raw": 0.0,
            "collab_score": 0.0,
            "genre_score": 0.0,
            "tag_score": 0.0,
            "quality_score_norm": 0.0,
            "content_score": 0.0,
            "pop_penalty_norm": 0.0,
        }
    })

    seed_ids_set = set(seed_ids)

    for seed_id in seed_ids:
        candidates = get_candidates_for_movie(seed_id, limit=per_seed_limit)

        for c in candidates:
            target_id = c["movie_id"]

            if target_id in seed_ids_set:
                continue

            item = aggregated[target_id]
            item["movie_id"] = target_id
            item["title"] = c["title"]
            item["total_score"] += c["score_raw"]
            item["appearances"] += 1
            item["best_score"] = max(item["best_score"], c["score_raw"])

            if seed_id not in item["from_seed_ids"]:
                item["from_seed_ids"].append(seed_id)

            item["components"]["score_raw"] += c["score_raw"]
            item["components"]["collab_score"] += c["collab_score"]
            item["components"]["genre_score"] += c["genre_score"]
            item["components"]["tag_score"] += c["tag_score"]
            item["components"]["quality_score_norm"] += c["quality_score_norm"]
            item["components"]["content_score"] += c["content_score"]
            item["components"]["pop_penalty_norm"] += c["pop_penalty_norm"]

    results = []
    for _, item in aggregated.items():
        appearances = item["appearances"]

        item["avg_score"] = item["total_score"] / appearances if appearances else 0.0

        for key in item["components"]:
            item["components"][key] = item["components"][key] / appearances if appearances else 0.0

        item["why_seed_ids"] = item["from_seed_ids"][:]

        results.append(item)

    results.sort(
        key=lambda x: (
            x["appearances"],
            x["avg_score"],
            x["best_score"]
        ),
        reverse=True
    )

    return results[:top_k]

def get_movie_release_year(title: str):
    if not TMDB_API_KEY:
        return 0

    try:
        url = "https://api.themoviedb.org/3/search/movie"
        params = {
            "api_key": TMDB_API_KEY,
            "query": title,
            "language": "it-IT"
        }

        response = requests.get(url, params=params, timeout=5)
        data = response.json()

        if data.get("results"):
            first = data["results"][0]
            release_date = first.get("release_date")

            if release_date and len(release_date) >= 4:
                return int(release_date[:4])

    except Exception:
        return 0

    return 0

def recommend_from_seed_titles(seed_titles: list[str], top_k: int = 20, per_seed_limit: int = 50):
    resolved_seeds = []
    missing_titles = []

    for title in seed_titles:
        movie = find_movie_by_title(title)
        if movie:
            resolved_seeds.append(movie)
        else:
            missing_titles.append(title)

    seed_ids = [m["movie_id"] for m in resolved_seeds]

    # prendiamo più candidati del top_k finale, così il filtro franchise
    # non lavora su una lista troppo corta
    expanded_top_k = max(top_k * 5, 50)

    recommendations = recommend_from_seed_ids(
        seed_ids=seed_ids,
        top_k=expanded_top_k,
        per_seed_limit=per_seed_limit
    )

    seed_map = {m["movie_id"]: m["title"] for m in resolved_seeds}

    for rec in recommendations:
        rec["why_titles"] = [
            seed_map[sid]
            for sid in rec.get("why_seed_ids", [])
            if sid in seed_map
        ]

        penalty = 0.0

        for seed_title in seed_titles:
            if is_same_franchise(seed_title, rec["title"]):

                if is_sequel(rec["title"]):
                    penalty = max(penalty, 0.70)  # 🔥 più forte
                else:
                    penalty = max(penalty, 0.45)  # media
            else:
                overlap = token_overlap(seed_title, rec["title"])
                if overlap >= 0.5:
                    penalty = max(penalty, 0.20)

        components = rec.get("components", {})

        avg_score = rec.get("avg_score", 0)
        genre_score = components.get("genre_score", 0)
        tag_score = components.get("tag_score", 0)
        collab_score = components.get("collab_score", 0)
        appearances = rec.get("appearances", 1)

        base_score = (
            avg_score * 0.4 +
            collab_score * 0.2 +
            genre_score * 0.2 +
            tag_score * 0.1 +
            min(appearances, 2) * 0.1
        )
        multi_seed_bonus = 0

        if len(rec.get("why_seed_ids", [])) >= 2:
            multi_seed_bonus = 0.05

        rec["final_score"] = (base_score + multi_seed_bonus) * (1 - penalty)
        rec["franchise_key"] = get_franchise_key(rec["title"])
        rec["franchise_penalty"] = penalty
        rec["is_sequel"] = is_sequel(rec["title"])

    # ordinamento iniziale
    recommendations = sorted(
        recommendations,
        key=lambda x: x["final_score"],
        reverse=True
    )

    # 🧠 DIVERSITY LAYER
    diversified = []

    for candidate in recommendations:
        penalty = 0

        for chosen in diversified:
            sim = simple_similarity(candidate, chosen)

            if sim >= 2:
                penalty += 0.15
            elif sim == 1:
                penalty += 0.07

        candidate["adjusted_score"] = candidate["final_score"] * (1 - penalty)

        diversified.append(candidate)

    # riordino finale
    recommendations = sorted(
        diversified,
        key=lambda x: x["adjusted_score"],
        reverse=True
    )

    filtered = []
    franchise_count = {}
    genre_tracker = {}
    seed_tracker = {}

    for rec in recommendations:
        
        fk = rec.get("franchise_key", "")
        components = rec.get("components", {})

        quality_score = components.get("quality_score_norm", 0)
        genre_score = components.get("genre_score", 0)
        tag_score = components.get("tag_score", 0)
        collab_score = components.get("collab_score", 0)

        print(
            "DEBUG MOVIE:",
            rec.get("title"),
            "| avg=", rec.get("avg_score", 0),
            "| quality=", quality_score,
            "| genre=", genre_score,
            "| tag=", tag_score,
            "| collab=", collab_score,
        )

        # filtro qualità base
        if quality_score < 0.45:
            print("SCARTATO quality:", rec.get("title"))
            continue

        # filtro rilevanza generale
        if rec.get("avg_score", 0) < 0.22:
            print("SCARTATO avg:", rec.get("title"))
            continue

        if rec.get("adjusted_score", 0) < 0.24:
            continue

        # filtro "film vuoti" (pochi segnali reali)
        if genre_score < 0.2 and tag_score < 0.1 and collab_score < 0.1:
            print("SCARTATO vuoto:", rec.get("title"))
            continue

        release_year = get_movie_release_year(rec.get("title", ""))
        best_seed = build_movie_best_seed_title(rec)
        print("YEAR PARSED:", rec.get("title"), release_year)

        if release_year >= 2015:
            rec["avg_score"] += 0.05
        elif release_year < 1990:
            rec["avg_score"] -= 0.05

        main_genres = rec.get("genres", [])
        primary_genre = main_genres[0] if main_genres else None

        if primary_genre:
            genre_count = genre_tracker.get(primary_genre, 0)

            # massimo 2 film per stesso genere principale
            if genre_count >= 2:
                continue
        
        if best_seed:
            seed_count = seed_tracker.get(best_seed, 0)

            # massimo 3 film trainati dallo stesso seed
            if seed_count >= 3:
                continue
        
        # evita troppi film della stessa saga/franchise
        if fk and franchise_count.get(fk, 0) >= 1:
            continue

        filtered.append(rec)
        if best_seed:
            seed_tracker[best_seed] = seed_tracker.get(best_seed, 0) + 1

        print("BEST SEED:", rec.get("title"), best_seed)

        if primary_genre:
            genre_tracker[primary_genre] = genre_tracker.get(primary_genre, 0) + 1

        if fk:
            franchise_count[fk] = franchise_count.get(fk, 0) + 1

        if len(filtered) >= top_k:
            break

        # fallback: se i filtri sono troppo stretti, riempi leggermente la lista
        if len(filtered) < top_k:
            for rec in recommendations:
                if rec in filtered:
                    continue

                fk = rec.get("franchise_key", "")

                # evita comunque troppi film della stessa saga/franchise
                if fk and franchise_count.get(fk, 0) >= 1:
                    continue

                genre_score_fallback = rec.get("components", {}).get("genre_score", 0)
                tag_score_fallback = rec.get("components", {}).get("tag_score", 0)
                collab_score_fallback = rec.get("components", {}).get("collab_score", 0)
                quality_score_fallback = rec.get("components", {}).get("quality_score_norm", 0)
                adjusted_score_fallback = rec.get("adjusted_score", 0)

                # scarta solo i film completamente vuoti
                if genre_score_fallback == 0 and tag_score_fallback == 0 and collab_score_fallback == 0:
                    continue

                # fallback più morbido ma ancora controllato
                if adjusted_score_fallback >= 0.18 and quality_score_fallback >= 0.50:
                    filtered.append(rec)

                    if fk:
                        franchise_count[fk] = franchise_count.get(fk, 0) + 1

                if len(filtered) >= top_k:
                    break

    for i, rec in enumerate(filtered):
        rec["best_seed_title"] = build_movie_best_seed_title(rec)

        if i == 0:
            rec["badge"] = {"text": "⭐ Miglior match", "type": "top"}
        else:
            rec["badge"] = build_movie_badge(rec)

        rec["explanation"] = build_movie_explanation(rec, index=i)
        rec["ui_signals"] = build_movie_ui_signals(rec)
        components = rec.get("components", {})

        avg_score = rec.get("avg_score", 0)
        genre_score = components.get("genre_score", 0)
        tag_score = components.get("tag_score", 0)
        collab_score = components.get("collab_score", 0)

        # scala UI più "premium" e leggibile
        rec["match_score"] = round(min(9.8, 5.5 + avg_score * 8), 1)
        rec["genre_score_ui"] = round(min(9.7, 5.0 + genre_score * 4), 1)
        rec["vibe_score_ui"] = round(min(9.6, 5.0 + max(tag_score, collab_score) * 8), 1)


    return {
        "resolved_seeds": resolved_seeds,
        "missing_titles": missing_titles,
        "recommendations": filtered
    }



def search_movies(query: str, limit: int = 10):
    query = query.strip()
    if not query:
        return []

    conn = get_connection()
    cursor = conn.cursor()

    # 1) Ricerca diretta nel DB locale
    cursor.execute("""
    SELECT movielens_movie_id, title
    FROM titles
    WHERE LOWER(title) LIKE LOWER(?)
    ORDER BY LENGTH(title) ASC
    LIMIT ?
    """, (f"%{query}%", limit))

    rows = cursor.fetchall()

    results = []
    seen_titles = set()

    for row in rows:
        movie_id = row[0]
        title = row[1]

        display_title = get_tmdb_localized_title(title) or title

        results.append({
            "movie_id": movie_id,
            "title": title,
            "display_title": display_title
        })
        seen_titles.add(title.lower())

    # 2) Se i risultati sono pochi, prova anche TMDB con query italiana
    if len(results) < limit:
        tmdb_matches = search_tmdb_movies(query, limit=limit)

        for item in tmdb_matches:
            original_title = item.get("original_title")
            title_it = item.get("title_it")

            if not original_title:
                continue

            # Cerca il titolo originale nel DB locale
            cursor.execute("""
            SELECT movielens_movie_id, title
            FROM titles
            WHERE LOWER(title) = LOWER(?)
            LIMIT 1
            """, (original_title,))

            row = cursor.fetchone()

            # Se non lo trova in modo esatto, prova LIKE
            if not row:
                cursor.execute("""
                SELECT movielens_movie_id, title
                FROM titles
                WHERE LOWER(title) LIKE LOWER(?)
                ORDER BY LENGTH(title) ASC
                LIMIT 1
                """, (f"%{original_title}%",))
                row = cursor.fetchone()

            if not row:
                continue

            movie_id = row[0]
            db_title = row[1]

            if db_title.lower() in seen_titles:
                continue

            if title_it and title_it != db_title:
                display_title = f"{title_it} ({db_title})"
            else:
                display_title = db_title

            results.append({
                "movie_id": movie_id,
                "title": db_title,
                "display_title": display_title
            })
            seen_titles.add(db_title.lower())

            if len(results) >= limit:
                break

    conn.close()
    return results


TMDB_API_KEY = os.getenv("TMDB_API_KEY")

def get_tmdb_localized_title(title: str):
    if not TMDB_API_KEY:
        return None

    try:
        url = "https://api.themoviedb.org/3/search/movie"
        params = {
            "api_key": TMDB_API_KEY,
            "query": title,
            "language": "it-IT"
        }

        response = requests.get(url, params=params, timeout=5)
        data = response.json()

        if data.get("results"):
            first = data["results"][0]
            localized_title = first.get("title")
            original_title = first.get("original_title")

            if localized_title and original_title and localized_title != original_title:
                return f"{localized_title} ({original_title})"

            return localized_title or original_title

    except Exception:
        return None

    return None

def search_tmdb_movies(query: str, limit: int = 10):
    if not TMDB_API_KEY:
        return []

    try:
        url = "https://api.themoviedb.org/3/search/movie"
        params = {
            "api_key": TMDB_API_KEY,
            "query": query,
            "language": "it-IT"
        }

        response = requests.get(url, params=params, timeout=5)
        data = response.json()

        results = []
        for item in data.get("results", [])[:limit]:
            results.append({
                "title_it": item.get("title"),
                "original_title": item.get("original_title"),
            })

        return results

    except Exception:
        return []

def get_movie_poster(title: str):
    if not TMDB_API_KEY:
        return None

    try:
        url = "https://api.themoviedb.org/3/search/movie"
        params = {
            "api_key": TMDB_API_KEY,
            "query": title
        }

        response = requests.get(url, params=params)
        data = response.json()

        if data.get("results"):
            poster_path = data["results"][0].get("poster_path")
            if poster_path:
                return f"https://image.tmdb.org/t/p/w200{poster_path}"

    except Exception:
        return None

    return None

def get_movie_tmdb_info(title: str):
    if not TMDB_API_KEY:
        return {
            "poster_url": None,
            "display_title": title,
            "overview": None,
        }

    try:
        url = "https://api.themoviedb.org/3/search/movie"
        params = {
            "api_key": TMDB_API_KEY,
            "query": title,
            "language": "it-IT"
        }

        response = requests.get(url, params=params, timeout=5)
        data = response.json()

        if data.get("results"):
            first = data["results"][0]

            poster_path = first.get("poster_path")
            poster_url = f"https://image.tmdb.org/t/p/w300{poster_path}" if poster_path else None

            localized_title = first.get("title")
            original_title = first.get("original_title")
            overview = first.get("overview")

            if localized_title and original_title and localized_title != original_title:
                display_title = f"{localized_title} ({original_title})"
            else:
                display_title = localized_title or original_title or title

            return {
                "poster_url": poster_url,
                "display_title": display_title,
                "overview": overview,
            }

    except Exception:
        pass

    return {
        "poster_url": None,
        "display_title": title,
        "overview": None,
    }