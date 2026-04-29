import os
import sqlite3
import re
import requests
from collections import defaultdict
from core.explainability import enrich_with_explanations

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CANDIDATE_DB_PATHS = [
    os.path.join(BASE_DIR, "db", "cosaguardo.db"),
    os.path.join(BASE_DIR, "cosaguardo", "db", "cosaguardo.db"),
    os.path.join(os.getcwd(), "db", "cosaguardo.db"),
    os.path.join(os.getcwd(), "cosaguardo", "db", "cosaguardo.db"),
]

DB_PATH = next((p for p in CANDIDATE_DB_PATHS if os.path.exists(p)), CANDIDATE_DB_PATHS[0])

print("BASE_DIR =", BASE_DIR)
print("CWD =", os.getcwd())
print("DB_PATH scelto =", DB_PATH)
print("DB EXISTS =", os.path.exists(DB_PATH))
print("CANDIDATES =", CANDIDATE_DB_PATHS)

if not os.path.exists(DB_PATH):
    raise RuntimeError(
        f"DB NON TROVATO. BASE_DIR={BASE_DIR} | CWD={os.getcwd()} | DB_PATH={DB_PATH}"
    )

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

    movie_title = row[1]
    movie_genres = get_movie_genres(movie_title)

    return {
        "movie_id": row[0],
        "title": movie_title,
        "genres": movie_genres,
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

        # filtro qualità base
        if quality_score < 0.45:
            continue

        # filtro rilevanza generale
        if rec.get("avg_score", 0) < 0.22:
            continue

        if rec.get("adjusted_score", 0) < 0.24:
            continue

        # filtro "film vuoti" (pochi segnali reali)
        if genre_score < 0.2 and tag_score < 0.1 and collab_score < 0.1:
            continue

        release_year = get_movie_release_year(rec.get("title", ""))
        best_seed = build_movie_best_seed_title(rec)

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
        # mappa why_titles → matched_seed_titles per explainability.py
        rec["matched_seed_titles"] = rec.get("why_titles", [])
        rec["matched_keywords"] = rec.get("keywords", [])

        if i == 0:
            rec["badge"] = {"text": "⭐ Miglior match", "type": "top"}
        else:
            rec["badge"] = build_movie_badge(rec)

        rec["ui_signals"] = build_movie_ui_signals(rec)
        components = rec.get("components", {})

        avg_score = rec.get("avg_score", 0)
        genre_score = components.get("genre_score", 0)
        tag_score = components.get("tag_score", 0)
        collab_score = components.get("collab_score", 0)

        rec["match_score"] = round(min(9.8, 5.5 + avg_score * 8), 1)
        rec["genre_score_ui"] = round(min(9.7, 5.0 + genre_score * 4), 1)
        rec["vibe_score_ui"] = round(min(9.6, 5.0 + max(tag_score, collab_score) * 8), 1)

    # genera spiegazioni personalizzate con explainability.py
    # prima arricchisce genres/keywords da TMDb per i rec che li hanno null
    for rec in filtered:
        if not rec.get("genres") or not rec.get("matched_keywords"):
            try:
                tmdb = get_movie_tmdb_match(rec.get("title", ""))
                if tmdb:
                    if not rec.get("genres"):
                        rec["genres"] = movie_genre_ids_to_names(tmdb.get("genre_ids", []))
                    if not rec.get("matched_keywords") and tmdb.get("tmdb_id"):
                        url = f"https://api.themoviedb.org/3/movie/{tmdb['tmdb_id']}/keywords"
                        resp = requests.get(url, params={"api_key": TMDB_API_KEY}, timeout=4)
                        kws = [k["name"].strip().lower() for k in resp.json().get("keywords", []) if k.get("name")]
                        rec["matched_keywords"] = kws
            except Exception:
                pass

    enrich_with_explanations(filtered)


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

def get_movie_tmdb_match(title: str):
    if not TMDB_API_KEY or not title:
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

        results = data.get("results", [])
        if not results:
            return None

        first = results[0]

        return {
            "tmdb_id": first.get("id"),
            "title": first.get("title") or first.get("original_title") or title,
            "original_title": first.get("original_title") or first.get("title") or title,
            "overview": first.get("overview"),
            "genre_ids": first.get("genre_ids", []),
        }

    except Exception:
        return None


MOVIE_GENRE_NAMES = {
    28: "Action",
    12: "Adventure",
    16: "Animation",
    35: "Comedy",
    80: "Crime",
    99: "Documentary",
    18: "Drama",
    10751: "Family",
    14: "Fantasy",
    36: "History",
    27: "Horror",
    10402: "Music",
    9648: "Mystery",
    10749: "Romance",
    878: "Sci-Fi",
    10770: "TV Movie",
    53: "Thriller",
    10752: "War",
    37: "Western",
}


def movie_genre_ids_to_names(genre_ids):
    if not genre_ids:
        return []

    return [MOVIE_GENRE_NAMES[g] for g in genre_ids if g in MOVIE_GENRE_NAMES]


def get_movie_genres(title: str):
    match = get_movie_tmdb_match(title)
    if not match:
        return []

    return movie_genre_ids_to_names(match.get("genre_ids", []))


def get_movie_keywords(movie_id: int):
    if not movie_id or not TMDB_API_KEY:
        return []

    try:
        # movie_id qui è il movielens id del DB locale.
        # Cerchiamo prima il titolo corrispondente nel DB locale.
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
        SELECT title
        FROM titles
        WHERE movielens_movie_id = ?
        LIMIT 1
        """, (movie_id,))

        row = cursor.fetchone()
        conn.close()

        if not row:
            return []

        movie_title = row[0]
        tmdb_match = get_movie_tmdb_match(movie_title)

        if not tmdb_match or not tmdb_match.get("tmdb_id"):
            return []

        url = f"https://api.themoviedb.org/3/movie/{tmdb_match['tmdb_id']}/keywords"
        params = {
            "api_key": TMDB_API_KEY
        }

        response = requests.get(url, params=params, timeout=5)
        data = response.json()

        results = []
        for item in data.get("keywords", []):
            name = item.get("name")
            if name:
                results.append(name.strip().lower())

        return results

    except Exception:
        return []

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
                "tmdb_id": first.get("id"),
            }

    except Exception:
        pass

    return {
        "poster_url": None,
        "display_title": title,
        "overview": None,
        "tmdb_id": None,
    }


def get_top_rated_recent(limit: int = 10) -> list:
    """
    I più apprezzati del momento: film e serie TV usciti negli ultimi 6 mesi
    con almeno 500 voti e rating >= 7.0. Pool di 40 titoli shufflati con seed
    giornaliero (cambia ogni giorno, stabile durante la giornata).
    """
    if not TMDB_API_KEY:
        return []

    import datetime, random

    # Data 6 mesi fa
    today = datetime.date.today()
    six_months_ago = (today - datetime.timedelta(days=180)).isoformat()

    base_params = {
        "api_key":           TMDB_API_KEY,
        "language":          "it-IT",
        "sort_by":           "vote_count.desc",
        "vote_count.gte":    500,
        "vote_average.gte":  7.0,
        "with_original_language": "en|it|fr|es|de|ko|ja",
    }

    pool = []

    # Film
    try:
        r = requests.get(
            "https://api.themoviedb.org/3/discover/movie",
            params={**base_params,
                    "primary_release_date.gte": six_months_ago,
                    "region": "IT"},
            timeout=6
        )
        for item in r.json().get("results", [])[:25]:
            pp = item.get("poster_path")
            title = item.get("title") or item.get("original_title") or ""
            if not pp or not title:
                continue
            pool.append({
                "tmdb_id":      item.get("id"),
                "title":        title,
                "content_type": "movie",
                "poster_url":   f"https://image.tmdb.org/t/p/w342{pp}",
                "overview":     (item.get("overview") or "")[:160],
                "vote_average": round(item.get("vote_average", 0), 1),
                "vote_count":   item.get("vote_count", 0),
                "release_date": item.get("release_date", ""),
                "label":        "Film",
            })
    except Exception:
        pass

    # Serie TV
    try:
        r = requests.get(
            "https://api.themoviedb.org/3/discover/tv",
            params={**base_params,
                    "first_air_date.gte": six_months_ago,
                    "watch_region": "IT"},
            timeout=6
        )
        for item in r.json().get("results", [])[:25]:
            pp = item.get("poster_path")
            title = item.get("name") or item.get("original_name") or ""
            if not pp or not title:
                continue
            pool.append({
                "tmdb_id":      item.get("id"),
                "title":        title,
                "content_type": "tv",
                "poster_url":   f"https://image.tmdb.org/t/p/w342{pp}",
                "overview":     (item.get("overview") or "")[:160],
                "vote_average": round(item.get("vote_average", 0), 1),
                "vote_count":   item.get("vote_count", 0),
                "release_date": item.get("first_air_date", ""),
                "label":        "Serie TV",
            })
    except Exception:
        pass

    if not pool:
        return []

    # Shuffle con seed giornaliero — ogni giorno ordine diverso, stabile durante la giornata
    seed = int(today.strftime("%Y%m%d"))
    rng = random.Random(seed)
    rng.shuffle(pool)

    return pool[:limit]


def get_trending_tmdb(limit: int = 12):
    """
    Recupera i contenuti trending del giorno da TMDb (film + serie TV).
    Restituisce una lista di dict con: title, content_type, poster_url, label.
    """
    if not TMDB_API_KEY:
        return []

    try:
        url = "https://api.themoviedb.org/3/trending/all/day"
        params = {
            "api_key": TMDB_API_KEY,
            "language": "it-IT",
        }

        response = requests.get(url, params=params, timeout=5)
        data = response.json()

        results = []
        for item in data.get("results", [])[:limit]:
            media_type = item.get("media_type", "")
            if media_type not in ("movie", "tv"):
                continue

            poster_path = item.get("poster_path")
            poster_url = f"https://image.tmdb.org/t/p/w342{poster_path}" if poster_path else None

            if not poster_url:
                continue

            if media_type == "movie":
                title = item.get("title") or item.get("original_title") or ""
                label = "Film"
            else:
                title = item.get("name") or item.get("original_name") or ""
                label = "Serie TV"

            if not title:
                continue

            results.append({
                "title": title,
                "content_type": media_type,
                "poster_url": poster_url,
                "label": label,
                "overview": (item.get("overview") or "")[:120],
            })

        return results

    except Exception:
        return []


def get_now_playing(limit: int = 8) -> list:
    """
    Film attualmente in sala in Italia (aggiornato giornalmente da TMDb).
    """
    if not TMDB_API_KEY:
        return []

    try:
        resp = requests.get(
            "https://api.themoviedb.org/3/movie/now_playing",
            params={"api_key": TMDB_API_KEY, "language": "it-IT", "region": "IT"},
            timeout=5
        )
        results = []
        for item in resp.json().get("results", [])[:limit]:
            poster_path = item.get("poster_path")
            if not poster_path:
                continue
            title = item.get("title") or item.get("original_title") or ""
            if not title:
                continue
            results.append({
                "tmdb_id": item.get("id"),
                "title": title,
                "poster_url": f"https://image.tmdb.org/t/p/w342{poster_path}",
                "overview": (item.get("overview") or "")[:120],
                "vote_average": round(item.get("vote_average", 0), 1),
                "release_date": item.get("release_date", ""),
                "content_type": "movie",
                "label": "In sala",
            })
        return results
    except Exception:
        return []


def get_upcoming(limit: int = 8) -> list:
    """
    Film in uscita prossimamente in Italia (aggiornato giornalmente da TMDb).
    """
    if not TMDB_API_KEY:
        return []

    try:
        resp = requests.get(
            "https://api.themoviedb.org/3/movie/upcoming",
            params={"api_key": TMDB_API_KEY, "language": "it-IT", "region": "IT"},
            timeout=5
        )
        results = []
        for item in resp.json().get("results", [])[:limit]:
            poster_path = item.get("poster_path")
            if not poster_path:
                continue
            title = item.get("title") or item.get("original_title") or ""
            if not title:
                continue
            results.append({
                "tmdb_id": item.get("id"),
                "title": title,
                "poster_url": f"https://image.tmdb.org/t/p/w342{poster_path}",
                "overview": (item.get("overview") or "")[:120],
                "vote_average": round(item.get("vote_average", 0), 1),
                "release_date": item.get("release_date", ""),
                "content_type": "movie",
                "label": "Prossimamente",
            })
        return results
    except Exception:
        return []


def get_detail_movie(tmdb_id: int) -> dict:
    """
    Dati completi di un film: info base, generi, cast, trailer YouTube, providers IT.
    """
    if not TMDB_API_KEY or not tmdb_id:
        return {}
    try:
        r = requests.get(
            f"https://api.themoviedb.org/3/movie/{tmdb_id}",
            params={"api_key": TMDB_API_KEY, "language": "it-IT",
                    "append_to_response": "credits,videos,watch/providers"},
            timeout=8
        )
        d = r.json()
        if d.get("status_code") == 34:   # not found
            return {}

        # Poster / backdrop
        poster_path   = d.get("poster_path") or ""
        backdrop_path = d.get("backdrop_path") or ""

        # Generi
        genres = [g["name"] for g in d.get("genres", [])]

        # Cast top 8
        cast = []
        for p in d.get("credits", {}).get("cast", [])[:8]:
            cast.append({
                "name":       p.get("name", ""),
                "character":  p.get("character", ""),
                "profile_url": (f"https://image.tmdb.org/t/p/w185{p['profile_path']}"
                                if p.get("profile_path") else ""),
            })

        # Regia
        directors = [p["name"] for p in d.get("credits", {}).get("crew", [])
                     if p.get("job") == "Director"]

        # Trailer YouTube (primo trailer IT, fallback EN)
        trailer_key = ""
        videos = d.get("videos", {}).get("results", [])
        for v in videos:
            if v.get("type") == "Trailer" and v.get("site") == "YouTube":
                trailer_key = v["key"]
                break

        # Watch providers IT
        prov_it = d.get("watch/providers", {}).get("results", {}).get("IT", {})
        jw_link = prov_it.get("link", "")

        def _parse_prov(items):
            out, seen = [], set()
            for p in (items or []):
                name = p.get("provider_name", "")
                if name in seen:
                    continue
                seen.add(name)
                pid  = p.get("provider_id")
                logo = p.get("logo_path", "")
                meta = PROVIDER_META.get(pid, {})
                out.append({
                    "name":     meta.get("name", name),
                    "logo_url": f"https://image.tmdb.org/t/p/w45{logo}" if logo else "",
                    "color":    meta.get("color", "#444"),
                    "link":     jw_link,
                })
            return out

        return {
            "tmdb_id":      tmdb_id,
            "title":        d.get("title") or d.get("original_title", ""),
            "original_title": d.get("original_title", ""),
            "tagline":      d.get("tagline", ""),
            "overview":     d.get("overview", ""),
            "release_date": d.get("release_date", ""),
            "runtime":      d.get("runtime") or 0,
            "vote_average": round(d.get("vote_average", 0), 1),
            "vote_count":   d.get("vote_count", 0),
            "poster_url":   f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else "",
            "backdrop_url": f"https://image.tmdb.org/t/p/w1280{backdrop_path}" if backdrop_path else "",
            "genres":       genres,
            "cast":         cast,
            "directors":    directors,
            "trailer_key":  trailer_key,
            "providers": {
                "flatrate": _parse_prov(prov_it.get("flatrate", [])),
                "rent":     _parse_prov(prov_it.get("rent", [])),
                "buy":      _parse_prov(prov_it.get("buy", [])),
                "link":     jw_link,
            },
            "content_type": "movie",
        }
    except Exception:
        return {}


def get_detail_tv(tmdb_id: int) -> dict:
    """
    Dati completi di una serie TV: info base, generi, cast, trailer YouTube, providers IT.
    """
    if not TMDB_API_KEY or not tmdb_id:
        return {}
    try:
        r = requests.get(
            f"https://api.themoviedb.org/3/tv/{tmdb_id}",
            params={"api_key": TMDB_API_KEY, "language": "it-IT",
                    "append_to_response": "credits,videos,watch/providers"},
            timeout=8
        )
        d = r.json()
        if d.get("status_code") == 34:
            return {}

        poster_path   = d.get("poster_path") or ""
        backdrop_path = d.get("backdrop_path") or ""
        genres        = [g["name"] for g in d.get("genres", [])]

        cast = []
        for p in d.get("credits", {}).get("cast", [])[:8]:
            cast.append({
                "name":        p.get("name", ""),
                "character":   p.get("character", ""),
                "profile_url": (f"https://image.tmdb.org/t/p/w185{p['profile_path']}"
                                if p.get("profile_path") else ""),
            })

        creators = [p["name"] for p in d.get("created_by", [])]

        trailer_key = ""
        for v in d.get("videos", {}).get("results", []):
            if v.get("type") == "Trailer" and v.get("site") == "YouTube":
                trailer_key = v["key"]
                break

        prov_it = d.get("watch/providers", {}).get("results", {}).get("IT", {})
        jw_link = prov_it.get("link", "")

        def _parse_prov(items):
            out, seen = [], set()
            for p in (items or []):
                name = p.get("provider_name", "")
                if name in seen:
                    continue
                seen.add(name)
                pid  = p.get("provider_id")
                logo = p.get("logo_path", "")
                meta = PROVIDER_META.get(pid, {})
                out.append({
                    "name":     meta.get("name", name),
                    "logo_url": f"https://image.tmdb.org/t/p/w45{logo}" if logo else "",
                    "color":    meta.get("color", "#444"),
                    "link":     jw_link,
                })
            return out

        seasons = d.get("number_of_seasons") or 0
        episodes = d.get("number_of_episodes") or 0

        return {
            "tmdb_id":        tmdb_id,
            "title":          d.get("name") or d.get("original_name", ""),
            "original_title": d.get("original_name", ""),
            "tagline":        d.get("tagline", ""),
            "overview":       d.get("overview", ""),
            "release_date":   d.get("first_air_date", ""),
            "seasons":        seasons,
            "episodes":       episodes,
            "vote_average":   round(d.get("vote_average", 0), 1),
            "vote_count":     d.get("vote_count", 0),
            "poster_url":     f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else "",
            "backdrop_url":   f"https://image.tmdb.org/t/p/w1280{backdrop_path}" if backdrop_path else "",
            "genres":         genres,
            "cast":           cast,
            "creators":       creators,
            "trailer_key":    trailer_key,
            "providers": {
                "flatrate": _parse_prov(prov_it.get("flatrate", [])),
                "rent":     _parse_prov(prov_it.get("rent", [])),
                "buy":      _parse_prov(prov_it.get("buy", [])),
                "link":     jw_link,
            },
            "content_type": "tv",
        }
    except Exception:
        return {}

# Mappa ID piattaforma TMDb → nome + colore brand
PROVIDER_META = {
    8:   {"name": "Netflix",        "color": "#E50914"},
    9:   {"name": "Prime Video",    "color": "#00A8E0"},
    10:  {"name": "Amazon Video",   "color": "#00A8E0"},
    35:  {"name": "Rakuten TV",     "color": "#BF0000"},
    39:  {"name": "NOW TV",         "color": "#00BCD4"},
    40:  {"name": "Chili",          "color": "#FF6600"},
    119: {"name": "Prime Video",    "color": "#00A8E0"},
    149: {"name": "Rakuten TV",     "color": "#BF0000"},
    337: {"name": "Disney+",        "color": "#113CCF"},
    341: {"name": "Apple TV+",      "color": "#000000"},
    350: {"name": "Apple TV+",      "color": "#000000"},
    381: {"name": "Canal+",         "color": "#000000"},
    531: {"name": "Paramount+",     "color": "#0064FF"},
    619: {"name": "Disney+",        "color": "#113CCF"},
}


def get_watch_providers(title: str, content_type: str = "movie", country: str = "IT") -> dict:
    """
    Recupera le piattaforme streaming per un film o serie TV.
    Restituisce dict con: flatrate, rent, buy, link.
    """
    if not TMDB_API_KEY or not title:
        return {}

    try:
        # 1. Cerca il titolo su TMDb
        if content_type == "tv":
            search_url = "https://api.themoviedb.org/3/search/tv"
        else:
            search_url = "https://api.themoviedb.org/3/search/movie"

        search_resp = requests.get(search_url, params={
            "api_key": TMDB_API_KEY,
            "query": title,
            "language": "it-IT",
        }, timeout=5)
        results = search_resp.json().get("results", [])

        if not results:
            return {}

        tmdb_id = results[0]["id"]

        # 2. Recupera watch providers
        if content_type == "tv":
            providers_url = f"https://api.themoviedb.org/3/tv/{tmdb_id}/watch/providers"
        else:
            providers_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/watch/providers"

        prov_resp = requests.get(providers_url, params={
            "api_key": TMDB_API_KEY,
        }, timeout=5)
        country_data = prov_resp.json().get("results", {}).get(country, {})

        if not country_data:
            return {}

        justwatch_link = country_data.get("link", "")

        def parse_providers(items):
            out = []
            seen = set()
            for p in (items or []):
                name = p.get("provider_name", "")
                if name in seen:
                    continue
                seen.add(name)
                pid = p.get("provider_id")
                logo_path = p.get("logo_path", "")
                meta = PROVIDER_META.get(pid, {})
                out.append({
                    "name":     meta.get("name", name),
                    "logo_url": f"https://image.tmdb.org/t/p/w45{logo_path}" if logo_path else "",
                    "color":    meta.get("color", "#444"),
                    "link":     justwatch_link,
                })
            return out

        return {
            "flatrate": parse_providers(country_data.get("flatrate", [])),
            "rent":     parse_providers(country_data.get("rent", [])),
            "buy":      parse_providers(country_data.get("buy", [])),
            "link":     justwatch_link,
        }

    except Exception:
        return {}
