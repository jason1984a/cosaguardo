import os
import sqlite3
import re
import requests
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
from core.explainability import enrich_with_explanations
from core.tmdb_cache import cached_call

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CANDIDATE_DB_PATHS = [
    os.path.join(BASE_DIR, "db", "cosaguardo.db"),
    os.path.join(BASE_DIR, "cosaguardo", "db", "cosaguardo.db"),
    os.path.join(os.getcwd(), "db", "cosaguardo.db"),
    os.path.join(os.getcwd(), "cosaguardo", "db", "cosaguardo.db"),
]

DB_PATH = next((p for p in CANDIDATE_DB_PATHS if os.path.exists(p)), CANDIDATE_DB_PATHS[0])

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
        "il signore degli anelli": "tolkien",
        "lo hobbit": "tolkien",

        "harry potter": "wizarding_world",
        "fantastic beasts": "wizarding_world",
        "animali fantastici": "wizarding_world",

        "batman": "batman",
        "dark knight": "batman",

        "spider man": "spiderman",
        "amazing spider man": "spiderman",
        "spiderman": "spiderman",

        "superman": "superman",
        "man of steel": "superman",

        "pirates of the caribbean": "pirati_caraibi",
        "pirati dei caraibi": "pirati_caraibi",
        "maledizione della prima luna": "pirati_caraibi",
        "forziere fantasma": "pirati_caraibi",
        "ai confini del mondo": "pirati_caraibi",
        "oltre i confini": "pirati_caraibi",

        "avengers": "mcu_avengers",
        "infinity war": "mcu_avengers",
        "endgame": "mcu_avengers",
        "iron man": "mcu_iron_man",
        "captain america": "mcu_cap",
        "thor": "mcu_thor",
        "guardians of the galaxy": "mcu_guardians",
        "guardiani della galassia": "mcu_guardians",

        "fast and furious": "fast_furious",
        "fast furious": "fast_furious",
        "veloce e furioso": "fast_furious",

        "mission impossible": "mission_impossible",
        "mission impossibile": "mission_impossible",

        "star wars": "star_wars",
        "guerre stellari": "star_wars",
        "jedi": "star_wars",
        "sith": "star_wars",
        "mandalorian": "star_wars",

        "john wick": "john_wick",

        "matrix": "matrix",

        "indiana jones": "indiana_jones",

        "jurassic": "jurassic",

        "transformers": "transformers",

        "alien": "alien_franchise",
        "aliens": "alien_franchise",
        "prometheus": "alien_franchise",
        "covenant": "alien_franchise",

        "terminator": "terminator",

        "rocky": "rocky",
        "creed": "rocky",

        "oceans eleven": "oceans",
        "ocean eleven": "oceans",
        "ocean twelve": "oceans",
        "ocean thirteen": "oceans",
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

    # Parallelizzo find_movie_by_title — ogni seed fa 1 query DB locale
    # + 1 chiamata cached a TMDb (per i generi). In parallelo: ~3-5x più veloce.
    with ThreadPoolExecutor(max_workers=8) as ex:
        seed_results = list(ex.map(
            lambda t: (t, find_movie_by_title(t)),
            seed_titles
        ))

    for title, movie in seed_results:
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
    # PRIMA arricchisce genres/keywords da TMDb per i rec che li hanno null,
    # IN PARALLELO usando la cache (24h memoria + 7gg DB).
    needs_enrichment = [
        rec for rec in filtered
        if not rec.get("genres") or not rec.get("matched_keywords")
    ]

    if needs_enrichment:
        def _fetch_movie_keywords_by_tmdb_id(tmdb_id: int):
            """Cache wrapper per keywords endpoint TMDb di un movie_id."""
            if not tmdb_id:
                return []
            cache_key = f"movie:keywords_by_tmdbid:{tmdb_id}"

            def _fetch():
                try:
                    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/keywords"
                    resp = requests.get(url, params={"api_key": TMDB_API_KEY}, timeout=4)
                    return [
                        k["name"].strip().lower()
                        for k in resp.json().get("keywords", [])
                        if k.get("name")
                    ]
                except Exception:
                    return []
            return cached_call(cache_key, _fetch)

        def _enrich_one(rec):
            try:
                tmdb = get_movie_tmdb_match(rec.get("title", ""))
                if not tmdb:
                    return
                if not rec.get("genres"):
                    rec["genres"] = movie_genre_ids_to_names(tmdb.get("genre_ids", []))
                if not rec.get("matched_keywords") and tmdb.get("tmdb_id"):
                    rec["matched_keywords"] = _fetch_movie_keywords_by_tmdb_id(tmdb["tmdb_id"])
            except Exception:
                pass

        # Parallel fetch — drasticamente più veloce del loop seriale precedente
        with ThreadPoolExecutor(max_workers=10) as ex:
            list(ex.map(_enrich_one, needs_enrichment))

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

            # Usa solo il titolo italiano se disponibile, senza AKA
            display_title = title_it if title_it else db_title

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
    """Cerca un film su TMDB. Cached 24h memoria + 7gg DB."""
    if not TMDB_API_KEY or not title:
        return None
    cache_key = f"movie:tmdb_match:{title.strip().lower()}"
    return cached_call(cache_key, lambda: _get_movie_tmdb_match_uncached(title))


def _get_movie_tmdb_match_uncached(title: str):
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

# Cache titoli localizzati — evita chiamate TMDb ripetute per lo stesso titolo
_localized_title_cache: dict = {}

def get_tmdb_localized_title(title: str):
    if title in _localized_title_cache:
        return _localized_title_cache[title]
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
            original_title  = first.get("original_title")

            # Usa solo il titolo localizzato — niente AKA che allunga inutilmente
            result = localized_title or original_title

            _localized_title_cache[title] = result
            return result

    except Exception:
        pass

    _localized_title_cache[title] = None
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
    """Recupera info film TMDb (poster, titolo, overview). Cached 24h memoria + 7gg DB."""
    if not TMDB_API_KEY:
        return {
            "poster_url": None,
            "display_title": title,
            "overview": None,
        }
    if not title or not title.strip():
        return {"poster_url": None, "display_title": title, "overview": None, "tmdb_id": None}

    cache_key = f"movie:tmdb_info:{title.strip().lower()}"
    cached = cached_call(cache_key, lambda: _get_movie_tmdb_info_uncached(title))
    return cached if cached else {"poster_url": None, "display_title": title, "overview": None, "tmdb_id": None}


def _get_movie_tmdb_info_uncached(title: str):
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
    Detail completo di un film.

    Cache strategy:
    - La risposta TMDb grezza è cached 24h memoria + 7gg DB
    - I providers (mapping nostro + affiliate links) vengono ricalcolati AD OGNI call
      così se cambia AFFILIATE_AMAZON / AFFILIATE_APPLE / PROVIDER_META,
      gli effetti sono immediati senza aspettare la scadenza cache.
    """
    if not TMDB_API_KEY or not tmdb_id:
        return {}

    # Cache key v2 — naming nuovo invalida implicitamente la cache vecchia
    cache_key = f"movie:detail:raw:v2:{tmdb_id}"
    raw = cached_call(cache_key, lambda: _fetch_detail_movie_raw(tmdb_id))
    if not raw:
        return {}

    return _build_movie_detail(tmdb_id, raw)


def _fetch_detail_movie_raw(tmdb_id: int) -> Optional[dict]:
    """Solo fetch grezza da TMDb. NIENTE elaborazione providers/affiliate qui."""
    try:
        r = requests.get(
            f"https://api.themoviedb.org/3/movie/{tmdb_id}",
            params={"api_key": TMDB_API_KEY, "language": "it-IT",
                    "append_to_response": "credits,videos,watch/providers"},
            timeout=8
        )
        d = r.json()
        if d.get("status_code") == 34:  # Not found
            return None
        return d
    except Exception:
        return None


def _build_movie_detail(tmdb_id: int, d: dict) -> dict:
    """
    Costruisce il dict detail completo dai dati grezzi TMDb.
    Mapping providers + affiliate links avvengono qui — NON cachato,
    sempre fresco rispetto alle env vars.
    """
    try:
        # Poster / backdrop
        poster_path   = d.get("poster_path") or ""
        backdrop_path = d.get("backdrop_path") or ""

        # Generi
        genres = [g["name"] for g in d.get("genres", [])]

        # Cast top 8
        cast = []
        for p in d.get("credits", {}).get("cast", [])[:8]:
            cast.append({
                "person_id":   p.get("id"),
                "name":        p.get("name", ""),
                "character":   p.get("character", ""),
                "profile_url": (f"https://image.tmdb.org/t/p/w185{p['profile_path']}"
                                if p.get("profile_path") else ""),
            })

        # Regia
        directors = [p["name"] for p in d.get("credits", {}).get("crew", [])
                     if p.get("job") == "Director"]

        # Trailer YouTube
        trailer_key = ""
        videos = d.get("videos", {}).get("results", [])
        for v in videos:
            if v.get("type") == "Trailer" and v.get("site") == "YouTube":
                trailer_key = v["key"]
                break

        # Watch providers IT
        prov_it = d.get("watch/providers", {}).get("results", {}).get("IT", {})
        jw_link = prov_it.get("link", "")
        title   = d.get("title") or d.get("original_title", "")

        def _parse_prov(items):
            out, seen = [], set()
            for p in (items or []):
                pid        = p.get("provider_id")
                logo       = p.get("logo_path", "")
                raw_name   = p.get("provider_name", "")
                prov_name, prov_color = _normalize_provider_name(raw_name, pid)
                # Dedup sul nome FINALE (post-mapping)
                if prov_name in seen:
                    continue
                seen.add(prov_name)
                # Usa link affiliato se disponibile, altrimenti JustWatch
                aff_link   = _build_affiliate_link(prov_name, title=title, tmdb_id=tmdb_id)
                out.append({
                    "name":       prov_name,
                    "logo_url":   f"https://image.tmdb.org/t/p/w45{logo}" if logo else "",
                    "color":      prov_color,
                    "link":       aff_link or jw_link,
                    "is_affiliate": bool(aff_link),
                })
            return out

        return {
            "tmdb_id":      tmdb_id,
            "title":        title,
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
    Detail completo di una serie TV.

    Cache strategy:
    - La risposta TMDb grezza è cached 24h memoria + 7gg DB (non cambia spesso)
    - I providers (mapping nostro + affiliate links) vengono ricalcolati AD OGNI call
      così se cambia AFFILIATE_AMAZON / AFFILIATE_APPLE / PROVIDER_META,
      gli effetti sono immediati senza aspettare la scadenza cache.
    """
    if not TMDB_API_KEY or not tmdb_id:
        return {}

    # Cache key v2 (raw response). Naming nuovo invalida implicitamente la cache vecchia.
    cache_key = f"tv:detail:raw:v2:{tmdb_id}"
    raw = cached_call(cache_key, lambda: _fetch_detail_tv_raw(tmdb_id))
    if not raw:
        return {}

    return _build_tv_detail(tmdb_id, raw)


def _fetch_detail_tv_raw(tmdb_id: int) -> Optional[dict]:
    """Solo fetch grezza da TMDb. NIENTE elaborazione providers/affiliate qui."""
    try:
        r = requests.get(
            f"https://api.themoviedb.org/3/tv/{tmdb_id}",
            params={"api_key": TMDB_API_KEY, "language": "it-IT",
                    "append_to_response": "credits,videos,watch/providers"},
            timeout=8
        )
        d = r.json()
        if d.get("status_code") == 34:  # Not found
            return None
        return d
    except Exception:
        return None


def _build_tv_detail(tmdb_id: int, d: dict) -> dict:
    """
    Costruisce il dict detail completo dai dati grezzi TMDb.
    Questo è il punto dove avviene il mapping providers + affiliate links,
    quindi NON cachato — sempre fresco rispetto alle env vars.
    """
    try:
        poster_path   = d.get("poster_path") or ""
        backdrop_path = d.get("backdrop_path") or ""
        genres        = [g["name"] for g in d.get("genres", [])]

        cast = []
        for p in d.get("credits", {}).get("cast", [])[:8]:
            cast.append({
                "person_id":   p.get("id"),
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
        title   = d.get("name") or d.get("original_name", "")

        def _parse_prov(items):
            out, seen = [], set()
            for p in (items or []):
                pid        = p.get("provider_id")
                logo       = p.get("logo_path", "")
                raw_name   = p.get("provider_name", "")
                prov_name, prov_color = _normalize_provider_name(raw_name, pid)
                # Dedup sul nome FINALE (post-mapping)
                if prov_name in seen:
                    continue
                seen.add(prov_name)
                # Usa link affiliato se disponibile, altrimenti JustWatch
                aff_link   = _build_affiliate_link(prov_name, title=title, tmdb_id=tmdb_id)
                out.append({
                    "name":       prov_name,
                    "logo_url":   f"https://image.tmdb.org/t/p/w45{logo}" if logo else "",
                    "color":      prov_color,
                    "link":       aff_link or jw_link,
                    "is_affiliate": bool(aff_link),
                })
            return out

        seasons = d.get("number_of_seasons") or 0
        episodes = d.get("number_of_episodes") or 0

        return {
            "tmdb_id":        tmdb_id,
            "title":          title,
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


def get_cinema_news(limit: int = 8) -> list:
    """
    Contenuti editoriali dalla sezione 'Dal mondo del cinema':
    mix di film recenti su TMDb presentati con backdrop/poster.
    Sempre funzionante, sempre con immagini.
    """
    if not TMDB_API_KEY:
        return []

    items = []

    # 1. Film usciti di recente (ultimi 60 giorni) — con backdrop
    try:
        import datetime
        sixty_days_ago = (datetime.date.today() - datetime.timedelta(days=60)).isoformat()
        r = requests.get(
            "https://api.themoviedb.org/3/discover/movie",
            params={
                "api_key": TMDB_API_KEY,
                "language": "it-IT",
                "region": "IT",
                "sort_by": "popularity.desc",
                "primary_release_date.gte": sixty_days_ago,
                "vote_count.gte": 50,
            },
            timeout=6
        )
        for item in r.json().get("results", [])[:4]:
            bp = item.get("backdrop_path") or item.get("poster_path")
            if not bp: continue
            title = item.get("title") or item.get("original_title","")
            if not title: continue
            tmdb_id = item.get("id")
            items.append({
                "title":     title,
                "link":      f"/film/{tmdb_id}" if tmdb_id else "/",
                "summary":   (item.get("overview") or "")[:160],
                "source":    "Al cinema",
                "thumb":     f"https://image.tmdb.org/t/p/w780{bp}",
                "tmdb_id":   tmdb_id,
                "internal":  True,
            })
    except Exception:
        pass

    # 2. Serie TV popolari recenti
    try:
        r = requests.get(
            "https://api.themoviedb.org/3/tv/popular",
            params={"api_key": TMDB_API_KEY, "language": "it-IT"},
            timeout=6
        )
        for item in r.json().get("results", [])[:4]:
            bp = item.get("backdrop_path") or item.get("poster_path")
            if not bp: continue
            title = item.get("name") or item.get("original_name","")
            if not title: continue
            tmdb_id = item.get("id")
            items.append({
                "title":     title,
                "link":      f"/serie/{tmdb_id}" if tmdb_id else "/",
                "summary":   (item.get("overview") or "")[:160],
                "source":    "Serie del momento",
                "thumb":     f"https://image.tmdb.org/t/p/w780{bp}",
                "tmdb_id":   tmdb_id,
                "internal":  True,
            })
    except Exception:
        pass

    # Mescola film e serie
    import random
    random.shuffle(items)
    return items[:limit]



def search_movies_fast(query: str, limit: int = 8) -> list:
    """
    Ricerca veloce film con ranking per popolarità (titoli famosi prima).

    Strategia ibrida:
    - Query corte (≤3 char): TMDb-first → risultati ordinati per popolarità.
      DB locale solo come complemento per indicizzare titoli storici/MovieLens
      che TMDb non rankerebbe in alto.
    - Query lunghe (4+ char): TMDb + DB locale paralleli, scoring custom che
      combina match esatto, startsWith, popolarità.

    Cache:
    - L1 in-memory (dict modulo) → istantanea per query ripetute nella stessa request
    - L2 tmdb_cache (DB) → 24h, condivisa fra processi/restart
    """
    query = query.strip()
    if len(query) < 2:
        return []

    import re as _re
    def normalize(s):
        return _re.sub(r"[-\'\s]+", " ", s).strip().lower()

    q_lower = query.lower()
    q_norm = normalize(query)

    # Cache key (stabile, case-insensitive)
    cache_key = f"search_movie_v2:{q_lower}:{limit}"

    # L2 cache hit?
    try:
        from core.tmdb_cache import cache_get, cache_set
        cached = cache_get(cache_key)
        if cached is not None:
            return cached
    except Exception:
        cache_get = None
        cache_set = None

    # ── 1. TMDb search ─────────────────────────────────────────────────
    # Default sort di TMDb è già un mix rilevanza+popolarità.
    # Chiediamo i primi 20 risultati per avere materia per il ranking custom.
    tmdb_results = []
    if TMDB_API_KEY:
        try:
            r = requests.get(
                "https://api.themoviedb.org/3/search/movie",
                params={
                    "api_key": TMDB_API_KEY,
                    "query": query,
                    "language": "it-IT",
                    "include_adult": "false",
                },
                timeout=4
            )
            for item in r.json().get("results", [])[:20]:
                t_it   = (item.get("title") or "").strip()
                t_orig = (item.get("original_title") or "").strip()
                if not t_it and not t_orig:
                    continue

                display = t_it or t_orig
                base    = t_orig or t_it

                pop = float(item.get("popularity") or 0)
                vc  = int(item.get("vote_count") or 0)

                # Filtro qualità minima: scarta titoli con ZERO voti
                # (sono spesso schede placeholder / film sconosciuti)
                if vc < 1:
                    continue

                tmdb_results.append({
                    "movie_id":      item.get("id"),
                    "tmdb_id":       item.get("id"),
                    "title":         base,
                    "display_title": display,
                    "_popularity":   pop,
                    "_vote_count":   vc,
                })
        except Exception:
            pass

    # ── 2. DB locale (fallback / complemento) ─────────────────────────
    db_results = []
    if len(tmdb_results) < limit:  # solo se TMDb non ne ha trovati abbastanza
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='titles'")
            if cursor.fetchone():
                cursor.execute("""
                    SELECT movielens_movie_id, title
                    FROM titles
                    WHERE LOWER(title) LIKE LOWER(?)
                    ORDER BY LENGTH(title) ASC
                    LIMIT ?
                """, (f"%{query}%", limit * 2))
                rows = cursor.fetchall()
                conn.close()

                # Evita di duplicare titoli già in tmdb_results
                tmdb_titles_lower = {r["title"].lower() for r in tmdb_results}
                tmdb_titles_lower |= {r["display_title"].lower() for r in tmdb_results}
                for row in rows:
                    title = row[1]
                    if title.lower() in tmdb_titles_lower:
                        continue
                    display = _localized_title_cache.get(title, title)
                    db_results.append({
                        "movie_id":      row[0],
                        "tmdb_id":       None,  # DB locale non ha tmdb_id
                        "title":         title,
                        "display_title": display or title,
                        "_popularity":   0,
                        "_vote_count":   0,
                    })
            else:
                conn.close()
        except Exception:
            pass

    # ── 3. Ranking custom: combina match qualitá + popolaritá ──────────
    def score(item):
        title_l = (item.get("title") or "").lower()
        disp_l  = (item.get("display_title") or "").lower()

        s = 0
        # Match esatto: super boost (utente ha digitato il titolo intero)
        if title_l == q_lower or disp_l == q_lower:
            s += 10000
        # StartsWith: boost grande (es. "house" → "House M.D.")
        elif title_l.startswith(q_lower) or disp_l.startswith(q_lower):
            s += 5000
        # Word-startsWith: una parola del titolo inizia col query
        elif any(w.startswith(q_lower) for w in title_l.split()) or \
             any(w.startswith(q_lower) for w in disp_l.split()):
            s += 2000
        # Contains: match generico
        elif q_lower in title_l or q_lower in disp_l:
            s += 500

        # Tie-breaker: popolarità (in scala log per non dominare)
        pop = item.get("_popularity", 0)
        if pop > 0:
            import math
            s += math.log10(pop + 1) * 100  # 100 popularity → +200, 10 → +100

        # Boost piccolo per titoli con molti voti (segno di "vero film famoso")
        vc = item.get("_vote_count", 0)
        if vc >= 1000:
            s += 50
        elif vc >= 100:
            s += 20

        return s

    combined = tmdb_results + db_results
    # Calcola score una volta sola e tieni traccia (servirà per merge client-side
    # quando la lente unisce risultati film+TV)
    scored = [(item, score(item)) for item in combined]
    scored.sort(key=lambda x: -x[1])

    # Pulisci campi interni prima di restituire
    cleaned = []
    seen = set()
    for item, s in scored:
        # Dedup finale per titolo (case-insensitive)
        key = (item.get("display_title") or item.get("title", "")).lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append({
            "movie_id":      item.get("movie_id"),
            "tmdb_id":       item.get("tmdb_id"),
            "title":         item.get("title"),
            "display_title": item.get("display_title"),
            "_score":        round(s, 1),  # esposto per merge client-side
        })
        if len(cleaned) >= limit:
            break

    # Cache risultato (24h: popolarità non cambia in fretta)
    if cache_set:
        try:
            cache_set(cache_key, cleaned, ttl=24 * 60 * 60)
        except Exception:
            pass

    return cleaned


def search_tv_fast(query: str, limit: int = 8) -> list:
    """Ricerca veloce serie TV su DB locale."""
    from core.recommendation_tv import search_tv_series
    try:
        results = search_tv_series(query, limit=limit)
        return results
    except Exception:
        return []



def get_person_detail(person_id: int) -> dict:
    """
    Dati completi di un attore/regista: bio, foto, filmografia completa.
    Usa append_to_response per una sola chiamata API.
    """
    if not TMDB_API_KEY or not person_id:
        return {}
    try:
        r = requests.get(
            f"https://api.themoviedb.org/3/person/{person_id}",
            params={
                "api_key": TMDB_API_KEY,
                "language": "it-IT",
                "append_to_response": "movie_credits,tv_credits",
            },
            timeout=8
        )
        d = r.json()
        if d.get("status_code") == 34:
            return {}

        profile = d.get("profile_path","")

        # Film — ordina per popolarità, filtra senza poster
        movies = []
        seen = set()
        for m in sorted(d.get("movie_credits",{}).get("cast",[]),
                        key=lambda x: x.get("popularity",0), reverse=True):
            mid = m.get("id")
            if not mid or mid in seen: continue
            seen.add(mid)
            pp = m.get("poster_path","")
            movies.append({
                "tmdb_id":    mid,
                "title":      m.get("title") or m.get("original_title",""),
                "poster_url": f"https://image.tmdb.org/t/p/w342{pp}" if pp else "",
                "year":       (m.get("release_date") or "")[:4],
                "character":  m.get("character",""),
                "vote":       round(m.get("vote_average",0),1),
            })

        # Serie TV
        tvs = []
        seen_tv = set()
        for t in sorted(d.get("tv_credits",{}).get("cast",[]),
                        key=lambda x: x.get("popularity",0), reverse=True):
            tid = t.get("id")
            if not tid or tid in seen_tv: continue
            seen_tv.add(tid)
            pp = t.get("poster_path","")
            tvs.append({
                "tmdb_id":    tid,
                "title":      t.get("name") or t.get("original_name",""),
                "poster_url": f"https://image.tmdb.org/t/p/w342{pp}" if pp else "",
                "year":       (t.get("first_air_date") or "")[:4],
                "character":  t.get("character",""),
                "vote":       round(t.get("vote_average",0),1),
            })

        # Bio troncata a 500 char
        bio = (d.get("biography") or "")
        bio_short = bio[:500] + ("…" if len(bio) > 500 else "")

        return {
            "person_id":   person_id,
            "name":        d.get("name",""),
            "birthday":    d.get("birthday",""),
            "deathday":    d.get("deathday",""),
            "place_of_birth": d.get("place_of_birth",""),
            "biography":   bio,
            "biography_short": bio_short,
            "profile_url": f"https://image.tmdb.org/t/p/w342{profile}" if profile else "",
            "known_for":   d.get("known_for_department",""),
            "movies":      movies,
            "tvs":         tvs,
        }
    except Exception:
        return {}



# Mappa generi TMDb
GENRE_MAP_MOVIE = {
    "azione": 28, "avventura": 12, "animazione": 16, "commedia": 35,
    "crimine": 80, "documentario": 99, "dramma": 18, "fantasy": 14,
    "horror": 27, "musica": 10402, "mistero": 9648, "romantico": 10749,
    "fantascienza": 878, "thriller": 53, "guerra": 10752, "western": 37,
}

GENRE_MAP_TV = {
    "azione": 10759, "animazione": 16, "commedia": 35, "crimine": 80,
    "documentario": 99, "dramma": 18, "fantasy": 10765, "kids": 10762,
    "mistero": 9648, "news": 10763, "reality": 10764, "fantascienza": 10765,
    "soap": 10766, "talk": 10767, "thriller": 9648, "guerra": 10768, "western": 37,
}

MOOD_GENRES = {
    "leggero":      {"movie": [35, 16, 10749], "tv": [35, 16, 10762]},
    "intenso":      {"movie": [53, 28, 80],    "tv": [9648, 80, 10759]},
    "romantico":    {"movie": [10749, 35, 18], "tv": [10749, 35, 18]},
    "adrenalinico": {"movie": [28, 12, 53],    "tv": [10759, 10765, 80]},
    "riflessivo":   {"movie": [18, 99, 36],    "tv": [18, 99, 10768]},
    "spaventoso":   {"movie": [27, 53, 9648],  "tv": [9648, 80, 53]},
}

PLATFORM_MAP = {
    "netflix":   8,
    "prime":     9,
    "disney":    337,
    "apple":     350,
    "paramount": 531,
    "now":       39,
}


def get_scopri_results(
    tipo: str = "film",
    genere: str = "",
    mood: str = "",
    piattaforma: str = "",
    anno: str = "",
    voto: str = "",
    page: int = 1,
    limit: int = 20,
) -> dict:
    """
    Risultati per la pagina /scopri con filtri combinabili.
    Usa TMDb /discover con i parametri giusti.
    Ritorna: {"results": [...], "total": int, "page": int}
    """
    if not TMDB_API_KEY:
        return {"results": [], "total": 0, "page": 1}

    import datetime
    today = datetime.date.today()

    is_tv = tipo == "serie"
    endpoint = "tv" if is_tv else "movie"

    params = {
        "api_key":          TMDB_API_KEY,
        "language":         "it-IT",
        "sort_by":          "popularity.desc",
        "vote_count.gte":   50,
        "page":             page,
        "watch_region":     "IT",
    }

    # Genere
    if genere and not mood:
        gmap = GENRE_MAP_TV if is_tv else GENRE_MAP_MOVIE
        genre_id = gmap.get(genere.lower())
        if genre_id:
            params["with_genres"] = genre_id

    # Mood → più generi
    if mood:
        mood_genres = MOOD_GENRES.get(mood.lower(), {})
        gids = mood_genres.get("tv" if is_tv else "movie", [])
        if gids:
            params["with_genres"] = "|".join(str(g) for g in gids[:2])

    # Piattaforma
    if piattaforma:
        pid = PLATFORM_MAP.get(piattaforma.lower())
        if pid:
            params["with_watch_providers"] = pid
            params["with_watch_monetization_types"] = "flatrate"

    # Anno
    date_field_gte = "first_air_date.gte" if is_tv else "primary_release_date.gte"
    date_field_lte = "first_air_date.lte" if is_tv else "primary_release_date.lte"

    if anno == "recenti":
        params[date_field_gte] = (today - datetime.timedelta(days=180)).isoformat()
    elif anno == "anno":
        params[date_field_gte] = (today - datetime.timedelta(days=365)).isoformat()
    elif anno == "classici":
        params[date_field_lte] = "2000-12-31"
        params["vote_count.gte"] = 200

    # Voto minimo
    if voto == "7":
        params["vote_average.gte"] = 7.0
    elif voto == "8":
        params["vote_average.gte"] = 8.0

    try:
        r = requests.get(
            f"https://api.themoviedb.org/3/discover/{endpoint}",
            params=params,
            timeout=8
        )
        data = r.json()
        raw  = data.get("results", [])
        total = min(data.get("total_results", 0), 500)

        import re as _re
        _non_latin = _re.compile(r'[\u0400-\u04FF\u0600-\u06FF\u4E00-\u9FFF\u3040-\u309F\u30A0-\u30FF]')

        results = []
        for item in raw[:limit]:
            pp = item.get("poster_path","")
            bp = item.get("backdrop_path","")
            title = item.get("title") or item.get("name") or item.get("original_title") or item.get("original_name","")
            if not title or not pp:
                continue
            # Salta titoli con caratteri non latini (cirillico, arabo, cinese, giapponese)
            if _non_latin.search(title):
                continue
            results.append({
                "tmdb_id":      item.get("id"),
                "title":        title,
                "poster_url":   f"https://image.tmdb.org/t/p/w342{pp}" if pp else "",
                "backdrop_url": f"https://image.tmdb.org/t/p/w780{bp}" if bp else "",
                "vote_average": round(item.get("vote_average",0),1),
                "vote_count":   item.get("vote_count",0),
                "release_date": item.get("release_date","") or item.get("first_air_date",""),
                "overview":     (item.get("overview","") or "")[:200],
                "content_type": "tv" if is_tv else "movie",
            })

        return {"results": results, "total": total, "page": page}

    except Exception:
        return {"results": [], "total": 0, "page": page}



# Configurazione strip per /scopri — ordinate per engagement
SCOPRI_STRIPS = [
    {"id": "thriller",     "label": "Thriller",          "emoji": "🔪", "genre_movie": 53,    "genre_tv": 9648},
    {"id": "azione",       "label": "Azione",            "emoji": "💥", "genre_movie": 28,    "genre_tv": 10759},
    {"id": "commedia",     "label": "Commedia",          "emoji": "😄", "genre_movie": 35,    "genre_tv": 35},
    {"id": "horror",       "label": "Horror",            "emoji": "👻", "genre_movie": 27,    "genre_tv": 9648},
    {"id": "dramma",       "label": "Dramma",            "emoji": "🎭", "genre_movie": 18,    "genre_tv": 18},
    {"id": "fantascienza", "label": "Fantascienza",      "emoji": "🚀", "genre_movie": 878,   "genre_tv": 10765},
    {"id": "romantico",    "label": "Romantico",         "emoji": "❤️", "genre_movie": 10749, "genre_tv": 10749},
    {"id": "animazione",   "label": "Animazione",        "emoji": "🎨", "genre_movie": 16,    "genre_tv": 16},
    {"id": "crimine",      "label": "Crimine",           "emoji": "🕵️", "genre_movie": 80,    "genre_tv": 80},
    {"id": "documentario", "label": "Documentario",      "emoji": "🎥", "genre_movie": 99,    "genre_tv": 99},
]


def _fetch_strip(strip_cfg: dict, tipo: str, limit: int = 12) -> list:
    """Carica una singola strip di contenuti da TMDb."""
    if not TMDB_API_KEY:
        return []
    is_tv    = tipo == "serie"
    endpoint = "tv" if is_tv else "movie"
    genre_id = strip_cfg["genre_tv"] if is_tv else strip_cfg["genre_movie"]
    try:
        r = requests.get(
            f"https://api.themoviedb.org/3/discover/{endpoint}",
            params={
                "api_key":        TMDB_API_KEY,
                "language":       "it-IT",
                "sort_by":        "popularity.desc",
                "with_genres":    genre_id,
                "vote_count.gte": 100,
                "watch_region":   "IT",
            },
            timeout=6
        )
        results = []
        for item in r.json().get("results", [])[:limit]:
            pp = item.get("poster_path","")
            title = item.get("title") or item.get("name") or ""
            if not title or not pp:
                continue
            results.append({
                "tmdb_id":      item.get("id"),
                "title":        title,
                "poster_url":   f"https://image.tmdb.org/t/p/w342{pp}",
                "vote_average": round(item.get("vote_average",0),1),
                "release_date": item.get("release_date","") or item.get("first_air_date",""),
                "content_type": "tv" if is_tv else "movie",
            })
        return results
    except Exception:
        return []


def get_scopri_strips(tipo: str = "film") -> list:
    """
    Carica tutte le strip in parallelo con deduplicazione globale —
    ogni titolo appare in una sola strip (la più rilevante per popolarità).
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Fetch parallelo — ogni strip chiede più titoli per compensare la dedup
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {
            ex.submit(_fetch_strip, s, tipo, 20): s  # 20 invece di 12
            for s in SCOPRI_STRIPS
        }
        results_map = {}
        for fut in as_completed(futures):
            strip_cfg = futures[fut]
            try:
                results_map[strip_cfg["id"]] = fut.result()
            except Exception:
                results_map[strip_cfg["id"]] = []

    # Deduplicazione globale — ogni tmdb_id appare solo nella prima strip
    seen_ids = set()
    strips = []
    for s in SCOPRI_STRIPS:
        raw = results_map.get(s["id"], [])
        unique = []
        for item in raw:
            tid = item.get("tmdb_id")
            if tid and tid not in seen_ids:
                seen_ids.add(tid)
                unique.append(item)
            if len(unique) >= 12:
                break
        if unique:
            strips.append({
                "id":    s["id"],
                "label": s["label"],
                "emoji": s["emoji"],
                "cards": unique,
            })

    return strips



def get_similar_movies_tmdb(tmdb_id: int, limit: int = 6) -> list:
    """
    Recupera film simili via TMDb /movie/{id}/similar.
    Usato come fallback quando l'algoritmo principale non trova risultati.
    """
    if not TMDB_API_KEY or not tmdb_id:
        return []
    try:
        r = requests.get(
            f"https://api.themoviedb.org/3/movie/{tmdb_id}/similar",
            params={"api_key": TMDB_API_KEY, "language": "it-IT"},
            timeout=5
        )
        results = []
        for item in r.json().get("results", [])[:limit]:
            pp = item.get("poster_path","")
            title = item.get("title") or item.get("original_title","")
            if not title: continue
            results.append({
                "title":        title,
                "poster_url":   f"https://image.tmdb.org/t/p/w342{pp}" if pp else "",
                "tmdb_id":      item.get("id"),
                "vote_average": round(item.get("vote_average",0),1),
                "overview":     (item.get("overview","") or "")[:200],
                "is_fallback":  True,
            })
        return results
    except Exception:
        return []


def get_popular_by_genre_tmdb(genre_id: int, content_type: str = "movie", limit: int = 6) -> list:
    """
    Recupera titoli popolari di un genere via TMDb /discover.
    Usato come fallback finale garantito.
    """
    if not TMDB_API_KEY:
        return []
    endpoint = "tv" if content_type == "tv" else "movie"
    try:
        r = requests.get(
            f"https://api.themoviedb.org/3/discover/{endpoint}",
            params={
                "api_key":        TMDB_API_KEY,
                "language":       "it-IT",
                "sort_by":        "popularity.desc",
                "with_genres":    genre_id,
                "vote_count.gte": 500,
                "watch_region":   "IT",
            },
            timeout=5
        )
        results = []
        for item in r.json().get("results", [])[:limit]:
            pp = item.get("poster_path","")
            title = item.get("title") or item.get("name") or ""
            if not title: continue
            results.append({
                "title":        title,
                "poster_url":   f"https://image.tmdb.org/t/p/w342{pp}" if pp else "",
                "tmdb_id":      item.get("id"),
                "vote_average": round(item.get("vote_average",0),1),
                "overview":     (item.get("overview","") or "")[:200],
                "is_fallback":  True,
            })
        return results
    except Exception:
        return []

# Mappa ID piattaforma TMDb → nome + colore brand
# ── Configurazione affiliati ──────────────────────────────────────────────
# Inserisci i tuoi tag/ID affiliazione nelle variabili d'ambiente su Render:
#   AFFILIATE_AMAZON   = il_tuo_tag (es. cosaguardo-21)
#   AFFILIATE_AWIN_ID  = il_tuo_id_awin (es. 123456)
#   AFFILIATE_APPLE    = il_tuo_token_apple
# Se non configurati, i link puntano direttamente alle piattaforme (no commissione).

def _build_affiliate_link(provider_name: str, title: str = "", tmdb_id: int = None) -> str:
    """
    Costruisce link affiliato per la piattaforma.
    Se il tag affiliato non è configurato, restituisce stringa vuota
    (in quel caso si usa il link JustWatch di default).
    """
    import urllib.parse

    title_enc = urllib.parse.quote_plus(title) if title else ""

    # ── Amazon Associates — Prime Video (tutte le varianti) ────────────────
    # TMDb usa nomi diversi per Amazon a seconda del paese/sottoscrizione:
    #   - "Prime Video" (id 9, 119)
    #   - "Amazon Video" (id 10) — noleggio/acquisto
    #   - "Amazon Prime Video" (id 119, 9) — abbonamento principale
    #   - "Amazon Prime Video with Ads" (id 1796) — abbonamento con pubblicità
    # Il check qui sotto cattura tutte le varianti ("Amazon" o "Prime Video" nel nome).
    amazon_tag = os.environ.get("AFFILIATE_AMAZON", "")
    if amazon_tag:
        pn_lower = (provider_name or "").lower()
        is_amazon = ("amazon" in pn_lower) or ("prime video" in pn_lower)
        if is_amazon:
            # Link di ricerca Amazon con tag affiliato
            return f"https://www.amazon.it/s?k={title_enc}&i=instant-video&tag={amazon_tag}"

    # ── Apple TV+ — Apple Performance Partners (Partnerize) ────────────────
    # Formato ufficiale: https://geo.tv.apple.com/<region>/<path>?at=<token>&ct=<campaign>
    # Il dominio geo.tv.apple.com fa redirect automatico al market dell'utente.
    # ct = campaign token (max 40 char, no '?', '!', '&') — utile per tracking.
    apple_token = os.environ.get("AFFILIATE_APPLE", "")
    if provider_name == "Apple TV+" and apple_token:
        return (
            f"https://geo.tv.apple.com/it/search?term={title_enc}"
            f"&at={apple_token}&ct=cosaguardo_recs"
        )

    # ── Awin — solo programmi davvero disponibili su Awin Italia ───────────
    # Nota: Disney+, Paramount+ e Netflix NON hanno programma Awin IT,
    # per quei provider si usa JustWatch come fallback automatico.
    awin_id = os.environ.get("AFFILIATE_AWIN_ID", "")
    if awin_id:
        # Mappa provider → (env var con merchant ID, URL destinazione)
        # I merchant ID si trovano sul pannello Awin dopo l'approvazione
        # del programma (vedi anche env vars AWIN_MID_NOW / AWIN_MID_TIMVISION).
        awin_programs = {
            "NOW":       (os.environ.get("AWIN_MID_NOW", ""),       "https://www.nowtv.it/"),
            "NOW TV":    (os.environ.get("AWIN_MID_NOW", ""),       "https://www.nowtv.it/"),
            "TIMVISION": (os.environ.get("AWIN_MID_TIMVISION", ""), "https://www.timvision.it/"),
        }
        if provider_name in awin_programs:
            mid, dest_url = awin_programs[provider_name]
            if mid:
                dest_enc = urllib.parse.quote_plus(dest_url)
                return f"https://www.awin1.com/cread.php?awinmid={mid}&awinaffid={awin_id}&ued={dest_enc}"

    return ""  # Nessun affiliato configurato → usa JustWatch


PROVIDER_META = {
    8:    {"name": "Netflix",        "color": "#E50914"},
    9:    {"name": "Prime Video",    "color": "#00A8E0"},
    10:   {"name": "Amazon Video",   "color": "#00A8E0"},
    35:   {"name": "Rakuten TV",     "color": "#BF0000"},
    39:   {"name": "NOW",            "color": "#00BCD4"},
    40:   {"name": "Chili",          "color": "#FF6600"},
    119:  {"name": "Prime Video",    "color": "#00A8E0"},
    149:  {"name": "Rakuten TV",     "color": "#BF0000"},
    235:  {"name": "TIMVISION",      "color": "#E60000"},
    337:  {"name": "Disney+",        "color": "#113CCF"},
    341:  {"name": "Apple TV+",      "color": "#555555"},
    350:  {"name": "Apple TV+",      "color": "#555555"},
    381:  {"name": "Canal+",         "color": "#000000"},
    531:  {"name": "Paramount+",     "color": "#0064FF"},
    619:  {"name": "Disney+",        "color": "#113CCF"},
    1796: {"name": "Prime Video",    "color": "#00A8E0"},  # Amazon Prime Video with Ads
}


def _normalize_provider_name(raw_name: str, pid: int = None) -> tuple[str, str]:
    """
    Restituisce (nome_canonico, colore) per un provider TMDb.

    Strategia robusta:
    1. Se il provider_id è in PROVIDER_META → usa quello (path veloce)
    2. Altrimenti, pattern matching sul nome per varianti note
       (es. "Amazon Prime Video with Ads" → "Prime Video")

    Questo evita che ogni nuova variante TMDb (es. "Apple TV+ con pubblicità",
    "Disney+ Standard with Ads") sfugga al raggruppamento.
    """
    # 1. Lookup esplicito (path veloce)
    if pid and pid in PROVIDER_META:
        meta = PROVIDER_META[pid]
        return (meta["name"], meta.get("color", "#444"))

    # 2. Pattern matching sul nome (case-insensitive)
    name_lower = (raw_name or "").lower()

    # Amazon (tutte le varianti: with Ads, Prime, Channels, ...)
    if "amazon" in name_lower or "prime video" in name_lower:
        return ("Prime Video", "#00A8E0")

    # Apple (qualunque variante)
    if "apple tv" in name_lower:
        return ("Apple TV+", "#555555")

    # Disney
    if "disney" in name_lower:
        return ("Disney+", "#113CCF")

    # Netflix (sometimes "Netflix Standard with Ads")
    if "netflix" in name_lower:
        return ("Netflix", "#E50914")

    # Paramount
    if "paramount" in name_lower:
        return ("Paramount+", "#0064FF")

    # Default: usa il nome originale TMDb
    return (raw_name or "", "#444")
# ──────────────────────────────────────────────────────────────────────────


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
                pid       = p.get("provider_id")
                logo_path = p.get("logo_path", "")
                raw_name  = p.get("provider_name", "")
                prov_name, prov_color = _normalize_provider_name(raw_name, pid)
                # Dedup sul nome FINALE: così "Amazon Prime Video with Ads" e
                # "Amazon Prime Video" entrambi mappati a "Prime Video" non si duplicano
                if prov_name in seen:
                    continue
                seen.add(prov_name)
                aff_link  = _build_affiliate_link(prov_name, title=title)
                out.append({
                    "name":         prov_name,
                    "logo_url":     f"https://image.tmdb.org/t/p/w45{logo_path}" if logo_path else "",
                    "color":        prov_color,
                    "link":         aff_link or justwatch_link,
                    "is_affiliate": bool(aff_link),
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


# ─── Home platform strip ──────────────────────────────────────────────────
# Lista hardcoded dei provider_id TMDb per le piattaforme principali italiane.
# Sono ID stabili (cambiano molto raramente), ma se TMDb dovesse rinominare
# qualcosa la chiamata fallback e mostriamo solo quelli che TMDb ci ritorna.
HOME_PLATFORM_IDS = [
    8,    # Netflix
    119,  # Amazon Prime Video
    337,  # Disney+
    39,   # NOW
    350,  # Apple TV+
    531,  # Paramount+
    261,  # RaiPlay
    484,  # Mediaset Infinity
    29,   # Sky Go
    283,  # Crunchyroll
]

def get_home_platforms() -> list[dict]:
    """
    Ritorna i 10 provider streaming principali con loghi TMDb.
    Cache 30 giorni (loghi cambiano raramente).

    Output: [{provider_id, name, logo_url}, ...]
    Ordine: HOME_PLATFORM_IDS (manualmente curato per rilevanza IT).
    Se TMDb non risponde o non ha un provider, viene saltato.
    """
    cache_key = "home:platforms:v2:IT:movie"
    try:
        from core.tmdb_cache import cache_get, cache_set
        cached = cache_get(cache_key)
        if cached is not None:
            return cached
    except Exception:
        cache_get = None
        cache_set = None

    if not TMDB_API_KEY:
        return []

    # Recupera lista completa provider movie + tv per la regione IT.
    # Movie copre la maggior parte; TV usato come merge per provider mancanti.
    by_id = {}

    for endpoint in ("movie", "tv"):
        try:
            r = requests.get(
                f"https://api.themoviedb.org/3/watch/providers/{endpoint}",
                params={
                    "api_key": TMDB_API_KEY,
                    "language": "it-IT",
                    "watch_region": "IT",
                },
                timeout=5,
            )
            for p in (r.json().get("results") or []):
                pid = p.get("provider_id")
                if pid in HOME_PLATFORM_IDS and pid not in by_id:
                    by_id[pid] = {
                        "provider_id": pid,
                        "slug":        PLATFORM_ID_TO_SLUG.get(pid, ""),
                        "name":        p.get("provider_name") or "",
                        "logo_url":    f"https://image.tmdb.org/t/p/original{p.get('logo_path','')}"
                                       if p.get("logo_path") else "",
                    }
        except Exception:
            continue

    # Ordina secondo HOME_PLATFORM_IDS (manualmente curato), salta mancanti
    ordered = [by_id[pid] for pid in HOME_PLATFORM_IDS if pid in by_id and by_id[pid]["logo_url"]]

    if cache_set and ordered:
        try:
            cache_set(cache_key, ordered, ttl=30 * 24 * 60 * 60)  # 30gg
        except Exception:
            pass

    return ordered


# ═════════════════════════════════════════════════════════════════════════
# PAGINA FILTRO PIATTAFORMA — /piattaforma/{slug}
# ═════════════════════════════════════════════════════════════════════════

# Mappa slug URL → (provider_id TMDb, nome display, URL ufficiale per CTA)
# Slug volutamente in italiano, semplice, SEO-friendly. Sono URL stabili.
PLATFORM_SLUGS = {
    "netflix":            (8,   "Netflix",            "https://www.netflix.com/it/"),
    "prime-video":        (119, "Prime Video",        "https://www.primevideo.com/"),
    "disney-plus":        (337, "Disney+",            "https://www.disneyplus.com/it-it"),
    "now":                (39,  "NOW",                "https://www.nowtv.it/"),
    "apple-tv-plus":      (350, "Apple TV+",          "https://tv.apple.com/it"),
    "paramount-plus":     (531, "Paramount+",         "https://www.paramountplus.com/it/"),
    "raiplay":            (261, "RaiPlay",            "https://www.raiplay.it/"),
    "mediaset-infinity":  (484, "Mediaset Infinity",  "https://mediasetinfinity.mediaset.it/"),
    "sky-go":             (29,  "Sky Go",             "https://www.sky.it/sky-go"),
    "crunchyroll":        (283, "Crunchyroll",        "https://www.crunchyroll.com/it/"),
}

# Mappa inversa per URL building dalla home (provider_id → slug)
PLATFORM_ID_TO_SLUG = {pid: slug for slug, (pid, _, _) in PLATFORM_SLUGS.items()}


def get_platform_subscribe_link(slug: str) -> str:
    """
    Costruisce il CTA "Abbonati a X" per la pagina piattaforma.
    Usa _build_affiliate_link se programma attivo, altrimenti link ufficiale.
    """
    if slug not in PLATFORM_SLUGS:
        return ""
    pid, name, fallback_url = PLATFORM_SLUGS[slug]

    # Prova affiliate (vuota se non configurato)
    aff = _build_affiliate_link(name, title="", tmdb_id=None)
    if aff:
        return aff

    # Fallback: link ufficiale alla piattaforma
    return fallback_url


def get_platform_content(slug: str, content_type: str = "movie", limit: int = 60) -> tuple:
    """
    Top film/serie più popolari su una piattaforma streaming.
    Cache 6h.

    Returns: (items, is_fallback)
        items: lista di dict (tmdb_id, title, poster_url, rating, year, content_type)
        is_fallback: True se TMDb aveva troppo pochi risultati per questa piattaforma
                     e abbiamo riempito con i top popolari generali (es. RaiPlay).

    Soglia fallback: <12 risultati = piattaforma "povera" su TMDb.
    """
    if slug not in PLATFORM_SLUGS or not TMDB_API_KEY:
        return [], False

    pid, _, _ = PLATFORM_SLUGS[slug]
    ct = "tv" if content_type == "tv" else "movie"

    cache_key = f"platform_content:v2:{slug}:{ct}:{limit}"
    try:
        from core.tmdb_cache import cache_get, cache_set
        cached = cache_get(cache_key)
        if cached is not None:
            # cached è [items, is_fallback]
            if isinstance(cached, list) and len(cached) == 2 and isinstance(cached[0], list):
                return cached[0], cached[1]
            # retrocompatibilità v1 cache: era una lista flat
            return cached, False
    except Exception:
        cache_get = None
        cache_set = None

    # 1. Tentativo standard: filtro per piattaforma
    results = _discover_tmdb(ct=ct, with_provider=pid, limit=limit)

    # 2. Se troppo pochi → fallback "popolari in Italia"
    is_fallback = False
    if len(results) < 12:
        is_fallback = True
        seen_ids = {r["tmdb_id"] for r in results}
        extra = _discover_tmdb(ct=ct, with_provider=None, limit=limit + 5)
        for r in extra:
            if r["tmdb_id"] not in seen_ids:
                results.append(r)
                seen_ids.add(r["tmdb_id"])
                if len(results) >= limit:
                    break

    if cache_set and results:
        try:
            cache_set(cache_key, [results, is_fallback], ttl=6 * 60 * 60)
        except Exception:
            pass

    return results, is_fallback


def _discover_tmdb(ct: str, with_provider: int = None, limit: int = 60) -> list:
    """Helper: chiama TMDb /discover su 3 pagine, restituisce lista normalizzata."""
    out = []
    seen_ids = set()

    for page in range(1, 4):
        if len(out) >= limit:
            break
        try:
            params = {
                "api_key": TMDB_API_KEY,
                "language": "it-IT",
                "watch_region": "IT",
                "sort_by": "popularity.desc",
                "page": page,
                "vote_count.gte": 5,
            }
            if with_provider:
                params["with_watch_providers"] = with_provider
            r = requests.get(
                f"https://api.themoviedb.org/3/discover/{ct}",
                params=params,
                timeout=6,
            )
            for item in r.json().get("results", []):
                tid = item.get("id")
                if not tid or tid in seen_ids:
                    continue
                seen_ids.add(tid)
                poster = item.get("poster_path")
                if not poster:
                    continue

                if ct == "movie":
                    title = item.get("title") or item.get("original_title", "")
                    date  = item.get("release_date", "")
                else:
                    title = item.get("name") or item.get("original_name", "")
                    date  = item.get("first_air_date", "")
                if not title:
                    continue

                out.append({
                    "tmdb_id":      tid,
                    "title":        title,
                    "poster_url":   f"https://image.tmdb.org/t/p/w342{poster}",
                    "rating":       round(float(item.get("vote_average") or 0), 1),
                    "year":         date[:4] if date else "",
                    "content_type": ct,
                })
                if len(out) >= limit:
                    break
        except Exception:
            continue

    return out


def get_platform_meta(slug: str) -> dict:
    """
    Metadata per l'header pagina piattaforma:
    nome display, logo TMDb (recuperato da get_home_platforms cache), CTA abbonamento.
    """
    if slug not in PLATFORM_SLUGS:
        return {}
    pid, name, _ = PLATFORM_SLUGS[slug]

    # Riusa la cache di get_home_platforms per il logo
    logo_url = ""
    try:
        platforms = get_home_platforms()
        for p in platforms:
            if p.get("provider_id") == pid:
                logo_url = p.get("logo_url", "")
                break
    except Exception:
        pass

    return {
        "slug":            slug,
        "provider_id":     pid,
        "name":            name,
        "logo_url":        logo_url,
        "subscribe_link":  get_platform_subscribe_link(slug),
    }


# ═════════════════════════════════════════════════════════════════════════
# PAGINE SEO "/migliori-{tipo}-{genere}-su-{piattaforma}"
# ═════════════════════════════════════════════════════════════════════════

# Selezione pilot: 6 generi × 2 tipi × 5 piattaforme = 60 pagine.
# Limitato alle piattaforme con catalogo TMDb completo (le big 5).
BEST_PILOT_GENRES = ["thriller", "comedy", "drammatici", "azione", "fantasy", "horror"]

# Mappa genere "pilot" → (genre_id_movie, genre_id_tv, label_singolare, label_plurale)
# Label per i titoli SEO ("migliori film thriller", "migliori serie TV comedy")
BEST_GENRE_META = {
    "thriller":    {"movie_id": 53,  "tv_id": 9648, "label": "thriller"},
    "comedy":      {"movie_id": 35,  "tv_id": 35,   "label": "commedia"},
    "drammatici":  {"movie_id": 18,  "tv_id": 18,   "label": "drammatici"},
    "azione":      {"movie_id": 28,  "tv_id": 10759,"label": "d'azione"},
    "fantasy":     {"movie_id": 14,  "tv_id": 10765,"label": "fantasy"},
    "horror":      {"movie_id": 27,  "tv_id": 9648, "label": "horror"},
}

BEST_PILOT_PLATFORMS = [
    "netflix", "prime-video", "disney-plus", "apple-tv-plus", "paramount-plus",
]


def parse_best_slug(slug: str) -> dict | None:
    """
    Decompone uno slug "/migliori-{slug}" nei suoi componenti.

    Esempi accettati:
        film-thriller-su-netflix       → {tipo:film, genere:thriller, platform:netflix}
        serie-tv-comedy-su-prime-video → {tipo:serie, genere:comedy, platform:prime-video}

    Restituisce None se il pattern non matcha (404).
    """
    if not slug or "-su-" not in slug:
        return None

    left, _, platform = slug.partition("-su-")
    if platform not in BEST_PILOT_PLATFORMS:
        return None

    # left è "{tipo}-{genere}". Tipo può essere "film" o "serie-tv".
    if left.startswith("serie-tv-"):
        tipo = "serie"
        genere = left[len("serie-tv-"):]
    elif left.startswith("film-"):
        tipo = "film"
        genere = left[len("film-"):]
    else:
        return None

    if genere not in BEST_PILOT_GENRES:
        return None

    return {"tipo": tipo, "genere": genere, "platform": platform}


def build_best_slug(tipo: str, genere: str, platform: str) -> str:
    """Inverso di parse_best_slug. Costruisce lo slug canonico."""
    type_part = "serie-tv" if tipo == "serie" else "film"
    return f"{type_part}-{genere}-su-{platform}"


def iter_all_best_combos():
    """
    Itera tutte le 60 combinazioni pilot.
    Yields: (slug, tipo, genere, platform) per ognuna.
    Usato dalla sitemap.
    """
    for tipo in ("film", "serie"):
        for genere in BEST_PILOT_GENRES:
            for platform in BEST_PILOT_PLATFORMS:
                slug = build_best_slug(tipo, genere, platform)
                yield slug, tipo, genere, platform


def get_best_content(tipo: str, genere: str, platform: str, limit: int = 30) -> list:
    """
    Top {limit} titoli per la combinazione tipo+genere+piattaforma.
    Cache 6h (popolarità fluttua poco).

    Usa _discover_tmdb come backbone (estensione di /piattaforma/).
    Ritorna lista di dict con: tmdb_id, title, poster_url, rating, year,
    content_type, overview (per le top 5 con descrizione).
    """
    if not TMDB_API_KEY:
        return []

    if platform not in PLATFORM_SLUGS or genere not in BEST_GENRE_META:
        return []

    pid, _, _ = PLATFORM_SLUGS[platform]
    ct = "tv" if tipo == "serie" else "movie"

    genre_id = BEST_GENRE_META[genere].get("tv_id" if ct == "tv" else "movie_id")
    if not genre_id:
        return []

    cache_key = f"best:v1:{tipo}:{genere}:{platform}:{limit}"
    try:
        from core.tmdb_cache import cache_get, cache_set
        cached = cache_get(cache_key)
        if cached is not None:
            return cached
    except Exception:
        cache_get = None
        cache_set = None

    # Chiamata TMDb con doppio filtro: provider + genre
    out = []
    seen_ids = set()
    for page in range(1, 4):
        if len(out) >= limit:
            break
        try:
            r = requests.get(
                f"https://api.themoviedb.org/3/discover/{ct}",
                params={
                    "api_key": TMDB_API_KEY,
                    "language": "it-IT",
                    "watch_region": "IT",
                    "with_watch_providers": pid,
                    "with_genres": genre_id,
                    "sort_by": "popularity.desc",
                    "page": page,
                    "vote_count.gte": 50,  # qualità più stretta delle pagine piattaforma
                    "vote_average.gte": 6.0,  # solo "buoni"/"ottimi"
                },
                timeout=6,
            )
            for item in r.json().get("results", []):
                tid = item.get("id")
                if not tid or tid in seen_ids:
                    continue
                seen_ids.add(tid)
                poster = item.get("poster_path")
                if not poster:
                    continue

                if ct == "movie":
                    title = item.get("title") or item.get("original_title", "")
                    date  = item.get("release_date", "")
                else:
                    title = item.get("name") or item.get("original_name", "")
                    date  = item.get("first_air_date", "")
                if not title:
                    continue

                out.append({
                    "tmdb_id":      tid,
                    "title":        title,
                    "poster_url":   f"https://image.tmdb.org/t/p/w342{poster}",
                    "rating":       round(float(item.get("vote_average") or 0), 1),
                    "year":         date[:4] if date else "",
                    "content_type": ct,
                    "overview":     (item.get("overview") or "").strip(),
                })
                if len(out) >= limit:
                    break
        except Exception:
            continue

    if cache_set and out:
        try:
            cache_set(cache_key, out, ttl=6 * 60 * 60)
        except Exception:
            pass

    return out


def get_best_meta(tipo: str, genere: str, platform: str) -> dict:
    """
    Metadata SEO + display per la pagina "I migliori {tipo} {genere} su {platform}".
    """
    if platform not in PLATFORM_SLUGS or genere not in BEST_GENRE_META:
        return {}

    _, platform_name, _ = PLATFORM_SLUGS[platform]
    genre_label = BEST_GENRE_META[genere]["label"]

    # "I migliori film thriller su Netflix" / "Le migliori serie TV thriller su Netflix"
    if tipo == "serie":
        h1   = f"Le migliori serie TV {genre_label} su {platform_name}"
        seo_title = f"Migliori serie TV {genre_label} su {platform_name} | CosaGuardo"
        seo_desc  = (
            f"Le migliori serie TV {genre_label} disponibili su {platform_name} oggi in Italia. "
            f"Classifica aggiornata in base alla popolarità e ai voti del pubblico."
        )
    else:
        h1   = f"I migliori film {genre_label} su {platform_name}"
        seo_title = f"Migliori film {genre_label} su {platform_name} | CosaGuardo"
        seo_desc  = (
            f"I migliori film {genre_label} disponibili su {platform_name} oggi in Italia. "
            f"Classifica aggiornata in base alla popolarità e ai voti del pubblico."
        )

    return {
        "h1":               h1,
        "seo_title":        seo_title,
        "seo_desc":         seo_desc,
        "tipo":             tipo,
        "genere":           genere,
        "genre_label":      genre_label,
        "platform":         platform,
        "platform_name":    platform_name,
        "platform_logo":    "",  # popolato dalla view
        "platform_subscribe_link": get_platform_subscribe_link(platform),
    }
