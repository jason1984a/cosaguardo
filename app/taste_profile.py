from collections import Counter
import json

# Importa qui le funzioni già esistenti del tuo progetto.
# Dovrai adattare SOLO questi import al nome reale dei tuoi file/funzioni.
#
# Esempio:
# from app.recommend_movie import find_movie_by_title, get_movie_keywords
# from app.recommend_tv import find_tv_by_title, get_tv_keywords, build_tv_ui_signals
#
# Se hai funzioni con nomi diversi, basta sostituire gli import.

try:
    from core.recommendation_api import find_movie_by_title, get_movie_keywords
except Exception:
    find_movie_by_title = None
    get_movie_keywords = None

try:
    from core.recommendation_tv import find_tv_by_title, get_tv_keywords, build_tv_ui_signals
except Exception:
    find_tv_by_title = None
    get_tv_keywords = None
    build_tv_ui_signals = None

GENRE_BLACKLIST = {
    "TV Movie",
}

KEYWORD_BLACKLIST = {
    "",
    "movie",
    "tv",
    "series",
    "film",
}

VIBE_KEYWORD_MAP = {
    "detective": "investigativo",
    "investigation": "investigativo",
    "murder": "oscuro",
    "serial killer": "oscuro",
    "kidnapping": "teso",
    "revenge": "intenso",
    "thriller": "teso",
    "neo-noir": "atmosferico",
    "crime": "crudo",
    "drugs": "crudo",
    "drug cartel": "crudo",
    "drug lord": "crudo",
    "mafia": "crudo",
    "mind control": "cerebrale",
    "time travel": "cerebrale",
    "parallel world": "cerebrale",
    "space opera": "epico",
    "alien invasion": "adrenalinico",
    "post apocalypse": "survival",
    "apocalypse": "survival",
    "zombie": "survival",
    "magic": "fantasy",
    "dragon": "epico",
    "kingdom": "epico",
    "political intrigue": "intrigante",
    "friendship": "emotivo",
    "family": "emotivo",
    "romance": "romantico",
}

def parse_seed_titles(seed_titles_raw):
    if not seed_titles_raw:
        return []

    # Caso 1: salvato come JSON list
    try:
        parsed = json.loads(seed_titles_raw)
        if isinstance(parsed, list):
            return [str(x).strip() for x in parsed if str(x).strip()]
    except Exception:
        pass

    # Caso 2: salvato come stringa separata da virgole
    return [x.strip() for x in seed_titles_raw.split(",") if x.strip()]


def normalize_keyword(keyword):
    if not keyword:
        return None

    value = str(keyword).strip().lower()
    if not value or value in KEYWORD_BLACKLIST:
        return None

    return value

def keyword_to_vibe(keyword):
    normalized = normalize_keyword(keyword)
    if not normalized:
        return None

    return VIBE_KEYWORD_MAP.get(normalized)

def extract_vibes_from_item(item, content_type):
    vibes = []

    # 1. vibes da keyword
    for keyword in item.get("keywords", []):
        if isinstance(keyword, dict):
            keyword = keyword.get("name")

        vibe = keyword_to_vibe(keyword)
        if vibe:
            vibes.append(vibe)

    # 2. vibes leggere da generi
    genres = item.get("genres", []) or []

    genre_vibe_map = {
        "Sci-Fi": "cerebrale",
        "Sci-Fi & Fantasy": "cerebrale",
        "Thriller": "teso",
        "Crime": "crudo",
        "Drama": "intenso",
        "Mystery": "investigativo",
        "Fantasy": "epico",
        "Action": "adrenalinico",
        "Action & Adventure": "adrenalinico",
        "Horror": "oscuro",
        "Romance": "romantico",
    }

    for genre in genres:
        vibe = genre_vibe_map.get(genre)
        if vibe:
            vibes.append(vibe)

    # dedup mantenendo ordine
    vibes = list(dict.fromkeys(vibes))

    return vibes[:4]

def resolve_title_metadata(title, content_type):
    if content_type == "movie":
        if not find_movie_by_title:
            return None

        item = find_movie_by_title(title)
        if not item:
            return None

        keywords = []
        movie_id = item.get("movie_id") or item.get("id")
        if movie_id and get_movie_keywords:
            try:
                keywords = get_movie_keywords(movie_id) or []
            except Exception:
                keywords = []

        return {
            "title": item.get("title") or title,
            "genres": item.get("genres", []),
            "keywords": keywords,
            "vibes": extract_vibes_from_item(item, content_type="movie"),
        }

    if content_type == "tv":
        if not find_tv_by_title:
            return None

        item = find_tv_by_title(title)
        if not item:
            return None

        keywords = []
        tv_id = item.get("tv_id") or item.get("id")
        if tv_id and get_tv_keywords:
            try:
                keywords = get_tv_keywords(tv_id) or []
            except Exception:
                keywords = []

        return {
            "title": item.get("title") or title,
            "genres": item.get("genres", []),
            "keywords": keywords,
            "vibes": extract_vibes_from_item(item, content_type="tv"),
        }

    return None


def build_taste_profile(searches, max_searches=10, top_genres=3, top_keywords=6, top_vibes=3):
    if not searches:
        return {
            "genres": [],
            "keywords": [],
            "vibes": [],
        }

    genre_counter = Counter()
    keyword_counter = Counter()
    vibe_counter = Counter()

    for search in searches[:max_searches]:
        if isinstance(search, dict):
            seed_titles_raw = search.get("seed_titles", "")
            content_type = (search.get("content_type") or "").strip().lower()
        else:
            seed_titles_raw = search["seed_titles"] if search["seed_titles"] else ""
            content_type = (search["content_type"] or "").strip().lower()

        titles = list(dict.fromkeys(parse_seed_titles(seed_titles_raw)))

        for title in titles:
            metadata = resolve_title_metadata(title, content_type)
            if not metadata:
                continue

            for genre in metadata.get("genres", []):
                if not genre or genre in GENRE_BLACKLIST:
                    continue
                genre_counter[genre] += 1

            for keyword in metadata.get("keywords", []):
                if isinstance(keyword, dict):
                    keyword = keyword.get("name")
                normalized = normalize_keyword(keyword)
                if normalized:
                    keyword_counter[normalized] += 1

            for vibe in metadata.get("vibes", []):
                vibe_norm = normalize_keyword(vibe)
                if vibe_norm:
                    vibe_counter[vibe_norm] += 1

    return {
        "genres": [name for name, _ in genre_counter.most_common(top_genres)],
        "keywords": [name for name, _ in keyword_counter.most_common(top_keywords)],
        "vibes": [name.capitalize() for name, _ in vibe_counter.most_common(top_vibes)],
    }