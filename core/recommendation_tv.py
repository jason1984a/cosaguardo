import os
import requests
from collections import Counter
from core.explainability import enrich_with_explanations


TMDB_API_KEY = os.getenv("TMDB_API_KEY")

GENERIC_KEYWORDS = {
    "based on novel or book",
    "murder",
    "detective",
    "police",
    "investigation",
    "death",
    "friendship",
    "family",
    "love",
    "female protagonist",
    "male protagonist",
    "based on comic",
    "based on manga",
    "crime fighting",
    "good versus evil",
    "husband wife relationship",
    "father son relationship",
    "mother son relationship",
    "police officer",
    "detective inspector",
    "serial killer",
    "school",
    "high school",
    "supernatural power",
    "murder investigation",
    "police procedural",
    "homicide detective",
    "crime scene investigation",
    "crime scene investigator",
    "forensic",
    "forensic evidence",
    "forensic expert",
    "criminal investigation",
    "police detective",
    "detective duo",
    "private investigator",
    "investigator",
    "cop",
    "law enforcement",
}

STRONG_KEYWORDS = {
    "cartel",
    "drug dealer",
    "dragon",
    "sword",
    "kingdom",
    "magic",
    "fantasy world",
    "drug trafficking",
    "outlaw",
    "time travel",
    "post apocalypse",
    "apocalypse",
    "zombie",
    "survival",
    "kingdom",
    "dragon",
    "political intrigue",
    "space opera",
    "alien invasion",
    "vampire",
    "witch",
    "prison",
    "mafia",
    "gang war",
    "mind control",
    "parallel world",
}

EXCLUDED_GENRE_IDS = {
    16,   # Animation
    10751 # Family
}

TV_GENRE_NAMES = {
    10759: "Action & Adventure",
    16: "Animation",
    35: "Comedy",
    80: "Crime",
    99: "Documentary",
    18: "Drama",
    10751: "Family",
    10762: "Kids",
    9648: "Mystery",
    10763: "News",
    10764: "Reality",
    10765: "Sci-Fi & Fantasy",
    10766: "Soap",
    10767: "Talk",
    10768: "War & Politics",
    37: "Western",
}

KEYWORD_MAP = {
    "group of friends": "amicizia",
    "friends": "amicizia",
    "friendship": "amicizia",
    "searching for love": "relazioni",
    "love": "relazioni",
    "romance": "relazioni",
    "sitcom": "vita quotidiana",
    "roommates": "convivenza",
    "new york city": "vita urbana",
    "family": "famiglia",
    "drama": "dramma",
    "crime": "crimine",
    "police": "indagini",
    "detective": "indagini",
    "high school": "adolescenza",
    "teen": "adolescenza"
}

def genre_ids_to_names(genre_ids):
    if not genre_ids:
        return []

    return [TV_GENRE_NAMES[g] for g in genre_ids if g in TV_GENRE_NAMES]

def simple_similarity(a, b):
    score = 0

    # generi
    if set(a.get("genres", [])) & set(b.get("genres", [])):
        score += 1

    # keyword
    if set(a.get("keywords", [])) & set(b.get("keywords", [])):
        score += 1

    return score

def find_tv_by_title(title_query: str):
    """
    Cerca una serie TV su TMDB e restituisce un seed minimale.
    """
    title_query = title_query.strip()
    if not title_query or not TMDB_API_KEY:
        return None

    try:
        url = "https://api.themoviedb.org/3/search/tv"
        params = {
            "api_key": TMDB_API_KEY,
            "query": title_query,
            "language": "it-IT"
        }

        response = requests.get(url, params=params, timeout=5)
        data = response.json()

        results = data.get("results", [])
        if not results:
            return None

        first = results[0]

        return {
            "tv_id": first.get("id"),
            "title": first.get("name") or first.get("original_name") or title_query,
            "original_title": first.get("original_name") or first.get("name") or title_query,
            "poster_path": first.get("poster_path"),
            "overview": first.get("overview"),
            "genres": genre_ids_to_names(first.get("genre_ids", [])),
        }

    except Exception:
        return None

def get_tv_keywords(tv_id: int):
    """
    Recupera le keyword TMDB di una serie TV.
    """
    if not tv_id or not TMDB_API_KEY:
        return []

    try:
        url = f"https://api.themoviedb.org/3/tv/{tv_id}/keywords"
        params = {
            "api_key": TMDB_API_KEY
        }

        response = requests.get(url, params=params, timeout=5)
        data = response.json()

        results = []
        for item in data.get("results", []):
            name = item.get("name")
            if name:
                results.append(name.strip().lower())

        return results

    except Exception:
        return []

def normalize_keyword_name(name: str) -> str:
    return (name or "").strip().lower()


def get_keyword_weight(keyword_name: str) -> float:
    kw = normalize_keyword_name(keyword_name)

    if not kw:
        return 0.0

    if kw in GENERIC_KEYWORDS:
        return 0.05

    if kw in STRONG_KEYWORDS:
        return 2.5

    return 1.0

def has_excluded_genres(genres: list[int]) -> bool:
    if not genres:
        return False

    return any(g in EXCLUDED_GENRE_IDS for g in genres)

def tokenize_title(title: str) -> set[str]:
    if not title:
        return set()

    cleaned = (
        title.lower()
        .replace(":", " ")
        .replace("-", " ")
        .replace("/", " ")
    )

    return {
        token.strip()
        for token in cleaned.split()
        if token.strip() and len(token.strip()) >= 3
    }


def is_franchise_duplicate(candidate_title: str, seed_titles: list[str]) -> bool:
    candidate_tokens = tokenize_title(candidate_title)

    if not candidate_tokens:
        return False

    exact_franchise_tokens = {
        "csi", "ncis", "narcos", "batman", "superman",
        "marvel", "dc", "harry", "potter", "star", "trek"
    }

    for seed_title in seed_titles:
        seed_tokens = tokenize_title(seed_title)

        if not seed_tokens:
            continue

        shared = candidate_tokens & seed_tokens

        if shared & exact_franchise_tokens:
            return True

        if len(seed_tokens) >= 2 and len(shared) == len(seed_tokens):
            return True

    return False

def build_seed_keyword_profile(seed_items):
    """
    Costruisce un profilo keyword pesato partendo dai seed.
    Più una keyword compare in seed diversi, più pesa.
    Inoltre applica un peso diverso tra keyword generiche e specifiche.
    """
    keyword_counter = Counter()

    for seed in seed_items:
        tv_id = seed.get("tv_id")
        if not tv_id:
            continue

        keywords = get_tv_keywords(tv_id)

        for kw in set(keywords):
            normalized_kw = normalize_keyword_name(kw)
            if not normalized_kw:
                continue

            keyword_counter[normalized_kw] += 1

    weighted_profile = {}
    for kw, freq in keyword_counter.items():
        base_weight = get_keyword_weight(kw)

        # boost frequenza tra seed (super importante)
        freq_boost = freq ** 1.5

        weighted_profile[kw] = base_weight * freq_boost

    return weighted_profile


def keyword_overlap_score(candidate_keywords, seed_keyword_profile):
    """
    Score keyword pesato tra candidato e profilo seed.
    Restituisce:
    - score normalizzato
    - lista keyword matchate ordinate per importanza
    """
    if not candidate_keywords or not seed_keyword_profile:
        return 0.0, []

    candidate_set = {
        normalize_keyword_name(kw)
        for kw in candidate_keywords
        if normalize_keyword_name(kw)
    }

    if not candidate_set:
        return 0.0, []

    shared_keywords = [
        kw for kw in candidate_set
        if kw in seed_keyword_profile
    ]

    if not shared_keywords:
        return 0.0, []

    matched_weight = sum(seed_keyword_profile.get(kw, 0.0) for kw in shared_keywords)
    total_profile_weight = sum(seed_keyword_profile.values())

    if total_profile_weight <= 0:
        return 0.0, []

    score = matched_weight / total_profile_weight

    top_shared = sorted(
        shared_keywords,
        key=lambda kw: seed_keyword_profile.get(kw, 0.0),
        reverse=True
    )[:5]

    return score, top_shared

THEME_MAP = {
    "medieval": "ambientazione medievale",
    "king": "lotte di potere",
    "queen": "lotte di potere",
    "kingdom": "regni e potere",
    "throne": "lotte di potere",
    "war": "guerre e conflitti",
    "battle": "guerre e conflitti",
    "politics": "intrighi politici",
    "political intrigue": "intrighi politici",
    "betrayal": "tradimenti e tensioni",
    "revenge": "vendetta",
    "viking": "atmosfere nordiche e guerriere",
    "sword": "epica e combattimenti",
    "dragon": "fantasy epico",
    "magic": "elementi fantasy",
    "sorcery": "elementi fantasy",
    "monster": "creature e minacce",
    "mafia": "criminalità organizzata",
    "cartel": "criminalità organizzata",
    "drug lord": "mondo criminale",
    "crime": "tensione crime",
    "murder": "indagini e delitti",
    "detective": "indagini",
    "investigation": "indagini",
    "serial killer": "tensione oscura",
    "survival": "sopravvivenza",
    "post apocalypse": "scenario post-apocalittico",
    "apocalypse": "scenario apocalittico",
    "zombie": "minaccia survival",
    "prison": "ambienti duri e chiusi",
    "family": "dinamiche familiari",
    "friendship": "relazioni tra personaggi",
    "romance": "componente romantica",
    "coming of age": "crescita personale",
    "supernatural": "elementi soprannaturali",
    "time travel": "viaggi nel tempo",
}


def keyword_set_from_list(keywords):
    if not keywords:
        return set()

    return {
        normalize_keyword_name(k)
        for k in keywords
        if normalize_keyword_name(k)
    }


def extract_human_themes(overlap_keywords, max_themes=3):
    themes = []

    for kw in overlap_keywords:
        label = THEME_MAP.get(normalize_keyword_name(kw))
        if label and label not in themes:
            themes.append(label)

    return themes[:max_themes]


def get_top_matching_seeds(item, resolved_seeds, top_n=2):
    item_keywords = keyword_set_from_list(item.get("keywords", []))
    item_genres = set(item.get("genres", []))

    seed_matches = []

    for seed in resolved_seeds:
        seed_keywords = keyword_set_from_list(seed.get("keywords", []))
        seed_genres = set(seed.get("genres", []))

        kw_overlap = item_keywords & seed_keywords
        genre_overlap = item_genres & seed_genres

        score = len(kw_overlap) * 2 + len(genre_overlap)

        if score > 0:
            seed_matches.append({
                "title": seed.get("title", ""),
                "score": score,
                "kw_overlap": kw_overlap,
                "genre_overlap": genre_overlap,
            })

    seed_matches.sort(key=lambda x: x["score"], reverse=True)
    return seed_matches[:top_n]


def format_list_natural(items):
    items = [x for x in items if x]

    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} e {items[1]}"

    return ", ".join(items[:-1]) + f" e {items[-1]}"


def generate_explanation(item, resolved_seeds):
    top_seed_matches = get_top_matching_seeds(item, resolved_seeds, top_n=2)

    if not top_seed_matches:
        genres = genre_ids_to_names(item.get("genres", []))
        if genres:
            genre_text = format_list_natural(genres[:2])
            return f"Consigliata per affinità di genere, soprattutto {genre_text}."
        return "Consigliata per forte affinità complessiva con le serie che hai inserito."

    seed_titles = [match["title"] for match in top_seed_matches if match.get("title")]

    combined_kw = set()
    combined_genres = set()

    for match in top_seed_matches:
        combined_kw.update(match.get("kw_overlap", set()))
        combined_genres.update(match.get("genre_overlap", set()))

    themes = extract_human_themes(combined_kw, max_themes=3)

    seed_part = format_list_natural(seed_titles)
    theme_part = format_list_natural(themes)

    if theme_part:
        return f"Te la consigliamo perché richiama {seed_part} per {theme_part}."

    if combined_genres:
        genre_names = genre_ids_to_names(list(combined_genres))
        if genre_names:
            genre_part = format_list_natural(genre_names[:2])
            return f"Te la consigliamo perché è vicina a {seed_part}, soprattutto per i generi {genre_part}."

    return f"Te la consigliamo perché ha diversi elementi in comune con {seed_part}."

def translate_keywords(keywords):
    seen = set()
    translated = []

    for kw in keywords:
        k = kw.lower().strip()
        value = KEYWORD_MAP.get(k, k)

        if value not in seen:
            translated.append(value)
            seen.add(value)

    return translated

def get_similar_tv(tv_id: int, limit: int = 10):
    """
    Recupera serie simili da TMDB
    """
    if not tv_id or not TMDB_API_KEY:
        return []

    try:
        url = f"https://api.themoviedb.org/3/tv/{tv_id}/similar"
        params = {
            "api_key": TMDB_API_KEY,
            "language": "it-IT"
        }

        response = requests.get(url, params=params, timeout=5)
        data = response.json()

        results = []
        for item in data.get("results", [])[:limit]:
            title = item.get("name") or item.get("original_name")
            if not title:
                continue

            results.append({
                "tv_id": item.get("id"),
                "title": title,
                "poster_path": item.get("poster_path"),
                "overview": item.get("overview"),
                "vote_average": item.get("vote_average", 0),
                "popularity": item.get("popularity", 0),
                "genres": item.get("genre_ids", []),
                "original_language": item.get("original_language"),
                "source_type": "similar",
            })

        return results

    except Exception:
        return []


def get_recommended_tv(tv_id: int, limit: int = 10):
    """
    Recupera serie consigliate da TMDB
    """
    if not tv_id or not TMDB_API_KEY:
        return []

    try:
        url = f"https://api.themoviedb.org/3/tv/{tv_id}/recommendations"
        params = {
            "api_key": TMDB_API_KEY,
            "language": "it-IT"
        }

        response = requests.get(url, params=params, timeout=5)
        data = response.json()

        results = []
        for item in data.get("results", [])[:limit]:
            title = item.get("name") or item.get("original_name")
            if not title:
                continue

            results.append({
                "tv_id": item.get("id"),
                "title": title,
                "poster_path": item.get("poster_path"),
                "overview": item.get("overview"),
                "vote_average": item.get("vote_average", 0),
                "popularity": item.get("popularity", 0),
                "genres": item.get("genre_ids", []),
                "original_language": item.get("original_language"),
                "source_type": "recommended",
            })

        return results

    except Exception:
        return []

def build_tv_ui_signals(rec):
    kw_score = rec.get("keyword_score", 0)
    coverage = rec.get("seed_coverage", 0)

    signals = []

    # 🎯 MATCH
    signals.append({
        "icon": "🎯",
        "label": "Match",
        "value": None
    })

    # 🎬 GENERE -> qualitativo come nei film
    if coverage >= 2:
        genre_value = "coerente"
    elif coverage == 1:
        genre_value = "buona"
    else:
        genre_value = "discreta"

    signals.append({
        "icon": "🎬",
        "label": "Genere",
        "value": genre_value
    })

    # ✨ VIBE -> qualitativo come nei film
    if kw_score >= 0.20:
        vibe_value = "forte"
    elif kw_score >= 0.10:
        vibe_value = "buona"
    else:
        vibe_value = "discreta"

    signals.append({
        "icon": "✨",
        "label": "Vibe",
        "value": vibe_value
    })

    return signals

def build_tv_explanation(rec, index=0):
    matched_seed_titles = list(rec.get("matched_seed_titles", []))
    best_seed_title = rec.get("best_seed_title")
    kw_score = rec.get("keyword_score", 0)
    coverage = rec.get("seed_coverage", 0)

    if index == 0:
        return (
            "È il suggerimento più forte del gruppo: combina al meglio affinità, coerenza "
            "e potenziale interesse rispetto agli altri titoli proposti."
        )

    if coverage >= 2 and len(matched_seed_titles) >= 2:
        return (
            f"Unisce elementi di {matched_seed_titles[0]} e {matched_seed_titles[1]}, "
            "restando molto coerente con le serie che hai inserito."
        )

    if best_seed_title:
        if kw_score >= 0.20:
            return (
                f"Se ti è piaciuto {best_seed_title}, questa serie richiama bene temi, "
                "atmosfera e tipo di racconto delle tue scelte iniziali."
            )

        return (
            f"Se ti è piaciuto {best_seed_title}, questa è una delle proposte più vicine "
            "ai tuoi gusti tra quelle emerse."
        )

    return rec.get("explanation") or "Consigliata per affinità con le serie che hai inserito."

def recommend_tv_from_seed_titles(seed_titles: list[str], top_k: int = 10):
    resolved_seeds = []
    missing_titles = []
    all_candidates = {}
    seed_genres = []

    seed_ids = set()
    seed_title_keys = set()
    seed_titles_clean = []

    # =========================
    # FASE 1 — RISOLUZIONE SEED
    # =========================
    for title in seed_titles:
        tv_show = find_tv_by_title(title)

        if not tv_show:
            missing_titles.append(title)
            continue

        resolved_seeds.append(tv_show)

        seed_titles_clean.append(tv_show.get("title", ""))
        seed_titles_clean.append(tv_show.get("original_title", ""))

        if tv_show.get("genres"):
            seed_genres.extend(tv_show["genres"])

        tv_show["keywords"] = get_tv_keywords(tv_show["tv_id"])

        if tv_show.get("tv_id"):
            seed_ids.add(tv_show["tv_id"])

        seed_title_keys.add((tv_show.get("title") or "").lower().strip())
        seed_title_keys.add((tv_show.get("original_title") or "").lower().strip())

    # =========================
    # FASE 2 — CANDIDATI
    # =========================
    for tv_show in resolved_seeds:
        similar_list = get_similar_tv(tv_show["tv_id"], limit=12)
        recommended_list = get_recommended_tv(tv_show["tv_id"], limit=12)

        combined = similar_list + recommended_list

        for sim in combined:
            candidate_id = sim.get("tv_id")
            candidate_title = sim.get("title", "").strip()
            candidate_key = candidate_title.lower()

            if not candidate_title:
                continue

            if candidate_id in seed_ids:
                continue

            normalized_candidate_title = candidate_title.lower().strip()
            if normalized_candidate_title in {
                (t or "").lower().strip()
                for t in seed_titles_clean
            }:
                continue

            candidate_tokens = tokenize_title(candidate_title)
            is_same_as_seed = False

            for seed_title in seed_titles_clean:
                seed_tokens = tokenize_title(seed_title)

                if candidate_tokens == seed_tokens:
                    is_same_as_seed = True
                    break

            if is_same_as_seed:
                continue

            if candidate_key in seed_title_keys:
                continue

            if is_franchise_duplicate(candidate_title, seed_titles_clean):
                continue

            if has_excluded_genres(sim.get("genres", [])):
                continue

            original_lang = sim.get("original_language")
            if original_lang not in {"en", "it"}:
                continue

            candidate_keywords = get_tv_keywords(candidate_id)

            if candidate_key not in all_candidates:
                all_candidates[candidate_key] = {
                    "tv_id": candidate_id,
                    "title": candidate_title,
                    "poster_path": sim.get("poster_path"),
                    "overview": sim.get("overview"),
                    "score": sim.get("vote_average", 0),
                    "popularity": sim.get("popularity", 0),
                    "appearances": 1,
                    "similar_hits": 1 if sim.get("source_type") == "similar" else 0,
                    "recommended_hits": 1 if sim.get("source_type") == "recommended" else 0,
                    "genres": sim.get("genres", []),
                    "keywords": candidate_keywords,
                    "matched_seed_ids": {tv_show["tv_id"]},
                    "matched_seed_titles": {tv_show.get("title", "")},
                }
            else:
                all_candidates[candidate_key]["score"] += sim.get("vote_average", 0)
                all_candidates[candidate_key]["popularity"] += sim.get("popularity", 0)
                all_candidates[candidate_key]["appearances"] += 1
                all_candidates[candidate_key]["matched_seed_ids"].add(tv_show["tv_id"])
                all_candidates[candidate_key]["matched_seed_titles"].add(tv_show.get("title", ""))

                if sim.get("source_type") == "similar":
                    all_candidates[candidate_key]["similar_hits"] += 1
                if sim.get("source_type") == "recommended":
                    all_candidates[candidate_key]["recommended_hits"] += 1

    # =========================
    # SCORING
    # =========================
    seed_keyword_profile = build_seed_keyword_profile(resolved_seeds)

    genre_counts = Counter(seed_genres)
    top_genres = {g for g, count in genre_counts.items() if count >= 2}

    scored_candidates = []

    for item in all_candidates.values():
        appearances = item["appearances"]
        avg_vote = item["score"] / appearances if appearances else 0
        avg_popularity = item["popularity"] / appearances if appearances else 0
        seed_coverage = len(item.get("matched_seed_ids", set()))

        kw_score, matched_keywords = keyword_overlap_score(
            item.get("keywords", []),
            seed_keyword_profile
        )

        if kw_score < 0.01:
            continue
        if seed_coverage == 1 and kw_score < 0.045:
            continue

        multi_seed_bonus = 0
        if seed_coverage >= 3:
            multi_seed_bonus = 8
        elif seed_coverage == 2:
            multi_seed_bonus = 3
        elif seed_coverage == 1:
            multi_seed_bonus = -3

        genre_bonus = 0
        if top_genres and any(g in top_genres for g in item.get("genres", [])):
            genre_bonus = 3

        final_score = (
            appearances * 1.0
            + multi_seed_bonus
            + genre_bonus
            + item["recommended_hits"] * 1.5
            + item["similar_hits"] * 1.0
            + avg_vote * 0.6
            + min(avg_popularity, 100) * 0.04
            + kw_score * 12.0
        )

        # scaling keyword
        if kw_score < 0.04:
            final_score *= 0.65
        elif kw_score < 0.08:
            final_score *= 0.80
        elif kw_score > 0.40:
            final_score *= 1.30
        elif kw_score > 0.28:
            final_score *= 1.20

        # penalità coverage
        if seed_coverage == 1 and kw_score < 0.10:
            final_score *= 0.70
        if seed_coverage == 1 and kw_score < 0.06:
            final_score *= 0.55
        if seed_coverage == 2 and kw_score < 0.05:
            final_score *= 0.85

        # penalità procedural
        procedural_keywords = {
            "police procedural",
            "crime investigation",
            "detective",
            "murder investigation"
        }

        penalty = sum(
            1 for kw in item.get("keywords", [])
            if kw.lower() in procedural_keywords
        )

        final_score -= penalty * 2.0

        # boost crime
        strong_keywords = {
            "drug cartel",
            "drug lord",
            "cartel",
            "undercover",
            "mafia",
            "gang",
            "organized crime"
        }

        bonus = sum(
            1 for kw in item.get("keywords", [])
            if kw.lower() in strong_keywords
        )

        final_score += bonus * 2

        # penalità spin-off
        spin_off_keywords = {"origin", "origins", "prequel", "sequel"}
        spin_penalty = sum(
            1 for word in spin_off_keywords
            if word in item["title"].lower()
        )

        final_score -= spin_penalty * 2.5

        # qualità
        if avg_popularity < 20:
            final_score *= 0.85
        if avg_vote < 6.5:
            final_score *= 0.85

        item["avg_score"] = final_score
        item["tmdb_vote_avg"] = avg_vote
        item["tmdb_popularity_avg"] = avg_popularity
        item["keyword_score"] = kw_score
        item["seed_coverage"] = seed_coverage
        item["matched_keywords"] = translate_keywords(matched_keywords)
        item["explanation"] = generate_explanation(item, resolved_seeds)

        if final_score < 6:
            continue

        scored_candidates.append(item)

    # ordinamento iniziale
    recommendations = sorted(
        scored_candidates,
        key=lambda x: x["avg_score"],
        reverse=True
    )

    # diversity layer
    diversified = []

    for candidate in recommendations:
        penalty = 0

        for chosen in diversified:
            sim = simple_similarity(candidate, chosen)

            if sim >= 2:
                penalty += 0.15
            elif sim == 1:
                penalty += 0.07

        candidate["adjusted_score"] = candidate["avg_score"] * (1 - penalty)
        diversified.append(candidate)

    # riordino finale
    recommendations = sorted(
        diversified,
        key=lambda x: x["adjusted_score"],
        reverse=True
    )[:top_k]

    for rec in recommendations:
        rec["best_seed_title"] = None

        max_overlap = 0

        for seed in resolved_seeds:
            seed_keywords = set(seed.get("keywords", []))
            rec_keywords = set(rec.get("keywords", []))

            overlap = len(seed_keywords & rec_keywords)

            if overlap > max_overlap:
                max_overlap = overlap
                rec["best_seed_title"] = seed.get("title")

    recommendations = enrich_with_explanations(recommendations, resolved_seeds)

    for i, rec in enumerate(recommendations):
        if i == 0:
            rec["badge"] = {"text": "⭐ Miglior match", "type": "top"}
        else:
            rec["badge"] = build_badge(rec)

        rec["match_score"] = round(min(9.8, 5.5 + rec["avg_score"] * 0.25), 1)
        rec["ui_signals"] = build_tv_ui_signals(rec)
        rec["genre_score_ui"] = rec["ui_signals"][1]["value"]
        rec["vibe_score_ui"] = rec["ui_signals"][2]["value"]
        rec["explanation"] = build_tv_explanation(rec, i)
   
    return {
        "resolved_seeds": resolved_seeds,
        "missing_titles": missing_titles,
        "recommendations": recommendations
    }

def build_badge(rec):
    coverage = rec.get("seed_coverage", 0)
    kw_score = rec.get("keyword_score", 0)
    seed_title = rec.get("best_seed_title")

    # 🔥 super match (multi-seed forte)
    if coverage >= 3:
        return {
            "text": "🔥 Super match",
            "type": "top"
        }

    # 🎯 match forte (2 seed)
    if coverage == 2:
        return {
            "text": "🎯 Match forte",
            "type": "highlight"
        }

    # 🧠 temi molto forti
    if kw_score >= 0.25:
        return {
            "text": "🧠 Temi molto simili",
            "type": "mind"
        }

    # fallback su seed
    if seed_title:
        return {
            "text": f"🎯 Simile a {seed_title}",
            "type": "highlight"
        }

    # fallback finale
    return {
        "text": "✨ Consiglio",
        "type": "default"
    }

def search_tv_series(query: str, limit: int = 8):
    """
    Autocomplete Serie TV via TMDB.
    """
    query = query.strip()
    if not query or not TMDB_API_KEY:
        return []

    try:
        url = "https://api.themoviedb.org/3/search/tv"
        params = {
            "api_key": TMDB_API_KEY,
            "query": query,
            "language": "it-IT"
        }

        response = requests.get(url, params=params, timeout=5)
        data = response.json()

        results = []
        seen_titles = set()

        for item in data.get("results", [])[:limit]:
            name = item.get("name")
            original_name = item.get("original_name")

            if not name and not original_name:
                continue

            display_title = name or original_name
            base_title = original_name or name

            if not base_title:
                continue

            key = base_title.lower().strip()
            if key in seen_titles:
                continue

            results.append({
                "title": base_title,
                "display_title": display_title
            })
            seen_titles.add(key)

        return results

    except Exception:
        return []
