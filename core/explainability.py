import random

# ---------------------------------------------------------------------------
# KEYWORD → etichetta leggibile in italiano
# ---------------------------------------------------------------------------

KEYWORD_LABELS = {
    "space": "spazio",
    "space travel": "viaggi spaziali",
    "spacecraft": "astronavi",
    "astronaut": "astronauti",
    "time travel": "viaggi nel tempo",
    "artificial intelligence": "intelligenza artificiale",
    "robot": "robot",
    "dystopia": "distopia",
    "post-apocalyptic": "post-apocalisse",
    "post apocalypse": "post-apocalisse",
    "survival": "sopravvivenza",
    "zombie": "zombie",
    "vampire": "vampiri",
    "witch": "stregoneria",
    "magic": "magia",
    "fantasy world": "mondi fantasy",
    "dragon": "draghi",
    "kingdom": "regni medievali",
    "sword": "combattimenti",
    "political intrigue": "intrighi politici",
    "conspiracy": "complotti",
    "heist": "colpi grossi",
    "mafia": "mafia",
    "cartel": "cartelli",
    "drug trafficking": "narcotraffico",
    "undercover": "infiltrati",
    "serial killer": "serial killer",
    "psychological thriller": "thriller psicologico",
    "mind control": "controllo mentale",
    "parallel world": "mondi paralleli",
    "alien": "alieni",
    "alien invasion": "invasione aliena",
    "superhero": "supereroi",
    "prison": "prigione",
    "war": "guerra",
    "based on true story": "storia vera",
    "biography": "biografia",
    "based on novel": "romanzo",
    "coming of age": "crescita personale",
    "redemption": "redenzione",
    "revenge": "vendetta",
    "betrayal": "tradimento",
    "corruption": "corruzione",
}

VIBE_BY_GENRE = {
    "Thriller": "atmosfera tesa",
    "Crime": "lato oscuro e criminale",
    "Drama": "profondità nei personaggi",
    "Comedy": "tono leggero e ironico",
    "Sci-Fi": "visione fantascientifica",
    "Science Fiction": "visione fantascientifica",
    "Sci-Fi & Fantasy": "mix di sci-fi e fantasy",
    "Horror": "tensione e paura",
    "Action": "ritmo d'azione",
    "Action & Adventure": "avventura e azione",
    "Mystery": "mistero e suspense",
    "Romance": "storie d'amore",
    "Fantasy": "magia e mondi fantastici",
    "War": "contesto bellico",
    "History": "sfondo storico",
    "Animation": "stile animato",
    "Documentary": "sguardo documentaristico",
    "Western": "atmosfera western",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _prettify_title(title: str) -> str:
    """Rimuove articoli in coda tipo ', The' → 'The ...'"""
    if not title:
        return title
    suffixes = [", The", ", A", ", An", ", La", ", Le", ", Les", ", Il", ", Lo", ", L'"]
    for suffix in suffixes:
        if title.endswith(suffix):
            base = title[: -len(suffix)].strip()
            article = suffix[2:].strip()
            return f"{article} {base}"
    return title


def _keywords_to_labels(keywords: list, max_kw: int = 2) -> list:
    """Converte keyword tecniche in etichette leggibili, max max_kw."""
    labels = []
    for kw in keywords:
        kw_lower = (kw or "").strip().lower()
        label = KEYWORD_LABELS.get(kw_lower)
        if label and label not in labels:
            labels.append(label)
        if len(labels) >= max_kw:
            break
    return labels


def _vibe_from_genres(genres: list) -> str | None:
    """Restituisce la prima vibe riconosciuta dai generi."""
    for genre in (genres or []):
        vibe = VIBE_BY_GENRE.get(genre)
        if vibe:
            return vibe
    return None


def _seed_titles_text(rec) -> str:
    """
    Costruisce la parte 'Simile a X' o 'Simile a X e Y'
    usando matched_seed_titles se disponibile, altrimenti best_seed_title.
    """
    matched = rec.get("matched_seed_titles")

    if matched:
        if isinstance(matched, set):
            matched = list(matched)
        matched = [_prettify_title(t) for t in matched if t][:2]

    if not matched:
        best = rec.get("best_seed_title")
        if best:
            matched = [_prettify_title(best)]

    if not matched:
        return ""

    if len(matched) == 1:
        return f"Simile a {matched[0]}"
    else:
        return f"Simile a {matched[0]} e {matched[1]}"


# ---------------------------------------------------------------------------
# Builder principale
# ---------------------------------------------------------------------------

def _build_explanation(rec, index: int, all_recs: list) -> str:
    """
    Genera una spiegazione personalizzata in linguaggio naturale.
    Non rivela i seed — usa solo keyword e vibe per sembrare intelligente.
    """

    kw_labels = _keywords_to_labels(rec.get("matched_keywords", []))
    vibe = _vibe_from_genres(rec.get("genres", []))

    # --- PRIMO posto ---
    if index == 0:
        if kw_labels:
            kw_str = " e ".join(kw_labels)
            return f"Il consiglio più forte — temi di {kw_str}."
        if vibe:
            return f"Il consiglio più forte — {vibe}."
        return "Il suggerimento più in linea con i tuoi gusti."

    # --- ALTRI posti ---
    if kw_labels and vibe:
        kw_str = " e ".join(kw_labels)
        return f"Temi di {kw_str} — {vibe}."

    if kw_labels:
        kw_str = " e ".join(kw_labels)
        return f"Temi di {kw_str}."

    if vibe:
        return f"Coerente con i tuoi gusti — {vibe}."

    return "Coerente con i titoli che ti piacciono di più."


# ---------------------------------------------------------------------------
# Badge builder
# ---------------------------------------------------------------------------

def build_badge(rec):
    genres = set(rec.get("genres", []))

    if "Comedy" in genres:
        return "🎭 Più leggero"
    if "Crime" in genres and "Thriller" in genres:
        return "🔥 Più intenso"
    if "Drama" in genres:
        return "🧠 Più psicologico"
    if "Sci-Fi" in genres or "Science Fiction" in genres or "Sci-Fi & Fantasy" in genres:
        return "🚀 Sci-fi"
    if "Horror" in genres:
        return "😱 Più oscuro"
    if "Action" in genres or "Action & Adventure" in genres:
        return "💥 Più action"

    return "✨ Consiglio"


# ---------------------------------------------------------------------------
# Entry point pubblico
# ---------------------------------------------------------------------------

def enrich_with_explanations(recommendations, seeds=None):
    for i, rec in enumerate(recommendations):
        rec["explanation"] = _build_explanation(rec, i, recommendations)
        rec["badge"] = build_badge(rec)

    return recommendations
