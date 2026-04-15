import random

REASON_TEMPLATES = {
    "dark_tone": [
        "ha un tono più cupo e intenso",
        "punta su un’atmosfera più oscura e tesa"
    ],
    "character": [
        "funziona molto per come costruisce i personaggi",
        "mette al centro i conflitti tra i personaggi"
    ],
    "tension": [
        "tiene alta la tensione per tutta la durata",
        "è costruito per tenerti sempre sul filo"
    ],
    "fast_paced": [
        "ha un ritmo veloce e scorrevole",
        "va dritto al punto senza rallentare troppo"
    ],
    "smart_plot": [
        "ha una trama costruita in modo intelligente",
        "gioca molto su intrecci e sviluppo narrativo"
    ],
    "similar_audience": [
        "piace spesso a chi ha gusti simili ai tuoi",
        "è molto in linea con le tue scelte iniziali"
    ],
    "original": [
        "è una scelta un po’ più originale rispetto agli altri",
        "si discosta un po’ ma resta coerente con i tuoi gusti"
    ]
}


def build_reason_candidates(rec):
    reasons = []

    score = rec.get("score", 0)

    if score > 0.8:
        reasons.append(("similar_audience", 0.9))

    if rec.get("genres"):
        if "Crime" in rec["genres"] or "Thriller" in rec["genres"]:
            reasons.append(("tension", 0.8))

        if "Drama" in rec["genres"]:
            reasons.append(("character", 0.7))

    if rec.get("popularity", 0) < 50:
        reasons.append(("original", 0.6))

    if not reasons:
        reasons.append(("smart_plot", 0.5))

    return sorted(reasons, key=lambda x: x[1], reverse=True)


def pick_main_reason(candidates, used_types):
    for reason_type, score in candidates:
        if reason_type not in used_types:
            return reason_type

    return candidates[0][0]


def build_unique_reason(rec, all_recs):
    genres = set(rec.get("genres", []))

    # flag principali
    is_comedy = "Comedy" in genres
    is_drama = "Drama" in genres
    is_crime = "Crime" in genres
    is_thriller = "Thriller" in genres

    # 👇 logica più "umana"
    if is_comedy:
        return "Rispetto agli altri suggerimenti, ha un tono più leggero e ironico"

    if is_crime and is_thriller:
        return "Rispetto agli altri suggerimenti, è più teso e orientato al lato criminale"

    if is_drama and not is_comedy:
        return "Rispetto agli altri suggerimenti, punta di più sui personaggi e sulle relazioni"

    # fallback intelligente
    return "Rispetto agli altri suggerimenti, ha uno stile diverso dagli altri titoli proposti"

def enrich_with_explanations(recommendations, seeds=None):
    used_types = set()

    for i, rec in enumerate(recommendations):
        candidates = build_reason_candidates(rec)

        main_type = pick_main_reason(candidates, used_types)
        used_types.add(main_type)

        main_text = random.choice(REASON_TEMPLATES[main_type])
        unique_text = build_unique_reason(rec, recommendations)

        seed_text = ""

        if rec.get("best_seed_title"):
            seed_text = f"Se ti è piaciuto {rec['best_seed_title']}, "

        if i == 0:
            rec["explanation"] = (
                "È il suggerimento più forte del gruppo: combina al meglio affinità, coerenza e potenziale interesse. "
                f"{unique_text}."
            )
        else:
            rec["explanation"] = f"{seed_text}{main_text}. {unique_text}."
        rec["badge"] = build_badge(rec)

    return recommendations

def build_badge(rec):
    genres = set(rec.get("genres", []))

    if "Comedy" in genres:
        return "🎭 Più leggero"

    if "Crime" in genres and "Thriller" in genres:
        return "🔥 Più intenso"

    if "Drama" in genres:
        return "🧠 Più psicologico"

    return "✨ Consiglio"