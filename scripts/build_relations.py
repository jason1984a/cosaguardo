import os
import sqlite3
import pandas as pd
from itertools import combinations
from collections import defaultdict
import math
import re

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "db", "coseguardo.db")

MIN_RATING = 4.0
MIN_SHARED_USERS = 5
TOP_N_PER_SOURCE = 20

WEIGHT_COLLAB = 0.12
WEIGHT_QUALITY = 0.10
WEIGHT_GENRE = 0.12
WEIGHT_POP = 0.05
WEIGHT_TAG = 0.10
WEIGHT_CONTENT = 0.45

def normalize_series(series):
    min_val = series.min()
    max_val = series.max()
    if max_val == min_val:
        return pd.Series([0.0] * len(series), index=series.index)
    return (series - min_val) / (max_val - min_val)

def parse_genres(genres_raw):
    if not genres_raw or genres_raw == "(no genres listed)":
        return set()
    return set(g.strip() for g in genres_raw.split("|") if g.strip())

def normalize_tag(tag):
    if not tag:
        return ""
    tag = tag.lower().strip()
    tag = tag.replace("-", " ")
    tag = re.sub(r"[^\w\s]", "", tag)
    tag = re.sub(r"\s+", " ", tag)
    return tag

def build_movie_tags(tags_df):
    movie_tags = defaultdict(set)

    movie_id_col = "movie_id" if "movie_id" in tags_df.columns else "movieId"

    for _, row in tags_df.iterrows():
        movie_id = row[movie_id_col]
        tag = normalize_tag(row["tag"])

        if tag:
            movie_tags[movie_id].add(tag)

    return dict(movie_tags)

def jaccard_similarity(set_a, set_b):
    if not set_a and not set_b:
        return 0.0
    union = set_a | set_b
    if not union:
        return 0.0
    return len(set_a & set_b) / len(union)

conn = sqlite3.connect(DB_PATH)

print("BUILD_RELATIONS DB:", DB_PATH)

cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
print("Tabelle viste da build_relations:")
for row in cur.fetchall():
    print("-", row[0])

print("📥 Carico i rating alti...")
ratings = pd.read_sql_query("""
    SELECT user_id, movie_id, rating
    FROM ratings
    WHERE rating >= ?
""", conn, params=(MIN_RATING,))

print(f"✅ Rating alti caricati: {len(ratings)}")

titles_df = pd.read_sql_query("""
    SELECT movielens_movie_id, title, genres_raw
    FROM titles
""", conn)

metrics_df = pd.read_sql_query("""
    SELECT movie_id, avg_rating, rating_count, bayesian_rating
    FROM title_metrics
""", conn)

tags_df = pd.read_sql_query("""
    SELECT movie_id, tag
    FROM tags
    WHERE tag IS NOT NULL AND TRIM(tag) != ''
""", conn)

if metrics_df.empty:
    print("❌ title_metrics è vuota. Esegui prima build_metrics.py")
    conn.close()
    raise SystemExit

movie_genres = {
    row["movielens_movie_id"]: parse_genres(row["genres_raw"])
    for _, row in titles_df.iterrows()
}

bayesian_map = {
    row["movie_id"]: row["bayesian_rating"]
    for _, row in metrics_df.iterrows()
}

movie_tags = build_movie_tags(tags_df)

from collections import Counter

tag_counts = Counter()

for tags in movie_tags.values():
    for tag in tags:
        tag_counts[tag] += 1

total_movies = len(movie_tags)

def tag_weight(tag):
    return math.log(1 + total_movies / (1 + tag_counts.get(tag, 0)))

def weighted_tag_similarity(tags_a, tags_b):
    if not tags_a or not tags_b:
        return 0.0

    intersection = tags_a & tags_b
    union = tags_a | tags_b

    if not union:
        return 0.0

    inter_weight = sum(tag_weight(t) for t in intersection)
    union_weight = sum(tag_weight(t) for t in union)

    if union_weight == 0:
        return 0.0

    return inter_weight / union_weight

user_movies = ratings.groupby("user_id")["movie_id"].apply(list)

pair_counts = defaultdict(float)
pair_shared_users = defaultdict(int)
movie_like_counts = defaultdict(int)

print("🔄 Calcolo co-occorrenze...")
for movies in user_movies:
    unique_movies = sorted(set(movies))

    for movie_id in unique_movies:
        movie_like_counts[movie_id] += 1

    user_weight = 1 / math.log(1 + len(unique_movies))

    for a, b in combinations(unique_movies, 2):
        pair_counts[(a, b)] += user_weight
        pair_shared_users[(a, b)] += 1

print(f"✅ Coppie trovate: {len(pair_counts)}")

relations = []

print("🧠 Costruisco relazioni...")
for (a, b), shared_users_weighted in pair_counts.items():
    count_a = movie_like_counts[a]
    count_b = movie_like_counts[b]

    if min(count_a, count_b) < MIN_SHARED_USERS:
        continue

    collab_score = shared_users_weighted / math.sqrt(count_a * count_b)

    genre_score = jaccard_similarity(
        movie_genres.get(a, set()),
        movie_genres.get(b, set())
    )

    tag_score = weighted_tag_similarity(
        movie_tags.get(a, set()),
        movie_tags.get(b, set())
    )

    quality_b = bayesian_map.get(b, 0.0)
    quality_a = bayesian_map.get(a, 0.0)

    pop_penalty_b = 1 / math.log(1 + count_b)
    pop_penalty_a = 1 / math.log(1 + count_a)

    relations.append((a, b, collab_score, genre_score, tag_score, quality_b, pop_penalty_b))
    relations.append((b, a, collab_score, genre_score, tag_score, quality_a, pop_penalty_a))

relations_df = pd.DataFrame(
    relations,
    columns=[
        "source_movie_id",
        "target_movie_id",
        "collab_score",
        "genre_score",
        "tag_score",
        "quality_score",
        "pop_penalty"
    ]
)

if relations_df.empty:
    print("⚠️ Nessuna relazione trovata.")
    conn.close()
    raise SystemExit

from pathlib import Path

content_path = Path(BASE_DIR) / "data" / "content_similarity.parquet"

print("📦 Carico content similarity da:", content_path)

if content_path.exists():
    content_df = pd.read_parquet(content_path)

    print("Content DF colonne:", content_df.columns.tolist())
    print("Content DF righe:", len(content_df))

    
       # 🔥 STEP 1: carica mapping tmdb -> movielens
    links_df = pd.read_sql_query("""
        SELECT movieId, tmdbId
        FROM movie_links
    """, conn)

    # 🔥 STEP 2: merge per source
    content_df = content_df.merge(
        links_df.rename(columns={
            "movieId": "source_movie_id",
            "tmdbId": "tmdbId"
        }),
        on="tmdbId",
        how="inner"
    )

    # 🔥 STEP 3: merge per target
    content_df = content_df.merge(
        links_df.rename(columns={
            "movieId": "target_movie_id",
            "tmdbId": "similar_tmdbId"
        }),
        on="similar_tmdbId",
        how="inner"
    )

    # 🔥 STEP 4: tieni solo colonne finali
    content_df = content_df[[
        "source_movie_id",
        "target_movie_id",
        "content"
    ]].rename(columns={
        "content": "content_score"
    })

    print("Content dopo mapping:", len(content_df))

    # 🔥 dtype coerenti
    content_df["source_movie_id"] = content_df["source_movie_id"].astype("int64")
    content_df["target_movie_id"] = content_df["target_movie_id"].astype("int64")

else:
    print("❌ content_similarity.parquet NON trovato")
    content_df = pd.DataFrame(columns=[
        "source_movie_id",
        "target_movie_id",
        "content_score"
    ])

relations_df = relations_df.merge(
    content_df,
    on=["source_movie_id", "target_movie_id"],
    how="left"
)

relations_df["content_score"] = relations_df["content_score"].fillna(0)

print("🔥 Content > 0:", (relations_df["content_score"] > 0).sum())

# aggiungi anche i candidati content puri
content_only_df = content_df.copy()

content_only_df["collab_score"] = 0
content_only_df["shared_users"] = 0
content_only_df["genre_score"] = 0
content_only_df["tag_score"] = 0

quality_map = relations_df.groupby("target_movie_id")["quality_score"].max().to_dict()
pop_map = relations_df.groupby("target_movie_id")["pop_penalty"].max().to_dict()

# fallback quality: media globale
global_quality = relations_df["quality_score"].mean()

content_only_df["quality_score"] = content_only_df["target_movie_id"].map(quality_map)
content_only_df["quality_score"] = content_only_df["quality_score"].fillna(global_quality)

content_only_df["pop_penalty"] = content_only_df["target_movie_id"].map(pop_map).fillna(0)


# 🔥 separa content puro
content_candidates = content_only_df.copy()

# tieni solo content decente
content_candidates = content_candidates[
    content_candidates["content_score"] >= 0.15
]

# limita a top N per source (solo content)
content_candidates = content_candidates.sort_values(
    ["source_movie_id", "content_score"],
    ascending=[True, False]
)

content_candidates = content_candidates.groupby("source_movie_id").head(10)

print("Content candidates selezionati:", len(content_candidates))

debug_content = content_candidates[
    content_candidates["source_movie_id"] == 589
].sort_values("content_score", ascending=False)

print("\nDEBUG CONTENT PURI PER 589:")
print(debug_content.head(10))

# 🔥 unisci ai collab
relations_df = pd.concat([relations_df, content_candidates], ignore_index=True)

# aggrega tutto
relations_df = relations_df.groupby(
    ["source_movie_id", "target_movie_id"],
    as_index=False
).agg({
    "collab_score": "max",
    "shared_users": "max",
    "genre_score": "max",
    "tag_score": "max",
    "content_score": "max",
    "quality_score": "max",
    "pop_penalty": "max"
})

relations_df["quality_score_norm"] = normalize_series(relations_df["quality_score"])
relations_df["pop_penalty_norm"] = normalize_series(relations_df["pop_penalty"])

# 🔥 boost quando content è presente
content_boost = 1 + (relations_df["content_score"] ** 2 * 3)

relations_df["score_raw"] = (
    WEIGHT_COLLAB * relations_df["collab_score"] +
    WEIGHT_QUALITY * relations_df["quality_score_norm"] +
    WEIGHT_GENRE * relations_df["genre_score"] +
    WEIGHT_TAG * relations_df["tag_score"] +
    WEIGHT_CONTENT * relations_df["content_score"] -
    WEIGHT_POP * relations_df["pop_penalty_norm"]
)

# applica boost UNA sola volta
relations_df["score_raw"] = relations_df["score_raw"] * content_boost

relations_df["relation_type"] = "hybrid_v2_content"

shared_users_lookup = {}
for (a, b), value in pair_counts.items():
    shared_users_lookup[(a, b)] = value
    shared_users_lookup[(b, a)] = value

relations_df["shared_users"] = relations_df.apply(
    lambda row: round(shared_users_lookup.get((row["source_movie_id"], row["target_movie_id"]), 0)),
    axis=1
)

# 🔥 QUI VA IL FILTRO
relations_df = relations_df[
    (relations_df["shared_users"] >= MIN_SHARED_USERS) |
    (relations_df["content_score"] >= 0.15)
].copy()

print("Min shared_users:", relations_df["shared_users"].min())
print("Min content_score:", relations_df["content_score"].min())

relations_df = relations_df.sort_values(
    by=["source_movie_id", "score_raw"],
    ascending=[True, False]
)

relations_df = relations_df.groupby("source_movie_id").head(TOP_N_PER_SOURCE).reset_index(drop=True)

debug_source = 589

debug_df = relations_df[relations_df["source_movie_id"] == debug_source].copy()
debug_df = debug_df.sort_values("score_raw", ascending=False)

print("\nDEBUG TOP RELAZIONI PER SOURCE 589:\n")
print(
    debug_df[
        [
            "source_movie_id",
            "target_movie_id",
            "collab_score",
            "genre_score",
            "tag_score",
            "content_score",   # 👈 AGGIUNGI QUESTO
            "quality_score_norm",
            "pop_penalty_norm",
            "score_raw",
            "shared_users"
        ]
    ].head(15).to_string(index=False)
)

final_df = relations_df[
    [
        "source_movie_id",
        "target_movie_id",
        "relation_type",
        "score_raw",
        "shared_users",
        "collab_score",
        "genre_score",
        "tag_score",
        "quality_score_norm",
        "content_score",
        "pop_penalty_norm"
    ]
]

print(f"✅ Relazioni finali: {len(final_df)}")

conn.execute("DELETE FROM title_relations")
conn.commit()

final_df.to_sql("title_relations", conn, if_exists="replace", index=False)

conn.close()

print("🎯 Relazioni ibride salvate con successo in title_relations")