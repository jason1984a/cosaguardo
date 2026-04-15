import pandas as pd
import sqlite3
from pathlib import Path

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "coseguardo.db"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# ===== 1. Leggiamo i film =====
cur.execute("""
SELECT tmdbId, content_text
FROM tmdb_movies
WHERE content_text IS NOT NULL
""")

rows = cur.fetchall()

tmdb_ids = [row[0] for row in rows]
texts = [row[1] for row in rows]

print(f"Film caricati: {len(texts)}")

# ===== 2. TF-IDF =====
vectorizer = TfidfVectorizer(
    max_features=20000,
    stop_words="english"
)

tfidf_matrix = vectorizer.fit_transform(texts)

print("TF-IDF matrix creata")

# ===== 3. Similarità =====
similarity_matrix = cosine_similarity(tfidf_matrix)

print("Matrice similarità calcolata")

# ===== 4. Creiamo tabella =====
cur.execute("""
CREATE TABLE IF NOT EXISTS content_similarity (
    tmdbId INTEGER,
    similar_tmdbId INTEGER,
    score REAL,
    PRIMARY KEY (tmdbId, similar_tmdbId)
)
""")

conn.commit()

# ===== 5. Salviamo top 50 =====
TOP_K = 50

for i, tmdb_id in enumerate(tmdb_ids):
    scores = similarity_matrix[i]

    # prendi i più simili (escludendo sé stesso)
    similar_indices = scores.argsort()[::-1][1:TOP_K+1]

    for idx in similar_indices:
        cur.execute("""
        INSERT OR REPLACE INTO content_similarity (tmdbId, similar_tmdbId, score)
        VALUES (?, ?, ?)
        """, (
            tmdb_id,
            tmdb_ids[idx],
            float(scores[idx])
        ))

    if i % 500 == 0:
        print(f"Processati: {i}")

conn.commit()

# ===== 6. Export parquet =====
output_path = BASE_DIR / "data" / "content_similarity.parquet"

content_df = pd.read_sql_query("""
SELECT
    tmdbId,
    similar_tmdbId,
    score AS content
FROM content_similarity
""", conn)

print("Righe content_similarity:", len(content_df))
print("Salvo parquet in:", output_path)

output_path.parent.mkdir(parents=True, exist_ok=True)
content_df.to_parquet(output_path, index=False)

print("File creato?", output_path.exists())
if output_path.exists():
    print("Dimensione file:", output_path.stat().st_size, "bytes")

conn.close()

print("Content similarity salvata.")