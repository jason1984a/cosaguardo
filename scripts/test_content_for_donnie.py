import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "db", "coseguardo.db")

SOURCE_TITLE = "Donnie Darko"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# 1) trova Donnie Darko
cursor.execute("""
SELECT movielens_movie_id, title
FROM titles
WHERE title LIKE ?
LIMIT 1
""", (f"%{SOURCE_TITLE}%",))
row = cursor.fetchone()

if not row:
    print("Film non trovato")
    conn.close()
    raise SystemExit

source_movie_id, source_title = row
print(f"Film trovato: {source_title} ({source_movie_id})")

# 2) trova il tmdbId di Donnie
cursor.execute("""
SELECT movieId, tmdbId
FROM movie_links
WHERE movieId = ?
""", (source_movie_id,))
row = cursor.fetchone()

print("\nMapping movie_links:")
print(row)

if not row or row[1] is None:
    print("Donnie Darko non ha tmdbId in movie_links")
    conn.close()
    raise SystemExit

source_tmdb_id = row[1]

# 3) mostra i top content simili puri
cursor.execute("""
SELECT
    cs.similar_tmdbId,
    cs.score
FROM content_similarity cs
WHERE cs.tmdbId = ?
ORDER BY cs.score DESC
LIMIT 20
""", (source_tmdb_id,))
sim_rows = cursor.fetchall()

print("\nTop content_similarity grezzi:")
for r in sim_rows:
    print(r)

# 4) prova a mapparli a movieId + titolo
cursor.execute("""
SELECT
    ml.movieId,
    t.title,
    cs.score
FROM content_similarity cs
JOIN movie_links ml
    ON cs.similar_tmdbId = ml.tmdbId
LEFT JOIN titles t
    ON ml.movieId = t.movielens_movie_id
WHERE cs.tmdbId = ?
ORDER BY cs.score DESC
LIMIT 20
""", (source_tmdb_id,))
mapped_rows = cursor.fetchall()

print("\nTop content_similarity mappati su titoli:")
for r in mapped_rows:
    print(r)

conn.close()