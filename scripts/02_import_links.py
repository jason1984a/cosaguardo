import csv
import sqlite3
from pathlib import Path

# Percorsi
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "coseguardo.db"
LINKS_CSV = BASE_DIR / "data" / "ml-latest-small" / "links.csv"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

rows_to_insert = []

with open(LINKS_CSV, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    for row in reader:
        movie_id = int(row["movieId"])

        imdb_id = row["imdbId"].strip() if row["imdbId"] else None
        tmdb_id = int(row["tmdbId"]) if row["tmdbId"] else None

        # MovieLens spesso salva imdbId senza prefisso "tt"
        if imdb_id and not imdb_id.startswith("tt"):
            imdb_id = f"tt{imdb_id}"

        rows_to_insert.append((
            movie_id,
            imdb_id,
            tmdb_id,
            "movielens_links"
        ))

cur.executemany("""
INSERT OR REPLACE INTO movie_links (movieId, imdbId, tmdbId, mapping_source)
VALUES (?, ?, ?, ?)
""", rows_to_insert)

conn.commit()
conn.close()

print(f"Import completato: {len(rows_to_insert)} righe inserite in movie_links.")
print(f"CSV usato: {LINKS_CSV}")
print(f"Database usato: {DB_PATH}")