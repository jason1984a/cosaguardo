import sqlite3
from pathlib import Path

# Percorsi
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "coseguardo.db"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Totale film
cur.execute("SELECT COUNT(*) FROM movie_links")
total = cur.fetchone()[0]

# Con tmdbId
cur.execute("SELECT COUNT(*) FROM movie_links WHERE tmdbId IS NOT NULL")
with_tmdb = cur.fetchone()[0]

# Senza tmdbId
cur.execute("SELECT COUNT(*) FROM movie_links WHERE tmdbId IS NULL")
without_tmdb = cur.fetchone()[0]

# Percentuale
pct = (with_tmdb / total) * 100 if total else 0

conn.close()

print("=== TMDB COVERAGE ===")
print(f"Totale film: {total}")
print(f"Con tmdbId: {with_tmdb}")
print(f"Senza tmdbId: {without_tmdb}")
print(f"Copertura: {pct:.2f}%")