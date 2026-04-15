import time
import sqlite3
import requests
from pathlib import Path

# ===== CONFIG =====
API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJlNzI3OWJlY2NhZDk5MThhN2YyZTg4MDFhYmEzODI4ZCIsIm5iZiI6MTc3MDM3MDM2OC44MzgsInN1YiI6IjY5ODViNTQwNTVjYWFiMzU4Mjk5ZDViOSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.6j_Eli9bFZ8xXag63ItOoK-IM37ziSdXra2woIfjr6M"

BASE_URL = "https://api.themoviedb.org/3"

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "coseguardo.db"

# ===== DB =====
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Prendiamo solo quelli non ancora scaricati
cur.execute("""
SELECT tmdbId
FROM movie_links
WHERE tmdbId IS NOT NULL
AND tmdbId NOT IN (SELECT tmdbId FROM tmdb_movies)
""")

tmdb_ids = [row[0] for row in cur.fetchall()]

print(f"Film da scaricare: {len(tmdb_ids)}")

# ===== SESSION API =====
session = requests.Session()
session.headers.update({
    "accept": "application/json",
    "Authorization": f"Bearer {API_KEY}"
})

# ===== LOOP =====
count = 0

for tmdb_id in tmdb_ids:
    try:
        # ===== DETAILS =====
        url = f"{BASE_URL}/movie/{tmdb_id}"
        r = session.get(url, timeout=20)
        r.raise_for_status()
        d = r.json()

        cur.execute("""
        INSERT OR REPLACE INTO tmdb_movies (
            tmdbId, title, original_title, overview, release_date,
            runtime, popularity, vote_average, vote_count, original_language
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            d.get("id"),
            d.get("title"),
            d.get("original_title"),
            d.get("overview"),
            d.get("release_date"),
            d.get("runtime"),
            d.get("popularity"),
            d.get("vote_average"),
            d.get("vote_count"),
            d.get("original_language")
        ))

        # ===== KEYWORDS =====
        url_k = f"{BASE_URL}/movie/{tmdb_id}/keywords"
        rk = session.get(url_k, timeout=20)
        rk.raise_for_status()
        kdata = rk.json()

        # pulizia precedente
        cur.execute("DELETE FROM tmdb_keywords WHERE tmdbId = ?", (tmdb_id,))

        for kw in kdata.get("keywords", []):
            cur.execute("""
            INSERT OR REPLACE INTO tmdb_keywords (tmdbId, keyword_id, keyword_name)
            VALUES (?, ?, ?)
            """, (
                tmdb_id,
                kw["id"],
                kw["name"]
            ))

        count += 1

        if count % 100 == 0:
            print(f"Scaricati: {count}")

        time.sleep(0.2)  # rate limit safe

    except Exception as e:
        print(f"Errore tmdbId={tmdb_id}: {e}")

conn.commit()
conn.close()

print("Fetch completato.")