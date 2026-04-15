import re
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "coseguardo.db"


def clean_text(text: str) -> str:
    if not text:
        return ""

    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# 1) Aggiungiamo la colonna content_text se non esiste già
cur.execute("PRAGMA table_info(tmdb_movies)")
columns = [row[1] for row in cur.fetchall()]

if "content_text" not in columns:
    cur.execute("ALTER TABLE tmdb_movies ADD COLUMN content_text TEXT")
    print("Colonna content_text aggiunta a tmdb_movies.")
else:
    print("Colonna content_text già presente.")

# 2) Recuperiamo overview + keywords
cur.execute("""
SELECT
    m.tmdbId,
    m.title,
    m.overview,
    GROUP_CONCAT(k.keyword_name, ' ')
FROM tmdb_movies m
LEFT JOIN tmdb_keywords k ON m.tmdbId = k.tmdbId
GROUP BY m.tmdbId
""")

rows = cur.fetchall()

updated = 0

for tmdb_id, title, overview, keywords in rows:
    title_clean = clean_text(title or "")
    overview_clean = clean_text(overview or "")
    keywords_clean = clean_text(keywords or "")

    # più peso al titolo e alle keywords
    parts = []

    if title_clean:
        parts.extend([title_clean, title_clean])

    if overview_clean:
        parts.append(overview_clean)

    if keywords_clean:
        parts.extend([keywords_clean, keywords_clean])

    content_text = " ".join(parts).strip()

    cur.execute("""
    UPDATE tmdb_movies
    SET content_text = ?
    WHERE tmdbId = ?
    """, (content_text, tmdb_id))

    updated += 1

conn.commit()
conn.close()

print(f"Content text costruito per {updated} film.")
print(f"Database usato: {DB_PATH}")