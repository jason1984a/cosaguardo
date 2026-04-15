import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "db", "coseguardo.db")

SOURCE_TITLE = "Donnie Darko"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("""
SELECT movielens_movie_id, title
FROM titles
WHERE title LIKE ?
LIMIT 1
""", (f"%{SOURCE_TITLE}%",))

row = cursor.fetchone()

if not row:
    print("❌ Film non trovato")
    conn.close()
    raise SystemExit

source_movie_id, source_title = row

print(f"🎬 Film trovato: {source_title} ({source_movie_id})")
print("\nConsigliati:\n")

cursor.execute("""
SELECT
    t.title,
    tr.score_raw,
    tr.shared_users,
    tr.collab_score,
    tr.genre_score,
    tr.tag_score,
    tr.quality_score_norm,
    tr.content_score,
    tr.pop_penalty_norm
FROM title_relations tr
JOIN titles t
    ON tr.target_movie_id = t.movielens_movie_id
WHERE tr.source_movie_id = ?
ORDER BY tr.content_score DESC, tr.score_raw DESC
LIMIT 10
""", (source_movie_id,))

results = cursor.fetchall()

if not results:
    print("⚠️ Nessuna raccomandazione trovata")
else:
    for row in results:
        (
            title,
            score,
            shared_users,
            collab,
            genre,
            tag,
            quality,
            content,
            pop
        ) = row

        print(f"""
{title}
  score={score:.3f} | shared={shared_users}
  collab={collab:.3f} | genre={genre:.3f} | tag={tag:.3f}
  quality={quality:.3f} | content={content:.3f} | pop_penalty={pop:.3f}
""")

conn.close()