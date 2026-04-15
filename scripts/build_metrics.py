import os
import sqlite3
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "db", "coseguardo.db")

MIN_VOTES = 20

conn = sqlite3.connect(DB_PATH)

ratings = pd.read_sql_query("""
    SELECT movie_id, rating
    FROM ratings
""", conn)

if ratings.empty:
    print("❌ Nessun rating trovato")
    conn.close()
    raise SystemExit

global_mean = ratings["rating"].mean()

movie_stats = (
    ratings.groupby("movie_id")
    .agg(
        avg_rating=("rating", "mean"),
        rating_count=("rating", "count")
    )
    .reset_index()
)

movie_stats["bayesian_rating"] = (
    (movie_stats["rating_count"] / (movie_stats["rating_count"] + MIN_VOTES)) * movie_stats["avg_rating"]
    + (MIN_VOTES / (movie_stats["rating_count"] + MIN_VOTES)) * global_mean
)

conn.execute("DELETE FROM title_metrics")
conn.commit()

movie_stats.to_sql("title_metrics", conn, if_exists="append", index=False)

conn.close()

print("✅ Metriche calcolate e salvate in title_metrics")
print(f"Media globale rating: {global_mean:.3f}")
print(f"Film con metriche: {len(movie_stats)}")