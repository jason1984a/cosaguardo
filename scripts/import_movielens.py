import pandas as pd
import sqlite3
import os

DB_PATH = "db/coseguardo.db"
DATA_PATH = "data/ml-latest-small/"

conn = sqlite3.connect(DB_PATH)

# --- MOVIES ---
movies = pd.read_csv(DATA_PATH + "movies.csv")

# split anno dal titolo
movies["year"] = movies["title"].str.extract(r"\((\d{4})\)").astype(float)
movies["title_clean"] = movies["title"].str.replace(r"\(\d{4}\)", "", regex=True).str.strip()

titles_df = movies[["movieId", "title_clean", "year", "genres"]]
titles_df.columns = ["movielens_movie_id", "title", "year", "genres_raw"]

titles_df.to_sql("titles", conn, if_exists="append", index=False)

print("✅ Titles importati")

# --- RATINGS ---
ratings = pd.read_csv(DATA_PATH + "ratings.csv")
ratings.columns = ["user_id", "movie_id", "rating", "timestamp"]

ratings.to_sql("ratings", conn, if_exists="append", index=False)

print("✅ Ratings importati")

# --- TAGS ---
tags = pd.read_csv(DATA_PATH + "tags.csv")
tags.columns = ["user_id", "movie_id", "tag", "timestamp"]

tags.to_sql("tags", conn, if_exists="append", index=False)

print("✅ Tags importati")

# --- LINKS ---
links = pd.read_csv(DATA_PATH + "links.csv")
links.columns = ["movie_id", "imdb_id", "tmdb_id"]

links.to_sql("links", conn, if_exists="append", index=False)

print("✅ Links importati")

conn.close()

print("🎯 Import completato!")