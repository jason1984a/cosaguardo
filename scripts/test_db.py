import sqlite3

conn = sqlite3.connect("db/coseguardo.db")
cursor = conn.cursor()

cursor.execute("SELECT COUNT(*) FROM titles")
print("Film:", cursor.fetchone()[0])

cursor.execute("SELECT COUNT(*) FROM ratings")
print("Ratings:", cursor.fetchone()[0])

cursor.execute("""
SELECT title, COUNT(*) as n
FROM ratings r
JOIN titles t ON r.movie_id = t.movielens_movie_id
GROUP BY movie_id
ORDER BY n DESC
LIMIT 10
""")

print("\nTop film più votati:")
for row in cursor.fetchall():
    print(row)

conn.close()