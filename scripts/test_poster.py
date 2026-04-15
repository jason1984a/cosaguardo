import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from core.recommendation_api import get_movie_poster

print("API KEY:", os.getenv("TMDB_API_KEY"))

print(get_movie_poster("The Matrix"))
print(get_movie_poster("Pulp Fiction"))
print(get_movie_poster("Donnie Darko"))