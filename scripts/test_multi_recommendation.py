import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from core.recommendation_api import recommend_from_seed_titles

SEED_TITLES = [
    "Donnie Darko",
    "Fight Club",
    "The Matrix",
    "Se7en",
    "Memento"
]

result = recommend_from_seed_titles(SEED_TITLES, top_k=10, per_seed_limit=30)

print("🎬 Seed riconosciuti:")
for seed in result["resolved_seeds"]:
    print(f"- {seed['title']} ({seed['movie_id']})")

if result["missing_titles"]:
    print("\n⚠️ Non trovati:")
    for title in result["missing_titles"]:
        print(f"- {title}")

print("\n🔥 Raccomandazioni:\n")
for idx, rec in enumerate(result["recommendations"], start=1):
    print(f"{idx}. {rec['title']}")
    print(f"   movie_id={rec['movie_id']}")
    print(f"   appearances={rec['appearances']}")
    print(f"   avg_score={rec['avg_score']:.3f}")
    print(f"   best_score={rec['best_score']:.3f}")
    print(f"   collab={rec['components']['collab_score']:.3f}")
    print(f"   genre={rec['components']['genre_score']:.3f}")
    print(f"   tag={rec['components']['tag_score']:.3f}")
    print(f"   quality={rec['components']['quality_score_norm']:.3f}")
    print(f"   content={rec['components']['content_score']:.3f}")
    print(f"   pop_penalty={rec['components']['pop_penalty_norm']:.3f}")
    print()