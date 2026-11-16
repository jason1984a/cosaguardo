"""
Sonda diagnostica: dove finisce un titolo famoso nei risultati di TMDb?

Serve a decidere come correggere l'autocomplete dei film. Se il titolo atteso
compare a pagina 1-2, basta allargare il bacino come fa gia' la ricerca serie.
Se compare molto piu' in fondo, allargare non basta e serve un'altra strategia.

USO (dalla cartella del server, dove la chiave e' gia' nell'ambiente):

    python sonda_ricerca.py

⚠️ La chiave NON va scritta qui dentro ne' incollata da nessuna parte: viene
letta da TMDB_API_KEY come fa il resto del progetto.
"""

import os
import sys
import requests

CHIAVE = os.getenv("TMDB_API_KEY")

# (query digitata, pezzo di titolo che ci aspettiamo in cima)
PROVE = [
    ("inter",  "interstellar"),
    ("matr",   "matrix"),
    ("titan",  "titanic"),
    ("padri",  "padrino"),
]

PAGINE = 6


def cerca(query, pagina):
    r = requests.get(
        "https://api.themoviedb.org/3/search/movie",
        params={
            "api_key": CHIAVE,
            "query": query,
            "language": "it-IT",
            "include_adult": "false",
            "page": pagina,
        },
        timeout=8,
    )
    return r.json()


def main():
    if not CHIAVE:
        print("TMDB_API_KEY non trovata nell'ambiente.")
        print("Lanciala dalla cartella del server, dove la chiave e' gia' impostata.")
        sys.exit(1)

    for query, atteso in PROVE:
        print("=" * 64)
        print(f"QUERY: '{query}'   —   cerco un titolo che contenga '{atteso}'")
        print("=" * 64)

        trovato = None
        posizione_globale = 0
        totale_dichiarato = None

        for pagina in range(1, PAGINE + 1):
            dati = cerca(query, pagina)
            risultati = dati.get("results", [])
            if totale_dichiarato is None:
                totale_dichiarato = dati.get("total_results")
            if not risultati:
                break

            for item in risultati:
                posizione_globale += 1
                titolo = item.get("title") or ""
                originale = item.get("original_title") or ""
                voti = int(item.get("vote_count") or 0)

                if atteso in titolo.lower() or atteso in originale.lower():
                    if trovato is None:
                        trovato = (pagina, posizione_globale, titolo, voti)

                # Mostra i primi 5 della prima pagina, per capire cosa vince
                if pagina == 1 and posizione_globale <= 5:
                    print(f"   pag.1 #{posizione_globale}: {titolo[:44]:<44} voti={voti}")

        print()
        print(f"   Risultati totali dichiarati da TMDb: {totale_dichiarato}")
        if trovato:
            pag, pos, tit, voti = trovato
            print(f"   ✓ TROVATO '{tit}' a pagina {pag}, posizione globale {pos}, voti={voti}")
        else:
            print(f"   ✗ NON trovato nelle prime {PAGINE} pagine ({PAGINE * 20} risultati)")
        print()


if __name__ == "__main__":
    main()
