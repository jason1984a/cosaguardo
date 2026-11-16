#!/usr/bin/env python3
"""Verifica se SoccersAPI conosce i broadcaster ITALIANI.

E' la domanda da cui dipende tutta la sezione sport: sapere che esiste un
campo "TV station" non dice se per Napoli-Roma esce DAZN oppure solo
emittenti inglesi e albanesi. Questo script lo chiede all'API e basta.

USO
    1) registrati gratis su soccersapi.com e abilita Serie A (+ Champions)
       fra le 3 leghe del piano Free
    2) metti la chiave in una variabile d'ambiente:
           PowerShell:  $env:SOCCERSAPI_KEY="la-tua-chiave"
           bash:        export SOCCERSAPI_KEY=la-tua-chiave
    3) python prova_soccersapi.py

    Date diverse:  python prova_soccersapi.py 2026-11-21 2026-11-22

⚠️ La chiave si legge SOLO dall'ambiente e non viene mai stampata: nei log
   compare come ***. Non incollarla in chat.

⚠️ Il piano Free ha un tetto giornaliero di richieste e non e' documentato.
   Lo script fa il minimo indispensabile (circa 6-10 chiamate), stampa
   `requests_left` a ogni passo e salva le risposte grezze in
   risposte_soccersapi.json, cosi' la seconda analisi non costa nulla.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta

# Windows: la console e' cp1252 e si strozza sui caratteri non ASCII che
# tornano dall'API (nomi di canali, squadre). Senza questo lo script muore
# su un print, non su una chiamata — un errore che sembrerebbe dell'API.
for flusso in (sys.stdout, sys.stderr):
    try:
        flusso.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

BASE = "https://api.soccersapi.com/v2.2"
CHIAVE = os.environ.get("SOCCERSAPI_KEY", "").strip()

if not CHIAVE:
    sys.exit(
        "ERRORE: manca SOCCERSAPI_KEY nell'ambiente.\n"
        '  PowerShell:  $env:SOCCERSAPI_KEY="la-tua-chiave"\n'
        "  bash:        export SOCCERSAPI_KEY=la-tua-chiave"
    )

# Le piattaforme che ci interessano davvero, coi nomi che potrebbero usare
# loro. Il confronto e' su minuscolo e per sottostringa: "Sky Sport Uno",
# "Sky Sport Calcio" e "Sky Sport 24" devono tutti contare come Sky.
#
# ⚠️ Gli aghi scritti fra spazi (" now ", " rai ") vanno confrontati a parola
# intera: il nome viene imbottito di spazi prima del confronto. Senza questo
# un canale che si chiama esattamente "NOW" non verrebbe riconosciuto — e
# "rai" dentro un'altra parola darebbe un falso positivo.
ATTESE = {
    "DAZN":        ("dazn",),
    "Sky Sport":   ("sky sport", "sky calcio", "sky italia"),
    "NOW":         ("now tv", " now "),
    "Prime Video": ("prime video", "amazon"),
    "Mediaset":    ("mediaset", "infinity", "canale 5", "italia 1"),
    "Rai":         (" rai ",),
    "TV8":         ("tv8",),
    "Eurosport":   ("eurosport", "discovery+"),
}

richieste_fatte = 0
ultimo_meta = {}
grezzo = {}


def chiama(percorso: str, **parametri):
    """Una chiamata all'API. Non solleva: restituisce (dati, errore)."""
    global richieste_fatte, ultimo_meta

    parametri["api_key"] = CHIAVE
    url = f"{BASE}/{percorso}/?" + urllib.parse.urlencode(parametri)

    # ⚠️ Per i log: la chiave fuori, sempre. L'URL vero non si stampa mai.
    visibile = url.replace(urllib.parse.quote(CHIAVE, safe=""), "***").replace(CHIAVE, "***")

    richieste_fatte += 1
    try:
        richiesta = urllib.request.Request(url, headers={"User-Agent": "CosaGuardo/prova"})
        with urllib.request.urlopen(richiesta, timeout=30) as risposta:
            corpo = json.loads(risposta.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        testo = e.read().decode("utf-8", "replace")[:400]
        return None, f"HTTP {e.code} su {visibile}\n       {testo}"
    except Exception as e:
        return None, f"{type(e).__name__} su {visibile}: {e}"

    meta = corpo.get("meta") or {}
    if meta:
        ultimo_meta = meta
    # ⚠️ L'API puo' rispondere 200 con l'errore dentro meta.msg (operazione
    # non compresa nel piano, lega non abilitata). Un 200 non vuol dire che
    # e' andata bene.
    msg = str(meta.get("msg") or "").strip()
    dati = corpo.get("data")
    if dati is None:
        return None, f"nessun campo 'data' su {visibile}" + (f" — meta.msg: {msg}" if msg else "")
    if msg:
        print(f"    (meta.msg: {msg})")
    time.sleep(0.4)
    return dati, None


def resta():
    r = ultimo_meta.get("requests_left")
    p = ultimo_meta.get("plan")
    pezzi = []
    if p:
        pezzi.append(f"piano: {p}")
    if r is not None:
        pezzi.append(f"richieste rimaste: {r}")
    return "  [" + ", ".join(pezzi) + "]" if pezzi else ""


def riconosci(nome: str):
    """Quale delle nostre piattaforme e' questo canale, se lo e'."""
    # Imbottito di spazi: cosi' gli aghi come " now " trovano anche il nome
    # che e' esattamente "NOW", e non trovano "shownow".
    n = " " + (nome or "").lower().strip() + " "
    for etichetta, aghi in ATTESE.items():
        if any(a in n for a in aghi):
            return etichetta
    return None


print("=" * 66)
print("SoccersAPI — conosce i broadcaster italiani?")
print("=" * 66)

# ─────────────────────────────────────────────────────────────────────────
# PASSO 1 — l'id dell'Italia nel loro catalogo.
# I countries non sono filtrati dalle leghe abilitate, quindi questo passo
# funziona anche sul piano Free.
# ─────────────────────────────────────────────────────────────────────────
print("\n[1] Cerco l'id dell'Italia")
id_italia = None
for pagina in range(1, 6):
    dati, err = chiama("countries", t="list", page=pagina)
    if err:
        print(f"  FALLITO: {err}")
        break
    if not dati:
        break
    for paese in dati:
        if str(paese.get("cc", "")).lower() == "it" or paese.get("name") == "Italy":
            id_italia = paese.get("id")
            print(f"  Italia = country_id {id_italia}{resta()}")
            break
    if id_italia:
        break

if id_italia is None:
    print("  ⚠️ Italia non trovata nelle prime 5 pagine: molto strano, da guardare a mano.")

# ─────────────────────────────────────────────────────────────────────────
# PASSO 2 — LA PROVA DECISIVA: elenco dei canali italiani.
# Se qui non c'e' DAZN, non c'e' nient'altro da verificare: le pagine per
# partita non si possono costruire su questa fonte.
# ─────────────────────────────────────────────────────────────────────────
canali_it = []
if id_italia is not None:
    print(f"\n[2] Canali italiani nel catalogo (country_id={id_italia})")
    for pagina in range(1, 6):
        dati, err = chiama("broadcast", t="list", country_id=id_italia, page=pagina)
        if err:
            print(f"  FALLITO: {err}")
            break
        if not dati:
            break
        canali_it.extend(dati)
        if pagina >= int(ultimo_meta.get("pages") or 1):
            break
    grezzo["canali_italiani"] = canali_it
    print(f"  {len(canali_it)} canali italiani{resta()}")

    trovate = {}
    for canale in canali_it:
        et = riconosci(canale.get("name", ""))
        if et:
            trovate.setdefault(et, []).append(canale.get("name"))

    print("\n  Le nostre piattaforme:")
    for etichetta in ATTESE:
        nomi = trovate.get(etichetta)
        if nomi:
            campione = ", ".join(nomi[:4]) + (f" … (+{len(nomi) - 4})" if len(nomi) > 4 else "")
            print(f"    SI   {etichetta:<12} -> {campione}")
        else:
            print(f"    NO   {etichetta}")

    if canali_it:
        print("\n  Primi 15 canali italiani, per vedere com'e' fatto il dato:")
        for canale in canali_it[:15]:
            print(f"    {canale.get('id'):>6}  {str(canale.get('name'))[:38]:<38}"
                  f"  {str(canale.get('type'))[:22]}")
        esempio = canali_it[0]
        print(f"\n  Campi di un record: {sorted(esempio.keys())}")
        print(f"  Ha il logo (img): {bool(esempio.get('img'))}"
              f"   Ha il link (channel_url): {bool(esempio.get('channel_url'))}")

# ─────────────────────────────────────────────────────────────────────────
# PASSO 3 — le partite di una data coi loro canali, filtrati sull'Italia.
# E' la chiamata che useremmo in produzione: una sola, per tutte le partite
# del giorno, con i canali dentro.
# ─────────────────────────────────────────────────────────────────────────
date_da_provare = sys.argv[1:]
if not date_da_provare:
    oggi = date.today()
    sabato = oggi + timedelta(days=(5 - oggi.weekday()) % 7 or 7)
    date_da_provare = [sabato.isoformat(), (sabato + timedelta(days=1)).isoformat()]

print(f"\n[3] Partite con i canali — date: {', '.join(date_da_provare)}")
print("    (utc=1, cioe' ora italiana invernale)")

partite_utili = 0
for giorno in date_da_provare:
    dati, err = chiama("broadcast", t="schedule", d=giorno, include="tvs", utc=1)
    if err:
        print(f"\n  {giorno}: FALLITO")
        print(f"       {err}")
        print("       ⚠️ Se e' un 403, l'operazione 'schedule' non e' compresa nel")
        print("          piano Free: ripiego sul passo 4 (match_tvs sul singolo id).")
        continue

    grezzo.setdefault("giornate", {})[giorno] = dati
    print(f"\n  {giorno}: {len(dati)} partite{resta()}")

    for partita in dati:
        squadre = partita.get("teams") or {}
        casa = (squadre.get("home") or {}).get("name", "?")
        fuori = (squadre.get("away") or {}).get("name", "?")
        lega = (partita.get("league") or {}).get("name", "?")
        ora = (partita.get("time") or {}).get("time", "")
        tv = partita.get("tvs") or []
        italiani = [c for c in tv
                    if str(((c.get("country") or {}).get("cc") or "")).lower() == "it"]

        if italiani:
            partite_utili += 1
            nostre = sorted({riconosci(c.get("name", "")) or c.get("name") for c in italiani})
            print(f"    {ora:>5}  {casa} - {fuori}  [{lega}]")
            print(f"           {len(tv)} canali totali, {len(italiani)} italiani: "
                  f"{', '.join(str(n) for n in nostre)}")
        else:
            copertura = partita.get("coverage") or {}
            print(f"    {ora:>5}  {casa} - {fuori}  [{lega}]"
                  f"  — {len(tv)} canali, NESSUNO italiano"
                  f"  (has_tvs={copertura.get('has_tvs')})")

# ─────────────────────────────────────────────────────────────────────────
# PASSO 4 — ripiego: i canali di una singola partita.
# Serve se 'schedule' non e' nel piano, o se vogliamo vedere il dato di una
# partita precisa. L'id si prende dal passo 3.
# ─────────────────────────────────────────────────────────────────────────
id_partita = None
for giorno_dati in (grezzo.get("giornate") or {}).values():
    if giorno_dati:
        id_partita = giorno_dati[0].get("id")
        break

if id_partita:
    print(f"\n[4] Controprova su una partita sola (id {id_partita})")
    dati, err = chiama("broadcast", t="match_tvs", id=id_partita)
    if err:
        print(f"  FALLITO: {err}")
    else:
        grezzo["match_tvs"] = dati
        italiani = [c for c in dati
                    if str(((c.get("country") or {}).get("cc") or "")).lower() == "it"]
        print(f"  {len(dati)} canali nel mondo, {len(italiani)} italiani{resta()}")
        for canale in italiani:
            print(f"    {canale.get('name')}  ({canale.get('type')})")
        if dati and not italiani:
            paesi = sorted({((c.get('country') or {}).get('cc') or '?') for c in dati})
            print(f"  ⚠️ Nessun canale italiano. Paesi presenti: {', '.join(paesi[:30])}")
else:
    print("\n[4] Saltato: nessun id di partita dal passo 3.")

# ─────────────────────────────────────────────────────────────────────────
# Esito
# ─────────────────────────────────────────────────────────────────────────
dove = os.path.join(os.path.dirname(os.path.abspath(__file__)), "risposte_soccersapi.json")
try:
    with open(dove, "w", encoding="utf-8") as f:
        json.dump(grezzo, f, ensure_ascii=False, indent=1)
    salvato = dove
except Exception as e:
    salvato = f"non salvato ({e})"

print("\n" + "=" * 66)
print("ESITO")
print("=" * 66)
ha_dazn = any(riconosci(c.get("name", "")) == "DAZN" for c in canali_it)
ha_sky = any(riconosci(c.get("name", "")) == "Sky Sport" for c in canali_it)

if ha_dazn and ha_sky and partite_utili:
    print("VERDE — ci sono i canali italiani E sono agganciati alle partite.")
    print("        Le pagine per singola partita si possono costruire su questa fonte.")
elif canali_it and not partite_utili:
    print("GIALLO — i canali italiani esistono nel catalogo ma non risultano")
    print("         agganciati alle partite provate. Puo' essere la data (sosta")
    print("         per le nazionali) o la lega non abilitata: riprova con")
    print("         un'altra data prima di concludere.")
elif not canali_it:
    print("ROSSO — nessun canale italiano. Su questa fonte le pagine per partita")
    print("        non si fanno: resta la mappa diritti a mano (diritti-italia.yaml).")
else:
    print("DA LEGGERE A MANO — vedi sopra.")

print(f"\nChiamate fatte: {richieste_fatte}{resta()}")
print(f"Risposte grezze: {salvato}")
