#!/usr/bin/env python3
"""Aggiorna sport/partite.yaml con il calendario dei prossimi giorni.

    python aggiorna_partite.py --prova        # scarica e stampa, non scrive niente
    python aggiorna_partite.py                # scrive sport/partite.proposta.yaml
    python aggiorna_partite.py --pubblica     # scrive sport/partite.yaml

Serve FOOTBALL_DATA_TOKEN nell'ambiente (registrazione gratuita su
football-data.org/client/register).

──────────────────────────────────────────────────────────────────────────
CHE COSA SA FARE DA SOLO, E CHE COSA NO

Il piano gratuito di football-data.org dà **calendario e orari**, non i
broadcaster: nessuna API gratuita li ha, ed e' la stessa conclusione a cui
eravamo arrivati scegliendo il piano B (vedi handover 48).

Quindi le piattaforme non vengono indovinate: si ricavano SOLO dove la regola
e' certa per tutte le partite di quella competizione, cioe' dalle piattaforme
marcate `tutte_le_partite: true` in diritti-italia.yaml. Tutta la Serie A e'
su DAZN: quello e' sicuro e si scrive. Le tre co-esclusive Sky di ogni
giornata, la partita Prime di Champions e le dirette in chiaro su TV8 NON
sono ricavabili: restano fuori.

⚠️ Questo rende la pagina **incompleta, mai sbagliata**: dire "DAZN" per una
partita che e' anche su Sky e' un'omissione; dire "Sky" per una che non c'e'
sarebbe una bugia, e la pagina vale solo finche' il dato e' giusto. Lo script
elenca a fine corsa le righe da completare a mano, col link alla fonte.

⚠️ E NON SOVRASCRIVE IL LAVORO MANUALE. Una partita gia' presente nel file
resta com'e', comprese le piattaforme e il blocco `chiaro` aggiunti a mano:
lo script aggiunge solo quelle nuove. Se cambia l'orario di una che c'e'
gia', lo corregge e lo dice. Senza questa regola ogni corsa del lunedi'
cancellerebbe le verifiche fatte il giovedi'.
──────────────────────────────────────────────────────────────────────────
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

# ⚠️ Niente sys.exit qui: questo modulo lo importa anche main.py per far
# girare l'aggiornamento dal server. Un sys.exit durante l'import
# spegnerebbe il sito invece di stampare un errore. Le dipendenze mancanti
# le controlla main(), che e' l'unico contesto in cui uscire ha senso.
try:
    import yaml
except ImportError:
    yaml = None

try:
    from zoneinfo import ZoneInfo
    ROMA = ZoneInfo("Europe/Rome")
except Exception:
    ROMA = None

QUI = os.path.dirname(os.path.abspath(__file__))
DIRITTI = os.path.join(QUI, "sport", "diritti-italia.yaml")
# Su Render il filesystem del repo e' effimero: a ogni riavvio tornerebbe la
# copia del commit, cancellando l'aggiornamento automatico. PARTITE_PATH
# punta al disco persistente, accanto al database.
PARTITE = os.environ.get("PARTITE_PATH") or os.path.join(QUI, "sport", "partite.yaml")
PARTITE_REPO = os.path.join(QUI, "sport", "partite.yaml")
PROPOSTA = os.path.join(QUI, "sport", "partite.proposta.yaml")

API = "https://api.football-data.org/v4/competitions/{codice}/matches"

# Codice football-data.org -> slug nostro. Solo le competizioni del piano
# gratuito: Europa League e Conference non ci sono e restano a mano.
COMPETIZIONI = {
    "SA": "serie-a",
    "CL": "champions-league",
}

# Per le coppe europee mostriamo solo le italiane: diciotto partite di
# Champions in pagina sarebbero un tabellone, non una risposta. La Serie A
# invece si prende tutta.
SOLO_ITALIANE = {"champions-league"}

SQUADRE_ITALIANE = {
    "Atalanta", "Bologna", "Cagliari", "Como", "Fiorentina", "Frosinone",
    "Genoa", "Inter", "Juventus", "Lazio", "Lecce", "Milan", "Monza",
    "Napoli", "Parma", "Roma", "Sassuolo", "Torino", "Udinese", "Venezia",
}

# Come li chiama l'API -> come li chiama la gente. Solo i casi in cui
# `shortName` non va bene: il resto passa cosi' com'e'.
NOMI = {
    "FC Internazionale Milano": "Inter",
    "Internazionale": "Inter",
    "AC Milan": "Milan",
    "SSC Napoli": "Napoli",
    "AS Roma": "Roma",
    "SS Lazio": "Lazio",
    "Juventus FC": "Juventus",
    "ACF Fiorentina": "Fiorentina",
    "Atalanta BC": "Atalanta",
    "Bologna FC 1909": "Bologna",
    "Torino FC": "Torino",
    "Udinese Calcio": "Udinese",
    "Genoa CFC": "Genoa",
    "Cagliari Calcio": "Cagliari",
    "US Lecce": "Lecce",
    "Parma Calcio 1913": "Parma",
    "US Sassuolo Calcio": "Sassuolo",
    "Venezia FC": "Venezia",
    "AC Monza": "Monza",
    "Como 1907": "Como",
    "Frosinone Calcio": "Frosinone",
}

FONTE_COESCLUSIVE = "https://programmi.sky.it/sport/calcio/serie-a"


# ─── lettura ──────────────────────────────────────────────────────────────

def carica_yaml(percorso, obbligatorio=True):
    try:
        with open(percorso, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        if obbligatorio:
            sys.exit(f"Manca {percorso}")
        return {}


def piattaforme_certe(diritti):
    """Per ogni slug, le piattaforme valide per OGNI partita.

    Si legge da diritti-italia.yaml: una piattaforma conta solo se ha
    `tutte_le_partite: true`. Senza quel flag non si scrive, perche' una
    piattaforma che trasmette "alcune" partite non si puo' attribuire a una
    partita precisa senza saperlo.
    """
    fuori = {}
    for c in diritti.get("competizioni") or []:
        nomi = [p.get("nome") for p in (c.get("piattaforme") or [])
                if p.get("tutte_le_partite")]
        fuori[c.get("slug")] = nomi
    return fuori


# ─── API ──────────────────────────────────────────────────────────────────

def chiama(codice, dal, al, token):
    url = f"{API.format(codice=codice)}?dateFrom={dal}&dateTo={al}"
    req = urllib.request.Request(url, headers={"X-Auth-Token": token})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        corpo = e.read().decode("utf-8", "replace")[:300]
        if e.code == 403:
            sys.exit(f"403 su {codice}: competizione fuori dal piano gratuito.\n{corpo}")
        if e.code == 429:
            sys.exit("429: superato il limite di 10 chiamate al minuto. Riprova fra un minuto.")
        sys.exit(f"HTTP {e.code} su {codice}: {corpo}")
    except urllib.error.URLError as e:
        sys.exit(f"Rete non raggiungibile: {e.reason}")


def nome(squadra):
    grezzo = squadra.get("name") or ""
    if grezzo in NOMI:
        return NOMI[grezzo]
    breve = squadra.get("shortName")
    return breve or grezzo


def scarica(token, giorni):
    oggi = datetime.now(ROMA).date()
    dal = oggi.isoformat()
    al = (oggi + timedelta(days=giorni)).isoformat()

    trovate, problemi = [], []
    for i, (codice, slug) in enumerate(COMPETIZIONI.items()):
        if i:
            time.sleep(7)        # 10 chiamate/minuto: stiamo larghi
        dati = chiama(codice, dal, al, token)
        n = 0
        for m in dati.get("matches") or []:
            if m.get("status") in ("FINISHED", "CANCELLED", "AWARDED"):
                continue
            utc = m.get("utcDate")
            if not utc:
                problemi.append(f"{slug}: partita senza data, saltata")
                continue
            quando = (datetime.fromisoformat(utc.replace("Z", "+00:00"))
                      .astimezone(ROMA))
            casa, osp = nome(m.get("homeTeam") or {}), nome(m.get("awayTeam") or {})
            if not casa or not osp:
                problemi.append(f"{slug}: squadra senza nome, saltata")
                continue
            if slug in SOLO_ITALIANE and not (
                    casa in SQUADRE_ITALIANE or osp in SQUADRE_ITALIANE):
                continue
            trovate.append({"competizione": slug, "casa": casa, "ospite": osp,
                            "inizio": quando.strftime("%Y-%m-%d %H:%M")})
            n += 1
        print(f"  {codice} ({slug}): {n} partite nei prossimi {giorni} giorni")
    return trovate, problemi


# ─── unione ───────────────────────────────────────────────────────────────

def chiave(p):
    return (p.get("competizione"), p.get("casa"), p.get("ospite"))


def unisci(esistenti, nuove, certe):
    """Le righe gia' nel file vincono. Ritorna (lista, aggiunte, orari_corretti)."""
    oggi = datetime.now(ROMA).date()
    per_chiave = {chiave(p): p for p in esistenti}

    tenute = []
    for p in esistenti:
        try:
            g = datetime.strptime(str(p.get("inizio", "")), "%Y-%m-%d %H:%M").date()
        except ValueError:
            tenute.append(p)         # riga strana: non la buttiamo, la segnala il sito
            continue
        if g >= oggi:
            tenute.append(p)         # il passato esce da solo

    aggiunte, corretti = [], []
    for n in nuove:
        vecchia = per_chiave.get(chiave(n))
        if vecchia is not None:
            if vecchia.get("inizio") != n["inizio"] and vecchia in tenute:
                corretti.append(f"{n['casa']}–{n['ospite']}: "
                                f"{vecchia.get('inizio')} → {n['inizio']}")
                vecchia["inizio"] = n["inizio"]
            continue
        dove = list(certe.get(n["competizione"]) or [])
        if not dove:
            # Senza nemmeno una piattaforma certa la riga sarebbe muta: meglio
            # segnalarla che pubblicarla a meta'.
            continue
        n["dove"] = dove
        tenute.append(n)
        aggiunte.append(n)

    tenute.sort(key=lambda p: str(p.get("inizio", "")))
    return tenute, aggiunte, corretti


# ─── scrittura ────────────────────────────────────────────────────────────

def blocco_testa(*percorsi):
    """L'intestazione del file, verbatim, dal primo percorso che esiste.

    ⚠️ Non si usa yaml.dump sull'intero file: cancellerebbe i commenti, che
    qui sono le istruzioni per compilarlo a mano. Si riscrive solo l'elenco.

    ⚠️ Si prova anche la copia del repo: sul server, alla prima corsa, il
    file sul disco persistente non esiste ancora e senza questo ripiego le
    istruzioni andrebbero perse per sempre al primo giro automatico.
    """
    for p in percorsi:
        if not p:
            continue
        try:
            testo = open(p, encoding="utf-8").read()
        except OSError:
            continue
        i = testo.find("\npartite:")
        return testo[:i + 1] if i > 0 else testo
    return "meta:\n  aggiornato: ''\n\n"


def riga_yaml(p):
    fuori = [f"- competizione: {p['competizione']}",
             f"  casa: {p['casa']}",
             f"  ospite: {p['ospite']}",
             f"  inizio: '{p['inizio']}'",
             "  dove: [" + ", ".join(p.get("dove") or []) + "]"]
    ch = p.get("chiaro")
    if ch:
        fuori.append("  chiaro:")
        fuori.append(f"    canale: {ch.get('canale')}")
        if ch.get("nota"):
            fuori.append(f"    nota: {ch['nota']}")
    return "\n".join(fuori)


def scrivi(percorso, partite, testa_da=None):
    testa = blocco_testa(*(testa_da or (percorso, PARTITE, PARTITE_REPO)))
    oggi = datetime.now(ROMA).date().isoformat()
    # `meta.aggiornato` compare in pagina: se lo lasciassimo vecchio diremmo al
    # lettore che il dato e' piu' stantio di quanto sia.
    righe = []
    for r in testa.splitlines():
        if r.strip().startswith("aggiornato:"):
            r = f"  aggiornato: '{oggi}'"
        righe.append(r)
    testa = "\n".join(righe)

    corpo = ["partite:", ""]
    giorno = None
    for p in partite:
        g = str(p.get("inizio", ""))[:10]
        if g != giorno:
            corpo.append(f"# ─── {g} ───")
            giorno = g
        corpo.append(riga_yaml(p))
        corpo.append("")

    with open(percorso, "w", encoding="utf-8") as f:
        f.write(testa.rstrip() + "\n\n" + "\n".join(corpo).rstrip() + "\n")


# ─── controlli prima di pubblicare ────────────────────────────────────────

def controlli(partite, diritti):
    """Ritorna la lista dei motivi per NON pubblicare. Vuota = via libera."""
    slug_validi = {c.get("slug") for c in diritti.get("competizioni") or []}
    nomi_validi = {p.get("nome")
                   for c in diritti.get("competizioni") or []
                   for p in (c.get("piattaforme") or [])}
    guai = []
    if not partite:
        guai.append("nessuna partita: non si pubblica un calendario vuoto")
    for p in partite:
        eti = f"{p.get('casa')}–{p.get('ospite')}"
        if p.get("competizione") not in slug_validi:
            guai.append(f"{eti}: competizione '{p.get('competizione')}' non in diritti-italia.yaml")
        try:
            datetime.strptime(str(p.get("inizio", "")), "%Y-%m-%d %H:%M")
        except ValueError:
            guai.append(f"{eti}: orario '{p.get('inizio')}' non valido")
        if not p.get("dove"):
            guai.append(f"{eti}: nessuna piattaforma")
        for d in p.get("dove") or []:
            if d not in nomi_validi:
                guai.append(f"{eti}: piattaforma '{d}' sconosciuta (niente link affiliato)")
    return guai


# ─── esecuzione, condivisa fra riga di comando e server ───────────────────

def esegui(token, giorni=9, partite_path=None, diritti_path=None,
           pubblica=False, proposta_path=None):
    """Fa il lavoro e ritorna un resoconto. Non stampa niente.

    La usano sia main() sia il worker di Render: se fossero due percorsi di
    codice diversi, il comportamento a PC spento divergerebbe da quello che
    abbiamo provato a mano, e ce ne accorgeremmo solo quando sbaglia.
    """
    partite_path = partite_path or PARTITE
    diritti_path = diritti_path or DIRITTI

    diritti = carica_yaml(diritti_path)
    certe = piattaforme_certe(diritti)
    nuove, problemi = scarica(token, giorni)

    attuali = (carica_yaml(partite_path, obbligatorio=False).get("partite")
               or carica_yaml(PARTITE_REPO, obbligatorio=False).get("partite")
               or [])
    partite, aggiunte, corretti = unisci(attuali, nuove, certe)
    guai = controlli(partite, diritti)

    esito = {
        "prima": len(attuali), "dopo": len(partite),
        "aggiunte": aggiunte, "corretti": corretti,
        "problemi": problemi, "guai": guai,
        "partite": partite, "scritto": None,
        "da_controllare": [p for p in aggiunte
                           if p.get("competizione") == "serie-a"],
        "senza_regola": [s for s in COMPETIZIONI.values() if not certe.get(s)],
    }

    if pubblica and not guai:
        scrivi(partite_path, partite)
        esito["scritto"] = partite_path
    elif pubblica:
        scrivi(proposta_path or PROPOSTA, partite)
        esito["scritto"] = proposta_path or PROPOSTA
    return esito


# ─── principale ───────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prova", action="store_true",
                    help="scarica e stampa, non scrive nessun file")
    ap.add_argument("--pubblica", action="store_true",
                    help="scrive sport/partite.yaml invece della proposta")
    ap.add_argument("--giorni", type=int, default=9,
                    help="quanti giorni in avanti (default 9: copre il turno "
                         "infrasettimanale successivo)")
    a = ap.parse_args()

    if yaml is None:
        sys.exit("Manca PyYAML:  pip install pyyaml")
    if ROMA is None:
        sys.exit("Manca zoneinfo (serve Python 3.9+)")

    token = os.environ.get("FOOTBALL_DATA_TOKEN")
    if not token:
        sys.exit("Manca FOOTBALL_DATA_TOKEN nell\'ambiente.\n"
                 "Registrazione gratuita: https://www.football-data.org/client/register")

    print(f"Scarico il calendario (prossimi {a.giorni} giorni)\u2026")
    e = esegui(token, giorni=a.giorni,
               pubblica=(a.pubblica and not a.prova))

    if e["senza_regola"]:
        print("\u26a0\ufe0f  Nessuna piattaforma con `tutte_le_partite: true` per: "
              + ", ".join(e["senza_regola"]))
        print("    Le partite di queste competizioni sono state saltate.")
    for p in e["problemi"]:
        print(f"  \u26a0\ufe0f  {p}")

    print(f"\nIn file: {e['prima']}  \u2192  dopo l'unione: {e['dopo']}  "
          f"(nuove: {len(e['aggiunte'])})")
    for c in e["corretti"]:
        print(f"  orario cambiato \u2014 {c}")

    if e["guai"]:
        print("\n\u26a0\ufe0f  Controlli non superati:")
        for g in e["guai"]:
            print(f"   \u00b7 {g}")

    # Le righe che un umano deve ancora guardare: SOLO quelle appena aggiunte.
    #
    # \u26a0\ufe0f Prima contavo tutte le righe di Serie A con la sola DAZN, e ne
    # usciva "11 da completare" quando quelle davvero da controllare erano 4:
    # le altre sette erano gia' state verificate ed erano DAZN e basta, che e'
    # la risposta giusta. Un avviso che segnala anche il lavoro gia' fatto
    # diventa rumore, e un avviso che e' rumore smette di essere letto \u2014 e'
    # lo stesso difetto del cap sul pannello SEO.
    if e["da_controllare"]:
        print(f"\nDa controllare a mano, {len(e['da_controllare'])} partite nuove di Serie A:")
        for p in e["da_controllare"]:
            print(f"   \u00b7 {p['inizio']}  {p['casa']}\u2013{p['ospite']}")
        print("  Tre per giornata sono in co-esclusiva su Sky Go e NOW, e l'API "
              "non lo sa.")
        print(f"  Fonte: {FONTE_COESCLUSIVE}")
        print("  Finche' non le aggiungi la pagina resta corretta, solo incompleta.")

    if a.prova:
        print("\n--prova: non ho scritto niente. Ecco come verrebbe:\n")
        for p in e["partite"][:40]:
            ch = f"  [chiaro: {p['chiaro'].get('canale')}]" if p.get("chiaro") else ""
            # La colonna si allinea sulla COPPIA, non sul solo ospite: con
            # "Borac Banja Luka" la tabella si sfalsava.
            sfida = f"{p['casa']}\u2013{p['ospite']}"
            print(f"  {p['inizio']}  {sfida:<32} "
                  f"{p['competizione']:<18} {', '.join(p.get('dove') or [])}{ch}")
        return 0

    if a.pubblica:
        if e["guai"]:
            print("\n\u2717 NON pubblico: prima vanno sistemati i punti qui sopra.")
            print(f"  Proposta scritta comunque \u2192 {e['scritto']}")
            return 1
        print(f"\n\u2713 Scritto {e['scritto']}")
        print("  Ricorda commit e push. Reinvio della sitemap non serve: "
              "la URL esiste gia'.")
        return 0

    scrivi(PROPOSTA, e["partite"])
    print(f"\n\u2713 Scritto {PROPOSTA}")
    print("  Controlla, completa le co-esclusive, poi rinominalo in partite.yaml.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
