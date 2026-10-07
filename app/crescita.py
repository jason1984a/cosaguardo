"""
Pannello Crescita (/admin/crescita).

Per ogni giorno di registrazione e gruppo di origine:
  - registrati
  - con segnali: almeno una ricerca o un titolo segnato (visto, preferito,
    non mi piace) entro FINESTRA_GIORNI dalla registrazione
  - tornati: attivi in almeno un giorno DIVERSO da quello di registrazione,
    entro FINESTRA_GIORNI

⚠️ La finestra fissa e' il punto di tutto il pannello: senza, i giorni piu'
vecchi sembrano sempre migliori solo perche' hanno avuto piu' tempo per
tornare, e il confronto fra giorni non vale niente. Un giorno e' "maturato"
quando la finestra di tutti i suoi registrati si e' chiusa.

Giorni in UTC, come users.created_at e user_activity.day.
"""
from datetime import date, timedelta

from app.db import get_connection

FINESTRA_GIORNI = 7
GIORNI_TABELLA = 30

# (chiave, etichetta). "tutti" e' il totale, non un'origine.
GRUPPI = [
    ("tutti", "Tutte"),
    ("android", "App Android"),
    ("ios", "App iOS"),
    ("instagram", "Instagram"),
    ("meta", "Meta ads"),
    ("sito", "Sito"),
    ("altro", "Altro"),
]
_CHIAVI = {k for k, _ in GRUPPI}


def gruppo_origine(signup_source) -> str:
    """
    Raggruppa le etichette grezze di signup_source.
    ⚠️ "app" e' l'app ANDROID (start_url ?src=app della TWA); l'app iOS
    registra "ios" / "ios-apple". "instagram" e "ig" sono la stessa cosa.
    """
    s = (signup_source or "").strip().lower()
    if s == "app":
        return "android"
    if s.startswith("ios"):
        return "ios"
    if s in ("instagram", "ig"):
        return "instagram"
    if s in ("meta", "facebook", "fb"):
        return "meta"
    if s == "sito":
        return "sito"
    return "altro"


def _colonna_data_title_state(cur) -> str:
    """
    Le prime versioni di user_title_state non avevano created_at.
    updated_at e' il ripiego: cambia a ogni modifica, quindi su quelle righe
    il segnale puo' risultare piu' tardi del vero (sottostima, mai sovrastima).
    """
    colonne = {r[1] for r in cur.execute("PRAGMA table_info(user_title_state)").fetchall()}
    return "created_at" if "created_at" in colonne else "updated_at"


def _righe_utenti(giorni_indietro: int):
    """Un dict per utente registrato negli ultimi giorni_indietro giorni."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        col_ts = _colonna_data_title_state(cur)
        oggi = cur.execute("SELECT date('now')").fetchone()[0]
        finestra = f"+{FINESTRA_GIORNI} days"
        # col_ts arriva da una lista chiusa, non dall'utente: l'f-string e' sicura.
        cur.execute(f"""
            SELECT
                u.created_at,
                u.signup_source,
                EXISTS (
                    SELECT 1 FROM searches s
                    WHERE s.user_id = u.id
                      AND s.created_at < datetime(u.created_at, :fin)
                ) AS ha_ricerca,
                EXISTS (
                    SELECT 1 FROM user_title_state t
                    WHERE t.user_id = u.id
                      AND t.{col_ts} < datetime(u.created_at, :fin)
                ) AS ha_titolo,
                EXISTS (
                    SELECT 1 FROM user_activity a
                    WHERE a.user_id = u.id
                      AND a.day >  date(u.created_at)
                      AND a.day <= date(u.created_at, :fin)
                ) AS tornato
            FROM users u
            WHERE u.created_at >= date('now', :da)
        """, {"fin": finestra, "da": f"-{giorni_indietro} days"})
        righe = []
        for r in cur.fetchall():
            righe.append({
                "giorno":  (r["created_at"] or "")[:10],
                "gruppo":  gruppo_origine(r["signup_source"]),
                "segnale": bool(r["ha_ricerca"] or r["ha_titolo"]),
                "tornato": bool(r["tornato"]),
            })
        return date.fromisoformat(oggi), righe
    finally:
        conn.close()


def _conta(righe, gruppo: str, dal: date, al: date) -> dict:
    """Registrati, segnali e tornati fra due giorni inclusi."""
    a, b = dal.isoformat(), al.isoformat()
    sel = [r for r in righe
           if a <= r["giorno"] <= b and (gruppo == "tutti" or r["gruppo"] == gruppo)]
    reg = len(sel)
    seg = sum(r["segnale"] for r in sel)
    tor = sum(r["tornato"] for r in sel)
    return {
        "registrati": reg,
        "segnali": seg,
        "tornati": tor,
        "pct_segnali": round(100 * seg / reg) if reg else None,
        "pct_tornati": round(100 * tor / reg) if reg else None,
    }


def _delta_pct(attuale: int, precedente: int):
    if not precedente:
        return None
    return round(100 * (attuale - precedente) / precedente)


def _delta_punti(attuale, precedente):
    if attuale is None or precedente is None:
        return None
    return attuale - precedente


def get_crescita(gruppo: str = "tutti") -> dict:
    if gruppo not in _CHIAVI:
        gruppo = "tutti"

    # 21 giorni bastano per i confronti settimanali; la tabella ne vuole 30.
    oggi, righe = _righe_utenti(max(GIORNI_TABELLA, 3 * FINESTRA_GIORNI))
    giorno = timedelta(days=1)

    # Un giorno e' maturato quando anche chi si e' registrato alle 23:59 ha
    # avuto la finestra intera.
    ultimo_maturo = oggi - timedelta(days=FINESTRA_GIORNI + 1)

    # Registrati: ultimi 7 giorni contro i 7 precedenti (dato gia' definitivo).
    reg_ora = _conta(righe, gruppo, oggi - 6 * giorno, oggi)
    reg_prima = _conta(righe, gruppo, oggi - 13 * giorno, oggi - 7 * giorno)

    # Qualita': ultima settimana maturata contro quella prima.
    sett_a = (ultimo_maturo - 6 * giorno, ultimo_maturo)
    sett_b = (ultimo_maturo - 13 * giorno, ultimo_maturo - 7 * giorno)
    q_a = _conta(righe, gruppo, *sett_a)
    q_b = _conta(righe, gruppo, *sett_b)

    # Ripartizione per origine sulla settimana maturata.
    per_origine = []
    for chiave, etichetta in GRUPPI:
        if chiave == "tutti":
            continue
        c = _conta(righe, chiave, *sett_a)
        if c["registrati"]:
            per_origine.append({"chiave": chiave, "etichetta": etichetta, **c})
    per_origine.sort(key=lambda x: -x["registrati"])

    # Tabella giornaliera, dal piu' recente, giorni vuoti compresi.
    tabella = []
    for i in range(GIORNI_TABELLA):
        g = oggi - i * giorno
        c = _conta(righe, gruppo, g, g)
        tabella.append({"giorno": g.isoformat(), "maturo": g <= ultimo_maturo, **c})

    return {
        "gruppo": gruppo,
        "gruppi": GRUPPI,
        "finestra": FINESTRA_GIORNI,
        "oggi": oggi.isoformat(),
        "registrati": {
            "attuale": reg_ora["registrati"],
            "precedente": reg_prima["registrati"],
            "delta_pct": _delta_pct(reg_ora["registrati"], reg_prima["registrati"]),
        },
        "settimana_matura": {"dal": sett_a[0].isoformat(), "al": sett_a[1].isoformat()},
        "segnali": {
            "attuale": q_a["pct_segnali"], "precedente": q_b["pct_segnali"],
            "delta_punti": _delta_punti(q_a["pct_segnali"], q_b["pct_segnali"]),
            "conteggio": q_a["segnali"], "su": q_a["registrati"],
        },
        "tornati": {
            "attuale": q_a["pct_tornati"], "precedente": q_b["pct_tornati"],
            "delta_punti": _delta_punti(q_a["pct_tornati"], q_b["pct_tornati"]),
            "conteggio": q_a["tornati"], "su": q_a["registrati"],
        },
        "per_origine": per_origine,
        "tabella": tabella,
    }
