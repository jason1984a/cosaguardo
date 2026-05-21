"""
Aggregazioni DB per la dashboard admin (/admin/report).

Architettura:
- Tutte le funzioni accettano `period` ∈ {'today', '7d', '30d'}
- Ogni metrica ritorna {"current": N, "previous": M, "delta_pct": X.X}
  così la dashboard mostra confronti vs periodo precedente
- Date sempre in Europe/Rome lato utente, confrontate con CURRENT_TIMESTAMP UTC del DB
  (SQLite default = UTC)

Nessuna dipendenza esterna oltre stdlib + sqlite3. Le query usano placeholder ?
per evitare SQL injection. Pattern try/finally + conn.close() come da convenzione
CosaGuardo.

Performance: nessuna view o trigger, solo query aggregate. Ogni funzione apre/chiude
una connection veloce (SQLite single-file in-process è sub-ms). Cache lato Python
NON applicata qui: la dashboard è admin-only, traffico negligibile, dati sempre live.
"""
import os
import sqlite3
import logging
from datetime import datetime, timedelta
from collections import Counter
from typing import Optional

log = logging.getLogger("cosaguardo")

# Stesso DB del resto del progetto
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_DB = os.path.join(_BASE_DIR, "app", "cosaguardo.db")
ADMIN_DB_PATH = os.environ.get("DATABASE_PATH") or _DEFAULT_DB


# ─── HELPERS PERIODO ──────────────────────────────────────────────────────
def _period_bounds(period: str) -> tuple[str, str, str, str, str]:
    """
    Restituisce (start_current, end_current, start_previous, end_previous, label).

    Le date sono in formato ISO UTC ('YYYY-MM-DD HH:MM:SS') per confronto diretto
    con i CURRENT_TIMESTAMP di SQLite.

    Logica "oggi": dalla mezzanotte Europe/Rome di oggi fino ad ora.
    "7d": ultimi 7x24h rolling vs i 7x24h precedenti.
    "30d": ultimi 30x24h rolling vs i 30x24h precedenti.

    Implementazione UTC: il DB salva tutto in UTC, quindi convertiamo i bound da
    Europe/Rome → UTC prima del confronto. Per 7d/30d rolling il fuso non importa
    (è una finestra mobile), ma per "today" sì (l'utente pensa "mezzanotte italiana").
    """
    try:
        from zoneinfo import ZoneInfo
        rome = ZoneInfo("Europe/Rome")
        utc = ZoneInfo("UTC")
        now_rome = datetime.now(rome)
    except Exception:
        # Fallback: trattiamo come UTC (errore minore, niente DST handling)
        now_rome = datetime.utcnow()
        rome = utc = None

    if period == "today":
        # Mezzanotte Rome di oggi
        start_rome = now_rome.replace(hour=0, minute=0, second=0, microsecond=0)
        end_rome = now_rome
        # Periodo confronto: ieri stessa fascia oraria
        prev_start_rome = start_rome - timedelta(days=1)
        prev_end_rome = end_rome - timedelta(days=1)
        label = "Oggi"
    elif period == "30d":
        end_rome = now_rome
        start_rome = end_rome - timedelta(days=30)
        prev_end_rome = start_rome
        prev_start_rome = prev_end_rome - timedelta(days=30)
        label = "Ultimi 30 giorni"
    else:  # default 7d
        end_rome = now_rome
        start_rome = end_rome - timedelta(days=7)
        prev_end_rome = start_rome
        prev_start_rome = prev_end_rome - timedelta(days=7)
        label = "Ultimi 7 giorni"

    # Converti in UTC per query
    if rome:
        start_utc = start_rome.astimezone(utc).strftime("%Y-%m-%d %H:%M:%S")
        end_utc = end_rome.astimezone(utc).strftime("%Y-%m-%d %H:%M:%S")
        prev_start_utc = prev_start_rome.astimezone(utc).strftime("%Y-%m-%d %H:%M:%S")
        prev_end_utc = prev_end_rome.astimezone(utc).strftime("%Y-%m-%d %H:%M:%S")
    else:
        # No zoneinfo: usa stringhe naive
        start_utc = start_rome.strftime("%Y-%m-%d %H:%M:%S")
        end_utc = end_rome.strftime("%Y-%m-%d %H:%M:%S")
        prev_start_utc = prev_start_rome.strftime("%Y-%m-%d %H:%M:%S")
        prev_end_utc = prev_end_rome.strftime("%Y-%m-%d %H:%M:%S")

    return start_utc, end_utc, prev_start_utc, prev_end_utc, label


def _delta_pct(current: int, previous: int) -> Optional[float]:
    """Variazione percentuale con guardia divisione per zero.
    None se previous=0 (UI mostrerà '—' invece di 'inf%')."""
    if previous == 0:
        return None
    return round((current - previous) / previous * 100, 1)


def _metric_with_compare(current: int, previous: int) -> dict:
    """Helper standardizzato per restituire {current, previous, delta_pct}."""
    return {
        "current": current,
        "previous": previous,
        "delta_pct": _delta_pct(current, previous),
    }


def _count_in_range(conn, sql_count: str, start: str, end: str) -> int:
    """Esegue una query COUNT con bind start/end e ritorna il risultato int.
    sql_count deve contenere 2 placeholder ? per (start, end).
    Eccezioni silenziate (DB locked, schema mismatch) → 0."""
    try:
        cur = conn.cursor()
        cur.execute(sql_count, (start, end))
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except sqlite3.Error as e:
        log.warning("admin_metrics: query failed: %s — sql=%s", e, sql_count[:100])
        return 0


# ─── METRICHE UTENTI ──────────────────────────────────────────────────────
def metric_new_users(period: str) -> dict:
    """Nuove registrazioni nel periodo + delta vs periodo precedente."""
    s, e, ps, pe, _ = _period_bounds(period)
    conn = sqlite3.connect(ADMIN_DB_PATH, timeout=3)
    try:
        sql = "SELECT COUNT(*) FROM users WHERE created_at >= ? AND created_at < ?"
        cur_v = _count_in_range(conn, sql, s, e)
        prev_v = _count_in_range(conn, sql, ps, pe)
    finally:
        conn.close()
    return _metric_with_compare(cur_v, prev_v)


def metric_total_users() -> int:
    """Utenti totali registrati (cumulativo, no periodo)."""
    conn = sqlite3.connect(ADMIN_DB_PATH, timeout=3)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        row = cur.fetchone()
        return int(row[0]) if row else 0
    except sqlite3.Error:
        return 0
    finally:
        conn.close()


def metric_active_users(period: str) -> dict:
    """Utenti distinti che hanno fatto almeno 1 search nel periodo.
    Proxy ragionevole di "utenti attivi" — il prodotto core è la search."""
    s, e, ps, pe, _ = _period_bounds(period)
    conn = sqlite3.connect(ADMIN_DB_PATH, timeout=3)
    try:
        sql = """
            SELECT COUNT(DISTINCT user_id) FROM searches
            WHERE created_at >= ? AND created_at < ?
        """
        cur_v = _count_in_range(conn, sql, s, e)
        prev_v = _count_in_range(conn, sql, ps, pe)
    finally:
        conn.close()
    return _metric_with_compare(cur_v, prev_v)


# ─── METRICHE RICERCHE ────────────────────────────────────────────────────
def metric_searches(period: str) -> dict:
    """Ricerche eseguite nel periodo."""
    s, e, ps, pe, _ = _period_bounds(period)
    conn = sqlite3.connect(ADMIN_DB_PATH, timeout=3)
    try:
        sql = "SELECT COUNT(*) FROM searches WHERE created_at >= ? AND created_at < ?"
        cur_v = _count_in_range(conn, sql, s, e)
        prev_v = _count_in_range(conn, sql, ps, pe)
    finally:
        conn.close()
    return _metric_with_compare(cur_v, prev_v)


def top_searched_titles(period: str, limit: int = 10) -> list[dict]:
    """Top N titoli più cercati come SEED nel periodo.

    `seed_titles` è una stringa CSV (es. "Inception,Dune,The Matrix"). Split
    lato Python perché SQLite non ha unnest/split nativo (potremmo fare regex
    ma è più sporco).

    Ritorna [{"title": "Inception", "count": 42}, ...] sorted DESC.
    """
    s, e, _, _, _ = _period_bounds(period)
    counter: Counter = Counter()
    conn = sqlite3.connect(ADMIN_DB_PATH, timeout=3)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT seed_titles FROM searches
            WHERE created_at >= ? AND created_at < ?
        """, (s, e))
        for (seeds_csv,) in cur.fetchall():
            if not seeds_csv:
                continue
            # Split su virgola, strip, ignora vuoti
            for t in seeds_csv.split(","):
                t = t.strip()
                if t:
                    counter[t] += 1
    except sqlite3.Error as e:
        log.warning("top_searched_titles: %s", e)
    finally:
        conn.close()
    return [{"title": t, "count": c} for t, c in counter.most_common(limit)]


# ─── METRICHE STREAMING ALERTS (conversion proxy) ─────────────────────────
def metric_alerts(period: str) -> dict:
    """Streaming alerts raccolti nel periodo (= email "avvisami quando arriva")."""
    s, e, ps, pe, _ = _period_bounds(period)
    conn = sqlite3.connect(ADMIN_DB_PATH, timeout=3)
    try:
        sql = """
            SELECT COUNT(*) FROM streaming_alerts
            WHERE created_at >= ? AND created_at < ?
        """
        cur_v = _count_in_range(conn, sql, s, e)
        prev_v = _count_in_range(conn, sql, ps, pe)
    finally:
        conn.close()
    return _metric_with_compare(cur_v, prev_v)


def top_alerted_titles(period: str, limit: int = 10) -> list[dict]:
    """Top N titoli più richiesti come alert nel periodo."""
    s, e, _, _, _ = _period_bounds(period)
    conn = sqlite3.connect(ADMIN_DB_PATH, timeout=3)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT title, content_type, tmdb_id, COUNT(*) AS n
            FROM streaming_alerts
            WHERE created_at >= ? AND created_at < ?
              AND title IS NOT NULL
            GROUP BY tmdb_id, content_type
            ORDER BY n DESC
            LIMIT ?
        """, (s, e, limit))
        return [
            {"title": r[0], "content_type": r[1], "tmdb_id": r[2], "count": r[3]}
            for r in cur.fetchall()
        ]
    except sqlite3.Error as e:
        log.warning("top_alerted_titles: %s", e)
        return []
    finally:
        conn.close()


# ─── METRICHE TRACKING SERIE ──────────────────────────────────────────────
def metric_series_tracked(period: str) -> dict:
    """Serie aggiunte al tracking nel periodo (= 'sto guardando questa serie')."""
    s, e, ps, pe, _ = _period_bounds(period)
    conn = sqlite3.connect(ADMIN_DB_PATH, timeout=3)
    try:
        sql = """
            SELECT COUNT(*) FROM user_series_tracking
            WHERE created_at >= ? AND created_at < ?
        """
        cur_v = _count_in_range(conn, sql, s, e)
        prev_v = _count_in_range(conn, sql, ps, pe)
    finally:
        conn.close()
    return _metric_with_compare(cur_v, prev_v)


# ─── METRICHE FEEDBACK ────────────────────────────────────────────────────
def metric_feedback(period: str) -> dict:
    """Feedback raccolti nel periodo, breakdown like/dislike."""
    s, e, ps, pe, _ = _period_bounds(period)
    conn = sqlite3.connect(ADMIN_DB_PATH, timeout=3)
    try:
        cur = conn.cursor()
        # Current breakdown
        cur.execute("""
            SELECT feedback_type, COUNT(*) FROM user_feedback
            WHERE created_at >= ? AND created_at < ?
            GROUP BY feedback_type
        """, (s, e))
        cur_by_type = {r[0]: r[1] for r in cur.fetchall()}
        # Previous totale (per delta)
        cur.execute("""
            SELECT COUNT(*) FROM user_feedback
            WHERE created_at >= ? AND created_at < ?
        """, (ps, pe))
        prev_total = cur.fetchone()[0] or 0
    except sqlite3.Error as e:
        log.warning("metric_feedback: %s", e)
        cur_by_type = {}
        prev_total = 0
    finally:
        conn.close()

    cur_total = sum(cur_by_type.values())
    return {
        "current": cur_total,
        "previous": prev_total,
        "delta_pct": _delta_pct(cur_total, prev_total),
        "by_type": cur_by_type,
    }


# ─── METRICHE DAILY RECOMMENDATIONS ───────────────────────────────────────
def metric_daily_recs(period: str) -> dict:
    """Daily recommendations generate nel periodo (proxy engagement giornaliero)."""
    s, e, ps, pe, _ = _period_bounds(period)
    conn = sqlite3.connect(ADMIN_DB_PATH, timeout=3)
    try:
        sql = """
            SELECT COUNT(*) FROM daily_recommendations
            WHERE created_at >= ? AND created_at < ?
        """
        cur_v = _count_in_range(conn, sql, s, e)
        prev_v = _count_in_range(conn, sql, ps, pe)
    finally:
        conn.close()
    return _metric_with_compare(cur_v, prev_v)


def metric_users_with_daily_recs(period: str) -> dict:
    """Utenti distinti che hanno avuto almeno una daily rec nel periodo.
    Più indicativo del COUNT puro per capire la copertura."""
    s, e, ps, pe, _ = _period_bounds(period)
    conn = sqlite3.connect(ADMIN_DB_PATH, timeout=3)
    try:
        sql = """
            SELECT COUNT(DISTINCT user_id) FROM daily_recommendations
            WHERE created_at >= ? AND created_at < ?
        """
        cur_v = _count_in_range(conn, sql, s, e)
        prev_v = _count_in_range(conn, sql, ps, pe)
    finally:
        conn.close()
    return _metric_with_compare(cur_v, prev_v)


# ─── REPORT AGGREGATO ─────────────────────────────────────────────────────
def get_full_report(period: str) -> dict:
    """
    Aggrega tutte le metriche in un dict unico per il template.
    Una sola chiamata della dashboard → un'unica funzione.
    """
    if period not in ("today", "7d", "30d"):
        period = "7d"
    _, _, _, _, label = _period_bounds(period)

    return {
        "period": period,
        "period_label": label,
        # Utenti
        "new_users":     metric_new_users(period),
        "total_users":   metric_total_users(),
        "active_users":  metric_active_users(period),
        # Ricerche
        "searches":      metric_searches(period),
        "top_searches":  top_searched_titles(period, limit=10),
        # Alert (conversion)
        "alerts":        metric_alerts(period),
        "top_alerts":    top_alerted_titles(period, limit=10),
        # Tracking
        "series_tracked": metric_series_tracked(period),
        # Feedback
        "feedback":      metric_feedback(period),
        # Daily recs
        "daily_recs":         metric_daily_recs(period),
        "users_with_recs":    metric_users_with_daily_recs(period),
    }
