"""
core/alerts_job.py — Controlla gli streaming alert in sospeso e, quando un
titolo diventa disponibile in streaming (flatrate) in Italia, invia il push
agli utenti che l'avevano richiesto e marca l'alert come notificato.

Gira una volta al giorno (scheduler in main.py). Import "pesanti" fatti dentro
la funzione per evitare cicli di import al load del modulo.
"""
import logging
from collections import defaultdict

log = logging.getLogger("cosaguardo")


def check_and_notify_alerts() -> dict:
    from app.db import list_pending_streaming_alerts, mark_streaming_alert_notified
    from core.recommendation_api import get_watch_providers
    from core.push import send_push_to_user

    alerts = list_pending_streaming_alerts()
    if not alerts:
        return {"pending": 0, "arrived": 0, "notified": 0}

    # Raggruppa per titolo così controlliamo i provider UNA volta per titolo
    groups = defaultdict(list)
    for a in alerts:
        groups[(a["tmdb_id"], a["content_type"], a["title"])].append(a)

    arrived = 0
    notified = 0
    for (tmdb_id, ct, title), subs in groups.items():
        if not title:
            continue
        try:
            prov = get_watch_providers(title, content_type=ct) or {}
        except Exception as e:
            log.warning("alerts_job: provider check fallito per '%s': %s", title, e)
            continue
        flatrate = prov.get("flatrate") or []
        if not flatrate:
            continue  # non ancora in streaming
        arrived += 1
        names = ", ".join(p.get("name", "") for p in flatrate[:3] if p.get("name"))
        path = f"/serie/{tmdb_id}" if ct == "tv" else f"/film/{tmdb_id}"
        for a in subs:
            uid = a.get("user_id")
            if not uid:
                # alert solo-email: lasciato pending per futuro invio email
                continue
            try:
                n = send_push_to_user(
                    uid,
                    title=f"{title} è arrivato in streaming!",
                    body=(f"Ora lo trovi su {names}." if names else "Ora è disponibile in streaming."),
                    url=path,
                    tag=f"alert-{tmdb_id}",
                )
                if n > 0:
                    notified += 1
                    mark_streaming_alert_notified(a["id"])
            except Exception as e:
                log.warning("alerts_job: push fallito (alert %s): %s", a.get("id"), e)

    log.info("alerts_job: pending=%d arrived=%d notified=%d", len(alerts), arrived, notified)
    return {"pending": len(alerts), "arrived": arrived, "notified": notified}
