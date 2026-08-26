/* CosaGuardo — JS del layout estratto da base.html (09/06/2026).
   Caricato con defer: gira dopo il parsing del DOM, prima di
   DOMContentLoaded. Il blocco analytics/consent resta inline in <head>
   perche' deve girare per primo. IS_LOGGED_IN arriva da window.__CG_LOGGED_IN
   (bootstrap inline). IMPORTANTE: ad ogni modifica bumpa ?v=N nel <script>. */


        // CG_NavProgress: feedback "stai navigando" al click.
        // Funziona così:
        //  1. Al click su <a href="..."> interno (no target=_blank, no hash, no js:),
        //     parte la bar che va al 25% subito, poi cresce graduale fino a 90%.
        //  2. Su 'pageshow' (= la nuova pagina si è caricata) o 'beforeunload',
        //     la bar va al 100% e fade out.
        //  3. Su navigazione browser (back/forward via popstate) idem, anche se
        //     il caso d'uso principale è il click.
        // Edge case: se il click non porta a navigazione (es. preventDefault da
        // altro listener), la bar comunque si autoresetta dopo 4s di timeout.
        (function() {
            var bar = document.getElementById('cg-nav-progress');
            if (!bar) return;
            var progressInterval = null;
            var resetTimeout = null;
            var currentWidth = 0;

            // Applica una percentuale di avanzamento via transform.
            // scaleX gira sul compositor: continua ad animare anche quando il
            // thread principale e' bloccato dalla navigazione in corso.
            function setPct(pct) {
                bar.style.transform = 'scaleX(' + (pct / 100) + ')';
            }

            function start() {
                if (progressInterval) return;  // già in corso
                clearTimeout(resetTimeout);
                currentWidth = 0;

                // Stato iniziale a zero SENZA transizione, poi reflow forzato:
                // senza il reflow il browser accorpa le due modifiche di stile
                // e la transizione non parte.
                bar.style.transition = 'none';
                setPct(0);
                bar.classList.add('cg-progress-active');
                void bar.offsetWidth;                 // <- reflow forzato
                bar.style.transition = '';            // torna alla transizione del CSS

                // BUG FIX iPad/Safari: il salto iniziale era dentro
                // requestAnimationFrame. Su Safari iOS i callback rAF smettono
                // di essere serviti appena inizia la navigazione, quindi quel
                // callback non girava mai e la barra restava a zero (invisibile).
                // Ora la larghezza iniziale e' assegnata in modo SINCRONO.
                currentWidth = 45;
                setPct(45);

                // Poi cresce graduale verso 95% (mai 100% finché non arriva la nuova pagina).
                progressInterval = setInterval(function() {
                    if (currentWidth < 95) {
                        currentWidth += (95 - currentWidth) * 0.25;
                        setPct(currentWidth);
                    }
                }, 180);
                // Safety: se la nav non avviene entro 4s, resetta
                resetTimeout = setTimeout(reset, 4000);
            }

            function finish() {
                if (!progressInterval) return;
                clearInterval(progressInterval);
                clearTimeout(resetTimeout);
                progressInterval = null;
                setPct(100);
                setTimeout(function() {
                    bar.classList.remove('cg-progress-active');
                    setTimeout(function() {
                        bar.style.transition = 'none';
                        setPct(0);
                    }, 220);
                }, 150);
            }

            function reset() {
                if (progressInterval) {
                    clearInterval(progressInterval);
                    progressInterval = null;
                }
                clearTimeout(resetTimeout);
                bar.classList.remove('cg-progress-active');
                bar.style.transition = 'none';
                setPct(0);
            }

            // Click handler delegato sul document (cattura anche link aggiunti dinamicamente)
            document.addEventListener('click', function(e) {
                // Trova l'<a> più vicino salendo
                var el = e.target;
                while (el && el !== document.body) {
                    if (el.tagName === 'A') break;
                    el = el.parentElement;
                }
                if (!el || el.tagName !== 'A') return;
                var href = el.getAttribute('href');
                if (!href) return;
                // Skip: external (target=_blank), mailto/tel, anchor puro, javascript:
                if (el.getAttribute('target') === '_blank') return;
                if (href.indexOf('mailto:') === 0) return;
                if (href.indexOf('tel:') === 0) return;
                if (href.indexOf('javascript:') === 0) return;
                if (href.indexOf('#') === 0) return;
                // Skip se modificatore tastiera (ctrl/cmd/shift = nuova tab)
                if (e.ctrlKey || e.metaKey || e.shiftKey) return;
                // Skip se dominio esterno
                if (/^https?:\/\//.test(href)) {
                    try {
                        var url = new URL(href);
                        if (url.host !== location.host) return;
                    } catch (_) { return; }
                }
                start();
            }, true);

            // Form submit: anche i form possono causare navigazione
            document.addEventListener('submit', function(e) {
                var form = e.target;
                if (form && form.tagName === 'FORM' && form.method.toLowerCase() !== 'get') {
                    // POST → ci sarà sicuramente una nav
                    start();
                }
            }, true);

            // Cleanup quando la pagina viene davvero scaricata.
            //
            // BUG FIX: precedente codice usava 'beforeunload' che scatta QUANDO il
            // browser INIZIA la navigazione (decine di ms dopo il click), NON quando
            // la nuova pagina arriva. Su mobile con pagine 2-5s la bar sembrava
            // scomparire prematuramente. Usiamo:
            //   - 'pagehide': scatta più tardi, quando la pagina viene davvero
            //     distaccata dal DOM (= la nuova è quasi visibile). Standard moderno.
            //   - 'pageshow' con persisted: per back/forward da bfcache (caso edge).
            // Niente 'beforeunload' qui: era la causa del fade-out prematuro.
            window.addEventListener('pagehide', finish);
            // Safari iOS non emette sempre 'pagehide' in modo affidabile:
            // visibilitychange scatta comunque quando la pagina sparisce.
            document.addEventListener('visibilitychange', function() {
                if (document.visibilityState === 'hidden') finish();
            });
            window.addEventListener('pageshow', function(e) {
                // pageshow scatta su back/forward dalla bfcache: completa la bar
                // se stava ancora correndo (= utente è tornato indietro mentre la
                // bar era attiva)
                if (e.persisted && progressInterval) finish();
            });
            // Safety: se per qualsiasi motivo la bar resta attiva dopo onload, reset
            window.addEventListener('load', function() {
                setTimeout(reset, 100);
            });

            // Esposta globalmente per le navigazioni programmatiche
            // (es. window.location.href nelle card di /recommend), che il click
            // listener sugli <a> non intercetta da solo.
            window.cgStartNavProgress = start;
        })();
    


        // Lucide caricato LAZY via requestIdleCallback per non competere
        // con le immagini sul critical path. Trade-off accettato:
        // le icone appaiono ~500ms dopo FCP, ma e' il prezzo per fixare NO_LCP.
        (function() {
            function loadLucide() {
                if (window.__lucideLoaded) return;
                window.__lucideLoaded = true;
                var s = document.createElement('script');
                s.src = 'https://unpkg.com/lucide@0.469.0/dist/umd/lucide.min.js';
                s.onload = function() {
                    if (typeof lucide !== 'undefined') lucide.createIcons();
                };
                document.head.appendChild(s);
            }
            // Aspetta idle CPU oppure 1.5s di timeout per stare fuori dalla
            // finestra di misurazione LCP di Lighthouse (~2s su mobile slow).
            if ('requestIdleCallback' in window) {
                requestIdleCallback(loadLucide, { timeout: 2000 });
            } else {
                setTimeout(loadLucide, 1500);
            }
            // Espongo loader globale: utile se in futuro si aggiungono icone
            // dinamiche dopo il primo paint.
            window.__loadLucide = loadLucide;
        })();
    


        (function() {
            // HTML del banner come stringa: iniettato solo quando serve, mai presente
            // nel DOM iniziale. Questo evita che il banner diventi LCP candidate
            // (è position:fixed bottom:0 max-width:780px → enorme rispetto ai poster mobile).
            var BANNER_HTML =
                '<div id="cookie-banner" role="dialog" aria-label="Consenso cookie">' +
                  '<div class="cookie-banner-inner">' +
                    '<div class="cookie-banner-text">' +
                      '<strong>Rispettiamo la tua privacy</strong>' +
                      '<p>Usiamo cookie analitici (Google Analytics) per capire come viene usato il sito e migliorarlo. Non vendiamo dati a terze parti.</p>' +
                      '<a href="/privacy" class="cookie-banner-link">Leggi la Privacy Policy</a>' +
                    '</div>' +
                    '<div class="cookie-banner-actions">' +
                      '<button onclick="acceptCookies()" class="cookie-btn cookie-btn--accept">Accetta</button>' +
                      '<button onclick="declineCookies()" class="cookie-btn cookie-btn--decline">Solo necessari</button>' +
                    '</div>' +
                  '</div>' +
                '</div>';

            function injectBanner() {
                if (document.getElementById('cookie-banner')) return;
                var wrap = document.createElement('div');
                wrap.innerHTML = BANNER_HTML;
                document.body.appendChild(wrap.firstChild);
            }

            function removeBanner() {
                var b = document.getElementById('cookie-banner');
                if (b && b.parentNode) b.parentNode.removeChild(b);
            }

            // Espongo le funzioni globali (chiamate da onclick inline e da footer "Cookie")
            window.acceptCookies = function() {
                localStorage.setItem('cg_cookie_consent', 'accepted');
                removeBanner();
                if (typeof loadGA4 === 'function') loadGA4();
                if (typeof loadMetaPixel === 'function') loadMetaPixel();
                if (typeof loadClarity === 'function') loadClarity();
            };

            window.declineCookies = function() {
                localStorage.setItem('cg_cookie_consent', 'declined');
                removeBanner();
            };

            window.resetCookieConsent = function() {
                localStorage.removeItem('cg_cookie_consent');
                injectBanner();
            };

            // Iniezione iniziale: solo se non c'è consenso registrato.
            // Aspetta idle/timeout per stare COMPLETAMENTE fuori dalla finestra LCP.
            var consent = localStorage.getItem('cg_cookie_consent');
            if (!consent) {
                if ('requestIdleCallback' in window) {
                    requestIdleCallback(injectBanner, { timeout: 2500 });
                } else {
                    setTimeout(injectBanner, 1800);
                }
            }
        })();
    


        if ('serviceWorker' in navigator) {
            window.addEventListener('load', function() {
                navigator.serviceWorker.register('/sw.js', { scope: '/' })
                    .then(function(reg) {
                        // Quando viene trovata una nuova versione del SW,
                        // notifichiamo all'utente che c'è un aggiornamento.
                        // Il SW si attiva subito grazie a skipWaiting() interno.
                        reg.addEventListener('updatefound', function() {
                            var newSW = reg.installing;
                            if (!newSW) return;
                            newSW.addEventListener('statechange', function() {
                                if (newSW.state === 'activated' && navigator.serviceWorker.controller) {
                                    // SW nuovo attivo + esiste già un SW controller
                                    // = stiamo aggiornando, ricarica per vedere nuova versione
                                    // (commentato di default per non infastidire — abilita se serve)
                                    // window.location.reload();
                                }
                            });
                        });
                    })
                    .catch(function(err) {
                        console.warn('SW registration failed:', err);
                    });
            });
        }

        // ─── WEB PUSH: attivazione notifiche ────────────────────────────────
        // Esposta globalmente: la chiami da un pulsante "Attiva notifiche"
        // (es. sul form "Avvisami quando arriva"). Chiede il permesso, crea la
        // sottoscrizione col SW e la invia al server. Solo per chi è loggato.
        function _urlB64ToUint8(base64) {
            var pad = '='.repeat((4 - base64.length % 4) % 4);
            var b64 = (base64 + pad).replace(/-/g, '+').replace(/_/g, '/');
            var raw = atob(b64);
            var arr = new Uint8Array(raw.length);
            for (var i = 0; i < raw.length; i++) arr[i] = raw.charCodeAt(i);
            return arr;
        }

        window.cgEnablePush = async function () {
            try {
                if (cgIsOptedOut && cgIsOptedOut()) return { ok: false, reason: 'optout' };
                if (!('serviceWorker' in navigator) || !('PushManager' in window)) {
                    return { ok: false, reason: 'unsupported' };
                }
                // 1) permesso notifiche
                var perm = await Notification.requestPermission();
                if (perm !== 'granted') return { ok: false, reason: 'denied' };

                // 2) chiave pubblica VAPID dal server
                var keyRes = await fetch('/api/push/public-key');
                var keyData = await keyRes.json();
                if (!keyData || !keyData.key) return { ok: false, reason: 'no-key' };

                // 3) sottoscrizione col push manager
                var reg = await navigator.serviceWorker.ready;
                var sub = await reg.pushManager.getSubscription();
                if (!sub) {
                    sub = await reg.pushManager.subscribe({
                        userVisibleOnly: true,
                        applicationServerKey: _urlB64ToUint8(keyData.key),
                    });
                }

                // 4) invio al server
                var res = await fetch('/api/push/subscribe', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(sub),
                });
                if (!res.ok) return { ok: false, reason: 'save-failed' };
                return { ok: true };
            } catch (e) {
                console.warn('cgEnablePush error:', e);
                return { ok: false, reason: 'error' };
            }
        };
    


    (function() {
        'use strict';

        // ─── Configurazione ─────────────────────────────────────────────
        // Sistema "user-earned": il banner appare SOLO se l'utente ha
        // mostrato segnali di interesse. Tre trigger indipendenti, basta
        // che UNO sia attivo per mostrare il banner (più 30s sulla pagina).
        //
        //   T1 — Visite multiple in giorni diversi (≥2 giorni distinti)
        //   T2 — Sessione lunga (≥3 pageview + ≥2min sul sito)
        //   T3 — Utente loggato (ha già investito = già engaged)
        //
        // Pagine escluse (mai banner): /register, /login, /admin/*, /recommend
        // Desktop: mai mostrato (raramente installato da banner web).
        // Dismiss: permanente (localStorage), una volta chiuso non torna.

        var STORAGE_KEY      = 'cg_install_state_v1';
        var ENGAGEMENT_KEY   = 'cg_engagement';
        var VISIT_DAYS_KEY   = 'cg_visit_days_v1';   // lista giorni distinti
        var MIN_DELAY_SEC    = 30;     // 30s minimi sulla pagina prima del banner
        var MIN_TIME_SEC     = 120;    // T2: 2 min totali su sito (sessione)
        var MIN_PAGEVIEWS    = 3;      // T2: 3 pageview minime (sessione)
        var MIN_VISIT_DAYS   = 2;      // T1: 2+ giorni distinti di visita
        var INITIAL_DELAY_MS = 5000;   // primo check ritardato di 5s (LCP safe)
        var EXCLUDED_PATHS   = ['/register', '/login', '/recommend'];

        // ─── APP ANDROID SU GOOGLE PLAY (online dal 26/08/2026) ─────────
        // Su Android non proponiamo piu' la PWA ma l'app vera dello Store:
        // la PWA non passa dal Play Store, non compare nelle statistiche di
        // Play Console e non riceverebbe le notifiche push native.
        // iOS e desktop restano sulla PWA, dove un'app nativa non c'e'.
        var PLAY_PACKAGE  = 'com.cosaguardo.app';
        // referrer= viene letto dal Play Install Referrer: serve a distinguere
        // le installazioni che arrivano dal sito da quelle organiche o dagli
        // annunci. Utile soprattutto dopo l'integrazione dell'SDK Meta.
        var PLAY_STORE_URL = 'https://play.google.com/store/apps/details?id=' + PLAY_PACKAGE +
                             '&referrer=' + encodeURIComponent('utm_source=sito&utm_medium=banner&utm_campaign=install');

        // Soglie piu' basse su Android: l'offerta ora e' un'app vera sullo
        // Store, non un "aggiungi a schermata Home", quindi vale prima. Non
        // azzerate pero': chi arriva da Google deve comunque aver capito a
        // cosa serve il sito, altrimenti la fascia interrompe e basta.
        var ANDROID_MIN_DELAY_SEC = 15;    // invece di 30
        var ANDROID_MIN_TIME_SEC  = 60;    // invece di 120
        var ANDROID_MIN_PAGEVIEWS = 2;     // invece di 3

        // Flag utente loggato (da Jinja, valutato server-side a render time)
        var IS_LOGGED_IN = (window.__CG_LOGGED_IN === true);

        // ─── Detection device/browser ───────────────────────────────────
        function isStandalone() {
            return (window.matchMedia && window.matchMedia('(display-mode: standalone)').matches)
                || window.navigator.standalone === true;
        }
        function isIOS() {
            // Include iPad moderni che fingono di essere Mac (iPadOS 13+)
            return /iPad|iPhone|iPod/.test(navigator.userAgent)
                || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
        }
        function isSafari() {
            var ua = navigator.userAgent;
            return /Safari/.test(ua) && !/Chrome|CriOS|FxiOS|EdgiOS/.test(ua);
        }
        function isChromeIOS() {
            return /CriOS/.test(navigator.userAgent);
        }
        function isAndroid() {
            return /Android/.test(navigator.userAgent);
        }
        function isTwa() {
            // App Android (wrapper PWABuilder = Trusted Web Activity): NON
            // deve mai vedere la fascia, sta gia' usando l'app.
            // Il segnale documentato e' document.referrer = "android-app://".
            // ⚠️ Compare SOLO alla prima navigazione della sessione: dopo un
            // click interno il referrer diventa la pagina precedente. Per
            // questo lo si memorizza in sessionStorage appena lo si vede.
            try {
                if (sessionStorage.getItem('cg_is_twa') === '1') return true;
                var ref = document.referrer || '';
                if (ref.indexOf('android-app://') === 0) {
                    sessionStorage.setItem('cg_is_twa', '1');
                    return true;
                }
                // Secondo segnale, disponibile dopo la ricompilazione di
                // settembre che aggiunge ?src=app in start_url.
                if (window.location.search.indexOf('src=app') !== -1) {
                    sessionStorage.setItem('cg_is_twa', '1');
                    return true;
                }
            } catch (e) {}
            return false;
        }
        function usePlayStore() {
            // Android fuori dall'app e fuori dai browser interni delle app
            // social (da li' il Play Store si apre male o per niente).
            return isAndroid() && !isTwa() && !isInAppBrowser();
        }
        function isInAppBrowser() {
            // Webview interni delle app social (dove atterra il traffico da
            // annunci): NON possono installare PWA né "Aggiungi a Home".
            // Vanno indirizzati ad aprire in Safari/Chrome vero.
            var ua = navigator.userAgent || '';
            return /FBAN|FBAV|FB_IAB|Instagram|Messenger|Line\/|Twitter|TikTok|Pinterest|Snapchat|LinkedInApp|WhatsApp|GSA\//i.test(ua);
        }
        function isMobile() {
            // Pointer coarse = touch device. Width sotto 1024 = mobile/tablet.
            // Più affidabile dell'UA sniffing (futureproof + iPad iOS13+).
            try {
                return window.matchMedia('(pointer: coarse)').matches
                    && window.matchMedia('(max-width: 1024px)').matches;
            } catch (e) {
                // Fallback su UA se matchMedia non disponibile
                return isIOS() || isAndroid();
            }
        }
        function isExcludedPath() {
            var p = window.location.pathname || '/';
            // Match esatto su /register, /login, /recommend
            if (EXCLUDED_PATHS.indexOf(p) !== -1) return true;
            // Match prefisso /admin/*
            if (p.indexOf('/admin') === 0) return true;
            return false;
        }

        // ─── State management ───────────────────────────────────────────
        function getState() {
            try {
                return JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}');
            } catch (e) { return {}; }
        }
        function setState(s) {
            try { localStorage.setItem(STORAGE_KEY, JSON.stringify(s)); }
            catch (e) {}
        }

        // ─── T1: tracking giorni distinti di visita ─────────────────────
        // Salva il giorno corrente (YYYY-MM-DD) in localStorage come array.
        // Trigger attivo se ≥MIN_VISIT_DAYS giorni distinti.
        function todayKey() {
            var d = new Date();
            var y = d.getFullYear();
            var m = String(d.getMonth() + 1).padStart(2, '0');
            var day = String(d.getDate()).padStart(2, '0');
            return y + '-' + m + '-' + day;
        }
        function recordVisitDay() {
            try {
                var raw = localStorage.getItem(VISIT_DAYS_KEY) || '[]';
                var days = JSON.parse(raw);
                if (!Array.isArray(days)) days = [];
                var t = todayKey();
                if (days.indexOf(t) === -1) {
                    days.push(t);
                    // Mantiene solo gli ultimi 60 giorni per non gonfiare localStorage
                    if (days.length > 60) days = days.slice(-60);
                    localStorage.setItem(VISIT_DAYS_KEY, JSON.stringify(days));
                }
            } catch (_) {}
        }
        function getDistinctVisitDays() {
            try {
                var raw = localStorage.getItem(VISIT_DAYS_KEY) || '[]';
                var days = JSON.parse(raw);
                return Array.isArray(days) ? days.length : 0;
            } catch (_) { return 0; }
        }

        // ─── T2: engagement (sessione lunga) ────────────────────────────
        function getEngagement() {
            try {
                var raw = sessionStorage.getItem(ENGAGEMENT_KEY) || '{}';
                var e = JSON.parse(raw);
                return { timeSec: e.timeSec || 0, pageviews: e.pageviews || 0 };
            } catch (err) { return { timeSec: 0, pageviews: 0 }; }
        }
        function bumpPageview() {
            var e = getEngagement();
            e.pageviews = (e.pageviews || 0) + 1;
            try { sessionStorage.setItem(ENGAGEMENT_KEY, JSON.stringify(e)); } catch (_) {}
        }
        function tickTime() {
            // Conta solo se la tab è attiva
            if (document.visibilityState !== 'visible') return;
            var e = getEngagement();
            e.timeSec = (e.timeSec || 0) + 1;
            try { sessionStorage.setItem(ENGAGEMENT_KEY, JSON.stringify(e)); } catch (_) {}

            // Trigger check ogni 10 secondi (non spam)
            if (e.timeSec % 10 === 0) maybeShowBanner();
        }

        // ─── Valutazione: deve mostrare il banner? ──────────────────────
        function shouldShow() {
            // Esclusioni "hard" (mai mostrare in nessun caso)
            if (isStandalone()) return false;
            if (isTwa()) return false;               // gia' dentro l'app Android
            if (!isMobile()) return false;           // solo mobile
            if (isExcludedPath()) return false;      // pagine escluse
            if (isIOS() && isChromeIOS()) return false; // Chrome iOS non può installare

            var s = getState();
            if (s.installed) return false;
            if (s.dismissed) return false;           // dismiss permanente

            // Tempo minimo SU QUESTA pagina — sempre richiesto, piu' basso
            // su Android dove l'offerta e' l'app dello Store.
            var play = usePlayStore();
            var minDelay = play ? ANDROID_MIN_DELAY_SEC : MIN_DELAY_SEC;
            var minTime  = play ? ANDROID_MIN_TIME_SEC  : MIN_TIME_SEC;
            var minViews = play ? ANDROID_MIN_PAGEVIEWS : MIN_PAGEVIEWS;

            var eng = getEngagement();
            if (eng.timeSec < minDelay) return false;

            // ── Trigger: basta UNO dei 3 ─────────────────────────────────
            // T3: loggato
            if (IS_LOGGED_IN) return true;

            // T1: ≥2 giorni distinti
            if (getDistinctVisitDays() >= MIN_VISIT_DAYS) return true;

            // T2: sessione lunga (soglie ridotte su Android)
            if (eng.pageviews >= minViews && eng.timeSec >= minTime) return true;

            return false;
        }

        // ─── GA4 tracking (solo se consenso accettato) ──────────────────
        function track(eventName, params) {
            try {
                if (localStorage.getItem('cg_optout') === '1') return;
                if (typeof gtag === 'function' &&
                    localStorage.getItem('cg_cookie_consent') === 'accepted') {
                    gtag('event', eventName, Object.assign({
                        event_category: 'pwa_install'
                    }, params || {}));
                }
            } catch (_) {}
        }

        // ─── beforeinstallprompt (Android Chrome / Desktop Chrome/Edge) ─
        var deferredPrompt = null;
        window.addEventListener('beforeinstallprompt', function(e) {
            // Blocca il prompt nativo automatico — lo gestiamo noi
            e.preventDefault();
            deferredPrompt = e;
            track('install_prompt_available', { platform: 'native' });
        });

        // Quando l'app viene effettivamente installata
        window.addEventListener('appinstalled', function() {
            var s = getState();
            s.installed = true;
            setState(s);
            hideBanner();
            track('app_installed', {});
            deferredPrompt = null;
        });

        // ─── Dismiss permanente ─────────────────────────────────────────
        function dismissBanner(reason) {
            var s = getState();
            s.dismissed = true;
            s.dismissedAt = Date.now();
            s.dismissReason = reason || 'manual';
            setState(s);
            hideBanner();
            hideIosGuide();
            track('install_banner_dismissed', { reason: reason });
        }

        // ─── UI: show/hide ──────────────────────────────────────────────
        var bannerShown = false;
        function showBanner() {
            if (bannerShown) return;
            // Non sovrapporsi al cookie banner (ora iniettato dinamicamente:
            // se l'elemento esiste nel DOM, è perché sta venendo mostrato).
            if (document.getElementById('cookie-banner')) {
                return;
            }
            var b = document.getElementById('install-banner');
            if (!b) return;

            // Su Android il testo cambia: si scarica un'app vera, non si
            // aggiunge una scorciatoia alla schermata Home.
            if (usePlayStore()) {
                var strong = b.querySelector('.install-banner-text strong');
                var span   = b.querySelector('.install-banner-text span');
                var azione = document.getElementById('install-btn-action');
                if (strong) strong.textContent = '📲 Scarica CosaGuardo su Google Play';
                if (span)   span.textContent   = 'gratis, senza pubblicità';
                if (azione) azione.textContent = 'Scarica';
            }

            b.classList.add('show');
            bannerShown = true;

            // Quale trigger ha attivato la mostra?
            var trigger = 'unknown';
            var eng = getEngagement();
            if (IS_LOGGED_IN) trigger = 'logged_in';
            else if (getDistinctVisitDays() >= MIN_VISIT_DAYS) trigger = 'multi_day_visit';
            else if (eng.pageviews >= MIN_PAGEVIEWS && eng.timeSec >= MIN_TIME_SEC) trigger = 'long_session';

            track('install_banner_shown', {
                platform: isIOS() ? 'ios' : (isAndroid() ? 'android' : 'desktop'),
                trigger: trigger,
                visit_days: getDistinctVisitDays(),
                pageviews: eng.pageviews,
                time_sec: eng.timeSec
            });
        }
        function hideBanner() {
            var b = document.getElementById('install-banner');
            if (b) b.classList.remove('show');
            bannerShown = false;
        }
        // Costruisce al volo il contenuto dell'overlay guida e lo mostra.
        // Riusa #ios-install-guide come contenitore (il markup statico in
        // base.html fa da fallback no-JS e viene sovrascritto qui).
        function renderGuideOverlay(title, stepsHtml, trackName) {
            var guide = document.getElementById('ios-install-guide');
            if (!guide) return;
            var inner = guide.querySelector('.ios-guide-inner');
            if (!inner) return;
            inner.innerHTML =
                '<button class="ios-guide-close" id="ios-guide-close" aria-label="Chiudi">\u00d7</button>' +
                '<h3 class="ios-guide-title">' + title + '</h3>' +
                stepsHtml;
            var newClose = inner.querySelector('#ios-guide-close');
            if (newClose) {
                newClose.addEventListener('click', function() {
                    hideIosGuide();
                    dismissBanner('ios_guide_close');
                });
            }
            guide.classList.add('show');
            if (trackName) track(trackName, {});
        }
        // iOS Safari "vero": istruzioni Aggiungi alla schermata Home
        // (etichetta verificata su iOS attuale = "Aggiungi alla schermata Home").
        function showIosGuide() {
            renderGuideOverlay(
                'Installa CosaGuardo sul tuo iPhone',
                '<div class="ios-guide-step"><div class="ios-guide-step-num">1</div>' +
                '<div>Tocca il bottone <span class="ios-guide-share">\u2b06</span> <strong>Condividi</strong> (il quadrato con la freccia, in fondo allo schermo)</div></div>' +
                '<div class="ios-guide-step"><div class="ios-guide-step-num">2</div>' +
                '<div>Scorri il menu verso il basso e tocca <strong>"Aggiungi alla schermata Home"</strong></div></div>' +
                '<div class="ios-guide-step"><div class="ios-guide-step-num">3</div>' +
                '<div>Tocca <strong>"Aggiungi"</strong> in alto a destra per confermare</div></div>',
                'ios_guide_shown'
            );
        }
        // iOS dentro browser in-app (Instagram/Facebook/ecc.) o Chrome iOS:
        // l'install non è possibile lì → guidalo ad aprire in Safari.
        function showOpenInSafariGuide() {
            hideBanner();
            renderGuideOverlay(
                'Apri in Safari per installare',
                '<p style="font-size:0.85rem;color:var(--muted);margin:0 0 14px;text-align:center;">' +
                'Stai navigando in un browser interno (es. Instagram o Facebook). Su iPhone l\'app si installa solo da <strong>Safari</strong>.</p>' +
                '<div class="ios-guide-step"><div class="ios-guide-step-num">1</div>' +
                '<div>Tocca i <strong>tre puntini</strong> (o l\'icona di condivisione) in alto a destra</div></div>' +
                '<div class="ios-guide-step"><div class="ios-guide-step-num">2</div>' +
                '<div>Tocca <strong>"Apri in Safari"</strong></div></div>' +
                '<div class="ios-guide-step"><div class="ios-guide-step-num">3</div>' +
                '<div>In Safari: <strong>Condividi</strong> \u2192 <strong>"Aggiungi alla schermata Home"</strong></div></div>',
                'ios_open_in_safari_shown'
            );
        }
        function hideIosGuide() {
            var g = document.getElementById('ios-install-guide');
            if (g) g.classList.remove('show');
        }

        function maybeShowBanner() {
            if (bannerShown) return;
            if (!shouldShow()) return;
            showBanner();
        }

        // ─── Click handlers ─────────────────────────────────────────────
        document.addEventListener('DOMContentLoaded', function() {
            // ── Trigger conversioni post-redirect ──────────────────────────
            // Il server redirecta su /profilo?registered=1 (registrazione) o
            // ?logged_in=1 (login). Spariamo l'evento UNA SOLA VOLTA su
            // GA4 + Meta Pixel, poi puliamo la URL così il reload non ritrigghera.
            try {
                var urlParams = new URLSearchParams(window.location.search);
                var convFired = false;
                if (urlParams.get('registered') === '1') {
                    if (typeof window.cgTrack === 'function') {
                        // cgTrack mappa sign_up → CompleteRegistration su Pixel
                        window.cgTrack('sign_up', { method: 'email' });
                    }
                    urlParams.delete('registered');
                    convFired = true;
                }
                if (urlParams.get('logged_in') === '1') {
                    if (typeof window.cgTrack === 'function') {
                        // cgTrack mappa login → Lead su Pixel
                        window.cgTrack('login', { method: 'email' });
                    }
                    urlParams.delete('logged_in');
                    convFired = true;
                }
                if (convFired) {
                    var newUrl = window.location.pathname +
                        (urlParams.toString() ? '?' + urlParams.toString() : '') +
                        window.location.hash;
                    history.replaceState(null, '', newUrl);
                }
            } catch (_) {}

            var btnInstall = document.getElementById('install-btn-action');
            var btnClose   = document.getElementById('install-btn-close');
            var btnIosX    = document.getElementById('ios-guide-close');

            if (btnInstall) {
                btnInstall.addEventListener('click', function() {
                    track('install_banner_clicked', {});

                    // iOS: l'install passa sempre per Safari.
                    if (isIOS()) {
                        hideBanner();
                        if (isSafari() && !isInAppBrowser()) {
                            showIosGuide();            // Safari vero → Aggiungi a Home
                        } else {
                            showOpenInSafariGuide();   // in-app / Chrome iOS → apri in Safari
                        }
                        return;
                    }

                    // Android fuori dall'app → Play Store, PRIMA di qualunque
                    // logica PWA: anche se deferredPrompt fosse disponibile,
                    // l'app vera e' l'offerta migliore.
                    if (usePlayStore()) {
                        track('play_store_redirect', { source: 'install_banner' });
                        // Il click sullo Store chiude la fascia in modo
                        // definitivo, come gia' avviene per il rifiuto del
                        // prompt nativo: l'offerta e' stata fatta e accolta.
                        // ⚠️ L'evento appinstalled NON scatta per le
                        // installazioni dal Play Store, quindi non si puo'
                        // sapere da qui se l'installazione e' avvenuta: il
                        // dato sta in Play Console.
                        dismissBanner('store_click');
                        window.open(PLAY_STORE_URL, '_blank', 'noopener');
                        return;
                    }

                    // Android/Desktop con prompt nativo disponibile (caso ideale)
                    if (deferredPrompt) {
                        deferredPrompt.prompt();
                        deferredPrompt.userChoice.then(function(choice) {
                            track('install_prompt_outcome', { outcome: choice.outcome });
                            if (choice.outcome === 'accepted') {
                                // L'evento appinstalled aggiornerà lo state
                                hideBanner();
                            } else {
                                // Rifiutato → tratta come dismiss
                                dismissBanner('rejected');
                            }
                            deferredPrompt = null;
                        });
                        return;
                    }

                    // Niente prompt disponibile su Chromium-based browser:
                    // di solito significa modalità incognito o requisiti non soddisfatti.
                    // Dirigliamolo alla pagina /installa con istruzioni manuali.
                    if (isAndroid()) {
                        // Android Chrome senza prompt → istruzione menu 3 puntini
                        showFallbackHint('android');
                    } else {
                        // Desktop senza prompt → suggerisci icona barra indirizzi o /installa
                        showFallbackHint('desktop');
                    }
                });
            }

            if (btnClose) {
                btnClose.addEventListener('click', function() {
                    dismissBanner('manual_close');
                });
            }

            if (btnIosX) {
                btnIosX.addEventListener('click', function() {
                    hideIosGuide();
                    dismissBanner('ios_guide_close');
                });
            }

            // ─── Inizia tracking engagement ─────────────────────────────
            bumpPageview();
            recordVisitDay();    // T1: registra il giorno corrente per multi-day visit trigger
            // setInterval(tickTime) ritardato di 5s: durante quei 5s la pagina
            // resta CPU-idle, permettendo a Lighthouse di chiudere la finestra
            // di misurazione LCP. Senza questo ritardo, il tick ogni 1s
            // impediva i 5s consecutivi di idle che Lighthouse esige.
            setTimeout(function() {
                setInterval(tickTime, 1000);
            }, 5000);

            // Primo check dopo INITIAL_DELAY_MS, ma minimo 5s per stessa ragione
            setTimeout(maybeShowBanner, Math.max(INITIAL_DELAY_MS, 5000));
        });

        // ─── Hint quando manca deferredPrompt (es. incognito) ───────────
        function showFallbackHint(platform) {
            hideBanner();
            // Riusa l'overlay iOS riadattando il contenuto al volo
            var guide = document.getElementById('ios-install-guide');
            if (!guide) return;
            var inner = guide.querySelector('.ios-guide-inner');
            if (!inner) return;

            var title = 'Per installare CosaGuardo';
            var stepsHtml = '';

            if (platform === 'android') {
                stepsHtml =
                    '<div class="ios-guide-step"><div class="ios-guide-step-num">1</div>' +
                    '<div>Tocca i <strong>3 puntini</strong> in alto a destra di Chrome</div></div>' +
                    '<div class="ios-guide-step"><div class="ios-guide-step-num">2</div>' +
                    '<div>Tocca <strong>"Installa app"</strong> o <strong>"Aggiungi alla schermata Home"</strong></div></div>' +
                    '<div class="ios-guide-step"><div class="ios-guide-step-num">3</div>' +
                    '<div>Conferma con <strong>"Installa"</strong></div></div>' +
                    '<p style="font-size:0.78rem;color:var(--muted);margin-top:14px;text-align:center;">' +
                    'Suggerimento: in modalità incognito l\'installazione è disabilitata da Chrome.</p>';
            } else {
                stepsHtml =
                    '<div class="ios-guide-step"><div class="ios-guide-step-num">1</div>' +
                    '<div>Cerca l\'icona <strong>☐⬇</strong> nella barra degli indirizzi (a destra dell\'URL)</div></div>' +
                    '<div class="ios-guide-step"><div class="ios-guide-step-num">2</div>' +
                    '<div>Click sull\'icona, poi <strong>"Installa"</strong></div></div>' +
                    '<p style="font-size:0.78rem;color:var(--muted);margin-top:14px;text-align:center;">' +
                    'Se non vedi l\'icona, prova a chiudere la modalità incognito o usa Chrome/Edge in modalità normale.</p>';
            }

            inner.innerHTML =
                '<button class="ios-guide-close" id="ios-guide-close" aria-label="Chiudi">×</button>' +
                '<h3 class="ios-guide-title">' + title + '</h3>' +
                stepsHtml;

            // Riattacca il listener al nuovo bottone close
            var newClose = inner.querySelector('#ios-guide-close');
            if (newClose) {
                newClose.addEventListener('click', function() {
                    hideIosGuide();
                    dismissBanner('fallback_hint_close');
                });
            }
            guide.classList.add('show');
            track('install_fallback_shown', { platform: platform });
        }

        // Esponi una funzione globale per forzare il banner da pagina /installa
        window.cgShowInstallPrompt = function() {
            // Stessa regola della fascia: su Android si va allo Store.
            if (usePlayStore()) {
                track('play_store_redirect', { source: 'installa_page' });
                window.open(PLAY_STORE_URL, '_blank', 'noopener');
                return;
            }
            if (isStandalone()) {
                alert('CosaGuardo è già installata!');
                return;
            }
            if (isIOS()) {
                if (isSafari() && !isInAppBrowser()) {
                    showIosGuide();
                } else {
                    showOpenInSafariGuide();
                }
                return;
            }
            if (deferredPrompt) {
                deferredPrompt.prompt();
                deferredPrompt.userChoice.then(function(choice) {
                    track('install_prompt_outcome', { outcome: choice.outcome, source: 'install_page' });
                    deferredPrompt = null;
                });
                return;
            }
            // Nessun prompt disponibile → guida fallback
            if (isAndroid()) {
                showFallbackHint('android');
            } else {
                showFallbackHint('desktop');
            }
        };
    })();
    


    (function() {
        var trigger = document.getElementById('qs-trigger');
        var overlay = document.getElementById('qs-overlay');
        var input   = document.getElementById('qs-input');
        var closeBtn= document.getElementById('qs-close');
        var results = document.getElementById('qs-results');
        var tabs    = document.querySelectorAll('.qs-tab');
        if (!trigger || !overlay || !input) return;

        var currentTab = 'all';
        var debounceTimer = null;
        var lastQuery = '';
        var lastReqId = 0;

        function openOverlay() {
            overlay.classList.add('is-open');
            overlay.setAttribute('aria-hidden', 'false');
            document.body.style.overflow = 'hidden';
            setTimeout(function() { input.focus(); }, 30);
        }
        function closeOverlay() {
            overlay.classList.remove('is-open');
            overlay.setAttribute('aria-hidden', 'true');
            document.body.style.overflow = '';
            input.value = '';
            results.innerHTML = '';
            lastQuery = '';
        }

        trigger.addEventListener('click', openOverlay);
        closeBtn.addEventListener('click', closeOverlay);
        // Click sul backdrop (overlay esterno al panel) chiude
        overlay.addEventListener('click', function(e) {
            if (e.target === overlay) closeOverlay();
        });
        // ESC chiude
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape' && overlay.classList.contains('is-open')) closeOverlay();
        });

        // Tabs
        tabs.forEach(function(t) {
            t.addEventListener('click', function() {
                tabs.forEach(function(x) { x.classList.remove('is-active'); });
                t.classList.add('is-active');
                currentTab = t.getAttribute('data-qs-tab');
                if (input.value.trim().length >= 2) doSearch(input.value.trim());
            });
        });

        input.addEventListener('input', function() {
            clearTimeout(debounceTimer);
            var q = input.value.trim();
            if (q.length < 2) {
                results.innerHTML = '';
                lastQuery = '';
                return;
            }
            if (q === lastQuery) return;
            debounceTimer = setTimeout(function() { doSearch(q); }, 180);
        });

        function doSearch(q) {
            lastQuery = q;
            var reqId = ++lastReqId;
            results.innerHTML = '<div class="qs-loading">Cerco…</div>';

            var fetches = [];
            if (currentTab === 'all' || currentTab === 'movie') {
                fetches.push(
                    fetch('/search-fast?q=' + encodeURIComponent(q) + '&content_type=movie')
                        .then(function(r) { return r.ok ? r.json() : []; })
                        .then(function(arr) { return arr.map(function(x) { x._kind = 'movie'; return x; }); })
                        .catch(function() { return []; })
                );
            }
            if (currentTab === 'all' || currentTab === 'tv') {
                fetches.push(
                    fetch('/search-fast?q=' + encodeURIComponent(q) + '&content_type=tv')
                        .then(function(r) { return r.ok ? r.json() : []; })
                        .then(function(arr) { return arr.map(function(x) { x._kind = 'tv'; return x; }); })
                        .catch(function() { return []; })
                );
            }

            Promise.all(fetches).then(function(arrs) {
                if (reqId !== lastReqId) return;  // risposta vecchia, ignora
                var merged = [].concat.apply([], arrs);
                // Riordina per _score discendente: film e serie si mescolano
                // in base a popolarità+match (no più "tutti film, poi tutte serie").
                merged.sort(function(a, b) {
                    return (b._score || 0) - (a._score || 0);
                });
                merged = merged.slice(0, 12);
                renderResults(merged, q);
            });
        }

        function escHtml(s) {
            return String(s || '').replace(/[&<>"']/g, function(c) {
                return ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c];
            });
        }

        function renderResults(items, query) {
            if (!items || !items.length) {
                results.innerHTML = '<div class="qs-empty">Nessun risultato per "' + escHtml(query) + '"</div>';
                return;
            }
            var html = items.map(function(it) {
                var displayTitle = it.display_title || it.title || '';
                var rawTitle     = it.title || '';
                var kind = it._kind === 'tv' ? 'tv' : 'movie';
                var label = kind === 'tv' ? 'SERIE TV' : 'FILM';
                // TV: campo "id" è tmdb. Movie: campo "tmdb_id" (potrebbe essere null se solo DB locale).
                var directId = (kind === 'tv') ? (it.id || it.tv_id || '')
                                               : (it.tmdb_id || '');
                return '<a class="qs-result" ' +
                       'data-kind="' + kind + '" ' +
                       'data-title="' + escHtml(rawTitle) + '" ' +
                       'data-direct-id="' + directId + '">' +
                       '<span class="qs-result-type ' + kind + '">' + label + '</span>' +
                       '<span class="qs-result-title">' + escHtml(displayTitle) + '</span>' +
                       '</a>';
            }).join('');
            results.innerHTML = html;
            results.querySelectorAll('.qs-result').forEach(function(el) {
                el.addEventListener('click', onResultClick);
            });
        }

        function onResultClick(e) {
            e.preventDefault();
            var el = e.currentTarget;
            var kind = el.getAttribute('data-kind');
            var directId = el.getAttribute('data-direct-id');
            var title = el.getAttribute('data-title');
            // Feedback visivo
            el.style.opacity = '0.5';
            el.style.pointerEvents = 'none';

            // Se abbiamo gia il tmdb_id, naviga direttamente (no round-trip extra)
            if (directId) {
                var path = (kind === 'tv') ? '/serie/' : '/film/';
                window.location.href = path + encodeURIComponent(directId);
                return;
            }
            // Fallback: tmdb_id non disponibile (titolo da DB locale MovieLens senza match TMDb).
            // Risolvi via endpoint server.
            fetch('/tmdb-id?title=' + encodeURIComponent(title) + '&content_type=' + kind)
                .then(function(r) { return r.ok ? r.json() : { tmdb_id: null }; })
                .then(function(data) {
                    if (data && data.tmdb_id) {
                        var path = (kind === 'tv') ? '/serie/' : '/film/';
                        window.location.href = path + encodeURIComponent(data.tmdb_id);
                    } else {
                        el.style.opacity = '1';
                        el.style.pointerEvents = '';
                        el.querySelector('.qs-result-title').textContent =
                            'Impossibile aprire la scheda. Riprova.';
                    }
                })
                .catch(function() {
                    el.style.opacity = '1';
                    el.style.pointerEvents = '';
                });
        }
    })();
    


    (function () {
        var trigger  = document.getElementById('cg-menu-trigger');
        var drawer   = document.getElementById('cg-drawer');
        var backdrop = document.getElementById('cg-drawer-backdrop');
        var closeBtn = document.getElementById('cg-drawer-close');
        if (!trigger || !drawer || !backdrop) return;

        var lastFocus = null;

        function openDrawer() {
            lastFocus = document.activeElement;
            backdrop.hidden = false;
            // forza reflow così la transizione di opacità parte
            void backdrop.offsetWidth;
            drawer.classList.add('cg-open');
            backdrop.classList.add('cg-open');
            drawer.setAttribute('aria-hidden', 'false');
            trigger.setAttribute('aria-expanded', 'true');
            document.body.style.overflow = 'hidden';
            if (closeBtn) closeBtn.focus();
            document.addEventListener('keydown', onKeydown);
        }

        function closeDrawer() {
            drawer.classList.remove('cg-open');
            backdrop.classList.remove('cg-open');
            drawer.setAttribute('aria-hidden', 'true');
            trigger.setAttribute('aria-expanded', 'false');
            document.body.style.overflow = '';
            document.removeEventListener('keydown', onKeydown);
            // nascondi il backdrop a fine transizione (evita click-blocking invisibile)
            setTimeout(function () {
                if (!drawer.classList.contains('cg-open')) backdrop.hidden = true;
            }, 260);
            if (lastFocus && typeof lastFocus.focus === 'function') lastFocus.focus();
        }

        function onKeydown(e) {
            if (e.key === 'Escape' || e.key === 'Esc') closeDrawer();
        }

        trigger.addEventListener('click', openDrawer);
        backdrop.addEventListener('click', closeDrawer);
        if (closeBtn) closeBtn.addEventListener('click', closeDrawer);

        // Click su un link: chiudi subito (la navigazione parte comunque).
        drawer.querySelectorAll('a.cg-drawer-link').forEach(function (a) {
            a.addEventListener('click', function () { closeDrawer(); });
        });
    })();
    
