# SEZIONE 39 — App iOS: funzioni native complete (30/09 – 07/10)

## RIEPILOGO
L'app iOS passa da "tre schede che replicano il sito" a prodotto completo:
ricerca diretta, autenticazione (email + Apple + Google), raccolta personale,
affiliazione nelle schede, quarta scheda Novità. In più un giro di
ottimizzazione e la chiusura definitiva dell'estensione di condivisione.

---

## 39.1 — Affiliazione nelle schede

### Server
- **`_api_piattaforme()`** in `main.py`: `get_detail_*` restituisce i provider
  come **dizionario** (`flatrate`/`rent`/`buy`), non lista — l'API lo passava
  così com'è e l'app non sapeva leggerlo. Ora appiattito con `accesso`
  esplicito, abbonamento per primo, senza doppioni.
- Chiave `piattaforme` aggiunta; `providers` **mantenuta** per compatibilità v1.
- **`_link_diretto_piattaforma()`** in `recommendation_api.py`: quando non c'è
  affiliazione si cadeva sul link JustWatch di TMDb — l'utente toccava Netflix
  e finiva su themoviedb.org in inglese.

| | |
|---|---|
| Ricerca precompilata | Netflix, Paramount+, Apple TV+ (verificate a mano) |
| Home piattaforma | Disney+, NOW, Rakuten, Chili, TIMVISION, Canal+ |
| Affiliato | Prime Video, NOW (invariati) |
| Non in tabella | JustWatch |

⚠️ **Disney+ NON va in ricerca**: `/it-it/search?q=` risponde "pagina non
trovata". Commento lasciato nel codice perché nessuno lo rimetta.

⚠️ Vale anche per il **sito**: le schede web avevano lo stesso rimando.

### App
- Sezione **"📺 Dove vederlo"** sopra la trama (è la domanda per cui si apre
  una scheda).
- Informativa affiliazione solo se almeno un link è affiliato — condizione
  del programma Amazon e richiesta App Store.
- Pulsante senza link: visibile ma spento.

### [DECISION] Amazon Associates
- URL app Android dichiarato nel profilo: `play.google.com/store/apps/details?id=com.cosaguardo.app`
- ⚠️ **URL app iOS da dichiarare dopo la pubblicazione** — facile dimenticarlo.

---

## 39.2 — Autenticazione (tre consegne)

### Token
- Tabella **`api_tokens`** (token, user_id, dispositivo, created_at, last_used).
- ⚠️ **Token opaco, NON JWT**: un JWT non si revoca senza una lista di revoche,
  cioè senza il database che si voleva evitare. La revoca serve davvero
  (cancellazione account, uscita da un dispositivo).
- Non scade; `last_used` aggiornato a ogni chiamata per pulizie future.
- Lato app nel **Keychain**, non in UserDefaults (finisce nei backup in chiaro).

### Endpoint
`/api/v1/auth/`: `registrazione`, `login`, `apple`, `completa-profilo`, `io`,
`esci`, `account` (DELETE).

- ⚠️ **Validazioni identiche a `/register`**: un'app più permissiva sarebbe il
  percorso per aggirare età e consensi.
- ⚠️ **Login indistinguibile**: stessa risposta se l'email esiste o no.
- Registrazioni da app → `signup_source` = `ios` / `ios-apple`.

### Sign in with Apple
- Verifica firma contro chiavi pubbliche Apple (cache 1h), controllo
  **audience** ed **emittente**.
- Nuova colonna **`apple_user_id`**: l'email può essere un indirizzo di inoltro
  privato e cambia se l'utente revoca la condivisione.
- Chi era già registrato con la stessa email viene **collegato**, non duplicato.
- Dipendenza: **PyJWT** (`cryptography` arriva già con pywebpush).
- Variabile Render consigliata: `APPLE_BUNDLE_ID`.

⚠️ **Capability solo su Release**: `CosaGuardoRelease.entitlements` era legato
alla sola configurazione Release, mentre Cmd+R usa Debug → errore 1000.
Vale per qualunque capability futura: aggiungerla con **All** selezionato.

### Google
- Riusa il flusso OAuth del sito con `ASWebAuthenticationSession` — niente SDK.
- ⚠️ **La provenienza viaggia nello state** (prefisso `app-`), non nel cookie:
  ASWebAuthenticationSession isola i cookie e la sessione può non sopravvivere
  al giro su Google.
- Le **sei** uscite in errore del callback passano da `_uscita_oauth_errore`.
- `prefersEphemeralWebBrowserSession = true` → "usa un altro account" funziona.
- Guardia contro il doppio tocco (il sistema la fa per Apple, non qui).

### Completamento profilo
- Pannello **nativo**, non più link al sito.
- Raccoglie **gli stessi campi della pagina web**: nome, cognome, data
  nascita, preferenza contenuto, piattaforme, tre consensi.
- ⚠️ Preferenze salvate **fuori dal try dell'anagrafica**: se fallissero,
  l'account resterebbe senza consensi pur avendoli dati.
- Si apre **da solo** dopo un accesso social incompleto, **una volta sola**
  (`daCompletareSubito`, spento prima di aprire).
- ⚠️ `onChange(of:initial: true)` indispensabile: il segnale è già acceso
  quando la vista compare, e onChange non scatta sul valore iniziale.
- ⚠️ `ripristina()` NON lo accende: all'avvio non deve ricomparire.

---

## 39.3 — Raccolta personale

- Endpoint: `GET /api/v1/raccolta`, `POST /api/v1/raccolta`,
  `GET /api/v1/raccolta/stato`, `GET /api/v1/consigli-personali`.
- ⚠️ `user_title_state` è indicizzata **per titolo**, non per tmdb_id: l'app
  manda titolo + tipo. In lettura si riusa `_enrich_titles_with_posters`.
- ⚠️ **Per togliere un preferito si passa stringa vuota, non None**:
  `upsert_title_state` tratta None come "non toccare" e il preferito restava.
- Un endpoint solo per segnare: i campi non inviati restano invariati.

### Stato condiviso
- **`RaccoltaCondivisa`** (@Observable) al posto di NotificationCenter.
- ⚠️ In un TabView la vista di una scheda non selezionata **può non essere
  viva**: la notifica partiva e non la riceveva nessuno.
- Contatore di versione: la lista si ricarica solo se è cambiato qualcosa.
- Azzerata all'uscita e alla cancellazione account.

### [BUG RISOLTO] get_title_state non importata
- L'endpoint salvava e poi falliva rileggendo → **500 dopo il salvataggio**.
- L'app riportava indietro i pulsanti su un dato che il server aveva scritto.
- ⚠️ Controllo delle occorrenze ≠ controllo degli import: verificare
  sull'albero sintattico.

---

## 39.4 — Ricerca diretta

- **Lente in alto a destra** nella home; "Azzera" spostato a sinistra (la
  lente è permanente, Azzera compare e sparisce).
- Riusa `/search-fast`, nessun endpoint nuovo.
- ⚠️ **`title` è il titolo ORIGINALE, `display_title` è l'italiano.** Il server
  fa questa distinzione apposta: il motore confronta sui titoli originali.
  - Ricerca → `display_title` (l'utente legge l'italiano)
  - Menu autocomplete dei Consigli → **resta `title`**: quel testo va al motore.

---

## 39.5 — Novità (quarta scheda)

- `/api/v1/novita`: una striscia per piattaforma, riusa `get_scopri_results`.
- Riordinato **per data di uscita**: il motore ordina per popolarità, che
  metterebbe in cima i titoli più vecchi — l'opposto della promessa.
- Cache **24h per piattaforma**, non per risposta: se una fallisce le altre
  restano valide, e con cache precedente si restituisce l'ultima buona.
- **`_NOVITA_SOGLIA = 6`**: sotto, si allarga il periodo (`anno`, poi
  `classici`). Apple TV+ e Paramount+ hanno cataloghi piccoli.
- [DECISION] **L'etichetta NON cambia**: restano "le uscite più recenti di
  quella piattaforma", vero anche risalendo indietro.
- ⚠️ **"Usciti di recente" e non "Novità"**: TMDb non sa quando un titolo è
  ARRIVATO su una piattaforma, solo quando è uscito.

---

## 39.6 — Ottimizzazione

### [DECISION] Lo schema del difetto
Trovato **quattro volte**: una proprietà osservata che cambia spesso, letta
da una vista grande → ridisegno totale.

| Dove | Cosa |
|---|---|
| Login | `@State` doppio sulla stessa Sessione |
| CercaView | testo nel view model; poi `inCorso` acceso a ogni lettera |
| RicercaView | `puoCercare`/`titoliInseriti` leggevano i testi dei campi |
| ScopriView | `inCaricamentoAltro` letto nel corpo |

**Rimedio**: testo in `@State` locale, deposito `@ObservationIgnored`,
conteggio osservato che cambia di rado, sotto-viste per ciò che reagisce.

### [DECISION] AsyncImage non ha cache
Ogni locandina che usciva e rientrava veniva **riscaricata**. Causa più
probabile della lentezza diffusa. → **`CacheImmagini`** (memoria + disco,
NSCache, una richiesta per URL, `task(id:)` per le liste riusate).

### Scheda film
- `/api/v1/detail` **non calcola più i simili**: al primo accesso lanciavano
  il motore (4-5s) e bloccavano tutta la scheda.
- Nuovo `/api/v1/detail/{tipo}/{id}/simili`, chiamato a parte.
- `con_simili=1` per chi vuole una chiamata sola.

### Altro
- `RaccoltaView` aveva `.task` **e** `.onAppear` → doppio caricamento.
- Tastiera: `scrollDismissesKeyboard` chiude solo **trascinando** →
  `chiudiTastieraAlTocco()` con `simultaneousGesture` (non `onTapGesture`,
  che bloccherebbe i pulsanti) + "Fine" sopra la tastiera.
- ⚠️ Rimosso un `asyncAfter(0.35)` che apriva la tastiera **durante** la
  transizione del foglio: tastiera veloce ma prime lettere lente.

### Titoli ridondanti rimossi
Novità ("Le ultime uscite" + sottotitolo), Scopri ("Scopri cosa guardare"),
Profilo ("IL TUO ACCOUNT"), Consigli (sottotitolo).
[DECISION] "Cosa guardo stasera?" **resta**: è la domanda del prodotto.

---

## 39.7 — Widget

- Ritaglio **ancorato in alto**: centrando, una locandina 2:3 in un quadrato
  perde una fascia sopra, dove sta il soggetto.

→ **Seguito il 13/10 (sezione 40.1): la locandina "tagliata" non era un bug.**
- Medio: testo **centrato verticalmente**, titolo su 3 righe se manca la
  trama (TMDb non ce l'ha in italiano per i titoli appena usciti).
- ⚠️ Risolti in precedenza: `Color.clear` nel containerBackground (= bianco in
  tema chiaro), ZStack che non riempiva il riquadro, colori di sistema su
  fondo scuro.

---

## 39.8 — [DECISION] Estensione di condivisione ABBANDONATA

**Diagnosi completa:**
1. `@objc(ShareViewController)` ha risolto il mancato avvio (il nome con il
   modulo non veniva risolto).
2. `Info.plist` era malformato: array con stringa vuota + chiave "Item 0".
3. Estrazione sbagliata: da `cosaguardo.com/serie/61068` prendeva "61068"
   come titolo da cercare.
4. ⚠️ **`extensionContext.open()` restituisce sempre false.** Documentazione
   Apple: solo Today e iMessage supportano quel metodo. Provato anche con
   tocco esplicito dell'utente: **fallisce ugualmente**.

**L'espediente della catena dei responder violerebbe la regola delle sole API
pubbliche** — da non fare con il 5.6 in sospeso.

[DECISION] **Target rimosso.** Con la ricerca dentro l'app il guadagno era
minimo. Lo schema URL `cosaguardo` **resta** (serve al widget).

---

## 39.9 — [BUG] Consigli personali: id sbagliato

- `get_daily_recommendations` vuole **user_id E data** → chiamata col solo id
  sollevava TypeError, catturato come "nessun consiglio" → **sezione vuota in
  silenzio** mentre sul sito funzionava.
- Le righe non avevano `tmdb_id`: veniva **ricercato per titolo**, e con nomi
  brevi la ricerca prende il più popolare → **"Power" apriva "Power Rangers"**.
- Il motore lo restituisce già (`normalize_movie_rec`/`normalize_tv_rec`): si
  perdeva al salvataggio. Aggiunta colonna `tmdb_id`.

✅ **VERIFICATO (15/10)**: "Power" apre la serie giusta. La colonna `tmdb_id`
risolve, e il difetto era nelle righe vecchie, non nel salvataggio.

⚠️ Lo stesso difetto resta **sul sito** in `/la-mia-raccolta`, che legge da
`user_title_state` (senza id). Cantiere a sé.

---

## DECISIONI PERMANENTI AGGIUNTE

- **Token opaco in tabella, mai JWT**
- **Validazioni app = validazioni sito**, mai più permissive
- **Login/registrazione: risposta indistinguibile** sull'esistenza dell'email
- **`title` = originale (motore), `display_title` = italiano (utente)**
- **Capability sempre con "All" selezionato**, non solo Release
- **@ObservationIgnored per ciò che cambia a ogni lettera**
- **Mai AsyncImage: usare `CacheImmagini`**
- **Niente titolo in pagina se la barra lo dice già**
- **Un'estensione di condivisione NON può aprire l'app contenitore**
- **La geo-restrizione copre `/api/v1/`**: l'app non funziona all'estero —
  accettato

---

# SEZIONE 40 — Ottimizzazione e un bug SEO rimasto nascosto (13/10)

## RIEPILOGO
Un giro di ottimizzazione su app e server, e la scoperta — di rimbalzo — che
Googlebot era bloccato dal geo-guard sulle pagine SEO da chissà quanto.

---

## 40.1 — [DECISION] Widget: il bug che non c'era

Tre correzioni consecutive al ritaglio della locandina quadrata, tutte
inefficaci, perché il difetto era nell'immagine e non nel codice.

**La misura che ha chiuso la questione**: il widget **medio** usa un riquadro
74×111, cioè esattamente 2:3 — ritagliare è impossibile per costruzione. La
"testa tagliata" c'era lo stesso, e in fondo alla locandina compariva il blocco
dei credits completo. Quindi l'artwork è una locandina teaser già ritagliata
stretta all'origine.

⚠️ **Tre tentativi prima di misurare.** La segnalazione diceva "viene
tagliata", e si è cercato il ritaglio nel codice invece di chiedersi se
l'immagine di partenza fosse già così. Davanti a "non funziona", la prima mossa
è misurare l'input, non correggere l'output.

---

## 40.2 — Locandine: 50 chiamate TMDb diventate 9

`_enrich_titles_with_posters` in `main.py` richiedeva il poster anche per i
titoli che ce l'avevano già.

```python
needs = [(d['title'], d['content_type']) for d in out
         if not d.get('poster_url') or not d.get('tmdb_id')]
...
    if d.get('poster_url') and d.get('tmdb_id'):
        continue
```

- **`_titoli_irrisolti`** + `_e_irrisolto()` / `_segna_irrisolto()`: un titolo
  che TMDb non trova non va richiesto a ogni apertura. TTL 6h, tetto 5000 voci.
- Misura: **50 → 9 chiamate su 5 aperture**.

---

## 40.3 — `/scopri` in cache

`get_scopri_results` in `recommendation_api.py` diviso in
**`_scopri_results_live`** + involucro con cache.

- `_SCOPRI_TTL = 3 * 3600`, `_SCOPRI_MAX = 500`
- ⚠️ **`_scopri_copia()` restituisce una copia**: senza, chi riceve il
  risultato può modificare l'oggetto in cache e avvelenarlo per tutti.
- Warmup all'avvio.

---

## 40.4 — ScopriView: la prima volta non è più vuota

Alla prima apertura la scheda mostrava lo stato "vuoto" e poi si riempiva: da
fuori sembra un errore, non un caricamento.

- **`primaRicercaFatta`**: lo stato vuoto compare solo **dopo** la prima
  ricerca, mai prima.
- **`GrigliaSegnaposto`**: scheletro al posto del nulla.
- `.task` parallelo con `async let`: le strisce partono insieme.

---

## 40.5 — [BUG] Googlebot bloccato dal geo-guard (pre-esistente)

Il più grave dei tre giorni, e non era in programma trovarlo.

**Catena**: `main.py` non importava `requests` a livello di modulo — c'era un
solo `import requests` **dentro** un'altra funzione, e un import dentro una
funzione lega il nome solo a quella. `_fetch_googlebot_nets()` (riga ~541) lo
usava e sollevava `name 'requests' is not defined` a ogni chiamata. L'eccezione
veniva catturata e registrata come "Googlebot IP ranges: download fallito", la
lista restava vuota, `_is_verified_googlebot()` non corrispondeva mai →
**Googlebot preso per un visitatore estero e bloccato sulle pagine SEO.**

⚠️ Un `except` che registra e prosegue trasforma un errore fatale in un difetto
silenzioso. Qui ha nascosto per settimane un problema di indicizzazione.

⚠️ **Trovato con pyflakes mentre si cercava un bug proprio** (`json` non
importato, stesso schema). Lo scoping degli import non si vede leggendo: vale
la pena passare un analizzatore statico su un file da 7000 righe ogni tanto.

Riprodotto in isolamento prima di dichiararlo. Corretto con un import a livello
di modulo.

---

## 40.6 — TestFlight

[DECISION] Saltata la consulenza App Review sul 5.6.

⚠️ Nel merito: "ora siamo nativi" risponde alla **4.2** (Minimum
Functionality), non alla 5.6 (Developer Code of Conduct). Sono linee guida
diverse e la seconda non si risolve con la nativizzazione.

⚠️ I tester **interni** di TestFlight non passano dalla Beta App Review: si può
provare la build senza aspettare nessuno.

→ Programmato per **martedì 20/10**, dal Mac.

---

# SEZIONE 41 — Automazione social completa (14–15/10)

## RIEPILOGO
Da "monto in CapCut e pubblico a mano" a: un comando a settimana produce sei
contenuti, Marco approva dal telefono, e da lì escono da soli su Instagram,
Facebook, YouTube e TikTok.

Intervento umano: una sessione di approvazione a settimana, più ~30 secondi al
giorno per incollare il testo su TikTok.

**Documento operativo separato**: `claude/handover-automazione-social.md` nel
progetto (variabili d'ambiente, endpoint, cosa controllare). Questa sezione
tiene le decisioni e le trappole.

---

## 41.1 — Il giro

```
settimana.py → genera ×6 → monta ×6 → carica → approvazione → coda → uscita
   (PC)         (PC, AI)    (PC)       (PC)     (telefono)    (server)
```

| Piattaforma | Video | Carosello | Didascalia | Commento |
|---|---|---|---|---|
| Instagram | Reel auto | Carosello auto | auto | **auto** |
| Facebook | auto | — | auto | auto (da verificare) |
| YouTube | Short auto | — | auto | a mano |
| TikTok | bozza in casella | bozza in casella | **a mano** / auto (foto) | a mano |

---

## 41.2 — CapCut sostituito — `monta.py`

ffmpeg: concat demuxer con durate per immagine, sottotitoli ASS con tag `\k`
(karaoke), `loudnorm=I=-14:TP=-1.5:LRA=11`, `aformat=channel_layouts=stereo`.

- **ElevenLabs `/with-timestamps`** restituisce `audio_base64` + `alignment`
  (caratteri con tempi) → tempi delle parole **senza trascrizione**.
- ⚠️ Si usa `alignment`, **non** `normalized_alignment`: il secondo è la forma
  parlata ("5" → "cinque") e a schermo si vuole "5 FILM".
- ⚠️ Nel tag karaoke **lo spazio va dentro**: `f"{{\\k{cent}}}{parola} "` poi
  `.rstrip()`. Fuori, la sincronizzazione scivola.
- I tagli del duo vengono dalla **durata TTS misurata per blocco**, non da
  stime sul numero di parole.
- `SOSTITUZIONI_SOTTOTITOLI`: "cosa guardo punto com" → **"cosaguardo.com"**
  (la correzione che Marco faceva a mano). Le parole unite portano
  `"fisso": True` perché `pulisci_parola` non le trasformi in "cosaguardocom".
- Verificato: render pixel-identico all'export CapCut a meno della
  compressione (scarto medio 1,5/255), audio stereo, nessun clipping.

Voce: **Ginevra `QITiGyM4owEZrBEf0QV8`**.

---

## 41.3 — [DECISION] La coda sta sul server

**Instagram non sa programmare un post**: l'API pubblica nell'istante in cui la
chiami. Con l'orologio sul PC, a PC spento non esce niente.

E tenendo la logica solo sul server, "pubblica subito" e "programmato" sono
**lo stesso codice**. Due implementazioni della stessa cosa divergono sempre, e
divergono nel ramo che si prova meno.

### Il claim atomico

```sql
UPDATE coda_social SET stato='in_corso' WHERE id=? AND stato='in_attesa'
```
+ `rowcount == 1`.

⚠️ Il peggio che può capitare a una coda di pubblicazione non è saltare un
post: è pubblicarlo **due volte**.

### [BUG] Riga verde senza aver pubblicato

`_social_pubblica` restituiva con successo se `esiti` non era vuoto — anche
quando conteneva solo `tiktok_errore`.

```python
riusciti = [k for k in esiti
            if not k.endswith("_errore") and "_commento" not in k]
if not riusciti:
    raise RuntimeError(f"nessuna piattaforma ha pubblicato — {motivi}")
```

⚠️ Facebook, YouTube e TikTok sono **non fatali** di proposito (se Instagram è
già uscito, rimettere la riga in coda ne farebbe un doppione). Ma quella
scelta vale solo se **almeno una** piattaforma ha pubblicato davvero.

### Altre trappole

- ⚠️ `/media-temp` e `/coda-social` stanno **fuori** da
  `_EDGE_PROTECTED_PREFIXES`: Meta e TikTok devono scaricare dai loro
  datacenter.
- ⚠️ I media stanno **accanto al database** (disco persistente Render), non in
  `/tmp` che è effimero.
- ⚠️ La pulizia protegge anche lo stato `da_approvare`: una riga in attesa del
  sì può restare giorni, molto più di una programmata. Senza, l'approvazione
  fallirebbe con "il file non c'è più".
- ⚠️ La notifica parte **dopo `chiudi_post`, in un try suo**: una notifica
  fallita non deve rimettere in coda una riga già pubblicata.
- Il database è **SQLite**, non Postgres. (Affermato il contrario per un pezzo
  di conversazione; corretto.)

---

## 41.4 — Meta

- `token_meta.py`: token breve dell'Esplora → token utente 60gg → **token di
  pagina che non scade** (`debug_token` → `expires_at: 0`). Lo **verifica**
  invece di darlo per buono.
- ⚠️ Le versioni dell'API Graph si provano dalla più recente: scriverne una
  fissa significa consegnare uno script che fra un anno smette.
- ⚠️ Una versione non ancora uscita non dà "versione non supportata" ma
  **"Unknown path components: /me"**: va riconosciuta, o lo script si ferma al
  primo tentativo scambiandolo per un problema di token.
- ⚠️ `business_management` serve quando la pagina sta in un portfolio
  aziendale: senza, `/me/businesses` risponde "(#100) Missing Permission".
- ⚠️ `_segreti_meta.json` in `.gitignore` **prima** del primo `git add`.

---

## 41.5 — TikTok

[DECISION] **Si usa la casella (bozza), non il direct post.** L'audio nativo si
sceglie solo in app: il video arriva in casella, Marco apre, mette l'audio e
pubblica. Trenta secondi, e in cambio nessun audit.

### [DECISION] Resta in sandbox, di proposito

**Verificato il 15/10**: una bozza depositata da un'app **non auditata** e poi
pubblicata a mano dall'app esce **pubblica**. La restrizione "private-only
until audited" riguarda il direct post, non questo percorso.

⚠️ **Non documentato da nessuna parte** — è un risultato sperimentale. Se TikTok
stringesse, la coda segna `tiktok_errore` col motivo, le altre piattaforme
pubblicano lo stesso, e si torna a mettere i video a mano col testo già pronto
nel Telegram. Degrada, non si rompe.

⚠️ Fra i motivi di rifiuto dell'audit c'è **"app per uso privato o
personale"**, che descrive bene questa integrazione: andarci adesso è un
rischio senza guadagno.

### Dettagli

- `POST /v2/post/publish/inbox/video/init/` con `PULL_FROM_URL`. Dominio da
  verificare nel portale, e **la sandbox ne vuole una sua** (record TXT
  separato, aggiunto accanto, non al posto di).
- ⚠️ **La casella video NON porta il testo**: `post_info` esiste solo nel
  direct post.
- ⚠️ I **caroselli di foto** usano `/v2/post/publish/content/init/`
  (`media_type: PHOTO`, `post_mode: MEDIA_UPLOAD`), che **accetta titolo e
  descrizione**. Stesso scope `video.upload`, stessa verifica del dominio.
  Richiede app TikTok ≥ 31.8.
- ⚠️ **Il refresh token RUOTA**: va risalvato, o il rinnovo del giorno dopo
  fallisce. (Access 24h, refresh 365gg.)
- ⚠️ `_tiktok_stati`: gli state OAuth emessi, TTL 10 minuti, uso singolo. Senza,
  chiunque trovi `/tiktok/callback` può far salvare al server il token del
  **proprio** account.
- `non_sandbox_target` → l'account va aggiunto come test user della sandbox.

---

## 41.6 — YouTube

- OAuth web, redirect `https://cosaguardo.com/youtube/callback`, scope **solo
  `youtube.upload`**.
- ⚠️ **`access_type=offline` + `prompt=consent`**. Senza il secondo, Google
  consegna il refresh token **solo alla prima autorizzazione**: riautorizzando
  per cambiare canale si resta col token vecchio, che punta al canale
  sbagliato, senza nessun errore.
- ⚠️ **La schermata di consenso dev'essere "In produzione".** In *Testing* il
  refresh token muore dopo **7 giorni esatti**: funzionerebbe una settimana e
  poi si fermerebbe senza una ragione visibile. `/youtube/stato` riporta l'età
  del token proprio per rendere visibile la trappola.
- ⚠️ "App non verificata" è **un altro stato** e va benissimo: comporta solo la
  schermata di avviso e il tetto di 100 utenti.
- Caricamento **ripristinabile in due passi**: prima i metadati, poi i byte.
  Nel multipart un titolo troppo lungo si scopre **dopo** aver spedito il file.
- ⚠️ Titolo: max 100 caratteri, niente `<` `>`. Oltre, 400 generico.
- ⚠️ `selfDeclaredMadeForKids` obbligatorio.
- ⚠️ Quota: un caricamento costa **1600 su 10.000** al giorno → sei video.
- Lo Short è riconosciuto **da solo** (verticale + sotto i 3 minuti):
  l'etichetta "#Shorts" non serve più.
- [DECISION] **Niente `youtube.force-ssl`** per commentare: darebbe accesso
  completo in scrittura al canale, cancellare video compreso. Troppo per un
  commento al giorno.

---

## 41.7 — Calendario e approvazione

[DECISION] **Una pagina, non una conversazione Telegram.** Approvare sei
contenuti a messaggi significa ricostruire a mano quale risposta vale per quale
reel, e sbagliarsi una volta vuol dire pubblicare quello scartato.

- Stato **`da_approvare`**. ⚠️ Non può uscire per errore non perché ci sia un
  controllo da ricordarsi, ma perché il worker cerca solo `in_attesa` e **la
  query non la vede**.
- Pagina mobile: video riproducibile, giorno in chiaro, didascalia **intera**,
  orario modificabile, Pubblica / Scarta / Decido dopo (preselezionato su
  Pubblica — "tutto in blocco, con eccezioni").
- ⚠️ La conversione oraria si fa **server-side** con zoneinfo, non in
  JavaScript: in JS userebbe il fuso del telefono, sbagliando di un'ora se si
  approva da un altro fuso.
- ⚠️ Nomi dei giorni **scritti a mano**: il locale del server è quello di
  Render, `strftime` li darebbe in inglese.
- ⚠️ La transizione ammessa è **solo** da `da_approvare`: due invii della
  pagina non riapprovano né spostano la data.
- ⚠️ Approvare una riga il cui orario è **già passato** la pubblica subito. Da
  riga di comando un orario passato viene letto come "domani", sulla pagina no.
- ⚠️ Non fare niente = la riga resta in attesa all'infinito. Non esce e non
  viene scartata.
- Se non si approva, **i media restano protetti dalla pulizia** (vedi 41.3).

---

## 41.8 — `settimana.py`

[DECISION] **Non riscrive niente dei generatori**: li lancia come sono. Regole,
storico anti-ripetizioni, validazione e loop di modifica restano in
`genera*.py`, in un posto solo.

- Mix **2 tier list + 2 duo + 2 caroselli**, **alternati**: due dello stesso
  tipo di fila nel calendario si vedono.
- ⚠️ **Trova le cartelle prodotte fotografando il disco prima e dopo**, invece
  di sapere dove ciascun generatore scrive. I tre salvano in posti diversi, e
  scrivere qui quei percorsi significherebbe averli in due posti. Verificato
  con un generatore che scrive in una cartella mai vista: la trova.
- **`--riprendi`**: sei generazioni AI più sei montaggi sono lunghi; se salta
  al quinto, senza stato si butterebbero via quattro contenuti e i crediti
  vocali già spesi. Stato in `_settimana_in_corso.json`, cancellato a fine
  corsa perché un `--riprendi` non riparta su roba già caricata.
- ⚠️ **Prima esamina tutte le cartelle, poi carica**: se la quarta non ha la
  CAPTION, meglio saperlo prima di averne caricate tre.

---

## 41.9 — `poster.py` — la ricerca TMDb, e due difetti

Era copiata **identica in tre file**. Ora sta in `poster.py`, usata sia dai
renderer sia dal controllo preventivo.

⚠️ **Devono essere la stessa funzione.** Un controllo che cercasse diversamente
direbbe "va bene" su titoli che poi il renderer non trova: sicurezza falsa,
peggio di nessun controllo.

### [BUG] Si fermava alla prima variante con risultati

`if res: break`. Se la ricerca italiana restituiva dieci film sbagliati, quella
inglese — che avrebbe trovato il titolo giusto — non veniva mai provata.

### [BUG] L'anno era un filtro morbido — il peggiore dei due

Senza corrispondenza esatta sull'anno, invece di arrendersi teneva **tutti** i
risultati e prendeva il più popolare.

⚠️ Non produceva un poster mancante: produceva **il poster di un altro film**,
con l'aria di essere giusto. Un buco si vede, un titolo sbagliato con locandina
plausibile no — e finiva pubblicato.

### Adesso

Anno come **parametro vero dell'API**, candidati raccolti da tutte le varianti,
anno esatto preferito, scarto di **un** anno accettato e annotato (le uscite
italiane sono sfasate), oltre quello **segnalato**. Aggiunto `search/multi`,
che recupera i casi di **tipo sbagliato** (miniserie etichettata "film"): prima
sparivano.

⚠️ **La chiave della cache non è cambiata** (`<slug>_<film|tv>[_anno].jpg`):
un formato diverso renderebbe invisibili centinaia di immagini già scaricate.

⚠️ **Logica provata con risposte simulate: l'API vera non è mai stata chiamata**
dall'ambiente di sviluppo (rete bloccata). Da verificare al primo giro
guardando le righe `->`.

---

## 41.10 — [DECISION] Il controllo dei poster sta PRIMA della conferma

Osservazione di Marco, e ha cambiato il progetto in meglio: la ricerca avveniva
dentro i renderer, cioè **dopo il `s`**, quando l'unico rimedio era rifare
tutto.

Ora il controllo gira fra `valida()` e `mostra()`. I titoli introvabili vengono
**sostituiti uno a uno** chiamando `modifica()` del generatore.

⚠️ **Si sostituisce il singolo titolo, non si rigenera il contenuto**:
rigenerare butterebbe via quindici titoli buoni per colpa di uno, e
un'estrazione nuova potrebbe pescarne un altro introvabile e girare a vuoto.

⚠️ Se la sostituzione rompe il formato si tiene la versione **precedente**: una
proposta con un buco è meglio di una proposta rotta.

Non è lavoro doppio: i poster scaricati finiscono in cache e il renderer li
ritrova lì.

---

## 41.11 — Il commento da fissare

I generatori lo scrivono (`COMMENTO DA FISSARE`) ma non arrivava da nessuna
parte: restava in un txt sul PC, cioè il passaggio si saltava.

- Ora viaggia in `extra` e viene **pubblicato automaticamente su Instagram e
  Facebook** dopo l'uscita.
- ⚠️ **Fissarlo in cima non esiste in nessuna API** — né Instagram, né
  Facebook, né YouTube. È solo da app. Il valore dell'automazione è un altro:
  il commento col link c'è comunque, ed essendo immediato è il primo.
- Il Telegram dice **dove è stato pubblicato** e dà il testo da incollare
  **solo** per le piattaforme dove manca.
- ⚠️ Il commento è in un try suo e **non fa mai fallire la riga**: il post è già
  uscito.
- Permesso `instagram_manage_comments`. `pages_manage_engagement` non compare
  nell'Esplora in questa configurazione: se il commento Facebook fallisce, è
  quello che manca, e il messaggio lo dirà col nome esatto.

---

## 41.12 — [DECISION] Orari: 19:30, nessuna differenza per giorno

- ⚠️ **La documentazione ufficiale Instagram elenca "quando un post è stato
  condiviso" solo per il Feed, non per i Reels.** Per i Reels i segnali sono
  altri: l'orario incide solo su chi è online nei primi minuti.
- Quattro studi grandi (Buffer 9,6 mln di post, Metricool 24,3 mln, Later 6
  mln, Sprout 2 mld di interazioni) danno risposte **incompatibili**, compresi
  "domenica è il giorno migliore" e "il weekend è il peggiore".
- **Nessun dataset italiano con metodologia dichiarata.** Gli articoli in
  italiano riportano orari senza campione né periodo.
- ⚠️ Metricool vende la funzione "miglior orario": non è fonte neutrale su
  questo punto.

### `analisi_orari.py` — se un giorno si volesse rifare

⚠️ **Il confronto grezzo dà una risposta sbagliata.** Provato su dati costruiti
con crescita del profilo, cambio di orario a metà e **nessun effetto vero**: il
confronto diretto annuncia **+129% per la sera**. È tutta crescita.

Rimedio: ogni reel diviso per la **mediana dei vicini nel tempo**. Sullo stesso
dato: 0.97 contro 0.99, cioè nessun effetto — la risposta giusta.

⚠️ Il primo criterio per dire "c'è un segnale" era troppo grezzo e su un
effetto vero del +60% rispondeva "nessun segnale". Sostituito con Mann-Whitney
sui ranghi. Provato su 20 campioni per parte: **0 falsi positivi**, effetto
reale trovato **15 volte su 20**.

⚠️ Con ~50 reel si vede **solo** una differenza sopra il 50%. "Nessun segnale"
significa *non si vede*, non *non esiste* — e lo script lo scrive a schermo.

---

## 41.13 — Errori di processo da non ripetere

- ⚠️ **Tre volte di seguito consegnato un booleano o un 404 senza il motivo**
  (accodamento 500, stato TikTok, `inviato: false` di Telegram), e tre volte si
  è dovuti tornare ad aggiungere la diagnostica. Un esito che può fallire per
  tre cause diverse deve dire **quale** fin dalla prima consegna.
- ⚠️ **`--senza` si era preso l'`else` della catena if/elif**: controllo di
  sintassi e pyflakes **entrambi puliti**, il codice restava valido — cambiava
  solo a chi apparteneva l'else. L'ha trovato solo l'esecuzione.
- ⚠️ **Il 15/10 una sostituzione di testo con un intervallo sbagliato ha
  cancellato i blocchi TikTok e YouTube da `main.py`.** Ricostruiti e
  verificati per struttura (18 rotte, 23 funzioni, nessun duplicato), non solo
  "compila". Se dopo un deploy qualcosa in quei blocchi si comportasse in modo
  strano, è il primo posto dove guardare.

- ⚠️⚠️ **E NON ERA LA PRIMA VOLTA.** Il 16/10 è saltato fuori che
  `/admin/crescita` rispondeva 404 da tre giorni: la rotta era sparita dentro
  il commit **d29a080 del 13/10** ("api: i consigli dell'app non venivano mai
  salvati in cache"), una consegna che riguardava tutt'altro — **47 righe
  aggiunte, 74 rimosse**. Il pannello era stato creato il 07/10 e usato per
  una settimana.

  **Perché non se n'è accorto nessuno:** una rotta che sparisce non rompe
  niente. L'app non la chiama, i test non la toccano, il server parte
  regolarmente. Si manifesta solo il giorno in cui qualcuno apre quella
  pagina. Qui ci sono voluti tre giorni; poteva volerci un mese.

  **[DECISION] Dopo ogni modifica strutturale a `main.py`, confrontare
  l'inventario delle rotte** invece di fidarsi del fatto che compili:

  ```
  git show HEAD~1:app/main.py | grep -o '@app\.[a-z]*("[^"]*"' | sort > prima.txt
  grep -o '@app\.[a-z]*("[^"]*"' app/main.py | sort > dopo.txt
  diff prima.txt dopo.txt
  ```

  Un `diff` che mostra righe tolte quando ci si aspettavano solo aggiunte è il
  segnale. Costa dieci secondi e avrebbe intercettato entrambi i casi.

  ⚠️ **In `cmd.exe` il `^` è il carattere di escape**: `git show d29a080^` viene
  letto come `git show d29a080` e restituisce la versione sbagliata senza
  lamentarsi. Usare `d29a080~1`. Questo errore ha fatto perdere un giro
  d'analisi, confrontando un file con sé stesso.
- ⚠️ Affermato che il database fosse Postgres, che l'email fosse "la via più
  spiccia" (il server non manda email: `alerts_job.py` fa push), e che il video
  dimostrativo TikTok servisse solo per `video.publish`. Tutte e tre smentite
  dai fatti. **Verificare prima di affermare**, soprattutto quando la risposta
  cambia cosa si costruisce.

---

## DECISIONI PERMANENTI — aggiunte (40–41)

- **Davanti a "non funziona", misurare l'input prima di correggere l'output**
- **Un `except` che registra e prosegue nasconde i difetti**: passare pyflakes
  su `main.py` ogni tanto
- **La pubblicazione sta sul server**: Instagram non programma, e un solo
  percorso di codice per "subito" e "programmato"
- **Claim atomico sulla coda**: il doppione è peggio del post saltato
- **"Pubblicato" solo se almeno una piattaforma ha pubblicato davvero**
- **Mai la stessa ricerca in due posti**: controllo e uso devono essere la
  stessa funzione
- **Sostituire il singolo titolo, mai rigenerare il contenuto**
- **TikTok resta in sandbox** finché la casella funziona
- **Consenso Google "In produzione"**, e solo `youtube.upload`
- **Fissare un commento non esiste in nessuna API**
- **Ogni esito che può fallire per più cause deve dire quale**, dalla prima
  consegna

---

# SEZIONE 42 — App iOS: qualità e prestazioni (21–22/10)

Contesto: l'app è stata rifiutata da Apple con la linea guida **5.6**, e la
scheda su App Store Connect non è ripresentabile — ne è stata creata una nuova
con bundle identifier `com.cosaguardo.ios.CosaGuardo`. Prima di ripresentare si
è fatta una revisione schermata per schermata contro i quattro punti di qualità
citati nel rifiuto. Quello che segue è il risultato: cosa è stato corretto,
cosa è stato tentato e scartato, e perché.

Progetto: `~/Desktop/cosaguardo-app-ios/CosaGuardo/`, Xcode 16, iOS minimo 17.0,
dispositivo di prova iPhone 17.

## 42.1 Il ripristino della sessione non partiva all'avvio

⚠️ **Il difetto più grave trovato in questo giro.** `Sessione.ripristina()` era
chiamato solo dal `.task` di `ProfiloView`. In un `TabView` la vista di una
linguetta nasce quando la si apre, e la linguetta iniziale è Consigli: quindi
**finché non si toccava Profilo, l'app non verificava il token e `utente`
restava `nil`**.

Cosa vedeva un utente già collegato che apriva l'app e andava dritto su un
film: *"Accedi dal Profilo per salvare i titoli visti e i preferiti"*, e la
raccolta vuota. Tutto tornava a posto solo dopo essere passati dal Profilo, il
che non ha nessuna logica visibile da fuori.

La chiamata è stata spostata in `CosaGuardoApp.swift`, nel `.task` della vista
radice. `ProfiloView` la mantiene come rete di sicurezza, resa innocua da una
guardia `ripristinoAvviato` in `Sessione`: senza quella, all'avvio partivano
due verifiche del token in parallelo.

⚠️ La guardia **si riapre se la verifica fallisce per mancanza di rete**. Chi
apre l'app in galleria avrebbe altrimenti un token mai verificato e resterebbe
scollegato per tutta la sessione anche tornando in copertura. Con token
rifiutato invece resta chiusa: lì la risposta del server è definitiva.

## 42.2 "Sessione scaduta" per credenziali sbagliate

`ErroreRete.nonAutorizzato` produceva *"Sessione scaduta. Accedi di nuovo."*
per **ogni** 401, compreso quello di `/api/v1/auth/login`. Chi sbagliava la
password leggeva che era scaduta una sessione che non era mai esistita.

Aggiunto `accessoNegato(String)` in `Rete.swift`, e il 401 si smista sul
percorso: login e registrazione → *"Email o password non corretti."*, Apple →
*"Accesso con Apple non riuscito. Riprova."*, tutto il resto → "Sessione
scaduta" come prima.

⚠️ Il messaggio è **scritto nell'app, non preso dal `detail` del server**: il
server potrebbe distinguere "email inesistente" da "password sbagliata", e
mostrare la differenza direbbe a chiunque quali indirizzi sono registrati.
Resta valida la decisione permanente sulla risposta indistinguibile.

⚠️ `/api/v1/auth/io` **non** è nell'elenco, così `ripristina()` continua a
ricevere `nonAutorizzato` e a ripulire il portachiavi.

## 42.3 Sign in with Apple non rispondeva al tocco

Il pulsante non reagiva affatto: nessun pannello, nessun errore, nessun
messaggio. Tre tocchi, zero righe in console.

Causa: `AccessoView` applicava `.chiudiTastieraAlTocco()` all'intera schermata,
e quel modificatore mette un `simultaneousGesture(TapGesture())` su tutto.
**`SignInWithAppleButton` non è un pulsante SwiftUI ma un controllo UIKit
incapsulato**, e non convive con un gesto applicato a un antenato: il tocco
viene consumato prima di arrivargli. Il commento originale in
`ChiudiTastiera.swift` diceva che `simultaneousGesture` era stato scelto
*proprio* perché i pulsanti continuassero a funzionare — vale per quelli
SwiftUI, non per quelli UIKit.

⚠️ Questo è il motivo per cui "prima funzionava": la convivenza dipende
dall'ordine in cui SwiftUI costruisce la gerarchia, che cambia con le versioni
di iOS e con modifiche fatte altrove. Non è un difetto che si manifesta in modo
stabile.

Correzione: `chiudiTastieraAlTocco()` è stato spezzato in due.
- **`barraFineTastiera()`** — solo la barra con "Fine" sopra la tastiera.
  Innocua ovunque, non tocca i gesti di nessuno.
- **`chiudiTastieraAlTocco()`** — solo il gesto sullo sfondo.

In `AccessoView` la barra sta su tutta la pagina e il gesto solo intorno alle
due card dei moduli, dove vivono esclusivamente controlli SwiftUI.

**Regola generale:** mai applicare un gesto a un contenitore che racchiude
controlli UIKit incapsulati.

⚠️ Per diagnosticarlo è stata aggiunta una stampa in `AccessoApple.swift` nel
blocco che configura la richiesta — quel blocco gira **solo** se il tocco
arriva davvero al pulsante — e un'altra nel ramo `.canceled`, che prima usciva
in silenzio. **`.canceled` è anche ciò che iOS riporta quando manca
l'entitlement**, quindi "non succede niente" copriva tre cause diverse. Le due
stampe sono in `#if DEBUG`; toglierle prima dell'archivio finale.

## 42.4 Due secondi di blocco alla prima apertura di Cerca

Sintomo riferito: la tastiera compare subito ma le prime lettere arrivano con
un paio di secondi di ritardo. **Reale anche in build Release su iPhone 17**,
quindi non un artefatto di Debug.

La causa è stata trovata leggendo il log di sistema: subito prima del blocco
comparivano `UIEmojiSearchOperations` e una sfilza di *"variant selector cell
index number could not be found"*. Il sistema stava costruendo il **pannello
emoji** e le sue tabelle, che è una delle parti più pesanti del sistema di
input.

Correzione, una riga in `CercaView`:

```swift
.keyboardType(.asciiCapable)
```

Misurato: il blocco da **2050 ms è sparito**. Il tasto emoji scompare da quel
campo soltanto; le lettere accentate restano disponibili tenendo premuto. In un
campo dove si cerca il titolo di un film, le emoji non servono a nessuno.

Stessa riga applicata a `CampoSegreto` in `AccessoView`. Il campo Email non ne
ha bisogno: `keyboardType(.emailAddress)` è già una tastiera senza emoji.

## 42.5 Lo scorrimento ai suggerimenti non è mai partito

In `RicercaView`, sullo stesso `CampoTitolo`:

```swift
.id(campo.id)
.id("\(campo.id)-\(vm.generazione)")
```

**Due `.id()` sulla stessa vista.** In SwiftUI vince quello esterno, quindi il
primo era morto — e `lettore.scrollTo(campo.id, anchor: .top)` cercava proprio
quello.

Conseguenza, scritta nel commento del codice che la prevedeva: il menu dei
suggerimenti nasceva sotto la tastiera, l'utente non lo vedeva, scriveva il
titolo per intero, e al motore arrivava **il titolo italiano digitato a mano
invece dell'originale di TMDb**. Consigli peggiori, senza nessun segnale che
qualcosa non andasse.

Corretto con un solo identificatore, `ancoraCampo(campo)`, usato sia da `.id()`
sia da `scrollTo`.

## 42.6 Tre grammatiche di card per lo stesso contenuto

`CardTitolo` (Scopri) metteva il titolo **sopra** la locandina, `CardNovita`
(Usciti di recente) lo metteva **sotto**, `CardTrending` (home) di nuovo sopra.
Tre forme per lo stesso oggetto, a una linguetta di distanza. Entrambi i primi
due avevano un commento che difendeva la propria scelta: nessuno dei due
sbagliava da solo, insieme producevano un'incoerenza.

Unificate le prime due su "titolo sotto", in un unico `CardTitolo` con
parametro `larghezza` (nil = si adatta alla colonna, un valore = striscia
orizzontale). `CardNovita` eliminato.

Il criterio non è stato il pareggio ma il contrasto: **testo bianco sopra una
locandina qualsiasi ha un contrasto che nessuno controlla**, e su una copertina
chiara diventa illeggibile. Voto e anno restano in sovrimpressione: sono corti,
stanno in basso dove la sfumatura è più scura, e se si perdono non si perde il
titolo.

⚠️ **`CardTrending` è rimasta fuori** e va ancora allineata: ha in più
un'etichetta "FILM"/"SERIE TV" che richiede un parametro aggiuntivo.

⚠️ Unificando è stato necessario toccare anche `GrigliaSegnaposto`: ora che il
titolo sta sotto la locandina, i rettangoli segnaposto devono riservare anche
quello spazio. Senza, all'arrivo dei titoli veri ogni card cresceva di 30 punti
e la griglia saltava — esattamente ciò che il segnaposto esiste per evitare.

## 42.7 Titolo tagliato a metà parola, senza puntini

In Usciti di recente si leggeva "The Love Hypothe" e sotto "Il teorema
dell'am", entrambe tagliate a metà parola. Due righe tagliate così non sono un
testo che va a capo — se lo fosse, la prima si spezzerebbe su uno spazio.
Significa che **nella stringa c'è un'andata a capo**.

`CardTitolo` ora normalizza il titolo prima di mostrarlo (ritorni a capo e
tabulazioni → spazi, spazi multipli compressi), più `truncationMode(.tail)` e
`fixedSize(horizontal: false, vertical: true)` perché il testo vada a capo alla
larghezza proposta invece di essere tagliato. L'etichetta ha **altezza fissa a
due righe**, non "al massimo due": senza, le card con titolo corto finivano più
in alto delle altre e la griglia risultava sfalsata.

## 42.8 Scheda film: titolo doppio e bordi incoerenti

Il titolo compariva sia nella barra di navigazione sia dentro la card, contro
la decisione permanente *"niente titolo in pagina se la barra lo dice già"*.
Tolto dalla card. Anno e durata uniti su una riga sola separati da `·`, così la
colonna accanto alla locandina non resta mezza vuota.

I bordi dei pulsanti piattaforma usavano `piattaforma.coloreMarchio`: Apple TV+
grigio, Timvision e Amazon blu, **Rakuten rosso**. Il risultato era una fila di
bordi di colori diversi senza logica leggibile, e **un bordo rosso in
un'interfaccia significa "errore"** — quel pulsante sembrava rotto. Ora
`Tema.bordo` per tutti; il marchio lo porta il logo, che è il suo mestiere.

## 42.9 Pulsanti che non rispondono al tocco

`.buttonStyle(.plain)` toglie il colore azzurro che SwiftUI metterebbe
sull'etichetta di un pulsante disegnato a mano — ma insieme toglie anche la
**reazione alla pressione**. Chi preme non vede succedere niente e ripreme: sul
pulsante Google significava temere di aver sbagliato, accanto a Sign in with
Apple che invece la pressione la mostra.

Nuovo file `StilePulsante.swift` con `PulsanteReattivo`, un `ButtonStyle` che
legge `configuration.isPressed` e abbassa opacità (0.55) e scala (0.97) per 120
ms. Applicato a 12 punti in 6 file: card dei titoli ovunque, selettori Film /
Serie TV, pillole delle piattaforme, voci del menu suggerimenti, spunte legali.

⚠️ Il doppio tocco su Google **non** faceva partire due accessi: la guardia
`inApertura` in `PulsanteGoogle` lo impediva già. Il problema era di
percezione, non di funzionamento.

## 42.10 Pulsante Google invisibile

Riempimento `white.opacity(0.06)` e bordo `Tema.bordo` su fondo blu scuro: il
pulsante era indistinguibile da uno disattivato, e sotto c'era "Accedi" che
quando i campi sono vuoti **è** davvero disattivato e aveva lo stesso aspetto.
Due stati opposti non possono assomigliarsi così.

Portato a `white.opacity(0.14)` di riempimento, bordo `white.opacity(0.32)`,
testo bianco pieno.

⚠️ **Marcato ma non quanto Apple, di proposito**: le linee guida impongono che
Sign in with Apple non sia *meno* evidente degli altri accessi. Bianco pieno
contro superficie chiara con bordo mantiene la gerarchia richiesta.

## 42.11 Localizzazione italiana

`SignInWithAppleButton` si traduce da solo seguendo la lingua del dispositivo,
ma **solo se l'app dichiara di supportare l'italiano**. Impostata la *Default
localization* (`CFBundleDevelopmentRegion`) su `it`: il pulsante è diventato
"Accedi con Apple" senza toccare codice.

⚠️ Le Localizations stanno su **PROJECT → Info**, non su TARGETS → Info, dove
si finisce per istinto e si trovano solo le *Custom iOS Target Properties*.

---

## 42.12 — Due tentativi falliti, e perché

Questa parte vale più delle correzioni: sono due strade che sembravano ovvie e
non lo erano. Il codice ne porta il commento, per non ricascarci.

### Precaricare la tastiera all'avvio

Idea: la prima costruzione del sistema di input costa; pagarla all'avvio,
quando nessuno sta digitando. Implementata come un `UITextField` fuori schermo
che prende e rilascia il fuoco.

**Misurato: non serve e fa danno.** Il precaricamento bloccava il thread
principale **1932 ms all'avvio**, e all'apertura di Cerca il thread si bloccava
**comunque per altri 2050 ms**. Il costo veniva pagato due volte invece che
zero.

Motivo: un `UITextField` generico costruisce una tastiera con una
configurazione diversa da quella di Cerca (correzione disattivata, maiuscole
per parole, tasto "cerca"), e il lavoro non si trasferisce.

⚠️ E c'è un motivo per cui la strada è chiusa **comunque**: per costruire
davvero la tastiera bisogna tenerla attiva qualche decimo di secondo, e la
tastiera è interfaccia di sistema — comparirebbe visibilmente sulla home per
mezzo secondo all'avvio dell'app. Il primo tentativo non faceva quel lampo
proprio perché rilasciava subito il fuoco, ed è per questo che non serviva a
niente. `PrecaricaTastiera.swift` è stato cancellato; il commento resta in
`CosaGuardoApp.swift`.

### Decodificare le locandine fuori dal thread principale

`UIImage(data:)` non decodifica niente: costruisce un oggetto che tiene i byte
compressi e rimanda la decodifica al momento del disegno, che avviene **sul
thread principale**. Sembrava la spiegazione perfetta dei blocchi durante il
caricamento della home. Aggiunto `preparingForDisplay()` in `CacheImmagini`.

**Misurato: peggiorato.** I sei blocchi passati da ~6,2 s a ~9,8 s
complessivi, tutti peggiori in modo coerente. Al tatto: uguale. Modifica
annullata.

Poi il colpo di grazia alla teoria, da un `curl` all'API:

```
poster_url: "https://image.tmdb.org/t/p/w342/..."
```

**w342** = 342 pixel di larghezza, disegnata a 132 punti su schermo 3× = 396
pixel. **L'immagine è già più piccola di come viene mostrata**: non c'è niente
da ridimensionare, e dodici JPEG da 342 pixel non costano due secondi. Quindi i
blocchi all'avvio **non sono le immagini**, e tutta la pista era sbagliata
dall'inizio.

### ⚠️ Il limite dello strumento di misura

`Sonda.swift` (solo Debug) misura il ritardo con cui una `Task.sleep` riprende
sul thread principale, e stampa quando supera una soglia. Ha funzionato
benissimo sul caso della tastiera, dove il blocco era davvero sul thread
principale — il risultato è stato confermato anche a mano.

⚠️ Ma una `Task` può riprendere in ritardo **anche quando il thread principale
è libero**: basta che sia congestionato il pool di thread cooperativi che Swift
usa per il lavoro asincrono. `preparingForDisplay()` fa esattamente questo, e
la sonda registrava un ritardo che probabilmente non era affatto l'interfaccia
bloccata.

**La sonda non misura "thread principale fermo" come dice la sua etichetta.
Misura "qualcosa è congestionato".** Da leggere sempre insieme alla sensazione
a dita, che sul caso tastiera ha confermato e su questo ha smentito.

C'è anche un cronometro in `CampoRicerca` che stampa i millisecondi fra la
comparsa del campo e la prima lettera. ⚠️ **Quel numero contiene il tempo di
reazione di chi digita** e non misura la lentezza: serve solo a marcare il
momento nella sequenza. Prima correzione: 3343 ms "di ritardo" erano in realtà
2050 ms di blocco più circa un secondo di dito umano.

---

## 42.13 — Aperto, in ordine di importanza

1. ⚠️ **TestFlight non installa l'app.** Bloccante vero per la
   ripresentazione: tutto il resto è migliorabile, questo impedisce di andare
   avanti. Log del dispositivo: `failureReason: Error Downloading Install
   Data`. Già escluse: build di sviluppo da sostituire, versione iOS del
   widget, cambio di rete, cancellazione e reinstallazione, due build diverse
   su due schede App Store Connect diverse. Resta da provare: **un altro
   dispositivo**, e uscire/rientrare da Media e Acquisti sull'Apple ID.
2. **L'ordinamento della ricerca.** Cercando "Inter" deve uscire
   *Interstellar* per primo, non un ordine alfabetico. È lato server,
   `/search-fast`: serve un criterio di popolarità nel ranking.
3. **Il tasto cerca in tutte le schede**, non solo in Consigli.
4. **I ~6 secondi di blocchi fra avvio e caricamento home.** Le immagini sono
   escluse. Prossimo sospetto: la barra della tastiera aggiunta da
   `chiudiTastieraAlTocco()` in `RicercaView`, che è interfaccia che il sistema
   di input deve costruire e che `CercaView` non ha.
5. **`Invalid frame dimension (negative or non-finite)`**, due volte prima di
   aprire Cerca. Un calcolo di layout che produce un negativo o un NaN.
   `Poster` è il sospettato: è l'unico componente che calcola dimensioni da una
   larghezza che può essere `nil`. ⚠️ **Non esiste un `Poster.swift`**: il tipo
   è dichiarato dentro un altro file, da trovare con ⌘⇧F su `struct Poster`.
6. **`CardTrending` da allineare** alle altre card (vedi 42.6).
7. **Togliere `Sonda.swift`**, il cronometro in `CampoRicerca` e le due stampe
   in `AccessoApple.swift` prima dell'archivio finale.
8. **Ripensare la consulenza gratuita di App Review** prima di ripresentare:
   la posta in gioco dichiarata da Apple è *"may result in removal from the
   Apple Developer Program"*.

### Chiuso, non riaprire

- **I titoli in inglese**: falso allarme. `Titolo` ha un solo campo titolo e il
  server manda già l'italiano (verificato: *"Il monologo della Speziale"*,
  *"Clayface"*). L'app non sceglie male, non ha scelta.
- **Il ridimensionamento delle locandine**: impossibile, sono già w342, più
  piccole di come vengono disegnate.
- **La capability Sign in with Apple**: presente, su All.

---

## DECISIONI PERMANENTI — aggiunte (42)

- **Mai un gesto su un contenitore che racchiude controlli UIKit incapsulati**
- **Un ramo che esce in silenzio rende indistinguibili cause diverse**: in
  Debug stampare sempre prima di uscire, anche nei casi "normali"
- **Il ripristino della sessione sta nella radice dell'app**, mai in una
  linguetta: in un TabView le viste nascono quando le si apre
- **Un solo `.id()` per vista, e lo stesso valore che usa `scrollTo`**
- **Niente emoji nelle tastiere dove non servono**: `asciiCapable` vale due
  secondi alla prima apertura
- **Un componente card solo**, parametrizzato: due componenti gemelli divergono
- **Mai testo importante sopra un'immagine che non si controlla**
- **Un pulsante deve rispondere al tocco**: `.buttonStyle(.plain)` va sempre
  sostituito con uno stile che legge `isPressed`
- **Uno strumento di misura va validato prima di fidarsene**, e letto insieme
  alla sensazione a dita
- **Misurare in Release prima di correggere**: in Debug i numeri sono il caso
  peggiore, non quello degli utenti

---

# SEZIONE 43 — Ricerca, prestazioni, TestFlight (22/10 pomeriggio)

## 43.1 — Il contratto `tmdb_id`, rotto in due punti del percorso TV

⚠️ **Lo stesso difetto, trovato due volte nello stesso giorno in due posti
diversi.** Vale la pena fissarlo come regola, non come aneddoto.

**Primo punto — la ricerca.** `search_tv_series` restituiva `title`,
`display_title`, **`id`**, `_score`. Ma `Suggerimento` in `Modelli.swift` legge
**`tmdb_id`**, e quel campo non c'era. In `CercaView`:

```swift
if let id = s.tmdbId { NavigationLink { ... } }
else { RigaRisultato(suggerimento: s).opacity(0.5) }
```

Ogni risultato di una ricerca Serie TV arrivava senza identificativo, quindi
ogni riga veniva disegnata **spenta al 50% e non toccabile**. Un elenco
corretto e completamente inerte. Mancavano anche `poster_url` e `year`, che
arrivano nella stessa risposta TMDb senza costare una chiamata in piu': le
serie uscivano senza locandina mentre i film ce l'avevano, e sembravano due
funzioni diverse.

Aggiunti tutti e tre. `id` resta dov'era: lo usa l'autocomplete del sito.

**Secondo punto — i consigli.** In `main.py`, `_api_titolo_snello` faceva
`"tmdb_id": rec.get("tmdb_id")`. Ma `recommend_tv_from_seed_titles` produce
**`tv_id`**, non `tmdb_id`. Per le serie il campo usciva `null` — e in
`Modelli.swift` `tmdbId` **non e' opzionale**, quindi falliva la decodifica
dell'INTERA risposta e l'app mostrava *"Risposta non comprensibile dal
server"* invece dei consigli.

Corretto con un ripiego esplicito:

```python
"tmdb_id": rec.get("tmdb_id") or rec.get("tv_id") or rec.get("movie_id"),
```

⚠️ **La regola che ne esce**: un campo non opzionale in `Modelli.swift` e' un
contratto, e ogni percorso del server deve rispettarlo. Il percorso TV usa
nomi interni diversi (`tv_id`) e li lascia filtrare fino all'API. Quando si
aggiunge un endpoint o un formato, il controllo da fare e' *"questo campo
arriva valorizzato da TUTTI i percorsi, o solo da quello dei film?"*.

## 43.2 — L'ordinamento della ricerca: TMDb cerca per parole, non per prefissi

Sintomo riferito: cercando "Inter" non usciva *Interstellar*.

Prima ipotesi, sbagliata: il criterio di spareggio. Era la **popularity** di
TMDb, che non misura quanto un film e' noto ma **quante volte la scheda viene
consultata in questi giorni**. Un film uscito ieri puo' averla a 250,
*Interstellar* a 58. Corretto passando al **vote_count** — i voti accumulati
in tutta la vita del titolo, che non calano.

⚠️ Quella correzione esisteva gia' nel progetto: in `search_tv_series` c'era
il commento che spiegava esattamente questo ragionamento. Era stata applicata
alle serie e **mai portata sui film**.

Ma non bastava. Misurando con `sonda_ricerca.py` (script diagnostico, legge la
chiave da `TMDB_API_KEY`, non la contiene):

| query | dove sta il titolo famoso nei risultati TMDb | voti |
|---|---|---|
| `matr` | pagina 1, posizione 1 | 29.018 |
| `padri` | pagina 1, posizione 10 | 23.808 |
| `inter` | pagina 3, posizione **45** | 41.581 |
| `titan` | **non trovato in 120 risultati** | — |
| `break` (tv) | non trovato | — |

⚠️ **La ricerca di TMDb e' una ricerca PER PAROLE.** Chi ha "inter" come
parola intera (*Inter. Due Stelle sul Cuore*, 0 voti) vince su chi ce l'ha
come prefisso (*Interstellar*, 41.581 voti). E la profondita' necessaria e'
imprevedibile: 45 risultati per una query, oltre 120 per un'altra. **Nessun
numero di pagine va bene sempre**, e ogni pagina e' una chiamata HTTP a ogni
lettera digitata.

### La soluzione: il catalogo SEO come seconda fonte

`seo_titles` e' gia' li': ~700 titoli (350 film + 350 serie evergreen, piu' 300
uscite recenti), costruiti dalle top di `/movie/popular` e `/movie/top_rated`,
indicizzati per popularity, aggiornati ogni settimana per le pagine
`/dove-vedere`. Un `LIKE 'inter%'` ci mette un millisecondo e non tocca la
rete.

Ora **entrambe** le ricerche interrogano il catalogo **sempre**, non come
ripiego, e uniscono i risultati a quelli di TMDb. La query SQL e' una sola
(`righe_catalogo_seo` in `recommendation_api.py`); le due ricerche hanno
formati diversi e ognuna adatta per conto suo.

Le righe del catalogo non hanno `vote_count`, ma hanno un segnale piu' forte:
**la loro presenza li' dentro**. Valgono un bonus fisso di 1000 piu'
`log10(popularity)*100`, che resta sotto il salto fra un tier di match e
l'altro (2500) e quindi non scavalca mai un match migliore.

### Il match esatto va pesato sulla lunghezza della query

⚠️ Scoperto correggendo `titan`: *Titan* e *The Titan* sono match **esatti**
della parola digitata e battevano *Titanic*, che e' solo un prefisso.

"Esatto" e' una prova forte solo se l'utente ha scritto un titolo per intero.
Su un prefisso corto che per caso coincide con un titolo brevissimo non vuol
dire niente. Quindi:

```python
s += 5000 + max(0, len(q_lower) - 6) * 700
```

Sotto le 6 lettere un match esatto vale quanto un prefisso e a decidere resta
la fama; oltre, il bonus cresce fino a dominare. Verificato che non rompe il
caso opposto: scrivendo "interstellar" per intero il match esatto resta
nettamente primo (10377 contro 5523).

Applicato a film **e** serie, cosi' i due percorsi smettono di divergere.

⚠️ **La chiave di cache va alzata a ogni modifica dell'ORDINE**, non solo
della forma dei risultati: `search_movie_v4` → `v5` → `v6`. Dimenticarlo
significa servire la graduatoria vecchia per 24 ore e credere che la
correzione non funzioni. Il commento che lo avverte era gia' nel file.

**Limite noto**: `seo_titles` ha `popularity` ma non `vote_count`. Fra due
titoli del catalogo a pari match si ordina per popolarita' — la metrica
volatile di cui sopra. Fra titoli gia' filtrati per fama fa meno danno. Per
sistemarlo davvero basta aggiungere una colonna `vote_count` alla tabella e
riempirla al refresh settimanale: il codice e' gia' pronto a usarla.

## 43.3 — La lente in tutte le schede

Richiesta: il tasto cerca anche fuori da Consigli.

Fatto con **un modificatore solo**, `.conRicerca()` in `PulsanteCerca.swift`,
applicato a tutte e quattro le schede — e riscritta anche l'implementazione a
mano che `RicercaView` aveva. L'unica differenza legittima (in Consigli la
lente deve chiudere la tastiera dei tre campi titolo prima di aprire) passa da
un parametro opzionale.

⚠️ Non quattro copie: le card dei titoli erano gia' diventate **tre**
grammatiche diverse per lo stesso oggetto senza che nessuno lo avesse deciso
(vedi 42.6). Quattro lenti copiate sarebbero finite uguale.

Va applicato DENTRO il NavigationStack e DOPO il `.toolbar` della schermata,
cosi' la lente finisce all'estrema destra anche dove ci sono gia' altri
pulsanti (il filtro in Scopri).

## 43.4 — Prestazioni: quattro ipotesi, tutte cadute

Sintomo: ~6 secondi di blocchi fra avvio e caricamento della home.

1. **Il servizio Render che dorme.** Escluso: il piano e' a pagamento.
2. **La decodifica delle locandine sul thread principale.** Escluso con
   misura: `preparingForDisplay()` peggiorava i numeri (6,2 s → 9,8 s) e al
   tatto era uguale. Poi il colpo di grazia da un `curl` all'API: i poster
   sono **w342**, cioe' 342 pixel, disegnati a 132 punti su schermo 3× = 396
   pixel. **Sono gia' piu' piccoli di come vengono mostrati**: non c'e' niente
   da ridimensionare, e dodici JPEG da 342 pixel non costano due secondi.
3. **La rete lenta.** Escluso con misura. Aggiunto un cronometro per ogni
   richiesta in `Rete.swift` (solo Debug): stampa percorso e millisecondi
   sopra i 400 ms. Risultato:

   ```
   RETE: /api/v1/trending            — 945 ms
   RETE: /search-fast                — 478 ms
   RETE: /api/v1/detail/movie/876716 — 530 ms
   ```

   Tutte sotto il secondo. **Il server e' veloce.**
4. **Il resto era Debug.** In build Release l'avvio e' accettabile. I cinque
   secondi di blocchi misurati sono in larga parte l'assenza di ottimizzazioni
   di SwiftUI.

⚠️ **Conclusione operativa: la parte prestazioni e' chiusa.** Continuare a
limare su numeri che in produzione non esistono e' il modo migliore per
rompere qualcosa che funziona.

### Quello che resta, ed e' una correzione vera

**Timeout per tipo di chiamata** in `Rete.swift`. Erano 30 secondi per tutto,
impostati sulla sessione. Trenta secondi sono giusti per il motore dei
consigli e sbagliati per tutto il resto: se la striscia in cima alla home si
impianta, l'utente resta mezzo minuto davanti a una fascia decorativa. Ora:
12 s normale, 30 s solo `/api/v1/recommend`, 5 s il controllo di stato.

## 43.5 — ⚠️ I tre limiti di `Sonda`, lo strumento che ho scritto io

Vale piu' delle correzioni, perche' tre conclusioni sbagliate di oggi vengono
da qui.

1. **Non misura "thread principale fermo"**, come dice la sua etichetta.
   Misura il ritardo con cui una `Task.sleep` riprende sul main actor — e una
   Task puo' riprendere in ritardo anche a thread libero, se e' congestionato
   il pool di thread cooperativi che Swift usa per il lavoro asincrono.
   `preparingForDisplay()` fa esattamente questo, e la sonda ha registrato un
   ritardo che probabilmente non era affatto l'interfaccia bloccata.
2. **Il cronometro "prima lettera dopo N ms" contiene il tempo di reazione
   umano.** I 3343 ms misurati erano 2050 ms di blocco piu' circa un secondo
   di dito.
3. **Conta anche i secondi in cui l'app e' in secondo piano.** Misura con
   `Date()`, che continua a correre mentre l'app e' sospesa: i 14.317 ms
   registrati erano con ogni probabilita' il tempo passato a guardare Xcode
   sul Mac.

**Da leggere sempre insieme alla sensazione a dita.** Sul caso della tastiera
emoji ha funzionato e il risultato e' stato confermato; sulle locandine ha
mentito.

## 43.6 — Due fonti di `NaN` nel layout delle locandine

`Invalid frame dimension (negative or non-finite)`, due volte nel log. Non e'
cosmetico: SwiftUI lo assorbe ma rifa' i calcoli.

`Poster` sta in **`Risultatiview.swift`** (non esiste un `Poster.swift` — si
trova con ⌘⇧F su `struct Poster`).

1. **`.frame(maxWidth: .infinity)` seguito da `.aspectRatio()`.** Durante le
   passate di misura SwiftUI propone anche larghezze non finite — succede
   dentro gli ScrollView. A quel punto `aspectRatio` calcola
   `altezza = infinito × 1.5`. Ora la sagoma la fa `Color.clear` con la
   proporzione e il contenuto ci va sopra in overlay.
2. **Un'immagine di dimensione zero.** `scaledToFill()` calcola il rapporto
   larghezza/altezza: con entrambe a zero fa `0/0`, cioe' NaN. Capita con un
   file troncato o un formato che `UIImage` accetta ma non sa decodificare —
   l'oggetto esiste, la bitmap no. Ora e' trattato come fallimento.

⚠️ Non c'e' la prova di quale delle due fosse: per averla servirebbe
riprodurre il messaggio. Sono entrambe sbagliate di per se'.

## 43.7 — TestFlight: dove siamo

Nessuna installazione riuscita. **Il campo si e' pero' ristretto molto.**

**Il dato decisivo** viene dal log di Console del dispositivo: per
`com.cosaguardo.ios.CosaGuardo` **non c'e' NESSUNA attivita' di `installd`**.
Per l'app TestFlight, reinstallata nello stesso momento, il log mostra il
ciclo completo — staging, contenitori, firma, verifica, `Install successful`.
Per CosaGuardo, zero righe.

```
Claiming Next PostInstallStatusJob with bundle ID: com.cosaguardo.ios.CosaGuardo,
(v: (null), bevid: (null)), terminalReason: 1,
failureReason: Error Downloading Install Data, fullError: (null)
```

⚠️ **L'installazione non comincia nemmeno.** Il nome dell'errore va preso alla
lettera: *Downloading*. E `bevid` — build external version id — e' **null**:
TestFlight ha un lavoro in coda per quel bundle identifier e non sa quale
build scaricare.

**Escluso, con prove:**
- Il dispositivo — le beta di altri sviluppatori si installano senza problemi
  sullo stesso iPhone con lo stesso Apple ID (provata la beta pubblica di
  GitHub)
- L'app TestFlight — disinstallata e reinstallata
- Lo stato del tester — rimosso, riaggiunto, gruppo di test nuovo creato da zero
- La scheda App Store Connect — stesso errore su due schede con bundle
  identifier diversi
- Il build — quattro build, e comunque il pacchetto non viene mai scaricato
- La configurazione di archiviazione — Release
- I contratti — quello per le app gratuite e' Attivo (dal 21/10/2026)
- Lo stato del build — "Pronta per l'invio", nessuna conformita' mancante

**Corretti lungo la strada** (difetti veri, nessuno risolutivo):
- Il widget dichiarava **piu' destinazioni dell'app** (iPhone, iPad, Mac,
  Apple Vision contro il solo iPhone dell'app). Un'estensione non puo'
  supportare piu' destinazioni di chi la contiene.
- Il widget aveva **minimo iOS 26.2** contro il 17.0 dell'app.
- ⚠️ **`CFBundleVersion` disallineato** fra app ed estensione. Xcode lo
  segnalava in cima a tutti i warning con la parola "must", ed e' rimasto
  invisibile per giorni perche' sepolto sotto dieci warning di concorrenza
  Swift 6. **Errore mio**: per il build 2 avevo scritto di alzare il numero su
  entrambi i target, per il build 3 me ne sono dimenticato.

**Aperta una richiesta al supporto Apple** (developer.apple.com/contact →
App Store Connect → TestFlight, inclusa nell'iscrizione, non consuma i due
incidenti tecnici) con la diagnosi e la lista delle esclusioni.

⚠️ **Da non rifare**: altri cicli di archivio. Il pacchetto non viene
scaricato, quindi nessuna modifica al build puo' cambiare l'esito.

### RISPOSTA DI APPLE — 29/10/2026, caso 103005048626

Developer Support (Simone): *"The issue should be resolved within 48 hours,
please try to upload another build after 48 hours passes."*

**Era un problema loro.** Nessuna correzione richiesta al progetto.

⚠️ La diagnosi tecnica reggeva — `bevid: (null)` e zero righe di `installd`
dicevano che il pacchetto non veniva nemmeno scaricato — ma ci sono voluti
**giorni e quattro cicli di archivio** per arrivare a chiedere al supporto. La
lezione: quando le prove dicono che l'installazione *non comincia*, il campo
delle cause possibili non include il build, e continuare a ricostruirlo e'
tempo buttato. Aprire il caso prima.

**Da fare, dalla settimana del 2/11** (48 ore scadute sabato 31/10 ore 16):

1. Alzare `CFBundleVersion` su **entrambi** i target, app e widget, allineati
2. `Debug Information Format` su Release -> `DWARF with dSYM File`
3. Archive in Release, upload, prova di installazione da TestFlight

Le diagnostiche DEBUG (`Sonda`, i cronometri in `Rete` e `CampoRicerca`, i due
print in `AccessoApple`) restano: sono sotto `#if DEBUG` e in Release non
vengono compilate. Si tolgono prima dell'archivio definitivo per lo Store.

Se fallisce di nuovo, si riapre sul **numero di caso**, non da zero.

---

## 43.8 — Aperto

1. **TestFlight** — risposta ricevuta il 29/10: problema di Apple, risolto
   entro 48 ore. Nuovo build da caricare nella settimana del 2/11 (vedi la
   lista in 43.7). Bloccante per la ripresentazione; non blocca lo sviluppo,
   l'app gira da Xcode.
2. **`CardTrending` da allineare** alle altre card (vedi 42.6): e' la terza
   grammatica, titolo sopra la locandina, nella schermata che si apre per
   prima.
3. **Togliere la diagnostica prima dell'archivio finale**: `Sonda.swift`, il
   cronometro in `CampoRicerca`, le due stampe in `AccessoApple.swift`, il
   cronometro `RETE:` in `Rete.swift`. Sono tutti in `#if DEBUG` e in Release
   non esistono, ma non devono restare li' per sempre.
4. **`vote_count` in `seo_titles`** — vedi il limite noto in 43.2.
5. **I dieci warning di concorrenza Swift 6** (*"Main actor-isolated
   conformance of X to Decodable"*) su `RispostaNovita`, `RispostaRaccolta`,
   `OpzioniFiltri` e simili. Non c'entrano con niente di oggi, ma sono il
   rumore che ha nascosto per giorni il warning sul `CFBundleVersion`.
6. **`Debug Information Format`** su Release va messo a **DWARF with dSYM
   File**: oggi il caricamento avverte che mancano i dSYM, e senza quelli un
   rapporto di crash in produzione non dice niente.

---

## DECISIONI PERMANENTI — aggiunte (43)

- **Un campo non opzionale in `Modelli.swift` e' un contratto**: va verificato
  che arrivi valorizzato da TUTTI i percorsi del server, non solo da quello
  dei film
- **TMDb cerca per parole, non per prefissi**: per l'autocomplete serve una
  fonte locale, e allargare il bacino non e' una soluzione
- **Il match esatto vale quanto la query e' lunga**: su un prefisso corto non
  e' una prova
- **La chiave di cache si alza quando cambia l'ORDINE**, non solo la forma
- **Il tie-breaker della fama e' `vote_count`, mai `popularity`**: la seconda
  misura la tendenza di oggi, non la notorieta'
- **Uno strumento di misura va validato prima di fidarsene**, e letto sempre
  insieme alla sensazione a dita
- **Misurare in Release prima di ottimizzare**: in Debug i numeri sono il caso
  peggiore, non quello degli utenti
- **Un timeout unico per tutta l'app e' sbagliato**: lo decide il tipo di
  chiamata
- **Mai `.frame(maxWidth: .infinity)` prima di `.aspectRatio()`**
- **Un'unica implementazione per gli elementi che si ripetono fra schermate**
  (card, lente): due copie gemelle divergono senza che nessuno lo decida

---

# SEZIONE 44 — Pubblicazione social: tre difetti, e la rete (25-26/10)

## RIEPILOGO

Tre problemi segnalati da Marco sulla pubblicazione automatica, sistemati uno
alla volta: caroselli che non arrivavano su TikTok, carosello Instagram
tagliato ai lati, sottotitoli del duo troppo bassi e fuori dai bordi. Lungo la
strada sono emersi due problemi non segnalati — gli errori di rete che
uccidevano i montaggi e i caricamenti, e un comando usato per un altro.

File toccati: `carosello.py`, `pubblica.py`, `monta.py`, `reel.py`, `main.py`.

---

## 44.1 — Caroselli su TikTok: PNG rifiutato, e il fallimento invisibile

Sintomo: i caroselli uscivano solo su Instagram, su TikTok mai.

⚠️ **Il difetto peggiore non era il formato, era che nessuno lo sapeva.**
L'endpoint foto di TikTok (`/v2/post/publish/content/init/`) accetta **solo
JPEG e WEBP**, e `carosello.py` salvava PNG. Ma il rifiuto arriva **dopo** che
`init` ha gia' restituito un `publish_id`: la chiamata sembrava riuscita, la
riga risultava pubblicata, e il contenuto non compariva mai nella casella.

Due interventi, in quest'ordine:

1. **Prima la visibilita'**, in `main.py`: `_tiktok_attendi_esito(publish_id)`
   interroga `_tiktok_stato` ogni 5 secondi per 60, e su `FAILED` solleva con
   il motivo tradotto (dizionario `_TIKTOK_MOTIVI`). Se e' ancora in lavorazione
   esce senza sollevare. Chiamata da entrambi i rami, video e foto.
2. **Poi la causa**, in `carosello.py`: `salva(img, cartella, nome)` scrive
   JPEG q=92 `subsampling=0`.

[DECISION] Strumentare prima di correggere, anche avendo gia' il sospetto
giusto. Il sospetto era PNG e lo era davvero, ma senza il punto 1 la prossima
causa diversa sarebbe stata di nuovo invisibile.

Di conseguenza `_notifica_pubblicazione` ha cambiato regola per la didascalia:
`_serve_didascalia = bool(esiti.get("tiktok_bozza")) or bool(esiti.get("tiktok_errore"))`
— prima, quando il carosello falliva in silenzio, su Telegram non arrivava
nemmeno il testo da incollare.

⚠️ `pubblica.py` legge le slide con `_ESTENSIONI_SLIDE = (".jpg", ".jpeg",
".png")` e in caso di collisione **vince il JPEG**: le cartelle vecchie
continuano a funzionare.

---

## 44.2 — Il carosello Instagram tagliato ai lati

Sintomo: il carosello su IG non era piu' a tutta pagina, veniva tagliato di
lato.

**Il conto.** Instagram limita i caroselli a **4:5**. Un'immagine 9:16 dentro
quel contenitore occupa `1.25 x 9/16 = 70%` della larghezza, cioe' **15% di
margine per parte**. Non e' un ritaglio: e' l'immagine che non riempie.

[DECISION] **Due set di slide, non un ritaglio al volo.** `carosello.py`
produce le 9:16 come prima e, in `dest/instagram/`, le stesse ritagliate a
1080x1350 (`ritaglio_instagram`, finestra `(0, 285, 1080, 1635)`). Il `footer()`
e' salito da `BOTTOM_LIMIT - 20` a `BOTTOM_LIMIT - 70` per non finire dentro il
taglio.

`pubblica.py` rileva `cartella/"instagram"` e **la usa solo se il numero di
slide coincide**; altrimenti ripiega sul set unico. `carica_tutto()` e' stato
estratto dai due punti di caricamento e popola `extra["media_instagram"]`, che
`_social_pubblica` usa per il ciclo del carosello IG (`nomi_ig` / `url_ig`),
con controllo di esistenza del file e ripiego su `url`.

⚠️ Il ripiego sul numero di slide e' quello che rende la modifica retroattiva:
una cartella generata prima di oggi non ha `instagram/` e si comporta come
sempre, invece di rompersi.

---

## 44.3 — I sottotitoli del duo, e una fascia che non esisteva

Sintomo riferito: sottotitoli troppo bassi, finiscono sotto la didascalia del
post, e a volte escono dai bordi laterali.

**Il secondo difetto, risolto subito.** `WrapStyle: 2` significa *"non andare
mai a capo da solo"*: l'unico ritorno ammesso e' un `\N` scritto a mano. Era
stato scelto per tenere le tre parole su una riga, ma con parole lunghe la riga
supera i 1080 pixel e **esce dallo schermo invece di spezzarsi** — e in quella
modalita' i margini laterali **non vengono nemmeno letti**. Passato a
`WrapStyle: 0`, aggiunto `SOTTO_MARGINE_LATO = 90`, margine basso da 210 a
**400**, piu' le opzioni `--margine` e `--margine-lato`.

### ⚠️ La diagnosi incompleta, e cosa l'ha corretta

Con il margine a 400 Marco ha segnalato che un titolo su piu' righe finiva
sopra i sottotitoli. La mia prima risposta — *"il margine e' unico per tutto il
reel mentre l'altezza del titolo cambia, quindi va sistemato in `reel.py`"* —
era giusta ma **sottostimava il problema**.

Ho renderizzato le schermate con la fascia dei sottotitoli disegnata sopra.
Risultato: **sforavano tutte**, anche quelle con il titolo su una riga sola —
in "DARK" ci finiva dentro l'anno. Non si vedeva solo perche' li' sotto
passavano parole corte.

⚠️ **Disegnare il vincolo invece di ragionarci.** Il conto a mano diceva "il
caso a due righe e' stretto"; il rendering ha detto "non ce n'e' uno che non
sfora". Trenta righe di banco di prova, e sono servite anche dopo, per il
punto successivo.

### La fascia, e perche' prima non poteva funzionare

In `reel.py`:

```python
FONDO_CONTENUTO = H - SOT_MARGINE - int(SOT_CORPO*SOT_INTERLINEA)*SOT_RIGHE - 20
```

Sotto quella linea non si scrive. Il titolo, se lungo, **rimpicciolisce**
(`corpo_adattivo`, scaletta 76 -> 40) invece di crescere verso il basso.
Poster a 440x660, tutto alzato di ~110 px. Stessa regola applicata al
sottotitolo dell'hook.

[DECISION] **Poster e inizio del titolo alla stessa altezza su tutte le
schermate.** Farli scorrere in base alla lunghezza del titolo sarebbe stato
piu' semplice, ma nel video montato il poster sobbalzerebbe a ogni cambio
schermata. A cedere e' il corpo del testo, mai la posizione.

### ⚠️ Il pezzo mancante: la fascia non aveva un'altezza fissa

Costruito il lato `reel.py`, restava un buco nel ragionamento: **perche' una
fascia si possa riservare, deve avere un'altezza costante** — e non ce
l'aveva. Misurato su voiceover reali (37 gruppi): 21 a una riga, 16 a due,
**zero a tre**. Ma con parole italiane lunghe, tre:

```
PRIMA (3 parole fisse): 18 gruppi -> righe {1: 6, 2: 9, 3: 3}
DOPO  (con tetto):      20 gruppi -> righe {1: 6, 2: 14}
```

`raggruppa_parole(parole, corpo, lato)` in `monta.py` spezza il gruppo prima
che possa superare `SOTTO_RIGHE_MAX = 2`. Il tetto si stima dai **caratteri**
(`SOTTO_LARGHEZZA_MEDIA = 0.78` del corpo), non misurando il font: trovare il
`.ttf` a partire dal nome *"Arial Black"* funziona sul PC di Marco e si rompe
altrove. La stima e' prudente per scelta — al massimo spezza un gruppo che
sarebbe entrato, e due parole al posto di tre non si notano; il contrario sì.

Costo: due gruppi in piu' su diciotto. Il karaoke `\k` resta identico.

⚠️ **`reel.py` e `monta.py` sono ora accoppiati**: `SOT_MARGINE` / `SOT_CORPO`
/ `SOT_RIGHE` devono restare uguali a `SOTTO_MARGINE_BASSO` / `SOTTO_CORPO` /
`SOTTO_RIGHE_MAX`. Scritto nei commenti di entrambi. In pratica: lanciare
`monta.py --margine` con un valore diverso da 400 **le schermate non lo sanno**.

---

## 44.4 — Riuso degli audio

Un montaggio interrotto a meta' ricomprava tutta la voce da ElevenLabs.
`voce_blocco()` tiene la cache in `<cartella>/_voce` — **fuori da
`_lavorazione`**, che viene cancellata a fine montaggio. Stampa `[generata]` /
`[riusata]` per blocco, e `--rigenera-voce` la ignora.

⚠️ **La chiave e' il TESTO, non il numero del blocco** (sha1 di
`voce|modello|testo`). Con la chiave sul numero, correggere una riga del
voiceover avrebbe riusato in silenzio l'audio vecchio: video con i sottotitoli
nuovi e la voce che dice un'altra cosa, **senza nessun errore a segnalarlo**.
Conseguenza utile: riordinare i blocchi non ricompra niente.

Cache illeggibile o mp3 mancante -> rigenera, non e' un errore. Cache non
scrivibile -> avvisa e prosegue.

---

## 44.5 — Gli errori di rete, e perche' non si riprova tutto

Due volte in un'ora, a un giorno di distanza: `socket.gaierror: [Errno 11001]
getaddrinfo failed`, settanta righe di traceback, prima su `api.elevenlabs.io`
(montaggio) poi su `cosaguardo.com` (caricamento di sei cartelle).

⚠️ **`getaddrinfo failed` non puo' mai essere un problema di chiave o di
account**: la risoluzione del nome avviene *prima* di qualunque autenticazione,
la connessione non si e' mai aperta. Marco aveva ipotizzato "forse non ero
loggato in ElevenLabs" — lo script si autentica con una API key, e una chiave
sbagliata da' 401/403. Confermato: `ping` falliva, `nslookup ... 8.8.8.8`
rispondeva `34.8.184.191`. Era il DNS della sua rete di casa.

In `monta.py` (`tts_elevenlabs`) e in `pubblica.py` (`_apri`, usata da `chiama`
e `carica_media`): tre tentativi, e un messaggio che dice cosa guardare al
posto dello stack. 401/403/429 non si riprovano — il server ha risposto.

### [DECISION] In `pubblica.py` si riprova SOLO se la richiesta non e' partita

Si riprova su `socket.gaierror` e `ConnectionRefusedError` — gli unici due casi
in cui si sa per certo che il server non ha visto niente. **Non** su timeout o
connessione caduta a meta'.

⚠️ Una POST su `/coda-social` che il server ha eseguito e la cui risposta si e'
persa, se rimandata **accoda il post due volte**. Vale qui la stessa regola di
41.x sul claim atomico della coda: *il peggio che puo' capitare a una coda di
pubblicazione non e' saltare un post, e' pubblicarlo due volte.* Un caricamento
da rifare a mano costa meno.

Verificato con rete simulata: DNS -> 3 tentativi; connessione rifiutata -> 3;
timeout -> 1; HTTP 403 -> 0 tentativi e risollevato al chiamante.

**Consiglio dato a Marco, non ancora applicato**: cambiare i DNS sul **router**
di casa (1.1.1.1 / 8.8.8.8), non sulla scheda di rete del portatile. Quella
scheda la usa anche in azienda, e un DNS pubblico fisso gli impedirebbe di
risolvere i nomi interni aziendali.

---

## 44.6 — ⚠️ `pubblica.py --settimana` non e' `settimana.py`

Marco ha lanciato `python pubblica.py --settimana output_caroselli` pensando di
generare la settimana nuova, e si e' trovato davanti alla conferma per caricare
**33 caroselli vecchi** — sei settimane di contenuti gia' pubblicati in
passato, e per giunta generati prima dei fix di 44.1 e 44.2. Fermato in tempo.

I due comandi hanno nomi simili e fanno cose opposte:

| Comando | Cosa fa |
|---|---|
| `settimana.py --dal lunedi` | **genera** 6 contenuti nuovi, uno alla volta con `[s/m/r/n]`, poi carica |
| `pubblica.py --settimana CARTELLA` | **carica** tutte le sottocartelle di una cartella gia' pronta |

⚠️ `--annulla` prende **un id alla volta**: disfare 33 righe sarebbe stato
33 comandi.

⚠️ `pubblica.py --settimana` carica i file **come stanno sul disco in quel
momento**. Riaccodare cartelle vecchie dopo un fix ai generatori rimette in
coda esattamente i difetti appena corretti: i contenuti vanno **rigenerati**,
non riaccodati.

Quando poi il caricamento e' fallito per DNS, `settimana.py` si e' comportato
bene: ha stampato il comando `pubblica.py --cartelle ...` per riprendere senza
rigenerare niente e senza rispendere crediti ElevenLabs.

---

## 44.7 — Aperto

- **DNS di casa** da sistemare sul router (vedi 44.5)
- **`SOT_RIGHE = 2` e' una scelta, non una misura dell'esecuzione**: se un
  giorno i sottotitoli cambiassero corpo o margine, le schermate vanno
  riallineate a mano
- **LinkedIn**: valutato e messo in stand by il 26/10. La pagina aziendale e'
  automatizzabile (Community Management API, entita' legale + pagina
  verificata + review 1-4 settimane) ma senza reach; il profilo personale ha
  reach ed e' self-serve (`w_member_social`) ma il token dura 60 giorni senza
  refresh. Gli **Article nativi non si pubblicano via API** — quello che la
  Posts API chiama `article` e' un post con link. Fermato perche' il profilo di
  Marco (promozioni gaming) non e' attinente alla nicchia streaming.

---

## 44.8 — Da ricordare

- **Un `init` che restituisce un id non e' una pubblicazione riuscita**: se
  l'esito arriva dopo, va interrogato
- **Strumentare prima di correggere**, anche con il sospetto giusto in mano
- **Disegnare il vincolo invece di ragionarci**: il rendering ha trovato quello
  che il conto a mano aveva sottostimato
- **Una fascia si puo' riservare solo se ha altezza costante**: altrimenti e'
  una speranza, non un vincolo
- **La chiave di una cache audio e' il testo, mai l'indice**
- **`getaddrinfo failed` non e' mai un problema di credenziali**
- **Non riprovare una richiesta che puo' essere gia' stata eseguita**, quando
  il doppione costa piu' del rifare a mano
- **Due comandi con nomi simili e effetti opposti** sono una trappola: vale la
  pena saperlo prima di averne uno davanti alla conferma

---

# SEZIONE 45 — Play Store, misurazione, campagne (29-30/10)

## RIEPILOGO

Partiti da una domanda sulle norme Google Play, finiti a scoprire che **gli
aggiornamenti dell'app Android erano bloccati da fine agosto** e che il modo in
cui leggevamo i dati di GA4 era fuorviante. Pubblicata la 1.0.3 con target SDK
36, corretto l'ordine del consenso GA4, misurate le campagne Meta sui numeri
veri, aggiunta la richiesta di valutazione sul Play Store.

⚠️ Due diagnosi sbagliate per strada, entrambe mie. Stanno in 45.3 perche'
sono piu' istruttive dei risultati.

---

## 45.1 — targetSdk 36: una violazione aperta da due mesi, invisibile

Aprendo Play Console per un'altra ragione:

```
L'app deve avere come target Android 16 (livello API 36) o versioni successive
Livello API target rilevato: Android 15 (API 35)
Avviso inviato: 21 lug 2026 · Data applicazione: 31 ago 2026
"Non potrai rilasciare aggiornamenti dell'app"
```

⚠️ **Nei nostri appunti l'app risultava "API 36, costruita dopo aver verificato
che PWABuilder producesse targetSdk 36".** Google ne vedeva 35. La verifica era
stata fatta sull'output di PWABuilder, non sul bundle effettivamente caricato —
e il bundle in produzione era rimasto indietro.

⚠️ **La regola che ne esce**: il target SDK si verifica **sulla riga del bundle
in Play Console** (colonna "SDK target"), mai su quello che dichiara lo
strumento che l'ha prodotto. E' l'unico posto dove il numero e' quello che
Google legge davvero.

Costo del difetto: due mesi in cui qualunque aggiornamento sarebbe stato
rifiutato, senza che nessuno se ne accorgesse perche' nessun aggiornamento e'
stato tentato.

### Il percorso di correzione

[DECISION] **Rigenerare da PWABuilder invece di installare Bubblewrap in
locale.** I manutentori hanno annunciato che tutti i pacchetti Android ora
puntano ad API 36, e con l'antivirus aziendale che blocca gia' gli script
PowerShell, montare Node + JDK + SDK Android su quel PC sarebbe stata una
serata. Verificato dopo: il bundle esce davvero a 36.

⚠️ **I pacchetti PWABuilder non contengono il progetto sorgente** (nessun
`twa-manifest.json` sul disco), quindi non si poteva modificare `build.gradle`:
si poteva solo rigenerare. Alla prossima occasione conviene spuntare
**"Include source code"**.

---

## 45.2 — La chiave di upload: tre pacchetti, una chiave sola

In `Downloads` c'erano tre cartelle `CosaGuardo - Google Play package*`,
ciascuna con il suo `signing.keystore`. Il rischio era che fossero tre chiavi
diverse e di firmare con quella sbagliata.

Verificato con `keytool -list -v`: **identiche**. Stesso numero di serie
`1a071eccbd26b2d6`, stessa data (10 lug 2026), stesso SHA-256.

I due fingerprint pubblici in `assetlinks.json`:

| | |
|---|---|
| `17:35:4B:A6:...:48:56:EE` | chiave di firma di Play (la tiene Google) |
| `84:BE:C5:9C:...:C8:81:85` | **chiave di upload di Marco** — corrisponde ai tre keystore |

⚠️ **`assetlinks.json` e' pubblico**: per sapere quale chiave serve non c'e'
bisogno di entrare in Console, basta aprire
`cosaguardo.com/.well-known/assetlinks.json`. Utile da ricordare.

⚠️ **Rigenerare da PWABuilder senza fornire la propria chiave** crea un
keystore nuovo: l'upload viene rifiutato e il fingerprint in `assetlinks.json`
non corrisponde piu', quindi la TWA si aprirebbe con la barra del browser.
Opzione **"Use mine"**, alias `my-key-alias`.

### [BUG] Due campi adiacenti, uno solo compilato

Primo upload rifiutato: *"Il codice di versione 1 e' gia' stato utilizzato"*.
In PWABuilder ci sono **"App version"** (il nome, `1.0.3`) e **"App version
code"** (l'intero, `4`), uno sotto l'altro. Compilato il primo, il secondo era
rimasto a 1.

Nota utile: l'errore sul codice di versione e' arrivato *dopo* i controlli su
package ID e firma, quindi era gia' la prova che quelle due cose erano giuste.

### Pubblicazione

Caricato prima su **Test interni** per leggere la colonna "SDK target" senza
toccare la produzione, poi promosso. In produzione: **1.0.3 (versionCode 4),
SDK target 36, API 24+**.

---

## 45.3 — ⚠️ Due diagnosi sbagliate sul consenso GA4, e quella giusta

I dati GA4 mostravano un quadro allarmante: **retention settimana 1 al 2,6%**,
**nuovi utenti ≈ utenti totali** (1,3K su 1,3K). La firma classica di un
identificatore che non persiste.

### Errore 1 — ho scambiato il suo opt-out per un difetto

Ispezionando cosaguardo.com dal browser di Marco: nessuno script di
googletagmanager caricato, `dataLayer` con il solo `consent default: denied`,
nessuna richiesta a `/collect`. Ne ho concluso che **GA non parte per chi ha
gia' accettato i cookie**.

Era il suo **opt-out personale**: `?optout=1` scrive `cg_optout` in
localStorage e `loadGA4()` esce subito. Funzionava come progettato.

⚠️ Avevo cercato le chiavi di consenso con un filtro `/cons|cook|gdpr|cmp|
privacy/i` che **non intercetta `cg_optout`**, e ho letto l'assenza di GA come
un difetto invece che come una scelta. Un filtro troppo stretto nasconde
esattamente la riga che spiegherebbe tutto.

### Errore 2 — ho dato per stracciato un file che era intero

`base.html` aveva quattro blocchi `<script>` con il solo commento e niente
sotto (banner cookie, service worker, install banner, drawer), e
`resetCookieConsent()` richiamata ma mai definita. Ho concluso che l'allegato
avesse perso del codice e ho rifiutato di restituire il file.

Lo stesso file in zip e' arrivato **con md5 identico**: non era l'allegato.
Quel codice sta in `static/js/base.js` (48 KB), e i commenti in `base.html`
sono segnaposto rimasti da uno spostamento. Verificato scaricando `base.js` dal
sito: contiene `resetCookieConsent`, `beforeinstallprompt`, `serviceWorker`,
`cg-drawer`.

⚠️ La prudenza non era sbagliata — restituire un file che cancella quattro
funzionalita' e' un danno silenzioso. Era sbagliata la **spiegazione**: ho
dedotto una causa invece di verificarla, e bastava un md5.

### La diagnosi giusta

In `loadGA4()`, l'ordine delle chiamate:

```js
gtag('js', new Date());
gtag('set', 'user_properties', {...});
gtag('config', 'G-HMNFXJ98H1', {...});                        // il page_view parte QUI
gtag('consent', 'update', {'analytics_storage': 'granted'});  // il consenso arriva DOPO
```

`config` e' cio' che fa partire il primo `page_view`, e in quel momento il
consenso e' ancora il default `denied`. La documentazione Google e' esplicita:
il default vale finche' non arriva l'update, e l'update va chiamato il prima
possibile. Il `page_view` di ogni caricamento partiva quindi **senza
identificatore persistente**; gli eventi successivi, dopo l'update, ce
l'avevano.

Correzione: `consent update` spostato **prima** di tutto il resto nella
funzione.

⚠️ `base.js` chiama `loadGA4()` e **non ha `config` ne' `consent update`
propri**: la correzione in quella funzione copre entrambi i percorsi, chi ha
gia' accettato e chi accetta in quel momento. Verificato leggendo `base.js`.

**Da verificare a meta' novembre**: se la coorte settimanale sale dal 2,6%,
era questo. Se resta li', la causa e' altrove — ma almeno su dati puliti.

---

## 45.4 — Le campagne: i numeri veri, e perche' GA4 ne vede un quinto

### Google Ads e' fermo

`Search-1-cosaguardo`: **in pausa**, budget 6,50 €/giorno non speso, 0
impressioni e 0,00 € in 30 giorni. Anche il secondo account (campagne
affiliazioni/scommesse) e' tutto in pausa.

⚠️ L'assenza di "Paid Search" nei canali GA4 **non era un collegamento rotto**,
era la realta'. Avevo sollevato il sospetto il giorno prima: ritirato.

### Meta, 2-29 ottobre

| Campagna | Spesa | Landing page view | €/LPV |
|---|---|---|---|
| App Android — traffico Play Store | 278,79 € | 3.601 | 0,077 |
| CosaGuardo - Traffico - Ads video | 250,63 € | 2.499 | 0,100 |
| **Totale** | **529,42 €** | **6.100** | **0,087** |

€18,91 al giorno. La campagna video e' migliorata del **25%** fra la prima e la
seconda meta' del mese (0,117 → 0,088 €/LPV) a volume crescente; quella app e'
stabile.

⚠️ **Errore di lettura mio, corretto**: avevo calcolato "€1,15 a sessione"
dividendo la spesa per le sessioni GA4. GA4 non puo' fare da denominatore,
perche' `loadGA4()` parte **solo se il visitatore accetta i cookie**. La
metrica di Meta e' `omni_landing_page_view`, cioe' pagine caricate davvero: i
7-10 centesimi di Marco erano corretti.

### Il rapporto fra le fonti

| | Meta dichiara | Fonte indipendente | Rapporto |
|---|---|---|---|
| Campagna sito | 2.499 LPV | 487 sessioni GA4 | **19%** |
| Campagna app | 3.601 LPV | 565 installazioni Play | **16%** |

Due letture diverse:

- Il **19%** del sito non e' uno spreco: e' il cancello del consenso. Il
  traffico a pagamento arriva freddo, dentro il browser interno di Instagram e
  Facebook, dove il localStorage spesso non persiste e il banner ricompare ogni
  volta. ⚠️ **Non giudicare mai le campagne dai numeri di sessione di GA4**:
  per il volume si usa Meta, GA4 descrive solo chi accetta.
- Il **16%** dell'app non e' una perdita: e' la **conversione della scheda
  Play** (visite alla scheda → installazioni). €0,49 per installazione e' un
  buon prezzo; la leva e' quel 16%, non il costo del clic.

### Il ritorno, che e' la domanda vera

La campagna sito compra 2.499 visite al mese per €250. In GA4: 130
`select_provider`, cioe' clic sui link affiliati — **€1,93 per clic
affiliato**, su programmi che pagano sull'abbonamento e non sul clic.

- [stated] **NOW TV: affiliazione attiva da circa un mese, mai convertito.**
- TIMVision ancora da attivare su Awin.

[DECISION] Marco tiene le campagne come sono per ora. Registrato qui perche'
sia una decisione e non un'inerzia: e' meta' del budget senza ritorno
misurabile.

### Se un giorno serve il volume vero

Contarlo **lato server**: un contatore aggregato per UTM nel FastAPI non ha
bisogno di consenso perche' non identifica nessuno. Mezza giornata.

---

## 45.5 — Richiesta di valutazione sul Play Store

La scheda aveva **zero valutazioni** ("Valutazione media: –") su 319
installazioni attive. Una scheda senza stelle converte peggio nella ricerca
dello store: e' la leva piu' economica sul 16% di 45.4.

[DECISION] **Lato web, non In-App Review API.** Quella vuole codice nativo nel
guscio TWA, che PWABuilder rigenera da zero a ogni aggiornamento del target SDK
— se lo porterebbe via al primo giro senza che nessuno se ne accorga. Il
blocco lato web sopravvive a ogni ripacchettamento, e riusa `cgAppContext()`
che gia' esisteva.

Regole di attivazione:

- solo contesto `android_app` (PWA e iOS non possono valutare quella scheda)
- dopo **3 giorni distinti** di uso — non caricamenti di pagina
- solo dopo un momento positivo (clic su una piattaforma), e al caricamento
  **successivo**: chiedere mentre l'utente apre Netflix significa farsi ignorare
- "Valuta" → mai piu'; "Non ora" → fra 60 giorni
- ⚠️ **nessun filtro sul gradimento**: mostrare la richiesta solo a chi si
  dichiara soddisfatto viola le linee guida degli store

Eventi GA4: `rating_prompt_shown`, `rating_prompt_clicked`,
`rating_prompt_dismissed`. Prova senza aspettare: `cgRateDebug()` da console.

⚠️ **[BUG] trovato nel mio stesso codice prima di consegnarlo**: il ripiego da
`market://` a `https` scattava **sempre** dopo il timeout, anche quando il Play
Store si era aperto — chi ce l'ha si sarebbe ritrovato anche la pagina web
aperta dietro. Corretto annullando il ripiego su `visibilitychange`/`pagehide`.

Logica provata su 11 casi (primo avvio, giorni insufficienti, momento mancante,
gia' valutata, rinviata ieri, rinviata 70 giorni fa, PWA, iOS, browser, doppio
caricamento in giornata): tutti con l'esito atteso.

---

## 45.6 — Norme Google Play, aggiornamento del 29/10

Per CosaGuardo non c'era niente da fare.

- **Trader status**: niente in sospeso. Account **Personale**, nessun profilo
  pagamenti, app gratuita → per Google e' non-trader e non chiede nulla. La
  novita' di novembre riguarda chi guadagna sullo store.
- ⚠️ **Da sapere per quando monetizzera'**: il profilo sviluppatore pubblico
  avvisa che *"se scegli di guadagnare su Google Play, verra' mostrato
  pubblicamente l'indirizzo della tua sede legale completo"*. L'account e'
  personale e l'indirizzo e' quello di casa. Se non lo vuole pubblico, va
  passato a organizzazione **prima** di monetizzare, non dopo.
- **Da segnare**: febbraio 2027, nuovi requisiti su memoria e ottimizzazione
  del codice, che Google dichiara incidere su *"visibility and publishing
  capabilities"*.
- Non applicabili: Local Network, Manage Contacts, AppFunctions, abbonamenti
  con sconti, All Files Access, Accessibility, SMS/Call Log, Giappone, XR.

---

## 45.7 — Aperto

- **Domani**: verificare che la violazione sul target SDK sia sparita
- **Settimana del 2/11**: TestFlight (vedi 43.7)
- **8-10 novembre**: GA4, ripresa dei canali social dopo i fix del 25/10
- **Meta' novembre**: GA4, coorte settimanale dopo il fix del consenso
- **DNS del router di casa**: tre blocchi in due giorni (ElevenLabs,
  caricamento contenuti, `git push`)
- **Bug navigazione indietro app Android**, aperto dal 15/08: "Breaking Bad +
  The Office (US)" → tornando indietro escono i risultati di "Breaking Bad +
  This Is Us". E' l'unico difetto funzionale aperto che vedono gli utenti.
- **`CardTrending`** da allineare alle altre card (iOS, vedi 42.6)
- **Awin**: TIMVision da attivare; NOW TV attiva ma a zero conversioni
- [stated] **`patch_migliori` e' in produzione** — confermato da Marco il 30/10

---

## 45.8 — Da ricordare

- **Il target SDK si legge sulla riga del bundle in Play Console**, non
  sull'output dello strumento che l'ha prodotto
- **`assetlinks.json` e' pubblico**: dice quali chiavi sono registrate senza
  entrare in Console
- **Un errore tardivo nella catena di controlli e' una prova sui controlli
  precedenti**: il rifiuto sul codice di versione diceva che package ID e firma
  erano gia' a posto
- **Caricare su Test interni per leggere un numero** costa cinque minuti e
  evita di scoprire il problema dopo aver pubblicato
- ⚠️ **Un filtro di ricerca troppo stretto nasconde la riga che spiega tutto**
  (`cg_optout` fuori dal pattern)
- ⚠️ **Dedurre una causa non e' verificarla**: "l'allegato ha perso del codice"
  si smontava con un md5
- **GA4 misura solo chi accetta i cookie**: non e' un denominatore valido per
  il traffico a pagamento, mai
- **Una scheda store senza stelle converte peggio**, e la richiesta di
  valutazione va messa lato web se il guscio nativo viene rigenerato
