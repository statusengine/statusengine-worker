# Audit-Route durch den Worker

Eine Lesereihenfolge für das Code-Review vor dem Prod-Einsatz. Nicht als
Referenz gedacht, sondern als Route: von vorne nach hinten lesbar, und jeder
Durchgang setzt voraus, dass der vorherige gelesen wurde.

Der Umfang ist überschaubarer, als er wirkt. Von 14.077 Zeilen Go sind 7.775
Produktivcode und 6.302 Tests. Davon sind wiederum drei Dateien
(`registry.go`, `main.go`, `rabbitmq.go`) fast ein Drittel — der Rest ist klein
und einzeln lesbar.

**Zu Zeilennummern:** dieses Dokument nennt vorrangig Funktionsnamen, weil die
stabil bleiben. Wo Zeilennummern stehen, sind sie ein Sprungziel für den Stand
vom 14.08.2026, kein Versprechen.

---

## Die vier Durchgänge

| # | Durchgang | Dateien | Frage, die er beantwortet |
|---|---|---|---|
| 1 | Das Skelett | `cmd/app/main.go` | Was wird in welcher Reihenfolge gestartet? |
| 2 | Der Weg eines Events | `queue/router.go`, `queue/registry.go`, `db/db.go` | Was passiert mit einer Nachricht? |
| 3 | Die Nebenläufigkeit | `queue/gearman.go`, `websocket/hub.go`, `db/db.go` | Wer läuft parallel, wer wartet auf wen? |
| 4 | Die Sonderfälle | `queue/downtime_processor.go`, `queue/processor.go`, `registry.go` | Wo weicht der Code vom Muster ab? |

Halten Sie beim Lesen `CLAUDE.md` daneben. Die Architekturregeln 1–6 dort sind
keine nachträgliche Beschreibung, sondern die Vorgabe, gegen die der Code
geschrieben wurde — der Code verweist an vielen Stellen namentlich darauf.

---

## Durchgang 1 — Das Skelett

**Nur eine Datei: `cmd/app/main.go`, und darin nur `main()` ab Zeile 524.**
Alles darüber ist Konfigurationsauflösung (Flag > Env > YAML > Default); die
können Sie im ersten Durchgang überspringen und später gezielt nachlesen.

`main()` ist bewusst als nummerierte Sequenz geschrieben. Die Startreihenfolge:

1. **Konfiguration und Logger** — `loadConfig()`, `setupLogger()`
2. **`pipelineCtx`** wird angelegt. Wichtig für das Verständnis des ganzen
   Prozesses: dieser Context ist **nicht** der primäre Abschaltmechanismus,
   sondern ein Sicherheitsnetz. Er wird erst gecancelt, *nachdem* Consumer und
   Puffer schon sauber heruntergefahren sind.
3. **WebSocket-Hub** in eigener Goroutine (`hub.Run(pipelineCtx)`)
4. **`/ws`-HTTP-Server** auf `cfg.listenAddr`, Default `127.0.0.1:8080`
5. **`/metrics`-HTTP-Server** auf eigenem Port, Default `:9105` — bewusst ein
   getrennter Listener, damit ein Scrape nie die Request-Queue des
   WebSocket-Servers teilt
6. **MySQL**: `sql.Open`, `configureDBPool`, `PingContext` (5s),
   `logConnectionCharset`
7. **Graphite-Client** wird gebaut — aber noch nicht verbunden
8. **`queue.NewRouter(...)`** liefert `Router` (12 Queues) und `[]Runner`
   (15 Stück). Jeder Runner bekommt seine eigene Goroutine.
9. **Consumer** (`gearman` oder `rabbitmq`) wird gebaut und gestartet

Danach blockiert `main()` auf `<-sigCtx.Done()`.

**Worauf Sie hier achten sollten:** die drei Dinge, die *nicht* über `wg`
laufen. Die beiden HTTP-Server und der Drain-Loop für `rawMessages` (Zeile 628)
werden mit blankem `go func()` gestartet und nicht in die WaitGroup
aufgenommen. Bei den HTTP-Servern ist das korrekt — sie werden am Ende über
`Shutdown()` beendet. Ob der Drain-Loop sauber terminiert, hängt daran, dass
`consumer.Stop()` den Kanal schließt; das ist der Punkt, den Durchgang 3
aufgreift.

---

## Durchgang 2 — Der Weg eines Events

Nehmen Sie eine Nachricht auf `statusngin_hoststatus` und verfolgen Sie sie.

```mermaid
flowchart TD
    A[gearmand liefert Job] --> B[AddFunc-Closure<br/>queue/gearman.go:121]
    B --> C{beginHandler<br/>Consumer gestoppt?}
    C -->|ja| Z[Job als fehlgeschlagen melden]
    C -->|nein| D[out-Kanal, best effort<br/>voll = verwerfen]
    D --> E[observeHandler<br/>queue/router.go:56]
    E --> F[repairUTF8<br/>CP1252 reparieren]
    F --> G[Handler aus dem Router]
    G --> H[decodeHostStatus<br/>JSON-Bulk-Array]
    H --> I[pro Item: publish<br/>Hub, nicht blockierend]
    H --> J[pro Item: Enqueue<br/>BLOCKIERT bei vollem Puffer]
    J --> K[BulkInserter.Run<br/>eigene Goroutine]
    K --> L{100 Zeilen<br/>oder 250ms?}
    L --> M[ein Multi-Row-INSERT]
```

Die Kette in Dateien:

| Schritt | Ort |
|---|---|
| Job-Annahme | `queue/gearman.go`, `Start()` → `w.AddFunc(...)` |
| Zähler, UTF-8-Reparatur, Dauermessung | `queue/router.go`, `observeHandler` |
| Zuordnung Queue → Handler | `queue/registry.go`, `NewRouter` (Zeile 721) |
| Decode | `queue/decode.go`, `decodeHostStatus` |
| Broadcast | `queue/router.go`, `publish` |
| Persistenz | `db/db.go`, `Enqueue` → `Run` → `flushBuffer` |

**Die drei Stellen, an denen ich beim Review genau hinsehen würde:**

`NewHandler` in `router.go:106` ist das Muster, dem acht der zwölf Queues
folgen. Sie ist generisch über den Item-Typ und in vier Zeilen zu lesen — wenn
Sie die verstanden haben, verstehen Sie den Großteil des Routers.

`publish` in `router.go:152` prüft `hub.HasClients()` und überspringt das
JSON-Marshalling komplett, wenn niemand verbunden ist. Das ist der heißeste
Pfad im Worker. Die Konsequenz ist bewusst in Kauf genommen: die Antwort ist
eine Momentaufnahme, ein sich gerade verbindender Client verpasst das Event.

`Enqueue` in `db/db.go:179` ist **die einzige Stelle im ganzen Datenpfad, die
blockiert.** Wenn `b.in` (Kapazität 100) voll ist, wartet der Handler. Genau
das ist beabsichtigt: der Rückstau wandert über den Handler und den
Concurrency-Cap des Consumers bis zum Broker, wo er einen Worker-Neustart
überlebt. Regel 2 in `CLAUDE.md` beschreibt das im Detail.

---

## Durchgang 3 — Die Nebenläufigkeit

Der Teil mit dem höchsten Risiko und deshalb der, für den Sie sich am meisten
Zeit nehmen sollten.

### Wer läuft

Bei laufendem Betrieb mit Gearman-Backend und einem verbundenen Dashboard:

| Goroutine | Anzahl | Start | Ende |
|---|---|---|---|
| `hub.Run` | 1 | `main.go:549` | `pipelineCtx` gecancelt |
| `BulkInserter.Run` | 14 | `main.go:603` | `pipelineCtx` oder `b.in` geschlossen |
| `graphite.Client.Run` | 1 | ebenda (als 15. Runner) | dito |
| `/ws`- und `/metrics`-Server | 2 | `main.go:560`, `:572` | `Shutdown()` |
| `rawMessages`-Drain | 1 | `main.go:628` | Kanal geschlossen |
| `w.Work()` (Bibliothek) | 1 | `gearman.go:165` | `w.Close()` |
| Job-Handler | 0…64 | von der Bibliothek | pro Job |
| `logStatsPeriodically` | 1 | `gearman.go:166` | `statsDone` oder ctx |
| ctx-Wächter | 1 | `gearman.go:168` | ruft `Stop()` |
| `readPump`/`writePump` | 2 pro Client | `client.go:133` | Verbindungsende |

Die Job-Handler sind der einzige unbegrenzt wachsende Posten — und genau
deshalb gedeckelt. `-gearman-max-concurrent-jobs` (Default 64) ist kein
Durchsatzregler, sondern der Schutz gegen unbegrenzten Speicherverbrauch beim
Neustart nach einem Ausfall. Die Begründung steht ausführlich über
`NewGearmanConsumer` in `gearman.go:67`.

### Die Kanäle

| Kanal | Kapazität | Bei „voll" |
|---|---|---|
| `BulkInserter.in` | 100 | **blockiert** — der einzige Rückstaupunkt |
| `BulkInserter.flushReq` / `pauseReq` | 0 | Rendezvous mit `Run` |
| `Hub.broadcast` | 1024 | verwirft, zählt `publish_dropped_total` |
| `Hub.register` / `unregister` | 0 | Rendezvous mit `Run` |
| `Client.send` | 256 | verwirft für diesen Client |
| Consumer-`out` | 256 | verwirft (nur Observability) |

Die Asymmetrie ist die Kernaussage von Regel 4: **auf dem Weg zur Datenbank
wird gewartet, auf dem Weg zum WebSocket wird verworfen.** Ein langsamer
Browser darf die Ingestion nie ausbremsen.

### Wer auf wen wartet

Drei Wartepunkte, alle beim Herunterfahren:

- `GearmanConsumer.Stop()` wartet auf `handlerWG` — alle laufenden Job-Handler.
  Davor wird `stopped = true` unter Schreibsperre gesetzt, damit die WaitGroup
  nicht mehr wachsen kann. Die Begründung dieses Musters steht im Feldkommentar
  bei `stoppedMu` (`gearman.go:56`) und ist einer der Punkte, die ich beim
  Audit zweimal lesen würde.
- `flushRunners` wartet auf alle 15 Runner — **nebenläufig**, mit 10s
  Gesamtbudget. Der Kommentar bei `main.go:423` erklärt, warum sequenziell
  falsch war.
- `wg.Wait()` in `main()` wartet auf Hub und alle Runner-Loops.

`WithPaused` in `db/db.go:217` ist ein vierter, seltener Fall: der
Core-Restart-Handler hält den BulkInserter an, um selbst ein Statement gegen
dieselbe Tabelle abzusetzen. Wenn Sie eine Deadlock-Quelle suchen, ist das die
interessanteste Stelle im Repo.

---

## Durchgang 4 — Die vier Sonderfälle

Acht Queues folgen dem Muster aus Durchgang 2. Diese vier nicht:

**Perfdata** (`queue/processor.go`) — `NewPerfdataHandler` routet jede Metrik
nach `perfdataRoute` in MySQL, nach Graphite oder in beides (Regel 5). Der
Graphite-Client wird immer gestartet; wenn die Route ihn ausschließt, wird
schlicht nie `Enqueue` aufgerufen und keine Verbindung aufgebaut.

**Downtimes** (`queue/downtime_processor.go` + `db/downtime.go`) — der einzige
Bereich, der die BulkInserter-Abstraktion komplett umgeht. Eine Nachricht kann
je nach Typ INSERT, UPDATE oder DELETE über zwei Tabellenpaare auslösen.
`DetermineDowntimeActions` ist reine Logik ohne Datenbank und dadurch gut
testbar. **Lesen Sie vorher `.claude/specs/downtime_ablauf.txt`** — die
Zustandsmatrix dort ist die Spezifikation, der Code ist nur ihre Umsetzung.

**Core-Restart** (`registry.go:359`) — leert bei `object_type` 102 die
Status-Tabellen. Nutzt `WithPaused`, siehe oben. Das Verhalten hängt an
`enableOpenITCockpitTweaks`.

**Notifications** (`registry.go:228` und `:280`) — filtert nach NEB-Typ (605
bzw. 601) und teilt einen Eingangsstrom auf zwei Tabellen (Host/Service) auf.
Hier lohnt ein Blick darauf, ob die Filterkonstanten zu Ihrer Naemon-Version
passen.

---

## Der Shutdown

Der heikelste Teil und in `main.go:633` bewusst durchnummeriert. Die
Reihenfolge trägt die ganze Zusage aus Regel 6:

1. `consumer.Stop()` — **zuerst**, damit nichts Neues mehr hereinkommt
2. `flushRunners(flushCtx, runners)` — 10s, alle 15 nebenläufig
3. `cancelPipeline()` — beendet die (nun leeren) Run-Loops und den Hub
4. `wg.Wait()`
5. `sqlDB.Close()`
6. `httpServer.Shutdown()`, `metricsServer.Shutdown()` — 5s

Die Absicherung gegen Datenverlust liegt tiefer, in `BulkInserter.Run`: sowohl
bei `ctx.Done()` als auch beim geschlossenen Eingangskanal wird erst
`drainPending()` und dann `finalFlush()` gerufen. `finalFlush` baut sich einen
eigenen 5s-Context, weil der reguläre zu diesem Zeitpunkt schon abgelaufen ist.
Ohne diesen Umweg wäre der letzte Batch still verloren.

**Prüfen Sie das als Erstes praktisch**, nicht nur lesend: Worker unter Last
mit SIGTERM beenden und die Zeilenzahlen vorher/nachher vergleichen.

---

## Prüfpunkte

Konkrete Invarianten, die halten müssen — die Punkte, an denen ein Fehler
teuer wäre:

- [ ] **Kein Datenverlust bei SIGTERM unter Last.** Der Pfad
      `drainPending` → `finalFlush` ist die einzige Absicherung.
- [ ] **`-mysql-max-open-conns` ≥ Anzahl Runner**, sonst serialisiert der Pool
      den Shutdown-Flush innerhalb des 10s-Budgets. Default 25 gegen 15 Runner
      passt; bei mehr Queues mitwachsen lassen.
- [ ] **`-gearman-max-concurrent-jobs` ist nie 0.** `gearman.Unlimited` *ist*
      0, ein versehentliches Nullen stellt genau das unbegrenzte Verhalten
      wieder her, das der Cap verhindern soll.
- [ ] **API-Keys sind gesetzt.** Ohne Konfiguration erzeugt `resolveAPIKeys`
      einen Zufallsschlüssel pro Start und loggt ihn als Warnung — der Stream
      ist nie offen, aber nach jedem Neustart anders.
- [ ] **`-listen-addr` bleibt auf Loopback**, außer Sie exponieren bewusst.
- [ ] **Kein `charset=` in der MySQL-DSN.** Der Treiber handelt utf8mb4 im
      Handshake aus; `logConnectionCharset` warnt beim Start, wenn nicht.
- [ ] **Die `age_*`-Werte in der `config.yml` sind bewusst gesetzt.** `0`
      schaltet eine Tabelle ab — geerbte Semantik des PHP-Workers.
- [ ] **Alarm auf `statusengine_websocket_publish_dropped_total`**, nicht auf
      `messages_dropped_total`. Der erste bedeutet, dass der Hub insgesamt
      nicht mehr mitkommt; der zweite ist bei einem langsamen Browser-Tab
      normal.
- [ ] **`statusengine_queue_jobs_in_flight` dauerhaft am Cap** heißt: der
      Rückstau liegt beim Broker. Ein zweiter Worker-Prozess macht das
      schlimmer, nicht besser — der Engpass liegt hinter der Annahme.

---

## Was nicht im Worker-Prozess läuft

Damit Sie es beim Audit nicht doppelt lesen: `cmd/db_cleanup` (Retention,
für Cron/systemd-Timer), `cmd/db_verifier` (read-only Vergleich gegen die
PHP-Datenbank), `cmd/simulator`, `cmd/gearman_publisher` und
`cmd/rabbitmq_publisher` (Testwerkzeuge) sind eigenständige Binaries. Keines
davon läuft im Worker mit. `db_cleanup` teilt sich lediglich die YAML-Datei —
möglich, weil beide mit einfachem `yaml.Unmarshal` dekodieren und unbekannte
Schlüssel in beide Richtungen ignorieren.

Für den Prod-Einsatz ist von diesen nur `db_cleanup` relevant, und dort vor
allem eine Frage: läuft es in einem Cluster auf **genau einem** Knoten? Der
PHP-Worker warnt ausdrücklich davor, das parallel laufen zu lassen.

---

## Grenzen dieser Route

Damit klar ist, was dieses Dokument *nicht* leistet: es ist eine
Lesereihenfolge, kein Audit-Ergebnis. Ich habe den Aufrufgraph verifiziert und
die Nebenläufigkeitsstellen benannt, aber keine Sicherheitsanalyse der
Auth-Pfade gemacht, keine SQL-Injektionsprüfung über alle Query-Builder
gezogen und die Downtime-Zustandsmatrix nicht gegen die Spezifikation
durchgerechnet.

Insbesondere die Query-Konstruktion in `db/downtime.go` baut Tabellen- und
Spaltennamen per String-Konkatenation zusammen. Das ist hier vertretbar, weil
diese Namen ausschließlich aus Konstanten stammen und Werte durchgehend über
Platzhalter gehen — aber es ist eine Stelle, an der ein späterer Umbau leise
gefährlich werden kann, und damit ein guter Kandidat für Ihren eigenen zweiten
Blick.
