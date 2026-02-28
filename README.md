# Symmy Tasker: ERP → E-shop Integrace

Synchronizační můstek mezi ERP systémem a e-shopem postavený na Django, Celery, Redis a PostgreSQL.

## Architektura

```
┌──────────────┐      ┌────────────────────────────────────────────────────┐
│ erp_data.json│─────▶│  Celery Pipeline (chain)                          │
│  (ERP data)  │      │                                                    │
└──────────────┘      │  1. load_and_validate  ─▶  parsování + deduplikace │
                      │  2. transform          ─▶  DPH, stock, barva      │
                      │  3. delta_sync         ─▶  POST/PATCH do API      │
                      └──────────┬──────────────────────┬──────────────────┘
                                 │                      │
                      ┌──────────▼──────┐    ┌──────────▼──────────┐
                      │  Redis          │    │  E-shop API         │
                      │  (sync state +  │    │  POST /products/    │
                      │   Celery broker)│    │  PATCH /products/sk/│
                      └──────────┬──────┘    └─────────────────────┘
                                 │
                      ┌──────────▼──────┐
                      │  PostgreSQL     │
                      │  (fallback      │
                      │   sync state)   │
                      └─────────────────┘
```

### Komponenty

- **`integrator/tasks.py`** — Celery tasky zapojené do pipeline přes `chain`:
  1. `load_and_validate` — parsuje JSON, odstraní záznamy bez `id`, sloučí duplicity
  2. `transform` — přidá DPH (×1.21), sečte skladové zásoby, doplní barvu (`"N/A"`)
  3. `delta_sync` — porovná hashe s uloženým stavem, pošle jen změněné produkty

- **`integrator/services.py`** — business logika:
  - `transform_products` — transformační pravidla (DPH, stock, barva)
  - `HashDeltaStrategy` — SHA-256 hash pro detekci změn
  - `SyncStateManager` — správa sync stavu v Redis (primární) + PostgreSQL (fallback)
  - `TokenBucketRateLimiter` — dodržení limitu 5 req/s
  - `orchestrate_sync` — orchestrace celé synchronizace

- **`integrator/clients.py`** — `EshopAPIClient` s rate limitingem a retry logikou při HTTP 429

- **`integrator/models.py`** — `SyncRecord` model pro perzistentní sync stav v PostgreSQL

- **`integrator/views.py`** — mock endpoint e-shop API simulující reálné chování (429, 500, timeouty)

### Delta Sync

Produkty se odesílají jen pokud se změnily od poslední synchronizace. Detekce změn funguje přes SHA-256 hash kanonické JSON reprezentace produktu. Stav se ukládá primárně do Redis, sekundárně do PostgreSQL jako fallback.

### Rate Limiting & Retry

E-shop API má limit 5 req/s. Klient používá token bucket rate limiter a při HTTP 429 čeká dobu z hlavičky `Retry-After` a provede retry (viz. Možná vylepšení bod 1.).

## Spuštění

```bash
docker-compose up --build
```

Tím se spustí:
- **PostgreSQL** (port 5433)
- **Redis**
- **Django web server** (port 8000) — včetně mock API endpointu
- **Celery worker** — zpracovává synchronizační pipeline

### Spuštění synchronizace

Po nastartování kontejnerů spusťte management command:

```bash
docker-compose exec web python manage.py migrate
docker-compose exec web python manage.py json_file_sync
```

Případně s vlastním JSON souborem:

```bash
docker-compose exec web python manage.py json_file_sync --json-file /app/erp_data_modified.json
```

## Testy

```bash
docker-compose exec web pytest
```

## Technologie

- Python 3.11
- Django 6.0
- Celery + Redis (broker + sync state)
- PostgreSQL (databáze + fallback sync state)
- requests (HTTP klient)
- pytest + hypothesis + responses (testy)


## Možná vylepšení / diskuze

1. **Lepší retry logika (tenacity/urllib3/task)** — Současná retry logika při HTTP 429 čeká fixní dobu z `Retry-After` hlavičky (s fallbak na default 1s). Exponenciální backoff s knihovnou tenacity, urllib3 nebo separatni (make request) task s Retry zajistí robustnější chování při opakovaných selháních a sníží zátěž na API. Tento bod ma ovšem komplexní logiku, která závisí na znalosti chování a požadavků obou integrovaných apps.

2. **Přesun ESHOP_API_KEY do environment proměnné** — API klíč je aktuálně uložen v Django settings souboru. Přesun do environment proměnné zvýší bezpečnost a umožní snadnější rotaci klíčů bez změny kódu.

3. **Přesun SECRET_KEY do environment proměnné** — Django SECRET_KEY by neměl být uložen přímo v kódu. Přesun do environment proměnné je standardní bezpečnostní praxe pro produkční nasazení.

4. **Konfigurovatelný request timeout** — Timeout pro HTTP požadavky na e-shop API je aktuálně hardcoded. Konfigurovatelný timeout přes Django settings umožní přizpůsobení různým prostředím (dev, staging, produkce).

5. **Strukturované logování pomocí structlog** — Přechod na structlog umožní strojově čitelné logy (JSON formát), snadnější filtrování a agregaci v log management nástrojích (ELK, Datadog).

6. **Metriky a observabilita (Celery signals, Prometheus)** — Přidání metrik pomocí Celery signals a Prometheus exporteru umožní sledovat dobu běhu tasků, počet synchronizovaných produktů, chybovost a stav pipeline v reálném čase.
