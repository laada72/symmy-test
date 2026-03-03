# Symmy Tasker: ERP → E-shop Integrace

Synchronizační můstek mezi ERP systémem a e-shopem postavený na Django, Celery, Redis a PostgreSQL.
(Ultimátní řešení je samozřejmě 42, ale předpokladám, že s tím se bohužel nespokojíte)

## Architektura

Modul `integrator` je strukturován podle hexagonální (ports & adapters) architektury:

```
┌─────────────────────────────────────────────────────────────────────┐
│  Celery Pipeline (chain)                                            │
│                                                                     │
│  load_and_validate  ──▶  transform  ──▶  delta_sync                │
│  (tasks.py)              (tasks.py)      (tasks.py)                │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ volá
                ┌──────────────▼──────────────┐
                │  Domain Layer               │
                │  domain.py                  │
                │                             │
                │  transform_products()        │
                │  orchestrate_sync()          │
                │  HashDeltaStrategy           │
                │  TokenBucketRateLimiter      │
                │  SyncSummary                 │
                └──────┬──────────────┬────────┘
                       │ ports.py     │
              SyncStatePort      EshopPort
                       │              │
          ┌────────────▼──┐    ┌──────▼────────────┐
          │  SyncState-   │    │  EshopAPIClient   │
          │  Manager      │    │  (adapters.py)    │
          │  (adapters.py)│    │                   │
          └──────┬────────┘    └──────┬────────────┘
                 │                    │
        ┌────────▼──────┐    ┌────────▼──────┐
        │  Redis        │    │  E-shop API   │
        │  (primary)    │    │  POST/PATCH   │
        └────────┬──────┘    └───────────────┘
                 │ fallback
        ┌────────▼──────┐
        │  PostgreSQL   │
        │  SyncRecord   │
        └───────────────┘
```

### Vrstvy

**`integrator/ports.py`** — čisté Python `Protocol` rozhraní bez závislostí na infrastruktuře:
- `EshopPort` — kontrakt pro odesílání produktů do e-shopu
- `SyncStatePort` — kontrakt pro čtení a zápis stavu synchronizace

**`integrator/domain.py`** — čistá doménová logika bez Django/Redis/requests:
- `transform_products` — DPH (×1.21), agregace skladu, barva (`"N/A"`)
- `orchestrate_sync` — delta sync orchestrace přes porty
- `HashDeltaStrategy` — SHA-256 hash pro detekci změn
- `TokenBucketRateLimiter` — token bucket s injektovatelným clockem (neblokuje — vrací wait time)
- `SyncSummary` — TypedDict výsledku synchronizace

**`integrator/adapters.py`** — infrastrukturní adaptéry implementující porty:
- `EshopAPIClient` — HTTP klient s rate limitingem a retry logikou při HTTP 429
- `SyncStateManager` — Redis (primární) + PostgreSQL (fallback) správa sync stavu

**`integrator/tasks.py`** — Celery tasky zapojené do pipeline přes `chain`:
1. `load_and_validate` — parsuje JSON, odstraní záznamy bez `id`, sloučí duplicity (last wins)
2. `transform` — deleguje na `transform_products`
3. `delta_sync` — sestaví závislosti a deleguje na `orchestrate_sync`

**`integrator/models.py`** — `SyncRecord` Django model pro perzistentní sync stav v PostgreSQL

**`integrator/views.py`** — mock endpoint e-shop API simulující reálné chování (429, 500, timeouty)

### Delta Sync

Produkty se odesílají jen pokud se změnily od poslední synchronizace. Detekce změn funguje přes SHA-256 hash kanonické JSON reprezentace produktu. Stav se ukládá primárně do Redis; při Redis miss se provede fallback do PostgreSQL a Redis se obnoví.

### Rate Limiting & Retry

E-shop API má limit 5 req/s. `TokenBucketRateLimiter` v doménové vrstvě počítá potřebný wait time (nevolá `sleep` — to je zodpovědnost adaptéru). `EshopAPIClient` při HTTP 429 čeká dobu z hlavičky `Retry-After` a provede retry (max 3 pokusy).

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

Mimo Docker (bez PostgreSQL — DB testy se přeskočí):

```bash
python -m pytest integrator/tests/ --tb=short -q
```

Uvnitř Docker (včetně DB testů):

```bash
docker compose run web python -m pytest -v
```

DB testy se automaticky přeskočí pokud PostgreSQL není dostupný (TCP socket check při collection time).

## Technologie

- Python 3.11
- Django 6.0
- Celery + Redis (broker + sync state)
- PostgreSQL (databáze + fallback sync state)
- requests (HTTP klient)
- fakeredis (in-memory Redis pro testy)
- pytest + hypothesis + responses (testy)

## Možná vylepšení / diskuze

1. **Lepší retry logika (tenacity/urllib3/task)** — Současná retry logika při HTTP 429 čeká fixní dobu z `Retry-After` hlavičky (s fallback na default 1s). Exponenciální backoff s knihovnou tenacity, urllib3 nebo separátní (make request) task s Retry zajistí robustnější chování při opakovaných selháních a sníží zátěž na API. Tento bod má ovšem komplexní logiku, která závisí na znalosti chování a požadavků obou integrovaných apps.

2. **Přesun ESHOP_API_KEY do environment proměnné** — API klíč je aktuálně uložen v Django settings souboru. Přesun do environment proměnné zvýší bezpečnost a umožní snadnější rotaci klíčů bez změny kódu.

3. **Přesun SECRET_KEY do environment proměnné** — Django SECRET_KEY by neměl být uložen přímo v kódu. Přesun do environment proměnné je standardní bezpečnostní praxe pro produkční nasazení.

4. **Konfigurovatelný request timeout** — Timeout pro HTTP požadavky na e-shop API je aktuálně hardcoded. Konfigurovatelný timeout přes Django settings umožní přizpůsobení různým prostředím (dev, staging, produkce).

5. **Strukturované logování pomocí structlog** — Přechod na structlog umožní strojově čitelné logy (JSON formát), snadnější filtrování a agregaci v log management nástrojích (ELK, Datadog).

6. **Metriky a observabilita (Celery signals, Prometheus)** — Přidání metrik pomocí Celery signals a Prometheus exporteru umožní sledovat dobu běhu tasků, počet synchronizovaných produktů, chybovost a stav pipeline v reálném čase.

7. **SyncRecordPort abstrakce** — `SyncStateManager` aktuálně importuje `SyncRecord` přímo (záměrné zjednodušení). Zavedení `SyncRecordPort` protokolu by plně oddělilo doménovou vrstvu od Django ORM.
