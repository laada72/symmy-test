# symmy-test
Přijímací podušeníčko.

42

## Plánovaná vylepšení

1. **Lepší retry logika s exponenciálním backoffem (tenacity/urllib3)** — Současná retry logika při HTTP 429 čeká fixní dobu z `Retry-After` hlavičky. Exponenciální backoff s knihovnou tenacity nebo urllib3 Retry zajistí robustnější chování při opakovaných selháních a sníží zátěž na API.

2. **Přesun ESHOP_API_KEY do environment proměnné** — API klíč je aktuálně uložen v Django settings souboru. Přesun do environment proměnné zvýší bezpečnost a umožní snadnější rotaci klíčů bez změny kódu.

3. **Přesun SECRET_KEY do environment proměnné** — Django SECRET_KEY by neměl být uložen přímo v kódu. Přesun do environment proměnné je standardní bezpečnostní praxe pro produkční nasazení.

4. **Konfigurovatelný request timeout** — Timeout pro HTTP požadavky na e-shop API je aktuálně hardcoded. Konfigurovatelný timeout přes Django settings umožní přizpůsobení různým prostředím (dev, staging, produkce).

5. **Strukturované logování pomocí structlog** — Přechod na structlog umožní strojově čitelné logy (JSON formát), snadnější filtrování a agregaci v log management nástrojích (ELK, Datadog).

6. **Metriky a observabilita (Celery signals, Prometheus)** — Přidání metrik pomocí Celery signals a Prometheus exporteru umožní sledovat dobu běhu tasků, počet synchronizovaných produktů, chybovost a stav pipeline v reálném čase.

7. **Přesun mock endpointu z views.py do testovacích fixtures** — Mock endpoint pro e-shop API je aktuálně v produkčním `views.py`. Přesun do testovacích fixtures zajistí čistší oddělení produkčního a testovacího kódu.
