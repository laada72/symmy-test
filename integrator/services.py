"""Business logic for the integrator application."""

import hashlib
import json
import logging
import time
from collections.abc import Callable
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def transform_products(raw_products: list[dict]) -> list[dict]:
    """
    Apply business rules to raw ERP products.

    Rules:
    - Calculate price_vat_incl = price_vat_excl * 1.21 (rounded to 2 decimals)
    - Skip products with null or negative price (log warning)
    - Sum all stock values into stock_quantity (int or "N/A" → 0)
    - Set color from attributes, default "N/A"
    """
    result: list[dict] = []

    for product in raw_products:
        price = product.get("price_vat_excl")

        if price is None or price < 0:
            logger.warning(
                "[transform_products] Skipping product %s: invalid price %s",
                product.get("id"),
                price,
            )
            continue

        # Stock aggregation
        stocks = product.get("stocks") or {}
        stock_quantity = sum(v if isinstance(v, int) else 0 for v in stocks.values())

        # Color resolution
        attributes = product.get("attributes") or {}
        color = attributes.get("color") or "N/A"

        result.append(
            {
                "sku": product["id"],
                "title": product["title"],
                "price_vat_excl": price,
                "price_vat_incl": round(price * 1.21, 2),
                "stock_quantity": stock_quantity,
                "color": color,
            }
        )

    return result


class TokenBucketRateLimiter:
    """Token bucket rate limiter s injektovatelným zdrojem času."""

    def __init__(
        self,
        max_rps: int = 5,
        clock_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        """Inicializuje rate limiter s daným maximálním počtem požadavků za sekundu.

        Args:
            max_rps: Maximální počet požadavků za sekundu (výchozí 5).
            clock_fn: Funkce vracející aktuální čas v sekundách.
                Výchozí ``time.monotonic``, lze nahradit pro testování.
        """
        self._max_rps = max_rps
        self._min_interval = 1.0 / max_rps
        self._last_request_time: float = 0.0
        self._clock_fn = clock_fn

    def acquire(self) -> float:
        """Return wait time in seconds before next request.

        Returns:
            float: Seconds to wait (0.0 if no waiting needed).
        """
        now = self._clock_fn()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            return self._min_interval - elapsed
        return 0.0

    def record_request(self) -> None:
        """Record that a request was made, for computing next interval."""
        self._last_request_time = self._clock_fn()


class HashDeltaStrategy:
    """Strategie pro detekci změn produktů pomocí SHA-256 hashe.

    Vytváří kanonickou JSON reprezentaci produktu (seřazené klíče)
    a počítá z ní SHA-256 hash. Dva produkty se stejným hashem
    jsou považovány za identické.
    """

    def compute_hash(self, product: dict) -> str:
        """Vypočítá SHA-256 hash kanonické JSON reprezentace produktu.

        Args:
            product: Slovník s daty produktu.

        Returns:
            Hexadecimální řetězec SHA-256 hashe.
        """
        canonical = json.dumps(product, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class SyncStateManager:
    """Správce synchronizačního stavu produktů v Redis s fallbackem do PostgreSQL.

    Uchovává pro každé SKU hash obsahu, časové razítko poslední synchronizace
    a příznak synchronizace. Primárním úložištěm je Redis (rychlý přístup),
    sekundárním PostgreSQL (perzistence přes ``SyncRecord`` model).

    Attributes:
        REDIS_KEY_PREFIX: Prefix pro Redis klíče (``product_sync``).
        redis: Instance Redis klienta.
        strategy: Instance HashDeltaStrategy pro výpočet hashů.
    """

    REDIS_KEY_PREFIX = "product_sync"

    def __init__(self, redis_client):
        """Inicializuje správce s Redis klientem.

        Args:
            redis_client: Instance Redis klienta (``redis.Redis``).
        """
        self.redis = redis_client
        self.strategy = HashDeltaStrategy()

    def _key(self, sku: str) -> str:
        """Sestaví Redis klíč pro dané SKU.

        Args:
            sku: Identifikátor produktu.

        Returns:
            Redis klíč ve formátu ``product_sync:{sku}``.
        """
        return f"{self.REDIS_KEY_PREFIX}:{sku}"

    def filter_changed(self, products: list[dict]) -> tuple[list[dict], int]:
        """Porovná hashe produktů s uloženým stavem a vrátí pouze změněné.

        Pro každý produkt vypočítá hash a porovná ho s hodnotou uloženou
        v Redis. Produkty se shodným hashem jsou přeskočeny.

        Args:
            products: Seznam transformovaných produktů (musí obsahovat klíč ``sku``).

        Returns:
            Tuple ``(changed_products, skipped_count)`` — seznam změněných
            produktů a počet přeskočených (nezměněných).
        """
        changed: list[dict] = []
        skipped = 0

        for product in products:
            sku = product["sku"]
            new_hash = self.strategy.compute_hash(product)
            stored = self.redis.hget(self._key(sku), "content_hash")

            if stored is not None:
                # Redis may return bytes
                stored_str = stored.decode() if isinstance(stored, bytes) else stored

                if stored_str == new_hash:
                    skipped += 1
                    continue

            changed.append(product)

        return changed, skipped

    def mark_synced(self, sku: str, content_hash: str) -> None:
        """Zaznamená úspěšnou synchronizaci produktu do Redis a PostgreSQL.

        Primárně zapíše do Redis (výjimka se propaguje). Sekundárně
        zapíše do PostgreSQL přes ``SyncRecord`` model (chyba se pouze zaloguje).

        Args:
            sku: Identifikátor produktu.
            content_hash: SHA-256 hash aktuálního obsahu produktu.
        """
        # Redis write (primary) — exception propagates
        self.redis.hset(
            self._key(sku),
            mapping={
                "content_hash": content_hash,
                "last_synced": datetime.now(timezone.utc).isoformat(),
                "synced": "1",
            },
        )
        # PostgreSQL write (secondary) — error logged, not propagated
        try:
            from integrator.models import SyncRecord

            SyncRecord.objects.update_or_create(
                sku=sku,
                defaults={
                    "content_hash": content_hash,
                    "last_synced": datetime.now(timezone.utc),
                    "synced": True,
                },
            )
        except Exception:
            logger.warning(
                "[SyncStateManager] Failed to write SyncRecord for SKU %s",
                sku,
                exc_info=True,
            )

    def was_previously_synced(self, sku: str) -> bool:
        """Zjistí, zda byl produkt dříve synchronizován (pro rozhodnutí POST vs PATCH).

        Nejprve kontroluje Redis, při absenci záznamu se dotáže PostgreSQL.
        Pokud je záznam nalezen v DB, obnoví stav v Redis pro budoucí rychlý přístup.

        Args:
            sku: Identifikátor produktu.

        Returns:
            ``True`` pokud byl produkt dříve úspěšně synchronizován.
        """
        val = self.redis.hget(self._key(sku), "synced")
        if val is not None:
            return (val.decode() if isinstance(val, bytes) else val) == "1"
        # Fallback to PostgreSQL
        try:
            from integrator.models import SyncRecord

            record = SyncRecord.objects.get(sku=sku)
            if record.synced:
                # Restore Redis from DB
                self.redis.hset(
                    self._key(sku),
                    mapping={
                        "content_hash": record.content_hash,
                        "last_synced": record.last_synced.isoformat(),
                        "synced": "1",
                    },
                )
                return True
        except Exception:
            pass
        return False


def orchestrate_sync(
    products: list[dict],
    manager: SyncStateManager,
    api_client,  # EshopAPIClient - avoid circular import
) -> dict:
    """Orchestruje delta synchronizaci produktů do e-shopu.

    Filtruje změněné produkty, odesílá je do API (POST/PATCH podle
    předchozího stavu synchronizace) a aktualizuje sync stav.

    Args:
        products: Seznam transformovaných produktů.
        manager: Instance SyncStateManager pro správu stavu.
        api_client: Instance EshopAPIClient pro komunikaci s API.

    Returns:
        Slovník se souhrnem synchronizace::

            {
                "processed": int,        # celkový počet vstupních produktů
                "unchanged": int,         # počet přeskočených (nezměněných)
                "synced": int,            # počet úspěšně synchronizovaných
                "errors": int,            # počet chyb
                "failed_products": list,  # seznam {"sku": str, "error": str}
            }
    """
    changed, unchanged_count = manager.filter_changed(products)

    synced = 0
    errors = 0
    failed_products: list[dict[str, str]] = []

    for product in changed:
        sku = product["sku"]
        is_update = manager.was_previously_synced(sku)
        data, error = api_client.send_product(product, is_update=is_update)

        if error:
            logger.error("[orchestrate_sync] Failed to sync SKU %s: %s", sku, error)
            failed_products.append({"sku": sku, "error": error})
            errors += 1
            continue

        content_hash = manager.strategy.compute_hash(product)
        manager.mark_synced(sku, content_hash)
        synced += 1

    return {
        "processed": len(products),
        "unchanged": unchanged_count,
        "synced": synced,
        "errors": errors,
        "failed_products": failed_products,
    }
