"""Pure domain logic for the integrator module.

No imports from Django, Redis, or requests.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable
from typing import TypedDict

from integrator.ports import EshopPort, SyncStatePort

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HashDeltaStrategy
# ---------------------------------------------------------------------------


class HashDeltaStrategy:
    """Detects product changes via SHA-256 content hash.

    Produces a canonical JSON representation (sorted keys) and hashes it.
    Two products with the same hash are considered identical.
    """

    def compute_hash(self, product: dict) -> str:
        """Compute SHA-256 hash of the canonical JSON representation.

        Args:
            product: Product data dict.

        Returns:
            64-character hex SHA-256 digest.
        """
        canonical = json.dumps(product, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# TokenBucketRateLimiter
# ---------------------------------------------------------------------------


class TokenBucketRateLimiter:
    """Token bucket rate limiter with injectable clock.

    acquire() returns the wait time in seconds — it never calls time.sleep().
    The caller (adapter layer) is responsible for sleeping.
    """

    def __init__(
        self,
        max_rps: int = 5,
        clock_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self._max_rps = max_rps
        self._min_interval = 1.0 / max_rps
        self._last_request_time: float = 0.0
        self._clock_fn = clock_fn

    def acquire(self) -> float:
        """Return seconds to wait before the next request (0.0 if none needed)."""
        now = self._clock_fn()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            return self._min_interval - elapsed
        return 0.0

    def record_request(self) -> None:
        """Record that a request was made."""
        self._last_request_time = self._clock_fn()


# ---------------------------------------------------------------------------
# transform_products
# ---------------------------------------------------------------------------


def transform_products(raw_products: list[dict]) -> list[dict]:
    """Apply business rules to raw ERP products.

    Rules:
    - price_vat_incl = round(price_vat_excl * 1.21, 2)
    - Skip products with null or negative price (log warning)
    - stock_quantity = sum of int stock values ("N/A" → 0)
    - color from attributes, default "N/A"
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

        stocks = product.get("stocks") or {}
        stock_quantity = sum(v if isinstance(v, int) else 0 for v in stocks.values())

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


# ---------------------------------------------------------------------------
# SyncSummary TypedDict
# ---------------------------------------------------------------------------


class SyncSummary(TypedDict):
    processed: int
    unchanged: int
    synced: int
    errors: int
    failed_products: list[dict[str, str]]


# ---------------------------------------------------------------------------
# orchestrate_sync
# ---------------------------------------------------------------------------


def orchestrate_sync(
    products: list[dict],
    manager: SyncStatePort,
    api_client: EshopPort,
) -> SyncSummary:
    """Orchestrate delta sync: filter changed products, send to API, update state.

    Args:
        products: List of transformed products.
        manager: SyncStatePort implementation.
        api_client: EshopPort implementation.

    Returns:
        SyncSummary with processed/unchanged/synced/errors/failed_products.
    """
    strategy = HashDeltaStrategy()
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

        content_hash = strategy.compute_hash(product)
        manager.mark_synced(sku, content_hash)
        synced += 1

    logger.info(
        "[orchestrate_sync] Done: processed=%d unchanged=%d synced=%d errors=%d",
        len(products),
        unchanged_count,
        synced,
        errors,
    )

    return SyncSummary(
        processed=len(products),
        unchanged=unchanged_count,
        synced=synced,
        errors=errors,
        failed_products=failed_products,
    )
