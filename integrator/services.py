"""Business logic for the integrator application."""

import hashlib
import json
import logging
from datetime import datetime, timezone

from integrator.protocols import DeltaStrategy

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


class HashDeltaStrategy:
    """Simple JSON-based hash strategy for delta detection."""

    def compute_hash(self, product: dict) -> str:
        """SHA-256 of canonical JSON representation."""
        canonical = json.dumps(product, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class SyncStateManager:
    """Manages per-SKU sync state in Redis."""

    REDIS_KEY_PREFIX = "product_sync"

    def __init__(self, redis_client, strategy: DeltaStrategy | None = None):
        self.redis = redis_client
        self.strategy = strategy or HashDeltaStrategy()

    def _key(self, sku: str) -> str:
        return f"{self.REDIS_KEY_PREFIX}:{sku}"

    def filter_changed(self, products: list[dict]) -> tuple[list[dict], int]:
        """
        Compare product hashes with stored state.
        Returns (changed_products, skipped_count).
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
        """Update Redis with new hash and timestamp for SKU."""
        self.redis.hset(
            self._key(sku),
            mapping={
                "content_hash": content_hash,
                "last_synced": datetime.now(timezone.utc).isoformat(),
                "synced": "1",
            },
        )

    def was_previously_synced(self, sku: str) -> bool:
        """Check if SKU has been synced before (POST vs PATCH)."""
        val = self.redis.hget(self._key(sku), "synced")
        if val is None:
            return False
        return (val.decode() if isinstance(val, bytes) else val) == "1"
