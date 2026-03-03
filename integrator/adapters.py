"""Infrastructure adapters implementing domain ports.

EshopAPIClient  — HTTP adapter implementing EshopPort (duck typing)
SyncStateManager — Redis + PostgreSQL adapter implementing SyncStatePort (duck typing)

Note: SyncStateManager imports SyncRecord directly (intentional simplification).
Future improvement: introduce a SyncRecordPort abstraction.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import requests
from django.conf import settings

from integrator.domain import HashDeltaStrategy, TokenBucketRateLimiter
from integrator.models import SyncRecord

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# EshopAPIClient
# ---------------------------------------------------------------------------


class EshopAPIClient:
    """HTTP client for the e-shop REST API with rate limiting and retry logic.

    Structurally implements EshopPort (duck typing — no explicit inheritance).

    Attributes:
        base_url: Base URL of the e-shop API.
        api_key: API key for authentication.
        rate_limiter: TokenBucketRateLimiter instance.
    """

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        max_rps: int = 5,
        rate_limiter: TokenBucketRateLimiter | None = None,
    ) -> None:
        self.base_url = base_url or settings.ESHOP_API_BASE_URL
        self.api_key = api_key or settings.ESHOP_API_KEY
        self.rate_limiter = rate_limiter or TokenBucketRateLimiter(max_rps=max_rps)

    def _wait_for_rate_limit(self) -> None:
        wait_time = self.rate_limiter.acquire()
        if wait_time > 0:
            time.sleep(wait_time)
        self.rate_limiter.record_request()

    def send_product(
        self, product: dict, is_update: bool
    ) -> tuple[dict | None, str | None]:
        """Send a product via POST (new) or PATCH (update).

        Retries once on HTTP 429 after waiting Retry-After seconds (max 3 attempts).

        Returns:
            (response_data, None) on success, (None, error_message) on failure.
        """
        sku = product["sku"]
        payload = {
            "sku": sku,
            "title": product["title"],
            "price_vat_incl": product["price_vat_incl"],
            "stock_quantity": product["stock_quantity"],
            "color": product["color"],
        }
        headers = {"X-Api-Key": self.api_key}

        if is_update:
            url = f"{self.base_url}{sku}/"
            method = "PATCH"
        else:
            url = self.base_url
            method = "POST"

        self._wait_for_rate_limit()

        for attempt in range(3):
            try:
                resp = requests.request(
                    method, url, json=payload, headers=headers, timeout=1
                )
            except requests.RequestException as exc:
                error = (
                    f"[EshopAPIClient.send_product] Request failed for SKU {sku}: {exc}"
                )
                logger.error(error)
                return None, error

            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 1))
                logger.warning(
                    "[EshopAPIClient.send_product] Rate limited for SKU %s, "
                    "retrying after %.1fs (attempt %d/3)",
                    sku,
                    retry_after,
                    attempt + 1,
                )
                time.sleep(retry_after)
                self.rate_limiter.record_request()
                continue

            if resp.status_code >= 400:
                error = (
                    f"[EshopAPIClient.send_product] API error for SKU {sku}: "
                    f"HTTP {resp.status_code}"
                )
                logger.error(error)
                return None, error

            return resp.json(), None

        error = f"[EshopAPIClient.send_product] Max retries exceeded for SKU {sku}"
        logger.error(error)
        return None, error


# ---------------------------------------------------------------------------
# SyncStateManager
# ---------------------------------------------------------------------------


class SyncStateManager:
    """Redis + PostgreSQL adapter for sync state.

    Structurally implements SyncStatePort (duck typing — no explicit inheritance).

    Redis is the primary store; PostgreSQL (SyncRecord) is the secondary/fallback.

    Note: SyncRecord is imported at module level (intentional — see design.md).
    Future improvement: SyncRecordPort abstraction.
    """

    REDIS_KEY_PREFIX = "product_sync"

    def __init__(self, redis_client) -> None:
        self.redis = redis_client
        self.strategy = HashDeltaStrategy()

    def _key(self, sku: str) -> str:
        return f"{self.REDIS_KEY_PREFIX}:{sku}"

    def filter_changed(self, products: list[dict]) -> tuple[list[dict], int]:
        """Return only products whose content hash differs from stored state.

        Falls back to PostgreSQL when Redis has no data for a SKU.
        """
        changed: list[dict] = []
        skipped = 0

        for product in products:
            sku = product["sku"]
            new_hash = self.strategy.compute_hash(product)
            stored = self.redis.hget(self._key(sku), "content_hash")

            if stored is None:
                # Redis miss — try PostgreSQL fallback
                try:
                    record = SyncRecord.objects.get(sku=sku)
                    stored_str = record.content_hash
                    # Restore Redis from DB
                    self.redis.hset(
                        self._key(sku),
                        mapping={
                            "content_hash": record.content_hash,
                            "last_synced": record.last_synced.isoformat(),
                            "synced": "1" if record.synced else "0",
                        },
                    )
                except SyncRecord.DoesNotExist:
                    stored_str = None
                except Exception:
                    logger.warning(
                        "[SyncStateManager] PostgreSQL fallback failed for SKU %s",
                        sku,
                        exc_info=True,
                    )
                    stored_str = None
            else:
                stored_str = stored.decode() if isinstance(stored, bytes) else stored

            if stored_str is not None and stored_str == new_hash:
                skipped += 1
                continue

            changed.append(product)

        return changed, skipped

    def mark_synced(self, sku: str, content_hash: str) -> None:
        """Write sync state to Redis (primary) and PostgreSQL (secondary).

        Redis failure propagates; PostgreSQL failure is logged and swallowed.
        """
        now = datetime.now(timezone.utc)

        # Primary: Redis — exception propagates
        self.redis.hset(
            self._key(sku),
            mapping={
                "content_hash": content_hash,
                "last_synced": now.isoformat(),
                "synced": "1",
            },
        )

        # Secondary: PostgreSQL — error logged, not propagated
        try:
            SyncRecord.objects.update_or_create(
                sku=sku,
                defaults={
                    "content_hash": content_hash,
                    "last_synced": now,
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
        """Return True if SKU was previously synced.

        Checks Redis first; falls back to PostgreSQL and restores Redis on hit.
        """
        val = self.redis.hget(self._key(sku), "synced")
        if val is not None:
            return (val.decode() if isinstance(val, bytes) else val) == "1"

        # Fallback to PostgreSQL
        try:
            record = SyncRecord.objects.get(sku=sku)
            if record.synced:
                self.redis.hset(
                    self._key(sku),
                    mapping={
                        "content_hash": record.content_hash,
                        "last_synced": record.last_synced.isoformat(),
                        "synced": "1",
                    },
                )
                return True
        except SyncRecord.DoesNotExist:
            pass
        except Exception:
            logger.warning(
                "[SyncStateManager] PostgreSQL fallback failed for SKU %s",
                sku,
                exc_info=True,
            )

        return False
