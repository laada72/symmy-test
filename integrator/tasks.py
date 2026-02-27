"""Celery tasks for the ERP ↔ E-shop sync pipeline."""

import json
import logging
import traceback

import redis
from celery import chain, shared_task
from django.conf import settings

from integrator.clients import EshopAPIClient
from integrator.services import (
    HashDeltaStrategy,
    SyncStateManager,
    transform_products,
)

logger = logging.getLogger(__name__)


@shared_task
def load_and_validate(raw_json: str) -> list[dict]:
    """Parse raw JSON string, merge duplicates, return raw products."""
    try:
        data: list[dict] = json.loads(raw_json)

        # Merge duplicates — last occurrence wins
        seen: dict[str, dict] = {}
        for product in data:
            seen[product["id"]] = product

        products = list(seen.values())
        logger.info("[load_and_validate] Loaded %d raw products", len(products))
        return products
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        logger.error(
            "[load_and_validate] Failed to load ERP data: %s\n%s",
            exc,
            traceback.format_exc(),
        )
        raise


@shared_task
def transform(raw_products: list[dict]) -> list[dict]:
    """Apply business rules, return transformed products."""
    result = transform_products(raw_products)
    logger.info(
        "[transform] %d products in, %d products out", len(raw_products), len(result)
    )
    return result


@shared_task
def delta_sync(transformed_products: list[dict]) -> dict:
    """Detect changes via Redis, sync to e-shop API. Returns summary dict."""
    redis_client = redis.Redis.from_url(settings.CELERY_BROKER_URL)
    strategy = HashDeltaStrategy()
    manager = SyncStateManager(redis_client, strategy)
    api_client = EshopAPIClient()

    changed, skipped_unchanged = manager.filter_changed(transformed_products)

    synced = 0
    errors = 0
    for product in changed:
        sku = product["sku"]
        is_update = manager.was_previously_synced(sku)
        data, error = api_client.send_product(product, is_update=is_update)

        if error:
            errors += 1
            continue

        content_hash = strategy.compute_hash(product)
        manager.mark_synced(sku, content_hash)
        synced += 1

    summary = {
        "processed": len(transformed_products),
        "skipped_invalid": 0,
        "unchanged": skipped_unchanged,
        "synced": synced,
        "errors": errors,
    }
    logger.info(
        "[delta_sync] Sync complete: processed=%d, unchanged=%d, synced=%d, errors=%d",
        summary["processed"],
        summary["unchanged"],
        summary["synced"],
        summary["errors"],
    )
    return summary


def run_sync_pipeline(raw_json: str):
    """Orchestrate full pipeline as Celery chain."""
    return chain(
        load_and_validate.s(raw_json),
        transform.s(),
        delta_sync.s(),
    ).apply_async()
