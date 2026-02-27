"""Celery tasks for the ERP ↔ E-shop sync pipeline."""

import json
import logging
import traceback
from collections.abc import Callable

import redis
from celery import chain, shared_task
from django.conf import settings

from integrator.clients import EshopAPIClient
from integrator.services import (
    SyncStateManager,
    orchestrate_sync,
    transform_products,
)

DependencyFactory = Callable[[], tuple[SyncStateManager, EshopAPIClient]]

logger = logging.getLogger(__name__)


@shared_task
def load_and_validate(raw_json: str) -> list[dict]:
    """Parse raw JSON string, skip invalid records, merge duplicates, return raw products."""
    try:
        data: list[dict] = json.loads(raw_json)

        valid_products: list[dict] = []
        skipped = 0
        for idx, product in enumerate(data):
            if "id" not in product:
                logger.warning(
                    "[load_and_validate] Skipping record at index %d: missing 'id'. Data: %s",
                    idx,
                    product,
                )
                skipped += 1
                continue
            valid_products.append(product)

        if skipped > 0:
            logger.warning("[load_and_validate] Skipped %d invalid records", skipped)

        # Merge duplicates — last occurrence wins
        seen: dict[str, dict] = {}
        for product in valid_products:
            seen[product["id"]] = product

        products = list(seen.values())
        logger.info(
            "[load_and_validate] Loaded %d valid products (%d skipped)",
            len(products),
            skipped,
        )
        return products
    except (json.JSONDecodeError, TypeError) as exc:
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
def delta_sync(
    transformed_products: list[dict],
    dependency_factory: DependencyFactory | None = None,
) -> dict:
    """Thin wrapper: create dependencies and delegate to orchestrate_sync."""
    if dependency_factory is None:
        redis_client = redis.Redis.from_url(settings.CELERY_BROKER_URL)
        manager = SyncStateManager(redis_client)
        api_client = EshopAPIClient()
    else:
        manager, api_client = dependency_factory()

    summary = orchestrate_sync(transformed_products, manager, api_client)
    logger.info("[delta_sync] Sync complete: %s", summary)
    return summary


def run_sync_pipeline(raw_json: str):
    """Orchestrate full pipeline as Celery chain."""
    return chain(
        load_and_validate.s(raw_json),
        transform.s(),
        delta_sync.s(),
    ).apply_async()
