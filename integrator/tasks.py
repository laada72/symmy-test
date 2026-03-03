"""Celery tasks for the ERP ↔ E-shop sync pipeline."""

import json
import logging
import traceback
from collections.abc import Callable

import redis
from celery import chain, shared_task
from django.conf import settings

from integrator.adapters import EshopAPIClient, SyncStateManager
from integrator.domain import orchestrate_sync, transform_products
from integrator.ports import EshopPort, SyncStatePort

DependencyFactory = Callable[[], tuple[SyncStatePort, EshopPort]]

logger = logging.getLogger(__name__)


@shared_task
def load_and_validate(raw_json: str) -> list[dict]:
    """Load and validate raw ERP JSON data.

    Skips records missing the ``id`` field (with WARNING log), deduplicates
    by ``id`` (last occurrence wins).

    Args:
        raw_json: Raw JSON string containing an array of product objects.

    Returns:
        List of valid product dicts with unique ``id`` values.

    Raises:
        json.JSONDecodeError: If input is not valid JSON.
        TypeError: If input is wrong type.
    """
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
    """Apply business rules to raw ERP products.

    Delegates to ``transform_products`` — computes VAT price,
    aggregates stock, extracts color from attributes.

    Args:
        raw_products: List of raw product dicts from ERP.

    Returns:
        List of transformed products ready for sync.
    """
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
    """Delta-sync transformed products to the e-shop.

    Creates dependencies (Redis, SyncStateManager, EshopAPIClient) and
    delegates to ``orchestrate_sync``. Dependencies can be injected via
    ``dependency_factory`` for testing.

    Args:
        transformed_products: Transformed products from the previous pipeline step.
        dependency_factory: Optional factory returning ``(SyncStatePort, EshopPort)``.
            If omitted, creates default instances from Django settings.
            NOTE: Celery serialises task args as JSON, so this parameter cannot
            be passed via ``.delay()`` or chain — it is for direct test calls only.

    Returns:
        Sync summary dict (see ``orchestrate_sync``).
    """
    if dependency_factory is None:
        redis_client = redis.Redis.from_url(settings.CELERY_BROKER_URL)
        manager: SyncStatePort = SyncStateManager(redis_client)
        api_client: EshopPort = EshopAPIClient()
    else:
        manager, api_client = dependency_factory()

    summary = orchestrate_sync(transformed_products, manager, api_client)
    logger.info("[delta_sync] Sync complete: %s", summary)
    return summary


def run_sync_pipeline(raw_json: str):
    """Run the complete sync pipeline as a Celery chain.

    Pipeline: load_and_validate → transform → delta_sync

    Args:
        raw_json: Raw JSON string with ERP product data.

    Returns:
        ``celery.result.AsyncResult`` for the running task.
    """
    return chain(
        load_and_validate.s(raw_json),
        transform.s(),
        delta_sync.s(),
    ).apply_async()
