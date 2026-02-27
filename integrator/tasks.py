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
    """Načte a zvaliduje surová JSON data z ERP systému.

    Parsuje JSON řetězec, přeskočí záznamy bez klíče ``id`` (s logováním),
    a sloučí duplicity — při duplicitním ``id`` vyhrává poslední výskyt.

    Args:
        raw_json: Surový JSON řetězec obsahující pole produktových objektů.

    Returns:
        Seznam validních produktových slovníků s unikátními ``id``.

    Raises:
        json.JSONDecodeError: Pokud vstup není validní JSON.
        TypeError: Pokud vstup není správného typu.
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
    """Aplikuje business pravidla na surové produkty z ERP.

    Deleguje na ``transform_products`` — vypočítá cenu s DPH,
    agreguje skladové zásoby a extrahuje barvu z atributů.

    Args:
        raw_products: Seznam surových produktových slovníků z ERP.

    Returns:
        Seznam transformovaných produktů připravených k synchronizaci.
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
    """Provede delta synchronizaci transformovaných produktů do e-shopu.

    Vytvoří potřebné závislosti (Redis klient, SyncStateManager, EshopAPIClient)
    a deleguje na ``orchestrate_sync``. Závislosti lze injektovat přes
    ``dependency_factory`` pro testování.

    Args:
        transformed_products: Seznam transformovaných produktů z předchozího kroku.
        dependency_factory: Volitelná tovární funkce vracející tuple
            ``(SyncStateManager, EshopAPIClient)``. Pokud není zadána,
            vytvoří se výchozí instance z nastavení aplikace.

    Returns:
        Slovník se souhrnem synchronizace (viz ``orchestrate_sync``).
    """
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
    """Spustí kompletní synchronizační pipeline jako Celery chain.

    Pipeline se skládá ze tří kroků:
    1. ``load_and_validate`` — parsování a validace JSON dat
    2. ``transform`` — aplikace business pravidel
    3. ``delta_sync`` — delta synchronizace do e-shopu

    Args:
        raw_json: Surový JSON řetězec s produktovými daty z ERP.

    Returns:
        ``celery.result.AsyncResult`` s ID spuštěného tasku.
    """
    return chain(
        load_and_validate.s(raw_json),
        transform.s(),
        delta_sync.s(),
    ).apply_async()
