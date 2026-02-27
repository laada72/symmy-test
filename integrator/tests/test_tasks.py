"""Unit tests for Celery tasks and pipeline orchestration."""

import json
import logging
from unittest.mock import MagicMock, patch

import fakeredis
import responses

from integrator.tasks import delta_sync, load_and_validate, run_sync_pipeline, transform

BASE_URL = "https://api.fake-eshop.cz/v1/products/"


# -- Test: Task registration and autodiscover --


def test_tasks_registered_in_celery():
    """Celery autodiscover must find all integrator tasks."""
    from core.celery import app

    registered = app.tasks
    for name in [
        "integrator.tasks.load_and_validate",
        "integrator.tasks.transform",
        "integrator.tasks.delta_sync",
    ]:
        assert name in registered, f"Task {name} not registered"


# -- Test: Pipeline order (chain) --


def test_run_sync_pipeline_creates_chain():
    """run_sync_pipeline must create a chain in correct order:
    load_and_validate → transform → delta_sync."""
    with patch("integrator.tasks.chain") as mock_chain:
        mock_chain.return_value.apply_async.return_value = MagicMock()
        run_sync_pipeline('[{"id": "SKU-001"}]')

        mock_chain.assert_called_once()
        args = mock_chain.call_args[0]
        task_names = [sig.task for sig in args]
        assert task_names == [
            "integrator.tasks.load_and_validate",
            "integrator.tasks.transform",
            "integrator.tasks.delta_sync",
        ]


# -- Test: Summary logging after completion --


@responses.activate
def test_delta_sync_logs_summary(caplog):
    """After sync completes, delta_sync must log a summary with counts."""
    fake_redis = fakeredis.FakeRedis()

    responses.add(responses.POST, BASE_URL, json={"ok": True}, status=201)

    products = [
        {
            "sku": "SKU-001",
            "title": "Test Product",
            "price_vat_excl": 100.0,
            "price_vat_incl": 121.0,
            "stock_quantity": 10,
            "color": "red",
        },
    ]

    with (
        patch("integrator.tasks.redis.Redis.from_url", return_value=fake_redis),
        patch("integrator.tasks.EshopAPIClient") as mock_client_cls,
    ):
        mock_client = MagicMock()
        mock_client.send_product.return_value = ({"ok": True}, None)
        mock_client_cls.return_value = mock_client

        with caplog.at_level(logging.INFO, logger="integrator.tasks"):
            result = delta_sync(products)

    assert result["processed"] == 1
    assert result["synced"] == 1
    assert result["unchanged"] == 0
    assert "Sync complete" in caplog.text
    assert "synced=1" in caplog.text


# -- Test: Traceback logging on unhandled exception --


def test_load_and_validate_logs_traceback_on_missing_file(caplog):
    """When raw JSON references missing data, load_and_validate must log
    the full traceback and re-raise the exception."""
    import pytest

    with caplog.at_level(logging.ERROR, logger="integrator.tasks"):
        with pytest.raises((json.JSONDecodeError, KeyError, TypeError)):
            load_and_validate("not valid json at all")

    assert "Failed to load ERP data" in caplog.text


def test_load_and_validate_logs_traceback_on_invalid_json(caplog):
    """When JSON is invalid, load_and_validate must log traceback and fail."""
    import pytest

    with caplog.at_level(logging.ERROR, logger="integrator.tasks"):
        with pytest.raises(Exception):
            load_and_validate("{invalid json")

    assert "Failed to load ERP data" in caplog.text


# -- Test: load_and_validate returns correct data --


def test_load_and_validate_returns_products():
    """load_and_validate must return parsed products from raw JSON string."""
    data = [
        {
            "id": "SKU-001",
            "title": "A",
            "price_vat_excl": 10.0,
            "stocks": {},
            "attributes": None,
        }
    ]
    raw_json = json.dumps(data)

    result = load_and_validate(raw_json)
    assert len(result) == 1
    assert result[0]["id"] == "SKU-001"


# -- Test: transform task delegates correctly --


def test_transform_applies_business_rules():
    """transform task must apply VAT calculation and return transformed products."""
    raw = [
        {
            "id": "SKU-T",
            "title": "T",
            "price_vat_excl": 100.0,
            "stocks": {"wh1": 5},
            "attributes": {"color": "blue"},
        }
    ]
    result = transform(raw)
    assert len(result) == 1
    assert result[0]["price_vat_incl"] == 121.0
    assert result[0]["stock_quantity"] == 5
    assert result[0]["color"] == "blue"
