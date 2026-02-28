"""Unit tests for Celery tasks and pipeline orchestration."""

import json
import logging
from unittest.mock import MagicMock, patch

import fakeredis
import pytest
import responses
from hypothesis import given
from hypothesis import settings as hypothesis_settings
from hypothesis import strategies as st

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
    assert "'synced': 1" in caplog.text


# -- Test: Traceback logging on unhandled exception --


@pytest.mark.parametrize(
    "invalid_input",
    [
        pytest.param("not valid json at all", id="plain-text"),
        pytest.param("{invalid json", id="malformed-json"),
    ],
)
def test_load_and_validate_logs_traceback_on_invalid_input(caplog, invalid_input):
    """When JSON is invalid, load_and_validate must log traceback and fail."""
    with caplog.at_level(logging.ERROR, logger="integrator.tasks"):
        with pytest.raises(Exception):
            load_and_validate(invalid_input)

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


# -- Tests for refactored delta_sync (Task 5.6) --


def test_delta_sync_default_factory(caplog):
    """When no factory provided, delta_sync creates dependencies from Django settings."""
    fake_redis = fakeredis.FakeRedis()
    products = [
        {
            "sku": "SKU-DF",
            "title": "Default Factory",
            "price_vat_excl": 50.0,
            "price_vat_incl": 60.5,
            "stock_quantity": 5,
            "color": "green",
        },
    ]

    with (
        patch(
            "integrator.tasks.redis.Redis.from_url", return_value=fake_redis
        ) as mock_redis,
        patch("integrator.tasks.EshopAPIClient") as mock_client_cls,
        patch("integrator.tasks.orchestrate_sync") as mock_orch,
    ):
        mock_orch.return_value = {
            "processed": 1,
            "unchanged": 0,
            "synced": 1,
            "errors": 0,
            "failed_products": [],
        }
        delta_sync(products)

        # Default factory should create Redis client from settings
        mock_redis.assert_called_once()
        # Default factory should create EshopAPIClient
        mock_client_cls.assert_called_once()
        # orchestrate_sync should be called with the created dependencies
        mock_orch.assert_called_once()


def test_delta_sync_provided_factory():
    """When factory is provided, delta_sync uses it instead of default."""
    mock_manager = MagicMock()
    mock_api = MagicMock()
    factory = MagicMock(return_value=(mock_manager, mock_api))

    products = [
        {
            "sku": "SKU-PF",
            "title": "Provided Factory",
            "price_vat_excl": 50.0,
            "price_vat_incl": 60.5,
            "stock_quantity": 5,
            "color": "blue",
        },
    ]

    with (
        patch("integrator.tasks.redis.Redis.from_url") as mock_redis,
        patch("integrator.tasks.EshopAPIClient") as mock_client_cls,
        patch("integrator.tasks.orchestrate_sync") as mock_orch,
    ):
        mock_orch.return_value = {
            "processed": 1,
            "unchanged": 0,
            "synced": 1,
            "errors": 0,
            "failed_products": [],
        }
        delta_sync(products, dependency_factory=factory)

        # Factory should be called
        factory.assert_called_once()
        # Default creation should NOT be called
        mock_redis.assert_not_called()
        mock_client_cls.assert_not_called()
        # orchestrate_sync should receive factory-created deps
        mock_orch.assert_called_once_with(products, mock_manager, mock_api)


# -- Property 6: Filtrování nevalidních záznamů v load_and_validate --
# Feature: integrator-refactoring, Property 6: Filtrování nevalidních záznamů v load_and_validate

# Strategy for records with 'id' field
valid_record_strategy = st.fixed_dictionaries(
    {
        "id": st.text(
            alphabet=st.characters(
                whitelist_categories=("L", "N"), whitelist_characters="-_"
            ),
            min_size=1,
            max_size=20,
        ),
        "title": st.text(min_size=1, max_size=50),
        "price_vat_excl": st.floats(
            min_value=0, max_value=10000, allow_nan=False, allow_infinity=False
        ),
    }
)

# Strategy for records WITHOUT 'id' field
invalid_record_strategy = st.fixed_dictionaries(
    {
        "title": st.text(min_size=1, max_size=50),
        "price_vat_excl": st.floats(
            min_value=0, max_value=10000, allow_nan=False, allow_infinity=False
        ),
    }
)

mixed_record_strategy = st.one_of(valid_record_strategy, invalid_record_strategy)


@given(records=st.lists(mixed_record_strategy, min_size=0, max_size=20))
@hypothesis_settings(max_examples=100)
def test_property_filter_invalid_records(records: list[dict]) -> None:
    """Returned records subset of input, all have 'id', count <= input.

    **Validates: Requirements 6.1, 6.3**
    """
    raw_json = json.dumps(records)
    result = load_and_validate(raw_json)

    # All returned records must have 'id'
    for r in result:
        assert "id" in r

    # Count must be <= input
    assert len(result) <= len(records)

    # Returned records must be a subset of input valid records
    input_valid = [r for r in records if "id" in r]
    result_ids = {r["id"] for r in result}
    input_valid_ids = {r["id"] for r in input_valid}
    assert result_ids <= input_valid_ids


# -- Tests for load_and_validate edge cases (Task 5.9) --


def test_load_and_validate_logs_skipped_count(caplog):
    """Summary log must contain count of skipped invalid records."""
    data = [
        {"id": "SKU-OK", "title": "Valid"},
        {"title": "No ID 1"},
        {"title": "No ID 2"},
    ]
    raw_json = json.dumps(data)

    with caplog.at_level(logging.WARNING, logger="integrator.tasks"):
        result = load_and_validate(raw_json)

    assert len(result) == 1
    assert result[0]["id"] == "SKU-OK"
    assert "Skipped 2 invalid records" in caplog.text


def test_load_and_validate_all_invalid_returns_empty(caplog):
    """When all records are invalid, return empty list and log warning."""
    data = [
        {"title": "No ID 1"},
        {"title": "No ID 2"},
        {"price": 100},
    ]
    raw_json = json.dumps(data)

    with caplog.at_level(logging.WARNING, logger="integrator.tasks"):
        result = load_and_validate(raw_json)

    assert result == []
    assert "Skipped 3 invalid records" in caplog.text
