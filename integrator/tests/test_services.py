"""Unit and property-based tests for integrator services."""

import json
from datetime import datetime, timezone

import fakeredis
from hypothesis import given, settings
from hypothesis import strategies as st

from integrator.services import (
    HashDeltaStrategy,
    SyncStateManager,
    TokenBucketRateLimiter,
    orchestrate_sync,
    transform_products,
)
from integrator.tasks import load_and_validate

# -- Strategies --

sku_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="-_"),
    min_size=1,
    max_size=20,
)

product_strategy = st.fixed_dictionaries(
    {
        "id": sku_strategy,
        "title": st.text(min_size=1, max_size=50),
        "price_vat_excl": st.one_of(
            st.none(), st.floats(min_value=-100, max_value=10000, allow_nan=False)
        ),
        "stocks": st.dictionaries(
            st.text(min_size=1, max_size=10),
            st.one_of(st.integers(0, 999), st.just("N/A")),
        ),
        "attributes": st.one_of(
            st.none(),
            st.fixed_dictionaries({}, optional={"color": st.text(max_size=20)}),
        ),
    }
)


# -- Property 1: Duplicate SKU merging (last wins) --
# Feature: erp-eshop-sync, Property 1: Sloučení duplicitních SKU (poslední vyhrává)


@given(products=st.lists(product_strategy, min_size=0, max_size=30))
@settings(max_examples=100)
def test_property_duplicate_sku_last_wins(products: list[dict]) -> None:
    """For any list with duplicate SKUs, load_and_validate must keep only unique SKUs
    and for each duplicate the last occurrence in the original list wins."""
    raw_json = json.dumps(products)
    result = load_and_validate(raw_json)

    # Build expected: last occurrence per SKU
    expected: dict[str, dict] = {}
    for p in products:
        expected[p["id"]] = p

    result_by_sku = {p["id"]: p for p in result}

    # All SKUs unique
    assert len(result) == len(result_by_sku), "Result contains duplicate SKUs"

    # Exactly the expected set of SKUs
    assert result_by_sku.keys() == expected.keys()

    # Each SKU has data from the last occurrence
    for sku, data in expected.items():
        assert result_by_sku[sku] == data, f"SKU {sku}: expected last occurrence data"


# -- Property 2: VAT calculation --
# Feature: erp-eshop-sync, Property 2: Výpočet DPH


@given(
    price=st.floats(
        min_value=0, max_value=1_000_000, allow_nan=False, allow_infinity=False
    )
)
@settings(max_examples=100)
def test_property_vat_calculation(price: float) -> None:
    """For any non-negative price_vat_excl, output price_vat_incl must equal
    round(price_vat_excl * 1.21, 2)."""
    raw = [
        {
            "id": "TEST-SKU",
            "title": "Test",
            "price_vat_excl": price,
            "stocks": {},
            "attributes": None,
        }
    ]

    result = transform_products(raw)

    assert len(result) == 1
    assert result[0]["price_vat_incl"] == round(price * 1.21, 2)


# -- Property 3: Invalid price filtering --
# Feature: erp-eshop-sync, Property 3: Filtrování nevalidních cen


@given(
    sku=sku_strategy,
    price=st.one_of(
        st.none(),
        st.floats(max_value=-0.01, allow_nan=False, allow_infinity=False),
    ),
)
@settings(max_examples=100)
def test_property_invalid_price_filtered(sku: str, price: float | None) -> None:
    """Products with null or negative price_vat_excl must never appear
    in the transformer output."""
    raw = [
        {
            "id": sku,
            "title": "Test",
            "price_vat_excl": price,
            "stocks": {},
            "attributes": None,
        }
    ]

    result = transform_products(raw)

    assert len(result) == 0, f"Product with price={price} should have been filtered out"


# -- Property 4: Stock aggregation --
# Feature: erp-eshop-sync, Property 4: Agregace skladových zásob


@given(
    stocks=st.dictionaries(
        st.text(min_size=1, max_size=10),
        st.one_of(st.integers(0, 999), st.just("N/A")),
        max_size=10,
    ),
)
@settings(max_examples=100)
def test_property_stock_aggregation(stocks: dict[str, int | str]) -> None:
    """For any stocks dict with int values and/or "N/A" strings,
    output stock_quantity must equal the sum of all int values
    (where "N/A" counts as 0)."""
    raw = [
        {
            "id": "TEST-SKU",
            "title": "Test",
            "price_vat_excl": 100.0,
            "stocks": stocks,
            "attributes": None,
        }
    ]

    result = transform_products(raw)

    expected = sum(v for v in stocks.values() if isinstance(v, int))
    assert len(result) == 1
    assert result[0]["stock_quantity"] == expected


# -- Property 5: Color resolution --
# Feature: erp-eshop-sync, Property 5: Rozlišení barvy


@given(
    attributes=st.one_of(
        st.none(),
        st.just({}),
        st.fixed_dictionaries(
            {},
            optional={
                "color": st.one_of(st.just(""), st.text(min_size=0, max_size=20))
            },
        ),
    ),
)
@settings(max_examples=100)
def test_property_color_resolution(attributes: dict | None) -> None:
    """If attributes contains a non-empty 'color' string, output color must
    match that value; otherwise output color must be 'N/A'."""
    raw = [
        {
            "id": "TEST-SKU",
            "title": "Test",
            "price_vat_excl": 100.0,
            "stocks": {},
            "attributes": attributes,
        }
    ]

    result = transform_products(raw)

    assert len(result) == 1

    if attributes and attributes.get("color"):
        assert result[0]["color"] == attributes["color"]
    else:
        assert result[0]["color"] == "N/A"


# -- Delta Sync tests --

# Strategy for transformed products (valid, with all required fields)
transformed_product_strategy = st.fixed_dictionaries(
    {
        "sku": sku_strategy,
        "title": st.text(min_size=1, max_size=50),
        "price_vat_excl": st.floats(
            min_value=0, max_value=10000, allow_nan=False, allow_infinity=False
        ),
        "price_vat_incl": st.floats(
            min_value=0, max_value=15000, allow_nan=False, allow_infinity=False
        ),
        "stock_quantity": st.integers(min_value=0, max_value=9999),
        "color": st.text(min_size=1, max_size=20),
    }
)


# -- Property 6: Round-trip stavu synchronizace v Redis --
# Feature: erp-eshop-sync, Property 6: Round-trip stavu synchronizace v Redis


@given(sku=sku_strategy, product=transformed_product_strategy)
@settings(max_examples=100)
def test_property_redis_roundtrip(sku: str, product: dict) -> None:
    """For any SKU and content hash, after storing sync state in Redis,
    subsequent retrieval must return the same content hash, a valid
    timestamp, and the synced flag."""
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    manager = SyncStateManager(redis_client)

    content_hash = manager.strategy.compute_hash(product)
    before = datetime.now(timezone.utc)

    manager.mark_synced(sku, content_hash)

    # Verify content hash round-trip
    stored_hash = redis_client.hget(f"product_sync:{sku}", "content_hash")
    assert stored_hash == content_hash

    # Verify timestamp is valid and recent
    stored_ts = redis_client.hget(f"product_sync:{sku}", "last_synced")
    assert stored_ts is not None
    ts = datetime.fromisoformat(str(stored_ts))
    assert ts >= before

    # Verify synced flag
    assert manager.was_previously_synced(sku) is True


# -- Property 7: Delta detekce --
# Feature: erp-eshop-sync, Property 7: Delta detekce


@given(products=st.lists(transformed_product_strategy, min_size=1, max_size=20))
@settings(max_examples=100)
def test_property_delta_detection(products: list[dict]) -> None:
    """A product is marked for sync iff its content hash differs from
    the stored hash (or has no stored record)."""
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    manager = SyncStateManager(redis_client)

    # Deduplicate by SKU (last wins) to match real pipeline behavior
    unique: dict[str, dict] = {}
    for p in products:
        unique[p["sku"]] = p
    products = list(unique.values())

    # First run: all products should be marked as changed (no prior state)
    changed, skipped = manager.filter_changed(products)
    assert len(changed) == len(products)
    assert skipped == 0

    # Mark all as synced
    for p in products:
        manager.mark_synced(p["sku"], manager.strategy.compute_hash(p))

    # Second run with same data: all should be skipped
    changed, skipped = manager.filter_changed(products)
    assert len(changed) == 0
    assert skipped == len(products)


# -- Property 3: Token bucket rate limiter vynucuje minimální interval --
# Feature: integrator-refactoring, Property 3: Token bucket rate limiter vynucuje minimální interval


@given(
    max_rps=st.integers(min_value=1, max_value=100),
    num_calls=st.integers(min_value=2, max_value=50),
)
@settings(max_examples=100)
def test_property_rate_limiter_enforces_minimum_interval(
    max_rps: int, num_calls: int
) -> None:
    """For N calls to acquire() + record_request() with fake clock,
    sum of wait times >= (N-1) / max_rps.

    **Validates: Requirements 2.1, 2.2, 2.5**
    """
    current_time = 0.0

    def fake_clock() -> float:
        return current_time

    limiter = TokenBucketRateLimiter(max_rps=max_rps, clock_fn=fake_clock)

    total_wait = 0.0
    for _ in range(num_calls):
        wait = limiter.acquire()
        total_wait += wait
        # Advance clock by the wait time (simulating sleep)
        current_time += wait
        limiter.record_request()

    expected_min_wait = (num_calls - 1) / max_rps
    assert total_wait >= expected_min_wait - 1e-9, (
        f"Total wait {total_wait} < expected minimum {expected_min_wait} "
        f"for {num_calls} calls at {max_rps} rps"
    )


# -- Property 4: Token bucket acquire vrací nulový wait time po dostatečném čekání --
# Feature: integrator-refactoring, Property 4: Token bucket acquire vrací nulový wait time po dostatečném čekání


@given(
    max_rps=st.integers(min_value=1, max_value=100),
    extra_time=st.floats(
        min_value=0.0, max_value=10.0, allow_nan=False, allow_infinity=False
    ),
)
@settings(max_examples=100)
def test_property_rate_limiter_zero_wait_after_sufficient_time(
    max_rps: int, extra_time: float
) -> None:
    """For any max_rps and time shift >= 1/max_rps, acquire() returns 0.0.

    **Validates: Requirements 2.1**
    """
    current_time = 0.0

    def fake_clock() -> float:
        return current_time

    limiter = TokenBucketRateLimiter(max_rps=max_rps, clock_fn=fake_clock)

    # Make one request
    limiter.record_request()

    # Advance clock by at least 1/max_rps + extra
    min_interval = 1.0 / max_rps
    current_time = min_interval + extra_time

    wait = limiter.acquire()
    assert wait == 0.0, (
        f"Expected 0.0 wait after {current_time}s >= {min_interval}s interval, got {wait}"
    )


# -- Property 9: Determinismus hash výpočtu --
# Feature: integrator-refactoring, Property 9: Determinismus hash výpočtu


@given(product=transformed_product_strategy)
@settings(max_examples=100)
def test_property_hash_computation_determinism(product: dict) -> None:
    """Two calls to HashDeltaStrategy.compute_hash(product) return identical hash.

    **Validates: Requirements 4.4**
    """
    strategy = HashDeltaStrategy()

    hash1 = strategy.compute_hash(product)
    hash2 = strategy.compute_hash(product)

    assert hash1 == hash2, (
        f"Hash not deterministic: {hash1} != {hash2} for product {product}"
    )
    assert len(hash1) == 64, f"Expected SHA-256 hex (64 chars), got {len(hash1)}"


# -- Property 7: Dual-write sync stavu do Redis a PostgreSQL --
# Feature: integrator-refactoring, Property 7: Dual-write sync stavu do Redis a PostgreSQL


@given(sku=sku_strategy, product=transformed_product_strategy)
@settings(max_examples=100)
def test_property_dual_write_sync_state(sku: str, product: dict) -> None:
    """After mark_synced(sku, hash), same hash must be in both Redis and PostgreSQL.

    **Validates: Requirements 7.2**
    """
    from unittest.mock import MagicMock, patch

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    manager = SyncStateManager(redis_client)

    content_hash = manager.strategy.compute_hash(product)

    mock_record = MagicMock()
    with patch("integrator.models.SyncRecord") as MockSyncRecord:
        MockSyncRecord.objects.update_or_create.return_value = (mock_record, True)
        manager.mark_synced(sku, content_hash)

        # Verify Redis has the correct data
        stored_hash = redis_client.hget(f"product_sync:{sku}", "content_hash")
        assert stored_hash == content_hash

        stored_synced = redis_client.hget(f"product_sync:{sku}", "synced")
        assert stored_synced == "1"

        # Verify PostgreSQL update_or_create was called with correct args
        MockSyncRecord.objects.update_or_create.assert_called_once()
        call_kwargs = MockSyncRecord.objects.update_or_create.call_args
        assert call_kwargs[1]["sku"] == sku
        assert call_kwargs[1]["defaults"]["content_hash"] == content_hash
        assert call_kwargs[1]["defaults"]["synced"] is True


# -- Property 8: PostgreSQL fallback s obnovou Redis --
# Feature: integrator-refactoring, Property 8: PostgreSQL fallback s obnovou Redis


@given(sku=sku_strategy, product=transformed_product_strategy)
@settings(max_examples=100)
def test_property_postgresql_fallback_with_redis_restoration(
    sku: str, product: dict
) -> None:
    """SKU existing in PostgreSQL but not in Redis -> was_previously_synced returns True
    and restores Redis.

    **Validates: Requirements 7.3, 7.4, 7.5**
    """
    from unittest.mock import MagicMock, patch

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    manager = SyncStateManager(redis_client)

    content_hash = manager.strategy.compute_hash(product)
    last_synced = datetime.now(timezone.utc)

    # Ensure Redis has no data for this SKU
    assert redis_client.hget(f"product_sync:{sku}", "synced") is None

    # Mock SyncRecord.objects.get to return a record from PostgreSQL
    mock_record = MagicMock()
    mock_record.synced = True
    mock_record.content_hash = content_hash
    mock_record.last_synced = last_synced

    with patch("integrator.models.SyncRecord") as MockSyncRecord:
        MockSyncRecord.objects.get.return_value = mock_record

        result = manager.was_previously_synced(sku)

        # Must return True (found in PostgreSQL)
        assert result is True

        # Must have called SyncRecord.objects.get with correct SKU
        MockSyncRecord.objects.get.assert_called_once_with(sku=sku)

    # Verify Redis was restored from PostgreSQL
    restored_hash = redis_client.hget(f"product_sync:{sku}", "content_hash")
    assert restored_hash == content_hash

    restored_synced = redis_client.hget(f"product_sync:{sku}", "synced")
    assert restored_synced == "1"

    restored_ts = redis_client.hget(f"product_sync:{sku}", "last_synced")
    assert restored_ts == last_synced.isoformat()


# -- Property 1: Invariant souhrnného slovníku orchestrátoru --
# Feature: integrator-refactoring, Property 1: Invariant souhrnného slovníku orchestrátoru


@given(products=st.lists(transformed_product_strategy, min_size=0, max_size=20))
@settings(max_examples=100)
def test_property_orchestrator_summary_invariant(products: list[dict]) -> None:
    """processed == unchanged + synced + errors, all values non-negative.

    **Validates: Requirements 1.2, 1.3**
    """
    from unittest.mock import MagicMock

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    manager = SyncStateManager(redis_client)

    mock_api = MagicMock()
    # Randomly succeed or fail for each call
    import random

    def side_effect(product, is_update=False):
        if random.random() < 0.5:  # noqa: S311
            return {"ok": True}, None
        return None, f"Error syncing {product['sku']}"

    mock_api.send_product.side_effect = side_effect

    # Deduplicate by SKU to match real pipeline behavior
    unique: dict[str, dict] = {}
    for p in products:
        unique[p["sku"]] = p
    deduped = list(unique.values())

    summary = orchestrate_sync(deduped, manager, mock_api)

    assert summary["processed"] >= 0
    assert summary["unchanged"] >= 0
    assert summary["synced"] >= 0
    assert summary["errors"] >= 0
    assert (
        summary["processed"]
        == summary["unchanged"] + summary["synced"] + summary["errors"]
    )


# -- Property 2: Orchestrátor odesílá pouze změněné produkty --
# Feature: integrator-refactoring, Property 2: Orchestrátor odesílá pouze změněné produkty


@given(products=st.lists(transformed_product_strategy, min_size=0, max_size=20))
@settings(max_examples=100)
def test_property_orchestrator_sends_only_changed(products: list[dict]) -> None:
    """Number of sent products == synced + errors.

    **Validates: Requirements 1.2**
    """
    from unittest.mock import MagicMock

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    manager = SyncStateManager(redis_client)

    mock_api = MagicMock()
    import random

    def side_effect(product, is_update=False):
        if random.random() < 0.5:  # noqa: S311
            return {"ok": True}, None
        return None, f"Error syncing {product['sku']}"

    mock_api.send_product.side_effect = side_effect

    # Deduplicate by SKU
    unique: dict[str, dict] = {}
    for p in products:
        unique[p["sku"]] = p
    deduped = list(unique.values())

    summary = orchestrate_sync(deduped, manager, mock_api)

    # Number of API calls must equal synced + errors
    assert mock_api.send_product.call_count == summary["synced"] + summary["errors"]


# -- Property 5: Strukturované logování selhání synchronizace --
# Feature: integrator-refactoring, Property 5: Strukturované logování selhání synchronizace


@given(products=st.lists(transformed_product_strategy, min_size=1, max_size=20))
@settings(max_examples=100)
def test_property_structured_failure_logging(products: list[dict]) -> None:
    """Failed product -> failed_products contains {"sku": ..., "error": ...},
    errors == len(failed_products).

    **Validates: Requirements 5.1, 5.2, 5.3, 5.4**
    """
    from unittest.mock import MagicMock

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    manager = SyncStateManager(redis_client)

    # Make ALL products fail so we can verify failure tracking
    mock_api = MagicMock()
    mock_api.send_product.return_value = (None, "API error")

    # Deduplicate by SKU
    unique: dict[str, dict] = {}
    for p in products:
        unique[p["sku"]] = p
    deduped = list(unique.values())

    summary = orchestrate_sync(deduped, manager, mock_api)

    # errors must equal len(failed_products)
    assert summary["errors"] == len(summary["failed_products"])

    # Each failed product entry must have sku and error keys
    for entry in summary["failed_products"]:
        assert "sku" in entry
        assert "error" in entry
        assert isinstance(entry["sku"], str)
        assert isinstance(entry["error"], str)
        assert len(entry["error"]) > 0

    # All changed products should be in failed_products (since all fail)
    failed_skus = {e["sku"] for e in summary["failed_products"]}
    # changed = processed - unchanged
    assert len(failed_skus) == summary["errors"]
