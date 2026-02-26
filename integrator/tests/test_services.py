"""Unit and property-based tests for integrator services."""

import json
import tempfile
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from integrator.services import (
    HashDeltaStrategy,
    JSONFileLoader,
    SyncStateManager,
    transform_products,
)

import fakeredis
from datetime import datetime, timezone


# -- Strategies --

sku_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="-_"),
    min_size=1,
    max_size=20,
)

product_strategy = st.fixed_dictionaries({
    "id": sku_strategy,
    "title": st.text(min_size=1, max_size=50),
    "price_vat_excl": st.one_of(st.none(), st.floats(min_value=-100, max_value=10000, allow_nan=False)),
    "stocks": st.dictionaries(st.text(min_size=1, max_size=10), st.one_of(st.integers(0, 999), st.just("N/A"))),
    "attributes": st.one_of(st.none(), st.fixed_dictionaries({}, optional={"color": st.text(max_size=20)})),
})


# -- Property 1: Duplicate SKU merging (last wins) --
# Feature: erp-eshop-sync, Property 1: Sloučení duplicitních SKU (poslední vyhrává)

@given(products=st.lists(product_strategy, min_size=0, max_size=30))
@settings(max_examples=100)
def test_property_duplicate_sku_last_wins(products: list[dict]) -> None:
    """For any list with duplicate SKUs, loader must keep only unique SKUs
    and for each duplicate the last occurrence in the original list wins."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(products, f)
        tmp_path = Path(f.name)

    loader = JSONFileLoader(path=tmp_path)
    result = loader.load()

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

    tmp_path.unlink(missing_ok=True)


# -- Property 2: VAT calculation --
# Feature: erp-eshop-sync, Property 2: Výpočet DPH

@given(price=st.floats(min_value=0, max_value=1_000_000, allow_nan=False, allow_infinity=False))
@settings(max_examples=100)
def test_property_vat_calculation(price: float) -> None:
    """For any non-negative price_vat_excl, output price_vat_incl must equal
    round(price_vat_excl * 1.21, 2)."""
    raw = [{
        "id": "TEST-SKU",
        "title": "Test",
        "price_vat_excl": price,
        "stocks": {},
        "attributes": None,
    }]

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
    raw = [{
        "id": sku,
        "title": "Test",
        "price_vat_excl": price,
        "stocks": {},
        "attributes": None,
    }]

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
    raw = [{
        "id": "TEST-SKU",
        "title": "Test",
        "price_vat_excl": 100.0,
        "stocks": stocks,
        "attributes": None,
    }]

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
        st.fixed_dictionaries({}, optional={"color": st.one_of(st.just(""), st.text(min_size=0, max_size=20))}),
    ),
)
@settings(max_examples=100)
def test_property_color_resolution(attributes: dict | None) -> None:
    """If attributes contains a non-empty 'color' string, output color must
    match that value; otherwise output color must be 'N/A'."""
    raw = [{
        "id": "TEST-SKU",
        "title": "Test",
        "price_vat_excl": 100.0,
        "stocks": {},
        "attributes": attributes,
    }]

    result = transform_products(raw)

    assert len(result) == 1

    if attributes and attributes.get("color"):
        assert result[0]["color"] == attributes["color"]
    else:
        assert result[0]["color"] == "N/A"


# -- Delta Sync tests --

# Strategy for transformed products (valid, with all required fields)
transformed_product_strategy = st.fixed_dictionaries({
    "sku": sku_strategy,
    "title": st.text(min_size=1, max_size=50),
    "price_vat_excl": st.floats(min_value=0, max_value=10000, allow_nan=False, allow_infinity=False),
    "price_vat_incl": st.floats(min_value=0, max_value=15000, allow_nan=False, allow_infinity=False),
    "stock_quantity": st.integers(min_value=0, max_value=9999),
    "color": st.text(min_size=1, max_size=20),
})


# -- Property 6: Round-trip stavu synchronizace v Redis --
# Feature: erp-eshop-sync, Property 6: Round-trip stavu synchronizace v Redis

@given(sku=sku_strategy, product=transformed_product_strategy)
@settings(max_examples=100)
def test_property_redis_roundtrip(sku: str, product: dict) -> None:
    """For any SKU and content hash, after storing sync state in Redis,
    subsequent retrieval must return the same content hash, a valid
    timestamp, and the synced flag."""
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    strategy = HashDeltaStrategy()
    manager = SyncStateManager(redis_client, strategy)

    content_hash = strategy.compute_hash(product)
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
    strategy = HashDeltaStrategy()
    manager = SyncStateManager(redis_client, strategy)

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
        manager.mark_synced(p["sku"], strategy.compute_hash(p))

    # Second run with same data: all should be skipped
    changed, skipped = manager.filter_changed(products)
    assert len(changed) == 0
    assert skipped == len(products)


# -- Property 8: Aktualizace stavu po synchronizaci --
# Feature: erp-eshop-sync, Property 8: Aktualizace stavu po synchronizaci

@given(sku=sku_strategy, product=transformed_product_strategy)
@settings(max_examples=100)
def test_property_state_update_after_sync(sku: str, product: dict) -> None:
    """After calling mark_synced with a new hash, the stored hash in Redis
    must match the new hash and the timestamp must be current."""
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    strategy = HashDeltaStrategy()
    manager = SyncStateManager(redis_client, strategy)

    new_hash = strategy.compute_hash(product)
    before = datetime.now(timezone.utc)

    manager.mark_synced(sku, new_hash)

    stored_hash = redis_client.hget(f"product_sync:{sku}", "content_hash")
    assert stored_hash == new_hash

    stored_ts = redis_client.hget(f"product_sync:{sku}", "last_synced")
    assert stored_ts is not None
    ts = datetime.fromisoformat(str(stored_ts))
    assert ts >= before
    assert ts <= datetime.now(timezone.utc)
