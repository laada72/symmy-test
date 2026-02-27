"""Broad integration tests for the ERP → E-shop sync pipeline.

Verifies the complete pipeline (load_and_validate → transform → delta_sync)
with mocked external dependencies: fakeredis for Redis, responses library
for HTTP mocking, and synchronous Celery task calls.
"""

import json
import re
from unittest.mock import patch

import fakeredis
import responses
from hypothesis import given, settings
from hypothesis import strategies as st

from integrator.clients import EshopAPIClient
from integrator.tasks import delta_sync, load_and_validate, transform

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://api.fake-eshop.cz/v1/products/"
API_KEY = "test-api-key"

# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

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
            st.none(),
            st.floats(min_value=-100, max_value=10000, allow_nan=False),
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

valid_product_strategy = st.fixed_dictionaries(
    {
        "id": sku_strategy,
        "title": st.text(min_size=1, max_size=50),
        "price_vat_excl": st.floats(
            min_value=0.01, max_value=10000, allow_nan=False, allow_infinity=False
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

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def build_erp_json(products: list[dict]) -> str:
    """Serialize a list of ERP product dicts into a JSON string."""
    return json.dumps(products)


def run_pipeline_sync(
    raw_json: str,
    fake_redis: fakeredis.FakeRedis,
    api_base_url: str,
    api_key: str,
) -> dict:
    """Run the full pipeline synchronously with injected dependencies.

    Patches ``redis.Redis.from_url`` inside *delta_sync* to return the
    provided *fake_redis* instance and patches ``EshopAPIClient`` so that
    it is constructed with the given *api_base_url* / *api_key* and a high
    ``max_rps`` to avoid rate-limiting delays in tests.

    Returns the summary dict produced by ``delta_sync``.
    """
    raw_products = load_and_validate(raw_json)
    transformed_products = transform(raw_products)

    with (
        patch(
            "integrator.tasks.redis.Redis.from_url",
            return_value=fake_redis,
        ),
        patch(
            "integrator.tasks.EshopAPIClient",
            lambda **kwargs: EshopAPIClient(
                base_url=api_base_url, api_key=api_key, max_rps=1000
            ),
        ),
    ):
        summary = delta_sync(transformed_products)

    return summary


def get_responses_by_method(method: str) -> list:
    """Return captured ``responses.calls`` filtered by HTTP method."""
    return [c for c in responses.calls if c.request.method == method.upper()]


# ---------------------------------------------------------------------------
# First Run fixture data
# ---------------------------------------------------------------------------
# 5 ERP products: 1 duplicate SKU (SKU-001), 1 invalid price (SKU-004)
# After dedup (last wins): 4 unique  →  after transform: 3 valid

FIRST_RUN_PRODUCTS: list[dict] = [
    {
        "id": "SKU-001",
        "title": "Product A (first occurrence)",
        "price_vat_excl": 100.0,
        "stocks": {"wh1": 5, "wh2": 3},
        "attributes": {"color": "red"},
    },
    {
        "id": "SKU-002",
        "title": "Product B",
        "price_vat_excl": 200.0,
        "stocks": {"wh1": 10, "wh2": "N/A"},
        "attributes": None,
    },
    {
        # Duplicate SKU-001 — this one "wins" (last occurrence)
        "id": "SKU-001",
        "title": "Product A (last wins)",
        "price_vat_excl": 150.0,
        "stocks": {"wh1": 7, "wh2": 2},
        "attributes": {"color": "red"},
    },
    {
        "id": "SKU-003",
        "title": "Product C",
        "price_vat_excl": 50.0,
        "stocks": {"wh1": 0},
        "attributes": {"color": "blue"},
    },
    {
        # Invalid price — will be filtered out in transform
        "id": "SKU-004",
        "title": "Product D (invalid price)",
        "price_vat_excl": None,
        "stocks": {"wh1": 1},
        "attributes": None,
    },
]

FIRST_RUN_JSON = build_erp_json(FIRST_RUN_PRODUCTS)

# ---------------------------------------------------------------------------
# Second Run fixture data
# ---------------------------------------------------------------------------
# SKU-001: UNCHANGED (same data as first run's last-wins version)
# SKU-002: MODIFIED  (changed price)
# SKU-005: NEW product

SECOND_RUN_PRODUCTS: list[dict] = [
    {
        # Unchanged — identical to the last-wins SKU-001 from first run
        "id": "SKU-001",
        "title": "Product A (last wins)",
        "price_vat_excl": 150.0,
        "stocks": {"wh1": 7, "wh2": 2},
        "attributes": {"color": "red"},
    },
    {
        # Modified — price changed from 200.0 to 250.0
        "id": "SKU-002",
        "title": "Product B",
        "price_vat_excl": 250.0,
        "stocks": {"wh1": 10, "wh2": "N/A"},
        "attributes": None,
    },
    {
        # New product
        "id": "SKU-005",
        "title": "Product E (new)",
        "price_vat_excl": 75.0,
        "stocks": {"wh1": 20},
        "attributes": {"color": "green"},
    },
]

SECOND_RUN_JSON = build_erp_json(SECOND_RUN_PRODUCTS)


# ---------------------------------------------------------------------------
# Concrete integration tests
# ---------------------------------------------------------------------------

EXPECTED_FIRST_RUN = {
    "SKU-001": {
        "sku": "SKU-001",
        "title": "Product A (last wins)",
        "price_vat_incl": 181.5,
        "stock_quantity": 9,
        "color": "red",
    },
    "SKU-002": {
        "sku": "SKU-002",
        "title": "Product B",
        "price_vat_incl": 242.0,
        "stock_quantity": 10,
        "color": "N/A",
    },
    "SKU-003": {
        "sku": "SKU-003",
        "title": "Product C",
        "price_vat_incl": 60.5,
        "stock_quantity": 0,
        "color": "blue",
    },
}


@responses.activate
def test_first_run_full_pipeline():
    """First run: all valid products are new → 3× POST, 0 unchanged, 0 errors.

    Validates: Requirements 3.1, 3.2, 3.3, 3.4, 6.1
    """
    fake_redis = fakeredis.FakeRedis()
    responses.add(responses.POST, BASE_URL, json={"ok": True}, status=201)

    summary = run_pipeline_sync(FIRST_RUN_JSON, fake_redis, BASE_URL, API_KEY)

    # -- summary assertions --------------------------------------------------
    assert summary["synced"] == 3
    assert summary["unchanged"] == 0
    assert summary["errors"] == 0

    # -- HTTP call count ------------------------------------------------------
    post_calls = get_responses_by_method("POST")
    assert len(post_calls) == 3

    # -- payload & header assertions ------------------------------------------
    seen_skus: set[str] = set()
    for call in post_calls:
        payload = json.loads(call.request.body)
        sku = payload["sku"]
        seen_skus.add(sku)

        expected = EXPECTED_FIRST_RUN[sku]
        assert payload["title"] == expected["title"]
        assert payload["price_vat_incl"] == expected["price_vat_incl"]
        assert payload["stock_quantity"] == expected["stock_quantity"]
        assert payload["color"] == expected["color"]

        # X-Api-Key header present
        assert call.request.headers.get("X-Api-Key") == API_KEY

    # all three expected SKUs were sent
    assert seen_skus == {"SKU-001", "SKU-002", "SKU-003"}


@responses.activate
def test_second_run_delta_sync():
    """Second run: 1 unchanged (skip), 1 modified (PATCH), 1 new (POST).

    Validates: Requirements 4.1, 4.2, 4.3, 4.4, 6.2, 6.3
    """
    fake_redis = fakeredis.FakeRedis()

    # -- First Run: seed sync state in Redis --------------------------------
    responses.add(responses.POST, BASE_URL, json={"ok": True}, status=201)
    run_pipeline_sync(FIRST_RUN_JSON, fake_redis, BASE_URL, API_KEY)

    # Reset captured calls but keep fakeredis state (hashes persist)
    responses.reset()

    # -- Register mocks for Second Run --------------------------------------
    responses.add(responses.POST, BASE_URL, json={"ok": True}, status=201)
    responses.add(responses.PATCH, BASE_URL + "SKU-002/", json={"ok": True}, status=200)

    # -- Second Run ----------------------------------------------------------
    summary = run_pipeline_sync(SECOND_RUN_JSON, fake_redis, BASE_URL, API_KEY)

    # -- Summary assertions --------------------------------------------------
    assert summary["unchanged"] == 1
    assert summary["synced"] == 2
    assert summary["errors"] == 0

    # -- HTTP call count: only 2 calls (1 PATCH + 1 POST) -------------------
    assert len(responses.calls) == 2

    patch_calls = get_responses_by_method("PATCH")
    post_calls = get_responses_by_method("POST")

    assert len(patch_calls) == 1
    assert len(post_calls) == 1

    # -- PATCH call: modified product SKU-002 --------------------------------
    patch_call = patch_calls[0]
    assert "SKU-002" in patch_call.request.url
    patch_payload = json.loads(patch_call.request.body)
    assert patch_payload["sku"] == "SKU-002"
    assert patch_payload["price_vat_incl"] == round(250.0 * 1.21, 2)

    # -- POST call: new product SKU-005 --------------------------------------
    post_call = post_calls[0]
    assert post_call.request.url == BASE_URL
    post_payload = json.loads(post_call.request.body)
    assert post_payload["sku"] == "SKU-005"
    assert post_payload["price_vat_incl"] == round(75.0 * 1.21, 2)
    assert post_payload["stock_quantity"] == 20
    assert post_payload["color"] == "green"


def test_duplicate_sku_and_invalid_price_filtering():
    """Duplicate SKUs are merged (last wins) and invalid prices are excluded.

    Validates: Requirements 1.1, 1.2
    """
    # -- Step 1: load_and_validate deduplicates SKUs (last wins) -------------
    raw_products = load_and_validate(FIRST_RUN_JSON)

    skus = [p["id"] for p in raw_products]
    assert len(skus) == len(set(skus)), "Expected unique SKUs after load_and_validate"
    assert len(raw_products) == 4  # SKU-001 (deduped), SKU-002, SKU-003, SKU-004

    # SKU-001 should match the LAST occurrence
    sku001 = next(p for p in raw_products if p["id"] == "SKU-001")
    assert sku001["title"] == "Product A (last wins)"
    assert sku001["price_vat_excl"] == 150.0

    # -- Step 2: transform filters out invalid prices ------------------------
    transformed = transform(raw_products)

    transformed_skus = {p["sku"] for p in transformed}
    assert len(transformed) == 3  # SKU-004 (null price) excluded
    assert "SKU-004" not in transformed_skus

    # All remaining products have valid (non-null, non-negative) prices
    for product in transformed:
        assert product["price_vat_incl"] is not None
        assert product["price_vat_incl"] >= 0


# ---------------------------------------------------------------------------
# Property-based tests
# ---------------------------------------------------------------------------


# Feature: broad-integration-test, Property 2: Invalid price filtering through full pipeline
@given(products=st.lists(product_strategy, min_size=1, max_size=20))
@settings(max_examples=50)
def test_property_invalid_price_excluded(products: list[dict]) -> None:
    """For any ERP JSON with null/negative prices, after load_and_validate → transform,
    no product with invalid price shall appear in the output.

    **Validates: Requirements 1.2**
    """
    raw_json = build_erp_json(products)
    raw_products = load_and_validate(raw_json)
    transformed = transform(raw_products)

    for product in transformed:
        assert product["price_vat_excl"] is not None
        assert product["price_vat_excl"] >= 0
        assert product["price_vat_incl"] is not None
        assert product["price_vat_incl"] >= 0


# Feature: broad-integration-test, Property 3: Full pipeline payload correctness
@given(products=st.lists(valid_product_strategy, min_size=1, max_size=10))
@settings(max_examples=50)
def test_property_payload_correctness(products: list[dict]) -> None:
    """For any valid product passing through the complete pipeline, the HTTP
    payload must contain correctly transformed data: sku, title,
    price_vat_incl == round(price_vat_excl * 1.21, 2),
    stock_quantity == sum(int stocks), color from attributes or "N/A".

    **Validates: Requirements 2.1, 2.2, 2.3, 3.3, 6.4**
    """
    # Deduplicate by SKU (last wins) to predict expected output
    unique: dict[str, dict] = {}
    for p in products:
        unique[p["id"]] = p

    with responses.RequestsMock() as rsps:
        rsps.add(responses.POST, BASE_URL, json={"ok": True}, status=201)

        fake_redis = fakeredis.FakeRedis()
        raw_json = build_erp_json(products)
        run_pipeline_sync(raw_json, fake_redis, BASE_URL, API_KEY)

        post_calls = [c for c in rsps.calls if c.request.method == "POST"]

        # Every posted payload must match the expected transformations
        for call in post_calls:
            assert call.request.body is not None
            payload = json.loads(call.request.body)
            sku = payload["sku"]
            original = unique[sku]

            # Title preserved
            assert payload["title"] == original["title"]

            # VAT calculation
            assert payload["price_vat_incl"] == round(
                original["price_vat_excl"] * 1.21, 2
            )

            # Stock aggregation: sum int values, "N/A" → 0
            stocks = original.get("stocks") or {}
            expected_stock = sum(
                v if isinstance(v, int) else 0 for v in stocks.values()
            )
            assert payload["stock_quantity"] == expected_stock

            # Color from attributes or "N/A"
            attrs = original.get("attributes") or {}
            expected_color = attrs.get("color") or "N/A"
            assert payload["color"] == expected_color


# Feature: broad-integration-test, Property 4: First run sends all valid products via POST
@given(products=st.lists(valid_product_strategy, min_size=1, max_size=10))
@settings(max_examples=50)
def test_property_first_run_all_post(products: list[dict]) -> None:
    """For any set of valid products on a first run, delta_sync must send
    exactly one POST per valid product to the base API URL.

    **Validates: Requirements 3.1, 6.1**
    """
    # Deduplicate by SKU (last wins)
    unique: dict[str, dict] = {}
    for p in products:
        unique[p["id"]] = p
    expected_count = len(unique)

    with responses.RequestsMock() as rsps:
        rsps.add(responses.POST, BASE_URL, json={"ok": True}, status=201)

        fake_redis = fakeredis.FakeRedis()
        raw_json = build_erp_json(products)
        run_pipeline_sync(raw_json, fake_redis, BASE_URL, API_KEY)

        post_calls = [c for c in rsps.calls if c.request.method == "POST"]

        # Exactly one POST per valid unique product
        assert len(post_calls) == expected_count

        # All POSTs go to the base URL
        for call in post_calls:
            assert call.request.url == BASE_URL


# Feature: broad-integration-test, Property 5: First run summary correctness
@given(products=st.lists(valid_product_strategy, min_size=1, max_size=10))
@settings(max_examples=50)
def test_property_first_run_summary(products: list[dict]) -> None:
    """For any first run, summary must have synced == number of valid unique
    products, unchanged == 0, and errors == 0.

    **Validates: Requirements 3.2**
    """
    # Deduplicate by SKU (last wins)
    unique: dict[str, dict] = {}
    for p in products:
        unique[p["id"]] = p
    expected_synced = len(unique)

    with responses.RequestsMock() as rsps:
        rsps.add(responses.POST, BASE_URL, json={"ok": True}, status=201)

        fake_redis = fakeredis.FakeRedis()
        raw_json = build_erp_json(products)
        summary = run_pipeline_sync(raw_json, fake_redis, BASE_URL, API_KEY)

        assert summary["synced"] == expected_synced
        assert summary["unchanged"] == 0
        assert summary["errors"] == 0


# Feature: broad-integration-test, Property 6: API key present in all HTTP requests
@given(products=st.lists(valid_product_strategy, min_size=1, max_size=10))
@settings(max_examples=50)
def test_property_api_key_in_headers(products: list[dict]) -> None:
    """For any HTTP request sent by EshopAPIClient, the request headers
    must contain X-Api-Key with the configured API key value.

    **Validates: Requirements 3.4**
    """
    with responses.RequestsMock() as rsps:
        rsps.add(responses.POST, BASE_URL, json={"ok": True}, status=201)

        fake_redis = fakeredis.FakeRedis()
        raw_json = build_erp_json(products)
        run_pipeline_sync(raw_json, fake_redis, BASE_URL, API_KEY)

        # Every HTTP request must have X-Api-Key header
        for call in rsps.calls:
            assert call.request.headers.get("X-Api-Key") == API_KEY


# Feature: broad-integration-test, Property 7: Unchanged products are skipped in subsequent runs
@given(products=st.lists(valid_product_strategy, min_size=1, max_size=10))
@settings(max_examples=50)
def test_property_unchanged_skipped(products: list[dict]) -> None:
    """For any product synced in a previous run whose data has not changed,
    delta_sync in a subsequent run must skip it and generate no HTTP request.

    **Validates: Requirements 4.1**
    """
    fake_redis = fakeredis.FakeRedis()
    raw_json = build_erp_json(products)

    # Deduplicate to know expected count
    unique: dict[str, dict] = {}
    for p in products:
        unique[p["id"]] = p
    expected_count = len(unique)

    # -- First run: all products are new → POST all --------------------------
    with responses.RequestsMock() as rsps:
        rsps.add(responses.POST, BASE_URL, json={"ok": True}, status=201)
        first_summary = run_pipeline_sync(raw_json, fake_redis, BASE_URL, API_KEY)
        assert first_summary["synced"] == expected_count

    # -- Second run: same data → all unchanged, zero HTTP calls --------------
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.add(responses.POST, BASE_URL, json={"ok": True}, status=201)
        second_summary = run_pipeline_sync(raw_json, fake_redis, BASE_URL, API_KEY)

        assert second_summary["unchanged"] == expected_count
        assert second_summary["synced"] == 0
        assert second_summary["errors"] == 0
        assert len(rsps.calls) == 0


# Feature: broad-integration-test, Property 8: Correct HTTP method selection in second run
@given(
    products=st.lists(valid_product_strategy, min_size=1, max_size=5),
    new_products=st.lists(valid_product_strategy, min_size=1, max_size=3),
)
@settings(max_examples=50)
def test_property_method_selection_second_run(
    products: list[dict], new_products: list[dict]
) -> None:
    """For any second run, modified products must be sent via PATCH (URL
    contains SKU) and new products via POST (base URL).

    **Validates: Requirements 4.2, 4.3, 6.2**
    """
    fake_redis = fakeredis.FakeRedis()

    # Deduplicate first-run products by SKU
    unique_first: dict[str, dict] = {}
    for p in products:
        unique_first[p["id"]] = p
    first_run_skus = set(unique_first.keys())

    # -- First run -----------------------------------------------------------
    raw_json_1 = build_erp_json(products)
    with responses.RequestsMock() as rsps:
        rsps.add(responses.POST, BASE_URL, json={"ok": True}, status=201)
        run_pipeline_sync(raw_json_1, fake_redis, BASE_URL, API_KEY)

    # -- Build second run data -----------------------------------------------
    # Modify all first-run products (change price to trigger hash change)
    modified = []
    for _sku, p in unique_first.items():
        mod = dict(p)
        mod["price_vat_excl"] = p["price_vat_excl"] + 1.0  # change price
        modified.append(mod)

    # Filter new products to only those with truly new SKUs
    truly_new = []
    for p in new_products:
        if p["id"] not in first_run_skus:
            truly_new.append(p)

    second_run_data = modified + truly_new
    if not second_run_data:
        return  # nothing to test

    raw_json_2 = build_erp_json(second_run_data)

    # Deduplicate second run
    unique_second: dict[str, dict] = {}
    for p in second_run_data:
        unique_second[p["id"]] = p

    expected_patch_skus = {sku for sku in unique_second if sku in first_run_skus}
    expected_post_skus = {sku for sku in unique_second if sku not in first_run_skus}

    # -- Second run ----------------------------------------------------------
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.add(responses.POST, BASE_URL, json={"ok": True}, status=201)
        rsps.add(
            responses.PATCH,
            re.compile(re.escape(BASE_URL) + r".+"),
            json={"ok": True},
            status=200,
        )

        run_pipeline_sync(raw_json_2, fake_redis, BASE_URL, API_KEY)

        patch_calls = [c for c in rsps.calls if c.request.method == "PATCH"]
        post_calls = [c for c in rsps.calls if c.request.method == "POST"]

        # Modified products → PATCH; verify via payload SKU (URL may be encoded)
        patch_skus = set()
        for call in patch_calls:
            assert call.request.body is not None
            payload = json.loads(call.request.body)
            patch_skus.add(payload["sku"])
            assert call.request.method == "PATCH"

        assert patch_skus == expected_patch_skus

        # New products → POST to base URL
        post_skus = set()
        for call in post_calls:
            assert call.request.body is not None
            payload = json.loads(call.request.body)
            post_skus.add(payload["sku"])
            assert call.request.url == BASE_URL

        assert post_skus == expected_post_skus


# Feature: broad-integration-test, Property 9: Second run summary correctness
@given(
    products=st.lists(valid_product_strategy, min_size=1, max_size=5),
    new_products=st.lists(valid_product_strategy, min_size=1, max_size=3),
)
@settings(max_examples=50)
def test_property_second_run_summary(
    products: list[dict], new_products: list[dict]
) -> None:
    """For any second run with unchanged, modified, and new products, the
    summary must have correct unchanged/synced/errors counts and total HTTP
    calls must equal synced.

    **Validates: Requirements 4.4, 6.3**
    """
    fake_redis = fakeredis.FakeRedis()

    # Deduplicate first-run products by SKU
    unique_first: dict[str, dict] = {}
    for p in products:
        unique_first[p["id"]] = p
    first_run_skus = set(unique_first.keys())

    # -- First run -----------------------------------------------------------
    raw_json_1 = build_erp_json(products)
    with responses.RequestsMock() as rsps:
        rsps.add(responses.POST, BASE_URL, json={"ok": True}, status=201)
        run_pipeline_sync(raw_json_1, fake_redis, BASE_URL, API_KEY)

    # -- Build second run data -----------------------------------------------
    # Keep some unchanged, modify others, add new
    unchanged = []
    modified = []
    for i, (sku, p) in enumerate(unique_first.items()):
        if i % 2 == 0:
            # Unchanged — keep as-is
            unchanged.append(p)
        else:
            # Modified — change price
            mod = dict(p)
            mod["price_vat_excl"] = p["price_vat_excl"] + 1.0
            modified.append(mod)

    truly_new = [p for p in new_products if p["id"] not in first_run_skus]

    second_run_data = unchanged + modified + truly_new
    if not second_run_data:
        return

    raw_json_2 = build_erp_json(second_run_data)

    # Deduplicate second run to predict counts
    unique_second: dict[str, dict] = {}
    for p in second_run_data:
        unique_second[p["id"]] = p

    expected_unchanged_skus = set()
    expected_synced_skus = set()
    for sku, p in unique_second.items():
        if sku in first_run_skus:
            # Check if data actually changed by comparing with first run
            orig = unique_first[sku]
            if p == orig:
                expected_unchanged_skus.add(sku)
            else:
                expected_synced_skus.add(sku)
        else:
            expected_synced_skus.add(sku)

    # -- Second run ----------------------------------------------------------
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.add(responses.POST, BASE_URL, json={"ok": True}, status=201)
        rsps.add(
            responses.PATCH,
            re.compile(re.escape(BASE_URL) + r".+"),
            json={"ok": True},
            status=200,
        )

        summary = run_pipeline_sync(raw_json_2, fake_redis, BASE_URL, API_KEY)

        assert summary["unchanged"] == len(expected_unchanged_skus)
        assert summary["synced"] == len(expected_synced_skus)
        assert summary["errors"] == 0
        assert len(rsps.calls) == summary["synced"]
