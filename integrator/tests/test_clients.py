"""Property-based and unit tests for EshopAPIClient."""

import time

import responses
from hypothesis import given, settings
from hypothesis import strategies as st

from integrator.clients import EshopAPIClient

BASE_URL = "https://api.fake-eshop.cz/v1/products/"

sku_strategy = st.from_regex(r"[A-Za-z0-9_-]{1,20}", fullmatch=True)

product_strategy = st.fixed_dictionaries({
    "sku": sku_strategy,
    "title": st.text(min_size=1, max_size=50),
    "price_vat_incl": st.floats(min_value=0, max_value=15000, allow_nan=False, allow_infinity=False),
    "stock_quantity": st.integers(min_value=0, max_value=9999),
    "color": st.text(min_size=1, max_size=20),
})


# -- Property 9: Volba HTTP metody (POST vs PATCH) --
# Feature: erp-eshop-sync, Property 9: Volba HTTP metody (POST vs PATCH)

@responses.activate
@given(product=product_strategy, is_update=st.booleans())
@settings(max_examples=100)
def test_property_http_method_selection(product: dict, is_update: bool) -> None:
    """If product was not previously synced, client must use POST;
    if it was previously synced, client must use PATCH to URL containing SKU."""
    sku = product["sku"]

    # Register both POST and PATCH so either can match
    responses.add(responses.POST, BASE_URL, json={"ok": True}, status=201)
    responses.add(responses.PATCH, f"{BASE_URL}{sku}/", json={"ok": True}, status=200)

    client = EshopAPIClient(base_url=BASE_URL, api_key="test-key", max_rps=1000)
    client.send_product(product, is_update=is_update)

    last_call = responses.calls[-1]

    if is_update:
        assert last_call.request.method == "PATCH"
        assert last_call.request.url == f"{BASE_URL}{sku}/"
    else:
        assert last_call.request.method == "POST"
        assert last_call.request.url == BASE_URL

    responses.reset()


# -- Property 10: Rate limiting --
# Feature: erp-eshop-sync, Property 10: Rate limiting

@responses.activate
@given(n=st.integers(min_value=2, max_value=10))
@settings(max_examples=20, deadline=None)
def test_property_rate_limiting(n: int) -> None:
    """For N requests, total duration must be at least (N-1)/max_rps seconds."""
    max_rps = 5

    responses.add(responses.POST, BASE_URL, json={"ok": True}, status=201)

    client = EshopAPIClient(base_url=BASE_URL, api_key="test-key", max_rps=max_rps)

    product = {
        "sku": "RATE-TEST",
        "title": "Test",
        "price_vat_incl": 121.0,
        "stock_quantity": 10,
        "color": "red",
    }

    start = time.monotonic()
    for _ in range(n):
        client.send_product(product, is_update=False)
    elapsed = time.monotonic() - start

    min_expected = (n - 1) / max_rps
    assert elapsed >= min_expected * 0.95, (
        f"Sent {n} requests in {elapsed:.3f}s, expected >= {min_expected:.3f}s"
    )

    responses.reset()


# -- Unit test: Retry on 429 with Retry-After header --

@responses.activate
def test_retry_on_429_with_retry_after() -> None:
    """When API returns 429, client must wait Retry-After and retry."""
    responses.add(
        responses.POST, BASE_URL,
        json={"error": "rate limited"}, status=429,
        headers={"Retry-After": "0.1"},
    )
    responses.add(
        responses.POST, BASE_URL,
        json={"id": "SKU-001"}, status=201,
    )

    client = EshopAPIClient(base_url=BASE_URL, api_key="test-key", max_rps=1000)
    product = {
        "sku": "SKU-001",
        "title": "Test",
        "price_vat_incl": 121.0,
        "stock_quantity": 5,
        "color": "blue",
    }

    data, error = client.send_product(product, is_update=False)

    assert error is None
    assert data == {"id": "SKU-001"}
    assert len(responses.calls) == 2
    assert responses.calls[0].response.status_code == 429
    assert responses.calls[1].response.status_code == 201


# -- Unit test: Error logging on 500 --

@responses.activate
def test_error_logging_on_500(caplog) -> None:
    """When API returns 500, client must log error with SKU and status code."""
    responses.add(
        responses.PATCH, f"{BASE_URL}SKU-ERR/",
        json={"error": "internal"}, status=500,
    )

    client = EshopAPIClient(base_url=BASE_URL, api_key="test-key", max_rps=1000)
    product = {
        "sku": "SKU-ERR",
        "title": "Test",
        "price_vat_incl": 121.0,
        "stock_quantity": 0,
        "color": "N/A",
    }

    data, error = client.send_product(product, is_update=True)

    assert data is None
    assert error is not None
    assert "SKU-ERR" in error
    assert "500" in error
