"""E-shop API client with rate limiting and retry logic."""

import logging
import time

import requests
from django.conf import settings

logger = logging.getLogger(__name__)


class EshopAPIClient:
    """HTTP client for e-shop REST API with rate limiting and retry."""

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        max_rps: int = 5,
    ):
        self.base_url = base_url or settings.ESHOP_API_BASE_URL
        self.api_key = api_key or settings.ESHOP_API_KEY
        self.max_rps = max_rps
        self._min_interval = 1.0 / max_rps
        self._last_request_time: float = 0.0

    def _wait_for_rate_limit(self) -> None:
        """Sleep if needed to respect max requests per second."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.monotonic()

    def send_product(
        self, product: dict, is_update: bool
    ) -> tuple[dict | None, str | None]:
        """
        POST (new) or PATCH (existing) product to e-shop API.

        Returns (response_data, error_message).
        """
        sku = product["sku"]
        payload = {
            "sku": sku,
            "title": product["title"],
            "price_vat_incl": product["price_vat_incl"],
            "stock_quantity": product["stock_quantity"],
            "color": product["color"],
        }
        headers = {"X-Api-Key": self.api_key}

        if is_update:
            url = f"{self.base_url}{sku}/"
            method = "PATCH"
        else:
            url = self.base_url
            method = "POST"

        self._wait_for_rate_limit()

        try:
            resp = requests.request(method, url, json=payload, headers=headers, timeout=1)
        except requests.RequestException as exc:
            error = f"[EshopAPIClient.send_product] Request failed for SKU {sku}: {exc}"
            logger.error(error)
            return None, error

        # Retry on 429
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", 1))
            logger.warning("[EshopAPIClient.send_product] Rate limited for SKU %s, retrying after %.1fs", sku, retry_after)
            time.sleep(retry_after)
            self._last_request_time = time.monotonic()
            try:
                resp = requests.request(method, url, json=payload, headers=headers, timeout=1)
            except requests.RequestException as exc:
                error = f"[EshopAPIClient.send_product] Retry request failed for SKU {sku}: {exc}"
                logger.error(error)
                return None, error

        if resp.status_code >= 400:
            error = f"[EshopAPIClient.send_product] API error for SKU {sku}: HTTP {resp.status_code}"
            logger.error(error)
            return None, error

        return resp.json(), None
