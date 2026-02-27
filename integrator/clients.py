"""E-shop API client with rate limiting and retry logic."""

from __future__ import annotations

import logging
import time

import requests
from django.conf import settings

from integrator.services import TokenBucketRateLimiter

logger = logging.getLogger(__name__)


class EshopAPIClient:
    """HTTP klient pro e-shop REST API s rate limitingem a retry logikou.

    Odesílá produktová data do e-shopu pomocí POST (nový produkt)
    nebo PATCH (aktualizace existujícího). Automaticky dodržuje
    maximální počet požadavků za sekundu a při HTTP 429 provede
    jeden opakovaný pokus po uplynutí doby z hlavičky Retry-After.

    Attributes:
        base_url: Základní URL e-shop API.
        api_key: API klíč pro autentizaci.
        rate_limiter: Instance TokenBucketRateLimiter pro řízení rychlosti.
    """

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        max_rps: int = 5,
        rate_limiter: TokenBucketRateLimiter | None = None,
    ):
        """Inicializuje klienta s konfigurací API a rate limiterem.

        Args:
            base_url: Základní URL e-shop API. Pokud není zadáno,
                použije se hodnota z ``settings.ESHOP_API_BASE_URL``.
            api_key: API klíč. Pokud není zadán,
                použije se hodnota z ``settings.ESHOP_API_KEY``.
            max_rps: Maximální počet požadavků za sekundu (výchozí 5).
            rate_limiter: Vlastní instance rate limiteru. Pokud není zadána,
                vytvoří se nová s ``max_rps``.
        """
        self.base_url = base_url or settings.ESHOP_API_BASE_URL
        self.api_key = api_key or settings.ESHOP_API_KEY
        self.rate_limiter = rate_limiter or TokenBucketRateLimiter(max_rps=max_rps)

    def _wait_for_rate_limit(self) -> None:
        """Počká potřebnou dobu pro dodržení maximálního počtu požadavků za sekundu.

        Dotáže se rate limiteru na potřebnou čekací dobu a pokud je nenulová,
        uspí vlákno. Po probuzení zaznamená provedení požadavku.
        """
        wait_time = self.rate_limiter.acquire()
        if wait_time > 0:
            time.sleep(wait_time)
        self.rate_limiter.record_request()

    def send_product(
        self, product: dict, is_update: bool
    ) -> tuple[dict | None, str | None]:
        """Odešle produkt do e-shop API jako POST (nový) nebo PATCH (aktualizace).

        Při HTTP 429 (rate limit) provede jeden opakovaný pokus po uplynutí
        doby uvedené v hlavičce ``Retry-After``.

        Args:
            product: Slovník s daty produktu. Musí obsahovat klíče
                ``sku``, ``title``, ``price_vat_incl``, ``stock_quantity``, ``color``.
            is_update: ``True`` pro PATCH (aktualizace), ``False`` pro POST (nový).

        Returns:
            Tuple ``(response_data, None)`` při úspěchu,
            nebo ``(None, error_message)`` při chybě.
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
            resp = requests.request(
                method, url, json=payload, headers=headers, timeout=1
            )
        except requests.RequestException as exc:
            error = f"[EshopAPIClient.send_product] Request failed for SKU {sku}: {exc}"
            logger.error(error)
            return None, error

        # Retry on 429
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", 1))
            logger.warning(
                "[EshopAPIClient.send_product] Rate limited for SKU %s, retrying after %.1fs",
                sku,
                retry_after,
            )
            time.sleep(retry_after)
            self.rate_limiter.record_request()
            try:
                resp = requests.request(
                    method, url, json=payload, headers=headers, timeout=1
                )
            except requests.RequestException as exc:
                error = f"[EshopAPIClient.send_product] Retry request failed for SKU {sku}: {exc}"
                logger.error(error)
                return None, error

        if resp.status_code >= 400:
            error = f"[EshopAPIClient.send_product] API error for SKU {sku}: HTTP {resp.status_code}"
            logger.error(error)
            return None, error

        return resp.json(), None
