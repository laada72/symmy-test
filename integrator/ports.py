"""Port definitions (Python Protocols) for the integrator domain layer.

No imports from Django, Redis, or requests — pure interface contracts.
"""

from __future__ import annotations

from typing import Protocol


class EshopPort(Protocol):
    """Interface for sending products to the e-shop API."""

    def send_product(
        self, product: dict, is_update: bool
    ) -> tuple[dict | None, str | None]:
        """Send a product to the e-shop.

        Args:
            product: Transformed product dict.
            is_update: True for PATCH (existing), False for POST (new).

        Returns:
            (response_data, None) on success, (None, error_message) on failure.
        """
        ...


class SyncStatePort(Protocol):
    """Interface for reading and writing product sync state."""

    def filter_changed(self, products: list[dict]) -> tuple[list[dict], int]:
        """Return only products whose content hash has changed.

        Returns:
            (changed_products, skipped_count)
        """
        ...

    def mark_synced(self, sku: str, content_hash: str) -> None:
        """Record a successful sync for the given SKU."""
        ...

    def was_previously_synced(self, sku: str) -> bool:
        """Return True if the SKU was previously synced (POST vs PATCH decision)."""
        ...
