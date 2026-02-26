"""Protocol definitions for the integrator application."""

from typing import Protocol


class ERPDataLoader(Protocol):
    """Protocol for loading raw product data from an ERP source."""

    def load(self) -> list[dict]:
        """Load raw product data from ERP source."""
        ...


class DeltaStrategy(Protocol):
    """Protocol for computing content hashes for delta detection."""

    def compute_hash(self, product: dict) -> str:
        """Compute content hash for a product."""
        ...
