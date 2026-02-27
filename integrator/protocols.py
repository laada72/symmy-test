"""Protocol definitions for the integrator application."""

from typing import Protocol


class DeltaStrategy(Protocol):
    """Protocol for computing content hashes for delta detection."""

    def compute_hash(self, product: dict) -> str:
        """Compute content hash for a product."""
        ...
