"""Django models for the integrator application."""

from django.db import models


class SyncRecord(models.Model):
    """Perzistentní sync stav produktu v PostgreSQL jako fallback pro Redis."""

    sku = models.CharField(
        max_length=50,
        primary_key=True,
        help_text="Unikátní identifikátor produktu (Stock Keeping Unit)",
    )
    content_hash = models.CharField(
        max_length=64,
        help_text="SHA-256 hash kanonické JSON reprezentace produktu",
    )
    last_synced = models.DateTimeField(
        help_text="Časové razítko poslední úspěšné synchronizace",
    )
    synced = models.BooleanField(
        default=False,
        help_text="Příznak, zda byl produkt úspěšně synchronizován",
    )

    class Meta:
        db_table = "integrator_syncrecord"
        verbose_name = "Sync Record"
        verbose_name_plural = "Sync Records"

    def __str__(self) -> str:
        return f"SyncRecord(sku={self.sku}, synced={self.synced})"
