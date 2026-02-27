"""Django admin configuration for the integrator application."""

from django.contrib import admin

from integrator.models import SyncRecord


@admin.register(SyncRecord)
class SyncRecordAdmin(admin.ModelAdmin):  # type: ignore[type-arg]
    """Admin konfigurace pro model SyncRecord.

    Zobrazuje přehled synchronizačních záznamů s možností filtrování
    podle stavu synchronizace a vyhledávání podle SKU.
    Všechna pole jsou pouze pro čtení (stav se mění pouze programově).
    """

    list_display = ("sku", "content_hash", "last_synced", "synced")
    list_filter = ("synced",)
    search_fields = ("sku",)
    readonly_fields = ("sku", "content_hash", "last_synced", "synced")
