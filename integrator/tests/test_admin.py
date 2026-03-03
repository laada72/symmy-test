"""Unit tests for integrator Django admin configuration."""

from django.contrib import admin

from integrator.models import SyncRecord


def test_syncrecord_registered_in_admin() -> None:
    """SyncRecord must be registered in admin.site._registry.

    Requirements: 8.6
    """
    assert SyncRecord in admin.site._registry


def test_syncrecord_admin_list_display() -> None:
    """SyncRecordAdmin list_display contains expected fields."""
    model_admin = admin.site._registry[SyncRecord]
    assert "sku" in model_admin.list_display
    assert "content_hash" in model_admin.list_display
    assert "last_synced" in model_admin.list_display
    assert "synced" in model_admin.list_display


def test_syncrecord_admin_list_filter() -> None:
    """SyncRecordAdmin list_filter contains 'synced'."""
    model_admin = admin.site._registry[SyncRecord]
    assert "synced" in model_admin.list_filter


def test_syncrecord_admin_search_fields() -> None:
    """SyncRecordAdmin search_fields contains 'sku'."""
    model_admin = admin.site._registry[SyncRecord]
    assert "sku" in model_admin.search_fields


def test_syncrecord_admin_readonly_fields() -> None:
    """SyncRecordAdmin readonly_fields contains all fields."""
    model_admin = admin.site._registry[SyncRecord]
    for field in ("sku", "content_hash", "last_synced", "synced"):
        assert field in model_admin.readonly_fields
