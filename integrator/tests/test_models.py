"""Unit tests for integrator Django models."""

from datetime import datetime, timezone

from integrator.models import SyncRecord


def test_syncrecord_field_names() -> None:
    """SyncRecord has the required fields with correct attributes.

    Requirements: 8.1
    """
    field_names = {f.name for f in SyncRecord._meta.get_fields()}
    assert "sku" in field_names
    assert "content_hash" in field_names
    assert "last_synced" in field_names
    assert "synced" in field_names


def test_syncrecord_sku_is_primary_key() -> None:
    """sku field is the primary key."""
    pk_field = SyncRecord._meta.pk
    assert pk_field is not None
    assert pk_field.name == "sku"


def test_syncrecord_synced_default_false() -> None:
    """synced field defaults to False."""
    synced_field = SyncRecord._meta.get_field("synced")
    assert synced_field.default is False


def test_syncrecord_content_hash_max_length() -> None:
    """content_hash max_length is 64 (SHA-256 hex)."""
    field = SyncRecord._meta.get_field("content_hash")
    assert field.max_length == 64


def test_syncrecord_sku_max_length() -> None:
    """sku max_length is 50."""
    field = SyncRecord._meta.get_field("sku")
    assert field.max_length == 50


def test_syncrecord_str() -> None:
    """__str__ returns expected representation — no DB needed."""
    now = datetime.now(timezone.utc)
    record = SyncRecord(
        sku="SKU-STR", content_hash="c" * 64, last_synced=now, synced=True
    )
    assert str(record) == "SyncRecord(sku=SKU-STR, synced=True)"


def test_syncrecord_meta() -> None:
    """Meta db_table, verbose_name, verbose_name_plural are correct."""
    assert SyncRecord._meta.db_table == "integrator_syncrecord"
    assert str(SyncRecord._meta.verbose_name) == "Sync Record"
    assert str(SyncRecord._meta.verbose_name_plural) == "Sync Records"


# ---------------------------------------------------------------------------
# DB-backed tests — skipped when PostgreSQL is not reachable
# ---------------------------------------------------------------------------

import pytest


def _db_available() -> bool:
    """Check DB reachability via TCP socket — safe at collection time."""
    import socket

    from django.conf import settings

    db = settings.DATABASES["default"]
    host = db.get("HOST", "localhost")
    port = int(db.get("PORT", 5432))
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


skip_no_db = pytest.mark.skipif(
    not _db_available(),
    reason="PostgreSQL not reachable — run inside Docker to execute DB tests",
)


@skip_no_db
@pytest.mark.django_db
def test_syncrecord_create_and_retrieve() -> None:
    """SyncRecord can be created and retrieved from the real DB."""
    now = datetime.now(timezone.utc)
    SyncRecord.objects.create(
        sku="SKU-DB-001",
        content_hash="a" * 64,
        last_synced=now,
    )
    record = SyncRecord.objects.get(sku="SKU-DB-001")
    assert record.content_hash == "a" * 64
    assert record.synced is False


@skip_no_db
@pytest.mark.django_db
def test_syncrecord_update_or_create() -> None:
    """update_or_create upserts correctly."""
    now = datetime.now(timezone.utc)
    SyncRecord.objects.update_or_create(
        sku="SKU-DB-002",
        defaults={"content_hash": "b" * 64, "last_synced": now, "synced": True},
    )
    record = SyncRecord.objects.get(sku="SKU-DB-002")
    assert record.synced is True
    assert record.content_hash == "b" * 64
