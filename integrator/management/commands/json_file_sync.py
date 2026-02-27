"""Management command to trigger the ERP → E-shop sync pipeline from a JSON file."""

from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from integrator.tasks import run_sync_pipeline


class Command(BaseCommand):
    help = "Read a JSON file and run the ERP → E-shop sync pipeline."

    def add_arguments(self, parser):
        parser.add_argument(
            "--json-file",
            type=str,
            default=None,
            help="Path to the ERP JSON data file. Defaults to erp_data.json in BASE_DIR.",
        )

    def handle(self, *args, **options):
        json_file = options["json_file"]
        path = (
            Path(json_file) if json_file else Path(settings.BASE_DIR) / "erp_data.json"
        )

        if not path.exists():
            raise CommandError(f"File not found: {path}")

        raw_json = path.read_text(encoding="utf-8")
        self.stdout.write(f"Starting sync pipeline from {path}...")

        result = run_sync_pipeline(raw_json=raw_json)
        self.stdout.write(self.style.SUCCESS(f"Pipeline dispatched: {result.id}"))
