"""Management command pro spuštění ERP → E-shop sync pipeline z JSON souboru."""

from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from integrator.tasks import run_sync_pipeline


class Command(BaseCommand):
    """Django management příkaz pro spuštění ERP → E-shop sync pipeline ze souboru.

    Načte JSON soubor s produktovými daty z ERP a spustí asynchronní
    synchronizační pipeline přes Celery. Výchozí soubor je
    ``erp_data.json`` v kořenovém adresáři projektu.

    Použití::

        python manage.py json_file_sync
        python manage.py json_file_sync --json-file /cesta/k/souboru.json
    """

    help = "Read a JSON file and run the ERP → E-shop sync pipeline."

    def add_arguments(self, parser):
        """Definuje CLI argumenty příkazu.

        Args:
            parser: Instance ``ArgumentParser`` pro registraci argumentů.
        """
        parser.add_argument(
            "--json-file",
            type=str,
            default=None,
            help="Path to the ERP JSON data file. Defaults to erp_data.json in BASE_DIR.",
        )

    def handle(self, *args, **options):
        """Spustí synchronizační pipeline.

        Načte JSON soubor, ověří jeho existenci a odešle data
        do Celery pipeline. Vypíše ID dispatchnutého tasku.

        Args:
            *args: Poziční argumenty (nepoužívané).
            **options: CLI argumenty včetně ``json_file``.

        Raises:
            CommandError: Pokud zadaný soubor neexistuje.
        """
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
