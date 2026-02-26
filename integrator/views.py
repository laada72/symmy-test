"""Mock e-shop API views for integration testing with random failures."""

import json
import random
import time

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods


@csrf_exempt
@require_http_methods(["POST", "PATCH"])
def mock_product_endpoint(request, sku=None):
    """Accept POST/PATCH with random failures: 429 (15%), 500 (10%), timeout (5%)."""
    roll = random.random()

    # 5% chance: simulate timeout (sleep 15s, client has 10s timeout)
    if roll < 0.05:
        time.sleep(2)

    # 15% chance: rate limit
    if roll < 0.20:
        return JsonResponse(
            {"error": "Too Many Requests"},
            status=429,
            headers={"Retry-After": "1"},
        )

    # 10% chance: server error
    if roll < 0.30:
        return JsonResponse({"error": "Internal Server Error"}, status=500)

    # 70% chance: success
    data = json.loads(request.body) if request.body else {}
    status = 201 if request.method == "POST" else 200
    label = "created" if request.method == "POST" else "updated"
    return JsonResponse({"status": label, **data}, status=status)
