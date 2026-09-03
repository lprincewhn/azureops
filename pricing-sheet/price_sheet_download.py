"""Shared Azure Price Sheet download helpers."""

from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

MANAGEMENT_SCOPE = "https://management.azure.com/.default"
MANAGEMENT_URL = "https://management.azure.com"
API_VERSION = "2025-03-01"
TERMINAL_SUCCESS = {"completed", "succeeded"}
TERMINAL_FAILURE = {"failed", "canceled", "cancelled"}


def azure_cli_credential() -> Any:
    """Create a credential backed by the current Azure CLI login."""
    try:
        from azure.identity import AzureCliCredential
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency azure-identity; run "
            "'pip install -r requirements.txt'."
        ) from exc
    return AzureCliCredential()


def _request_json(
    url: str, credential: Any, method: str = "GET"
) -> tuple[int, Any, dict[str, str]]:
    token = credential.get_token(MANAGEMENT_SCOPE).token
    request = urllib.request.Request(
        url,
        data=b"" if method == "POST" else None,
        headers={
            "Authorization": "Bearer " + token,
            "Accept": "application/json",
        },
        method=method,
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        body = response.read()
        payload = json.loads(body) if body else {}
        return response.status, payload, dict(response.headers.items())


def _header(headers: dict[str, str], name: str) -> str:
    return next(
        (value for key, value in headers.items() if key.lower() == name.lower()),
        "",
    )


def _retry_after(headers: dict[str, str], default: int = 10) -> int:
    value = _header(headers, "Retry-After")
    try:
        return max(0, int(value)) if value else default
    except ValueError:
        return default


def _poll(
    status_url: str,
    credential: Any,
    timeout: int,
) -> tuple[dict[str, Any], dict[str, str]]:
    deadline = time.monotonic() + timeout
    delay = 0
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"Azure Price Sheet was not ready within {timeout} seconds."
            )
        if delay:
            time.sleep(min(delay, remaining))

        status_code, payload, headers = _request_json(status_url, credential)
        if status_code not in {200, 202}:
            raise RuntimeError(
                f"Unexpected Price Sheet status response: HTTP {status_code}."
            )
        status = str(payload.get("status") or "").strip().lower()
        if status in TERMINAL_SUCCESS:
            return payload, headers
        if status in TERMINAL_FAILURE:
            error = payload.get("error")
            detail = f": {error}" if error else ""
            raise RuntimeError(f"Price Sheet generation {status}{detail}.")
        delay = _retry_after(headers)


def _quote(value: str) -> str:
    return urllib.parse.quote(value, safe=":-_")


def request_by_billing_profile(
    billing_account: str,
    billing_profile: str,
    credential: Any,
    timeout: int,
) -> dict[str, Any]:
    """Generate the current Price Sheet for an MCA billing profile."""
    url = (
        f"{MANAGEMENT_URL}/providers/Microsoft.Billing/"
        f"billingAccounts/{_quote(billing_account)}/"
        f"billingProfiles/{_quote(billing_profile)}/"
        "providers/Microsoft.CostManagement/pricesheets/default/download"
        f"?api-version={API_VERSION}"
    )
    status_code, payload, headers = _request_json(
        url, credential, method="POST"
    )
    if status_code == 200:
        return payload
    if status_code != 202:
        raise RuntimeError(
            f"Unexpected Price Sheet response: HTTP {status_code}."
        )
    status_url = _header(headers, "Azure-Consumption-AsyncOperation")
    status_url = status_url or _header(headers, "Azure-AsyncOperation")
    if not status_url:
        raise RuntimeError(
            "Price Sheet response did not include an asynchronous status URL."
        )
    result, _ = _poll(status_url, credential, timeout)
    return result


def request_by_invoice(
    billing_account: str,
    billing_profile: str,
    invoice_id: str,
    credential: Any,
    timeout: int,
) -> dict[str, Any]:
    """Generate the Price Sheet associated with an MCA invoice."""
    url = (
        f"{MANAGEMENT_URL}/providers/Microsoft.Billing/"
        f"billingAccounts/{_quote(billing_account)}/"
        f"billingProfiles/{_quote(billing_profile)}/"
        f"invoices/{_quote(invoice_id)}/"
        "providers/Microsoft.CostManagement/pricesheets/default/download"
        f"?api-version={API_VERSION}"
    )
    status_code, payload, headers = _request_json(
        url, credential, method="POST"
    )
    if status_code == 200:
        return payload
    if status_code != 202:
        raise RuntimeError(
            f"Unexpected Price Sheet response: HTTP {status_code}."
        )

    status_url = _header(headers, "Azure-AsyncOperation")
    location_url = _header(headers, "Location")
    if not status_url or not location_url:
        raise RuntimeError(
            "Price Sheet response did not include Azure-AsyncOperation "
            "and Location URLs."
        )
    _poll(status_url, credential, timeout)
    result_code, result, _ = _request_json(location_url, credential)
    if result_code != 200:
        raise RuntimeError(
            f"Unexpected Price Sheet result response: HTTP {result_code}."
        )
    return result


def extract_download_url(result: dict[str, Any]) -> str:
    """Extract the temporary file URL from supported API response shapes."""
    properties = result.get("properties") or {}
    published = result.get("publishedEntity") or {}
    published_properties = published.get("properties") or {}
    for container in (result, properties, published_properties):
        for key in ("downloadUrl", "reportUrl"):
            value = container.get(key)
            if value:
                return str(value)
    raise RuntimeError("Price Sheet response did not include a download URL.")


def download_file(url: str, output: Path) -> None:
    """Stream a temporary Azure download URL to a local file atomically."""
    output = output.expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    partial = output.with_name(f".{output.name}.part")
    try:
        with urllib.request.urlopen(url, timeout=120) as response:
            with partial.open("wb") as target:
                while chunk := response.read(1024 * 1024):
                    target.write(chunk)
        os.replace(partial, output)
    finally:
        partial.unlink(missing_ok=True)


def positive_timeout(value: str) -> int:
    timeout = int(value)
    if timeout <= 0:
        raise ValueError("timeout must be greater than zero")
    return timeout
