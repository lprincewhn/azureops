#!/usr/bin/env python3
"""Download Cost Details reports and reconcile ActualCost with invoices."""

from __future__ import annotations

import argparse
import calendar
import csv
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from tempfile import TemporaryFile
from typing import Any


MANAGEMENT_URL = "https://management.azure.com"
getcontext().prec = 40
USAGE_API_VERSION = "2024-08-01"
COST_DETAILS_API_VERSION = "2025-03-01"
BILLING_API_VERSION = "2024-04-01"
INVOICE_FIELDS = ("subTotal", "taxAmount", "totalAmount", "billedAmount", "amountDue")
CSV_FIELDS = (
    "usageDetailId", "invoiceId", "costInBillingCurrency", "billingCurrency",
    "ResourceId", "subscriptionId", "date", "meterId", "meterCategory",
    "reservationId", "benefitId", "pricingModel", "chargeType", "quantity", "tags",
)
ISSUE_FIELDS = (
    "csvRow", "usageDetailId", "invoiceId", "reason",
    "billingCurrency", "costInBillingCurrency",
)


def money(value: Any, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{label}: missing or invalid amount {value!r}") from exc
    if not result.is_finite():
        raise ValueError(f"{label}: amount must be finite")
    return result


def text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def currency(value: Any, label: str) -> str:
    result = text(value).upper()
    if not re.fullmatch(r"[A-Z]{3}", result):
        raise ValueError(f"{label}: missing or invalid billing currency")
    return result


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("Unexpected HTTP redirect")


class AzureAPI:
    def __init__(self, credential: Any):
        self.credential = credential
        self.opener = urllib.request.build_opener(NoRedirect())

    def request(
        self, url: str, method: str = "GET", *, body: dict | None = None,
        deadline: float | None = None,
    ) -> tuple[int, dict, Any]:
        parsed = urllib.parse.urlsplit(url)
        if (
            parsed.scheme != "https" or parsed.netloc != "management.azure.com"
            or parsed.fragment
        ):
            raise ValueError("Refusing a non-Azure-management API URL")
        for attempt in range(5):
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("Cost Details report generation timed out")
            token = self.credential.get_token(MANAGEMENT_URL + "/.default").token
            request = urllib.request.Request(
                url, data=json.dumps(body).encode("utf-8") if body is not None
                else b"" if method == "POST" else None, method=method,
                headers={
                    "Authorization": "Bearer " + token, "Accept": "application/json",
                    **({"Content-Type": "application/json"} if body is not None else {}),
                },
            )
            try:
                timeout = 120 if deadline is None else min(120, deadline - time.monotonic())
                if timeout <= 0:
                    raise TimeoutError("Cost Details report generation timed out")
                with self.opener.open(request, timeout=timeout) as response:
                    response_body = response.read()
                    payload = json.loads(response_body, parse_float=Decimal) if response_body else {}
                    if not isinstance(payload, dict):
                        raise ValueError("Azure API response must be a JSON object")
                    return response.status, payload, response.headers
            except urllib.error.HTTPError as exc:
                if exc.code not in {429, 500, 502, 503, 504} or attempt == 4:
                    raise
                delays = [
                    int(value) for name, value in exc.headers.items()
                    if (
                        name.lower() == "retry-after"
                        or name.lower().startswith("x-ms-ratelimit-")
                        and name.lower().endswith("-retry-after")
                    ) and value.isdigit()
                ]
                delay = max(delays) if delays else 2 ** attempt
                if delay > 600:
                    raise RuntimeError(
                        f"Azure requires a {delay}-second retry delay; retry the download later"
                    ) from exc
                if deadline is not None and time.monotonic() + delay >= deadline:
                    raise TimeoutError("Cost Details report retry exceeds timeout") from exc
                exc.close()
                time.sleep(delay)
        raise AssertionError("Unreachable retry state")

    def list(self, url: str) -> list[dict]:
        return list(self.iter_list(url))

    def iter_list(self, url: str) -> Iterator[dict]:
        seen: set[str] = set()
        while url:
            if url in seen:
                raise ValueError("Azure pagination contains a repeated nextLink")
            seen.add(url)
            status, payload, _ = self.request(url)
            if status == 204:
                return
            if status != 200 or not isinstance(payload.get("value"), list):
                raise ValueError(f"Invalid Azure list response (HTTP {status})")
            if not all(isinstance(item, dict) for item in payload["value"]):
                raise ValueError("Azure list contains a non-object record")
            next_link = payload.get("nextLink")
            if next_link is not None and not isinstance(next_link, str):
                raise ValueError("Invalid Azure nextLink")
            yield from payload["value"]
            url = urllib.parse.urljoin(url, next_link) if next_link else ""
            del payload


def build_urls(args: argparse.Namespace) -> tuple[str, dict[str, str], str]:
    quote = lambda value: urllib.parse.quote(value, safe=":-_")
    account = (
        MANAGEMENT_URL + "/providers/Microsoft.Billing/billingAccounts/"
        + quote(args.billing_account_name)
    )
    scope = account
    if args.billing_profile_name:
        scope += "/billingProfiles/" + quote(args.billing_profile_name)
    params = {
        "api-version": USAGE_API_VERSION,
        "$expand": "meterDetails,additionalInfo",
    }
    if args.usage_schema == "modern":
        # Modern accounts use date query parameters, not the legacy date filter.
        params.update(startDate=args.start_date.isoformat(), endDate=args.end_date.isoformat())
    else:
        params["$filter"] = (
            f"properties/usageStart ge '{args.start_date.isoformat()}' and "
            f"properties/usageEnd le '{args.end_date.isoformat()}'"
        )
    usage_urls = {
        metric: scope + "/providers/Microsoft.Consumption/usageDetails?"
        + urllib.parse.urlencode({**params, "metric": metric})
        for metric in ("ActualCost", "AmortizedCost")
    }
    if args.usage_source == "cost-details":
        usage_urls = {
            metric: scope + "/providers/Microsoft.CostManagement/generateCostDetailsReport?"
            + urllib.parse.urlencode({"api-version": COST_DETAILS_API_VERSION})
            for metric in ("ActualCost", "AmortizedCost")
        }
    invoice_url = scope + "/invoices?" + urllib.parse.urlencode({
        "api-version": BILLING_API_VERSION,
        "periodStartDate": args.invoice_start_date.isoformat(),
        "periodEndDate": args.invoice_end_date.isoformat(),
    })
    return account, usage_urls, invoice_url


def normalize_usage(records: list[dict], schema: str) -> list[dict]:
    return list(iter_normalize_usage(records, schema))


def iter_normalize_usage(records: Iterable[dict], schema: str) -> Iterator[dict]:
    for index, record in enumerate(records, 1):
        kind = record.get("kind")
        if kind != schema:
            raise ValueError(f"Usage row {index}: expected {schema} schema, got {kind!r}")
        props = record.get("properties")
        if not isinstance(props, dict):
            raise ValueError(f"Usage row {index}: missing properties")
        row = dict(props)
        row_id = text(record.get("id") or record.get("name"))
        # Consumption can reuse an ID for distinct charges. Preserve every row.
        row["usageDetailId"] = row_id
        row["invoiceId"] = text(props.get("invoiceId"))
        amount_field = "costInBillingCurrency" if schema == "modern" else "cost"
        row["costInBillingCurrency"] = money(props.get(amount_field), f"Usage row {index}")
        row["billingCurrency"] = currency(
            props.get("billingCurrencyCode") if schema == "modern"
            else props.get("billingCurrency"), f"Usage row {index}",
        )
        row["ResourceId"] = text(props.get("resourceId") or props.get("instanceName"))
        row["subscriptionId"] = text(props.get("subscriptionId") or props.get("subscriptionGuid"))
        if "costInUSD" in props and "costInUsd" not in props:
            row["costInUsd"] = props["costInUSD"]
        row["tags"] = record.get("tags") or props.get("tags") or {}
        details = props.get("meterDetails")
        if isinstance(details, dict):
            for key, value in details.items():
                if row.get(key) in (None, ""):
                    row[key] = value
        yield row


def reconcile(
    actual: Iterable[dict], amortized: Iterable[dict], invoices: list[dict],
    amount_field: str = "subTotal", tolerance: Decimal = Decimal("0.01"),
    *, issue_sink: Callable[[dict], Any] | None = None,
) -> tuple[dict, list[dict]]:
    if amount_field not in INVOICE_FIELDS:
        raise ValueError("Unsupported invoice amount field")
    if not tolerance.is_finite() or tolerance < 0:
        raise ValueError("Tolerance must be finite and nonnegative")
    invoice_map = {}
    for invoice in invoices:
        name = text(invoice.get("name"))
        if not name or name in invoice_map:
            raise ValueError(f"Missing or duplicate invoice name: {name!r}")
        if not isinstance(invoice.get("properties"), dict):
            raise ValueError(f"Invoice {name}: missing properties")
        invoice_map[name] = invoice

    actual_totals: dict[str, Decimal] = defaultdict(Decimal)
    amortized_totals: dict[str, Decimal] = defaultdict(Decimal)
    invoice_totals: dict[str, Decimal] = defaultdict(Decimal)
    grouped: dict[str, dict[str, Decimal]] = {}
    counts: dict[str, int] = defaultdict(int)
    issues = []
    emit_issue = issues.append if issue_sink is None else issue_sink
    actual_count = amortized_count = missing_id_count = issue_count = 0
    for index, row in enumerate(actual, 2):
        actual_count += 1
        code = row["billingCurrency"]
        amount = row["costInBillingCurrency"]
        actual_totals[code] += amount
        invoice_id = row["invoiceId"]
        if invoice_id:
            grouped.setdefault(invoice_id, defaultdict(Decimal))[code] += amount
            counts[invoice_id] += 1
        reason = (
            "missing_invoice_id" if not invoice_id
            else "unknown_invoice_id" if invoice_id not in invoice_map else ""
        )
        if reason:
            issue_count += 1
            missing_id_count += reason == "missing_invoice_id"
            emit_issue({
                "csvRow": index, "usageDetailId": row["usageDetailId"],
                "invoiceId": invoice_id, "reason": reason,
                "billingCurrency": code, "costInBillingCurrency": amount,
            })
    for row in amortized:
        amortized_count += 1
        amortized_totals[row["billingCurrency"]] += row["costInBillingCurrency"]

    comparisons = []
    for name, invoice in invoice_map.items():
        props = invoice["properties"]
        selected = props.get(amount_field)
        if not isinstance(selected, dict):
            raise ValueError(f"Invoice {name}: missing {amount_field}")
        code = currency(selected.get("currency"), f"Invoice {name}")
        expected = money(selected.get("value"), f"Invoice {name} {amount_field}")
        invoice_totals[code] += expected
        amounts = grouped.get(name, {})
        found = counts[name] > 0
        currencies_match = set(amounts) == {code}
        observed = amounts.get(code, Decimal(0))
        delta = observed - expected
        comparisons.append({
            "invoiceId": name, "billingCurrency": code, "invoiceAmount": expected,
            "actualAmount": observed, "difference": delta,
            "actualAmountsByCurrency": dict(amounts), "actualRowCount": counts[name],
            "status": (
                "missing_actual_rows" if not found
                else "currency_mismatch" if not currencies_match
                else "matched" if abs(delta) <= tolerance else "amount_mismatch"
            ),
            "invoiceAmounts": {field: props.get(field) for field in INVOICE_FIELDS},
        })
    missing = [name for name in invoice_map if not counts[name]]
    completeness = bool(actual_count and invoices) and not issue_count and not missing
    totals = [{
        "billingCurrency": code,
        "actualAmount": actual_totals.get(code, Decimal(0)),
        "invoiceAmount": invoice_totals.get(code, Decimal(0)),
        "difference": actual_totals.get(code, Decimal(0)) - invoice_totals.get(code, Decimal(0)),
        "amortizedAmount": amortized_totals.get(code, Decimal(0)),
    } for code in sorted(set(actual_totals) | set(invoice_totals) | set(amortized_totals))]
    totals_match = all(
        abs(actual_totals.get(code, Decimal(0)) - invoice_totals.get(code, Decimal(0))) <= tolerance
        for code in set(actual_totals) | set(invoice_totals)
    )
    amounts_match = bool(actual_count and invoices) and totals_match and all(
        item["status"] == "matched" for item in comparisons
    )
    report = {
        "passed": bool(amortized_count) and completeness and amounts_match,
        "invoiceAmountField": amount_field, "tolerance": tolerance,
        "actualRowCount": actual_count, "amortizedRowCount": amortized_count,
        "invoiceCount": len(invoices), "invoiceIdsComplete": completeness,
        "invoiceAmountsMatch": amounts_match,
        "missingInvoiceIdRows": missing_id_count,
        "unknownInvoiceIds": sorted(set(grouped) - set(invoice_map)),
        "invoicesWithoutActualRows": missing,
        "emptyDatasets": [
            name for name, count in (
                ("actual", actual_count), ("amortized", amortized_count), ("invoices", len(invoices))
            ) if not count
        ],
        "totalsByCurrency": totals, "invoices": comparisons,
    }
    return report, issues


def write_json(path: Path, value: Any) -> None:
    with path.open("w", encoding="utf-8") as target:
        json.dump(value, target, ensure_ascii=False, indent=2, default=str)
        target.write("\n")


def csv_row(row: dict) -> dict:
    return {
        key: json.dumps(value, ensure_ascii=False, default=str)
        if isinstance(value, (dict, list)) else value
        for key, value in row.items()
    }


def write_csv(path: Path, rows: list[dict], fields: tuple[str, ...]) -> None:
    columns = list(fields) + sorted({key for row in rows for key in row} - set(fields))
    with path.open("w", encoding="utf-8-sig", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(csv_row(row))


def export_usage(
    records: Iterable[dict], schema: str, raw_path: Path, csv_path: Path,
) -> Iterator[dict]:
    with raw_path.open("w", encoding="utf-8") as raw:
        raw.write("[")

        def save_records() -> Iterator[dict]:
            separator = ""
            for record in records:
                raw.write(separator + "\n")
                json.dump(record, raw, ensure_ascii=False, indent=2, default=str)
                separator = ","
                yield record

        yield from export_rows(iter_normalize_usage(save_records(), schema), csv_path)
        raw.write("\n]\n")


def export_rows(rows: Iterable[dict], csv_path: Path) -> Iterator[dict]:
    # Later partitions can add columns. Spool cells to disk instead of retaining rows.
    with TemporaryFile(mode="w+", encoding="utf-8", dir=csv_path.parent) as spool:
        columns = set(CSV_FIELDS)
        for row in rows:
            columns.update(row)
            spool.write(json.dumps(csv_row(row), ensure_ascii=False, default=str) + "\n")
            yield row
        spool.seek(0)
        with csv_path.open("w", encoding="utf-8-sig", newline="") as target:
            writer = csv.DictWriter(
                target, fieldnames=list(CSV_FIELDS) + sorted(columns - set(CSV_FIELDS)),
            )
            writer.writeheader()
            for line in spool:
                writer.writerow(json.loads(line))


def report_periods(start: date, end: date) -> Iterator[tuple[date, date]]:
    while start <= end:
        last = min(end, start.replace(day=calendar.monthrange(start.year, start.month)[1]))
        yield start, last
        if last == end:
            break
        start = last + timedelta(days=1)


def generate_cost_report(
    api: AzureAPI, url: str, body: dict, timeout: int,
) -> dict:
    deadline = time.monotonic() + timeout
    status, payload, headers = api.request(url, "POST", body=body, deadline=deadline)
    polling = False
    while status == 202:
        location = headers.get("Location")
        if location:
            url = urllib.parse.urljoin(url, location)
        elif not polling:
            raise ValueError("Cost Details response has no polling Location")
        delay = str(headers.get("Retry-After", "5"))
        if not delay.isdigit():
            raise ValueError("Invalid Cost Details Retry-After (expected seconds)")
        wait = int(delay)
        if time.monotonic() + wait >= deadline:
            raise TimeoutError("Cost Details report was not ready before timeout")
        time.sleep(wait)
        status, payload, headers = api.request(url, deadline=deadline)
        polling = True
    if time.monotonic() >= deadline:
        raise TimeoutError("Cost Details report generation timed out")
    if status == 204:
        return {"status": "NoDataFound"}
    if status != 200:
        raise ValueError(f"Invalid Cost Details response (HTTP {status})")
    if payload.get("status") == "Failed":
        raise ValueError(f"Cost Details report failed: {payload.get('error')}")
    if payload.get("status") not in {"Completed", "NoDataFound"}:
        raise ValueError(f"Unexpected Cost Details status: {payload.get('status')!r}")
    return payload


def cost_report_blobs(result: dict) -> list[dict]:
    if result["status"] == "NoDataFound":
        return []
    manifest = result.get("manifest")
    if not isinstance(manifest, dict):
        raise ValueError("Cost Details report has no manifest")
    if manifest.get("dataFormat") != "Csv" or manifest.get("compressData") is not False:
        raise ValueError("Cost Details report must contain uncompressed CSV files")
    blobs = manifest.get("blobs")
    if not isinstance(blobs, list) or not blobs:
        raise ValueError("Completed Cost Details report has no blobs")
    if type(manifest.get("blobCount")) is not int or manifest["blobCount"] != len(blobs):
        raise ValueError("Cost Details manifest blobCount mismatch")
    links = set()
    total = 0
    for blob in blobs:
        if not isinstance(blob, dict) or not isinstance(blob.get("blobLink"), str):
            raise ValueError("Invalid Cost Details blob")
        link = blob["blobLink"]
        parsed = urllib.parse.urlsplit(link)
        if (
            parsed.scheme != "https" or not parsed.netloc.endswith(".blob.core.windows.net")
            or parsed.username or parsed.password or parsed.fragment
        ):
            raise ValueError("Cost Details blob must use an Azure public Blob HTTPS URL")
        if link in links:
            raise ValueError("Cost Details manifest contains duplicate blob links")
        links.add(link)
        size = blob.get("byteCount")
        if type(size) is not int or size < 0:
            raise ValueError("Invalid Cost Details blob byteCount")
        total += size
    if type(manifest.get("byteCount")) is not int or manifest["byteCount"] != total:
        raise ValueError("Cost Details manifest byteCount mismatch")
    return blobs


def download_cost_blob(url: str, output: Path, expected_bytes: int) -> None:
    partial = output.with_suffix(".part")
    try:
        # Signed Blob URLs must never receive an ARM token or follow a redirect.
        opener = urllib.request.build_opener(NoRedirect())
        with opener.open(url, timeout=120) as response, partial.open("wb") as target:
            if response.status != 200:
                raise ValueError(f"Invalid Cost Details blob response (HTTP {response.status})")
            count = 0
            while chunk := response.read(1024 * 1024):
                count += len(chunk)
                if count > expected_bytes:
                    raise ValueError("Cost Details blob exceeds manifest byteCount")
                target.write(chunk)
            if count != expected_bytes:
                raise ValueError("Incomplete Cost Details blob: byteCount mismatch")
        partial.replace(output)
    finally:
        partial.unlink(missing_ok=True)


def read_cost_csv(path: Path) -> Iterator[dict]:
    aliases = {
        name: (name,) for name in CSV_FIELDS + (
            "costInUsd", "meterName", "meterSubCategory", "meterRegion", "resourceLocation",
            "location", "additionalInfo", "reservationName", "subscriptionName",
            "unitOfMeasure", "effectivePrice", "unitPrice", "payGPrice",
        )
    }
    aliases.update({
        "costInBillingCurrency": ("costInBillingCurrency", "cost"),
        "billingCurrency": ("billingCurrency", "billingCurrencyCode", "currency"),
        "ResourceId": ("ResourceId", "instanceId", "instanceName"),
        "subscriptionId": ("subscriptionId", "subscriptionGuid"),
        "resourceGroupName": ("resourceGroupName", "resourceGroup"),
        "date": ("date", "usageDateTime"),
    })
    with path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source, strict=True)
        fields = reader.fieldnames or []
        headers = {field.replace(" ", "").lower(): field for field in fields}
        if not fields or "" in headers or len(headers) != len(fields):
            raise ValueError(f"{path.name}: missing or duplicate CSV headers")
        for required in ("costInBillingCurrency", "billingCurrency"):
            if not any(alias.lower() in headers for alias in aliases[required]):
                raise ValueError(f"{path.name}: missing {required} CSV column")
        for index, record in enumerate(reader, 2):
            label = f"{path.parent.parent.name}/{path.parent.name}/{path.name}:{index}"
            if None in record or any(value is None for value in record.values()):
                raise ValueError(f"{label}: CSV row width does not match header")
            row = dict(record)
            for name, options in aliases.items():
                row[name] = next(
                    (record[headers[option.lower()]] for option in options
                     if option.lower() in headers), "",
                )
            row["usageDetailId"] = text(row["usageDetailId"]) or label
            row["invoiceId"] = text(row["invoiceId"])
            row["costInBillingCurrency"] = money(row["costInBillingCurrency"], label)
            row["billingCurrency"] = currency(row["billingCurrency"], label)
            tags = text(row["tags"])
            if tags.startswith('"'):
                tags = "{" + tags + "}"
            row["tags"] = json.loads(tags) if tags else {}
            if not isinstance(row["tags"], dict):
                raise ValueError(f"{label}: tags must be a JSON object")
            yield row


def iter_cost_details(
    args: argparse.Namespace, api: AzureAPI, url: str, metric: str,
) -> Iterator[dict]:
    for start, end in report_periods(args.start_date, args.end_date):
        body = {"metric": metric, "timePeriod": {"start": str(start), "end": str(end)}}
        result = generate_cost_report(api, url, body, args.cost_details_timeout)
        blobs = cost_report_blobs(result)
        folder = args.output_dir / "cost-details" / metric / f"{start}_{end}"
        folder.mkdir(parents=True)
        # Persist provenance without persisting the SAS credentials in blobLink.
        manifest = dict(result.get("manifest") or {})
        manifest["blobs"] = [
            {"file": f"part-{index:04d}.csv", "byteCount": blob["byteCount"]}
            for index, blob in enumerate(blobs, 1)
        ]
        write_json(folder / "manifest.json", {
            "status": result["status"], "validTill": result.get("validTill"),
            "request": body, "manifest": manifest,
        })
        for index, blob in enumerate(blobs, 1):
            path = folder / f"part-{index:04d}.csv"
            download_cost_blob(blob["blobLink"], path, blob["byteCount"])
            yield from read_cost_csv(path)


def download_invoice(api: AzureAPI, account_url: str, name: str, output: Path, timeout: int) -> Path:
    url = (
        account_url + "/invoices/" + urllib.parse.quote(name, safe="")
        + "/download?api-version=" + BILLING_API_VERSION
    )
    deadline = time.monotonic() + timeout
    status, payload, headers = api.request(url, "POST")
    while status == 202:
        location = headers.get("Location")
        if not location:
            raise ValueError("Invoice download response has no Location header")
        remaining = deadline - time.monotonic()
        delay = headers.get("Retry-After", "5")
        wait = int(delay) if delay.isdigit() else 5
        if remaining <= wait:
            raise TimeoutError("Invoice download URL was not ready before timeout")
        time.sleep(wait)
        url = urllib.parse.urljoin(url, location)
        status, payload, headers = api.request(url)
    download_url = payload.get("url")
    if status != 200 or not isinstance(download_url, str):
        raise ValueError(f"Invalid invoice download response (HTTP {status})")
    if urllib.parse.urlsplit(download_url).scheme != "https":
        raise ValueError("Invoice document URL must use HTTPS")
    partial = output.with_suffix(".part")
    try:
        # The document URL is signed; never send the ARM bearer token to it.
        with urllib.request.urlopen(download_url, timeout=120) as response:
            first_chunk = response.read(1024 * 1024)
            if first_chunk.startswith(b"%PDF-"):
                output = output.with_suffix(".pdf")
            elif first_chunk.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")):
                output = output.with_suffix(".zip")
            else:
                raise ValueError("Invoice download is neither a PDF nor a ZIP document")
            with partial.open("wb") as target:
                target.write(first_chunk)
                while chunk := response.read(1024 * 1024):
                    target.write(chunk)
        partial.replace(output)
        return output
    finally:
        partial.unlink(missing_ok=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--billing-account-name", required=True)
    parser.add_argument("--billing-profile-name", help="MCA/MPA billing profile name")
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    parser.add_argument("--invoice-start-date", type=date.fromisoformat)
    parser.add_argument("--invoice-end-date", type=date.fromisoformat)
    parser.add_argument("--usage-schema", choices=("modern", "legacy"), default="modern")
    parser.add_argument(
        "--usage-source", choices=("cost-details", "consumption"), default="cost-details",
        help="Cost Details reports by default; Consumption only for explicit compatibility",
    )
    parser.add_argument("--cost-details-timeout", type=int, default=1800,
                        help="Report generation/polling timeout in seconds per metric/month")
    parser.add_argument("--invoice-amount-field", choices=("subTotal", "totalAmount", "billedAmount"), default="subTotal")
    parser.add_argument("--tolerance", default="0.01")
    parser.add_argument("--output-dir", type=Path, default=Path("billing-download"))
    parser.add_argument("--download-invoices", action="store_true", help="Also download invoice documents")
    parser.add_argument("--invoice-download-timeout", type=int, default=600)
    args = parser.parse_args(argv)
    args.invoice_start_date = args.invoice_start_date or args.start_date
    args.invoice_end_date = args.invoice_end_date or args.end_date
    if args.start_date > args.end_date or args.invoice_start_date > args.invoice_end_date:
        parser.error("Start dates must not be after end dates")
    if args.invoice_download_timeout <= 0:
        parser.error("--invoice-download-timeout must be positive")
    if args.cost_details_timeout <= 0:
        parser.error("--cost-details-timeout must be positive")
    if args.usage_source != "consumption" and args.usage_schema != "modern":
        parser.error("--usage-schema legacy requires --usage-source consumption")
    try:
        args.tolerance = money(args.tolerance, "--tolerance")
    except ValueError as exc:
        parser.error(str(exc))
    if args.tolerance < 0:
        parser.error("--tolerance must be nonnegative")
    for field in ("billing_account_name", "billing_profile_name"):
        value = getattr(args, field)
        if value is not None and (not value.strip() or "/" in value or value in {".", ".."}):
            parser.error(f"--{field.replace('_', '-')} must be a resource name, not a resource ID")
    return args


def run(args: argparse.Namespace, api: AzureAPI) -> dict:
    root = args.output_dir
    root.mkdir(parents=True, exist_ok=False)
    account_url, usage_urls, invoice_url = build_urls(args)
    write_json(root / "request.json", {
        "usageSource": args.usage_source, "usageUrls": usage_urls, "invoiceUrl": invoice_url,
        "usageSchema": args.usage_schema, "invoiceAmountField": args.invoice_amount_field,
        "tolerance": args.tolerance,
        "startDate": str(args.start_date), "endDate": str(args.end_date),
    })
    invoices = api.list(invoice_url)
    write_json(root / "invoices.json", invoices)
    datasets = {}
    for metric, prefix in (("ActualCost", "Actual"), ("AmortizedCost", "Amortized")):
        folder = root / prefix.lower()
        folder.mkdir()
        csv_path = folder / f"{prefix}_{args.start_date}_{args.end_date}.csv"
        if args.usage_source == "cost-details":
            datasets[metric] = export_rows(
                iter_cost_details(args, api, usage_urls[metric], metric), csv_path,
            )
        else:
            datasets[metric] = export_usage(
                api.iter_list(usage_urls[metric]), args.usage_schema, root / f"{prefix}.json",
                csv_path,
            )
    try:
        with (root / "invoice-id-issues.csv").open("w", encoding="utf-8-sig", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=ISSUE_FIELDS)
            writer.writeheader()
            report, _ = reconcile(
                datasets["ActualCost"], datasets["AmortizedCost"], invoices,
                args.invoice_amount_field, args.tolerance, issue_sink=writer.writerow,
            )
    finally:
        for dataset in datasets.values():
            dataset.close()
    write_csv(root / "invoice-reconciliation.csv", report["invoices"], (
        "invoiceId", "billingCurrency", "invoiceAmount", "actualAmount", "difference", "status",
    ))
    if args.download_invoices:
        folder = root / "invoice-documents"
        folder.mkdir()
        documents = []
        for index, invoice in enumerate(invoices, 1):
            filename = f"invoice-{index:04d}.pdf"
            downloaded = download_invoice(
                api, account_url, invoice["name"], folder / filename, args.invoice_download_timeout,
            )
            documents.append({"invoiceId": invoice["name"], "file": downloaded.name})
        write_json(folder / "index.json", documents)
    write_json(root / "validation-summary.json", report)
    return report


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    from azure.identity import AzureCliCredential

    report = run(args, AzureAPI(AzureCliCredential()))
    print(json.dumps(report, indent=2, default=str))
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    sys.exit(main())
