#!/usr/bin/env python3
"""Download Consumption cost details and reconcile ActualCost with invoices."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any


MANAGEMENT_URL = "https://management.azure.com"
getcontext().prec = 40
USAGE_API_VERSION = "2024-08-01"
BILLING_API_VERSION = "2024-04-01"
INVOICE_FIELDS = ("subTotal", "taxAmount", "totalAmount", "billedAmount", "amountDue")
CSV_FIELDS = (
    "usageDetailId", "invoiceId", "costInBillingCurrency", "billingCurrency",
    "ResourceId", "subscriptionId", "date", "meterId", "meterCategory",
    "reservationId", "benefitId", "pricingModel", "chargeType", "quantity", "tags",
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
        raise ValueError("Unexpected redirect from Azure management API")


class AzureAPI:
    def __init__(self, credential: Any):
        self.credential = credential
        self.opener = urllib.request.build_opener(NoRedirect())

    def request(self, url: str, method: str = "GET") -> tuple[int, dict, Any]:
        parsed = urllib.parse.urlsplit(url)
        if (
            parsed.scheme != "https" or parsed.netloc != "management.azure.com"
            or parsed.fragment
        ):
            raise ValueError("Refusing a non-Azure-management API URL")
        for attempt in range(5):
            token = self.credential.get_token(MANAGEMENT_URL + "/.default").token
            request = urllib.request.Request(
                url, data=b"" if method == "POST" else None, method=method,
                headers={"Authorization": "Bearer " + token, "Accept": "application/json"},
            )
            try:
                with self.opener.open(request, timeout=120) as response:
                    body = response.read()
                    payload = json.loads(body, parse_float=Decimal) if body else {}
                    if not isinstance(payload, dict):
                        raise ValueError("Azure API response must be a JSON object")
                    return response.status, payload, response.headers
            except urllib.error.HTTPError as exc:
                if exc.code not in {429, 500, 502, 503, 504} or attempt == 4:
                    raise
                delay = exc.headers.get("Retry-After", "")
                time.sleep(min(int(delay), 120) if delay.isdigit() else 2 ** attempt)
        raise AssertionError("Unreachable retry state")

    def list(self, url: str) -> list[dict]:
        records: list[dict] = []
        seen: set[str] = set()
        while url:
            if url in seen:
                raise ValueError("Azure pagination contains a repeated nextLink")
            seen.add(url)
            status, payload, _ = self.request(url)
            if status == 204:
                return records
            if status != 200 or not isinstance(payload.get("value"), list):
                raise ValueError(f"Invalid Azure list response (HTTP {status})")
            if not all(isinstance(item, dict) for item in payload["value"]):
                raise ValueError("Azure list contains a non-object record")
            records.extend(payload["value"])
            next_link = payload.get("nextLink")
            if next_link is not None and not isinstance(next_link, str):
                raise ValueError("Invalid Azure nextLink")
            url = urllib.parse.urljoin(url, next_link) if next_link else ""
        return records


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
    invoice_url = scope + "/invoices?" + urllib.parse.urlencode({
        "api-version": BILLING_API_VERSION,
        "periodStartDate": args.invoice_start_date.isoformat(),
        "periodEndDate": args.invoice_end_date.isoformat(),
    })
    return account, usage_urls, invoice_url


def normalize_usage(records: list[dict], schema: str) -> list[dict]:
    rows = []
    seen: set[str] = set()
    for index, record in enumerate(records, 1):
        kind = record.get("kind")
        if kind != schema:
            raise ValueError(f"Usage row {index}: expected {schema} schema, got {kind!r}")
        props = record.get("properties")
        if not isinstance(props, dict):
            raise ValueError(f"Usage row {index}: missing properties")
        row = dict(props)
        row_id = text(record.get("id") or record.get("name"))
        if row_id and row_id in seen:
            raise ValueError(f"Duplicate usage detail ID: {row_id}")
        if row_id:
            seen.add(row_id)
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
        rows.append(row)
    return rows


def reconcile(
    actual: list[dict], amortized: list[dict], invoices: list[dict],
    amount_field: str = "subTotal", tolerance: Decimal = Decimal("0.01"),
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
    for index, row in enumerate(actual, 2):
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
            issues.append({
                "csvRow": index, "usageDetailId": row["usageDetailId"],
                "invoiceId": invoice_id, "reason": reason,
                "billingCurrency": code, "costInBillingCurrency": amount,
            })
    for row in amortized:
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
    completeness = bool(actual and invoices) and not issues and not missing
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
    amounts_match = bool(actual and invoices) and totals_match and all(
        item["status"] == "matched" for item in comparisons
    )
    report = {
        "passed": bool(amortized) and completeness and amounts_match,
        "invoiceAmountField": amount_field, "tolerance": tolerance,
        "actualRowCount": len(actual), "amortizedRowCount": len(amortized),
        "invoiceCount": len(invoices), "invoiceIdsComplete": completeness,
        "invoiceAmountsMatch": amounts_match,
        "missingInvoiceIdRows": sum(item["reason"] == "missing_invoice_id" for item in issues),
        "unknownInvoiceIds": sorted(set(grouped) - set(invoice_map)),
        "invoicesWithoutActualRows": missing,
        "emptyDatasets": [
            name for name, rows in (
                ("actual", actual), ("amortized", amortized), ("invoices", invoices)
            ) if not rows
        ],
        "totalsByCurrency": totals, "invoices": comparisons,
    }
    return report, issues


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict], fields: tuple[str, ...]) -> None:
    columns = list(fields) + sorted({key for row in rows for key in row} - set(fields))
    with path.open("w", encoding="utf-8-sig", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False, default=str)
                if isinstance(value, (dict, list)) else value
                for key, value in row.items()
            })


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
        "usageUrls": usage_urls, "invoiceUrl": invoice_url,
        "usageSchema": args.usage_schema, "invoiceAmountField": args.invoice_amount_field,
        "tolerance": args.tolerance,
    })
    datasets = {}
    for metric, prefix in (("ActualCost", "Actual"), ("AmortizedCost", "Amortized")):
        records = api.list(usage_urls[metric])
        write_json(root / f"{prefix}.json", records)
        rows = normalize_usage(records, args.usage_schema)
        folder = root / prefix.lower()
        folder.mkdir()
        write_csv(folder / f"{prefix}_{args.start_date}_{args.end_date}.csv", rows, CSV_FIELDS)
        datasets[metric] = rows
    invoices = api.list(invoice_url)
    write_json(root / "invoices.json", invoices)
    report, issues = reconcile(
        datasets["ActualCost"], datasets["AmortizedCost"], invoices,
        args.invoice_amount_field, args.tolerance,
    )
    write_csv(root / "invoice-id-issues.csv", issues, (
        "csvRow", "usageDetailId", "invoiceId", "reason",
        "billingCurrency", "costInBillingCurrency",
    ))
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
