#!/usr/bin/env python3
"""Download the current Azure Price Sheet by billing profile."""

from __future__ import annotations

import argparse
from pathlib import Path

from price_sheet_download import (
    azure_cli_credential,
    download_file,
    extract_download_url,
    positive_timeout,
    request_by_billing_profile,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download the current Azure Price Sheet by billing profile."
    )
    parser.add_argument("billing_account", help="Full MCA billing account name")
    parser.add_argument("billing_profile", help="MCA billing profile name")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("pricesheet.zip"),
        help="Local output path (default: pricesheet.zip)",
    )
    parser.add_argument(
        "--timeout",
        type=positive_timeout,
        default=1800,
        help="Generation timeout in seconds (default: 1800)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    credential = azure_cli_credential()
    result = request_by_billing_profile(
        args.billing_account,
        args.billing_profile,
        credential,
        args.timeout,
    )
    download_file(extract_download_url(result), args.output)
    print(f"Downloaded Price Sheet to {args.output.resolve()}")


if __name__ == "__main__":
    main()
