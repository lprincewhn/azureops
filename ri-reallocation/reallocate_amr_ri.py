#!/usr/bin/env python3
"""Reallocate Azure Managed Redis RI benefit to bound projects."""

from ri_reallocation_core import AMR, main


if __name__ == "__main__":
    main(AMR)
