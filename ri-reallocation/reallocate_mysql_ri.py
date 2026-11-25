#!/usr/bin/env python3
"""Reallocate Azure Database for MySQL RI benefit to bound projects."""

from ri_reallocation_core import MYSQL, main


if __name__ == "__main__":
    main(MYSQL)
