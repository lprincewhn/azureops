#!/usr/bin/env python3
"""Reallocate Azure Database for MySQL RI benefit to bound projects."""

from reallocate_vm_ri import MYSQL, main


if __name__ == "__main__":
    main(MYSQL)
