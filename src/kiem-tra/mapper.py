#!/usr/bin/env python3
"""Mapper Hadoop Streaming: doanh thu theo danh mục từ retail_transactions.csv."""
import sys
for line in sys.stdin:
    p = line.rstrip("\n").split(",")
    if len(p) != 10 or p[0] == "transaction_id":
        continue
    try:
        rev = int(p[4]) * float(p[5]) * (1 - float(p[6]))
    except ValueError:
        continue
    print(f"{p[3]}\t{rev:.0f}")
