#!/usr/bin/env python3
"""Reducer Hadoop Streaming: cộng dồn doanh thu theo khoá (đầu vào đã sắp xếp theo khoá)."""
import sys
cur, total = None, 0.0
for line in sys.stdin:
    k, v = line.rstrip("\n").split("\t", 1)
    if k != cur:
        if cur is not None:
            print(f"{cur}\t{total:.0f}")
        cur, total = k, 0.0
    total += float(v)
if cur is not None:
    print(f"{cur}\t{total:.0f}")
