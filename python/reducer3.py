#!/usr/bin/env python3

import sys

prev_category = 0
prev_product = 0
total_product = 0

for line in sys.stdin:
    line = line.strip()
    key, product = line.split('\t')
    category, product = key.split(',')

    if category != prev_category:
        # different category, then product must also be different
        if prev_category != 0:
            print("%s has %d different products" % (prev_category, total_product))
        prev_category = category
        total_product = 1
    else:
        if product != prev_product:
            # same category, different product
            total_product += 1
            prev_product = product
        else:
            # same category, same product
            continue

print("%s has %d different products" % (prev_category, total_product))