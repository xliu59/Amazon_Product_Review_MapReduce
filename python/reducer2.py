#!/usr/bin/env python3

import sys


prev_category = 0
total_reviews = 0

for line in sys.stdin:
    line = line.strip()
    category, count = line.split('\t')

    if prev_category != category:
        if prev_category != 0:
            print("%s has %d reviews" % (prev_category, total_reviews))
        prev_category = category
        total_reviews = 0

    total_reviews += int(count)

print("%s has %d reviews" % (prev_category, total_reviews))