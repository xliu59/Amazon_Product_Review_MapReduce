#!/usr/bin/env python3

import sys


prev_date = 0
total_reviews = 0

for line in sys.stdin:
    line = line.strip()
    review_date, count = line.split('\t')

    if prev_date != review_date:
        if prev_date != 0:
            print("on %s received %d reviews" % (prev_date, total_reviews))
        prev_date = review_date
        total_reviews = 0

    total_reviews += int(count)

print("on %s received %d reviews" % (prev_date, total_reviews))