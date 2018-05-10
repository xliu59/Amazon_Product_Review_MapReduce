#!/usr/bin/env python3

import sys


prev_id = 0
total_stars = 0
total_reviews = 0
stared_reviews = 0
unstared_reviews = 0

for line in sys.stdin:
    line = line.strip()
    prod_id, stars = line.split('\t')

    if prev_id != prod_id:
        if prev_id != 0:
            print("%s has %d stars \t in %d total review \t avg=%.1f \t (%d stared \t %d unstared)" %
                  (prev_id, total_stars, total_reviews, (total_stars / total_reviews), stared_reviews, unstared_reviews))
        prev_id = prod_id
        total_stars = 0
        total_reviews = 0
        stared_reviews = 0
        unstared_reviews = 0

    total_reviews += 1
    if int(stars) == 0:
        unstared_reviews += 1
    else:
        stared_reviews += 1
        total_stars += int(stars)

print("%s has %d stars \t in %d total review \t avg=%.1f \t (%d stared \t %d unstared)" %
      (prev_id, total_stars, total_reviews, (total_stars/total_reviews), stared_reviews, unstared_reviews))