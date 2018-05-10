#!/usr/bin/env python3
import sys
import re

header = "^marketplace.*"
for line in sys.stdin:
    if re.match(header, line):
        continue
    line = line.strip()
    fields = line.split("\t")
    field_num = len(fields)
    if field_num != 15:
        raise Exception("line format wrong! " + line)

    marketplace, customer_id, review_id, product_id, \
    product_parent, product_title, product_category, \
    star_rating, helpful_votes, total_votes, vine, \
    verified_purchase, review_headline, review_body, review_date = fields

    print("%s\t%d" % (review_date, 1))