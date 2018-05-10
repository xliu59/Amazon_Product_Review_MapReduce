#!/usr/bin/env python3

import sys

prev_product = 0
prev_review = 0
max_helpful_rate = 0
helpful_count = 0
best_reviews = []

for line in sys.stdin:
    line = line.strip()
    key, helpful_votes, total_votes = line.split('\t')
    helpful_votes, total_votes = int(helpful_votes), int(total_votes)
    product_id, review_id = key.split(',')

    if product_id != prev_product:
        # different product, then review_id must also be different
        if prev_product != 0 and max_helpful_rate > 0:
            print("%s is(are) the most helpful review for %s, with rate %.3f, helpful vote number %d" %
                  (", ".join(best_reviews), prev_product, max_helpful_rate, helpful_count))
        # update stat info
        prev_product = product_id
        prev_review = review_id
        max_helpful_rate = 0 if total_votes == 0 else helpful_votes / total_votes
        helpful_count = helpful_votes
        best_reviews = [prev_review]

    else:
        if prev_review != review_id:
            # same product, different reviews
            helpful_rate = 0 if total_votes == 0 else helpful_votes / total_votes
            if helpful_rate > max_helpful_rate:
                max_helpful_rate = helpful_rate
                helpful_count = helpful_votes
                best_reviews = [review_id]
            elif helpful_rate > 0 and helpful_rate == max_helpful_rate:
                if helpful_votes > helpful_count:
                    best_reviews = [review_id]
                    helpful_count = helpful_votes
                elif helpful_votes == helpful_count:
                    best_reviews.append(review_id)
                else:
                    continue
            prev_review = review_id
        else:
            # same product, same review id
            raise Exception("different reviews but sharing same review id")

if max_helpful_rate > 0:
    print("%s is(are) the most helpful review for %s, with rate %.3f, helpful vote number %d" %
        (", ".join(best_reviews), prev_product, max_helpful_rate, helpful_count))