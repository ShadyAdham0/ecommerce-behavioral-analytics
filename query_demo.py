import argparse
import os
import sys

from pymongo import ASCENDING, DESCENDING, MongoClient


def parse_args():
    p = argparse.ArgumentParser(description="Query recommendations (user + item context)")
    p.add_argument("--user-id", required=True, help="User identifier as stored in logs / Mongo")
    p.add_argument("--item-id", required=True, help="Product / item identifier")
    p.add_argument("--mongo-uri", default=os.environ.get("MONGO_URI", "mongodb://localhost:27017/"))
    p.add_argument("--mongo-db", default=os.environ.get("MONGO_DB", "ecommerce_analytics"))
    p.add_argument("--top-user-categories", type=int, default=5)
    p.add_argument("--top-co-items", type=int, default=10)
    return p.parse_args()


def main():
    args = parse_args()
    client = MongoClient(args.mongo_uri)
    db = client[args.mongo_db]
    ua = db["user_affinity"]
    co = db["co_occurrence"]

    uid = args.user_id
    iid = args.item_id
    try:
        uid_alt = int(str(uid))
    except ValueError:
        uid_alt = uid

    user_q = {"$or": [{"user_id": uid}, {"user_id": uid_alt}]}
    print(f"\n=== User affinities for user_id={uid!r} (top {args.top_user_categories}) ===\n")
    cur = (
        ua.find(user_q, {"_id": 0})
        .sort([("rank", DESCENDING), ])
        .limit(args.top_user_categories)
    )
    rows = list(cur)
    if not rows:
        print("(No user_affinity rows - run Phase 1 Task 1.2 and task2_ingestion.py.)")
    else:
        for r in rows:
            print(
                f"  rank {int(r.get('rank', 0)):>3}  "
                f"category={r.get('category')!r}  score={r.get('weighted_score')}"
            )

    print(f"\n=== Co-purchase recommendations for item_id={iid!r} (top {args.top_co_items}) ===\n")
    try:
        iid_alt = int(str(iid))
    except ValueError:
        iid_alt = iid

    pair_q = {
        "$or": [
            {"item_a": iid},
            {"item_a": iid_alt},
            {"item_b": iid},
            {"item_b": iid_alt},
        ]
    }
    recs = []
    for doc in co.find(pair_q, {"_id": 0}):
        a, b = doc.get("item_a"), doc.get("item_b")
        other = b if str(a) == str(iid) or str(a) == str(iid_alt) else a
        freq = int(doc.get("frequency", 0))
        recs.append((other, freq))

    recs.sort(key=lambda x: -x[1])
    seen = set()
    n = 0
    for other, freq in recs:
        key = str(other)
        if key in seen:
            continue
        seen.add(key)
        print(f"  item={other!r}  co-occurrence_count={freq}")
        n += 1
        if n >= args.top_co_items:
            break

    if n == 0:
        print("(No co_occurrence rows for this item - run Task 1.1 and task2_ingestion.py.)")

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
