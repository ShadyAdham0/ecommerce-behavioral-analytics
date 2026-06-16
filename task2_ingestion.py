import argparse
import glob
import os
import sys

import pandas as pd
from pymongo import MongoClient, ASCENDING


def pick_csv(glob_pattern: str) -> str:
    files = sorted(glob.glob(glob_pattern))
    if not files:
        raise FileNotFoundError(f"No CSV match: {glob_pattern}")
    return files[0]


def load_dataframe(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def ingest_co_occurrence(coll, df: pd.DataFrame):
    required = {"item_a", "item_b", "frequency"}
    if not required.issubset(df.columns):
        raise ValueError(f"co_occurrence needs columns {required}, got {list(df.columns)}")
    records = df.to_dict("records")
    coll.delete_many({})
    if records:
        coll.insert_many(records)
    coll.create_index([("item_a", ASCENDING), ("item_b", ASCENDING)])
    coll.create_index([("item_b", ASCENDING), ("item_a", ASCENDING)])
    coll.create_index("frequency")


def ingest_user_affinity(coll, df: pd.DataFrame):
    required = {"user_id", "category", "weighted_score", "rank"}
    if not required.issubset(df.columns):
        raise ValueError(f"user_affinity needs columns {required}, got {list(df.columns)}")
    records = df.to_dict("records")
    coll.delete_many({})
    if records:
        coll.insert_many(records)
    coll.create_index([("user_id", ASCENDING), ("rank", ASCENDING)])
    coll.create_index("category")


def parse_args():
    root = os.path.dirname(os.path.abspath(__file__))
    default_out = os.path.join(root, "data", "output")
    p = argparse.ArgumentParser(description="Ingest Phase 1 CSVs into MongoDB")
    p.add_argument("--mongo-uri", default=os.environ.get("MONGO_URI", "mongodb://localhost:27017/"))
    p.add_argument("--db", default=os.environ.get("MONGO_DB", "ecommerce_analytics"))
    p.add_argument(
        "--pairs-glob",
        default=os.path.join(default_out, "co_occurrence_pairs", "part*.csv"),
        help="Glob for Task 1.1 co-occurrence CSV part file",
    )
    p.add_argument(
        "--affinity-glob",
        default=os.path.join(default_out, "user_affinity_aggregation", "part*.csv"),
        help="Glob for Task 1.2 user affinity CSV part file",
    )
    return p.parse_args()


def main():
    args = parse_args()
    pairs_path = pick_csv(args.pairs_glob)
    aff_path = pick_csv(args.affinity_glob)
    print("Co-occurrence CSV:", pairs_path)
    print("User affinity CSV:", aff_path)

    pairs_df = load_dataframe(pairs_path)
    aff_df = load_dataframe(aff_path)

    client = MongoClient(args.mongo_uri)
    db = client[args.db]
    ingest_co_occurrence(db["co_occurrence"], pairs_df)
    ingest_user_affinity(db["user_affinity"], aff_df)

    print(f"Done. Database={args.db}, counts: co_occurrence={db['co_occurrence'].estimated_document_count()}, "
          f"user_affinity={db['user_affinity'].estimated_document_count()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
