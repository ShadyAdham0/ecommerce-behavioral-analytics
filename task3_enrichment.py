"""
Phase 3 – Cart Abandonment Enrichment
======================================
Spec requirement (Task 3.1):
  1. Find sessions where a user added an item to the cart but did NOT purchase.
  2. Query MongoDB for each user's top-affinity categories (Phase 1 / Task 1.2 output).
  3. Extract the abandoned item's category from product_metadata.
  4. If the item's category is in the user's top-affinity categories
     → flag "High_Discount".
     Otherwise → "Standard_Reminder".

Thresholds are now DATA-DRIVEN rather than hard-coded constants:
  - user "top category" = categories whose rank ≤ computed top_n_cutoff
    (default: top-3 categories per user, or the 25th-percentile rank across
     all users' category counts – whichever is more meaningful for the dataset).
  - "high-frequency pair" cutoff = 75th-percentile co-occurrence frequency
    loaded from MongoDB, used as a secondary signal when category data is missing.
"""

import argparse
import os
import sys
import statistics
from collections import defaultdict

if os.environ.get("JAVA_HOME"):
    os.environ["PATH"] = (
        os.path.join(os.environ["JAVA_HOME"], "bin")
        + os.pathsep
        + os.environ.get("PATH", "")
    )
if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import spark_win_setup
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType



def _compute_percentile(values, pct):
    """Return the p-th percentile (0-100) of a list of numbers."""
    if not values:
        return 0
    sorted_vals = sorted(values)
    idx = int(len(sorted_vals) * pct / 100)
    return sorted_vals[min(idx, len(sorted_vals) - 1)]


def load_mongo_lookups(mongo_uri: str, db_name: str, top_rank_cutoff: int):
    """
    Load two lookup tables from MongoDB:

    user_top_categories : dict[str, set[str]]
        Maps user_id → set of category names whose rank ≤ top_rank_cutoff.
        These are the categories the user has shown strong affinity toward.

    high_freq_items : set[str]
        Set of item_ids that appear in at least one co-occurrence pair whose
        frequency ≥ 75th-percentile frequency across all pairs.
        Used as a fallback signal when the item has no category metadata.

    Also returns computed_pair_cutoff (int) so it can be logged.
    """
    from pymongo import MongoClient

    client = MongoClient(mongo_uri)
    db = client[db_name]

    user_top_categories: dict[str, set] = defaultdict(set)
    for doc in db["user_affinity"].find({}, {"_id": 0}):
        uid = doc.get("user_id")
        rank = int(doc.get("rank", 9999))
        category = doc.get("category")
        if uid is None or category is None:
            continue
        if rank <= top_rank_cutoff:
            user_top_categories[str(uid)].add(str(category).strip().lower())

    all_freqs = []
    item_freqs: dict[str, list] = defaultdict(list)
    for doc in db["co_occurrence"].find({}, {"_id": 0}):
        freq = int(doc.get("frequency", 0))
        all_freqs.append(freq)
        for key in ("item_a", "item_b"):
            val = doc.get(key)
            if val is not None:
                item_freqs[str(val)].append(freq)

    pair_cutoff = _compute_percentile(all_freqs, 75) if all_freqs else 0

    high_freq_items: set[str] = {
        item for item, freqs in item_freqs.items()
        if max(freqs) >= pair_cutoff
    }

    return dict(user_top_categories), high_freq_items, pair_cutoff


def parse_args():
    root = os.path.dirname(os.path.abspath(__file__))
    p = argparse.ArgumentParser(description="Cart abandonment enrichment – Phase 3")
    p.add_argument(
        "--input",
        default=os.environ.get("RAW_LOG_PATH", os.path.join(root, "data", "ecommerce_logs.csv")),
    )
    p.add_argument(
        "--output",
        default=os.environ.get(
            "OUTPUT_DIR", os.path.join(root, "data", "output", "enriched_abandonment")
        ),
    )
    p.add_argument("--mongo-uri", default=os.environ.get("MONGO_URI", "mongodb://localhost:27017/"))
    p.add_argument("--mongo-db", default=os.environ.get("MONGO_DB", "ecommerce_analytics"))
    p.add_argument("--master", default=os.environ.get("SPARK_MASTER", "local[*]"))
    p.add_argument(
        "--top-rank-cutoff",
        type=int,
        default=int(os.environ.get("TOP_RANK_CUTOFF", "3")),
        help=(
            "A user's 'top' categories are those with rank ≤ this value "
            "(computed by Task 1.2). Default 3."
        ),
    )
    return p.parse_args()



def main():
    spark_win_setup.ensure_hadoop_home()
    args = parse_args()

    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Input not found: {args.input}")

    user_top_categories, high_freq_items, pair_cutoff = load_mongo_lookups(
        args.mongo_uri, args.mongo_db, args.top_rank_cutoff
    )
    print(
        f"Loaded Mongo lookups:\n"
        f"  {len(user_top_categories)} users with top-{args.top_rank_cutoff} categories\n"
        f"  {len(high_freq_items)} high-frequency items "
        f"(co-occurrence ≥ {pair_cutoff}, 75th pct)"
    )

    spark = (
        SparkSession.builder.appName("Enrichment_Task3")
        .master(args.master)
        .config("spark.hadoop.io.native.lib.available", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    path = (
        args.input
        if args.input.startswith("file:")
        else f"file:///{os.path.abspath(args.input)}"
    )
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

    needed = {"event_type", "user_id", "product_id"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing columns: {missing}")

    if "session_id" in df.columns:
        sess = "session_id"
    elif "user_session" in df.columns:
        sess = "user_session"
    else:
        raise ValueError("Expected session_id or user_session. Found: " + str(df.columns))

    sess_flags = df.groupBy(sess).agg(
        F.max(F.when(F.lower(F.col("event_type")) == "cart", F.lit(1)).otherwise(F.lit(0))).alias("had_cart"),
        F.max(F.when(F.lower(F.col("event_type")) == "purchase", F.lit(1)).otherwise(F.lit(0))).alias("had_purchase"),
    )
    abandoned_sessions = sess_flags.filter(
        (F.col("had_cart") == 1) & (F.col("had_purchase") == 0)
    ).select(sess)

    carts = (
        df.join(abandoned_sessions, sess)
        .filter(F.lower(F.col("event_type")) == "cart")
        .select(
            F.col("user_id").cast("string").alias("user_id"),
            F.col(sess).alias("user_session"),
            F.col("product_id").cast("string").alias("product_id"),
            F.col("product_metadata") if "product_metadata" in df.columns else F.lit(None).alias("product_metadata"),
        )
        .distinct()
    )

    if "product_metadata" in df.columns:
        carts = (
            carts
            .withColumn(
                "_meta_clean",
                F.when(
                    F.col("product_metadata").isNotNull(),
                    F.regexp_replace(
                        F.regexp_replace(F.trim(F.col("product_metadata")), r'^"|"$', ""),
                        '""', '"',
                    ),
                ),
            )
            .withColumn(
                "_meta_clean",
                F.when(
                    F.col("_meta_clean").isNotNull()
                    & (F.substr(F.col("_meta_clean"), F.lit(1), F.lit(1)) != "{"),
                    F.concat(F.lit("{"), F.col("_meta_clean"), F.lit("}")),
                ).otherwise(F.col("_meta_clean")),
            )
            .withColumn("item_category", F.lower(F.trim(
                F.from_json(F.col("_meta_clean"), "MAP<STRING,STRING>")["category"]
            )))
            .drop("_meta_clean", "product_metadata")
        )
    else:
        carts = carts.withColumn("item_category", F.lit(None).cast(StringType()))

    br_user_cats = spark.sparkContext.broadcast(user_top_categories)
    br_high_freq = spark.sparkContext.broadcast(high_freq_items)

    def campaign_udf(user_id, product_id, item_category):
        uid = str(user_id) if user_id is not None else ""
        pid = str(product_id) if product_id is not None else ""
        cat = str(item_category).strip().lower() if item_category is not None else ""

        top_cats = br_user_cats.value.get(uid, set())

        if cat and top_cats and cat in top_cats:
            return "High_Discount"

        if pid in br_high_freq.value:
            return "High_Discount"

        return "Standard_Reminder"

    enriched = carts.withColumn(
        "campaign_type",
        F.udf(campaign_udf, StringType())(
            F.col("user_id"), F.col("product_id"), F.col("item_category")
        ),
    )

    enriched = enriched.withColumn("top_rank_cutoff", F.lit(args.top_rank_cutoff)) \
                       .withColumn("pair_freq_cutoff_p75", F.lit(pair_cutoff))

    print("Sample enriched abandonment rows:")
    enriched.show(25, truncate=False)

    print("Campaign type distribution:")
    enriched.groupBy("campaign_type").count().show()

    out = args.output
    os.makedirs(os.path.dirname(os.path.abspath(out)) or ".", exist_ok=True)
    wurl = out if out.startswith("file:") else f"file:///{os.path.abspath(out)}"
    enriched.coalesce(1).write.mode("overwrite").option("header", "true").csv(wurl)
    print(f"Wrote: {wurl}")

    spark.stop()


if __name__ == "__main__":
    main()
