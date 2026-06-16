"""
Phase 1 – Task 1.2 (part B): Association Rules
================================================
Computes support, confidence, and lift for every item pair found in purchase
sessions, then writes the top rules (sorted by lift) to CSV.

Requires: co-occurrence pair counts produced by task1_1_market_basket.py
          (or re-derives them here directly from the raw log).

Portability fix: removed hard-coded Windows paths.
  Configure via env vars or CLI arguments instead.
"""

import argparse
import os
import sys
from itertools import combinations

import spark_win_setup


if os.environ.get("JAVA_HOME"):
    os.environ["PATH"] = (
        os.path.join(os.environ["JAVA_HOME"], "bin")
        + os.pathsep
        + os.environ.get("HADOOP_HOME", "")
        + (os.pathsep if os.environ.get("HADOOP_HOME") else "")
        + os.environ["PATH"]
    )
if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F


def parse_args():
    root = os.path.dirname(os.path.abspath(__file__))
    p = argparse.ArgumentParser(description="Association rules (support / confidence / lift)")
    p.add_argument(
        "--input",
        default=os.environ.get("RAW_LOG_PATH", os.path.join(root, "data", "ecommerce_logs.csv")),
        help="Path to raw ecommerce log CSV.",
    )
    p.add_argument(
        "--output",
        default=os.environ.get(
            "OUTPUT_DIR", os.path.join(root, "data", "output", "association_rules")
        ),
        help="Output directory for the rules CSV.",
    )
    p.add_argument("--master", default=os.environ.get("SPARK_MASTER", "local[*]"))
    p.add_argument(
        "--min-support",
        type=float,
        default=float(os.environ.get("MIN_SUPPORT", "0.0")),
        help="Minimum support threshold (fraction of transactions). Default: 0 (keep all).",
    )
    p.add_argument(
        "--min-confidence",
        type=float,
        default=float(os.environ.get("MIN_CONFIDENCE", "0.0")),
        help="Minimum confidence threshold. Default: 0 (keep all).",
    )
    return p.parse_args()


def _session_column(df):
    if "session_id" in df.columns:
        return "session_id"
    if "user_session" in df.columns:
        return "user_session"
    raise ValueError("Expected session_id or user_session. Found: " + str(df.columns))


def main():
    spark_win_setup.ensure_hadoop_home()
    args = parse_args()

    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Input CSV not found: {args.input}")

    spark = (
        SparkSession.builder.appName("AssociationRules_Task1_2")
        .master(args.master)
        .config("spark.hadoop.io.native.lib.available", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    file_url = (
        args.input
        if args.input.startswith("file:")
        else f"file:///{os.path.abspath(args.input)}"
    )
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_url)

    print("Raw row count:", df.count())
    print("Distinct event types:")
    df.select("event_type").distinct().show()

    sess = _session_column(df)
    purchases = df.filter(df["event_type"] == "purchase").select(sess, "product_id")
    purchases = purchases.filter(
        purchases["product_id"].isNotNull() & purchases[sess].isNotNull()
    )

    print("After filter count:", purchases.count())

    total_transactions = purchases.select(sess).distinct().count()
    print(f"Total Transactions (N): {total_transactions}")

   
    item_counts = (
        purchases.groupBy("product_id")
        .agg(F.countDistinct(sess).alias("frequency_a"))
        .collect()
    )
    item_freq_dict = {row["product_id"]: row["frequency_a"] for row in item_counts}

    session_items_rdd = (
        purchases.rdd
        .map(lambda row: (row[sess], row["product_id"]))
        .groupByKey()
        .map(lambda x: sorted(set(x[1])))
    )

    def extract_pairs(items):
        for pair in combinations(items, 2):
            yield (pair, 1)

    pair_counts = (
        session_items_rdd
        .flatMap(extract_pairs)
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )

    rules_list = []

    for (item_a, item_b), freq_ab in pair_counts:
        freq_a = item_freq_dict.get(item_a, 1)
        freq_b = item_freq_dict.get(item_b, 1)

        support_ab = freq_ab / total_transactions
        prob_a = freq_a / total_transactions
        prob_b = freq_b / total_transactions

        conf_a_b = freq_ab / freq_a
        lift_a_b = support_ab / (prob_a * prob_b)
        rules_list.append(
            Row(antecedent=item_a, consequent=item_b,
                support=support_ab, confidence=conf_a_b, lift=lift_a_b)
        )

        conf_b_a = freq_ab / freq_b
        lift_b_a = support_ab / (prob_b * prob_a)  
        rules_list.append(
            Row(antecedent=item_b, consequent=item_a,
                support=support_ab, confidence=conf_b_a, lift=lift_b_a)
        )

    rules_df = spark.createDataFrame(rules_list)

    if args.min_support > 0:
        rules_df = rules_df.filter(F.col("support") >= args.min_support)
    if args.min_confidence > 0:
        rules_df = rules_df.filter(F.col("confidence") >= args.min_confidence)

    sorted_rules_df = rules_df.orderBy(F.desc("lift"))

    print("\n" + "═" * 70)
    print("  TOP 20 ASSOCIATION RULES BY LIFT")
    print("═" * 70)
    sorted_rules_df.show(20, truncate=False)

    out = args.output
    os.makedirs(os.path.dirname(os.path.abspath(out)) or ".", exist_ok=True)
    wurl = out if out.startswith("file:") else f"file:///{os.path.abspath(out)}"
    sorted_rules_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(wurl)
    print(f"Rules successfully saved to: {wurl}")

    spark.stop()


if __name__ == "__main__":
    main()
