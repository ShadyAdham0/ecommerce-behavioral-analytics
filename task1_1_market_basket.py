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

from pyspark.sql import SparkSession


def _session_column(df):
    if "session_id" in df.columns:
        return "session_id"
    if "user_session" in df.columns:
        return "user_session"
    raise ValueError(
        "Expected session_id or user_session. Found: " + str(df.columns)
    )


def parse_args():
    p = argparse.ArgumentParser(description="Market basket co-occurrence (MapReduce)")
    p.add_argument(
        "--input",
        default=os.environ.get("RAW_LOG_PATH", os.path.join("data", "ecommerce_logs.csv")),
        help="CSV path (header: user_session/product_id, event_type, ...).",
    )
    p.add_argument(
        "--output",
        default=os.environ.get(
            "OUTPUT_DIR", os.path.join("data", "output", "co_occurrence_pairs")
        ),
        help="Output directory for co-occurrence CSV (single file via coalesce).",
    )
    p.add_argument("--master", default=os.environ.get("SPARK_MASTER", "local[*]"))
    return p.parse_args()


def main():
    spark_win_setup.ensure_hadoop_home()
    args = parse_args()
    in_path = args.input
    if not os.path.exists(in_path):
        raise FileNotFoundError(f"Input CSV not found: {in_path}")

    spark = (
        SparkSession.builder.appName("MarketBasketAnalysis_Task1_1")
        .master(args.master)
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.io.native.lib.available", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    file_url = in_path if in_path.startswith("file:") else f"file:///{os.path.abspath(in_path)}"
    raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_url)

    if "product_id" not in raw_df.columns:
        raise ValueError("Expected product_id. Found: " + str(raw_df.columns))
    sess = _session_column(raw_df)

    purchases_df = raw_df.filter(raw_df["event_type"] == "purchase").select(
        raw_df[sess].alias("session_id"),
        raw_df["product_id"].alias("item_id"),
    )
    purchases_df = purchases_df.filter(
        purchases_df["item_id"].isNotNull() & purchases_df["session_id"].isNotNull()
    )
    purchases_rdd = purchases_df.rdd.map(lambda row: (row["session_id"], row["item_id"]))

    session_items_rdd = purchases_rdd.groupByKey()

    def pairs_from_session(session_and_items):
        _session_id, items = session_and_items
        distinct = sorted({i for i in items if i is not None})
        for pair in combinations(distinct, 2):
            a, b = pair[0], pair[1]
            yield ((a, b), 1)

    pair_counts_rdd = session_items_rdd.flatMap(pairs_from_session).reduceByKey(
        lambda a, b: a + b
    )

    from pyspark.sql import Row

    out_rows = pair_counts_rdd.map(
        lambda x: Row(item_a=x[0][0], item_b=x[0][1], frequency=x[1])
    )
    out_df = spark.createDataFrame(out_rows).orderBy("frequency", ascending=False)

    out_path = args.output
    os.makedirs(os.path.dirname(os.path.abspath(out_path)) or ".", exist_ok=True)
    write_url = out_path if out_path.startswith("file:") else f"file:///{os.path.abspath(out_path)}"

    out_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(write_url)

    print("Top 20 co-purchased pairs:")
    out_df.show(20, truncate=False)
    print(f"Wrote: {write_url}")
    spark.stop()


if __name__ == "__main__":
    main()
