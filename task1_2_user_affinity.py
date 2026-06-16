import argparse
import os
import sys

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
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import spark_win_setup


def parse_args():
    p = argparse.ArgumentParser(description="User-category weighted affinity ranks")
    p.add_argument(
        "--input",
        default=os.environ.get("RAW_LOG_PATH", os.path.join("data", "ecommerce_logs_sample.csv")),
    )
    p.add_argument(
        "--output",
        default=os.environ.get(
            "OUTPUT_DIR", os.path.join("data", "output", "user_affinity_aggregation")
        ),
    )
    p.add_argument("--master", default=os.environ.get("SPARK_MASTER", "local[*]"))
    p.add_argument(
        "--top-n",
        type=int,
        default=0,
        help="If > 0, keep only top N categories per user (after ranking). 0 = keep all.",
    )
    return p.parse_args()


def main():
    spark_win_setup.ensure_hadoop_home()
    args = parse_args()
    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Input CSV not found: {args.input}")

    spark = (
        SparkSession.builder.appName("UserAffinity_Task1_2")
        .master(args.master)
        .config("spark.driver.memory", "2g")
        .config("spark.hadoop.io.native.lib.available", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    path = args.input if args.input.startswith("file:") else f"file:///{os.path.abspath(args.input)}"
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("escape", '"')
        .csv(path)
        .select("user_id", "product_metadata", "event_type")
    )

    print(f"Total rows: {df.count()}")

    with_cleaning = df.withColumn(
        "clean_metadata",
        F.when(
            F.col("product_metadata").isNotNull(),
            F.regexp_replace(
                F.regexp_replace(
                    F.trim(F.col("product_metadata")),  
                    r'^"|"$', ''  
                ),
                '""', '"'  
            )
        )
    ).withColumn(
        "clean_metadata",
        F.when(
            F.col("clean_metadata").isNotNull() & (F.substr(F.col("clean_metadata"), F.lit(1), F.lit(1)) != "{"),
            F.concat(F.lit("{"), F.col("clean_metadata"), F.lit("}"))
        ).otherwise(F.col("clean_metadata"))
    ).withColumn(
        "json_obj",
        F.from_json(F.col("clean_metadata"), "MAP<STRING,STRING>")
    ).withColumn(
        "category",
        F.col("json_obj")["category"]
    ).drop("json_obj")

    print("Sample cleaned data:")
    with_cleaning.select(
        F.col("product_metadata"),
        F.col("clean_metadata"),
        F.col("category")
    ).show(10, truncate=False)

    with_weights = with_cleaning.withColumn("weight",
          F.when(F.lower(F.col("event_type")) == "view",     F.lit(1))
           .when(F.lower(F.col("event_type")) == "cart",     F.lit(3))
           .when(F.lower(F.col("event_type")) == "purchase", F.lit(5))
           .otherwise(F.lit(0)))

    print(f"Rows with non-null category: {with_weights.filter(F.col('category').isNotNull()).count()}")

    exploded = with_weights.filter(
          F.col("user_id").isNotNull() &
          F.col("category").isNotNull() &
          (F.col("weight") > 0)
    ).drop("clean_metadata")

    print(f"Rows after all filters: {exploded.count()}")
    scored = exploded.groupBy("user_id", "category").agg(
        F.sum("weight").cast("long").alias("weighted_score")
    )

    w_rank = Window.partitionBy("user_id").orderBy(F.col("weighted_score").desc())
    ranked = scored.withColumn("rank", F.row_number().over(w_rank))

    if args.top_n > 0:
        ranked = ranked.filter(F.col("rank") <= args.top_n)

    out_path = args.output
    os.makedirs(os.path.dirname(os.path.abspath(out_path)) or ".", exist_ok=True)
    write_url = out_path if out_path.startswith("file:") else f"file:///{os.path.abspath(out_path)}"

    ranked.orderBy("user_id", "rank").coalesce(1).write.mode("overwrite").option(
        "header", "true"
    ).csv(write_url)

    print("Sample ranked affinities:")
    ranked.orderBy("user_id", "rank").show(30, truncate=False)
    print(f"Wrote: {write_url}")
    spark.stop()


if __name__ == "__main__":
    main()