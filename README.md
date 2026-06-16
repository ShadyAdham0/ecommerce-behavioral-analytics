# E-Commerce Behavioral Analytics & Recommendation Engine

End-to-end data engineering pipeline that processes raw e-commerce logs at scale
using Apache Spark, stores behavioral profiles in MongoDB, and generates targeted
marketing campaign flags for cart-abandonment recovery.

## Pipeline Overview
## Phases

### Phase 1 — Distributed Processing (Spark MapReduce)
- **Task 1.1** `task1_1_market_basket.py` — Item co-occurrence via MapReduce across purchase sessions
- **Task 1.2a** `task1_2_user_affinity.py` — Weighted user-category affinity scores (view=1, cart=3, purchase=5)
- **Task 1.2b** `task1_2_association_rules.py` — Association rules (support, confidence, lift) for item pairs

### Phase 2 — NoSQL Storage
- **Task 2** `task2_ingestion.py` — Loads Phase 1 output CSVs into MongoDB (`co_occurrence` + `user_affinity` collections)

### Phase 3 — Enrichment Pipeline
- **Task 3** `task3_enrichment.py` — Joins abandoned-cart sessions with MongoDB profiles; flags `High_Discount` or `Standard_Reminder` based on category-affinity match and data-driven co-occurrence thresholds

### Query Demo
- `query_demo.py` — Standalone script: fetches top affinity categories and co-purchase recommendations for any `user_id` / `item_id`

## Tech Stack

| Layer | Tool |
|---|---|
| Distributed processing | Apache Spark (PySpark) |
| NoSQL storage | MongoDB |
| Dataset | 10 GB+ CSV event logs |
| Language | Python 3.9+ |

## Setup

### Prerequisites
- Java 11 or 17 (set `JAVA_HOME`)
- Python 3.9+
- MongoDB running locally or via Docker
- (Windows only) Place Hadoop winutils in `hadoop_win/` — handled automatically by `spark_win_setup.py`

### Install dependencies

```bash
pip install pyspark pymongo pandas
```

### Run the pipeline

```bash
# Phase 1
python task1_1_market_basket.py --input data/ecommerce_logs.csv
python task1_2_user_affinity.py --input data/ecommerce_logs.csv
python task1_2_association_rules.py --input data/ecommerce_logs.csv

# Phase 2
python task2_ingestion.py

# Phase 3
python task3_enrichment.py --input data/ecommerce_logs.csv

# Query demo
python query_demo.py --user-id 12345 --item-id 67890
```

## Configuration

All scripts accept CLI arguments and respect environment variables:

| Variable | Default | Description |
|---|---|---|
| `RAW_LOG_PATH` | `data/ecommerce_logs.csv` | Input log file |
| `OUTPUT_DIR` | `data/output/` | Output directory |
| `MONGO_URI` | `mongodb://localhost:27017/` | MongoDB connection string |
| `MONGO_DB` | `ecommerce_analytics` | Database name |
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `TOP_RANK_CUTOFF` | `3` | Top-N affinity categories per user for High_Discount targeting |

## Data-Driven Thresholds (Phase 3)

The enrichment pipeline does **not** use hard-coded cutoffs. Instead:
- **Category match**: a user's "top" categories are those with rank ≤ `TOP_RANK_CUTOFF` (computed by Task 1.2)
- **Pair frequency cutoff**: dynamically computed as the **75th percentile** of all co-occurrence frequencies in MongoDB — adapts to the actual data distribution

## Output Schema

`enriched_abandonment/` CSV columns:

| Column | Description |
|---|---|
| `user_id` | User identifier |
| `user_session` | Session identifier |
| `product_id` | Abandoned product |
| `item_category` | Parsed category from product metadata |
| `campaign_type` | `High_Discount` or `Standard_Reminder` |
| `top_rank_cutoff` | Rank cutoff used (for auditability) |
| `pair_freq_cutoff_p75` | 75th-pct frequency cutoff used |

## Project Structure
## .gitignore
