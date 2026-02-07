"""
1) Clean & standardize rows (EXPLODED by genre)
3) Write cleaned output as PARQUET (cloud-friendly)
4) Log metrics (bad_dates, score_out_of_range, dropped_missing_id_loc_date, etc.)

Notes:
- This job writes Parquets
"""

from __future__ import annotations
import os
import re
from dataclasses import dataclass
from typing import Optional, Tuple, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from src.utils.logger_config import get_logger
from src.utils.azure_blob import configure_wasbs_account_key

# -----------------------------
# Config
# -----------------------------
EXPECTED_TEMPLATE = "spotify_rising_with_trends_{date}.csv"

TRANSFORMED_SCHEMA = StructType([
    StructField("artist", StringType(), True),
    StructField("id", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("location", StringType(), True),
    StructField("date", StringType(), True),
    StructField("trend_score", StringType(), True),
])

@dataclass(frozen=True)
class TransformPaths:
    raw_root: Optional[str]
    transform_root: str

# -----------------------------
# Logging
# -----------------------------
logger = get_logger("Spark_Transform")

# -----------------------------
# Spark bootstrap (Databricks + Azure safe)
# -----------------------------
def build_spark(app_name: str = "capstone-transform-spark") -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    storage_key = os.getenv("AZURE_STORAGE_KEY")

    if storage_account and storage_key:
        configure_wasbs_account_key(spark, storage_account, storage_key)
        logger.info("Azure Blob Storage configured")

    return spark

# -----------------------------
# Helpers
# -----------------------------
def _extract_batch_date_from_filename(name: str) -> Optional[str]:
    m = re.search(
        r"spotify_rising_with_trends_(\d{4}_\d{2}_\d{2})\.csv$",
        name,
        flags=re.I,
    )
    return m.group(1) if m else None

# -----------------------------
# Core transforms (mirror pandas logic)
# -----------------------------

def clean_and_validate(df: DataFrame) -> Tuple[DataFrame, Dict[str, int]]:
    """
      - trim strings
      - location upper
      - date '_' -> '-' then parse
      - bad_dates metric
      - trend_score numeric cast, out-of-range metric (null counts as out-of-range like pandas)
      - clip 0..100 and fill null with 0
      - drop missing id/location/date
      - genres null -> 'Unknown'
    """
    logger.info("Begin transformation cleaning operations.")
    logger.info("Trimming columnns")
    # Standardize base string columns
    df = (
        df
        .withColumn("artist", F.trim(F.col("artist")))
        .withColumn("id", F.trim(F.col("id")))
        .withColumn("location", F.upper(F.trim(F.col("location"))))
        .withColumn("genres", F.trim(F.col("genres")))
    )
    logger.info("Normalizing date values.")
    # Date normalize '_' -> '-' and parse,
    date_norm = F.trim(F.regexp_replace(F.col("date").cast("string"), "_", "-"))
    df = df.withColumn("date_parsed", F.to_date(date_norm, "yyyy-MM-dd"))

    # Base counts
    rows_in = df.count()

    bad_dates = df.filter(F.col("date_parsed").isNull()).count()
    
    # trend_score numeric
    ts_num = F.col("trend_score").cast("double")
    df = df.withColumn("ts_num", ts_num)

    logger.info("Replacing nulls with 0 in trend score.")
    # Out-of-range metric: (null OR <0 OR >100)
    score_out_of_range = df.filter(
        F.col("ts_num").isNull() | (F.col("ts_num") < F.lit(0.0)) | (F.col("ts_num") > F.lit(100.0))
    ).count()

    # Clip + fill
    df = df.withColumn(
        "trend_score_cleaned",
        F.when(F.col("ts_num").isNull(), F.lit(0.0))
         .otherwise(F.least(F.lit(100.0), F.greatest(F.lit(0.0), F.col("ts_num"))))
    )

    logger.info("Dropping records with nulls in id, location, and date.")
    # Drop missing essentials (id, location, date)
    before_drop = df.count()
    df = df.filter(
        F.col("id").isNotNull() &
        F.col("location").isNotNull() &
        F.col("date_parsed").isNotNull()
    )
    after_drop = df.count()
    dropped_missing = int(before_drop - after_drop)

    # Genres null -> Unknown
    df = df.withColumn("genres", F.coalesce(F.col("genres"), F.lit("Unknown")))

    # Finalize
    df = (
        df
        .drop("date")
        .withColumnRenamed("date_parsed", "date")
        .drop("trend_score")
        .withColumnRenamed("trend_score_cleaned", "trend_score")
        .drop("ts_num")
    )

    metrics = {
        "rows_in": int(rows_in),
        "bad_dates": int(bad_dates),
        "score_out_of_range": int(score_out_of_range),
        "dropped_missing_id_loc_date": int(dropped_missing),
    }

    logger.info("Cleaning process complete.")
    return df.select("artist", "id", "genres", "location", "date", "trend_score"), metrics


def explode_genres(df: DataFrame) -> Tuple[DataFrame, Dict[str, int]]:
    """
    Mirrors robust pandas logic:
      - normalize [|,] -> ';'
      - parse list-like strings '[...]' (single quotes possible) OR split on ';'
      - explode to one genre per row
      - clean tokens + lowercase
      - drop empties (keep 'unknown')
    """

    logger.info("Exploding nested genres.")
    # Normalize alt delimiters to ';'
    df = df.withColumn("genres_norm", F.regexp_replace(F.col("genres"), r"[|,]", ";"))

    # metric: delimiter replaced (rows where changed)
    delim_replaced = df.filter(F.col("genres") != F.col("genres_norm")).count()

    is_list_like = F.col("genres_norm").rlike(r"^\s*\[.*\]\s*$")

    # Attempt to parse JSON array. Replace single quotes with double quotes only for list-like.
    # If parsing fails, parsed_arr becomes null and fallback to split.
    json_ready = F.when(is_list_like, F.regexp_replace(F.col("genres_norm"), r"'", '"')).otherwise(F.lit(None))

    parsed_arr = F.from_json(json_ready, "array<string>")
    split_arr = F.split(F.col("genres_norm"), r"\s*;\s*")

    df = df.withColumn(
        "genres_arr",
        F.when(is_list_like & parsed_arr.isNotNull(), parsed_arr).otherwise(split_arr)
    )

    df = df.withColumn("genre", F.explode(F.col("genres_arr")))

    # Final token clean (mirrors regex + strip + lower)
    df = (df.withColumn("genre", 
                    F.lower(F.trim(F.regexp_replace(
                        F.col("genre").cast("string"),
                        r"^[\s\"'\[\]\(\)]+|[\s\"'\[\]\(\)]+$",
                        ""
                    )))))
    logger.info("Replacing empty genres with Unknown.")
    # Drop empty genres; keep "unknown"
    before = df.count()
    df = df.filter(F.col("genre").isNotNull() & (F.col("genre") != ""))
    after = df.count()
    dropped_empty = int(before - after)

    # Fill any nulls to "unknown" 
    df = df.withColumn("genre", F.coalesce(F.col("genre"), F.lit("unknown")))

    out = (df
        .drop("genres", "genres_norm", "genres_arr")
        .withColumnRenamed("genre", "genres")
        .select("artist", "id", "genres", "location", "date", "trend_score")
    )

    metrics = {
        "genre_delim_replaced": int(delim_replaced),
        "rows_after_explode": int(after),
        "dropped_empty_genre": int(dropped_empty),
    }
    logger.info("Genre explosion complete.")
    return out, metrics


# -----------------------------
# Write
# -----------------------------

def write_transform_parquet(df: DataFrame, transform_root: str, batch_date: str) -> str:
    """
    Writes Parquet to:
      data/transform/<batch_date>/spotify_rising_cleaned_<batch_date>/
    Partition by year,month for cloud-friendly layout.
    """
    outdir = (
            f"{transform_root.rstrip('/')}/"
            f"{batch_date}/spotify_rising_cleaned_{batch_date}"
    )

    # add partition cols
    dfw = (
        df
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
    )

    outdir_str = str(outdir)

    logger.info(f"[WRITE] Parquet -> {outdir_str}")
    (
        dfw
        .repartition("year", "month")  # helps avoid tiny files
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(outdir_str)
    )

    return outdir


# -----------------------------
# Job runner
# -----------------------------

def transform(paths: TransformPaths) -> None:
    spark = build_spark()

    # Override 
    override = os.getenv("TRANSFORM_ONE_OFF_INPUT")

    if not override:
        raise RuntimeError(
            "TRANSFORM_ONE_OFF_INPUT must be set when running in Databricks"
        )

    raw_path = override
    batch_date = _extract_batch_date_from_filename(raw_path.split("/")[-1])

    if not batch_date:
        raise ValueError(f"Cannot extract batch_date from {raw_path}")

    logger.info(f"[INPUT] {raw_path}")

    # Read CSV (schema locked for parity)
    df_raw = (
        spark.read
        .schema(TRANSFORMED_SCHEMA)
        .option("header", "true")
        .csv(str(raw_path))
    )

    # if os.getenv("SPARK_TEST_MODE") == "1":
    #     logger.info("Running in test mode. Input is row-limited.")
    #     df_raw = df_raw.limit(100_000)

    logger.info(f"[READ] raw rows (uncounted until action): columns={df_raw.columns}")

    # 1) base clean
    df_clean, m1 = clean_and_validate(df_raw)

    # 2) explode genres
    df_exploded, m2 = explode_genres(df_clean)

    # Trigger count once here for final row metric
    final_rows = df_exploded.count()

    # Write parquet only
    outdir = write_transform_parquet(df_exploded, paths.transform_root, batch_date)

    # Log summary metrics (mirrors pandas summary)
    total_metrics = {**m1, **m2, "rows_written": int(final_rows)}
    logger.info(
        "[SUMMARY] "
        f"batch_date={batch_date}, "
        f"rows_in={total_metrics['rows_in']:,}, "
        f"rows_written={total_metrics['rows_written']:,}, "
        f"bad_dates={total_metrics['bad_dates']:,}, "
        f"score_out_of_range={total_metrics['score_out_of_range']:,}, "
        f"dropped_missing_id_loc_date={total_metrics['dropped_missing_id_loc_date']:,}, "
        f"genre_delim_replaced={total_metrics['genre_delim_replaced']:,}, "
        f"rows_after_explode={total_metrics['rows_after_explode']:,}, "
        f"dropped_empty_genre={total_metrics['dropped_empty_genre']:,}"
    )
    logger.info(f"[DONE] transform written to: {outdir}")


def main() -> None:
    transform_root = os.getenv("TRANSFORM_ROOT")
    if not transform_root:
        raise EnvironmentError("TRANSFORM_ROOT must be set")

    transform(
        TransformPaths(
            raw_root=None,
            transform_root=transform_root,
        )
    )


if __name__ == "__main__":
    main()