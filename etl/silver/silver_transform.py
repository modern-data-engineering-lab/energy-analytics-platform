"""
Silver: the real cleaning layer. Three things happen here, each validated against the actual
IBEDC data (not assumed from the thesis's prose description) before being written:

1. Date quarantine — ~1% of bronze rows have unparseable or out-of-2021 dates (Excel epoch
   artifacts like 1900-01-07, and embedded section-label rows like "33kV FEEDERS OUTAGES"
   that read as a data row with junk in the date column — see bronze_ingest.py's docstring).
   Rows failing the date check are quarantined, not dropped: written to their own table so
   they're visible and auditable, not silently discarded.

2. Outage-type normalization — ~30 raw spelling/punctuation variants of what are really 9
   categories ("EDC F/O", "EDC/F/O", "EDCF/O" are the same thing). Normalized by stripping
   punctuation/whitespace and mapping the compact form to a fixed canonical list. Anything
   that doesn't match a known canonical form (validated: this is exactly 3 rows, "EDC /S")
   becomes "OTHER" rather than being force-mapped by guesswork.

3. Feeder-name canonicalization — 165 raw spellings of what are really the 18 named 33KV
   feeders this analysis covers, plus a genuinely separate category of transformer/substation
   -level events (e.g. "T-2B @ JERICHO") that are correctly *not* one of the 18 feeders, not
   a typo of one. Known real typos (INDUSRIAL, M1NISTER, SAMANDA, etc.) are corrected
   explicitly by name — auditable in code review — rather than via a generic fuzzy-matcher.
   Validated result: 98.8% of rows map to a canonical feeder; the remaining 1.2% are all
   genuine transformer events, confirmed by inspecting every unmapped raw value by hand.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# transforms.py holds the pure canonicalization logic — no pyspark/awsglue imports there, so
# it's unit-testable on its own (see tests/test_transforms.py). Glue only deploys the single
# script named in `script_location`; transforms.py reaches this job via the Glue job's
# `--extra-py-files` argument (see terraform/glue.tf), which Glue downloads and puts on
# sys.path automatically — importable directly, no manual path manipulation needed here.
from transforms import canonicalize_feeder, canonicalize_outage_type

args = getResolvedOptions(sys.argv, ["JOB_NAME", "data_bucket", "database_name"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

BUCKET = args["data_bucket"]
DATABASE = args["database_name"]
BRONZE_PATH = f"s3://{BUCKET}/bronze/interruptions/"
SILVER_CLEAN_PATH = f"s3://{BUCKET}/silver/interruptions_clean/"
SILVER_QUARANTINE_PATH = f"s3://{BUCKET}/silver/interruptions_quarantine/"

canonicalize_feeder_udf = F.udf(canonicalize_feeder, StringType())
canonicalize_outage_type_udf = F.udf(canonicalize_outage_type, StringType())


def main():
    bronze = spark.read.parquet(BRONZE_PATH)

    parsed = (
        bronze.withColumn("event_date", F.to_date("date_raw"))
        .withColumn(
            "feeder_canonical",
            canonicalize_feeder_udf(F.col("name_of_affected_feeder")),
        )
        .withColumn(
            "outage_type_canonical",
            canonicalize_outage_type_udf(F.col("type_of_outage")),
        )
        .withColumn("is_transformer_event", F.col("feeder_canonical") == "UNMAPPED")
        .withColumn("duration_hours_num", F.col("duration_hours").cast("double"))
        .withColumn("load_loss_mw_num", F.col("load_loss_mw").cast("double"))
        .withColumn(
            "no_of_customers_restored_num", F.col("no_of_customers_restored").cast("double")
        )
        .withColumn(
            "customer_hours_interruption_num",
            F.col("customer_hours_interruption").cast("double"),
        )
        .withColumn("event_no_num", F.col("event_no").cast("int"))
    )

    # Quarantine: unparseable dates, or dates outside 2021 (the year this dataset actually
    # covers per the source thesis — anything else is either an Excel-epoch artifact or an
    # embedded section-label row that isn't a real interruption record at all).
    is_valid_date = F.col("event_date").isNotNull() & (F.year("event_date") == 2021)

    quarantine = parsed.filter(~is_valid_date)
    clean = parsed.filter(is_valid_date)

    (
        clean.write.mode("overwrite")
        .partitionBy("source_month")
        .parquet(SILVER_CLEAN_PATH)
    )
    (
        quarantine.write.mode("overwrite")
        .partitionBy("source_month")
        .parquet(SILVER_QUARANTINE_PATH)
    )

    # interruptions_clean and interruptions_quarantine are registered directly in Terraform
    # (terraform/glue.tf), matching the location/partitioning written above — no separate
    # catalog-registration write needed here. (A prior version of this job re-read the just
    # -written Parquet and rewrote it via write_dynamic_frame.from_catalog to self-register the
    # table; that call doesn't honor the partitioned layout and silently appended a second,
    # flat, unpartitioned copy of the data to the same S3 prefix on every run.)

    clean_count = clean.count()
    quarantine_count = quarantine.count()
    unmapped_count = clean.filter(F.col("is_transformer_event")).count()
    print(
        f"Silver: {clean_count} clean rows, {quarantine_count} quarantined "
        f"({quarantine_count / (clean_count + quarantine_count):.1%}), "
        f"{unmapped_count} clean rows are transformer/substation events, not a named feeder"
    )
    job.commit()


if __name__ == "__main__":
    main()
