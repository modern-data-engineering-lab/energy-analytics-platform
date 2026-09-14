"""
Gold: two outputs from the clean silver table.

1. mttr_mtbf_by_feeder — reliability metrics per canonical feeder (the 18 named feeders only;
   transformer/substation events are excluded here, same as the thesis's own scope). Computed
   correctly per-feeder using a window function ordered by event timestamp *within each
   feeder's own event sequence* — deliberately not the thesis's original approach, which
   computed gaps between consecutive events across *all* feeders combined and produced a
   nonsensical negative MTBF as an acknowledged result of that overlap. Same metric, fixed
   computation.

   MTTR = mean(duration_hours) per feeder — directly available per event, no reconstruction
   needed. MTBF = mean(hours between one event's start and the next event's start, for that
   same feeder) — the first event per feeder has no prior event and is excluded from the mean.

2. classifier_features — the feature table the XGBoost outage-type classifier trains/infers
   against. Deliberately a smaller feature set than the thesis's own (duration, load loss,
   customers restored, customer-hours interruption, event number, feeder) — Relay Target is
   excluded here: it's a messy mixed-format string field ("6,1", "99", "1F") that would need
   its own dedicated cleaning pass to use safely, and doing that well is out of scope for
   this repo's "fast, self-contained AWS pipeline" purpose (see README's "Deliberately out of
   scope here"). A smaller, honestly-described feature set beats a larger one built on a
   field this pipeline doesn't actually clean.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "data_bucket", "database_name"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

BUCKET = args["data_bucket"]
DATABASE = args["database_name"]
SILVER_CLEAN_PATH = f"s3://{BUCKET}/silver/interruptions_clean/"
GOLD_MTTR_MTBF_PATH = f"s3://{BUCKET}/gold/mttr_mtbf_by_feeder/"
GOLD_FEATURES_PATH = f"s3://{BUCKET}/gold/classifier_features/"


def main():
    clean = spark.read.parquet(SILVER_CLEAN_PATH)

    feeder_events = (
        clean.filter(~F.col("is_transformer_event"))
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.concat_ws(" ", F.col("event_date"), F.col("start_time"))),
        )
        .filter(F.col("event_timestamp").isNotNull())
    )

    feeder_window = Window.partitionBy("feeder_canonical").orderBy("event_timestamp")
    with_gaps = feeder_events.withColumn(
        "hours_since_prior_event",
        (
            F.col("event_timestamp").cast("long")
            - F.lag("event_timestamp").over(feeder_window).cast("long")
        )
        / 3600.0,
    )

    mttr_mtbf = with_gaps.groupBy("feeder_canonical").agg(
        F.count("*").alias("total_outages"),
        F.round(F.mean("duration_hours_num"), 2).alias("mttr_hours"),
        F.round(F.mean("hours_since_prior_event"), 2).alias("mtbf_hours"),
        F.round(F.sum("customer_hours_interruption_num"), 1).alias(
            "total_customer_hours_interruption"
        ),
    )

    (mttr_mtbf.coalesce(1).write.mode("overwrite").parquet(GOLD_MTTR_MTBF_PATH))

    features = clean.select(
        "feeder_canonical",
        "outage_type_canonical",
        "duration_hours_num",
        "load_loss_mw_num",
        "no_of_customers_restored_num",
        "customer_hours_interruption_num",
        "event_no_num",
        "is_transformer_event",
        "source_month",
    )
    (
        features.write.mode("overwrite")
        .partitionBy("source_month")
        .parquet(GOLD_FEATURES_PATH)
    )

    # mttr_mtbf_by_feeder and classifier_features are registered directly in Terraform
    # (terraform/glue.tf), matching the location/partitioning written above — see
    # silver_transform.py's equivalent comment for why there's no separate catalog-
    # registration write here (write_dynamic_frame.from_catalog doesn't honor the partitioned
    # layout and would silently duplicate the data on every run).

    print(
        f"Gold: {mttr_mtbf.count()} feeders in mttr_mtbf_by_feeder, "
        f"{features.count()} rows in classifier_features"
    )
    job.commit()


if __name__ == "__main__":
    main()
