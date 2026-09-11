"""
Bronze: land the 12 raw monthly IBEDC interruption workbooks as-is into Parquet.

Deliberately does no cleaning — that's silver's job. This layer exists to answer "what did
IBEDC actually hand over," unmodified, so any cleaning decision made downstream can always be
checked against the real source instead of trusting silver's output on faith.

The raw files are real internal utility workbooks, not clean data tables:
  - Each .xlsx has 6-8 sheets (RAW MONTHLY, WORKING WEEKLY, SUMMARY OF ..., REVENUE LOSS, etc.)
    — only "RAW MONTHLY" holds the interruption records this pipeline cares about.
  - The header row isn't at a fixed position — it's preceded by 2-3 junk rows (a title, an
    email contact, a region label) that vary slightly month to month. Detected by content
    (the row containing "Event No"), not a hardcoded row index.
  - There are section-label rows embedded *mid-sheet* (e.g. "33kV FEEDERS OUTAGES") that read
    as a data row with junk in the Date column and NaN everywhere else. Bronze keeps these —
    silver's date-quarantine step (see silver_transform.py) is what filters them out, the same
    logic that also catches genuinely bad dates (Excel epoch artifacts, out-of-year rows).
  - Exact header text varies slightly by file ("Date  (For Carry Over Outage Only)" with
    inconsistent whitespace) — columns are assigned **positionally**, not by matching header
    text, since the position has been consistent across every file checked and the text hasn't.
"""

import re
import sys
from datetime import datetime, timezone

import boto3
import pandas as pd
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "data_bucket", "database_name"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

BUCKET = args["data_bucket"]
RAW_PREFIX = "bronze-raw/"
BRONZE_PREFIX = "bronze/interruptions/"

# Positional column names — see module docstring for why positional, not header-text-based.
# The 16th column is consistently blank across every file checked; dropped.
COLUMNS = [
    "date_raw",
    "location",
    "name_of_affected_feeder",
    "event_no",
    "type_of_outage",
    "nature_cause_of_outage",
    "relay_target",
    "start_time",
    "time_restored",
    "duration_hours",
    "load_loss_mw",
    "no_of_customers_restored",
    "total_customers_served",
    "customer_hours_interruption",
    "remarks",
]

MONTH_FROM_FILENAME = re.compile(
    r"(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEM\w*|OCTOBER|NOVEMBER|DECEMBER)",
    re.IGNORECASE,
)


def find_header_row(raw_df: pd.DataFrame) -> int:
    """Locate the header row by content (contains "Event No"), not a fixed row index — the
    exact number of preamble rows has varied by one between files already observed."""
    for i in range(min(10, len(raw_df))):
        row_values = raw_df.iloc[i].astype(str).str.strip()
        if row_values.isin(["Event No"]).any():
            return i
    raise ValueError('Could not locate header row (no cell containing "Event No" in first 10 rows)')


def read_one_workbook(local_path: str, source_file: str) -> pd.DataFrame:
    raw = pd.read_excel(local_path, sheet_name="RAW MONTHLY", header=None)
    header_row = find_header_row(raw)
    data = raw.iloc[header_row + 1 :].reset_index(drop=True)
    data = data.iloc[:, : len(COLUMNS)]
    data.columns = COLUMNS

    month_match = MONTH_FROM_FILENAME.search(source_file)
    data["source_file"] = source_file
    data["source_month"] = month_match.group(1).upper() if month_match else "UNKNOWN"
    data["ingested_at"] = datetime.now(timezone.utc).isoformat()
    return data


def main():
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=BUCKET, Prefix=RAW_PREFIX)
        for obj in page.get("Contents", [])
        if obj["Key"].lower().endswith(".xlsx")
    ]
    if not keys:
        raise RuntimeError(
            f"No .xlsx files found under s3://{BUCKET}/{RAW_PREFIX} — upload the 12 monthly "
            "IBEDC workbooks there first (see Getting Started)."
        )

    frames = []
    for key in keys:
        local_path = f"/tmp/{key.split('/')[-1]}"
        s3.download_file(BUCKET, key, local_path)
        source_file = key.split("/")[-1]
        frames.append(read_one_workbook(local_path, source_file))
        print(f"Read {len(frames[-1])} rows from {source_file}")

    combined = pd.concat(frames, ignore_index=True)
    # Every column as string at this layer — bronze preserves raw fidelity, including values
    # that will turn out to be unparseable. Silver is where real typing happens.
    combined = combined.astype(str)

    bronze_df = spark.createDataFrame(combined)
    (
        bronze_df.write.mode("overwrite")
        .partitionBy("source_month")
        .parquet(f"s3://{BUCKET}/{BRONZE_PREFIX}")
    )

    # Read the just-written Parquet back to fail fast here, at bronze, if it's somehow
    # unreadable — rather than surfacing that failure one step later in silver. Bronze's
    # output is consumed directly by silver via this S3 path; it doesn't need its own Glue
    # catalog table (silver registers the tables it produces, not what it reads).
    glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [f"s3://{BUCKET}/{BRONZE_PREFIX}"]},
        format="parquet",
    )

    print(f"Bronze: wrote {combined.shape[0]} rows across {combined['source_month'].nunique()} months")
    job.commit()


if __name__ == "__main__":
    main()
