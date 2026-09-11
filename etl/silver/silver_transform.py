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

import re
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

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

CANONICAL_FEEDERS = [
    "OLUYOLE", "INTERCHANGE", "EXPRESS", "LIBERTY", "INDUSTRIAL", "ROM", "APETE",
    "AGODI 1", "AGODI 2", "MINISTER", "SAMONDA", "FAN MILK", "ERUWA TOWN",
    "ERUWA/LANLATE", "OMI ADIO", "APATA", "IYAGANKU", "ELEYELE",
]

# Real typos observed in the source data (validated against all 6,609 rows before writing this
# — see the repo's data-exploration notes in README.md). Fixed explicitly, not via fuzzy match.
KNOWN_FEEDER_TYPOS = {
    "INDUSRIAL": "INDUSTRIAL", "INDUTRIAL": "INDUSTRIAL",
    "INTERCHANE": "INTERCHANGE", "INTERCHNAGE": "INTERCHANGE", "INTRCHANGE": "INTERCHANGE",
    "LBERTY": "LIBERTY", "LIBERTRY": "LIBERTY",
    "M1NISTER": "MINISTER", "MINISTETR": "MINISTER", "MINSTER": "MINISTER",
    "OLUYOLY": "OLUYOLE", "OLYOLE": "OLUYOLE",
    "SAMANDA": "SAMONDA", "SAMODA": "SAMONDA",
    "ERUW": "ERUWA",
    "OMI-ADIO": "OMI ADIO",
}

CANONICAL_OUTAGE_TYPES = {
    "EDCFO": "EDC F/O", "EDCEF": "EDC E/F", "EDCLS": "EDC L/S", "EDCPO": "EDC P/O",
    "EDCOC": "EDC O/C", "TCNLS": "TCN L/S", "TCNFO": "TCN F/O", "TCNPO": "TCN P/O",
    "GENSC": "GEN S/C",
}


def canonicalize_feeder(raw: str) -> str:
    if raw is None:
        return "UNMAPPED"
    s = raw.upper().strip()
    for typo, fix in KNOWN_FEEDER_TYPOS.items():
        s = s.replace(typo, fix)
    s = re.sub(r"[,.\-]", " ", s)
    s = re.sub(r"\b(FDR|FEEDER|LINE|RESTORED|33KLV|33IV|33BKV|\d+KV|\d+MVA|@)\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    n_nospace = s.replace(" ", "")

    if "AGODI" in n_nospace:
        if "1" in n_nospace:
            return "AGODI 1"
        if "2" in n_nospace:
            return "AGODI 2"
        return "UNMAPPED"
    if "ERUWA" in n_nospace:
        if "LANLATE" in n_nospace or "LANALTE" in n_nospace:
            return "ERUWA/LANLATE"
        if "TOWN" in n_nospace:
            return "ERUWA TOWN"
        return "UNMAPPED"
    if "APETE" in n_nospace:
        return "APETE"
    if "APATA" in n_nospace:
        return "APATA"
    for canon in CANONICAL_FEEDERS:
        canon_key = canon.replace(" ", "").replace("/", "")
        if canon_key in n_nospace or n_nospace in canon_key:
            return canon
    return "UNMAPPED"


def canonicalize_outage_type(raw: str) -> str:
    if raw is None:
        return "OTHER"
    s = raw.upper().strip()
    s = re.sub(r"[\s/.\-]", "", s)
    s = s.replace("0", "O")
    return CANONICAL_OUTAGE_TYPES.get(s, "OTHER")


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

    for table_name, path in [
        ("interruptions_clean", SILVER_CLEAN_PATH),
        ("interruptions_quarantine", SILVER_QUARANTINE_PATH),
    ]:
        dyf = glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [path]},
            format="parquet",
        )
        glue_context.write_dynamic_frame.from_catalog(
            frame=dyf, database=DATABASE, table_name=table_name
        )

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
