####################################################
# Glue Data Catalog database — one per environment, so stg and prd tables never collide even
# though they may share the same underlying bucket's different prefixes.
####################################################
resource "aws_glue_catalog_database" "this" {
  name = replace("${var.project}_${var.env}", "-", "_") # Glue database names can't contain hyphens
}

####################################################
# Bronze: land the 12 raw monthly IBEDC interruption files as-is (no cleaning) into
# Parquet, partitioned by month. This is "what IBEDC actually handed over," unmodified.
####################################################
resource "aws_glue_job" "bronze_ingest" {
  name         = "${var.project}-bronze-ingest-${var.env}"
  role_arn     = aws_iam_role.glue_job.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.bronze_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_lake.bucket}/tmp/bronze/"
    "--data_bucket"                      = aws_s3_bucket.data_lake.bucket
    "--database_name"                    = aws_glue_catalog_database.this.name
    "--additional-python-modules"        = "openpyxl"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 15
}

####################################################
# Silver: the actual cleaning layer — outage-type normalization, feeder-name canonicalization,
# date quarantine. See etl/silver/silver_transform.py for the real logic; this resource is
# just the Glue job wrapper.
####################################################
resource "aws_glue_job" "silver_transform" {
  name         = "${var.project}-silver-transform-${var.env}"
  role_arn     = aws_iam_role.glue_job.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.silver_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_lake.bucket}/tmp/silver/"
    "--data_bucket"                      = aws_s3_bucket.data_lake.bucket
    "--database_name"                    = aws_glue_catalog_database.this.name
    "--extra-py-files"                   = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.transforms_script.key}"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 15
}

####################################################
# Gold: MTTR/MTBF per canonical feeder + the feature table the XGBoost classifier trains/infers
# against.
####################################################
resource "aws_glue_job" "gold_aggregate" {
  name         = "${var.project}-gold-aggregate-${var.env}"
  role_arn     = aws_iam_role.glue_job.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.gold_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_lake.bucket}/tmp/gold/"
    "--data_bucket"                      = aws_s3_bucket.data_lake.bucket
    "--database_name"                    = aws_glue_catalog_database.this.name
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 15
}

####################################################
# Outage classifier — Glue **Python Shell**, not Spark ETL. At ~6,600 rows this doesn't need
# distributed compute; Python Shell starts faster and costs less for a job this size. See the
# README's "Why Glue Spark ETL for a dataset this small" note — Spark was chosen for
# bronze/silver/gold for stack-consistency reasons that don't apply here, since there's no
# "XGBoost on Spark" story this repo is trying to tell.
####################################################
resource "aws_glue_job" "outage_classifier" {
  name     = "${var.project}-outage-classifier-${var.env}"
  role_arn = aws_iam_role.glue_job.arn

  command {
    name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.classifier_script.key}"
    python_version  = "3.9"
  }

  default_arguments = {
    "--data_bucket"                      = aws_s3_bucket.data_lake.bucket
    "--additional-python-modules"        = "xgboost,scikit-learn,pandas,pyarrow"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  max_capacity = 0.0625 # smallest Python Shell capacity — 1/16 DPU, plenty for this data size
  timeout      = 15
}

####################################################
# The classifier's predictions table, registered directly in Terraform rather than by the
# Python Shell job itself. Python Shell jobs don't have GlueContext (that's Spark/ETL-job-only)
# — self-registration the way bronze/silver/gold do it isn't available here, and the schema is
# fixed and known upfront anyway, so defining it in Terraform is simpler than adding a direct
# boto3 create_table call to the training script.
####################################################
resource "aws_glue_catalog_table" "classifier_predictions" {
  name          = "classifier_predictions"
  database_name = aws_glue_catalog_database.this.name

  table_type = "EXTERNAL_TABLE"
  parameters = { classification = "parquet" }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.bucket}/gold/classifier_predictions/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "feeder_canonical"
      type = "string"
    }
    columns {
      name = "outage_type_canonical"
      type = "string"
    }
    columns {
      name = "source_month"
      type = "string"
    }
    columns {
      name = "predicted_outage_type"
      type = "string"
    }
    columns {
      name = "correct"
      type = "boolean"
    }
  }
}
