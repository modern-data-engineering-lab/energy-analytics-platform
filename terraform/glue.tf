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

####################################################
# Silver and gold's own output tables, registered in Terraform for the same reason as
# classifier_predictions above: glue_context.write_dynamic_frame.from_catalog() (used by
# silver_transform.py and gold_aggregate.py to register their tables for Athena) requires the
# target table to already exist — it's a write into an existing catalog entry, not a
# create-if-missing call. Schemas mirror exactly what each script writes.
####################################################
locals {
  # interruptions_clean and interruptions_quarantine share one schema: both are filters of the
  # same `parsed` DataFrame in silver_transform.py.
  interruptions_columns = [
    { name = "date_raw", type = "string" },
    { name = "location", type = "string" },
    { name = "name_of_affected_feeder", type = "string" },
    { name = "event_no", type = "string" },
    { name = "type_of_outage", type = "string" },
    { name = "nature_cause_of_outage", type = "string" },
    { name = "relay_target", type = "string" },
    { name = "start_time", type = "string" },
    { name = "time_restored", type = "string" },
    { name = "duration_hours", type = "string" },
    { name = "load_loss_mw", type = "string" },
    { name = "no_of_customers_restored", type = "string" },
    { name = "total_customers_served", type = "string" },
    { name = "customer_hours_interruption", type = "string" },
    { name = "remarks", type = "string" },
    { name = "source_file", type = "string" },
    { name = "ingested_at", type = "string" },
    { name = "event_date", type = "date" },
    { name = "feeder_canonical", type = "string" },
    { name = "outage_type_canonical", type = "string" },
    { name = "is_transformer_event", type = "boolean" },
    { name = "duration_hours_num", type = "double" },
    { name = "load_loss_mw_num", type = "double" },
    { name = "no_of_customers_restored_num", type = "double" },
    { name = "customer_hours_interruption_num", type = "double" },
    { name = "event_no_num", type = "int" },
  ]
}

resource "aws_glue_catalog_table" "interruptions_clean" {
  name          = "interruptions_clean"
  database_name = aws_glue_catalog_database.this.name

  table_type = "EXTERNAL_TABLE"
  parameters = { classification = "parquet" }

  partition_keys {
    name = "source_month"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.bucket}/silver/interruptions_clean/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = local.interruptions_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

resource "aws_glue_catalog_table" "interruptions_quarantine" {
  name          = "interruptions_quarantine"
  database_name = aws_glue_catalog_database.this.name

  table_type = "EXTERNAL_TABLE"
  parameters = { classification = "parquet" }

  partition_keys {
    name = "source_month"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.bucket}/silver/interruptions_quarantine/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = local.interruptions_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

resource "aws_glue_catalog_table" "mttr_mtbf_by_feeder" {
  name          = "mttr_mtbf_by_feeder"
  database_name = aws_glue_catalog_database.this.name

  table_type = "EXTERNAL_TABLE"
  parameters = { classification = "parquet" }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.bucket}/gold/mttr_mtbf_by_feeder/"
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
      name = "total_outages"
      type = "bigint"
    }
    columns {
      name = "mttr_hours"
      type = "double"
    }
    columns {
      name = "mtbf_hours"
      type = "double"
    }
    columns {
      name = "total_customer_hours_interruption"
      type = "double"
    }
  }
}

resource "aws_glue_catalog_table" "classifier_features" {
  name          = "classifier_features"
  database_name = aws_glue_catalog_database.this.name

  table_type = "EXTERNAL_TABLE"
  parameters = { classification = "parquet" }

  partition_keys {
    name = "source_month"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.bucket}/gold/classifier_features/"
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
      name = "duration_hours_num"
      type = "double"
    }
    columns {
      name = "load_loss_mw_num"
      type = "double"
    }
    columns {
      name = "no_of_customers_restored_num"
      type = "double"
    }
    columns {
      name = "customer_hours_interruption_num"
      type = "double"
    }
    columns {
      name = "event_no_num"
      type = "int"
    }
    columns {
      name = "is_transformer_event"
      type = "boolean"
    }
  }
}
