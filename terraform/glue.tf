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
    "--job-language"                    = "python"
    "--TempDir"                         = "s3://${aws_s3_bucket.data_lake.bucket}/tmp/bronze/"
    "--data_bucket"                     = aws_s3_bucket.data_lake.bucket
    "--database_name"                   = aws_glue_catalog_database.this.name
    "--enable-metrics"                  = "true"
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
    "--job-language"                    = "python"
    "--TempDir"                         = "s3://${aws_s3_bucket.data_lake.bucket}/tmp/silver/"
    "--data_bucket"                     = aws_s3_bucket.data_lake.bucket
    "--database_name"                   = aws_glue_catalog_database.this.name
    "--enable-metrics"                  = "true"
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
    "--job-language"                    = "python"
    "--TempDir"                         = "s3://${aws_s3_bucket.data_lake.bucket}/tmp/gold/"
    "--data_bucket"                     = aws_s3_bucket.data_lake.bucket
    "--database_name"                   = aws_glue_catalog_database.this.name
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 15
}
