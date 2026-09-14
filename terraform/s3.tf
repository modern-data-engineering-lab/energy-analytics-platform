####################################################
# One data lake bucket, bronze/silver/gold as prefixes (not separate buckets) — standard
# medallion-in-one-bucket layout, cheaper and simpler to manage than bucket-per-layer.
# Bucket names must be globally unique across all of AWS, hence the account ID suffix.
####################################################
resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project}-data-${var.env}-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name    = "Energy Analytics Data Lake"
    Project = var.project_tag
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Glue ETL script sources, uploaded by Terraform from ../etl/. Kept in the same bucket under
# its own prefix rather than a separate bucket — one less resource to manage for a
# single-project repo this size.
resource "aws_s3_object" "bronze_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/bronze_ingest.py"
  source = "${path.module}/../etl/bronze/bronze_ingest.py"
  etag   = filemd5("${path.module}/../etl/bronze/bronze_ingest.py")
}

resource "aws_s3_object" "silver_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/silver_transform.py"
  source = "${path.module}/../etl/silver/silver_transform.py"
  etag   = filemd5("${path.module}/../etl/silver/silver_transform.py")
}

# The pure canonicalization logic silver_transform.py imports — Glue only deploys the single
# file named in a job's script_location, so this reaches the job via --extra-py-files instead
# (see glue.tf). Kept as its own object, not bundled into silver_script, so it stays a single
# source of truth shared with tests/test_transforms.py (which imports the same file directly).
resource "aws_s3_object" "transforms_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/transforms.py"
  source = "${path.module}/../etl/transforms.py"
  etag   = filemd5("${path.module}/../etl/transforms.py")
}

resource "aws_s3_object" "gold_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/gold_aggregate.py"
  source = "${path.module}/../etl/gold/gold_aggregate.py"
  etag   = filemd5("${path.module}/../etl/gold/gold_aggregate.py")
}

resource "aws_s3_object" "classifier_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/train_and_predict.py"
  source = "${path.module}/../ml/outage_classifier/train_and_predict.py"
  etag   = filemd5("${path.module}/../ml/outage_classifier/train_and_predict.py")
}
