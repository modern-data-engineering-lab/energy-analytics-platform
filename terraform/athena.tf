####################################################
# Athena workgroup — query results land in the same data lake bucket under their own prefix,
# encrypted, with per-query cost capped so an accidental full-table scan can't run away.
####################################################
resource "aws_athena_workgroup" "this" {
  name = "${var.project}-${var.env}"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data_lake.bucket}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    bytes_scanned_cutoff_per_query = 1073741824 # 1 GB — far more than this dataset needs; a guard rail, not a real limit
  }
}
