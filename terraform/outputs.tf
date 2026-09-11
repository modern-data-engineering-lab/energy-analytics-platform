output "data_lake_bucket" {
  value = aws_s3_bucket.data_lake.bucket
}

output "glue_database" {
  value = aws_glue_catalog_database.this.name
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.pipeline.arn
}

output "athena_workgroup" {
  value = aws_athena_workgroup.this.name
}

output "raw_upload_prefix" {
  description = "Where to upload the 12 raw monthly IBEDC .xlsx files (Getting Started)."
  value       = "s3://${aws_s3_bucket.data_lake.bucket}/bronze-raw/"
}
