# Backend config for a remote S3 state backend, matching this portfolio's real AWS infra
# pattern (config/{stg,prd}.hcl passed to `terraform init -backend-config=...`). Now active —
# see versions.tf's `backend "s3" {}` block.
#
# Locking uses Terraform's native S3 conditional-write locking (`use_lockfile = true` in
# versions.tf, Terraform 1.10+), not a separate DynamoDB lock table — this matches the real
# AWS infra repo's own backend exactly (verified against its actual `terraform.tf`), corrected
# here from an earlier draft of this file that specified a `dynamodb_table` before that was
# checked against the real thing.
#
# Bootstrap (one-time, outside this config — same chicken-and-egg reasoning as any
# Terraform-managed state backend): create the S3 bucket with versioning + SSE + a blocked
# public-access configuration, then:
#   terraform init -backend-config=config/stg.hcl
bucket  = "energy-analytics-platform-tf-state"
key     = "stg/terraform.tfstate"
region  = "eu-north-1"
encrypt = true
