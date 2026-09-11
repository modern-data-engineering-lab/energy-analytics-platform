# Backend config for a remote S3 state backend, matching this portfolio's real AWS infra
# pattern (config/{stg,prd}.hcl passed to `terraform init -backend-config=...`). Not active by
# default — this repo ships with local state (see versions.tf) since it's one person on one
# account. To actually use this: add a `backend "s3" {}` block to versions.tf, create the
# bucket + DynamoDB lock table it references (bootstrap that separately, outside this config,
# same chicken-and-egg reasoning as any Terraform-managed state backend), then:
#   terraform init -backend-config=config/stg.hcl
bucket         = "energy-analytics-platform-tf-state"
key            = "stg/terraform.tfstate"
region         = "eu-north-1"
dynamodb_table = "energy-analytics-platform-tf-lock"
encrypt        = true
