# See stg.hcl for what this is, why it's now active, and why there's no DynamoDB table.
bucket  = "energy-analytics-platform-tf-state"
key     = "prd/terraform.tfstate"
region  = "eu-north-1"
encrypt = true
