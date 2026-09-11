# See stg.hcl for what this is and why it's not active by default.
bucket         = "energy-analytics-platform-tf-state"
key            = "prd/terraform.tfstate"
region         = "eu-north-1"
dynamodb_table = "energy-analytics-platform-tf-lock"
encrypt        = true
