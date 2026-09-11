provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "energy-analytics-platform"
      Environment = var.env
      ManagedBy   = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}
