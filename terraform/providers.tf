provider "aws" {
  region = var.aws_region

  # Kept deliberately minimal at provider level — mirrors this portfolio's real AWS infra house
  # style, where default_tags holds one account-wide constant and everything project-specific
  # (Project, selectively Name) is an explicit per-resource tag instead. Environment is never a
  # tag there either — it's threaded through resource names via var.env, not tagged.
  default_tags {
    tags = {
      ManagedBy = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}
