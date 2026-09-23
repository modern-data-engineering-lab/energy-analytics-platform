terraform {
  # 1.10+ required for native S3 conditional-write state locking (`use_lockfile` below) — the
  # same mechanism the real AWS infra repo this portfolio is modeled on actually uses, verified
  # against its real `terraform.tf` rather than assumed. No DynamoDB lock table needed.
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0.0, < 6.0.0"
    }
    github = {
      source  = "integrations/github"
      version = ">= 6.0.0, < 7.0.0"
    }
  }

  # Remote state: bucket/key/region come from -backend-config=config/{stg,prd}.hcl at init
  # time, so the same backend block works for both environments. CI runners have no local disk
  # to persist state on between runs, so this isn't optional once deploys move into CI/CD —
  # it's the actual reason this moved off local state, not just tidiness.
  backend "s3" {
    use_lockfile = true
  }
}
