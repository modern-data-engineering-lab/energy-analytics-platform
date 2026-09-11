terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0.0, < 6.0.0"
    }
  }

  # Local state by default (terraform.tfstate in this directory, gitignored). Fine for one
  # person running one AWS account. A real team would move this to a remote backend — S3 +
  # DynamoDB for locking, matching the `syno-ds-tf-state`-style pattern this portfolio's real
  # AWS infra repo already uses — see terraform/README.md's "State" section for the exact
  # bootstrap steps if you want to do that here too.
}
