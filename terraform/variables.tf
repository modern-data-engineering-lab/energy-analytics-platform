variable "env" {
  description = "Deployment environment — stg or prd. Threaded through every resource name."
  type        = string
  validation {
    condition     = contains(["stg", "prd"], var.env)
    error_message = "env must be \"stg\" or \"prd\"."
  }
}

variable "aws_region" {
  description = "AWS region. eu-north-1 (Stockholm) by default, matching this portfolio's real AWS infra."
  type        = string
  default     = "eu-north-1"
}

variable "project" {
  description = "Short project name, used as a prefix on every resource. Kebab-case."
  type        = string
  default     = "energy-analytics"
}

variable "project_tag" {
  description = "Human-readable project name for the Project tag on every taggable resource — mirrors this portfolio's real AWS infra house style (per-resource Project tag via a variable, rather than folding it into provider-level default_tags)."
  type        = string
  default     = "Energy Analytics Platform"
}

variable "notification_email" {
  description = "Email address subscribed to the pipeline-failure SNS topic (paired with the EventBridge Scheduler DLQ)."
  type        = string
}

variable "glue_worker_type" {
  description = "Glue worker type for the ETL jobs. G.1X is the smallest/cheapest standard worker — plenty for a dataset this size (a few thousand rows/year)."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers per job run. 2 is the minimum Glue allows."
  type        = number
  default     = 2
}

variable "github_owner" {
  description = "GitHub org or user that owns the repo."
  type        = string
  default     = "modern-data-engineering-lab"
}

variable "github_repository" {
  description = "Repository name only, no owner prefix."
  type        = string
  default     = "energy-analytics-platform"
}

variable "production_approver_github_user_id" {
  description = <<-EOT
    Numeric GitHub user ID (not username — get it with `gh api user -q .id`) required to
    approve a production deploy. Set to null to skip the required-reviewer gate entirely —
    same pattern as databricks-bundle-template's terraform, reused here rather than
    reinvented, since this repo's whole point in this pass is closing the same CI/CD gap that
    repo already solved.
  EOT
  type        = number
  default     = null
}

variable "state_bucket_arn" {
  description = <<-EOT
    ARN of the S3 state bucket — bootstrapped once, outside this config (see
    config/stg.hcl's comment), so it can't be looked up via a resource this config owns. The
    GitHub Actions deploy role needs read/write on it to run `terraform apply` at all.
  EOT
  type        = string
  default     = "arn:aws:s3:::energy-analytics-platform-tf-state"
}
