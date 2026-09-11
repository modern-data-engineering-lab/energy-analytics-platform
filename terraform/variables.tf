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
