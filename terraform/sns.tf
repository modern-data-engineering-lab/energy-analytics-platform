####################################################
# Failure notifications — paired with the Step Functions catch block and the EventBridge
# Scheduler's DLQ, matching the "scheduler + DLQ + alerting" pattern from this portfolio's
# real AWS infra rather than a scheduled job that fails silently.
####################################################
resource "aws_sns_topic" "pipeline_failures" {
  name = "${var.project}-pipeline-failures-${var.env}"

  tags = {
    Project = var.project_tag
  }
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.pipeline_failures.arn
  protocol  = "email"
  endpoint  = var.notification_email
}
