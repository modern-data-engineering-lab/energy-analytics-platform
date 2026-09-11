####################################################
# DLQ for the scheduler — if EventBridge Scheduler itself fails to invoke the state machine
# (as opposed to the pipeline failing after it starts, which SNS above handles), the failed
# invocation lands here instead of vanishing. Paired scheduler+DLQ, same as the real AWS infra
# pattern this portfolio follows.
####################################################
resource "aws_sqs_queue" "scheduler_dlq" {
  name                      = "${var.project}-scheduler-dlq-${var.env}"
  message_retention_seconds = 1209600 # 14 days, the SQS maximum
}

resource "aws_sqs_queue_policy" "scheduler_dlq" {
  queue_url = aws_sqs_queue.scheduler_dlq.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowEventBridgeScheduler"
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.scheduler_dlq.arn
      Condition = {
        ArnEquals = { "aws:SourceArn" = aws_scheduler_schedule.daily_pipeline.arn }
      }
    }]
  })
}

####################################################
# Weekly trigger for the pipeline. The underlying IBEDC dataset is a static 2021 archive, not
# a live feed — this schedule demonstrates the pattern a real periodic-ingestion job would use
# (e.g. "new interruption file dropped weekly"), not an actually-changing data source.
####################################################
resource "aws_scheduler_schedule" "daily_pipeline" {
  name       = "${var.project}-weekly-${var.env}"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "rate(7 days)"

  target {
    arn      = aws_sfn_state_machine.pipeline.arn
    role_arn = aws_iam_role.eventbridge_scheduler.arn

    dead_letter_config {
      arn = aws_sqs_queue.scheduler_dlq.arn
    }
  }
}
