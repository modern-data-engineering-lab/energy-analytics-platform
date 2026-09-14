####################################################
# Step Functions: bronze -> silver -> gold, each step a synchronous Glue job run
# (.sync integration — the state machine waits for the job to finish rather than firing and
# forgetting). Any failure anywhere in the chain publishes to SNS instead of failing silently
# — no separate orchestrator needed, this mirrors the real AWS infra pattern this portfolio is
# built on.
####################################################
resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.project}-pipeline-${var.env}"
  role_arn = aws_iam_role.step_functions.arn

  definition = jsonencode({
    Comment = "IBEDC interruption data: bronze -> silver -> gold -> classify"
    StartAt = "BronzeIngest"
    States = {
      BronzeIngest = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.bronze_ingest.name
        }
        Next  = "SilverTransform"
        Catch = [{ ErrorEquals = ["States.ALL"], Next = "NotifyFailure" }]
        Retry = [{ ErrorEquals = ["States.TaskFailed"], MaxAttempts = 1, IntervalSeconds = 30 }]
      }
      SilverTransform = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.silver_transform.name
        }
        Next  = "GoldAggregate"
        Catch = [{ ErrorEquals = ["States.ALL"], Next = "NotifyFailure" }]
        Retry = [{ ErrorEquals = ["States.TaskFailed"], MaxAttempts = 1, IntervalSeconds = 30 }]
      }
      GoldAggregate = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.gold_aggregate.name
        }
        Next  = "ClassifyOutages"
        Catch = [{ ErrorEquals = ["States.ALL"], Next = "NotifyFailure" }]
        Retry = [{ ErrorEquals = ["States.TaskFailed"], MaxAttempts = 1, IntervalSeconds = 30 }]
      }
      ClassifyOutages = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.outage_classifier.name
        }
        End   = true
        Catch = [{ ErrorEquals = ["States.ALL"], Next = "NotifyFailure" }]
        Retry = [{ ErrorEquals = ["States.TaskFailed"], MaxAttempts = 1, IntervalSeconds = 30 }]
      }
      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.pipeline_failures.arn
          Message  = "energy-analytics-platform pipeline (${var.env}) failed — check the Step Functions execution history for details."
        }
        End = true
      }
    }
  })
}
