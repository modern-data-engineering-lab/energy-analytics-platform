####################################################
# Permission boundary — the actual ceiling on what any role in this project can ever do,
# regardless of what its own policy grants. Matches the permission-boundary pattern from this
# portfolio's real AWS infra (see BUILD-GUIDE.md § AWS infra conventions): belt-and-suspenders
# scoping, not just "trust the role's own policy to be written correctly."
####################################################
resource "aws_iam_policy" "permission_boundary" {
  name        = "${var.project}-permission-boundary-${var.env}"
  description = "Ceiling permissions for every role in this project — S3 scoped to this project's bucket only, no IAM/account-wide actions."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ThisProjectOnly"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*",
        ]
      },
      {
        Sid      = "Glue"
        Effect   = "Allow"
        Action   = ["glue:*"]
        Resource = "*"
      },
      {
        Sid      = "Athena"
        Effect   = "Allow"
        Action   = ["athena:*"]
        Resource = "*"
      },
      {
        Sid      = "Logs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "*"
      },
      {
        Sid      = "SNSPublishOnly"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.pipeline_failures.arn
      },
      {
        Sid      = "StepFunctionsAndScheduler"
        Effect   = "Allow"
        Action   = ["states:StartExecution", "states:DescribeExecution"]
        Resource = "*"
      },
    ]
  })
}

####################################################
# Glue job execution role — what the bronze/silver/gold ETL jobs actually run as. Scoped by
# the permission boundary above; its own policy is deliberately narrower still (least
# privilege from both directions).
####################################################
resource "aws_iam_role" "glue_job" {
  name                 = "${var.project}-glue-job-${var.env}"
  permissions_boundary = aws_iam_policy.permission_boundary.arn

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "glue_job" {
  name = "${var.project}-glue-job-policy-${var.env}"
  role = aws_iam_role.glue_job.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DataLakeReadWrite"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*",
        ]
      },
      {
        Sid    = "GlueCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable",
          "glue:UpdateTable", "glue:BatchCreatePartition", "glue:GetPartitions",
        ]
        Resource = "*"
      },
      {
        Sid      = "Logs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_job_service_role" {
  role       = aws_iam_role.glue_job.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

####################################################
# Step Functions execution role — orchestrates bronze -> silver -> gold, publishes to SNS on
# failure. This is the state machine's own identity, separate from what the Glue jobs run as.
####################################################
resource "aws_iam_role" "step_functions" {
  name                 = "${var.project}-step-functions-${var.env}"
  permissions_boundary = aws_iam_policy.permission_boundary.arn

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "step_functions" {
  name = "${var.project}-step-functions-policy-${var.env}"
  role = aws_iam_role.step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "RunGlueJobs"
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
        Resource = "*"
      },
      {
        Sid      = "NotifyOnFailure"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.pipeline_failures.arn
      },
    ]
  })
}

####################################################
# EventBridge Scheduler's own execution role — permission to do exactly one thing: start the
# state machine. Matches the "scheduler + DLQ paired on every scheduled job" pattern from this
# portfolio's real AWS infra.
####################################################
resource "aws_iam_role" "eventbridge_scheduler" {
  name                 = "${var.project}-eventbridge-scheduler-${var.env}"
  permissions_boundary = aws_iam_policy.permission_boundary.arn

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_scheduler" {
  name = "${var.project}-eventbridge-scheduler-policy-${var.env}"
  role = aws_iam_role.eventbridge_scheduler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "StartPipelineExecution"
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = aws_sfn_state_machine.pipeline.arn
    }]
  })
}
