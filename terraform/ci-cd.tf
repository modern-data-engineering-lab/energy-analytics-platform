####################################################
# GitHub Actions OIDC — lets CI assume an AWS role with short-lived STS credentials, no
# long-lived access keys stored as a GitHub secret. Same mechanism as the real AWS infra repo
# and databricks-bundle-template's own ci-cd.tf, reused here rather than reinvented.
####################################################
resource "aws_iam_openid_connect_provider" "github_actions" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # Root CA SHA1 fingerprint — fetched directly from the live TLS chain at
  # token.actions.githubusercontent.com (openssl s_client -showcerts), not a commonly-copied
  # value from memory: GitHub's OIDC endpoint sits behind a Let's Encrypt root that's newer
  # than the "1c58a3a8..." value still floating around in a lot of older Terraform examples.
  # AWS no longer strictly re-validates this against the live cert for well-known public CAs,
  # but the resource schema still requires a syntactically valid one.
  thumbprint_list = ["ab9d0263244dd0326eb67015705a667e79cfe998"]
}

####################################################
# Two roles, not one — staging and production get separate assumable identities, matching the
# "one identity per environment, never shared" principle already used twice elsewhere in this
# portfolio (retail-customer-intelligence's two service principals; the real infra repo's two
# separate AWS accounts). This is one AWS account, so the isolation is narrower than that, but
# keeping the trust condition scoped per environment still means a compromised staging workflow
# can't assume the production role, and either can be revoked independently.
####################################################
resource "aws_iam_role" "github_actions_staging" {
  name = "${var.project}-github-actions-staging"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.github_actions.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        }
        StringLike = {
          "token.actions.githubusercontent.com:sub" = "repo:${var.github_owner}/${var.github_repository}:environment:staging"
        }
      }
    }]
  })

  tags = {
    Project = var.project_tag
  }
}

resource "aws_iam_role" "github_actions_production" {
  name = "${var.project}-github-actions-production"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.github_actions.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        }
        StringLike = {
          "token.actions.githubusercontent.com:sub" = "repo:${var.github_owner}/${var.github_repository}:environment:production"
        }
      }
    }]
  })

  tags = {
    Project = var.project_tag
  }
}

####################################################
# The deploy policy — what CI is actually allowed to do. IAM and S3 are scoped tightly to this
# project's own naming convention/buckets, since those are the two places a wildcard would
# reach outside this project's blast radius. Glue/Athena/Step Functions/Scheduler/SNS/SQS/Logs
# use Resource = "*" — not a new risk posture invented for this role, it's the exact same
# per-service scoping this repo's own permission_boundary (iam.tf) already accepts for these
# services, reused here rather than re-litigated. Both roles share one policy — staging and
# production need the same deploy actions, only the trust condition differs.
####################################################
resource "aws_iam_policy" "github_actions_deploy" {
  name        = "${var.project}-github-actions-deploy"
  description = "What CI is allowed to do: manage this project's own AWS resources and read/write its Terraform state."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "TerraformState"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [
          var.state_bucket_arn,
          "${var.state_bucket_arn}/*",
        ]
      },
      {
        Sid      = "DataLakeBucket"
        Effect   = "Allow"
        Action   = ["s3:*"]
        Resource = ["arn:aws:s3:::${var.project}-data-*", "arn:aws:s3:::${var.project}-data-*/*"]
      },
      {
        Sid    = "ProjectIamRolesAndPolicies"
        Effect = "Allow"
        Action = [
          "iam:CreateRole", "iam:DeleteRole", "iam:GetRole", "iam:UpdateRole",
          "iam:PutRolePolicy", "iam:DeleteRolePolicy", "iam:GetRolePolicy",
          "iam:AttachRolePolicy", "iam:DetachRolePolicy",
          "iam:ListRolePolicies", "iam:ListAttachedRolePolicies", "iam:ListInstanceProfilesForRole",
          "iam:TagRole", "iam:UntagRole",
          "iam:CreatePolicy", "iam:DeletePolicy", "iam:GetPolicy", "iam:GetPolicyVersion",
          "iam:ListPolicyVersions", "iam:CreatePolicyVersion", "iam:DeletePolicyVersion",
          "iam:TagPolicy", "iam:UntagPolicy",
        ]
        Resource = [
          "arn:aws:iam::*:role/${var.project}-*",
          "arn:aws:iam::*:policy/${var.project}-*",
        ]
      },
      {
        Sid      = "PassProjectRolesToAwsServices"
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = "arn:aws:iam::*:role/${var.project}-*"
        Condition = {
          StringEquals = {
            "iam:PassedToService" = ["glue.amazonaws.com", "states.amazonaws.com", "scheduler.amazonaws.com"]
          }
        }
      },
      { Sid = "Glue", Effect = "Allow", Action = ["glue:*"], Resource = "*" },
      { Sid = "Athena", Effect = "Allow", Action = ["athena:*"], Resource = "*" },
      { Sid = "StepFunctions", Effect = "Allow", Action = ["states:*"], Resource = "*" },
      { Sid = "Scheduler", Effect = "Allow", Action = ["scheduler:*"], Resource = "*" },
      { Sid = "Sns", Effect = "Allow", Action = ["sns:*"], Resource = "*" },
      { Sid = "Sqs", Effect = "Allow", Action = ["sqs:*"], Resource = "*" },
      { Sid = "Logs", Effect = "Allow", Action = ["logs:*"], Resource = "*" },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "github_actions_staging_deploy" {
  role       = aws_iam_role.github_actions_staging.name
  policy_arn = aws_iam_policy.github_actions_deploy.arn
}

resource "aws_iam_role_policy_attachment" "github_actions_production_deploy" {
  role       = aws_iam_role.github_actions_production.name
  policy_arn = aws_iam_policy.github_actions_deploy.arn
}

####################################################
# GitHub environments — branch-locked (stg -> staging, main -> production), with an optional
# required-reviewer gate on production. Same pattern as databricks-bundle-template's github.tf,
# and the actual reason this exists: staging deploys immediately on push, production waits for
# a human to click approve — without needing two different deploy mechanisms to get there.
####################################################
data "github_repository" "this" {
  full_name = "${var.github_owner}/${var.github_repository}"
}

resource "github_repository_environment" "staging" {
  repository  = data.github_repository.this.name
  environment = "staging"

  deployment_branch_policy {
    protected_branches     = false
    custom_branch_policies = true
  }
}

resource "github_repository_environment_deployment_policy" "staging" {
  repository     = data.github_repository.this.name
  environment    = github_repository_environment.staging.environment
  branch_pattern = "stg"
}

resource "github_repository_environment" "production" {
  repository  = data.github_repository.this.name
  environment = "production"

  deployment_branch_policy {
    protected_branches     = false
    custom_branch_policies = true
  }

  dynamic "reviewers" {
    for_each = var.production_approver_github_user_id != null ? [var.production_approver_github_user_id] : []
    content {
      users = [reviewers.value]
    }
  }
}

resource "github_repository_environment_deployment_policy" "production" {
  repository     = data.github_repository.this.name
  environment    = github_repository_environment.production.environment
  branch_pattern = "main"
}

# The role ARN isn't a secret — OIDC needs no credential value, just the ARN to ask STS for —
# so it's a plain environment variable the workflow reads, not a GitHub secret.
resource "github_actions_environment_variable" "staging_role_arn" {
  repository    = data.github_repository.this.name
  environment   = github_repository_environment.staging.environment
  variable_name = "AWS_ROLE_TO_ASSUME"
  value         = aws_iam_role.github_actions_staging.arn
}

resource "github_actions_environment_variable" "production_role_arn" {
  repository    = data.github_repository.this.name
  environment   = github_repository_environment.production.environment
  variable_name = "AWS_ROLE_TO_ASSUME"
  value         = aws_iam_role.github_actions_production.arn
}
