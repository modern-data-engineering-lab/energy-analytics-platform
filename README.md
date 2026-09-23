# energy-analytics-platform

A batch AWS data pipeline (S3, Glue, Athena, orchestrated by Step Functions on an EventBridge
Scheduler) built on real electricity grid interruption data, not a synthetic or Kaggle dataset.
The data comes from the Ibadan Electricity Distribution Company (IBEDC) in Nigeria, originally
collected for a university thesis on grid reliability. This repo re-engineers that same dataset
and use case as a proper AWS batch pipeline: real ingestion, real data-quality handling, a
deployed ML classifier, and SQL analytics. The things a one-off Colab notebook doesn't have to
deal with.

## Problem

Every AWS batch pipeline needs the same handful of decisions made before any real work starts:
how raw data gets landed without losing information, how cleaning happens without silently
discarding rows that don't fit expectations, how a multi-step job gets orchestrated and alerted
on failure, and how to avoid rebuilding all of this from scratch for the next project. Most
example pipelines answer these questions against a dataset chosen because it's already clean,
which means the pipeline never has to prove it can handle data that isn't. This repo
intentionally skips that shortcut. The source data is a real utility company's internal
spreadsheet export, and it is exactly as messy as that implies.

## The real data

**Source:** IBEDC feeder interruption records for the Ibadan region, January through December
2021, covering 18 named 33KV feeders. The data was originally collected for a 2023 University
of Ibadan mechanical engineering thesis (*Energy Grid Optimization Using Deep Machine
Learning*) that compared XGBoost, ANN, and RNN-LSTM models for outage-type prediction and
computed MTTR/MTBF reliability metrics per feeder.

**What "real" actually meant, once inspected directly rather than taken on the thesis's word:**

- The 12 monthly files aren't clean data tables. They're internal utility workbooks with 6 to 8
  sheets each (`RAW MONTHLY`, `WORKING WEEKLY`, `SUMMARY OF...`, `REVENUE LOSS`, etc). Only
  `RAW MONTHLY` holds interruption records.
- The header row isn't at a fixed position. Two to three junk rows precede it (a title, an
  email contact, a region label), and the exact count varies slightly by file. It's located by
  content (the row containing `"Event No"`), not a hardcoded row index, and that approach was
  checked against all 12 files before being trusted.
- There are section-label rows embedded mid-sheet (for example `"33kV FEEDERS OUTAGES"`) that
  read as a data row with junk in the date column and blanks everywhere else. Not a header, not
  real data, just a divider that Excel-based analysis usually scrolls past without thinking
  about.
- **`Type of Outage`** has about 30 raw spelling and punctuation variants of what are really 9
  categories: `"EDC F/O"`, `"EDC/F/O"`, `"EDCF/O"`, `"EDC /F/O"` are all the same thing.
- **`Name of Affected Feeder`** has **165** raw variants of what are really the **18** named
  feeders this analysis covers, plus a genuinely separate category of transformer and
  substation level events (`"T-2B @ JERICHO"`, `"JERICHO COMPLEX"`) that are correctly not one
  of the 18 feeders. Not a typo of one, a different kind of asset entirely.
- About 1% of rows have unparseable or out-of-2021 dates, a mix of Excel-epoch artifacts
  (`1900-01-07`) and the embedded section-label rows above.
- The thesis's own MTTR/MTBF calculation has an acknowledged bug: it computed the gap between
  consecutive failure events across all 18 feeders combined, rather than per feeder, and got a
  nonsensical negative MTBF as a result. The thesis reports this as a known limitation but
  never fixes it.

None of this was assumed from the thesis's description. Every normalization rule below was
checked against the actual 6,609-row dataset before being written into a Glue script.

## How the cleaning was actually validated, not just written

The feeder-name and outage-type canonicalization rules were prototyped and tested against the
real data in a plain Python/pandas script before a single line went into the Glue job. The
habit throughout this repo is to verify against the actual data rather than trust what a source
document claims about itself.

**Outage type:** normalize by stripping whitespace and punctuation, then mapping the compact
form to one of 9 canonical categories. Result: 99.95% of rows matched cleanly. The remaining 3
rows (`"EDC /S"`) are genuinely ambiguous and were left as `OTHER` rather than guessed at.

**Feeder name:** normalize by stripping known noise tokens (`FDR`, `FEEDER`, voltage suffixes),
then match against the 18 canonical names, with a small, explicit dictionary of real observed
typos (`INDUSRIAL` to `INDUSTRIAL`, `M1NISTER` to `MINISTER`, `SAMANDA` to `SAMONDA`, and
others, see `etl/transforms.py`'s `KNOWN_FEEDER_TYPOS`) fixed by name, not a generic fuzzy
matcher. Every correction is auditable in code review instead of hidden behind an edit-distance
threshold. Result: 98.0% of rows mapped to a canonical feeder. The remaining 2.0% were checked
by hand, every unique raw value, and are all genuine transformer or substation events, not a
missed typo. That verification is what makes the "transformer event" category defensible
rather than a dumping ground for whatever didn't match.

This mapping had a real bug, caught by its own unit tests, not hypothetically. The first
version checked for `"AGODI"` plus a digit and returned `AGODI 1` or `AGODI 2`, which meant
`"T1 15MVA @ AGODI"` (a transformer at the Agodi substation) matched on the `"1"` in `"T1"` and
got mapped to the `AGODI 1` feeder, a different asset entirely. A test asserting that
transformer-designator strings stay `UNMAPPED` (`tests/test_transforms.py`) failed against
this, which is what caught it. It was fixed by checking for transformer indicators (`MVA`,
`JERICHO`, a `T` plus digit pattern) before substation-name matching, not after. The corrected
match rate is 98.0%, lower than the buggy version's 98.8%, because the fix now correctly
excludes 52 transformer-event rows the bug had silently absorbed into feeder categories. Lower,
but actually right, which is the number worth trusting.

**MTTR/MTBF:** recomputed correctly, per feeder, using a window function ordered by event
timestamp within each feeder's own event sequence. This deliberately fixes the thesis's
acknowledged cross-feeder-overlap bug rather than reproducing it. See
`etl/gold/gold_aggregate.py`.

## Architecture

```
 12 raw IBEDC        Glue: bronze_ingest      Glue: silver_transform     Glue: gold_aggregate      Glue: train_and_predict
 monthly .xlsx  ──▶  (land as-is, detect ──▶  (quarantine bad dates, ──▶ (MTTR/MTBF per       ──▶  (XGBoost, Python Shell,
 workbooks           header by content,       normalize outage type      feeder, classifier         not Spark, see "Why
 (S3 bronze-raw/)    preserve junk rows)       + feeder name)             feature table)              Glue Spark ETL...")
                            │                         │                         │                            │
                            ▼                         ▼                         ▼                            ▼
                      S3 bronze/               S3 silver/clean +         S3 gold/ (Parquet,          S3 models/ + gold/
                      (Parquet, by month)       silver/quarantine        Glue Catalog tables)         classifier_predictions
                                                 (Parquet, by month)              │                    (Glue Catalog table)
                                                                                   │                            │
                                                                                   ▼                            ▼
                                                                                         Athena SQL views
                                                                       (MTTR/MTBF leaderboard, outage-type summary,
                                                                              per-type classifier accuracy)

 Orchestration: Step Functions (bronze → silver → gold → classify, using .sync integration so
 each state waits for its Glue job to finish before the next starts) triggered by an
 EventBridge Scheduler, with a paired SQS DLQ for invocation failures and an SNS topic + email
 subscription for pipeline failures anywhere in the chain.
```

### Design decisions (the *why* behind the *what*)

**Why real IBEDC data instead of a public/synthetic dataset.** A synthetic dataset is always
exactly as clean as its generator intended. It never forces a pipeline to prove it can handle
data that doesn't fit the schema it was written for. Real utility data does that by default,
for free, and the cleaning logic above exists specifically because of it.

**Why one bucket with `bronze/`/`silver/`/`gold/` prefixes, not three buckets.** Fewer
resources to manage, and access control (bucket policies, encryption, versioning) is set once
instead of three times. The medallion layers are a data-organization concept, not a resource
boundary.

**Why Glue Spark ETL (PySpark) for a dataset this small.** At about 6,600 rows, this genuinely
doesn't need distributed compute. A Glue Python Shell job would run the same logic cheaper and
start faster. Spark ETL was used anyway to get real hands-on experience with the tool that
actually shows up on production-scale AWS data platforms, and because the same job definition
scales unmodified if a real IBEDC feed grew to cover all of Nigeria's distribution regions
instead of just Ibadan. Worth knowing this tradeoff exists, rather than pretending Spark was
the only reasonable choice here.

**Why quarantine bad dates instead of dropping them.** A `WHERE` clause that silently drops
non-2021 dates is indistinguishable, later, from a pipeline that's hiding a real bug. Writing
quarantined rows to their own table (`interruptions_quarantine`) means anyone can query exactly
what got excluded and why. Keep it, tag it, don't discard it.

**Why positional column mapping in bronze, not header-text matching.** The literal header text
varies slightly by file (`"Date  (For Carry Over Outage Only)"` with inconsistent whitespace).
Column *position* has been consistent across every one of the 12 files checked; header *text*
hasn't. Matching on the thing that's actually stable was a deliberate choice, not laziness.

**Why an explicit typo dictionary instead of fuzzy string matching for feeder names.** A
fuzzy-match (Levenshtein distance, etc.) would probably catch these same typos, but it would
also risk silently merging two genuinely different feeders that happen to have similar names,
and there'd be no way to review which corrections it made without re-deriving them. A short,
explicit `dict` is auditable in a code review in about ten seconds; a fuzzy-match threshold
isn't.

**Why XGBoost only for the classifier, not all three models from the thesis.** XGBoost was the
thesis's best-performing model (80%, 82.4% tuned) and the simplest of the three to deploy as a
single batch job: no GPU, no deep-learning framework, no epoch or architecture tuning to
reproduce. The full three-model comparison, hyperparameter grid search, and the thesis's
data-enrichment ambitions (weather data, maintenance history) belong to a deliberately separate
research track, not this repo. See "Scope" below.

**Why this repo's classifier reports about 66% accuracy, not the thesis's 80 to 82%.** This is
a real finding, not a bug: this pipeline's feature set
(`ml/outage_classifier/train_and_predict.py`) deliberately excludes `Nature/Cause of Outage`,
which the thesis's feature set included. Tested directly, same data, same split, same model,
the only difference being whether that one column is included as a feature, adding it back in
moves accuracy from 66.6% to 82.4%, a roughly 15.7-point jump from one column. (82.4% lands
almost exactly on the thesis's own tuned XGBoost result of 82.41%, a good sign the comparison
is apples to apples.) `Nature/Cause of Outage` and `Type of Outage` (the classification target)
are two granularities of the same underlying tag, assigned by the same person at data-entry
time for the same event. Using one to predict the other is closer to leakage than to genuine
independent signal, and a jump this large from adding it is consistent with that. The thesis
doesn't flag this risk. This repo's lower, feature-set-honest number is the one that's actually
load-bearing for whether operational metrics alone can predict outage type, which is the more
useful question for a grid operator deciding what instrumentation to invest in. (`Relay
Target`, also in the thesis's feature list, is excluded here for the separate reason that it's
a messy mixed-format field this pipeline doesn't clean.)

**Why a permission boundary on every IAM role.** The role's own policy says what it's
*intended* to do; the permission boundary is the actual ceiling on what it can *ever* do,
regardless of a mistake in that policy. Two independent layers have to both be wrong for an
over-permissioned role to actually matter, rather than trusting a single policy document to be
airtight.

**Why Step Functions + EventBridge Scheduler + a paired DLQ, not a cron-triggered Lambda
calling three Glue jobs in sequence.** Two different failure modes need two different safety
nets: the *scheduler* failing to even invoke the pipeline (caught by the DLQ) versus the
*pipeline itself* failing partway through (caught by Step Functions' `Catch` block, which
publishes to SNS). A single Lambda doing both jobs would conflate these into one failure mode
and lose the distinction.

**Why tags are minimal at the provider level and explicit per resource.** `default_tags` on the
provider holds one account-wide constant (`ManagedBy`), and every taggable resource sets its
own `Project` tag from a variable (`var.project_tag`) rather than relying on `default_tags` to
carry it. A couple of headline resources (the S3 bucket, the Athena workgroup) also get a
human-readable `Name`. Environment is deliberately never a tag here: `${var.env}` is already
threaded through every resource *name*, so a redundant `Environment` tag would just be one more
thing that could drift out of sync with the name it's describing.

## Stack

AWS (S3, Glue, Athena, Lambda, Step Functions, EventBridge Scheduler, SNS, SQS) · Terraform ·
GitHub Actions (OIDC) · XGBoost

## Repository layout

```
terraform/                  Platform layer: S3, IAM (with permission boundaries), Glue
                             database + jobs (incl. the classifier's Python Shell job), Step
                             Functions, EventBridge Scheduler + DLQ, SNS, Athena workgroup,
                             GitHub OIDC role + environments. config/{stg,prd}.hcl configures
                             the remote S3 state backend (see "CI/CD" below).
etl/transforms.py           Pure canonicalization logic (feeder name, outage type). No
                             pyspark/awsglue imports, unit-tested directly (tests/).
etl/bronze/bronze_ingest.py Lands the 12 raw monthly workbooks as-is; see its docstring for
                             the header-detection and junk-row handling this required.
etl/silver/silver_transform.py
                             Date quarantine + canonicalization (wraps etl/transforms.py as
                             Spark UDFs). Every rule checked against the real data first.
etl/gold/gold_aggregate.py  MTTR/MTBF per feeder (correctly, per-feeder) + the classifier
                             feature table.
ml/outage_classifier/train_and_predict.py
                             XGBoost training and batch inference. Glue Python Shell, not
                             Spark; see "Why Glue Spark ETL for a dataset this small" above.
athena/views/                MTTR/MTBF leaderboard, outage-type summary, per-type classifier
                             accuracy: plain SQL against the gold-layer Glue Catalog tables.
tests/test_transforms.py    Unit tests against etl/transforms.py directly. No Spark, no AWS.
```

## Getting Started

Assumes an AWS account with credentials configured (`aws sts get-caller-identity` succeeds),
Terraform installed, and the 12 raw IBEDC monthly `.xlsx` workbooks available locally (see "The
real data" above; this dataset is personally sourced, not bundled in the repo).

### 1. Deploy the platform

```bash
cd terraform
terraform init -backend-config=config/stg.hcl
terraform apply -var-file=stg.tfvars
```

### 2. Upload the raw data

Every output below is used by a specific step further down. That's deliberate, not decorative;
see the table at the end of this section.

```bash
aws s3 cp <local-folder-of-12-xlsx-files>/ "$(terraform output -raw raw_upload_prefix)" \
  --recursive --exclude "*" --include "*.xlsx"
```

### 3. Run the pipeline

```bash
aws stepfunctions start-execution \
  --state-machine-arn "$(terraform output -raw state_machine_arn)" \
  --name "manual-run-$(date +%s)"
```

Takes roughly 8 minutes end-to-end (mostly Glue job cold start: `G.1X` workers spin up fresh
each run; the data itself is a few thousand rows). Poll with:

```bash
aws stepfunctions describe-execution --execution-arn <execution-arn-from-above> --query status
```

`describe-execution`'s status is now trustworthy: `NotifyFailure` (the SNS-publish state a Glue
failure gets routed to) falls through to a `Fail` state, so a real pipeline failure reports
`FAILED` at the top level, not `SUCCEEDED`. That wasn't always true. See the Troubleshooting
notes below for how the earlier, misleading version of this state machine caught (and masked)
the first three real bugs in this pipeline, and why the executions from that period still show
`SUCCEEDED` in the console even though they weren't: Step Functions execution history is
immutable, so only runs made after the fix report correctly.

### 4. Register partitions and create the Athena views

`interruptions_clean`, `interruptions_quarantine`, and `classifier_features` are
Hive-partitioned by `source_month`; Athena needs partitions registered once per run before it
can see new data (`MSCK REPAIR TABLE`). `mttr_mtbf_by_feeder` and `classifier_predictions`
aren't partitioned and don't need this.

```bash
DB="$(terraform output -raw glue_database)"
WG="$(terraform output -raw athena_workgroup)"

for t in classifier_features interruptions_clean interruptions_quarantine; do
  aws athena start-query-execution --query-string "MSCK REPAIR TABLE $t" \
    --query-execution-context Database="$DB" --work-group "$WG"
done

for f in athena/views/*.sql; do
  sql=$(sed "s/{database}/$DB/" "$f" | grep -v '^--')
  aws athena start-query-execution --query-string "$sql" \
    --query-execution-context Database="$DB" --work-group "$WG"
done
```

### 5. Query the results

```bash
aws athena start-query-execution --query-string "SELECT * FROM mttr_mtbf_leaderboard LIMIT 10" \
  --query-execution-context Database="$DB" --work-group "$WG"
# then: aws athena get-query-results --query-execution-id <id-from-above>
```

Or, independent of Athena entirely, inspect the classifier's trained model and metrics
directly:

```bash
aws s3 cp "s3://$(terraform output -raw data_lake_bucket)/models/outage_classifier/metrics.json" -
```

### What each Terraform output is actually for

| Output | Used in |
|---|---|
| `raw_upload_prefix` | Step 2, where the raw monthly workbooks are uploaded |
| `state_machine_arn` | Step 3, triggers Bronze → Silver → Gold → Classify |
| `glue_database` | Steps 4 and 5, the Athena database every query runs against |
| `athena_workgroup` | Steps 4 and 5, where Athena queries execute and results land |
| `data_lake_bucket` | Step 5 (alt path), direct S3 inspection of model/metrics, without Athena |

## CI/CD

Two branches, two environments: pushing to `stg` deploys to staging, and pushing to `main`
(normally by merging a pull request) deploys to production. Both go through the same GitHub
Actions workflow and the same `terraform apply`, so staging is a genuine rehearsal of the exact
mechanism that later touches production, not a separately-tested path. Production additionally
requires a manual reviewer approval before the job runs.

Authentication is OpenID Connect, not long-lived AWS keys: each GitHub Actions job requests a
short-lived token from GitHub, exchanges it with AWS's `sts:AssumeRoleWithWebIdentity` for
temporary credentials scoped to one IAM role, and the role's trust policy only accepts tokens
whose subject claim names this exact repository and environment. Terraform provisions both IAM
roles, the OIDC provider itself, and the GitHub repository environments (with production's
reviewer requirement and each environment's branch lock) from `terraform/ci-cd.tf`.

### Troubleshooting notes (real errors hit standing up CI/CD)

- **`AssumeRoleWithWebIdentity` denied even though the trust policy looked right.** GitHub
  issues OIDC tokens with a subject claim in one of two formats depending on the workflow:
  `repo:owner/repo:environment:staging`, or a newer form that appends numeric owner and repo
  IDs, `repo:owner@<owner_id>/repo@<repo_id>:environment:staging`. The trust policy only
  matched the first. Diagnosed by adding a temporary step that decoded the actual token the
  failing workflow received, which showed the second format in use. Fixed by matching both
  patterns in the trust policy's `StringLike` condition.
- **`terraform apply` succeeds, then the next one hangs on a stuck state lock.** The native S3
  lock file needs a `DeleteObject` call to release, not just the `PutObject`/`GetObject` calls
  needed to acquire it. The deploy role's policy only granted the latter, so every apply left a
  lock object behind that had to be removed by hand before the next apply could run. Fixed by
  adding `s3:DeleteObject` to the state bucket policy statement.
- **The deploy role failed to read its own OIDC provider.** Terraform refreshes every resource
  in state on each apply, including the `aws_iam_openid_connect_provider` the role itself
  authenticates through. The role's IAM permissions were scoped to `role/*` and `policy/*` by
  naming convention, which didn't cover the `oidc-provider/*` resource type, so this one read
  failed even though every actual workload resource refreshed cleanly. Fixed by adding a policy
  statement scoped to the OIDC provider's own ARN.
- **`terraform apply` in CI fails immediately with a 401 on every GitHub API call.** The
  `github` Terraform provider, used here to manage the repository environments, branch
  policies, and environment variables, needs a `GITHUB_TOKEN` environment variable, the same as
  running Terraform locally. Nothing set one in the CI job. Fixed by adding a personal access
  token as a repository secret and mapping it to `GITHUB_TOKEN` in the deploy job's `env:`
  block. (The secret itself can't be named `GITHUB_TOKEN`, since GitHub reserves that prefix
  for secret names, so it's stored as `TF_GITHUB_TOKEN` and remapped in the workflow.)
- **Same provider, next attempt: 403 instead of 401.** Once the token was in place, Terraform
  authenticated but got `403 Resource not accessible by personal access token` specifically on
  the calls that read and write GitHub Actions environment variables. A fine-grained personal
  access token's "Environments" permission is a separate category from "Administration," and
  the token had only Contents and Metadata checked. Fixed by adding Administration,
  Environments, and Variables (all set to Read and write) to the token's repository
  permissions.

## Current build status

Being upfront about exactly where this stands, rather than implying more than what's actually
been run:

- ✅ **Terraform applied** to a real AWS account (`eu-north-1`), remote state in S3: S3, IAM
  (including the GitHub OIDC role and permission boundaries), Glue (including the classifier's
  Python Shell job and all five catalog tables), Step Functions, EventBridge Scheduler, SNS,
  Athena workgroup.
- ✅ **Full pipeline run end-to-end against live AWS**, including the three real bugs described
  in Troubleshooting. Bronze → Silver → Gold → Classify all genuinely completed (verified via
  `get-execution-history`, not just a top-level "SUCCEEDED" status). The classifier's actual
  live-run accuracy (66.7%) landed within 0.1 point of the standalone pandas/scikit-learn
  validation quoted throughout this README (66.6%), real confirmation that the Glue port of
  that logic matches the validated logic, not a coincidence.
- ✅ **Unit tests written and passing** (`tests/test_transforms.py`, 14 tests) against the pure
  canonicalization logic in `etl/transforms.py`, no Spark/AWS needed to run them. One of these
  tests caught a real bug in the feeder-name mapping before it ever reached AWS. See "How the
  cleaning was actually validated" above.
- ✅ **Athena SQL views created and queried against live data** (`athena/views/`): MTTR/MTBF
  leaderboard, outage-type summary, per-type classifier accuracy. All three return real,
  non-empty results (see Getting Started, step 5).
- ✅ **CI/CD deploys both environments for real.** Pushing to `stg` deploys to staging, and
  pushing to `main` deploys to production behind a required review, both through the same
  Terraform apply over OIDC. Verified against actual AWS resources (the IAM roles, the Glue
  jobs, the Step Functions state machine), not just a green check mark. See "CI/CD" above.

## Cost awareness

- **Glue jobs** are billed per DPU-hour with a 1-minute minimum. `G.1X` workers (the smallest
  standard type) at the minimum count (2) keep each run cheap; a dataset this size (a few
  thousand rows/year) never needs more.
- **S3 storage** is partitioned by `source_month` at every layer specifically so a future
  Athena query (or a re-run of just one month) doesn't have to scan the whole dataset. Partition
  pruning only works if the partition key is actually useful to filter on.
- **Athena workgroup** has a 1GB-per-query scan cap configured as a guard rail. This dataset
  will never come close to it, but it's the kind of limit that costs nothing to set now and
  prevents an expensive mistake later on a bigger dataset.
- **EventBridge Scheduler** runs weekly against what is, right now, a static 2021 archive,
  demonstrating the periodic-ingestion pattern a real live feed would use, not implying the
  underlying data actually changes week to week.

## What this demonstrates

- A real AWS batch pipeline built on data that wasn't chosen for being clean, with the cleaning
  decisions checked against reality and documented, not assumed from a source document's
  description of itself
- The bronze/silver/gold medallion pattern with a genuine quarantine layer, not just a
  three-folder naming convention
- Step Functions + EventBridge Scheduler + DLQ + SNS as a complete orchestration and alerting
  story
- IAM permission boundaries applied consistently, not just on the one role that happened to
  need it
- A CI/CD pipeline that deploys to staging and production through the same mechanism over
  OIDC, gated by branch and a required review, not just validated as YAML
- A real prior research result (the thesis's MTTR/MTBF calculation) checked, found to have a
  genuine bug, and fixed rather than reproduced

## Scope

The source thesis's full research depth (all three models: XGBoost, ANN, RNN-LSTM;
hyperparameter grid search; and the data-enrichment work with weather data and maintenance
history aimed at a journal submission) is deliberately not part of this repo. It belongs to a
separate research effort. Folding research-grade ML depth into this repo would work against the
point of it, which is to be a fast, self-contained proof of a real AWS batch pipeline built on
genuinely messy data. If that research work produces something later, it can reference this
repo's data-cleaning logic instead of duplicating it.
