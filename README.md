# energy-analytics-platform

A batch AWS data pipeline — S3 → Glue → Athena, orchestrated by Step Functions on an
EventBridge Scheduler — built on **real electricity grid interruption data**, not a synthetic
or Kaggle dataset. The data is from the Ibadan Electricity Distribution Company (IBEDC) in
Nigeria, originally collected for a university thesis on grid reliability; this repo re-engineers
that same dataset and use case as a proper AWS batch pipeline: real ingestion, real data-quality
handling, a deployed ML classifier, and SQL analytics — the things a one-off Colab notebook
doesn't have to deal with, and a portfolio repo should.

## Problem

Every AWS batch pipeline needs the same handful of decisions made before any real work starts:
how is raw data landed without losing information, how does cleaning happen without silently
discarding rows that don't fit expectations, how does a multi-step job get orchestrated and
alerted on failure, and how do you avoid rebuilding all of this from scratch for the next
project. Most portfolio pipelines answer these questions against a dataset chosen *because*
it's already clean — which means the pipeline never has to prove it can handle data that
isn't. This repo intentionally doesn't get that shortcut: the source data is a real utility
company's internal spreadsheet export, and it is exactly as messy as that implies.

## The real data

**Source:** IBEDC feeder interruption records for the Ibadan region, January–December 2021 —
18 named 33KV feeders, originally collected for a 2023 University of Ibadan mechanical
engineering thesis (*Energy Grid Optimization Using Deep Machine Learning*) that compared
XGBoost/ANN/RNN-LSTM models for outage-type prediction and computed MTTR/MTBF reliability
metrics per feeder.

**What "real" actually meant, once inspected directly rather than taken on the thesis's word:**

- The 12 monthly files aren't clean data tables — they're internal utility workbooks with
  6–8 sheets each (`RAW MONTHLY`, `WORKING WEEKLY`, `SUMMARY OF...`, `REVENUE LOSS`, etc.).
  Only `RAW MONTHLY` holds interruption records.
- The header row isn't at a fixed position — 2–3 junk rows precede it (a title, an email
  contact, a region label), and the exact count varies slightly by file. Located by content
  (the row containing `"Event No"`), not a hardcoded row index — verified this holds across
  all 12 files before trusting it.
- There are section-label rows embedded **mid-sheet** (e.g. `"33kV FEEDERS OUTAGES"`) that
  read as a data row with junk in the date column and blanks everywhere else — not a header,
  not real data, just a divider that Excel-based analysis usually scrolls past without
  thinking about.
- **`Type of Outage`** has ~30 raw spelling/punctuation variants of what are really 9
  categories: `"EDC F/O"`, `"EDC/F/O"`, `"EDCF/O"`, `"EDC /F/O"` are all the same thing.
- **`Name of Affected Feeder`** has **165** raw variants of what are really the **18** named
  feeders this analysis covers, plus a genuinely separate category of transformer/substation
  -level events (`"T-2B @ JERICHO"`, `"JERICHO COMPLEX"`) that are correctly *not* one of the
  18 feeders — not a typo of one, a different kind of asset entirely.
- ~1% of rows have unparseable or out-of-2021 dates — a mix of Excel-epoch artifacts
  (`1900-01-07`) and the embedded section-label rows above.
- The thesis's own MTTR/MTBF calculation has an acknowledged bug: it computed the gap between
  consecutive failure events across *all 18 feeders combined*, rather than per feeder, and got
  a nonsensical **negative** MTBF as a result — reported in the thesis as a known limitation,
  not silently ignored, but never fixed.

None of this was assumed from the thesis's description — every normalization rule below was
validated against the actual 6,609-row dataset before being written into a Glue script.

## How the cleaning was actually validated, not just written

The feeder-name and outage-type canonicalization rules were prototyped and tested against the
real data in a plain Python/pandas script *before* a single line went into the Glue job — the
same "verify against reality, not against what the docs/thesis claim" discipline this
portfolio's Databricks repo used when debugging real platform bugs.

**Outage type:** normalize by stripping whitespace/punctuation and mapping the compact form to
one of 9 canonical categories. Result: **99.95%** of rows matched cleanly; the remaining 3 rows
(`"EDC /S"`) are genuinely ambiguous and were left as `OTHER` rather than guessed at.

**Feeder name:** normalize by stripping known noise tokens (`FDR`, `FEEDER`, voltage
suffixes), then match against the 18 canonical names — with a small, explicit dictionary of
**real observed typos** (`INDUSRIAL`→`INDUSTRIAL`, `M1NISTER`→`MINISTER`, `SAMANDA`→`SAMONDA`,
and others — see `etl/silver/silver_transform.py`'s `KNOWN_FEEDER_TYPOS`) fixed by name, not
via a generic fuzzy-matcher — every correction is auditable in code review instead of hidden
behind an edit-distance threshold. Result: **98.8%** of rows mapped to a canonical feeder.
The remaining 1.2% were checked **by hand, every unique raw value** — all genuine transformer
/substation events, not a missed typo. That verification is what makes the "transformer event"
category defensible rather than a dumping ground for whatever didn't match.

**MTTR/MTBF:** recomputed correctly, per feeder, using a window function ordered by event
timestamp *within each feeder's own event sequence* — deliberately fixing the thesis's
acknowledged cross-feeder-overlap bug rather than reproducing it. See
`etl/gold/gold_aggregate.py`.

## Architecture

```
 12 raw IBEDC          Glue: bronze_ingest        Glue: silver_transform       Glue: gold_aggregate
 monthly .xlsx    ──▶  (land as-is, detect   ──▶  (quarantine bad dates,  ──▶  (MTTR/MTBF per
 workbooks             header by content,         normalize outage type       feeder, classifier
 (S3 bronze-raw/)      preserve junk rows)         + feeder name)              feature table)
                              │                           │                          │
                              ▼                           ▼                          ▼
                        S3 bronze/                 S3 silver/clean +          S3 gold/ (Parquet,
                        (Parquet, by month)         silver/quarantine         Glue Catalog tables)
                                                     (Parquet, by month)              │
                                                                                       ▼
                                                                              Athena SQL views
                                                                          (MTTR/MTBF leaderboard,
                                                                           outage-type summary)

 Orchestration: Step Functions (bronze → silver → gold, .sync — waits for each Glue job to
 finish before the next starts) triggered by an EventBridge Scheduler, with a paired SQS DLQ
 for invocation failures and an SNS topic + email subscription for pipeline failures anywhere
 in the chain — no separate orchestrator needed, mirroring this portfolio's real AWS infra.
```

### Design decisions (the *why* behind the *what*)

**Why real IBEDC data instead of a public/synthetic dataset.** A synthetic dataset is always
exactly as clean as its generator intended — it never forces a pipeline to prove it can handle
data that doesn't fit the schema it was written for. Real utility data does that by default,
for free, and the cleaning logic above exists specifically because of it.

**Why one bucket with `bronze/`/`silver/`/`gold/` prefixes, not three buckets.** Fewer
resources to manage, and access control (bucket policies, encryption, versioning) is set once
instead of three times — the medallion layers are a data-organization concept, not a resource
boundary.

**Why Glue Spark ETL (PySpark) for a dataset this small.** At ~6,600 rows, this genuinely
doesn't need distributed compute — a Glue **Python Shell** job would run the same logic
cheaper and start faster. Spark ETL was chosen anyway for consistency with the stack this
repo is meant to demonstrate (`Glue ETL job (PySpark)` is the explicit ask), and because the
same job definition scales unmodified if a real IBEDC feed grew to cover all of Nigeria's
distribution regions instead of just Ibadan. Worth knowing this tradeoff exists, not pretending
Spark was the only reasonable choice here.

**Why quarantine bad dates instead of dropping them.** A `WHERE` clause that silently drops
non-2021 dates is indistinguishable, later, from a pipeline that's hiding a real bug. Writing
quarantined rows to their own table (`interruptions_quarantine`) means anyone can query exactly
what got excluded and why — the same "keep it, tag it, don't discard it" instinct as a proper
Lakeflow expectations `DROP` policy, just implemented in plain PySpark instead.

**Why positional column mapping in bronze, not header-text matching.** The literal header text
varies slightly by file (`"Date  (For Carry Over Outage Only)"` with inconsistent whitespace).
Column *position* has been consistent across every one of the 12 files checked; header *text*
hasn't. Matching on the thing that's actually stable was a deliberate choice, not laziness.

**Why an explicit typo dictionary instead of fuzzy string matching for feeder names.** A
fuzzy-match (Levenshtein distance, etc.) would probably catch these same typos, but it would
also risk silently merging two genuinely different feeders that happen to have similar names,
and there'd be no way to review *which* corrections it made without re-deriving them. A short,
explicit `dict` is auditable in a code review in about ten seconds; a fuzzy-match threshold
isn't.

**Why XGBoost only for the classifier, not all three models from the thesis.** XGBoost was the
thesis's best-performing model (80%, 82.4% tuned) *and* the simplest of the three to deploy as
a single batch job — no GPU, no deep-learning framework, no epoch/architecture tuning to
reproduce. The full three-model comparison, hyperparameter grid search, and the thesis's
data-enrichment ambitions (weather data, maintenance history) belong to a deliberately separate
research/publication track, not this repo — see "Where this fits in the portfolio" below.

**Why a permission boundary on every IAM role.** The role's own policy says what it's
*intended* to do; the permission boundary is the actual ceiling on what it can *ever* do,
regardless of a mistake in that policy. Belt-and-suspenders scoping, matching this portfolio's
real AWS infra convention rather than trusting a single policy document to be airtight.

**Why Step Functions + EventBridge Scheduler + a paired DLQ, not a cron-triggered Lambda
calling three Glue jobs in sequence.** Two different failure modes need two different safety
nets: the *scheduler* failing to even invoke the pipeline (caught by the DLQ) versus the
*pipeline itself* failing partway through (caught by Step Functions' `Catch` block, which
publishes to SNS). A single Lambda doing both jobs would conflate these into one failure mode
and lose the distinction.

## Stack

AWS (S3, Glue, Athena, Lambda, Step Functions, EventBridge Scheduler, SNS, SQS) · Terraform ·
XGBoost

## Repository layout

```
terraform/                  Platform layer: S3, IAM (with permission boundaries), Glue
                             database + jobs, Step Functions, EventBridge Scheduler + DLQ,
                             SNS, Athena workgroup. config/{stg,prd}.hcl for an optional
                             remote S3 backend (local state by default — see terraform/README.md).
etl/bronze/bronze_ingest.py Lands the 12 raw monthly workbooks as-is; see its docstring for
                             the header-detection and junk-row handling this required.
etl/silver/silver_transform.py
                             Date quarantine, outage-type + feeder-name canonicalization —
                             every rule validated against the real data before being written.
etl/gold/gold_aggregate.py  MTTR/MTBF per feeder (correctly, per-feeder) + the classifier
                             feature table.
ml/outage_classifier/       XGBoost training + batch inference (not yet built — see status
                             below).
athena/views/                Athena SQL views for the MTTR/MTBF leaderboard and outage-type
                             summary (not yet built — see status below).
```

## Current build status

Being upfront about exactly where this stands, rather than implying more than what's actually
been run:

- ✅ **Terraform written** — full platform layer (S3, IAM, Glue, Step Functions, EventBridge
  Scheduler, SNS, Athena). **Not yet applied** to the real AWS account.
- ✅ **Bronze/silver/gold ETL scripts written.** The cleaning *logic* (feeder/outage-type
  canonicalization, date quarantine) was validated standalone against the real 6,609-row
  dataset with plain pandas before being written into the Glue (PySpark) scripts — the
  percentages quoted above (99.95%, 98.8%) are from that standalone validation. **The actual
  Glue jobs have not yet been run** — PySpark-specific issues (the `awsglue`/Spark APIs behave
  differently from plain pandas in ways that only surface at runtime) haven't been ruled out
  yet.
- ⬜ **XGBoost classifier** — not yet built.
- ⬜ **Athena SQL views** — not yet built.
- ⬜ **End-to-end run against live AWS** — not yet done. Nothing in this repo has been deployed.

This README will be updated with real run results (and any bugs hit + fixed, the same
Troubleshooting-notes discipline as the rest of this portfolio) once the pipeline has actually
executed — not written retroactively to look cleaner than the process was.

## Cost awareness

- **Glue jobs** are billed per DPU-hour with a 1-minute minimum — `G.1X` workers (the
  smallest standard type) at the minimum count (2) keeps each run cheap; a dataset this size
  (a few thousand rows/year) never needs more.
- **S3 storage** is partitioned by `source_month` at every layer specifically so a future
  Athena query (or a re-run of just one month) doesn't have to scan the whole dataset —
  partition pruning only works if the partition key is actually useful to filter on.
- **Athena workgroup** has a 1GB-per-query scan cap configured as a guard rail — this dataset
  will never come close to it, but it's the kind of limit that costs nothing to set now and
  prevents an expensive mistake later on a bigger dataset.
- **EventBridge Scheduler** runs weekly against what is, right now, a static 2021 archive —
  demonstrating the periodic-ingestion pattern a real live feed would use, not implying the
  underlying data actually changes week to week.

## What this demonstrates

- A real AWS batch pipeline built on data that wasn't chosen for being clean — with the
  cleaning decisions validated against reality and documented, not assumed from a source
  document's description of itself
- The bronze/silver/gold medallion pattern with a genuine quarantine layer, not just a
  three-folder naming convention
- Step Functions + EventBridge Scheduler + DLQ + SNS as a complete orchestration and alerting
  story, matching a real production AWS pattern
- IAM permission boundaries applied consistently, not just on the one role that happened to
  need it
- A real prior research result (the thesis's MTTR/MTBF calculation) checked, found to have a
  genuine bug, and fixed rather than reproduced

## Where this fits in the portfolio

This is repo #3 in `modern-data-engineering-lab`, and — like `databricks-bundle-template`
before it — most of the value here is in decisions other repos in this portfolio can reuse
directly: the permission-boundary IAM pattern, the Step Functions + Scheduler + DLQ
orchestration shape, and the "quarantine, don't silently drop" approach to real-world data
quality all carry forward to `finance-lakehouse-platform` and any other AWS-touching repo
built after this one.

**Deliberately out of scope here, and why:** the source thesis's full research depth — all
three models (XGBoost/ANN/RNN-LSTM), hyperparameter grid search, and the data-enrichment work
(weather data, maintenance history) aimed at a journal submission — is being tracked as a
**fully separate project outside `modern-data-engineering-lab` entirely**, not a folder in this
repo. Cramming research-grade ML depth into a repo whose entire point is being the *fast,
self-contained* proof of AWS range would blow the reason this repo exists. If that research
track produces something later, it'll live on its own, referencing this repo's data-cleaning
work rather than duplicating it.
