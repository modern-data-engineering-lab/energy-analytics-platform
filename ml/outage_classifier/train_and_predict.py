"""
Outage-type classifier: XGBoost only (see README's "Why XGBoost only" — it was the thesis's
best-performing model and the simplest of the three to deploy as a single batch job).

Runs as a Glue **Python Shell** job, not Spark ETL — the gold feature table is a few thousand
rows, nowhere near needing distributed compute. Python Shell starts faster and costs less for
a job this size; see the README's "Why Glue Spark ETL for a dataset this small" note for the
same tradeoff applied to the bronze/silver/gold jobs, which went the other way for stack-
consistency reasons that don't apply here (there's no equivalent "XGBoost on Spark" story this
repo is trying to tell).

Trains on 80% of the clean, labeled feature rows, evaluates on the held-out 20%, and writes
three things to S3: the trained model, per-row predictions (actual vs predicted, for Athena to
query), and a metrics summary. "Labeled" excludes two categories deliberately:
  - is_transformer_event rows (not one of the 18 feeders this analysis covers)
  - outage_type_canonical == "OTHER" (the 3 genuinely-ambiguous rows from silver — you can't
    train a classifier to predict a label that itself isn't well-defined)
"""

import argparse
import io
import json
import re

import boto3
import pandas as pd
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier

FEATURE_COLUMNS = [
    "duration_hours_num",
    "load_loss_mw_num",
    "no_of_customers_restored_num",
    "customer_hours_interruption_num",
    "event_no_num",
]


PARTITION_RE = re.compile(r"/([^/=]+)=([^/]+)/")


def read_parquet_prefix(s3_client, bucket: str, prefix: str) -> pd.DataFrame:
    """gold/classifier_features/ is Hive-partitioned by source_month (a directory per month,
    e.g. source_month=APRIL/), which Spark strips from each file's own schema and reconstructs
    from the path on read. Plain pandas has no such Hive-partition awareness — reading each
    file directly leaves partition columns missing, so they're restored here from the S3 key.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    frames = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
            frame = pd.read_parquet(io.BytesIO(body))
            for partition_col, partition_val in PARTITION_RE.findall(key):
                if partition_col not in frame.columns:
                    frame[partition_col] = partition_val
            frames.append(frame)
    if not frames:
        raise RuntimeError(f"No Parquet files found under s3://{bucket}/{prefix}")
    return pd.concat(frames, ignore_index=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_bucket", required=True)
    args, _ = parser.parse_known_args()
    bucket = args.data_bucket

    s3 = boto3.client("s3")
    df = read_parquet_prefix(s3, bucket, "gold/classifier_features/")

    labeled = df[
        (~df["is_transformer_event"]) & (df["outage_type_canonical"] != "OTHER")
    ].copy()
    labeled = labeled.dropna(subset=FEATURE_COLUMNS + ["outage_type_canonical", "feeder_canonical"])

    feeder_dummies = pd.get_dummies(labeled["feeder_canonical"], prefix="feeder")
    X = pd.concat([labeled[FEATURE_COLUMNS], feeder_dummies], axis=1)

    label_encoder = LabelEncoder()
    y = label_encoder.fit_transform(labeled["outage_type_canonical"])

    X_train, X_test, y_train, y_test, idx_train, idx_test = train_test_split(
        X, y, labeled.index, test_size=0.2, random_state=42, stratify=y
    )

    model = XGBClassifier(
        objective="multi:softmax",
        num_class=len(label_encoder.classes_),
        max_depth=6,
        learning_rate=0.1,
        n_estimators=200,
        eval_metric="mlogloss",
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(
        y_test, y_pred, target_names=label_encoder.classes_, output_dict=True, zero_division=0
    )

    prediction_columns = ["feeder_canonical", "outage_type_canonical", "source_month"]
    predictions = labeled.loc[idx_test, prediction_columns].copy()
    predictions["predicted_outage_type"] = label_encoder.inverse_transform(y_pred)
    predictions["correct"] = (
        predictions["outage_type_canonical"] == predictions["predicted_outage_type"]
    )

    # XGBoost's save_model takes a file path, not a file-like object — write locally, then upload.
    model.save_model("/tmp/model.json")
    with open("/tmp/model.json", "rb") as f:
        s3.put_object(Bucket=bucket, Key="models/outage_classifier/model.json", Body=f.read())

    predictions_buf = io.BytesIO()
    predictions.to_parquet(predictions_buf, index=False)
    s3.put_object(
        Bucket=bucket,
        Key="gold/classifier_predictions/predictions.parquet",
        Body=predictions_buf.getvalue(),
    )

    metrics = {
        "accuracy": accuracy,
        "train_rows": len(X_train),
        "test_rows": len(X_test),
        "classes": list(label_encoder.classes_),
        "per_class": {
            cls: {
                "precision": report[cls]["precision"],
                "recall": report[cls]["recall"],
                "f1": report[cls]["f1-score"],
            }
            for cls in label_encoder.classes_
        },
    }
    s3.put_object(
        Bucket=bucket,
        Key="models/outage_classifier/metrics.json",
        Body=json.dumps(metrics, indent=2).encode(),
    )

    print(f"Accuracy: {accuracy:.4f} on {len(X_test)} held-out rows (train: {len(X_train)})")


if __name__ == "__main__":
    main()
