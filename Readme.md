# Argus — Real-Time Fraud Detection Pipeline

Argus is a streaming fraud-detection pipeline that ingests simulated e-commerce
transactions through Kafka, applies rule-based and windowed-aggregation fraud
checks in Spark Structured Streaming, and prints flagged events in real time.

This is the local-development phase of a larger design (see design doc) that
eventually adds a BigQuery sink, GKE deployment via the Spark Kubernetes
Operator, Terraform-provisioned infrastructure, CI/CD, and an ML scoring layer.
This phase focuses on getting the core stream-processing logic correct before
introducing cloud infrastructure.

## What it does right now

1. `event_generator.py` produces synthetic transaction events to a Kafka topic,
   partitioned by account ID so per-account ordering is preserved.
2. `fraud_detection_job.py` reads the stream and runs two independent checks:
   - Rule-based flags on individual transactions (high amount, card-not-present
     risk on large purchases)
   - Windowed velocity detection: flags an account with 3 or more transactions
     inside a 1-minute tumbling window, using a 2-minute watermark to bound how
     long the job waits for late-arriving events before closing a window

## Why these design choices

- Kafka over a plain queue: partition-level ordering per account is required
  for velocity detection to be meaningful.
- Two separate streaming queries instead of one: rule checks are stateless and
  can emit immediately; windowed aggregation is stateful and needs a watermark
  to bound memory use, so they have different output semantics.
- Watermark of 2 minutes: bounds how much state Spark keeps in memory for open
  windows, at the cost of dropping events that arrive later than that.

## Running locally

    docker compose up -d
    pip install -r requirements.txt
    python producer/event_generator.py

In a second terminal:

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_jobs/fraud_detection_job.py

## Current limitations

- Data is synthetic (Faker-generated), not real transaction data
- No persistence yet — output goes to the console, not a warehouse
- No ML scoring layer yet, rules only
- Not deployed — runs on a single machine via Docker Compose

## Planned next phases

- BigQuery sink for flagged events and raw archive
- Containerize the Spark job and deploy to GKE via the Spark Kubernetes Operator
- Terraform for all GCP infrastructure (VPC, GKE cluster, BigQuery datasets, IAM)
- GitHub Actions CI/CD: lint/test on PR, terraform plan/apply, build and deploy
  the job image on merge
- Scikit-learn model loaded via a Pandas UDF for a risk score layered on top
  of the rule engine
- Prometheus/Grafana monitoring, alerting on consumer lag and processing delay