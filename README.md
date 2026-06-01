# Real-time Sentiment Analysis Inference Pipeline on Google Cloud Platform

> **Author:** Arepalli Venkata Lakshmi
> **Email:** arepallivenkatasowjanya20@gmail.com
> **Domain:** Data Science | **Difficulty:** Intermediate
> **Stack:** Python 3.9+, NLTK VADER, Google Cloud Pub/Sub, Google Cloud Functions, Cloud Logging
> **GCP Project ID:** avian-current-498109-b0
> **Region:** us-central1

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture Diagram](#2-architecture-diagram)
3. [Repository Structure](#3-repository-structure)
4. [Prerequisites](#4-prerequisites)
5. [GCP Project Setup & API Enablement](#5-gcp-project-setup--api-enablement)
6. [GCP Authentication](#6-gcp-authentication)
7. [Cloud Pub/Sub Configuration](#7-cloud-pubsub-configuration)
8. [Local Environment Setup](#8-local-environment-setup)
9. [Cloud Function Deployment](#9-cloud-function-deployment)
10. [Running the Publisher](#10-running-the-publisher)
11. [Verifying Results in Cloud Logging](#11-verifying-results-in-cloud-logging)
12. [Running Unit Tests](#12-running-unit-tests)
13. [IAM Permissions](#13-iam-permissions)
14. [Environment Variables Reference](#14-environment-variables-reference)
15. [Design Decisions](#15-design-decisions)
16. [Common Mistakes & Troubleshooting](#16-common-mistakes--troubleshooting)

---

## 1. Project Overview

This project implements a **scalable, event-driven, real-time sentiment analysis inference pipeline** on Google Cloud Platform (GCP).

### What it does

| Step | Component | Description |
|------|-----------|-------------|
| 1 | `publisher.py` | Reads text entries from `sample_texts.json` and publishes each one as a JSON message to a Cloud Pub/Sub topic |
| 2 | **Cloud Pub/Sub** | Acts as a durable, scalable message broker decoupling the producer from the consumer |
| 3 | **Cloud Function** | Automatically triggered per message; decodes the payload, runs VADER sentiment analysis, and logs results |
| 4 | **Cloud Logging** | Stores all inference results and error events for observability |

### Why this architecture?

- **Serverless** — No server provisioning; GCP auto-scales Cloud Functions on demand.
- **Event-driven** — Processing is triggered only when new data arrives; zero idle cost.
- **Decoupled** — Publisher and consumer are independent; either can be replaced without breaking the other.
- **Observable** — All events (success + failure) are recorded in Cloud Logging with severity levels.

---

## 2. Architecture Diagram
+------------------------------------------------------------------+
|                        LOCAL MACHINE                             |
|                                                                  |
|   sample_texts.json                                              |
|         |                                                        |
|         v                                                        |
|   publisher.py  ----  google-cloud-pubsub  -------------------> |
+------------------------------------------------------------------+
|
| HTTPS / gRPC
v
+------------------------------------------------------------------+
|                   GOOGLE CLOUD PLATFORM                          |
|                   Project: avian-current-498109-b0               |
|                                                                  |
|  +---------------------------+                                   |
|  |   Cloud Pub/Sub           |                                   |
|  |                           |                                   |
|  |  Topic: sentiment-input   |<--- publisher.py publishes msgs   |
|  |            |              |                                   |
|  |  Subscription:            |                                   |
|  |  sentiment-input-         |                                   |
|  |  subscription             |                                   |
|  +------------+--------------+                                   |
|               |  push trigger (one msg per invocation)          |
|               v                                                  |
|  +--------------------------------------------------+            |
|  |  Cloud Function: sentiment-analyzer-function     |            |
|  |  Region: us-central1                             |            |
|  |                                                  |            |
|  |  process_pubsub_message(event, context)          |            |
|  |        |                                         |            |
|  |        |-- Base64 decode event['data']           |            |
|  |        |-- Parse JSON payload                    |            |
|  |        |-- Extract "text" field                  |            |
|  |        |-- analyze_sentiment(text) <-- VADER     |            |
|  |        +-- Log result / error                    |            |
|  +------------------------------+-------------------+            |
|                                 |                                |
|                                 v                                |
|  +--------------------------------------------------+            |
|  |   Cloud Logging                                  |            |
|  |                                                  |            |
|  |  INFO : {"text": "...",                          |            |
|  |          "sentiment_label": "POSITIVE",          |            |
|  |          "sentiment_score": 0.92}                |            |
|  |  WARN : missing 'text' field                     |            |
|  |  ERROR: invalid JSON / unexpected exception      |            |
|  +--------------------------------------------------+            |
+------------------------------------------------------------------+

### Data Flow (step-by-step)
[publisher.py]
|
|  1. Read entries from sample_texts.json
|  2. json.dumps(entry).encode("utf-8")
|  3. publisher.publish(topic_path, data_bytes)
|
v
[Cloud Pub/Sub - sentiment-input topic]
|
|  4. Message stored durably (7-day retention)
|  5. Fan-out to all subscriptions
|
v
[Cloud Function - process_pubsub_message]
|
|  6.  base64.b64decode(event["data"])
|  7.  json.loads(message_data)
|  8.  text = message_json["text"]
|  9.  result = analyze_sentiment(text)  VADER compound score
|  10. logger.info(json.dumps(result))
|
v
[Cloud Logging]
|
|  11. Log entry visible in GCP Console -> Logging -> Log Explorer
+--> query: resource.type="cloud_function"

---

## 3. Repository Structure
sentiment-pipeline/
+-- sentiment_analyzer/          # Cloud Function source
|   +-- main.py                  # Function logic + analyze_sentiment()
|   +-- requirements.txt         # Cloud Function dependencies
+-- tests/
|   +-- init.py
|   +-- test_sentiment.py        # Unit tests (91% line coverage)
+-- screenshots/                 # GCP Console screenshots
+-- publisher.py                 # Pub/Sub message publisher
+-- publisher_requirements.txt   # Publisher dependencies
+-- sample_texts.json            # Sample input data
+-- .env.example                 # Environment variables reference
+-- README.md                    # This file

---

## 4. Prerequisites

| Requirement | Minimum Version |
|-------------|----------------|
| Python | 3.9+ |
| Google Cloud SDK (gcloud) | Latest |
| A GCP account | avian-current-498109-b0 |
| Git | 2.x |

Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install

---

## 5. GCP Project Setup & API Enablement

### 5.1 Project Details

| Setting | Value |
|---------|-------|
| Project ID | avian-current-498109-b0 |
| Project Name | My Project 3003 |
| Region | us-central1 |
| Pub/Sub Topic | sentiment-input |
| Subscription | sentiment-input-subscription |

### 5.2 Set the active project

```bash
gcloud config set project avian-current-498109-b0
```

### 5.3 Enable required APIs

```bash
gcloud services enable pubsub.googleapis.com
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable logging.googleapis.com
```

---

## 6. GCP Authentication

### 6.1 Authenticate interactively

```bash
gcloud auth login
gcloud auth application-default login
gcloud auth application-default set-quota-project avian-current-498109-b0
```

### 6.2 Verify active account

```bash
gcloud auth list
gcloud config get-value project
```

Expected output:
avian-current-498109-b0

---

## 7. Cloud Pub/Sub Configuration

### 7.1 Create the topic

```bash
gcloud pubsub topics create sentiment-input
```

### 7.2 Create the pull subscription

```bash
gcloud pubsub subscriptions create sentiment-input-subscription \
  --topic=sentiment-input \
  --ack-deadline=10 \
  --message-retention-duration=604800s
```

### 7.3 Verify

```bash
gcloud pubsub topics list
gcloud pubsub subscriptions list
```

Expected output:
name: projects/avian-current-498109-b0/topics/sentiment-input

---

## 8. Local Environment Setup

### 8.1 Clone the repository

```bash
git clone https://github.com/arepallivenkatasowjanya20/sentiment-pipeline.git
cd sentiment-pipeline
```

### 8.2 Create and activate virtual environment

```bash
python -m venv venv
source venv/Scripts/activate
```

### 8.3 Install publisher dependencies

```bash
pip install -r publisher_requirements.txt
```

### 8.4 Install test dependencies

```bash
pip install pytest pytest-cov nltk
```

### 8.5 Download NLTK VADER lexicon

```bash
python -c "import nltk; nltk.download('vader_lexicon')"
```

### 8.6 Set environment variables

```bash
export GCP_PROJECT_ID="avian-current-498109-b0"
export PUB_SUB_TOPIC_ID="sentiment-input"
```

---

## 9. Cloud Function Deployment

### 9.1 Navigate to function source

```bash
cd sentiment_analyzer/
```

### 9.2 Deploy the function

```bash
gcloud functions deploy sentiment-analyzer-function \
  --runtime python39 \
  --trigger-topic sentiment-input \
  --entry-point process_pubsub_message \
  --memory 256MB \
  --region us-central1 \
  --set-env-vars GCP_PROJECT_ID=avian-current-498109-b0 \
  --source .
```

### 9.3 Verify deployment

```bash
gcloud functions describe sentiment-analyzer-function --region us-central1
```

Look for:
status: ACTIVE

### 9.4 Test the function locally

```bash
pip install functions-framework
functions-framework --target process_pubsub_message --port 8080
```

In a second terminal:

```bash
curl -X POST http://localhost:8080 \
  -H "Content-Type: application/json" \
  -d "{\"data\": \"$(echo -n '{\"id\": \"test\", \"text\": \"This product is great\"}' | base64)\"}"
```

---

## 10. Running the Publisher

```bash
export GCP_PROJECT_ID="avian-current-498109-b0"
export PUB_SUB_TOPIC_ID="sentiment-input"
python publisher.py
```

Expected output:
[INFO] Publishing 5 message(s) to topic: projects/avian-current-498109-b0/topics/sentiment-input
[INFO] Published message ID: 1234567890  | Payload: {"id": "1", "text": "This product is absolutely amazing. Highly recommend."}
[INFO] Published message ID: 2345678901  | Payload: {"id": "2", "text": "The customer service was terrible and I am very disappointed."}
[INFO] Published message ID: 3456789012  | Payload: {"id": "3", "text": "Its okay, nothing special but gets the job done."}
[INFO] Published message ID: 4567890123  | Payload: {"id": "4", "text": "The quality exceeded my expectations, truly fantastic"}
[INFO] Published message ID: 5678901234  | Payload: {"id": "5", "text": "Never buying from this brand again, complete waste of money."}
[INFO] All messages published successfully.

---

## 11. Verifying Results in Cloud Logging

### 11.1 Via GCP Console

1. Go to GCP Console -> Logging -> Log Explorer
2. Use this query:
resource.type="cloud_function"
resource.labels.function_name="sentiment-analyzer-function"
severity>=INFO

3. Expected log entries:

```json
{
  "textPayload": "Sentiment analysis result: {\"text\": \"This product is absolutely amazing\", \"sentiment_label\": \"POSITIVE\", \"sentiment_score\": 0.6588}",
  "severity": "INFO"
}
```

### 11.2 Via gcloud CLI

```bash
gcloud logging read \
  "resource.type=cloud_function AND resource.labels.function_name=sentiment-analyzer-function" \
  --limit 20 \
  --format json
```

---

## 12. Running Unit Tests

```bash
python -m pytest tests/ -v --cov=sentiment_analyzer --cov-report=term-missing
```

Expected output:
36 passed, 6 subtests passed
Coverage: 91%

To run via unittest:

```bash
python -m unittest discover -s tests -v
```

---

## 13. IAM Permissions

| Role | Purpose |
|------|---------|
| roles/pubsub.subscriber | Read and acknowledge Pub/Sub messages |
| roles/logging.logWriter | Write logs to Cloud Logging |

```bash
SA_EMAIL="731690254412-compute@developer.gserviceaccount.com"

gcloud projects add-iam-policy-binding avian-current-498109-b0 \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/pubsub.subscriber"

gcloud projects add-iam-policy-binding avian-current-498109-b0 \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/logging.logWriter"
```

---

## 14. Environment Variables Reference

| Variable | Value | Used In |
|----------|-------|---------|
| GCP_PROJECT_ID | avian-current-498109-b0 | publisher.py, Cloud Function |
| PUB_SUB_TOPIC_ID | sentiment-input | publisher.py |
| GOOGLE_APPLICATION_CREDENTIALS | path/to/key.json | Local development |

---

## 15. Design Decisions

### Model Choice: NLTK VADER

- **Lightweight** — No GPU required; fits in 256MB Cloud Function memory
- **No cold-start penalty** — Loaded globally outside handler function
- **Accurate** — Purpose-built for social media and review text
- **No external download at runtime** — Lexicon bundled with NLTK

### Global Model Initialisation

The SentimentIntensityAnalyzer instance is created at module level outside process_pubsub_message. This ensures it is reused across warm invocations, reducing per-request latency significantly.

### Error Handling Strategy

| Error Type | Log Level |
|------------|-----------|
| Missing data field | ERROR |
| Invalid JSON | ERROR |
| Missing text field | WARNING |
| Invalid text type or empty | ERROR |
| Unexpected exception | ERROR |

---

## 16. Common Mistakes & Troubleshooting

| Problem | Solution |
|---------|----------|
| PERMISSION_DENIED on publish | Run gcloud auth application-default login |
| Cloud Function not triggering | Confirm trigger-topic matches exact topic name |
| No module named nltk | Ensure nltk>=3.8.1 is in requirements.txt |
| Logs not appearing | Check function region and filter by function name |
| sample_texts.json not found | Run publisher.py from project root directory |
| Cold start latency | Model is initialised globally - already optimised |