# Policy Reviewer – S3 → Textract → S3 (Lambda-Only)

A production-ready pipeline to auto-ingest policy PDFs, run Amazon Textract, and persist results with full auditability.

## 📌 Objectives & Guarantees

**Trigger on arrival:** As soon as a new PDF lands in `s3://Policyreviewer-bucket/policy/pdf/`, the system:

- Detects the object (S3 event)
- Starts an async Textract job
- Receives completion via SNS → Lambda
- Persists raw pages + a manifest to `policy/textract-output/`
- Writes structured logs and an S3-backed audit trail

### Constraints

- **Lambda-only orchestration** (no EventBridge, no Step Functions, no VPC)
- **No Secrets Manager** (use `.env` locally, mirror as Lambda env vars)
- **Microservices mode** (thin Lambdas per stage; monolith optional)
- **Code style ready** (OOP, typed exceptions, structured logging, docstrings)

## 🧭 End-to-End Architecture

```
+-------------------------------------------------------------+
|                         AWS Account                          |
|  Region: us-east-1                                          |
+--------------------------+----------------------------------+
                           |
                           | 1) S3:ObjectCreated (PUT)
                           v
+--------------------------+----------------------------------+
|  S3 Bucket: Policyreviewer-bucket                           |
|  Prefixes:                                                  |
|   - policy/pdf/            <-- drop PDFs here               |
|   - policy/textract-output/ <-- results & manifests         |
|   - policy/audit/          <-- JSONL audit trail            |
+--------------------------+----------------------------------+
                           |
                           | invokes
                           v
+--------------------------+----------------------------------+
|  Lambda: policy_ingest_lambda                               |
|  - Validate key under policy/pdf/ & is .pdf                  |
|  - Start Textract (async) with NotificationChannel (SNS)     |
|  - Audit: stage=ingest_start, job_id, key, mode              |
+--------------------------+----------------------------------+
                           |
                           | async (Textract internal run)
                           v
+--------------------------+----------------------------------+
|  Amazon Textract                                            |
|  - StartDocumentTextDetection / StartDocumentAnalysis       |
|  - On completion -> publish to SNS topic (JobId, Status)    |
+--------------------------+----------------------------------+
                           |
                           | publish
                           v
+--------------------------+----------------------------------+
|  Amazon SNS: textract-complete-topic                        |
|  - Subscription: Lambda: policy_callback_lambda             |
+--------------------------+----------------------------------+
                           |
                           | invokes
                           v
+--------------------------+----------------------------------+
|  Lambda: policy_callback_lambda                             |
|  - Parse SNS -> JobId, Status, (source key best effort)     |
|  - Get all pages (paginated)                                |
|  - Persist pages + index.json under policy/textract-output/ |
|  - Audit: stage=ingest_complete, success/fail, output_path  |
+--------------------------+----------------------------------+
                           |
                           v
+--------------------------+----------------------------------+
|  CloudWatch Logs & Metrics                                  |
|  - JSON logs with stage/key/job_id/status                   |
|  - Filters & Alarms (optional)                              |
+--------------------------+----------------------------------+
```

**Why SNS is OK:** It's the standard Textract completion channel, not an orchestrator; it simply delivers the completion message to your Lambda—exactly within the "Lambda-only" design.

## 🗂 Folder Structure

```
repo-root/
├─ .env                                # local only; DO NOT commit
├─ pyproject.toml                      # uv-managed; add deps/scripts here
├─ README.md                           # this file
├─ logs/
│  └─ app.log                          # local runs (CloudWatch in Lambda)
├─ data/
│  └─ policy/
│     ├─ pdf/                          # local samples
│     └─ textract-output/              # local test outputs (optional)
├─ scripts/
│  ├─ run_local_ingest.sh              # optional: local test stub
│  ├─ init.py                          # optional: bootstrap helpers
│  └─ check_duplicates.py              # optional: sample dedup tool
└─ src/
   └─ policy_reviewer_agent/
      └─ policy_ingestion/
         ├─ __init__.py
         ├─ core/
         │  ├─ __init__.py
         │  ├─ settings.py             # env loader/validator
         │  ├─ logging_config.py       # JSON logs in CloudWatch format
         │  └─ exceptions.py           # typed exceptions
         ├─ services/
         │  ├─ __init__.py
         │  ├─ s3_client.py            # put_json, put_bytes, copy
         │  ├─ textract_client.py      # start async, get results
         │  ├─ result_persistor.py     # persist pages + index.json
         │  ├─ audit.py                # JSONL audit writer
         │  └─ file_utils.py           # key/mime validation helpers
         ├─ orchestrators/
         │  ├─ __init__.py
         │  └─ policy_pipeline.py      # coordinates ingest/callback
         └─ lambda_handlers/
            ├─ __init__.py
            ├─ policy_ingest_lambda.py
            └─ policy_callback_lambda.py
```

## 🔧 Environment & Configuration

Set these both in Lambda (Console → Configuration → Environment variables) and locally in `.env` (for testing):

| Key | Example | Required | Notes |
|-----|---------|----------|-------|
| `AWS_REGION` | `us-east-1` | ✅ | Region for Textract |
| `S3_BUCKET` | `Policyreviewer-bucket` | ✅ | Single bucket for input/output/audit |
| `POLICY_PDF_PREFIX` | `policy/pdf/` | ✅ | Input prefix (S3 event wired to this) |
| `POLICY_OUTPUT_PREFIX` | `policy/textract-output/` | ✅ | Output prefix for results/manifests |
| `TEXTRACT_SNS_TOPIC_ARN` | `arn:aws:sns:us-east-1:123456789012:textract-...` | ✅* | Required for async with callback |
| `TEXTRACT_PUBLISH_ROLE_ARN` | `arn:aws:iam::123456789012:role/TextractPublishRole` | ✅* | Role Textract assumes to publish to SNS |
| `LOG_LEVEL` | `INFO` | ❌ | DEBUG for dev |

\* Required for the microservices async mode (recommended).

## 📦 Installation (Local) — uv

```bash
# from repo-root
uv venv
uv pip install -U pip
uv add boto3 python-dotenv
```

### Optional pyproject.toml entry points

If you want local callable scripts:

```toml
[project.scripts]
policy-ingest-local = "policy_reviewer_agent.policy_ingestion.lambda_handlers.policy_ingest_lambda:handler"
policy-callback-local = "policy_reviewer_agent.policy_ingestion.lambda_handlers.policy_callback_lambda:handler"
```

## 🔐 IAM – Minimal Policies

### Lambda Execution Roles

Attach where applicable:

#### S3 (scope to your bucket & prefixes)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::Policyreviewer-bucket"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:CopyObject"],
      "Resource": "arn:aws:s3:::Policyreviewer-bucket/*"
    }
  ]
}
```

#### Textract

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "textract:StartDocumentTextDetection",
        "textract:StartDocumentAnalysis",
        "textract:GetDocumentTextDetection",
        "textract:GetDocumentAnalysis"
      ],
      "Resource": "*"
    }
  ]
}
```

#### CloudWatch Logs (standard)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    }
  ]
}
```

### Textract Publish Role

Assumed by Textract to publish to SNS:

#### Trust Policy (principal Textract)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"Service": "textract.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }
  ]
}
```

#### Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["sns:Publish"],
      "Resource": "arn:aws:sns:us-east-1:123456789012:textract-complete-topic"
    }
  ]
}
```

## 🔗 Wiring the Triggers

### S3 → Lambda (ingest)

- **Bucket:** `Policyreviewer-bucket`
- **Event:** PUT
- **Prefix:** `policy/pdf/`
- **Target:** `policy_ingest_lambda`

Add invoke permission:

```bash
aws lambda add-permission \
  --function-name policy_ingest_lambda \
  --statement-id s3invoke \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::Policyreviewer-bucket
```

### SNS (Textract complete) → Lambda (callback)

- **Topic:** `textract-complete-topic`

Subscribe the Lambda:

```bash
aws sns subscribe \
  --topic-arn <topic-arn> \
  --protocol lambda \
  --notification-endpoint arn:aws:lambda:us-east-1:<acct-id>:function:policy_callback_lambda
```

Allow SNS to invoke:

```bash
aws lambda add-permission \
  --function-name policy_callback_lambda \
  --statement-id snsinvoke \
  --action lambda:InvokeFunction \
  --principal sns.amazonaws.com \
  --source-arn <topic-arn>
```

### Textract Async with NotificationChannel

Ensure `TEXTRACT_SNS_TOPIC_ARN` and `TEXTRACT_PUBLISH_ROLE_ARN` are set in Lambda env vars. The code will pass them when starting the async job.

## 🧪 Testing & Smoke Checks

### Upload a file

```bash
aws s3 cp sample.pdf s3://Policyreviewer-bucket/policy/pdf/sample.pdf
```

### Watch logs (CloudWatch)

- `policy_ingest_lambda` should log `ingest_start` with a `job_id`
- After Textract completes, `policy_callback_lambda` logs `ingest_complete`

### Validate outputs

```
policy/textract-output/<UTCSTAMP>/<JOBID>/pages/page_0001.json
policy/textract-output/<UTCSTAMP>/<JOBID>/index.json
```

### Audit trail

```
policy/audit/YYYY/MM/DD/events.jsonl
```

Contains one-line JSON entries.

## 🔍 Observability & Audit

### Structured Logs (JSON)

Include keys: `stage`, `key`, `job_id`, `status`.

Use CloudWatch Metric Filters to count successes/failures and alert.

### Audit (S3 JSONL)

Example entry:

```json
{
  "ts": "2025-10-03T05:05:05.000Z",
  "stage": "ingest_start|ingest_complete",
  "key": "policy/pdf/sample.pdf",
  "status": "STARTED|SUCCESS|FAILED",
  "job_id": "abcd1234...",
  "mode": "text|analysis",
  "output_prefix": "policy/textract-output/"
}
```

## 🧱 Idempotency, Retries, Timeouts

### Idempotency

The pipeline is event-driven; to avoid reprocessing, track `(object_key, etag)` in audit or a small KV (optional).

### Retries

AWS will retry SNS→Lambda on transient errors. Keep handlers idempotent.

### Timeouts

- `policy_ingest_lambda`: 1–3 min (it only starts jobs)
- `policy_callback_lambda`: 2–5 min (fetch/persist results)

## 🧩 Modes (When to choose what)

### Asynchronous (recommended)

Large/multi-page PDFs; durable; no Lambda polling.

Uses `StartDocumentTextDetection` / `StartDocumentAnalysis` + SNS callback.

### Synchronous (optional/monolith)

Small files, immediate return; may hit timeouts on multi-page PDFs.

(If you need, add a monolith handler that polls `GetDocument*` in-Lambda.)

## 🛡️ Security Notes

- No Secrets Manager per your rule; keep `.env` local only, mirror keys as Lambda env vars
- Least privilege IAM on S3/Textract/SNS/Logs
- Do not commit `.env` or sensitive data
- Prefer scoped resource ARNs for S3 and SNS instead of `*` where practical

## 🧰 Troubleshooting Runbook

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| Ingest Lambda never fires | S3 event not attached or missing permission | Re-add S3 → Lambda event notification and add-permission |
| SNS callback never fires | Missing NotificationChannel or publish role | Ensure `TEXTRACT_SNS_TOPIC_ARN` & `TEXTRACT_PUBLISH_ROLE_ARN` set; publish role trust = `textract.amazonaws.com` |
| Callback reports non-SUCCEEDED | Textract job failed or partial | Inspect CloudWatch logs; verify input PDF, retry; consider DLQ or error topic |
| AccessDenied on S3 | IAM policy scope too tight | Add `s3:GetObject`/`PutObject`/`ListBucket` on your bucket/prefixes |
| Key validation fails | Wrong prefix or not a `.pdf` | Ensure files are under `policy/pdf/` and end with `.pdf` |

## 🚀 Deployment Steps (Console-first)

1. **Create/verify bucket & prefixes:**
   - `Policyreviewer-bucket/policy/pdf/`
   - `Policyreviewer-bucket/policy/textract-output/`
   - `Policyreviewer-bucket/policy/audit/`

2. **Create SNS topic** `textract-complete-topic`

3. **Create IAM roles:**
   - `policy_ingest_lambda_role` (S3, Textract, Logs)
   - `policy_callback_lambda_role` (S3, Textract, Logs)
   - `TextractPublishRole` (trust: Textract; permission: `sns:Publish` to your topic)

4. **Create Lambdas:**
   - `policy_ingest_lambda` (upload package from `src/…`), set env vars
   - `policy_callback_lambda` (upload package), set env vars

5. **Wire events:**
   - S3 Event Notification (prefix `policy/pdf/` → `policy_ingest_lambda`)
   - SNS subscription (topic → `policy_callback_lambda`) + `add-permission`

6. **Smoke test** by uploading `sample.pdf`

## 🔭 Future Enhancements

- **DB Writes:** On callback, transform Textract outputs and insert into RDS (env-based creds per your rule)
- **RAG Indexing:** Push normalized text to OpenSearch Serverless for semantic search
- **DLQ/Alarming:** Add SQS DLQs and CloudWatch alarms for failures
- **Versioned Storage:** `policy/textract-output/YYYY/MM/DD/<jobid>/...` is already time-segmented; add source ETag/version in manifest for lineage

## ✅ Completion Checklist

- [ ] Bucket & prefixes created
- [ ] SNS topic created; callback Lambda subscribed
- [ ] IAM roles attached (Lambdas + Textract publish role)
- [ ] Lambda env vars set (match `.env`)
- [ ] S3 → ingest Lambda wired (prefix `policy/pdf/`)
- [ ] Upload PDF → verify outputs & audit logs

---

That's it. This README provides everything needed—architecture, structure, env, IAM, wiring, ops, and runbooks—to deploy and operate the Lambda-only Textract pipeline with confidence.