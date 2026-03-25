# AWS Infrastructure Dashboard

A single-command Python tool that generates comprehensive **PDF and Excel dashboards** for your AWS environment. Run one command and get a full audit covering active resources, billing, performance analysis, right-sizing recommendations, and security findings — augmented with Gemini 2.5 Pro AI insights.

---

## Features

| Category | What it covers |
|---|---|
| **Active Resources** | All services, all regions — dynamically discovered at runtime |
| **Billing** | Total + per-service costs for 7d / 15d / 30d |
| **Performance** | CPU, memory, IOPS, network — CloudWatch metrics for all namespaces |
| **RDS Query Analysis** | Top SQL statements via Performance Insights (7d / 15d / 30d) |
| **Right-sizing** | Overprovisioned EC2, RDS, Lambda, ElastiCache — with savings estimates |
| **Security Audit** | IAM, S3, Security Groups, EBS, RDS, EKS, GuardDuty, CloudTrail |
| **AI Insights** | Gemini 2.5 Pro: executive summary, root-cause analysis, prioritised actions |

---

## Quick Start

### 1. Clone and install

```bash
git clone <repo-url>
cd aws-tool
pip install -r requirements.txt
```

### 2. Configure credentials

```bash
cp .env.example .env
# Edit .env with your AWS access key and (optionally) GCP key for AI
```

### 3. Run

```bash
# Generate both PDF and Excel with AI analysis
python main.py

# Excel only, no AI, specific regions
python main.py --format excel --no-ai --region us-east-1,eu-west-1

# PDF only, verbose logging
python main.py --format pdf --verbose
```

Reports are written to `./output/` by default.

---

## CLI Options

```
usage: main.py [-h] [--format {pdf,excel,both}] [--no-ai]
               [--region REGION] [--output DIR] [--verbose]

Options:
  --format  {pdf,excel,both}   Output format (default: both)
  --no-ai                      Skip Gemini AI analysis
  --region  REGION[,REGION]    Override region list (default: auto-discover all)
  --output  DIR                Output directory (default: ./output)
  --verbose                    Enable DEBUG logging
```

---

## Configuration (.env)

```ini
# AWS Credentials (required)
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1

# Optional: friendly name shown in reports
AWS_ACCOUNT_NAME=MyCompany-Prod

# Optional: comma-separated region list (overrides auto-discovery)
# AWS_REGIONS=us-east-1,eu-west-1,ap-southeast-1

# GCP / Vertex AI (required only for --ai, which is on by default)
GCP_SA_KEY_PATH=/path/to/service-account-key.json
GCP_PROJECT_ID=your-gcp-project
GCP_LOCATION=us-central1
```

---

## Required AWS IAM Permissions

Attach the following policy to the IAM user or role running the tool:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadOnlyDiscovery",
      "Effect": "Allow",
      "Action": [
        "ec2:Describe*",
        "rds:Describe*",
        "lambda:List*",
        "lambda:Get*",
        "s3:GetBucket*",
        "s3:ListAllMyBuckets",
        "dynamodb:Describe*",
        "elasticache:Describe*",
        "ecs:Describe*",
        "ecs:List*",
        "eks:Describe*",
        "eks:List*",
        "redshift:Describe*",
        "sqs:GetQueueAttributes",
        "sqs:ListQueues",
        "tag:GetResources",
        "config:ListDiscoveredResources",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:ListMetrics",
        "pi:DescribeDimensionKeys",
        "pi:GetResourceMetrics",
        "ce:GetCostAndUsage",
        "ce:GetRightsizingRecommendation",
        "iam:GetAccountSummary",
        "iam:ListUsers",
        "iam:ListMFADevices",
        "iam:ListAccessKeys",
        "iam:GetAccountPasswordPolicy",
        "cloudtrail:DescribeTrails",
        "guardduty:ListDetectors",
        "guardduty:GetDetector",
        "cloudfront:ListDistributions",
        "route53:ListHostedZones",
        "sts:GetCallerIdentity"
      ],
      "Resource": "*"
    }
  ]
}
```

---

## Output Reports

### PDF Report Sections

1. **Cover Page** — account info, KPI snapshot
2. **Executive Summary** — AI-generated narrative (or fallback text)
3. **Active Resources** — tables + pie chart by service type
4. **Billing & Cost** — trend charts, top services table
5. **Cost Optimisation** — recommendations with savings, AI implementation guide  
6. **Performance Analysis** — time-series charts, anomaly table, RDS query data
7. **Security Audit** — severity donut, findings table, AI prioritisation

### Excel Workbook Sheets

| Sheet | Content |
|---|---|
| Summary | KPI dashboard |
| Billing | Cost by window + top services chart |
| Resources | All resources flat table |
| Recommendations | Right-sizing table with severity colours |
| Security Findings | All findings with severity colour-coding |
| Perf_{Namespace} | Per-namespace anomalies + embedded chart |
| RDS Query Analysis | Top SQL queries per instance + PI advisory |
| AI Insights | Executive summary + root cause analysis |

---

## Architecture

```
main.py
  |
  +-- config.py              (credentials, thresholds)
  |
  +-- service_discovery.py   (regions + all services via 3 sources)
  +-- resource_collector.py  (ARN-driven metadata collector)
  +-- cost_analyzer.py       (Cost Explorer: 7d/15d/30d + native recs)
  +-- performance_analyzer.py(CloudWatch: all namespaces, spike correlation)
  +-- query_analyzer.py      (RDS Performance Insights)
  +-- recommendations.py     (right-sizing + merge native recs)
  +-- security_auditor.py    (IAM, S3, SGs, EBS, RDS, EKS, monitoring)
  +-- ai_analyzer.py         (Gemini 2.5 Pro via Vertex AI)
  +-- report_generator.py    (PDF via reportlab, Excel via openpyxl)
  |
  +-- registries/
  |     metrics_registry.py  (namespace -> CloudWatch metric definitions)
  |     security_registry.py (domain -> check function names)
  |     rightsizing_registry.py (service -> thresholds + templates)
  |
  +-- utils/
        helpers.py           (matplotlib chart generators -> PNG bytes)
```

### Service Discovery (3 sources)

1. **AWS Resource Tagging API** — `tag:GetResources` paginator across all resource types
2. **CloudWatch `list_metrics`** — discovers any namespace that has active metrics
3. **AWS Config** — `config:ListDiscoveredResources` with ~40 resource types

Global services (IAM, S3, CloudFront, Route53) are collected separately.

---

## Supported Services

EC2, RDS, Lambda, S3, DynamoDB, ElastiCache, ECS, EKS, Redshift, SQS, SNS,
API Gateway, CloudFront, Route53, IAM, Kinesis, Step Functions, and any other
service that publishes CloudWatch metrics or is tracked by AWS Config.

---

## GCP / Vertex AI Setup (for AI analysis)

1. Create a GCP project and enable the **Vertex AI API**
2. Create a Service Account with the `Vertex AI User` role
3. Download the JSON key file
4. Set `GCP_SA_KEY_PATH` and `GCP_PROJECT_ID` in your `.env`

To skip AI analysis entirely, use `--no-ai`.

---

## Troubleshooting

**`Missing required environment variables`**  
→ Copy `.env.example` to `.env` and fill in `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

**`Cost Explorer query failed`**  
→ Cost Explorer must be enabled in the AWS console (Billing → Cost Explorer → Enable).

**`Performance Insights not available`**  
→ Enable Performance Insights on your RDS instances (RDS Console → Modify → Performance Insights).

**`Could not initialise Vertex AI client`**  
→ Check `GCP_SA_KEY_PATH` points to a valid JSON key file, or run with `--no-ai`.

**Large accounts taking a long time**  
→ Use `--region us-east-1` to limit scope, or run with `--verbose` to watch progress.

---

## Security Notes

- This tool is **read-only** — it never creates, modifies, or deletes AWS resources.
- Credentials are loaded from environment variables via `.env` — never hardcoded.
- The `.env` file is excluded from git via `.gitignore`.
- GCP service account key file path should point to a file outside the repo.
