# AWS Infrastructure Dashboard

A single-command Python tool that audits your entire AWS account and produces professional **PDF and Excel reports** — covering resources, costs, performance, right-sizing recommendations, and security findings. Optionally enhanced with **Gemini 2.5 Pro AI insights**.

> **Read-only.** This tool never creates, modifies, or deletes any AWS resource.

---

## What It Does

| Area | What you get |
|---|---|
| **Resource Inventory** | Every service, every region — auto-discovered |
| **Billing & Costs** | 7-day, 15-day, 30-day spend + month-to-date projection |
| **Cost Trends** | Daily rate trend, week-over-week change, projected month total |
| **Performance** | CPU, memory, IOPS, network via CloudWatch — anomaly detection |
| **RDS Query Analysis** | Slow query analysis via Performance Insights |
| **Right-sizing** | Over/under-provisioned EC2, RDS, Lambda, ElastiCache with savings estimates |
| **Security Audit** | IAM, S3, Security Groups, EBS, RDS, EKS, GuardDuty, CloudTrail |
| **AI Insights** | Gemini 2.5 Pro executive summary, root-cause analysis, action plan |

---

## How It Works — Pipeline Flow

```
python main.py
       │
       ├─ 1. service_discovery    Finds all active regions and services
       ├─ 2. resource_collector   Collects metadata for every resource
       ├─ 3. cost_analyzer        Pulls 7d/15d/30d billing from Cost Explorer
       ├─ 4. performance_analyzer Fetches CloudWatch metrics for all namespaces
       ├─ 5. query_analyzer       Reads RDS slow queries via Performance Insights
       ├─ 6. recommendations      Evaluates right-sizing opportunities
       ├─ 7. security_auditor     Runs security checks across all domains
       ├─ 8. trend_analyzer       Computes cost/perf/fleet/security patterns
       ├─ 9. ai_analyzer          Sends summary to Gemini 2.5 Pro (optional)
       └─10. report_generator     Writes PDF + Excel to ./output/
```

Each step passes its output to the next — no AWS calls are repeated.

---

## Quick Start

### 1. Clone and install dependencies

```bash
git clone <repo-url>
cd aws-tool
pip install -r requirements.txt
```

### 2. Set up credentials

```bash
cp .env.example .env
# Open .env and fill in your AWS keys (and optionally GCP keys for AI)
```

### 3. Run the tool

```bash
# Full run — PDF + Excel + AI analysis
python main.py

# Skip AI (faster, no GCP needed)
python main.py --no-ai

# Excel only, one region
python main.py --format excel --no-ai --region us-east-1

# PDF only, with detailed logging
python main.py --format pdf --verbose

# Specific regions, save to custom folder
python main.py --region us-east-1,eu-west-1 --output ./reports
```

Reports are saved to `./output/` by default.

---

## CLI Reference

```
usage: python main.py [options]

  --format  {pdf, excel, both}   Output format            (default: both)
  --no-ai                        Skip Gemini AI analysis
  --region  REGION[,REGION,...]  Specific regions only    (default: all)
  --output  DIR                  Output folder            (default: ./output)
  --verbose                      Show DEBUG-level logs
```

---

## Configuration — `.env` File

```ini
# ── AWS (required) ────────────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1

# Friendly account name shown in report headers
AWS_ACCOUNT_NAME=MyCompany-Prod

# Limit to specific regions (optional — default is all active regions)
# AWS_REGIONS=us-east-1,eu-west-1,ap-southeast-1

# ── GCP / Vertex AI (required only when using AI analysis) ───────────────────
GCP_SA_KEY_PATH=/path/to/service-account-key.json
GCP_PROJECT_ID=your-gcp-project
GCP_LOCATION=us-central1
```

---

## Report Contents

### PDF — 8 Sections

| # | Section | What it shows |
|---|---|---|
| 1 | Cover Page | Account name, date, KPI summary cards |
| 2 | Executive Summary | AI narrative or auto-generated summary |
| 3 | Active Resources | Resource counts by service, region breakdown |
| 4 | Billing & Cost | MTD billing table, 30-day breakdown by service |
| 5 | Cost Optimisation | Right-sizing recommendations with savings estimates |
| 6 | Performance Analysis | CloudWatch metric trends, anomalies, RDS query data |
| 7 | Trends | Cost direction, performance changes, fleet health, security patterns |
| 8 | Security Audit | Findings by severity, domain hotspots, IAM/S3/SG issues |

### Excel — 9 Sheets

| Sheet | Content |
|---|---|
| Summary | KPI dashboard |
| Billing | Cost by window + top services |
| Resources | Full resource inventory |
| Recommendations | Right-sizing table with severity colours |
| Security Findings | All findings with colour-coded severity |
| Performance Trends | Per-namespace metric changes |
| Cost Trends | Service-level cost trend table |
| RDS Query Analysis | Top SQL queries per instance |
| AI Insights | Executive summary + root-cause analysis |

---

## Required AWS IAM Permissions

The IAM user or role must have the following read-only permissions:


---

## Project Structure

```
aws-tool/
├── main.py                      Entry point — orchestrates the pipeline
├── config.py                    Loads .env, defines thresholds and settings
├── requirements.txt             Python dependencies
│
├── modules/
│   ├── service_discovery.py     Discovers all active regions and services
│   ├── resource_collector.py    Collects metadata for each resource
│   ├── cost_analyzer.py         Cost Explorer: rolling windows + MTD billing
│   ├── performance_analyzer.py  CloudWatch metrics across all namespaces
│   ├── query_analyzer.py        RDS Performance Insights slow queries
│   ├── recommendations.py       Right-sizing evaluation engine
│   ├── security_auditor.py      Security checks: IAM, S3, SGs, EBS, RDS, EKS
│   ├── trend_analyzer.py        Cost/performance/fleet/security pattern engine
│   ├── pricing_estimator.py     AWS Pricing API — confirms downsize targets
│   ├── ai_analyzer.py           Gemini 2.5 Pro via Vertex AI
│   └── report_generator.py      PDF (ReportLab) + Excel (openpyxl) output
│
├── registries/
│   ├── metrics_registry.py      CloudWatch namespace → metric definitions
│   ├── security_registry.py     Security domain → check function map
│   └── rightsizing_registry.py  Service → thresholds and recommendation templates
│
└── utils/
    └── helpers.py               Chart generators (matplotlib → PNG bytes)
```

---

## GCP / Vertex AI Setup (for AI analysis)

1. Create or select a **GCP project** and enable the **Vertex AI API**
2. Create a **Service Account** with the `Vertex AI User` role
3. Download the **JSON key file**
4. Set `GCP_SA_KEY_PATH` and `GCP_PROJECT_ID` in your `.env`
5. Run normally — AI analysis is on by default. Use `--no-ai` to skip it.

---

## Supported AWS Services

EC2, RDS, Lambda, S3, DynamoDB, ElastiCache, ECS, EKS, Redshift, SQS, SNS,
API Gateway, CloudFront, Route 53, IAM, Kinesis, Step Functions, and any other
service that publishes CloudWatch metrics or is tracked by AWS Config.

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `Missing required environment variables` | Copy `.env.example` to `.env` and fill in `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` |
| `Cost Explorer query failed` | Enable Cost Explorer in the AWS Console: Billing → Cost Explorer → Enable |
| `Performance Insights not available` | Enable Performance Insights on your RDS instances via the RDS Console → Modify |
| `Could not initialise Vertex AI client` | Check `GCP_SA_KEY_PATH` points to a valid JSON key, or run with `--no-ai` |
| Slow on large accounts | Use `--region us-east-1` to limit scope, or `--verbose` to monitor progress |

---

## Security Notes

- **Read-only** — no AWS resource is ever created, modified, or deleted.
- Credentials are loaded from `.env` — never hardcoded in source files.
- `.env` is excluded from git via `.gitignore`.
- GCP key file should be stored outside the repository directory.

---

## License

Internal use. Not for public distribution.
