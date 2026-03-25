# AWS Cloud Audit & Dashboard Tool - Project Plan

## Aim

To provide a **single-command Python tool** that automatically audits an entire AWS environment
across all services and all regions, then generates a ready-to-share PDF and/or Excel dashboard
covering active resources, billing insights, cost optimisation recommendations, cross-service
performance diagnostics, and a security audit - powered by rule-based analysis augmented by
**Gemini 2.5 Pro (Vertex AI)**.

---

## Use Cases

| # | Use Case | What the Tool Does |
|---|---|---|
| 1 | Monthly Cloud Cost Review | Consolidated spend by service, 7d / 15d / 30d trends, replaces manual Cost Explorer navigation |
| 2 | Infrastructure Rightsizing Audit | Flags underutilised resources across all service types, recommends specific downgrades with estimated savings |
| 3 | Performance Incident Investigation | Cross-service metric trends (CPU, memory, IOPS, network); PI-backed SQL query analysis for RDS spikes |
| 4 | Recurring Automated Reporting | Schedule via cron / Task Scheduler; outputs PDF / Excel every run without manual effort |
| 5 | FinOps / Cloud Governance | Actionable remediation backlog with per-resource savings justification |
| 6 | Security Audit & Compliance Review | CIS-aligned checks across IAM, S3, EC2, RDS, ECS, EKS, DynamoDB, API Gateway, Secrets Manager, and monitoring services |

---

## Key Design Decisions

| Decision | Choice |
|---|---|
| Resource scope | ALL active AWS services - dynamically discovered at runtime (not hardcoded) |
| Discovery method | 3-source: Resource Groups Tagging API + CloudWatch list_metrics() + AWS Config |
| Region scope | ALL enabled account regions - auto-discovered via ec2.describe_regions(); --region flag for override |
| Region strategy | Regional services scanned per-region in parallel via ThreadPoolExecutor; global services called once |
| Auth - AWS | Access key + secret key via .env file |
| Auth - AI | GCP Service Account JSON key via .env file |
| Output format | --format pdf or excel or both  (default: both) |
| AI layer | Gemini 2.5 Pro via Vertex AI - on by default, opt-out with --no-ai; augments rule-based engine, never replaces it |
| RDS query analysis | Performance Insights if enabled -> top SQL by db.load; fallback: advisory notice in report |
| Security checks | Registry-driven - only checks for discovered services are executed |
| Extensibility | Adding a new service = one entry each in metrics_registry, security_registry, rightsizing_registry |

---

## Project Structure

```
c:\PROJECTS\AWS-TOOL\
|
+-- main.py                      # Orchestration entry point + CLI
+-- config.py                    # Credential loading, thresholds, constants
+-- requirements.txt             # Python dependencies
+-- .env.example                 # Template for credentials and config
+-- .gitignore                   # Excludes .env, output/, __pycache__/
+-- README.md                    # Setup guide and usage instructions
+-- plan.md                      # This file
|
+-- modules/
|   +-- __init__.py
|   +-- aws_client.py            # boto3 multi-region client factory
|   +-- service_discovery.py     # Dynamic region + service discovery (runs first)
|   +-- resource_collector.py    # ARN-driven dispatcher -> unified resource list
|   +-- cost_analyzer.py         # Cost Explorer: overall + per-service billing
|   +-- performance_analyzer.py  # CloudWatch: all namespaces, 7d / 15d / 30d
|   +-- query_analyzer.py        # RDS Performance Insights + CloudWatch fallback
|   +-- recommendations.py       # Registry-driven rightsizing engine
|   +-- security_auditor.py      # Registry-driven security checks
|   +-- ai_analyzer.py           # Gemini 2.5 Pro via Vertex AI
|   +-- report_generator.py      # PDF (reportlab) + Excel (openpyxl) output
|
+-- registries/
|   +-- __init__.py
|   +-- metrics_registry.py      # { AWS/Namespace: [metric, stat, dimension] }
|   +-- security_registry.py     # { service_type: [check_fn, ...] }
|   +-- rightsizing_registry.py  # { service_type: { thresholds, recommend_fn } }
|
+-- utils/
|   +-- __init__.py
|   +-- helpers.py               # matplotlib chart generators (returns PNG bytes)
|
+-- output/                      # gitignored - generated reports land here
```

---

## Architecture Flowchart

```
+------------------------------------------------------------------+
|                       python main.py                             |
+------------------------------+-----------------------------------+
                               |
                               v
                  +------------------------+
                  |    Load .env Config    |
                  |  AWS keys, GCP key,    |
                  |  thresholds, format    |
                  +------------------------+
                               |
                  +------------------------+
                  | Credentials Valid?     |--- No ---> [ EXIT ]
                  +------------------------+
                               | Yes
                               v

+==================================================================+
|        PHASE 2 : Dynamic Region and Service Discovery            |
|                                                                  |
|  (1) ec2.describe_regions()                                      |
|      --> All enabled account regions (auto-discovered)           |
|                                                                  |
|  (2) Per-Region in Parallel  (ThreadPoolExecutor)                |
|      +-- Tagging API       --> All tagged resource ARNs          |
|      +-- CloudWatch        --> Active CloudWatch namespaces      |
|      +-- AWS Config        --> Untagged resources (if enabled)   |
|                                                                  |
|  (3) Global  (Called Once)                                       |
|      +-- IAM               --> Users, roles, policies, root      |
|      +-- S3                --> Buckets + home region per bucket  |
|      +-- CloudFront/Route53 --> Global CDN and DNS               |
|                                                                  |
|  OUTPUT : Canonical Service Map                                  |
|           { service_type --> ARNs, namespace, region }           |
+==================================================================+
                               |
                               v

+==================================================================+
|        PHASE 3 : Data Collection                                 |
|                                                                  |
|  resource_collector.py  --> ARN-driven dispatcher                |
|      --> Unified Resource List: arn, service, type, region, tags |
|                                                                  |
|  cost_analyzer.py       --> Cost Explorer  (us-east-1 endpoint)  |
|      --> Billing totals + per-service breakdown: 7d / 15d / 30d  |
+==================================================================+
                               |
                               v

+==================================================================+
|        PHASE 4 : Analysis Engine  (All Parallel)                 |
|                                                                  |
|  [4A] performance_analyzer.py  -- namespace-driven               |
|        +-- Namespace in metrics_registry                         |
|        |     --> Pull registered metrics  7d / 15d / 30d         |
|        +-- Namespace NOT in registry                             |
|        |     --> list_metrics() fallback, all available metrics   |
|        +--> Cross-service spike correlation across all services   |
|                                                                  |
|  [4B] query_analyzer.py  -- RDS only                             |
|        +-- Performance Insights ON                               |
|        |     --> Top SQL by db.load during CPU spike windows      |
|        +-- Performance Insights OFF                              |
|              --> Advisory notice added to report                 |
|                                                                  |
|  [4C] recommendations.py  -- via rightsizing_registry            |
|        +-- Service registered                                    |
|        |     --> Thresholds: EC2, RDS, Lambda, DynamoDB,         |
|        |         ECS, Redshift, ElastiCache                      |
|        +-- Service not registered                                |
|        |     --> Cost only, no sizing recommendation             |
|        +--> Merged with AWS native rightsizing API signal        |
|                                                                  |
|  [4D] security_auditor.py  -- via security_registry              |
|        +-- IAM, S3, EC2/Network, RDS, DynamoDB, ElastiCache      |
|        +-- ECS, EKS, SQS/SNS, API Gateway, Secrets Manager       |
|        +-- Monitoring: CloudTrail, GuardDuty, Config, Sec Hub    |
|        +--> Findings: HIGH / MEDIUM / LOW + region + fix         |
+==================================================================+
                               |
                               v

+==================================================================+
|        PHASE 5 : AI Analysis  -- Gemini 2.5 Pro / Vertex AI     |
|                                                                  |
|  --no-ai flag set?  -->  Skip AI, rule-based output only         |
|                                                                  |
|  AI enabled (default):                                           |
|   (1) Executive Summary narrative                                |
|   (2) Performance Root Cause analysis per spike event            |
|   (3) SQL Query analysis + index / rewrite suggestions           |
|   (4) Recommendations enhancement + burst-pattern detection      |
|   (5) Security findings prioritisation + environment context     |
+==================================================================+
                               |
                               v
              +--------------------------------+
              |      Aggregate All Data        |
              |  Rule-based + AI insights      |
              |  Unified result dict           |
              +--------------------------------+
                               |
                               v
              +--------------------------------+
              |  helpers.py  -- matplotlib     |
              |  Generate PNG chart bytes      |
              |  Reused in both output formats |
              +--------------------------------+
                               |
                  +------------v-----------+
                  |      --format flag     |
                  +------+-----------+-----+
               excel/both|           |pdf/both
                          |           |
   +-------------------+   +-----------------------+
   | PHASE 6A : Excel  |   | PHASE 6B : PDF        |
   | (.xlsx openpyxl)  |   | (.pdf  reportlab)     |
   |                   |   |                       |
   | Fixed sheets:     |   |  (1) Cover Page       |
   |  - Dashboard      |   |  (2) Exec Summary     |
   |  - Cost/Billing   |   |  (3) Active Resources |
   |  - Recommendations|   |  (4) Billing & Cost   |
   |  - Security       |   |  (5) Cost Optimisation|
   |  - IAM/Network    |   |  (6) Performance      |
   |  - Query Analysis |   |  (7) Security Audit   |
   |                   |   +-----------+-----------+
   | Dynamic sheets:   |               |
   |  1 per service    |               |
   |  1 per namespace  |               |
   +---------+---------+               |
             +-------------+-----------+
                           |
                           v
          +-------------------------------+
          |  output/                      |
          |  aws_dashboard_YYYYMMDD.xlsx  |
          |  aws_dashboard_YYYYMMDD.pdf   |
          +-------------------------------+
                           |
                           v
          File paths printed to console
```

---

## Implementation Phases

### Phase 1 - Project Bootstrap

1. `requirements.txt` - packages: boto3, pandas, matplotlib, reportlab, openpyxl, python-dotenv, google-cloud-aiplatform, vertexai
2. `.env.example` - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, AWS_REGIONS (optional), GCP_SA_KEY_PATH, GCP_PROJECT_ID, GCP_LOCATION
3. `.gitignore` - exclude .env, output/, *.pyc, __pycache__/
4. `config.py` - load and validate .env; expose region list, time-window constants (7/15/30 days), rightsizing thresholds
5. `modules/aws_client.py` - boto3 client factory: get_client(service, region) and get_all_regional_clients(service, regions)

---

### Phase 2 - Dynamic Region and Service Discovery (runs before everything)

6. `modules/service_discovery.py`

   Step 1 - Region discovery:
   - ec2.describe_regions(Filters=[opt-in-not-required, opted-in])
   - Returns all enabled regions for the account
   - If AWS_REGIONS set in .env -> use as override subset

   Step 2 - Per-region parallel scan (ThreadPoolExecutor, one thread per region):
   - resourcegroupstaggingapi.get_resources()  -> all tagged ARNs
   - cloudwatch.list_metrics()                 -> all active namespaces in region
   - config.list_discovered_resources()        -> untagged resources (if Config enabled)

   Step 3 - Global resources (called once, not per-region):
   - iam        -> users, roles, policies, root account metadata
   - s3          -> list_buckets() + per-bucket get_bucket_location()
   - cloudfront  -> list_distributions()
   - route53     -> list_hosted_zones()

   Output - canonical service map:
   {
     "AWS::EC2::Instance":    { namespace: "AWS/EC2",      arns: [...], regions: [...] },
     "AWS::RDS::DBInstance":  { namespace: "AWS/RDS",      arns: [...], regions: [...] },
     "AWS::DynamoDB::Table":  { namespace: "AWS/DynamoDB", arns: [...], regions: [...] }
   }

   Automatically covers 40+ services including:
   EC2, RDS, Lambda, S3, ECS, EKS, DynamoDB, ElastiCache, Redshift, OpenSearch,
   SQS, SNS, Kinesis, CloudFront, ALB, NLB, API Gateway, Step Functions, Glue,
   EMR, Athena, EventBridge, CodeBuild, Elastic Beanstalk, MSK, AppSync, WAF,
   Route53, Secrets Manager, and any future service AWS adds.

---

### Phase 3 - Data Collection (driven by discovery output)

7. `modules/resource_collector.py` - ARN-driven dispatcher:
   - Parses each ARN -> extracts service type -> calls appropriate describe_* API
   - Unknown service types -> stored as generic record {arn, type, region, tags}
   - Returns unified list: [{arn, service, type, region, name, tags, metadata}]

8. `modules/cost_analyzer.py` - Cost Explorer (single us-east-1 client, service-agnostic):
   - get_cost_and_usage()                      -> totals for 7d / 15d / 30d
   - get_cost_and_usage(GroupBy=SERVICE)        -> per-service breakdown
   - get_rightsizing_recommendation()           -> AWS native signals as secondary input

---

### Phase 4 - Analysis Engine (parallel, all registry-driven)

#### 4A - Performance Analysis

9. `modules/performance_analyzer.py` - namespace-driven:
   - For each discovered namespace -> lookup metrics_registry.py -> pull time-series (7d/15d/30d)
   - Namespace not in registry -> cloudwatch.list_metrics(Namespace=ns) -> pull all available metrics
   - Cross-service spike correlation: overlapping 1-hour buckets where 2+ services show anomalous values

   | Service     | Key Metrics                                                          |
   |-------------|----------------------------------------------------------------------|
   | EC2         | CPUUtilization (avg+max), NetworkIn/Out, DiskReadOps/WriteOps        |
   | RDS         | CPUUtilization, FreeableMemory, DatabaseConnections, IOPS, Latency   |
   | Lambda      | Invocations, Errors, Throttles, Duration (avg+max)                   |
   | S3          | NumberOfRequests, 4xxErrors, 5xxErrors, FirstByteLatency             |
   | DynamoDB    | ConsumedReadCapacityUnits, ConsumedWriteCapacityUnits, Latency        |
   | ECS         | CPUUtilization, MemoryUtilization                                    |
   | ElastiCache | CPUUtilization, CacheHits, CacheMisses, FreeableMemory               |
   | SQS         | NumberOfMessagesSent, ApproximateNumberOfMessagesVisible             |
   | ALB / NLB   | RequestCount, TargetResponseTime, HTTPCode_Target_5XX_Count          |
   | API Gateway | Count, Latency, 5XXError, 4XXError                                   |
   | Any other   | All metrics returned by list_metrics() for that namespace            |

#### 4B - RDS Query Analysis

10. `modules/query_analyzer.py`:
    - PI enabled  -> pi.describe_dimension_keys() -> top SQL by db.load during CPU spike windows
    - PI disabled -> advisory note with enablement instructions in report

#### 4C - Recommendations

11. `modules/recommendations.py` - via registries/rightsizing_registry.py:

    | Service     | Rightsizing Logic                                                       |
    |-------------|-------------------------------------------------------------------------|
    | EC2         | avg CPU <10% AND peak <30% over 30d -> suggest next smaller type        |
    | RDS         | avg CPU <15% AND FreeableMemory >80% of total -> smaller DB class       |
    | Lambda      | avg duration <20% of timeout OR avg memory <30% configured             |
    | ElastiCache | CPU + CacheMisses -> smaller or fewer nodes                             |
    | DynamoDB    | Provisioned vs consumed RCU/WCU -> switch to on-demand                  |
    | ECS         | Task CPU/memory reservation vs actual -> reduce task definition limits  |
    | Redshift    | CPU + storage -> smaller node type or fewer nodes                       |
    | Unregistered| Cost only - no sizing recommendation attempted                          |

#### 4D - Security Audit

12. `modules/security_auditor.py` - via registries/security_registry.py:

    | Domain          | Checks                                                                    |
    |-----------------|---------------------------------------------------------------------------|
    | IAM             | Root last used, users without MFA, keys >90 days, AdministratorAccess on users, inline policies |
    | S3              | BlockPublicAcls/Policy disabled, ACL public-read/write, no SSE, no versioning |
    | EC2 / Network   | SGs with 0.0.0.0/0 on ports 22/3389/3306/5432/1433, unencrypted EBS, no key pair |
    | RDS             | PubliclyAccessible=true, StorageEncrypted=false, automated backups disabled |
    | DynamoDB        | Encryption off, point-in-time recovery off, public resource policy        |
    | ElastiCache     | Encryption in-transit/at-rest off, no auth token                         |
    | ECS             | Privileged containers, task role with * permissions                       |
    | EKS             | Public API endpoint, no envelope encryption, permissive RBAC             |
    | SQS / SNS       | Public resource policies (Principal: *)                                   |
    | API Gateway     | No authoriser, access logging disabled                                    |
    | Secrets Manager | Not rotated >90 days, no customer KMS key                                |
    | Monitoring      | CloudTrail off per region, GuardDuty inactive, Config off, Security Hub off |

    Each finding: { severity: HIGH/MEDIUM/LOW, domain, resource_id, region, issue, recommendation }

---

### Phase 5 - AI Analysis via Gemini 2.5 Pro

13. `modules/ai_analyzer.py` - 5 focused Vertex AI calls:

    | Call                        | AI Output                                               |
    |-----------------------------|---------------------------------------------------------|
    | Executive Summary           | Narrative paragraph for cover and summary section       |
    | Performance Root Cause      | Per-spike cross-service explanation                     |
    | Query Analysis              | SQL structural analysis, index and rewrite suggestions  |
    | Recommendations Enhancement | Burst-pattern detection, context-aware justification    |
    | Security Prioritisation     | Environment-specific risk narrative, prioritised fixes  |

    - Auth via GCP_SA_KEY_PATH from .env
    - Graceful degradation: --no-ai or Vertex AI unreachable -> rule-based output, note in report
    - AI output is additive - rule-based findings always present; AI adds an "AI Analysis" column

---

### Phase 6 - Report Generation

14. `utils/helpers.py` - chart generators returning in-memory PNG bytes:
    - time_series_chart(data, title, ylabel)   -> metric trends
    - bar_chart(labels, values, title)          -> billing by service
    - pie_chart(labels, values, title)          -> cost distribution
    - severity_donut(high, medium, low)         -> security summary
    - Charts generated once -> embedded in both PDF and Excel

15. `modules/report_generator.py`:

    Excel Workbook (openpyxl) - dynamic sheet count:

    | Sheet Type          | Description                                                   |
    |---------------------|---------------------------------------------------------------|
    | Fixed sheets        | Dashboard Summary, Cost by Service, Billing Trend, Recommendations, Security Audit, IAM Risks, Network Security, Query Analysis |
    | Dynamic inventory   | One sheet per discovered service type (auto-generated)        |
    | Dynamic performance | One sheet per discovered CloudWatch namespace with charts     |

    PDF Report (reportlab) - 7 sections:

    | Section             | Content                                                       |
    |---------------------|---------------------------------------------------------------|
    | Cover               | Account name, account ID, scanned regions, generated timestamp |
    | Executive Summary   | KPI cards + AI-written narrative                              |
    | Active Resources    | One table per discovered service (Region column on every row) |
    | Billing & Cost      | Trend charts (3 windows) + service cost pie + anomaly callouts |
    | Cost Optimisation   | Recommendations table with AI-enhanced justification          |
    | Performance Analysis| Per-service charts (7d/15d/30d) + spike correlation + AI root cause |
    | Security Audit      | Findings by domain, severity colour-coded, AI-prioritised fixes |

---

### Phase 7 - Entry Point and Documentation

16. `main.py` CLI:

    ```
    python main.py                                  # both formats, all regions, AI on
    python main.py --format pdf                     # PDF only
    python main.py --format excel                   # Excel only
    python main.py --no-ai                          # skip Vertex AI calls
    python main.py --region us-east-1,eu-west-1    # restrict to specific regions
    ```

    Execution order:
    load config -> discover regions -> discover services -> collect resources
    -> analyze costs -> analyze performance -> analyze queries
    -> recommendations -> security audit -> AI analysis
    -> generate charts -> write report(s) -> print output paths

17. `README.md` - prerequisites, .env setup, IAM policy JSON, GCP SA key setup, how to run

---

## Accuracy Strategy

### Cost Recommendations
- Uses actual 30-day CloudWatch data - both average AND peak to avoid false positives from one-off spikes
- Cross-checks with AWS native get_rightsizing_recommendation() - only surfaces if both signals agree
- AI detects burst patterns (e.g. daily 09:00 batch job) that a simple threshold misses
- Every recommendation shows raw metric justification (avg CPU %, memory headroom %)

### Performance Analysis
- Compares 7d vs 15d vs 30d to distinguish sustained degradation from one-off spikes
- Cross-correlates spikes across ALL discovered services in the same 1-hour bucket
- RDS query attribution via Performance Insights - most precise signal available from AWS

### Security Findings
- Direct AWS API calls - real-time configuration state
- Modelled on CIS AWS Foundations Benchmark v2
- Per-region checking - a service off in one region but on in another is explicitly flagged

---

## Verification Checklist

- [ ] pip install -r requirements.txt - all packages resolve
- [ ] Configure .env - no AWS_REGIONS set (test auto-discovery)
- [ ] python main.py - auto-discovers all regions, output/ contains .xlsx and .pdf
- [ ] Excel: dynamic sheets match actual account services; Region column on all rows
- [ ] Excel: Security Audit has per-region monitoring findings; Recommendations show metric justification
- [ ] PDF: Cover shows all scanned regions; Security section colour-coded by severity
- [ ] python main.py --no-ai - completes without error, report notes AI unavailable
- [ ] python main.py --format pdf --region us-east-1 - single region, PDF only
- [ ] Cross-check one resource ID and one finding against AWS Console
- [ ] Region with no resources - no crash, graceful empty state
- [ ] .env and output/ excluded from git before first commit

---

## IAM Permissions Required (Read-Only)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:Describe*",
        "tag:GetResources",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:ListMetrics",
        "rds:Describe*",
        "lambda:List*",
        "lambda:GetFunction",
        "s3:ListAllMyBuckets",
        "s3:GetBucketLocation",
        "s3:GetBucketAcl",
        "s3:GetBucketEncryption",
        "s3:GetPublicAccessBlock",
        "iam:List*",
        "iam:Get*",
        "iam:GenerateCredentialReport",
        "ce:GetCostAndUsage",
        "ce:GetRightsizingRecommendation",
        "pi:GetResourceMetrics",
        "pi:DescribeDimensionKeys",
        "config:ListDiscoveredResources",
        "config:DescribeConfigurationRecorders",
        "cloudtrail:DescribeTrails",
        "cloudtrail:GetTrailStatus",
        "guardduty:ListDetectors",
        "guardduty:GetDetector",
        "securityhub:DescribeHub",
        "elasticache:Describe*",
        "dynamodb:ListTables",
        "dynamodb:DescribeTable",
        "ecs:ListClusters",
        "ecs:ListServices",
        "ecs:DescribeServices",
        "eks:ListClusters",
        "eks:DescribeCluster",
        "sqs:ListQueues",
        "sqs:GetQueueAttributes",
        "sns:ListTopics",
        "sns:GetTopicAttributes",
        "redshift:DescribeClusters",
        "apigateway:GET",
        "secretsmanager:ListSecrets",
        "secretsmanager:DescribeSecret",
        "cloudfront:ListDistributions",
        "route53:ListHostedZones"
      ],
      "Resource": "*"
    }
  ]
}
```

---

## Excluded from Scope

- Writing or modifying any AWS resource (strictly read-only)
- Active security scanning or penetration testing
- Multi-account (AWS Organizations) - single account only
- Real-time streaming dashboards - reports are point-in-time snapshots