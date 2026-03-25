"""
registries/security_registry.py
--------------------------------
Maps domain keys to lists of check-function names defined in
modules/security_auditor.py.  Only domains whose key appears in
the discovered service types are executed - unrecognised services
are skipped rather than causing errors.

Domain keys use simplified lowercase identifiers that are matched
against the middle segment of AWS resource type ARNs
(e.g. "AWS::EC2::Instance" -> "ec2").

Special keys:
  "iam"        - always checked (global service)
  "monitoring" - always checked (per-region monitoring services)
"""

SECURITY_REGISTRY = {
    # ── Identity & Access Management ──────────────────────────────────────────
    "iam": [
        "check_iam",
        "check_iam_admin_policies",
        "check_iam_unused_credentials",
        "check_iam_inline_policies",
    ],
    # ── Storage ───────────────────────────────────────────────────────────────
    "s3": [
        "check_s3",
        "check_s3_bucket_policies",
        "check_s3_access_logging",
    ],
    # ── Compute / Network ────────────────────────────────────────────────────
    "ec2": [
        "check_security_groups",
        "check_ebs_encryption",
        "check_sg_unrestricted_egress",
        "check_ec2_imdsv2",
        "check_ec2_no_instance_profile",
        "check_ec2_stopped_long",
        "check_vpc_flow_logs",
        "check_default_vpc",
        "check_eip_unused",
        "check_ebs_unattached",
        # Idle resource detection
        "check_ec2_idle",
        "check_natgw_idle",
        # Unused VPC detection
        "check_vpc_unused",
    ],
    # ── Databases ─────────────────────────────────────────────────────────────
    "rds": [
        "check_rds",
        "check_rds_deletion_protection",
        "check_rds_public_snapshots",
        "check_rds_enhanced_monitoring",
        # Idle resource detection
        "check_rds_no_connections",
    ],
    "dynamodb": [
        "check_dynamodb",
    ],
    "redshift": [
        "check_redshift",
        # Idle resource detection
        "check_redshift_idle",
    ],
    # ── Caching ───────────────────────────────────────────────────────────────
    "elasticache": [
        "check_elasticache",
        # Idle resource detection
        "check_elasticache_idle",
    ],
    # ── Containers ────────────────────────────────────────────────────────────
    "ecs": [
        # Idle resource detection
        "check_ecs_empty_services",
    ],
    "eks": [
        "check_eks",
    ],
    # ── Serverless ────────────────────────────────────────────────────────────
    "lambda": [
        "check_lambda_public_invoke",
        "check_lambda_deprecated_runtime",
        # Idle resource detection
        "check_lambda_no_invocations",
    ],
    # ── Secrets & Encryption ──────────────────────────────────────────────────
    "secretsmanager": [
        "check_secrets_rotation",
    ],
    "kms": [
        "check_kms_key_rotation",
    ],
    # ── Messaging ─────────────────────────────────────────────────────────────
    "sqs": [
        "check_sqs",
        # Idle resource detection
        "check_sqs_empty_inactive",
    ],
    # ── Streaming ──────────────────────────────────────────────────────
    "kinesis": [
        "check_kinesis_idle",
    ],
    # ── API ───────────────────────────────────────────────────────────
    "apigateway": [
        "check_apigw_idle",
    ],
    # ── AI / ML ───────────────────────────────────────────────────────
    "sagemaker": [
        "check_sagemaker_idle_notebooks",
    ],
    # ── SNS ────────────────────────────────────────────────────────────
    "sns": [
        "check_snstopic_no_publish",
    ],
    # ── Load Balancing ────────────────────────────────────────────────────────
    "elasticloadbalancing": [
        "check_alb_http_listeners",
        # Idle resource detection
        "check_elb_idle",
    ],
    # ── Monitoring (always checked per region) ────────────────────────────────
    "monitoring": [
        "check_monitoring",
        "check_cloudwatch_log_retention",
        "check_cloudtrail_validation",
    ],
}

