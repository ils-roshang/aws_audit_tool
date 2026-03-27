"""
modules/security_auditor.py
------------------------------
Phase 6: Runs security checks across all discovered AWS services.

Architecture
-------------
- Each check function is a standalone callable that accepts
  (resources, regions, get_client_fn) and returns a list of finding dicts.
- CHECK_FUNCTIONS dict maps string names (referenced by SECURITY_REGISTRY)
  to the actual functions, eliminating circular imports.
- The dispatcher determines which domains are active based on the
  service_map, then runs only relevant checks.

Finding schema:
{
  "severity":       "HIGH" | "MEDIUM" | "LOW" | "INFO",
  "domain":         "IAM" | "Network" | "Storage" | ...,
  "resource_id":    "sg-abc123",
  "resource_type":  "AWS::EC2::SecurityGroup",
  "region":         "us-east-1",
  "issue":          "Port 22 open to 0.0.0.0/0",
  "recommendation": "Restrict SSH access or use SSM Session Manager",
}
"""

import csv
import io as _io
import json as _json
import logging
import re as _re
import time as _time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import modules.aws_client as aws_client
import config
from registries.security_registry import SECURITY_REGISTRY

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Hourly on-demand pricing lookup tables (us-east-1 rates, USD).
# Used by idle/unused checks to compute estimated_monthly_cost_usd so the
# Unused Resources sheet never shows "See billing" — always a real number.
# Rates are intentionally conservative (median across families / engines).
# ─────────────────────────────────────────────────────────────────────────────
_HOURS_PER_MONTH = 730

# RDS: on-demand hourly rates by DB instance class (single-AZ, us-east-1).
# Source: https://aws.amazon.com/rds/pricing/  (MySQL / PostgreSQL)
_RDS_HOURLY: dict = {
    "db.t3.micro":    0.017, "db.t3.small":    0.034, "db.t3.medium":   0.068,
    "db.t3.large":    0.136, "db.t3.xlarge":   0.272, "db.t3.2xlarge":  0.544,
    "db.t4g.micro":   0.016, "db.t4g.small":   0.032, "db.t4g.medium":  0.065,
    "db.t4g.large":   0.130, "db.t4g.xlarge":  0.260, "db.t4g.2xlarge": 0.520,
    "db.m5.large":    0.171, "db.m5.xlarge":   0.342, "db.m5.2xlarge":  0.684,
    "db.m5.4xlarge":  1.368, "db.m5.8xlarge":  2.736, "db.m5.16xlarge": 5.472,
    "db.m6g.large":   0.153, "db.m6g.xlarge":  0.306, "db.m6g.2xlarge": 0.612,
    "db.r5.large":    0.240, "db.r5.xlarge":   0.480, "db.r5.2xlarge":  0.960,
    "db.r5.4xlarge":  1.920, "db.r5.8xlarge":  3.840, "db.r5.16xlarge": 7.680,
    "db.r6g.large":   0.216, "db.r6g.xlarge":  0.432, "db.r6g.2xlarge": 0.864,
}
_RDS_HOURLY_DEFAULT = 0.171  # db.m5.large as fallback

# RDS: per-GB/month storage rates by volume type (us-east-1, Single-AZ).
# Source: https://aws.amazon.com/rds/pricing/
# io1 base rate only — additional IOPS cost ($0.10/IOPS/month) not included.
_RDS_STORAGE_GB_RATE: dict = {
    "gp2":      0.115,
    "gp3":      0.115,   # same as gp2 base; gp3 may be lower with provisioned tier
    "io1":      0.125,
    "standard": 0.065,   # magnetic — legacy, rare
}
_RDS_STORAGE_GB_RATE_DEFAULT = 0.115  # gp2 as conservative fallback

# EBS: per-GB/month rates by volume type (us-east-1).
# Source: https://aws.amazon.com/ebs/pricing/
# io1/io2: base storage rate only — provisioned IOPS ($0.065-0.10/IOPS/month) excluded.
_EBS_GB_RATE: dict = {
    "gp3":      0.080,   # $0.08/GB-month
    "gp2":      0.100,   # $0.10/GB-month
    "io1":      0.125,   # $0.125/GB-month
    "io2":      0.125,   # $0.125/GB-month
    "st1":      0.045,   # $0.045/GB-month (throughput HDD)
    "sc1":      0.015,   # $0.015/GB-month (cold HDD)
    "standard": 0.050,   # $0.05/GB-month  (magnetic, legacy)
}
_EBS_GB_RATE_DEFAULT = 0.100  # gp2 as conservative fallback

# Redshift: on-demand hourly rates per node (us-east-1, Single-AZ).
# Source: https://aws.amazon.com/redshift/pricing/
_REDSHIFT_HOURLY: dict = {
    "dc2.large":    0.250,  "dc2.8xlarge":   4.800,
    "ra3.xlplus":   0.650,  "ra3.4xlarge":   3.260,  "ra3.16xlarge": 13.040,
    "ds2.xlarge":   0.850,  "ds2.8xlarge":   6.800,
}
_REDSHIFT_HOURLY_DEFAULT = 0.250  # dc2.large as fallback

# ElastiCache: on-demand hourly rates by node type (us-east-1).
# Source: https://aws.amazon.com/elasticache/pricing/
_ELASTICACHE_HOURLY: dict = {
    "cache.t3.micro":   0.017, "cache.t3.small":   0.034, "cache.t3.medium":  0.068,
    "cache.t4g.micro":  0.016, "cache.t4g.small":  0.032, "cache.t4g.medium": 0.065,
    "cache.m5.large":   0.156, "cache.m5.xlarge":  0.311, "cache.m5.2xlarge": 0.622,
    "cache.m6g.large":  0.140, "cache.m6g.xlarge": 0.280, "cache.m6g.2xlarge":0.560,
    "cache.r5.large":   0.218, "cache.r5.xlarge":  0.436, "cache.r5.2xlarge": 0.872,
    "cache.r6g.large":  0.196, "cache.r6g.xlarge": 0.393, "cache.r6g.2xlarge":0.786,
}
_ELASTICACHE_HOURLY_DEFAULT = 0.156  # cache.m5.large as fallback

# EC2: on-demand hourly rates by full instance type (us-east-1, Linux, USD).
# Source: https://aws.amazon.com/ec2/pricing/on-demand/
# Covers the most common instance families; moved here from check_ec2_idle so the
# Pricing API helpers (below) can reference the table as a fallback.
_EC2_HOURLY: dict = {
    # Burstable general purpose (t3 / t3a / t4g)
    "t3.nano": 0.005,   "t3.micro": 0.010,  "t3.small": 0.021,
    "t3.medium": 0.042, "t3.large": 0.083,  "t3.xlarge": 0.166,  "t3.2xlarge": 0.333,
    "t3a.nano": 0.005,  "t3a.micro": 0.009, "t3a.small": 0.019,
    "t3a.medium": 0.038,"t3a.large": 0.075, "t3a.xlarge": 0.150, "t3a.2xlarge": 0.301,
    "t4g.nano": 0.004,  "t4g.micro": 0.008, "t4g.small": 0.017,
    "t4g.medium": 0.034,"t4g.large": 0.067, "t4g.xlarge": 0.134, "t4g.2xlarge": 0.269,
    # General purpose (m5 / m5a / m6i / m6a / m7i)
    "m5.large": 0.096,   "m5.xlarge": 0.192,   "m5.2xlarge": 0.384,
    "m5.4xlarge": 0.768, "m5.8xlarge": 1.536,  "m5.16xlarge": 3.072,
    "m5a.large": 0.086,  "m5a.xlarge": 0.172,  "m5a.2xlarge": 0.344,
    "m5a.4xlarge": 0.688,"m5a.8xlarge": 1.376, "m5a.16xlarge": 2.752,
    "m6i.large": 0.096,  "m6i.xlarge": 0.192,  "m6i.2xlarge": 0.384,
    "m6i.4xlarge": 0.768,"m6i.8xlarge": 1.536, "m6i.16xlarge": 3.072,
    "m6a.large": 0.086,  "m6a.xlarge": 0.173,  "m6a.2xlarge": 0.346,
    "m7i.large": 0.101,  "m7i.xlarge": 0.202,  "m7i.2xlarge": 0.403,
    "m7i.4xlarge": 0.806,"m7i.8xlarge": 1.613, "m7i.16xlarge": 3.226,
    # Compute optimized (c5 / c5a / c6i / c6a / c7i)
    "c5.large": 0.085,   "c5.xlarge": 0.170,   "c5.2xlarge": 0.340,
    "c5.4xlarge": 0.680, "c5.9xlarge": 1.530,  "c5.18xlarge": 3.060,
    "c5a.large": 0.077,  "c5a.xlarge": 0.154,  "c5a.2xlarge": 0.308,
    "c5a.4xlarge": 0.616,"c5a.8xlarge": 1.232, "c5a.16xlarge": 2.464,
    "c6i.large": 0.085,  "c6i.xlarge": 0.170,  "c6i.2xlarge": 0.340,
    "c6i.4xlarge": 0.680,"c6i.8xlarge": 1.360, "c6i.16xlarge": 2.720,
    "c6a.large": 0.076,  "c6a.xlarge": 0.153,  "c6a.2xlarge": 0.306,
    "c7i.large": 0.089,  "c7i.xlarge": 0.179,  "c7i.2xlarge": 0.357,
    "c7i.4xlarge": 0.714,"c7i.8xlarge": 1.428, "c7i.16xlarge": 2.856,
    # Memory optimized (r5 / r5a / r6i / r6a / r7i / x1e)
    "r5.large": 0.126,   "r5.xlarge": 0.252,   "r5.2xlarge": 0.504,
    "r5.4xlarge": 1.008, "r5.8xlarge": 2.016,  "r5.16xlarge": 4.032,
    "r5a.large": 0.113,  "r5a.xlarge": 0.226,  "r5a.2xlarge": 0.452,
    "r5a.4xlarge": 0.904,"r5a.8xlarge": 1.808, "r5a.16xlarge": 3.616,
    "r6i.large": 0.126,  "r6i.xlarge": 0.252,  "r6i.2xlarge": 0.504,
    "r6i.4xlarge": 1.008,"r6i.8xlarge": 2.016, "r6i.16xlarge": 4.032,
    "r6a.large": 0.113,  "r6a.xlarge": 0.226,  "r6a.2xlarge": 0.452,
    "r7i.large": 0.132,  "r7i.xlarge": 0.264,  "r7i.2xlarge": 0.528,
    "r7i.4xlarge": 1.056,"r7i.8xlarge": 2.113, "r7i.16xlarge": 4.226,
    "x1e.xlarge": 0.834, "x1e.2xlarge": 1.668, "x1e.4xlarge": 3.336,
    "x1e.8xlarge": 6.672,"x1e.16xlarge":13.344,"x1e.32xlarge":26.688,
    # Storage optimized (i3 / i3en / i4i)
    "i3.large": 0.156,   "i3.xlarge": 0.312,   "i3.2xlarge": 0.624,
    "i3.4xlarge": 1.248, "i3.8xlarge": 2.496,  "i3.16xlarge": 4.992,
    "i3en.large": 0.226, "i3en.xlarge": 0.452, "i3en.2xlarge": 0.904,
    "i3en.3xlarge": 1.357,"i3en.6xlarge": 2.714,"i3en.12xlarge": 5.424,
    "i4i.large": 0.160,  "i4i.xlarge": 0.319,  "i4i.2xlarge": 0.638,
    "i4i.4xlarge": 1.277,"i4i.8xlarge": 2.554, "i4i.16xlarge": 5.107,
    # GPU — Accelerated computing (p3 / p4 / g4dn / g5)
    "p3.2xlarge": 3.060, "p3.8xlarge": 12.240, "p3.16xlarge": 24.480,
    "p4d.24xlarge": 32.773,
    "g4dn.xlarge": 0.526, "g4dn.2xlarge": 0.752, "g4dn.4xlarge": 1.204,
    "g4dn.8xlarge": 2.264,"g4dn.12xlarge": 3.912,"g4dn.16xlarge": 4.528,
    "g5.xlarge": 1.006,  "g5.2xlarge": 1.212,  "g5.4xlarge": 1.624,
    "g5.8xlarge": 2.448, "g5.16xlarge": 4.096, "g5.48xlarge": 16.288,
}
_EC2_HOURLY_DEFAULT = 0.096  # m5.large as conservative fallback

# ─────────────────────────────────────────────────────────────────────────────
# AWS Pricing API — live price resolution with hardcoded-table fallback.
#
# Strategy (Option 2):
#   1. Check the in-process cache (_PRICE_CACHE) first — zero network cost.
#   2. Query the AWS Pricing API (always us-east-1, free to call) for the
#      current on-demand rate of the exact instance/node type.
#   3. On any error (IAM permission denied, throttle, unknown type), fall
#      back silently to the hardcoded lookup tables above.
#
# Required IAM permission: pricing:GetProducts
# The Pricing API returns current list prices (no RI/Savings Plan discounts).
# ─────────────────────────────────────────────────────────────────────────────

# Module-level cache shared across all check functions for the lifetime of the
# process — avoids redundant API calls when the same type is seen many times.
_PRICE_CACHE: dict = {}  # e.g. { "ec2:m5.large": 0.096, "rds:db.m5.large": 0.171 }


def _get_ec2_price(instance_type: str, get_client_fn) -> float:
    """
    Return the on-demand Linux hourly rate for *instance_type* (us-east-1).
    Queries the AWS Pricing API first; silently falls back to _EC2_HOURLY.
    """
    cache_key = f"ec2:{instance_type}"
    if cache_key in _PRICE_CACHE:
        return _PRICE_CACHE[cache_key]
    try:
        pricing = get_client_fn("pricing", "us-east-1")
        resp = pricing.get_products(
            ServiceCode="AmazonEC2",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "instanceType",    "Value": instance_type},
                {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
                {"Type": "TERM_MATCH", "Field": "location",        "Value": "US East (N. Virginia)"},
                {"Type": "TERM_MATCH", "Field": "tenancy",         "Value": "Shared"},
                {"Type": "TERM_MATCH", "Field": "capacitystatus",  "Value": "Used"},
                {"Type": "TERM_MATCH", "Field": "preInstalledSw",  "Value": "NA"},
            ],
            MaxResults=1,
        )
        if resp.get("PriceList"):
            item  = _json.loads(resp["PriceList"][0])
            od    = item["terms"]["OnDemand"]
            dims  = next(iter(next(iter(od.values()))["priceDimensions"].values()))
            price = float(dims["pricePerUnit"]["USD"])
            if price > 0:
                _PRICE_CACHE[cache_key] = price
                return price
    except Exception:
        pass  # Pricing API unavailable or permission denied — use fallback
    return _EC2_HOURLY.get(instance_type, _EC2_HOURLY_DEFAULT)


def _get_rds_price(instance_class: str, get_client_fn) -> float:
    """
    Return the on-demand MySQL single-AZ hourly rate for *instance_class* (us-east-1).
    Queries the AWS Pricing API first; silently falls back to _RDS_HOURLY.
    """
    cache_key = f"rds:{instance_class}"
    if cache_key in _PRICE_CACHE:
        return _PRICE_CACHE[cache_key]
    try:
        pricing = get_client_fn("pricing", "us-east-1")
        resp = pricing.get_products(
            ServiceCode="AmazonRDS",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "instanceType",     "Value": instance_class},
                {"Type": "TERM_MATCH", "Field": "databaseEngine",   "Value": "MySQL"},
                {"Type": "TERM_MATCH", "Field": "location",         "Value": "US East (N. Virginia)"},
                {"Type": "TERM_MATCH", "Field": "deploymentOption", "Value": "Single-AZ"},
            ],
            MaxResults=1,
        )
        if resp.get("PriceList"):
            item  = _json.loads(resp["PriceList"][0])
            od    = item["terms"]["OnDemand"]
            dims  = next(iter(next(iter(od.values()))["priceDimensions"].values()))
            price = float(dims["pricePerUnit"]["USD"])
            if price > 0:
                _PRICE_CACHE[cache_key] = price
                return price
    except Exception:
        pass
    return _RDS_HOURLY.get(instance_class, _RDS_HOURLY_DEFAULT)


def _get_elasticache_price(node_type: str, get_client_fn) -> float:
    """
    Return the on-demand Redis hourly rate for *node_type* (us-east-1).
    Queries the AWS Pricing API first; silently falls back to _ELASTICACHE_HOURLY.
    """
    cache_key = f"elasticache:{node_type}"
    if cache_key in _PRICE_CACHE:
        return _PRICE_CACHE[cache_key]
    try:
        pricing = get_client_fn("pricing", "us-east-1")
        resp = pricing.get_products(
            ServiceCode="AmazonElastiCache",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "cacheNodeType", "Value": node_type},
                {"Type": "TERM_MATCH", "Field": "location",      "Value": "US East (N. Virginia)"},
            ],
            MaxResults=1,
        )
        if resp.get("PriceList"):
            item  = _json.loads(resp["PriceList"][0])
            od    = item["terms"]["OnDemand"]
            dims  = next(iter(next(iter(od.values()))["priceDimensions"].values()))
            price = float(dims["pricePerUnit"]["USD"])
            if price > 0:
                _PRICE_CACHE[cache_key] = price
                return price
    except Exception:
        pass
    return _ELASTICACHE_HOURLY.get(node_type, _ELASTICACHE_HOURLY_DEFAULT)


def _get_redshift_price(node_type: str, get_client_fn) -> float:
    """
    Return the on-demand hourly rate per node for *node_type* (us-east-1).
    Queries the AWS Pricing API first; silently falls back to _REDSHIFT_HOURLY.
    """
    cache_key = f"redshift:{node_type}"
    if cache_key in _PRICE_CACHE:
        return _PRICE_CACHE[cache_key]
    try:
        pricing = get_client_fn("pricing", "us-east-1")
        resp = pricing.get_products(
            ServiceCode="AmazonRedshift",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "nodeType", "Value": node_type},
                {"Type": "TERM_MATCH", "Field": "location", "Value": "US East (N. Virginia)"},
            ],
            MaxResults=1,
        )
        if resp.get("PriceList"):
            item  = _json.loads(resp["PriceList"][0])
            od    = item["terms"]["OnDemand"]
            dims  = next(iter(next(iter(od.values()))["priceDimensions"].values()))
            price = float(dims["pricePerUnit"]["USD"])
            if price > 0:
                _PRICE_CACHE[cache_key] = price
                return price
    except Exception:
        pass
    return _REDSHIFT_HOURLY.get(node_type, _REDSHIFT_HOURLY_DEFAULT)


def check_iam(resources, regions, get_client):
    """IAM-wide checks: root usage, MFA, old access keys, over-permissive policies."""
    findings = []
    try:
        iam = get_client("iam", "us-east-1")

        # Root account usage check
        try:
            summary = iam.get_account_summary().get("SummaryMap", {})
            if summary.get("AccountMFAEnabled", 0) == 0:
                findings.append({
                    "severity":       "HIGH",
                    "domain":         "IAM",
                    "resource_id":    "root-account",
                    "resource_type":  "AWS::IAM::Root",
                    "region":         "global",
                    "issue":          "Root account MFA is not enabled",
                    "recommendation": "Enable MFA on the root account immediately. "
                                      "Use the root account only for tasks that specifically require it.",
                })
        except Exception as e:
            logger.debug(f"IAM root check failed: {e}")

        # IAM users: MFA, old access keys
        try:
            paginator = iam.get_paginator("list_users")
            now = datetime.now(timezone.utc)
            for page in paginator.paginate():
                for user in page.get("Users", []):
                    uid = user["UserName"]

                    # MFA check
                    try:
                        mfa_resp = iam.list_mfa_devices(UserName=uid)
                        if not mfa_resp.get("MFADevices"):
                            findings.append({
                                "severity":       "HIGH",
                                "domain":         "IAM",
                                "resource_id":    uid,
                                "resource_type":  "AWS::IAM::User",
                                "region":         "global",
                                "issue":          f"User '{uid}' does not have MFA enabled",
                                "recommendation": "Enable MFA for all IAM users with console access.",
                            })
                    except Exception:
                        pass

                    # Access key age check
                    try:
                        keys_resp = iam.list_access_keys(UserName=uid)
                        for key in keys_resp.get("AccessKeyMetadata", []):
                            if key.get("Status") != "Active":
                                continue
                            created = key.get("CreateDate")
                            if created and (now - created).days > config.IAM_KEY_MAX_AGE_DAYS:
                                findings.append({
                                    "severity":       "MEDIUM",
                                    "domain":         "IAM",
                                    "resource_id":    key["AccessKeyId"],
                                    "resource_type":  "AWS::IAM::AccessKey",
                                    "region":         "global",
                                    "issue":          (
                                        f"Access key for '{uid}' is "
                                        f"{(now - created).days} days old "
                                        f"(max {config.IAM_KEY_MAX_AGE_DAYS} days)"
                                    ),
                                    "recommendation": "Rotate access keys regularly. "
                                                      "Consider using IAM roles instead of long-lived keys.",
                                })
                    except Exception:
                        pass
        except Exception as e:
            logger.debug(f"IAM user enumeration failed: {e}")

        # Password policy
        try:
            policy = iam.get_account_password_policy().get("PasswordPolicy", {})
            if not policy.get("RequireUppercaseCharacters", False):
                findings.append({
                    "severity":       "LOW",
                    "domain":         "IAM",
                    "resource_id":    "password-policy",
                    "resource_type":  "AWS::IAM::PasswordPolicy",
                    "region":         "global",
                    "issue":          "Password policy does not require uppercase characters",
                    "recommendation": "Strengthen the account password policy.",
                })
        except iam.exceptions.NoSuchEntityException:
            findings.append({
                "severity":       "MEDIUM",
                "domain":         "IAM",
                "resource_id":    "password-policy",
                "resource_type":  "AWS::IAM::PasswordPolicy",
                "region":         "global",
                "issue":          "No IAM account password policy is configured",
                "recommendation": "Create a password policy enforcing minimum length, complexity, and rotation.",
            })
        except Exception as e:
            logger.debug(f"Password policy check failed: {e}")

    except Exception as e:
        logger.debug(f"IAM check failed: {e}")

    return findings


def check_s3(resources, regions, get_client):
    """S3 checks: public access block, encryption, versioning."""
    findings = []
    s3_resources = [r for r in resources if r.get("type") == "AWS::S3::Bucket"]

    for resource in s3_resources:
        bucket = resource.get("name")
        meta   = resource.get("metadata", {})
        region = meta.get("bucket_region", resource.get("region", "us-east-1"))

        # Public access block
        if not meta.get("public_access_blocked", False):
            findings.append({
                "severity":       "HIGH",
                "domain":         "Storage",
                "resource_id":    bucket,
                "resource_type":  "AWS::S3::Bucket",
                "region":         region,
                "issue":          f"S3 bucket '{bucket}' does not have Public Access Block fully enabled",
                "recommendation": "Enable all four S3 Public Access Block settings to prevent accidental public exposure.",
            })

        # Encryption
        if meta.get("encryption", "none").lower() in ("none", ""):
            findings.append({
                "severity":       "MEDIUM",
                "domain":         "Storage",
                "resource_id":    bucket,
                "resource_type":  "AWS::S3::Bucket",
                "region":         region,
                "issue":          f"S3 bucket '{bucket}' does not have server-side encryption enabled",
                "recommendation": "Enable SSE-S3 or SSE-KMS encryption on all S3 buckets.",
            })

        # Versioning
        if meta.get("versioning", "Disabled") not in ("Enabled",):
            findings.append({
                "severity":       "LOW",
                "domain":         "Storage",
                "resource_id":    bucket,
                "resource_type":  "AWS::S3::Bucket",
                "region":         region,
                "issue":          f"S3 bucket '{bucket}' does not have versioning enabled",
                "recommendation": "Enable versioning to protect against accidental deletion.",
            })

    return findings


def check_security_groups(resources, regions, get_client):
    """EC2 security group checks: open ports to 0.0.0.0/0 or ::/0."""
    findings = []
    sensitive_ports = set(config.SENSITIVE_PORTS)

    for region in regions:
        try:
            ec2 = get_client("ec2", region)
            paginator = ec2.get_paginator("describe_security_groups")
            for page in paginator.paginate():
                for sg in page.get("SecurityGroups", []):
                    sg_id   = sg["GroupId"]
                    sg_name = sg.get("GroupName", sg_id)

                    for perm in sg.get("IpPermissions", []):
                        from_port = perm.get("FromPort", 0)
                        to_port   = perm.get("ToPort", 65535)
                        protocol  = perm.get("IpProtocol", "all")

                        # Check for 0.0.0.0/0 or ::/0
                        open_cidrs = [
                            r["CidrIp"] for r in perm.get("IpRanges", [])
                            if r.get("CidrIp") in ("0.0.0.0/0",)
                        ]
                        open_cidrs += [
                            r["CidrIpv6"] for r in perm.get("Ipv6Ranges", [])
                            if r.get("CidrIpv6") in ("::/0",)
                        ]

                        if not open_cidrs:
                            continue

                        # Check sensitive ports
                        for port in sensitive_ports:
                            if protocol == "-1" or (from_port <= port <= to_port):
                                severity = "HIGH" if port in (22, 3389) else "MEDIUM"
                                findings.append({
                                    "severity":       severity,
                                    "domain":         "Network",
                                    "resource_id":    sg_id,
                                    "resource_type":  "AWS::EC2::SecurityGroup",
                                    "region":         region,
                                    "issue":          (
                                        f"Security group '{sg_name}' ({sg_id}) allows "
                                        f"inbound port {port} from {', '.join(open_cidrs)}"
                                    ),
                                    "recommendation": (
                                        f"Restrict port {port} to specific IP ranges. "
                                        "Use SSM Session Manager for SSH/RDP instead of direct access."
                                    ),
                                })
                        # Check all-traffic open
                        if protocol == "-1" and open_cidrs:
                            findings.append({
                                "severity":       "HIGH",
                                "domain":         "Network",
                                "resource_id":    sg_id,
                                "resource_type":  "AWS::EC2::SecurityGroup",
                                "region":         region,
                                "issue":          (
                                    f"Security group '{sg_name}' ({sg_id}) allows ALL inbound "
                                    f"traffic from {', '.join(open_cidrs)}"
                                ),
                                "recommendation": "Remove all-traffic inbound rules and allow only necessary ports.",
                            })
        except Exception as e:
            logger.debug(f"Security group check failed in {region}: {e}")

    return findings


def check_ebs_encryption(resources, regions, get_client):
    """Check for unencrypted EBS volumes attached to EC2 instances."""
    findings = []

    for region in regions:
        try:
            ec2 = get_client("ec2", region)
            paginator = ec2.get_paginator("describe_volumes")
            for page in paginator.paginate(
                Filters=[{"Name": "status", "Values": ["in-use"]}]
            ):
                for vol in page.get("Volumes", []):
                    if not vol.get("Encrypted", False):
                        vol_id = vol["VolumeId"]
                        instance_id = ""
                        attachments = vol.get("Attachments", [])
                        if attachments:
                            instance_id = attachments[0].get("InstanceId", "")
                        findings.append({
                            "severity":       "MEDIUM",
                            "domain":         "Storage",
                            "resource_id":    vol_id,
                            "resource_type":  "AWS::EC2::Volume",
                            "region":         region,
                            "issue":          (
                                f"EBS volume '{vol_id}' "
                                + (f"(attached to {instance_id}) " if instance_id else "")
                                + "is not encrypted"
                            ),
                            "recommendation": "Enable EBS encryption by default in the region and encrypt existing volumes.",
                        })
        except Exception as e:
            logger.debug(f"EBS encryption check failed in {region}: {e}")

    return findings


def check_rds(resources, regions, get_client):
    """RDS checks: public accessibility, encryption, backup retention, minor version upgrades."""
    findings = []
    rds_resources = [r for r in resources if r.get("service") == "rds"]

    for resource in rds_resources:
        rid    = resource.get("name")
        region = resource.get("region", "us-east-1")
        meta   = resource.get("metadata", {})

        if meta.get("publicly_accessible", False):
            findings.append({
                "severity":       "HIGH",
                "domain":         "Database",
                "resource_id":    rid,
                "resource_type":  "AWS::RDS::DBInstance",
                "region":         region,
                "issue":          f"RDS instance '{rid}' is publicly accessible",
                "recommendation": "Disable public accessibility and allow only authorized IP addresses.",
            })

        if not meta.get("storage_encrypted", False):
            findings.append({
                "severity":       "MEDIUM",
                "domain":         "Database",
                "resource_id":    rid,
                "resource_type":  "AWS::RDS::DBInstance",
                "region":         region,
                "issue":          f"RDS instance '{rid}' storage is not encrypted",
                "recommendation": "Enable RDS storage encryption. Requires a snapshot + restore for existing instances.",
            })

        backup_retention = meta.get("backup_retention_period", 0)
        if backup_retention < 7:
            findings.append({
                "severity":       "MEDIUM",
                "domain":         "Database",
                "resource_id":    rid,
                "resource_type":  "AWS::RDS::DBInstance",
                "region":         region,
                "issue":          f"RDS instance '{rid}' backup retention is {backup_retention} days (recommend ≥7)",
                "recommendation": "Set backup retention to at least 7 days for production databases.",
            })

    return findings


def check_dynamodb(resources, regions, get_client):
    """DynamoDB checks: encryption, PITR, billing mode."""
    findings = []
    ddb_resources = [r for r in resources if r.get("service") == "dynamodb"]

    for resource in ddb_resources:
        rid    = resource.get("name")
        region = resource.get("region", "us-east-1")
        meta   = resource.get("metadata", {})

        if meta.get("encryption_type", "DISABLED") == "DISABLED":
            findings.append({
                "severity":       "MEDIUM",
                "domain":         "Database",
                "resource_id":    rid,
                "resource_type":  "AWS::DynamoDB::Table",
                "region":         region,
                "issue":          f"DynamoDB table '{rid}' encryption is disabled",
                "recommendation": "Enable DynamoDB server-side encryption with KMS.",
            })

        if not meta.get("pitr_enabled", False):
            findings.append({
                "severity":       "LOW",
                "domain":         "Database",
                "resource_id":    rid,
                "resource_type":  "AWS::DynamoDB::Table",
                "region":         region,
                "issue":          f"DynamoDB table '{rid}' does not have Point-in-Time Recovery (PITR) enabled",
                "recommendation": "Enable PITR for production tables to allow recovery to any point in the last 35 days.",
            })

    return findings


def check_elasticache(resources, regions, get_client):
    """ElastiCache checks: encryption at-rest and in-transit, auth."""
    findings = []
    ec_resources = [r for r in resources if r.get("service") == "elasticache"]

    for resource in ec_resources:
        rid    = resource.get("name")
        region = resource.get("region", "us-east-1")
        meta   = resource.get("metadata", {})

        if not meta.get("encryption_at_rest", False):
            findings.append({
                "severity":       "MEDIUM",
                "domain":         "Cache",
                "resource_id":    rid,
                "resource_type":  "AWS::ElastiCache::CacheCluster",
                "region":         region,
                "issue":          f"ElastiCache cluster '{rid}' does not have encryption at rest",
                "recommendation": "Enable encryption at rest for ElastiCache clusters.",
            })

        if not meta.get("encryption_transit", False):
            findings.append({
                "severity":       "MEDIUM",
                "domain":         "Cache",
                "resource_id":    rid,
                "resource_type":  "AWS::ElastiCache::CacheCluster",
                "region":         region,
                "issue":          f"ElastiCache cluster '{rid}' does not have in-transit encryption",
                "recommendation": "Enable TLS for Redis/Memcached in-transit encryption.",
            })

    return findings


def check_eks(resources, regions, get_client):
    """EKS checks: public endpoint, encryption config."""
    findings = []
    eks_resources = [r for r in resources if r.get("service") == "eks"]

    for resource in eks_resources:
        rid    = resource.get("name")
        region = resource.get("region", "us-east-1")
        meta   = resource.get("metadata", {})

        if meta.get("endpoint_public_access", True) and not meta.get("endpoint_private_access", False):
            findings.append({
                "severity":       "HIGH",
                "domain":         "Kubernetes",
                "resource_id":    rid,
                "resource_type":  "AWS::EKS::Cluster",
                "region":         region,
                "issue":          f"EKS cluster '{rid}' has public endpoint access and no private endpoint",
                "recommendation": "Enable private endpoint access and restrict or disable public endpoint access.",
            })

        if not meta.get("encryption_config", False):
            findings.append({
                "severity":       "MEDIUM",
                "domain":         "Kubernetes",
                "resource_id":    rid,
                "resource_type":  "AWS::EKS::Cluster",
                "region":         region,
                "issue":          f"EKS cluster '{rid}' does not have secrets encryption configured",
                "recommendation": "Enable envelope encryption for Kubernetes secrets using a KMS key.",
            })

    return findings


def check_monitoring(resources, regions, get_client):
    """Check CloudTrail and GuardDuty are enabled."""
    findings = []

    # CloudTrail
    for region in regions[:1]:  # Check one region for multi-region trail
        try:
            ct = get_client("cloudtrail", region)
            trails = ct.describe_trails(includeShadowTrails=True).get("trailList", [])
            multi_region = [t for t in trails if t.get("IsMultiRegionTrail")]
            if not multi_region:
                findings.append({
                    "severity":       "HIGH",
                    "domain":         "Monitoring",
                    "resource_id":    "cloudtrail",
                    "resource_type":  "AWS::CloudTrail::Trail",
                    "region":         region,
                    "issue":          "No multi-region CloudTrail is configured",
                    "recommendation": "Enable a multi-region CloudTrail with log file validation and S3 encryption.",
                })
        except Exception as e:
            logger.debug(f"CloudTrail check failed: {e}")

    # GuardDuty
    for region in regions:
        try:
            gd = get_client("guardduty", region)
            detectors = gd.list_detectors().get("DetectorIds", [])
            if not detectors:
                findings.append({
                    "severity":       "HIGH",
                    "domain":         "Monitoring",
                    "resource_id":    f"guardduty-{region}",
                    "resource_type":  "AWS::GuardDuty::Detector",
                    "region":         region,
                    "issue":          f"GuardDuty is not enabled in {region}",
                    "recommendation": "Enable GuardDuty in all active regions for threat detection.",
                })
            else:
                detector_id = detectors[0]
                detail = gd.get_detector(DetectorId=detector_id)
                if detail.get("Status", "DISABLED") == "DISABLED":
                    findings.append({
                        "severity":       "HIGH",
                        "domain":         "Monitoring",
                        "resource_id":    detector_id,
                        "resource_type":  "AWS::GuardDuty::Detector",
                        "region":         region,
                        "issue":          f"GuardDuty detector in {region} is disabled",
                        "recommendation": "Enable the GuardDuty detector to resume threat detection.",
                    })
        except Exception as e:
            logger.debug(f"GuardDuty check failed in {region}: {e}")

    return findings


def check_redshift(resources, regions, get_client):
    """Redshift checks: public accessibility, encryption."""
    findings = []
    rs_resources = [r for r in resources if r.get("service") == "redshift"]

    for resource in rs_resources:
        rid    = resource.get("name")
        region = resource.get("region", "us-east-1")
        meta   = resource.get("metadata", {})

        if meta.get("publicly_accessible", False):
            findings.append({
                "severity":       "HIGH",
                "domain":         "Database",
                "resource_id":    rid,
                "resource_type":  "AWS::Redshift::Cluster",
                "region":         region,
                "issue":          f"Redshift cluster '{rid}' is publicly accessible",
                "recommendation": "Disable public accessibility and use a VPC for database access.",
            })

        if not meta.get("encrypted", False):
            findings.append({
                "severity":       "MEDIUM",
                "domain":         "Database",
                "resource_id":    rid,
                "resource_type":  "AWS::Redshift::Cluster",
                "region":         region,
                "issue":          f"Redshift cluster '{rid}' is not encrypted",
                "recommendation": "Enable Redshift cluster encryption.",
            })

    return findings


def check_sqs(resources, regions, get_client):
    """SQS checks: encryption."""
    findings = []
    sqs_resources = [r for r in resources if r.get("service") == "sqs"]

    for resource in sqs_resources:
        rid    = resource.get("name")
        region = resource.get("region", "us-east-1")
        meta   = resource.get("metadata", {})

        if not meta.get("kms_key", ""):
            findings.append({
                "severity":       "LOW",
                "domain":         "Messaging",
                "resource_id":    rid,
                "resource_type":  "AWS::SQS::Queue",
                "region":         region,
                "issue":          f"SQS queue '{rid}' does not use KMS encryption",
                "recommendation": "Enable server-side encryption with a KMS key for sensitive queues.",
            })

    return findings


# ────────────────────────────────────────────────────────────────────────────
# Enterprise security check functions
# ────────────────────────────────────────────────────────────────────────────

def check_iam_admin_policies(resources, regions, get_client):
    """
    IAM: Identify users and roles with AdministratorAccess policy or
    custom wildcard Action:'*' attached — a critical privilege escalation risk.
    """
    findings = []
    try:
        iam = get_client("iam", "us-east-1")

        # ── Users ──────────────────────────────────────────────────────────
        try:
            paginator = iam.get_paginator("list_users")
            for page in paginator.paginate():
                for user in page.get("Users", []):
                    uid = user["UserName"]
                    try:
                        att = iam.list_attached_user_policies(UserName=uid)
                        for pol in att.get("AttachedPolicies", []):
                            if pol["PolicyName"] == "AdministratorAccess":
                                findings.append({
                                    "severity":       "HIGH",
                                    "domain":         "IAM",
                                    "resource_id":    uid,
                                    "resource_type":  "AWS::IAM::User",
                                    "region":         "global",
                                    "issue":          f"User '{uid}' has AdministratorAccess policy attached",
                                    "recommendation": (
                                        "Follow least-privilege. Remove AdministratorAccess "
                                        "and grant only the permissions actually required."
                                    ),
                                })
                    except Exception:
                        pass
        except Exception as exc:
            logger.debug(f"IAM admin policy user check failed: {exc}")

        # ── Roles ──────────────────────────────────────────────────────────
        try:
            paginator = iam.get_paginator("list_roles")
            for page in paginator.paginate():
                for role in page.get("Roles", []):
                    rname = role["RoleName"]
                    # Skip AWS-managed service-linked roles
                    if role.get("Path", "").startswith("/aws-service-role/"):
                        continue
                    try:
                        att = iam.list_attached_role_policies(RoleName=rname)
                        for pol in att.get("AttachedPolicies", []):
                            if pol["PolicyName"] == "AdministratorAccess":
                                findings.append({
                                    "severity":       "HIGH",
                                    "domain":         "IAM",
                                    "resource_id":    rname,
                                    "resource_type":  "AWS::IAM::Role",
                                    "region":         "global",
                                    "issue":          f"Role '{rname}' has AdministratorAccess policy attached",
                                    "recommendation": (
                                        "Apply least-privilege. Restrict the role to only required permissions."
                                    ),
                                })
                    except Exception:
                        pass
        except Exception as exc:
            logger.debug(f"IAM admin policy role check failed: {exc}")

    except Exception as exc:
        logger.debug(f"check_iam_admin_policies failed: {exc}")

    return findings


def check_iam_unused_credentials(resources, regions, get_client):
    """
    IAM: Find users with console passwords or access keys unused for
    more than IAM_KEY_MAX_AGE_DAYS days (credential report-based).
    """
    findings = []
    try:
        iam = get_client("iam", "us-east-1")

        # Request credential report and wait up to 10 seconds for it to be ready
        for _ in range(5):
            resp = iam.generate_credential_report()
            if resp.get("State") == "COMPLETE":
                break
            _time.sleep(2)

        report    = iam.get_credential_report()
        raw       = report["Content"]
        content   = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        reader    = csv.DictReader(_io.StringIO(content))
        now       = datetime.now(timezone.utc)

        for row in reader:
            user = row.get("user", "")
            if user == "<root_account>":
                continue

            # Console last-used date
            last_login_str = row.get("password_last_used", "")
            if last_login_str not in ("no_information", "N/A", "not_supported", ""):
                try:
                    last_login = datetime.fromisoformat(last_login_str.replace("Z", "+00:00"))
                    days_idle  = (now - last_login).days
                    if days_idle > config.IAM_KEY_MAX_AGE_DAYS:
                        findings.append({
                            "severity":       "MEDIUM",
                            "domain":         "IAM",
                            "resource_id":    user,
                            "resource_type":  "AWS::IAM::User",
                            "region":         "global",
                            "issue":          f"IAM user '{user}' has not logged in for {days_idle} days",
                            "recommendation": "Disable or remove IAM users that are no longer active.",
                        })
                except Exception:
                    pass

            # Active access keys last-used date
            for key_num in ("1", "2"):
                key_active    = row.get(f"access_key_{key_num}_active", "false")
                key_last_used = row.get(f"access_key_{key_num}_last_used_date", "")
                if key_active == "true" and key_last_used not in ("N/A", "", "no_information"):
                    try:
                        key_last_dt  = datetime.fromisoformat(key_last_used.replace("Z", "+00:00"))
                        days_unused  = (now - key_last_dt).days
                        if days_unused > config.IAM_KEY_MAX_AGE_DAYS:
                            findings.append({
                                "severity":       "MEDIUM",
                                "domain":         "IAM",
                                "resource_id":    user,
                                "resource_type":  "AWS::IAM::AccessKey",
                                "region":         "global",
                                "issue":          (
                                    f"IAM user '{user}' access key {key_num} is active but "
                                    f"unused for {days_unused} days"
                                ),
                                "recommendation": "Deactivate or delete unused access keys.",
                            })
                    except Exception:
                        pass

    except Exception as exc:
        logger.debug(f"check_iam_unused_credentials failed: {exc}")

    return findings


def check_iam_inline_policies(resources, regions, get_client):
    """
    IAM: Identify users, roles, and groups that use inline policies
    instead of managed policies — makes auditing and reuse harder.
    """
    findings = []
    try:
        iam = get_client("iam", "us-east-1")

        # Users
        try:
            for page in iam.get_paginator("list_users").paginate():
                for user in page.get("Users", []):
                    uid = user["UserName"]
                    try:
                        inline = iam.list_user_policies(UserName=uid).get("PolicyNames", [])
                        if inline:
                            findings.append({
                                "severity":       "LOW",
                                "domain":         "IAM",
                                "resource_id":    uid,
                                "resource_type":  "AWS::IAM::User",
                                "region":         "global",
                                "issue":          (
                                    f"User '{uid}' has inline polic(ies): "
                                    f"{', '.join(inline[:5])}"
                                ),
                                "recommendation": (
                                    "Replace inline policies with managed policies "
                                    "for better reusability and auditability."
                                ),
                            })
                    except Exception:
                        pass
        except Exception as exc:
            logger.debug(f"IAM inline user check failed: {exc}")

        # Roles (skip service-linked)
        try:
            for page in iam.get_paginator("list_roles").paginate():
                for role in page.get("Roles", []):
                    rname = role["RoleName"]
                    if role.get("Path", "").startswith("/aws-service-role/"):
                        continue
                    try:
                        inline = iam.list_role_policies(RoleName=rname).get("PolicyNames", [])
                        if inline:
                            findings.append({
                                "severity":       "LOW",
                                "domain":         "IAM",
                                "resource_id":    rname,
                                "resource_type":  "AWS::IAM::Role",
                                "region":         "global",
                                "issue":          (
                                    f"Role '{rname}' has inline polic(ies): "
                                    f"{', '.join(inline[:5])}"
                                ),
                                "recommendation": (
                                    "Replace inline policies with managed policies "
                                    "for better governance."
                                ),
                            })
                    except Exception:
                        pass
        except Exception as exc:
            logger.debug(f"IAM inline role check failed: {exc}")

    except Exception as exc:
        logger.debug(f"check_iam_inline_policies failed: {exc}")

    return findings


def check_s3_bucket_policies(resources, regions, get_client):
    """
    S3: Find buckets with policies that grant public access
    via Principal:'*' — exposes data to the entire Internet.
    """
    findings = []
    try:
        s3 = get_client("s3", "us-east-1")

        try:
            buckets = s3.list_buckets().get("Buckets", [])
        except Exception as exc:
            logger.debug(f"S3 list_buckets failed: {exc}")
            return findings

        for bucket in buckets:
            bname = bucket["Name"]
            try:
                policy_str = s3.get_bucket_policy(Bucket=bname).get("Policy", "")
                policy     = _json.loads(policy_str)
                for stmt in policy.get("Statement", []):
                    principal = stmt.get("Principal", "")
                    is_public = principal == "*" or (
                        isinstance(principal, dict)
                        and principal.get("AWS") in ("*", ["*"])
                    )
                    if is_public and stmt.get("Effect", "Allow") == "Allow":
                        findings.append({
                            "severity":       "HIGH",
                            "domain":         "Storage",
                            "resource_id":    bname,
                            "resource_type":  "AWS::S3::Bucket",
                            "region":         "global",
                            "issue":          (
                                f"S3 bucket '{bname}' policy allows public access "
                                f"(Principal: '*')"
                            ),
                            "recommendation": (
                                "Remove wildcard principal from bucket policy. "
                                "Grant access only to specific principals or AWS services."
                            ),
                        })
                        break
            except s3.exceptions.NoSuchBucketPolicy:
                pass
            except Exception as exc:
                logger.debug(f"S3 bucket policy check for '{bname}' failed: {exc}")

    except Exception as exc:
        logger.debug(f"check_s3_bucket_policies failed: {exc}")

    return findings


def check_s3_access_logging(resources, regions, get_client):
    """S3: Find buckets that do not have server access logging enabled."""
    findings = []
    try:
        s3 = get_client("s3", "us-east-1")

        try:
            buckets = s3.list_buckets().get("Buckets", [])
        except Exception as exc:
            logger.debug(f"S3 list_buckets failed: {exc}")
            return findings

        for bucket in buckets:
            bname = bucket["Name"]
            try:
                logging_cfg = s3.get_bucket_logging(Bucket=bname).get("LoggingEnabled")
                if not logging_cfg:
                    findings.append({
                        "severity":       "LOW",
                        "domain":         "Storage",
                        "resource_id":    bname,
                        "resource_type":  "AWS::S3::Bucket",
                        "region":         "global",
                        "issue":          f"S3 bucket '{bname}' does not have access logging enabled",
                        "recommendation": (
                            "Enable S3 server access logging to a dedicated audit bucket "
                            "for compliance and incident investigation."
                        ),
                    })
            except Exception as exc:
                logger.debug(f"S3 access logging check for '{bname}' failed: {exc}")

    except Exception as exc:
        logger.debug(f"check_s3_access_logging failed: {exc}")

    return findings


def check_vpc_flow_logs(resources, regions, get_client):
    """Network: Find VPCs in every region that have no active flow logs."""
    findings = []
    for region in regions:
        try:
            ec2  = get_client("ec2", region)
            vpcs = ec2.describe_vpcs().get("Vpcs", [])
            if not vpcs:
                continue

            # Collect VPC IDs that already have an active flow log
            fl_resp   = ec2.describe_flow_logs(
                Filters=[{"Name": "resource-type", "Values": ["VPC"]}]
            )
            logged_ids = {
                fl["ResourceId"]
                for fl in fl_resp.get("FlowLogs", [])
                if fl.get("FlowLogStatus") == "ACTIVE"
            }

            for vpc in vpcs:
                vpc_id     = vpc["VpcId"]
                is_default = vpc.get("IsDefault", False)
                if vpc_id not in logged_ids:
                    findings.append({
                        "severity":       "MEDIUM",
                        "domain":         "Network",
                        "resource_id":    vpc_id,
                        "resource_type":  "AWS::EC2::VPC",
                        "region":         region,
                        "issue":          f"VPC '{vpc_id}' in {region} has no active flow logs",
                        "recommendation": (
                            "Enable VPC flow logs to S3 or CloudWatch Logs for network "
                            "traffic auditing and threat detection."
                        ),
                    })
        except Exception as exc:
            logger.debug(f"VPC flow log check failed in {region}: {exc}")

    return findings


def check_default_vpc(resources, regions, get_client):
    """
    Network: Flag default VPCs — especially those running production workloads.
    Default VPCs lack the security controls of custom, well-designed VPCs.
    """
    findings = []
    for region in regions:
        try:
            ec2   = get_client("ec2", region)
            vpcs  = ec2.describe_vpcs(
                Filters=[{"Name": "isDefault", "Values": ["true"]}]
            ).get("Vpcs", [])

            for vpc in vpcs:
                vpc_id = vpc["VpcId"]
                # Count running/stopped instances inside the default VPC
                inst_resp      = ec2.describe_instances(
                    Filters=[
                        {"Name": "vpc-id",             "Values": [vpc_id]},
                        {"Name": "instance-state-name","Values": ["running", "stopped"]},
                    ]
                )
                instance_count = sum(
                    len(r.get("Instances", []))
                    for r in inst_resp.get("Reservations", [])
                )
                # Only flag default VPCs that are actively in use — empty ones are noise
                if instance_count == 0:
                    continue
                findings.append({
                    "severity":       "MEDIUM",
                    "domain":         "Network",
                    "resource_id":    vpc_id,
                    "resource_type":  "AWS::EC2::VPC",
                    "region":         region,
                    "issue":          (
                        f"Default VPC '{vpc_id}' in {region} has {instance_count} "
                        f"instance(s) — production workloads should use custom VPCs"
                    ),
                    "recommendation": (
                        "Migrate workloads from the default VPC to a custom VPC with "
                        "proper network segmentation, private subnets, and NACLs."
                    ),
                })
        except Exception as exc:
            logger.debug(f"Default VPC check failed in {region}: {exc}")

    return findings


def check_sg_unrestricted_egress(resources, regions, get_client):
    """
    Network: Security groups with unrestricted egress (-1 protocol to 0.0.0.0/0).
    While common, this prevents defence-in-depth via egress filtering.
    """
    findings = []
    for region in regions:
        try:
            ec2 = get_client("ec2", region)
            for page in ec2.get_paginator("describe_security_groups").paginate():
                for sg in page.get("SecurityGroups", []):
                    sg_id   = sg["GroupId"]
                    sg_name = sg.get("GroupName", sg_id)
                    # Every VPC's default SG has this rule by design — skip to avoid noise
                    if sg_name == "default":
                        continue
                    for perm in sg.get("IpPermissionsEgress", []):
                        if perm.get("IpProtocol") != "-1":
                            continue
                        open_cidrs = [
                            r["CidrIp"]   for r in perm.get("IpRanges",   []) if r.get("CidrIp")   == "0.0.0.0/0"
                        ] + [
                            r["CidrIpv6"] for r in perm.get("Ipv6Ranges", []) if r.get("CidrIpv6") == "::/0"
                        ]
                        if open_cidrs:
                            findings.append({
                                "severity":       "LOW",
                                "domain":         "Network",
                                "resource_id":    sg_id,
                                "resource_type":  "AWS::EC2::SecurityGroup",
                                "region":         region,
                                "issue":          (
                                    f"Security group '{sg_name}' ({sg_id}) has "
                                    f"unrestricted all-traffic egress to {', '.join(open_cidrs)}"
                                ),
                                "recommendation": (
                                    "Restrict egress rules to only necessary "
                                    "destinations and ports for defence in depth."
                                ),
                            })
                            break  # One finding per SG is sufficient
        except Exception as exc:
            logger.debug(f"SG unrestricted egress check failed in {region}: {exc}")

    return findings


def check_ec2_imdsv2(resources, regions, get_client):
    """
    EC2: Instances not enforcing IMDSv2 (HttpTokens=optional) are
    vulnerable to SSRF attacks that can steal IAM credentials via the metadata service.
    """
    findings = []
    for region in regions:
        try:
            ec2 = get_client("ec2", region)
            for page in ec2.get_paginator("describe_instances").paginate(
                Filters=[{"Name": "instance-state-name", "Values": ["running", "stopped"]}]
            ):
                for reservation in page.get("Reservations", []):
                    for inst in reservation.get("Instances", []):
                        iid          = inst["InstanceId"]
                        http_tokens  = inst.get("MetadataOptions", {}).get("HttpTokens", "optional")
                        if http_tokens != "required":
                            name = next(
                                (t["Value"] for t in inst.get("Tags", []) if t["Key"] == "Name"),
                                iid,
                            )
                            findings.append({
                                "severity":       "HIGH",
                                "domain":         "Compute",
                                "resource_id":    iid,
                                "resource_type":  "AWS::EC2::Instance",
                                "region":         region,
                                "issue":          (
                                    f"EC2 instance '{name}' ({iid}) does not enforce "
                                    f"IMDSv2 (HttpTokens=optional)"
                                ),
                                "recommendation": (
                                    "Set HttpTokens to 'required' on existing instances using "
                                    "modify-instance-metadata-options CLI command. "
                                    "Enforce IMDSv2 via SCP to block future non-compliant launches."
                                ),
                            })
        except Exception as exc:
            logger.debug(f"EC2 IMDSv2 check failed in {region}: {exc}")

    return findings


def check_ec2_no_instance_profile(resources, regions, get_client):
    """
    EC2: Running instances without an IAM instance profile may be
    using hardcoded credentials — a security and operational risk.
    """
    findings = []
    for region in regions:
        try:
            ec2 = get_client("ec2", region)
            for page in ec2.get_paginator("describe_instances").paginate(
                Filters=[{"Name": "instance-state-name", "Values": ["running"]}]
            ):
                for reservation in page.get("Reservations", []):
                    for inst in reservation.get("Instances", []):
                        iid = inst["InstanceId"]
                        if not inst.get("IamInstanceProfile"):
                            name = next(
                                (t["Value"] for t in inst.get("Tags", []) if t["Key"] == "Name"),
                                iid,
                            )
                            findings.append({
                                "severity":       "MEDIUM",
                                "domain":         "Compute",
                                "resource_id":    iid,
                                "resource_type":  "AWS::EC2::Instance",
                                "region":         region,
                                "issue":          (
                                    f"EC2 instance '{name}' ({iid}) has no IAM instance profile"
                                ),
                                "recommendation": (
                                    "Attach a least-privilege IAM instance profile. "
                                    "Avoid hardcoding AWS credentials inside the instance."
                                ),
                            })
        except Exception as exc:
            logger.debug(f"EC2 instance profile check failed in {region}: {exc}")

    return findings


def check_ec2_stopped_long(resources, regions, get_client):
    """
    EC2: Instances stopped for more than 7 days still incur EBS storage
    costs and represent stale infrastructure that may accumulate security debt.
    """
    findings = []
    max_stopped_days = 7

    for region in regions:
        try:
            ec2 = get_client("ec2", region)
            now = datetime.now(timezone.utc)
            for page in ec2.get_paginator("describe_instances").paginate(
                Filters=[{"Name": "instance-state-name", "Values": ["stopped"]}]
            ):
                for reservation in page.get("Reservations", []):
                    for inst in reservation.get("Instances", []):
                        iid    = inst["InstanceId"]
                        reason = inst.get("StateTransitionReason", "")
                        # StateTransitionReason format: "User initiated (2024-01-15 10:00:00 GMT)"
                        match  = _re.search(
                            r"\((\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} GMT)\)", reason
                        )
                        if match:
                            try:
                                stopped_at   = datetime.strptime(
                                    match.group(1), "%Y-%m-%d %H:%M:%S GMT"
                                ).replace(tzinfo=timezone.utc)
                                days_stopped = (now - stopped_at).days
                                if days_stopped > max_stopped_days:
                                    name  = next(
                                        (t["Value"] for t in inst.get("Tags", []) if t["Key"] == "Name"),
                                        iid,
                                    )
                                    itype = inst.get("InstanceType", "unknown")
                                    # Stopped instances: no compute charge, but attached
                                    # EBS volumes still bill at full per-GB/month rate.
                                    ebs_cost = 0.0
                                    for bdm in inst.get("BlockDeviceMappings", []):
                                        vol_id = bdm.get("Ebs", {}).get("VolumeId", "")
                                        if vol_id:
                                            try:
                                                vol_info = ec2.describe_volumes(
                                                    VolumeIds=[vol_id]
                                                ).get("Volumes", [])
                                                if vol_info:
                                                    v = vol_info[0]
                                                    ebs_cost += v.get("Size", 0) * _EBS_GB_RATE.get(
                                                        v.get("VolumeType", "gp2"),
                                                        _EBS_GB_RATE_DEFAULT,
                                                    )
                                            except Exception:
                                                pass
                                    findings.append({
                                        "severity":       "LOW",
                                        "domain":         "Compute",
                                        "category":       "unused_resource",
                                        "resource_id":    iid,
                                        "resource_type":  "AWS::EC2::Instance",
                                        "region":         region,
                                        "estimated_monthly_cost_usd": round(ebs_cost, 2),
                                        "issue":          (
                                            f"EC2 instance '{name}' ({iid}, {itype}) "
                                            f"has been stopped for {days_stopped} days"
                                            + (f" — EBS storage ~${ebs_cost:.2f}/month" if ebs_cost else "")
                                        ),
                                        "recommendation": (
                                            "Terminate or snapshot unused stopped instances "
                                            "to reduce EBS costs and stale access risks."
                                        ),
                                    })
                            except Exception:
                                pass
        except Exception as exc:
            logger.debug(f"EC2 stopped-long check failed in {region}: {exc}")

    return findings


def check_rds_deletion_protection(resources, regions, get_client):
    """
    RDS: Instances without deletion protection can be accidentally or
    maliciously deleted, causing irreversible data loss.
    """
    findings = []
    for region in regions:
        try:
            rds = get_client("rds", region)
            for page in rds.get_paginator("describe_db_instances").paginate():
                for db in page.get("DBInstances", []):
                    dbid = db["DBInstanceIdentifier"]
                    if not db.get("DeletionProtection", False):
                        findings.append({
                            "severity":       "MEDIUM",
                            "domain":         "Database",
                            "resource_id":    dbid,
                            "resource_type":  "AWS::RDS::DBInstance",
                            "region":         region,
                            "issue":          (
                                f"RDS instance '{dbid}' does not have deletion protection enabled"
                            ),
                            "recommendation": (
                                "Enable deletion protection on all production RDS instances "
                                "to prevent accidental data loss."
                            ),
                        })
        except Exception as exc:
            logger.debug(f"RDS deletion protection check failed in {region}: {exc}")

    return findings


def check_rds_public_snapshots(resources, regions, get_client):
    """
    RDS: Manual snapshots shared with 'all' are publicly restorable
    by any AWS account — an immediate data exposure risk.
    """
    findings = []
    for region in regions:
        try:
            rds = get_client("rds", region)
            for page in rds.get_paginator("describe_db_snapshots").paginate(
                SnapshotType="manual"
            ):
                for snap in page.get("DBSnapshots", []):
                    snap_id = snap["DBSnapshotIdentifier"]
                    try:
                        result = rds.describe_db_snapshot_attributes(
                            DBSnapshotIdentifier=snap_id
                        ).get("DBSnapshotAttributesResult", {})
                        for attr in result.get("DBSnapshotAttributes", []):
                            if (
                                attr.get("AttributeName") == "restore"
                                and "all" in attr.get("AttributeValues", [])
                            ):
                                findings.append({
                                    "severity":       "HIGH",
                                    "domain":         "Database",
                                    "resource_id":    snap_id,
                                    "resource_type":  "AWS::RDS::DBSnapshot",
                                    "region":         region,
                                    "issue":          (
                                        f"RDS snapshot '{snap_id}' is publicly restorable "
                                        f"(shared with 'all')"
                                    ),
                                    "recommendation": (
                                        "Remove public restore permissions immediately. "
                                        "Publicly shared snapshots expose your database content."
                                    ),
                                })
                    except Exception:
                        pass
        except Exception as exc:
            logger.debug(f"RDS public snapshot check failed in {region}: {exc}")

    return findings


def check_rds_enhanced_monitoring(resources, regions, get_client):
    """
    RDS: Enhanced monitoring at ≤60-second granularity provides OS-level
    metrics that are essential for performance troubleshooting.
    """
    findings = []
    for region in regions:
        try:
            rds = get_client("rds", region)
            for page in rds.get_paginator("describe_db_instances").paginate():
                for db in page.get("DBInstances", []):
                    dbid = db["DBInstanceIdentifier"]
                    if not db.get("MonitoringInterval", 0):
                        findings.append({
                            "severity":       "LOW",
                            "domain":         "Database",
                            "resource_id":    dbid,
                            "resource_type":  "AWS::RDS::DBInstance",
                            "region":         region,
                            "issue":          (
                                f"RDS instance '{dbid}' does not have enhanced monitoring enabled"
                            ),
                            "recommendation": (
                                "Enable enhanced monitoring (1–60 second granularity) "
                                "for richer OS and RDS process metrics."
                            ),
                        })
        except Exception as exc:
            logger.debug(f"RDS enhanced monitoring check failed in {region}: {exc}")

    return findings


def check_lambda_public_invoke(resources, regions, get_client):
    """
    Lambda: Functions with resource-based policies granting Principal:'*'
    can be invoked by anyone on the Internet without authentication.
    """
    findings = []
    for region in regions:
        try:
            lam = get_client("lambda", region)
            for page in lam.get_paginator("list_functions").paginate():
                for func in page.get("Functions", []):
                    fname = func["FunctionName"]
                    try:
                        policy = _json.loads(
                            lam.get_policy(FunctionName=fname).get("Policy", "{}")
                        )
                        for stmt in policy.get("Statement", []):
                            principal = stmt.get("Principal", "")
                            is_public   = principal == "*" or (
                                isinstance(principal, dict)
                                and principal.get("AWS") in ("*", ["*"])
                            )
                            if is_public and stmt.get("Effect", "Allow") == "Allow":
                                findings.append({
                                    "severity":       "HIGH",
                                    "domain":         "Compute",
                                    "resource_id":    fname,
                                    "resource_type":  "AWS::Lambda::Function",
                                    "region":         region,
                                    "issue":          (
                                        f"Lambda function '{fname}' has a resource policy "
                                        f"allowing public invocation (Principal: '*')"
                                    ),
                                    "recommendation": (
                                        "Remove wildcard principal from Lambda resource policy. "
                                        "Grant invocation to specific services or accounts only."
                                    ),
                                })
                                break
                    except lam.exceptions.ResourceNotFoundException:
                        pass
                    except Exception:
                        pass
        except Exception as exc:
            logger.debug(f"Lambda public invoke check failed in {region}: {exc}")

    return findings


def check_lambda_deprecated_runtime(resources, regions, get_client):
    """
    Lambda: Functions using end-of-life or deprecated runtimes no longer
    receive security patches from AWS.
    """
    DEPRECATED_RUNTIMES = {
        "python3.6":     "EOL",
        "python3.7":     "EOL",
        "python3.8":     "Deprecated",
        "nodejs10.x":    "EOL",
        "nodejs12.x":    "EOL",
        "nodejs14.x":    "EOL",
        "nodejs16.x":    "Deprecated",
        "java8":         "Deprecated",
        "dotnetcore2.1": "EOL",
        "dotnetcore3.1": "EOL",
        "ruby2.5":       "EOL",
        "ruby2.7":       "EOL",
        "go1.x":         "Deprecated",
    }

    findings = []
    for region in regions:
        try:
            lam = get_client("lambda", region)
            for page in lam.get_paginator("list_functions").paginate():
                for func in page.get("Functions", []):
                    fname   = func["FunctionName"]
                    runtime = func.get("Runtime", "")
                    if runtime in DEPRECATED_RUNTIMES:
                        status   = DEPRECATED_RUNTIMES[runtime]
                        severity = "HIGH" if status == "EOL" else "MEDIUM"
                        findings.append({
                            "severity":       severity,
                            "domain":         "Compute",
                            "resource_id":    fname,
                            "resource_type":  "AWS::Lambda::Function",
                            "region":         region,
                            "issue":          (
                                f"Lambda function '{fname}' uses {status.lower()} "
                                f"runtime '{runtime}'"
                            ),
                            "recommendation": (
                                f"Upgrade to a supported runtime. '{runtime}' is "
                                f"{status.lower()} and receives no security patches from AWS."
                            ),
                        })
        except Exception as exc:
            logger.debug(f"Lambda deprecated runtime check failed in {region}: {exc}")

    return findings


def check_secrets_rotation(resources, regions, get_client):
    """
    Secrets Manager: Secrets without automatic rotation or last rotated
    more than SECRET_MAX_ROTATION_DAYS ago represent a long-term credential exposure risk.
    """
    findings = []
    now = datetime.now(timezone.utc)

    for region in regions:
        try:
            sm = get_client("secretsmanager", region)
            for page in sm.get_paginator("list_secrets").paginate():
                for secret in page.get("SecretList", []):
                    sid = secret.get("Name", secret.get("ARN", "unknown"))
                    rot = secret.get("RotationEnabled", False)

                    if not rot:
                        findings.append({
                            "severity":       "MEDIUM",
                            "domain":         "Secrets",
                            "resource_id":    sid,
                            "resource_type":  "AWS::SecretsManager::Secret",
                            "region":         region,
                            "issue":          f"Secret '{sid}' does not have automatic rotation enabled",
                            "recommendation": (
                                "Enable automatic rotation using a Lambda rotator. "
                                "Regularly rotated secrets limit the blast radius of a credential leak."
                            ),
                        })
                    else:
                        last_rot = secret.get("LastRotatedDate")
                        if last_rot:
                            days_since = (now - last_rot).days
                            if days_since > config.SECRET_MAX_ROTATION_DAYS:
                                findings.append({
                                    "severity":       "MEDIUM",
                                    "domain":         "Secrets",
                                    "resource_id":    sid,
                                    "resource_type":  "AWS::SecretsManager::Secret",
                                    "region":         region,
                                    "issue":          (
                                        f"Secret '{sid}' was last rotated {days_since} days ago "
                                        f"(threshold: {config.SECRET_MAX_ROTATION_DAYS}d)"
                                    ),
                                    "recommendation": (
                                        "Investigate why automatic rotation has not run. "
                                        "Check the rotation Lambda function for errors."
                                    ),
                                })
        except Exception as exc:
            logger.debug(f"Secrets rotation check failed in {region}: {exc}")

    return findings


def check_kms_key_rotation(resources, regions, get_client):
    """
    KMS: Customer-managed symmetric keys without annual automatic rotation
    increase risk if key material is ever compromised.
    """
    findings = []
    for region in regions:
        try:
            kms = get_client("kms", region)
            for page in kms.get_paginator("list_keys").paginate():
                for key in page.get("Keys", []):
                    key_id = key["KeyId"]
                    try:
                        meta = kms.describe_key(KeyId=key_id).get("KeyMetadata", {})
                        # Only check customer-managed, enabled, symmetric keys
                        if meta.get("KeyManager") != "CUSTOMER":
                            continue
                        if meta.get("KeySpec", "SYMMETRIC_DEFAULT") != "SYMMETRIC_DEFAULT":
                            continue
                        if meta.get("KeyState") not in ("Enabled",):
                            continue

                        rot_enabled = kms.get_key_rotation_status(
                            KeyId=key_id
                        ).get("KeyRotationEnabled", False)

                        if not rot_enabled:
                            # Try to get a human-readable alias for the key
                            alias = key_id
                            try:
                                aliases = kms.list_aliases(KeyId=key_id).get("Aliases", [])
                                if aliases:
                                    alias = aliases[0].get("AliasName", key_id)
                            except Exception:
                                pass

                            findings.append({
                                "severity":       "MEDIUM",
                                "domain":         "Encryption",
                                "resource_id":    key_id,
                                "resource_type":  "AWS::KMS::Key",
                                "region":         region,
                                "issue":          (
                                    f"KMS CMK '{alias}' ({key_id}) does not have "
                                    f"automatic annual key rotation enabled"
                                ),
                                "recommendation": (
                                    "Enable automatic key rotation (annual). AWS rotates the "
                                    "key material without changing the key ID or ARN, with zero downtime."
                                ),
                            })
                    except Exception:
                        pass
        except Exception as exc:
            logger.debug(f"KMS key rotation check failed in {region}: {exc}")

    return findings


def check_cloudwatch_log_retention(resources, regions, get_client):
    """
    CloudWatch Logs: Log groups without a retention policy keep logs
    indefinitely, driving up costs and complicating compliance.
    """
    findings = []
    for region in regions:
        try:
            logs               = get_client("logs", region)
            no_retention: list = []
            for page in logs.get_paginator("describe_log_groups").paginate():
                for lg in page.get("logGroups", []):
                    if lg.get("retentionInDays") is None:
                        no_retention.append(lg.get("logGroupName", "unknown"))

            if no_retention:
                count    = len(no_retention)
                examples = ", ".join(no_retention[:3])
                suffix   = f" ... (+{count - 3} more)" if count > 3 else ""
                findings.append({
                    "severity":       "LOW",
                    "domain":         "Monitoring",
                    "resource_id":    f"log-groups-no-retention-{region}",
                    "resource_type":  "AWS::Logs::LogGroup",
                    "region":         region,
                    "issue":          (
                        f"{count} CloudWatch log group(s) in {region} have no retention "
                        f"policy (e.g. {examples}{suffix})"
                    ),
                    "recommendation": (
                        "Set a retention policy (e.g., 90 or 365 days) on all log groups "
                        "to control costs and meet data-retention requirements."
                    ),
                })
        except Exception as exc:
            logger.debug(f"CloudWatch log retention check failed in {region}: {exc}")

    return findings


def check_alb_http_listeners(resources, regions, get_client):
    """
    ELB: Application/Network Load Balancers with plain HTTP listeners
    (without redirect to HTTPS) transmit data in cleartext.
    """
    findings = []
    for region in regions:
        try:
            elbv2 = get_client("elbv2", region)
            for page in elbv2.get_paginator("describe_load_balancers").paginate():
                for lb in page.get("LoadBalancers", []):
                    lb_arn  = lb["LoadBalancerArn"]
                    lb_name = lb.get("LoadBalancerName", lb_arn)
                    lb_type = lb.get("Type", "application").capitalize()
                    try:
                        for listener in elbv2.describe_listeners(
                            LoadBalancerArn=lb_arn
                        ).get("Listeners", []):
                            if listener.get("Protocol") != "HTTP":
                                continue
                            is_redirect = any(
                                a.get("Type") == "redirect"
                                and a.get("RedirectConfig", {}).get("Protocol") == "HTTPS"
                                for a in listener.get("DefaultActions", [])
                            )
                            if not is_redirect:
                                findings.append({
                                    "severity":       "MEDIUM",
                                    "domain":         "Network",
                                    "resource_id":    lb_name,
                                    "resource_type":  f"AWS::ElasticLoadBalancingV2::{lb_type}LoadBalancer",
                                    "region":         region,
                                    "issue":          (
                                        f"Load balancer '{lb_name}' has an HTTP listener "
                                        f"(port {listener.get('Port', 80)}) without HTTPS redirect"
                                    ),
                                    "recommendation": (
                                        "Add an HTTPS listener and configure the HTTP listener "
                                        "to redirect (301) to HTTPS. Disable plain HTTP access."
                                    ),
                                })
                    except Exception:
                        pass
        except Exception as exc:
            logger.debug(f"ALB HTTP listener check failed in {region}: {exc}")

    return findings


def check_eip_unused(resources, regions, get_client):
    """
    EC2: Unassociated Elastic IPs incur unnecessary charges and represent
    orphaned infrastructure.
    """
    findings = []
    for region in regions:
        try:
            ec2 = get_client("ec2", region)
            for addr in ec2.describe_addresses().get("Addresses", []):
                # Unassociated = no InstanceId AND no NetworkInterfaceId
                if not addr.get("InstanceId") and not addr.get("NetworkInterfaceId"):
                    eip   = addr.get("PublicIp", addr.get("AllocationId", "unknown"))
                    alloc = addr.get("AllocationId", "")
                    findings.append({
                        "severity":                   "LOW",
                        "domain":                     "Network",
                        "category":                   "unused_resource",
                        "resource_id":                alloc or eip,
                        "resource_type":              "AWS::EC2::EIP",
                        "region":                     region,
                        "estimated_monthly_cost_usd": round(0.005 * 730, 2),  # $3.65/month per idle EIP
                        "issue":          (
                            f"Elastic IP '{eip}' ({alloc}) is not associated "
                            f"with any instance or network interface"
                        ),
                        "recommendation": (
                            "Release unassociated Elastic IPs to avoid unnecessary charges "
                            "and reduce IP address sprawl."
                        ),
                    })
        except Exception as exc:
            logger.debug(f"EIP unused check failed in {region}: {exc}")

    return findings


def check_ebs_unattached(resources, regions, get_client):
    """
    EC2 / Storage: EBS volumes in 'available' status are not attached to any
    instance.  They incur full storage costs with zero benefit and represent
    stale infrastructure that may contain sensitive data.

    Cost estimate uses published on-demand prices per GB/month:
      gp3 ~$0.10 | gp2 / st1 / sc1 / standard ~$0.08 (conservative average)
    """
    findings = []
    for region in regions:
        try:
            ec2 = get_client("ec2", region)
            paginator = ec2.get_paginator("describe_volumes")
            for page in paginator.paginate(
                Filters=[{"Name": "status", "Values": ["available"]}]
            ):
                for vol in page.get("Volumes", []):
                    vol_id   = vol["VolumeId"]
                    vol_type = vol.get("VolumeType", "gp2")
                    size_gb  = vol.get("Size", 0)

                    # Skip sub-1 GB scratch volumes to avoid noise
                    if size_gb < 1:
                        continue

                    # Accurate monthly cost using published AWS EBS per-GB rates
                    cost_per_gb = _EBS_GB_RATE.get(vol_type, _EBS_GB_RATE_DEFAULT)
                    est_cost    = round(size_gb * cost_per_gb, 2)

                    name = next(
                        (t["Value"] for t in vol.get("Tags", []) if t["Key"] == "Name"),
                        "",
                    )
                    display = f"'{name}' ({vol_id})" if name else f"'{vol_id}'"

                    findings.append({
                        "severity":                   "LOW",
                        "domain":                     "Storage",
                        "category":                   "unused_resource",
                        "resource_id":                vol_id,
                        "resource_type":              "AWS::EC2::Volume",
                        "region":                     region,
                        "estimated_monthly_cost_usd": est_cost,
                        "issue":          (
                            f"EBS volume {display} ({vol_type}, {size_gb} GB) is unattached "
                            f"— estimated monthly cost: ~${est_cost:.2f}"
                        ),
                        "recommendation": (
                            "Delete or snapshot-then-delete unattached EBS volumes to "
                            "eliminate unnecessary storage costs."
                        ),
                    })
        except Exception as exc:
            logger.debug(f"EBS unattached check failed in {region}: {exc}")

    return findings


def check_ec2_idle(resources, regions, get_client):
    """
    EC2: Running instances whose average CPU utilisation is below 1% over
    14 days are effectively idle — the workload has likely been migrated or
    decommissioned but the instance is still incurring full hourly charges.
    This threshold (1%) is deliberately much lower than the right-sizing
    threshold (10%) to flag only genuinely idle — not just under-utilised —
    compute.
    """
    findings       = []
    _CPU_THRESHOLD = 1.0   # percent — average over the lookback window
    _DAYS          = 14

    now   = datetime.now(timezone.utc)
    start = now - timedelta(days=_DAYS)

    for region in regions:
        try:
            ec2 = get_client("ec2", region)
            cw  = get_client("cloudwatch", region)
            for page in ec2.get_paginator("describe_instances").paginate(
                Filters=[{"Name": "instance-state-name", "Values": ["running"]}]
            ):
                for reservation in page.get("Reservations", []):
                    for inst in reservation.get("Instances", []):
                        iid   = inst["InstanceId"]
                        itype = inst.get("InstanceType", "unknown")
                        name  = next(
                            (t["Value"] for t in inst.get("Tags", []) if t["Key"] == "Name"),
                            iid,
                        )
                        try:
                            resp       = cw.get_metric_statistics(
                                Namespace="AWS/EC2",
                                MetricName="CPUUtilization",
                                Dimensions=[{"Name": "InstanceId", "Value": iid}],
                                StartTime=start,
                                EndTime=now,
                                Period=86400,       # daily buckets
                                Statistics=["Average"],
                            )
                            datapoints = resp.get("Datapoints", [])
                            if not datapoints:
                                continue  # no CW data — cannot assess
                            avg_cpu = sum(d["Average"] for d in datapoints) / len(datapoints)
                            if avg_cpu >= _CPU_THRESHOLD:
                                continue  # active enough
                            # Resolve hourly rate via AWS Pricing API (falls back to hardcoded table)
                            est_hourly   = _get_ec2_price(itype, get_client)
                            compute_cost = round(est_hourly * 730, 2)
                            # Add attached EBS volume cost for a complete monthly estimate
                            ebs_cost = 0.0
                            for bdm in inst.get("BlockDeviceMappings", []):
                                vol_id = bdm.get("Ebs", {}).get("VolumeId", "")
                                if vol_id:
                                    try:
                                        vol_info = ec2.describe_volumes(
                                            VolumeIds=[vol_id]
                                        ).get("Volumes", [])
                                        if vol_info:
                                            v = vol_info[0]
                                            ebs_cost += v.get("Size", 0) * _EBS_GB_RATE.get(
                                                v.get("VolumeType", "gp2"),
                                                _EBS_GB_RATE_DEFAULT,
                                            )
                                    except Exception:
                                        pass
                            est_monthly = round(compute_cost + ebs_cost, 2)
                            findings.append({
                                "severity":                   "MEDIUM",
                                "domain":                     "Compute",
                                "category":                   "idle_resource",
                                "resource_id":                iid,
                                "resource_type":              "AWS::EC2::Instance",
                                "region":                     region,
                                "estimated_monthly_cost_usd": est_monthly,
                                "issue": (
                                    f"EC2 instance '{name}' ({iid}, {itype}) is running "
                                    f"with avg CPU {avg_cpu:.2f}% over {_DAYS} days — "
                                    f"effectively idle (~${est_monthly}/month)"
                                ),
                                "recommendation": (
                                    "Confirm the workload is no longer needed, then stop or "
                                    "terminate the instance. Create a final AMI before "
                                    "termination if the configuration may be reused."
                                ),
                            })
                        except Exception as cw_exc:
                            logger.debug(f"CW CPU check failed for {iid}: {cw_exc}")
        except Exception as exc:
            logger.debug(f"EC2 idle check failed in {region}: {exc}")

    return findings


def check_rds_no_connections(resources, regions, get_client):
    """
    RDS idle / unused detection.  Three cases are handled:

    1. STOPPED instances — status is "stopped".  Instance-hour charges have
       ceased but the allocated storage volume keeps billing at full per-GB
       rates. These are flagged as idle_resource with the storage-only cost.

    2. AVAILABLE with 0 connections — status is "available" and the
       CloudWatch DatabaseConnections Maximum was 0 for every day in the
       last 14 days.  Full instance + storage cost is reported.

    3. AVAILABLE with no CloudWatch data — the metric has never been
       published (e.g., newly created or monitoring disabled).  Flagged as
       INFO so the operator knows to investigate.

    Previously only case 2 was detected; cases 1 and 3 were silently
    skipped, causing instances to vanish from the report after being stopped
    or when CloudWatch data wasn't available.
    """
    findings = []
    _DAYS    = 14

    now   = datetime.now(timezone.utc)
    start = now - timedelta(days=_DAYS)

    for region in regions:
        try:
            rds = get_client("rds", region)
            cw  = get_client("cloudwatch", region)
            for page in rds.get_paginator("describe_db_instances").paginate():
                for db in page.get("DBInstances", []):
                    status = db.get("DBInstanceStatus", "")
                    db_id  = db["DBInstanceIdentifier"]
                    cls    = db.get("DBInstanceClass", "unknown")
                    engine = db.get("Engine", "unknown")
                    storage_gb   = db.get("AllocatedStorage", 0)
                    storage_type = db.get("StorageType", "gp2")
                    storage_cost = round(
                        storage_gb * _RDS_STORAGE_GB_RATE.get(
                            storage_type, _RDS_STORAGE_GB_RATE_DEFAULT
                        ),
                        2,
                    )

                    # ── Case 1: Stopped instance — storage costs still apply ──
                    if status == "stopped":
                        findings.append({
                            "severity":                   "MEDIUM",
                            "domain":                     "Database",
                            "category":                   "idle_resource",
                            "resource_id":                db_id,
                            "resource_type":              "AWS::RDS::DBInstance",
                            "region":                     region,
                            "estimated_monthly_cost_usd": storage_cost,
                            "issue": (
                                f"RDS instance '{db_id}' ({cls}, {engine}) is STOPPED "
                                f"but still incurring storage charges "
                                f"(~${storage_cost:.2f}/month for {storage_gb} GB {storage_type})"
                            ),
                            "recommendation": (
                                "Take a final snapshot and delete the stopped instance to "
                                "eliminate ongoing storage costs, or restart it if the "
                                "workload is still required."
                            ),
                        })
                        continue

                    # Only run CloudWatch connection check for running instances
                    if status != "available":
                        continue

                    # ── Cases 2 & 3: Running instance — check connection metrics ──
                    try:
                        resp       = cw.get_metric_statistics(
                            Namespace="AWS/RDS",
                            MetricName="DatabaseConnections",
                            Dimensions=[{"Name": "DBInstanceIdentifier", "Value": db_id}],
                            StartTime=start,
                            EndTime=now,
                            Period=86400,
                            Statistics=["Maximum"],
                        )
                        datapoints = resp.get("Datapoints", [])

                        # Case 3: No CloudWatch data at all — flag as INFO
                        if not datapoints:
                            findings.append({
                                "severity":                   "INFO",
                                "domain":                     "Database",
                                "category":                   "idle_resource",
                                "resource_id":                db_id,
                                "resource_type":              "AWS::RDS::DBInstance",
                                "region":                     region,
                                "estimated_monthly_cost_usd": round(
                                    (_get_rds_price(cls, get_client) * _HOURS_PER_MONTH)
                                    + storage_cost,
                                    2,
                                ),
                                "issue": (
                                    f"RDS instance '{db_id}' ({cls}, {engine}) has no "
                                    f"CloudWatch DatabaseConnections data in the last "
                                    f"{_DAYS} days — monitoring may be disabled or the "
                                    f"instance may be unused"
                                ),
                                "recommendation": (
                                    "Enable Enhanced Monitoring and verify the instance "
                                    "is actively serving traffic. Delete if unused."
                                ),
                            })
                            continue

                        # Has had at least one connection — not idle
                        if max(d["Maximum"] for d in datapoints) > 0:
                            continue

                        # Case 2: Available but 0 connections — idle, full cost applies
                        hourly   = _get_rds_price(cls, get_client)
                        est_cost = round((hourly * _HOURS_PER_MONTH) + storage_cost, 2)
                        findings.append({
                            "severity":                   "MEDIUM",
                            "domain":                     "Database",
                            "category":                   "idle_resource",
                            "resource_id":                db_id,
                            "resource_type":              "AWS::RDS::DBInstance",
                            "region":                     region,
                            "estimated_monthly_cost_usd": est_cost,
                            "issue": (
                                f"RDS instance '{db_id}' ({cls}, {engine}) has had "
                                f"ZERO database connections over {_DAYS} days "
                                f"(est. cost: ~${est_cost:.2f}/month)"
                            ),
                            "recommendation": (
                                "Take a final snapshot then delete the instance if it is "
                                "no longer needed. To preserve configuration, consider "
                                "using RDS Stop Instance to halt billing temporarily."
                            ),
                        })
                    except Exception as cw_exc:
                        logger.debug(f"CW connections check failed for {db_id}: {cw_exc}")
        except Exception as exc:
            logger.debug(f"RDS idle check failed in {region}: {exc}")

    return findings


def check_lambda_no_invocations(resources, regions, get_client):
    """
    Lambda: Functions with zero invocations over 30 days represent stale
    code deployments.  While Lambda itself has near-zero idle cost, unused
    functions add attack surface, inflate IAM blast-radius, and accumulate
    configuration drift.
    """
    findings = []
    _DAYS    = 30

    now   = datetime.now(timezone.utc)
    start = now - timedelta(days=_DAYS)

    for region in regions:
        try:
            lam    = get_client("lambda", region)
            cw     = get_client("cloudwatch", region)
            marker = None
            while True:
                kwargs = {"Marker": marker} if marker else {}
                resp   = lam.list_functions(**kwargs)
                for fn in resp.get("Functions", []):
                    fn_name = fn["FunctionName"]
                    runtime = fn.get("Runtime", "unknown")
                    try:
                        cw_resp    = cw.get_metric_statistics(
                            Namespace="AWS/Lambda",
                            MetricName="Invocations",
                            Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
                            StartTime=start,
                            EndTime=now,
                            Period=86400,
                            Statistics=["Sum"],
                        )
                        datapoints = cw_resp.get("Datapoints", [])
                        if sum(d["Sum"] for d in datapoints) > 0:
                            continue
                        # Lambda has no base cost; estimate storage at $0.20/GB/month.
                        code_size_bytes = fn.get("CodeSize", 0)
                        est_cost        = round((code_size_bytes / (1024 ** 3)) * 0.20, 4)
                        findings.append({
                            "severity":                   "LOW",
                            "domain":                     "Serverless",
                            "category":                   "idle_resource",
                            "resource_id":                fn_name,
                            "resource_type":              "AWS::Lambda::Function",
                            "region":                     region,
                            "estimated_monthly_cost_usd": est_cost,
                            "issue": (
                                f"Lambda function '{fn_name}' ({runtime}) has had "
                                f"ZERO invocations over {_DAYS} days"
                            ),
                            "recommendation": (
                                "Delete unused Lambda functions to reduce attack surface "
                                "and stale infrastructure. Archive the deployment package "
                                "to S3 first if future use is possible."
                            ),
                        })
                    except Exception as cw_exc:
                        logger.debug(f"CW invocations check failed for {fn_name}: {cw_exc}")
                marker = resp.get("NextMarker")
                if not marker:
                    break
        except Exception as exc:
            logger.debug(f"Lambda idle check failed in {region}: {exc}")

    return findings


def check_natgw_idle(resources, regions, get_client):
    """
    Network: NAT Gateways with zero outbound bytes over 14 days are idle.
    Each idle NAT Gateway incurs ~$32-45 USD/month in base hourly charges
    (excluding per-GB data processing fees) with no active traffic.
    """
    findings    = []
    _DAYS       = 14
    _EST_HOURLY = 0.045          # USD/hour — approximate (us-east-1 rate)
    _EST_MONTHLY = round(_EST_HOURLY * 730, 2)

    now   = datetime.now(timezone.utc)
    start = now - timedelta(days=_DAYS)

    for region in regions:
        try:
            ec2 = get_client("ec2", region)
            cw  = get_client("cloudwatch", region)
            for page in ec2.get_paginator("describe_nat_gateways").paginate(
                Filters=[{"Name": "state", "Values": ["available"]}]
            ):
                for ngw in page.get("NatGateways", []):
                    ngw_id = ngw["NatGatewayId"]
                    tags   = {t["Key"]: t["Value"] for t in ngw.get("Tags", [])}
                    name   = tags.get("Name", ngw_id)
                    try:
                        resp       = cw.get_metric_statistics(
                            Namespace="AWS/NATGateway",
                            MetricName="BytesOutToDestination",
                            Dimensions=[{"Name": "NatGatewayId", "Value": ngw_id}],
                            StartTime=start,
                            EndTime=now,
                            Period=86400,
                            Statistics=["Sum"],
                        )
                        datapoints = resp.get("Datapoints", [])
                        if sum(d["Sum"] for d in datapoints) > 0:
                            continue  # has active traffic
                        findings.append({
                            "severity":                   "MEDIUM",
                            "domain":                     "Network",
                            "category":                   "idle_resource",
                            "resource_id":                ngw_id,
                            "resource_type":              "AWS::EC2::NatGateway",
                            "region":                     region,
                            "estimated_monthly_cost_usd": _EST_MONTHLY,
                            "issue": (
                                f"NAT Gateway '{name}' ({ngw_id}) has zero outbound bytes "
                                f"over {_DAYS} days — idle at ~${_EST_MONTHLY}/month"
                            ),
                            "recommendation": (
                                "Delete the NAT Gateway if no private subnets require "
                                "outbound internet access. Verify route tables and any "
                                "pending workloads before deletion."
                            ),
                        })
                    except Exception as cw_exc:
                        logger.debug(f"CW NAT GW check failed for {ngw_id}: {cw_exc}")
        except Exception as exc:
            logger.debug(f"NAT GW idle check failed in {region}: {exc}")

    return findings


def check_elb_idle(resources, regions, get_client):
    """
    Load Balancing: ALBs / NLBs with zero requests (ALB) or zero active
    flows (NLB) over 14 days are idle — incurring base LCU/NLCU charges
    (~$16-22 USD/month) with no active traffic.
    """
    findings  = []
    _DAYS     = 14
    _ALB_BASE = round(0.022 * 730, 2)   # USD/month base for ALB (~$16)
    _NLB_BASE = round(0.028 * 730, 2)   # USD/month base for NLB (~$20)

    now   = datetime.now(timezone.utc)
    start = now - timedelta(days=_DAYS)

    for region in regions:
        try:
            elbv2 = get_client("elbv2", region)
            cw    = get_client("cloudwatch", region)
            for page in elbv2.get_paginator("describe_load_balancers").paginate():
                for lb in page.get("LoadBalancers", []):
                    if lb.get("State", {}).get("Code") != "active":
                        continue
                    lb_arn  = lb["LoadBalancerArn"]
                    lb_name = lb["LoadBalancerName"]
                    lb_type = lb.get("Type", "application")
                    # CW dimension value = the path after "loadbalancer/" in the ARN
                    lb_dim  = lb_arn.split(":loadbalancer/")[-1] if ":loadbalancer/" in lb_arn else lb_name
                    if lb_type == "application":
                        namespace = "AWS/ApplicationELB"
                        metric    = "RequestCount"
                        stat      = "Sum"
                        est_cost  = _ALB_BASE
                    else:
                        namespace = "AWS/NetworkELB"
                        metric    = "ActiveFlowCount"
                        stat      = "Maximum"
                        est_cost  = _NLB_BASE
                    try:
                        resp       = cw.get_metric_statistics(
                            Namespace=namespace,
                            MetricName=metric,
                            Dimensions=[{"Name": "LoadBalancer", "Value": lb_dim}],
                            StartTime=start,
                            EndTime=now,
                            Period=86400,
                            Statistics=[stat],
                        )
                        datapoints = resp.get("Datapoints", [])
                        traffic    = sum(d[stat] for d in datapoints) if datapoints else 0
                        if traffic > 0:
                            continue
                        findings.append({
                            "severity":                   "MEDIUM",
                            "domain":                     "Network",
                            "category":                   "idle_resource",
                            "resource_id":                lb_name,
                            "resource_type":              "AWS::ElasticLoadBalancingV2::LoadBalancer",
                            "region":                     region,
                            "estimated_monthly_cost_usd": est_cost,
                            "issue": (
                                f"{lb_type.capitalize()} load balancer '{lb_name}' has zero "
                                f"traffic over {_DAYS} days — idle at ~${est_cost}/month"
                            ),
                            "recommendation": (
                                "Delete the load balancer if no longer needed. "
                                "Review DNS CNAME records, target groups, and any "
                                "associated listeners before removal."
                            ),
                        })
                    except Exception as cw_exc:
                        logger.debug(f"CW ELB check failed for {lb_name}: {cw_exc}")
        except Exception as exc:
            logger.debug(f"ELB idle check failed in {region}: {exc}")

    return findings


def check_elasticache_idle(resources, regions, get_client):
    """
    ElastiCache: Clusters with zero cache hits AND zero cache misses over
    14 days are idle — no application is reading from them.  Full instance-
    hour billing continues regardless of traffic.
    """
    findings = []
    _DAYS    = 14
    now   = datetime.now(timezone.utc)
    start = now - timedelta(days=_DAYS)

    for region in regions:
        try:
            ec = get_client("elasticache", region)
            cw = get_client("cloudwatch", region)
            for page in ec.get_paginator("describe_cache_clusters").paginate(
                ShowCacheNodeInfo=True
            ):
                for cluster in page.get("CacheClusters", []):
                    if cluster.get("CacheClusterStatus") != "available":
                        continue
                    cid       = cluster["CacheClusterId"]
                    node_type = cluster.get("CacheNodeType", "unknown")
                    engine    = cluster.get("Engine", "unknown")
                    try:
                        hits   = cw.get_metric_statistics(
                            Namespace="AWS/ElastiCache",
                            MetricName="CacheHits",
                            Dimensions=[{"Name": "CacheClusterId", "Value": cid}],
                            StartTime=start, EndTime=now,
                            Period=86400, Statistics=["Sum"],
                        )
                        misses = cw.get_metric_statistics(
                            Namespace="AWS/ElastiCache",
                            MetricName="CacheMisses",
                            Dimensions=[{"Name": "CacheClusterId", "Value": cid}],
                            StartTime=start, EndTime=now,
                            Period=86400, Statistics=["Sum"],
                        )
                        total_hits   = sum(d["Sum"] for d in hits.get("Datapoints", []))
                        total_misses = sum(d["Sum"] for d in misses.get("Datapoints", []))
                        if total_hits + total_misses > 0:
                            continue
                        if not hits.get("Datapoints") and not misses.get("Datapoints"):
                            continue  # no CW data — cannot assess
                        num_nodes  = cluster.get("NumCacheNodes", 1) or 1
                        hourly     = _get_elasticache_price(node_type, get_client)
                        est_cost   = round(hourly * num_nodes * _HOURS_PER_MONTH, 2)
                        findings.append({
                            "severity":                   "MEDIUM",
                            "domain":                     "Caching",
                            "category":                   "idle_resource",
                            "resource_id":                cid,
                            "resource_type":              "AWS::ElastiCache::CacheCluster",
                            "region":                     region,
                            "estimated_monthly_cost_usd": est_cost,
                            "issue": (
                                f"ElastiCache cluster '{cid}' ({node_type}, {engine}) "
                                f"has zero cache hits and misses over {_DAYS} days — idle"
                            ),
                            "recommendation": (
                                "Confirm no application is using this cluster, then delete "
                                "it. For Redis, take a final backup snapshot first."
                            ),
                        })
                    except Exception as cw_exc:
                        logger.debug(f"CW ElastiCache check failed for {cid}: {cw_exc}")
        except Exception as exc:
            logger.debug(f"ElastiCache idle check failed in {region}: {exc}")

    return findings


def check_sqs_empty_inactive(resources, regions, get_client):
    """
    SQS: Queues with zero sent, received, and deleted messages over 14 days
    are inactive.  While SQS pricing is per-request (near-zero idle cost),
    idle queues indicate orphaned integrations that inflate operational noise.
    """
    findings = []
    _DAYS    = 14
    now   = datetime.now(timezone.utc)
    start = now - timedelta(days=_DAYS)

    for region in regions:
        try:
            sqs = get_client("sqs", region)
            cw  = get_client("cloudwatch", region)
            response = sqs.list_queues()
            for url in response.get("QueueUrls", []):
                qname = url.rsplit("/", 1)[-1]
                try:
                    metrics = ["NumberOfMessagesSent", "NumberOfMessagesReceived",
                               "NumberOfMessagesDeleted"]
                    total = 0
                    has_data = False
                    for metric in metrics:
                        resp = cw.get_metric_statistics(
                            Namespace="AWS/SQS",
                            MetricName=metric,
                            Dimensions=[{"Name": "QueueName", "Value": qname}],
                            StartTime=start, EndTime=now,
                            Period=86400, Statistics=["Sum"],
                        )
                        dps = resp.get("Datapoints", [])
                        if dps:
                            has_data = True
                        total += sum(d["Sum"] for d in dps)
                    if not has_data or total > 0:
                        continue
                    findings.append({
                        "severity":                   "LOW",
                        "domain":                     "Messaging",
                        "category":                   "idle_resource",
                        "resource_id":                qname,
                        "resource_type":              "AWS::SQS::Queue",
                        "region":                     region,
                        "estimated_monthly_cost_usd": 0.0,
                        "issue": (
                            f"SQS queue '{qname}' has had zero message activity "
                            f"over {_DAYS} days"
                        ),
                        "recommendation": (
                            "Delete unused SQS queues and remove associated IAM permissions "
                            "and event source mappings to reduce attack surface."
                        ),
                    })
                except Exception as cw_exc:
                    logger.debug(f"CW SQS check failed for {qname}: {cw_exc}")
        except Exception as exc:
            logger.debug(f"SQS idle check failed in {region}: {exc}")

    return findings


def check_redshift_idle(resources, regions, get_client):
    """
    Redshift: Clusters with zero database connections over 14 days are idle.
    Redshift clusters incur significant per-node-hour charges regardless of
    activity — an idle dc2.large node costs ~$180 USD/month.
    """
    findings = []
    _DAYS    = 14
    now   = datetime.now(timezone.utc)
    start = now - timedelta(days=_DAYS)

    for region in regions:
        try:
            rs = get_client("redshift", region)
            cw = get_client("cloudwatch", region)
            for page in rs.get_paginator("describe_clusters").paginate():
                for cluster in page.get("Clusters", []):
                    if cluster.get("ClusterStatus") != "available":
                        continue
                    cid        = cluster["ClusterIdentifier"]
                    node_type  = cluster.get("NodeType", "unknown")
                    node_count = cluster.get("NumberOfNodes", 1)
                    # Resolve node-type hourly rate via AWS Pricing API (falls back to hardcoded table)
                    hourly_per_node = _get_redshift_price(node_type, get_client)
                    est_cost        = round(hourly_per_node * node_count * 730, 2)
                    try:
                        resp = cw.get_metric_statistics(
                            Namespace="AWS/Redshift",
                            MetricName="DatabaseConnections",
                            Dimensions=[{"Name": "ClusterIdentifier", "Value": cid}],
                            StartTime=start, EndTime=now,
                            Period=86400, Statistics=["Maximum"],
                        )
                        dps = resp.get("Datapoints", [])
                        if not dps:
                            continue
                        if max(d["Maximum"] for d in dps) > 0:
                            continue
                        findings.append({
                            "severity":                   "HIGH",
                            "domain":                     "Database",
                            "category":                   "idle_resource",
                            "resource_id":                cid,
                            "resource_type":              "AWS::Redshift::Cluster",
                            "region":                     region,
                            "estimated_monthly_cost_usd": est_cost,
                            "issue": (
                                f"Redshift cluster '{cid}' ({node_type} × {node_count} nodes) "
                                f"has ZERO connections over {_DAYS} days "
                                f"— estimated cost ~${est_cost}/month"
                            ),
                            "recommendation": (
                                "Pause or delete the cluster. Use 'Pause cluster' in the "
                                "console to retain configuration without incurring compute "
                                "charges. Take a final snapshot before deletion."
                            ),
                        })
                    except Exception as cw_exc:
                        logger.debug(f"CW Redshift check failed for {cid}: {cw_exc}")
        except Exception as exc:
            logger.debug(f"Redshift idle check failed in {region}: {exc}")

    return findings


def check_ecs_empty_services(resources, regions, get_client):
    """
    ECS: Services with desiredCount=0 and runningCount=0 are dormant.
    The service itself has no compute cost but its attached load balancer
    target groups and CloudWatch log groups continue to incur charges.
    """
    findings = []
    for region in regions:
        try:
            ecs = get_client("ecs", region)
            for cluster_page in ecs.get_paginator("list_clusters").paginate():
                for cluster_arn in cluster_page.get("clusterArns", []):
                    cluster_name = cluster_arn.split("/")[-1]
                    try:
                        for svc_page in ecs.get_paginator("list_services").paginate(
                            cluster=cluster_arn
                        ):
                            arns = svc_page.get("serviceArns", [])
                            if not arns:
                                continue
                            for i in range(0, len(arns), 10):
                                batch = arns[i:i + 10]
                                desc  = ecs.describe_services(
                                    cluster=cluster_arn, services=batch
                                )
                                for svc in desc.get("services", []):
                                    if svc.get("status") != "ACTIVE":
                                        continue
                                    desired = svc.get("desiredCount", 0)
                                    running = svc.get("runningCount", 0)
                                    if desired > 0 or running > 0:
                                        continue
                                    svc_name = svc["serviceName"]
                                    # ECS service has no direct compute cost when scaled to 0;
                                    # ALB rule costs ~$0.008/hour if a listener rule is attached.
                                    findings.append({
                                        "severity":                   "LOW",
                                        "domain":                     "Compute",
                                        "category":                   "idle_resource",
                                        "resource_id":                svc_name,
                                        "resource_type":              "AWS::ECS::Service",
                                        "region":                     region,
                                        "estimated_monthly_cost_usd": 0.0,
                                        "issue": (
                                            f"ECS service '{svc_name}' in cluster "
                                            f"'{cluster_name}' has 0 desired and 0 running "
                                            f"tasks — service is dormant"
                                        ),
                                        "recommendation": (
                                            "Delete dormant ECS services to remove orphaned "
                                            "load balancer rules, target groups, and IAM task "
                                            "role references."
                                        ),
                                    })
                    except Exception as svc_exc:
                        logger.debug(
                            f"ECS service check failed for cluster {cluster_name}: {svc_exc}"
                        )
        except Exception as exc:
            logger.debug(f"ECS idle check failed in {region}: {exc}")

    return findings


def check_kinesis_idle(resources, regions, get_client):
    """
    Kinesis Data Streams: Streams with zero incoming records over 14 days
    are idle.  Each shard incurs ~$10.95 USD/month regardless of traffic.
    """
    findings = []
    _DAYS    = 14
    now   = datetime.now(timezone.utc)
    start = now - timedelta(days=_DAYS)

    for region in regions:
        try:
            ks = get_client("kinesis", region)
            cw = get_client("cloudwatch", region)
            names = []
            kwargs = {}
            while True:
                resp  = ks.list_streams(Limit=100, **kwargs)
                names.extend(resp.get("StreamNames", []))
                if not resp.get("HasMoreStreams"):
                    break
                kwargs = {"ExclusiveStartStreamName": names[-1]}
            for sname in names:
                try:
                    desc   = ks.describe_stream_summary(StreamName=sname)
                    summary = desc.get("StreamDescriptionSummary", {})
                    if summary.get("StreamStatus") != "ACTIVE":
                        continue
                    shards  = summary.get("OpenShardCount", 1)
                    est_cost = round(shards * 10.95, 2)
                    resp_cw = cw.get_metric_statistics(
                        Namespace="AWS/Kinesis",
                        MetricName="IncomingRecords",
                        Dimensions=[{"Name": "StreamName", "Value": sname}],
                        StartTime=start, EndTime=now,
                        Period=86400, Statistics=["Sum"],
                    )
                    dps   = resp_cw.get("Datapoints", [])
                    if not dps or sum(d["Sum"] for d in dps) > 0:
                        continue
                    findings.append({
                        "severity":                   "MEDIUM",
                        "domain":                     "Streaming",
                        "category":                   "idle_resource",
                        "resource_id":                sname,
                        "resource_type":              "AWS::Kinesis::Stream",
                        "region":                     region,
                        "estimated_monthly_cost_usd": est_cost,
                        "issue": (
                            f"Kinesis stream '{sname}' ({shards} shard(s)) has zero "
                            f"incoming records over {_DAYS} days "
                            f"— idle at ~${est_cost}/month"
                        ),
                        "recommendation": (
                            "Delete the stream or reduce the shard count to the minimum "
                            "(1) to lower costs. Ensure no producers rely on this stream "
                            "before modification."
                        ),
                    })
                except Exception as s_exc:
                    logger.debug(f"Kinesis stream check failed for {sname}: {s_exc}")
        except Exception as exc:
            logger.debug(f"Kinesis idle check failed in {region}: {exc}")

    return findings


def check_apigw_idle(resources, regions, get_client):
    """
    API Gateway (REST + HTTP): APIs with zero invocations over 14 days
    are idle.  API Gateway has no base charge, but idle APIs represent
    stale endpoints with open threat surface and orphaned integrations.
    """
    findings = []
    _DAYS    = 14
    now   = datetime.now(timezone.utc)
    start = now - timedelta(days=_DAYS)

    for region in regions:
        try:
            apigw = get_client("apigateway", region)
            cw    = get_client("cloudwatch", region)
            # REST APIs (v1)
            rest_apis = apigw.get_rest_apis().get("items", [])
            for api in rest_apis:
                api_id   = api["id"]
                api_name = api.get("name", api_id)
                try:
                    resp = cw.get_metric_statistics(
                        Namespace="AWS/ApiGateway",
                        MetricName="Count",
                        Dimensions=[{"Name": "ApiName", "Value": api_name}],
                        StartTime=start, EndTime=now,
                        Period=86400, Statistics=["Sum"],
                    )
                    dps = resp.get("Datapoints", [])
                    if not dps or sum(d["Sum"] for d in dps) > 0:
                        continue
                    findings.append({
                        "severity":                   "LOW",
                        "domain":                     "API",
                        "category":                   "idle_resource",
                        "resource_id":                api_id,
                        "resource_type":              "AWS::ApiGateway::RestApi",
                        "region":                     region,
                        "estimated_monthly_cost_usd": 0.0,
                        "issue": (
                            f"REST API '{api_name}' ({api_id}) has zero invocations "
                            f"over {_DAYS} days"
                        ),
                        "recommendation": (
                            "Remove unused API Gateway deployments and associated Lambda "
                            "integrations, IAM permissions, and custom domain mappings."
                        ),
                    })
                except Exception as cw_exc:
                    logger.debug(f"CW API GW check failed for {api_id}: {cw_exc}")
        except Exception as exc:
            logger.debug(f"API GW idle check failed in {region}: {exc}")

        # HTTP APIs (v2)
        try:
            apigwv2 = get_client("apigatewayv2", region)
            cw      = get_client("cloudwatch", region)
            v2_apis = apigwv2.get_apis().get("Items", [])
            for api in v2_apis:
                api_id   = api["ApiId"]
                api_name = api.get("Name", api_id)
                try:
                    resp = cw.get_metric_statistics(
                        Namespace="AWS/ApiGateway",
                        MetricName="Count",
                        Dimensions=[{"Name": "ApiId", "Value": api_id}],
                        StartTime=start, EndTime=now,
                        Period=86400, Statistics=["Sum"],
                    )
                    dps = resp.get("Datapoints", [])
                    if not dps or sum(d["Sum"] for d in dps) > 0:
                        continue
                    findings.append({
                        "severity":                   "LOW",
                        "domain":                     "API",
                        "category":                   "idle_resource",
                        "resource_id":                api_id,
                        "resource_type":              "AWS::ApiGatewayV2::Api",
                        "region":                     region,
                        "estimated_monthly_cost_usd": 0.0,
                        "issue": (
                            f"HTTP API '{api_name}' ({api_id}) has zero invocations "
                            f"over {_DAYS} days"
                        ),
                        "recommendation": (
                            "Delete unused HTTP APIs and clean up routes, integrations, "
                            "and authorisers to reduce attack surface."
                        ),
                    })
                except Exception as cw_exc:
                    logger.debug(f"CW API GWv2 check failed for {api_id}: {cw_exc}")
        except Exception as exc:
            logger.debug(f"API GWv2 idle check failed in {region}: {exc}")

    return findings


def check_sagemaker_idle_notebooks(resources, regions, get_client):
    """
    SageMaker: Notebook instances in 'InService' state that have not been
    accessed for 7+ days are idle.  ml.t3.medium costs ~$43/month; larger
    instances cost considerably more.
    """
    findings = []
    _IDLE_DAYS = 7

    # Rough on-demand hourly rates (ml family, USD)
    _SM_HOURLY = {
        "ml.t3.medium": 0.059, "ml.t3.large": 0.121, "ml.t3.xlarge": 0.242,
        "ml.m5.xlarge":  0.269,  "ml.m5.2xlarge": 0.538, "ml.m5.4xlarge": 1.075,
        "ml.p3.2xlarge": 3.825,
    }
    _DEFAULT_HOURLY = 0.269  # ml.m5.xlarge as a conservative default

    for region in regions:
        try:
            sm = get_client("sagemaker", region)
            for page in sm.get_paginator("list_notebook_instances").paginate(
                StatusEquals="InService"
            ):
                for nb in page.get("NotebookInstances", []):
                    nb_name    = nb["NotebookInstanceName"]
                    inst_type  = nb.get("InstanceType", "unknown")
                    last_mod   = nb.get("LastModifiedTime")
                    if not last_mod:
                        continue
                    # last_mod is already a timezone-aware datetime from boto3
                    now     = datetime.now(timezone.utc)
                    idle_d  = (now - last_mod).days
                    if idle_d < _IDLE_DAYS:
                        continue
                    hourly   = _SM_HOURLY.get(inst_type, _DEFAULT_HOURLY)
                    est_cost = round(hourly * 730, 2)
                    findings.append({
                        "severity":                   "MEDIUM",
                        "domain":                     "AI/ML",
                        "category":                   "idle_resource",
                        "resource_id":                nb_name,
                        "resource_type":              "AWS::SageMaker::NotebookInstance",
                        "region":                     region,
                        "estimated_monthly_cost_usd": est_cost,
                        "issue": (
                            f"SageMaker notebook '{nb_name}' ({inst_type}) is InService "
                            f"but has not been modified for {idle_d} days "
                            f"— ~${est_cost}/month"
                        ),
                        "recommendation": (
                            "Stop the notebook instance when not actively used. "
                            "SageMaker notebooks can be restarted in seconds; stopping "
                            "eliminates the instance-hour charge entirely."
                        ),
                    })
        except Exception as exc:
            logger.debug(f"SageMaker idle notebook check failed in {region}: {exc}")

    return findings


def check_snstopic_no_publish(resources, regions, get_client):
    """
    SNS: Topics with zero published messages over 14 days are inactive.
    SNS has no base charge, but orphaned topics hold dangling subscriptions,
    IAM policies, and endpoint registrations that can be a security risk.
    """
    findings = []
    _DAYS    = 14
    now   = datetime.now(timezone.utc)
    start = now - timedelta(days=_DAYS)

    for region in regions:
        try:
            sns = get_client("sns", region)
            cw  = get_client("cloudwatch", region)
            for page in sns.get_paginator("list_topics").paginate():
                for topic in page.get("Topics", []):
                    topic_arn  = topic["TopicArn"]
                    topic_name = topic_arn.split(":")[-1]
                    try:
                        resp = cw.get_metric_statistics(
                            Namespace="AWS/SNS",
                            MetricName="NumberOfMessagesPublished",
                            Dimensions=[{"Name": "TopicName", "Value": topic_name}],
                            StartTime=start, EndTime=now,
                            Period=86400, Statistics=["Sum"],
                        )
                        dps = resp.get("Datapoints", [])
                        if not dps or sum(d["Sum"] for d in dps) > 0:
                            continue
                        findings.append({
                            "severity":                   "LOW",
                            "domain":                     "Messaging",
                            "category":                   "idle_resource",
                            "resource_id":                topic_name,
                            "resource_type":              "AWS::SNS::Topic",
                            "region":                     region,
                            "estimated_monthly_cost_usd": 0.0,
                            "issue": (
                                f"SNS topic '{topic_name}' has zero published messages "
                                f"over {_DAYS} days"
                            ),
                            "recommendation": (
                                "Delete inactive SNS topics and revoke associated IAM "
                                "publish permissions. Unsubscribe all endpoints to prevent "
                                "dangling delivery attempts."
                            ),
                        })
                    except Exception as cw_exc:
                        logger.debug(f"CW SNS check failed for {topic_name}: {cw_exc}")
        except Exception as exc:
            logger.debug(f"SNS idle check failed in {region}: {exc}")

    return findings


def check_cloudtrail_validation(resources, regions, get_client):
    """
    CloudTrail: Trails without log file integrity validation cannot
    guarantee that log files have not been tampered with or deleted.
    """
    findings = []
    try:
        ct     = get_client("cloudtrail", "us-east-1")
        trails = ct.describe_trails(includeShadowTrails=False).get("trailList", [])

        for trail in trails:
            trail_name = trail.get("Name", trail.get("TrailARN", "unknown"))
            home_region = trail.get("HomeRegion", "global")

            # Log file validation
            if not trail.get("LogFileValidationEnabled", False):
                findings.append({
                    "severity":       "MEDIUM",
                    "domain":         "Monitoring",
                    "resource_id":    trail_name,
                    "resource_type":  "AWS::CloudTrail::Trail",
                    "region":         home_region,
                    "issue":          (
                        f"CloudTrail trail '{trail_name}' does not have "
                        f"log file integrity validation enabled"
                    ),
                    "recommendation": (
                        "Enable log file validation to detect tampered or deleted log files "
                        "using SHA-256 digest files."
                    ),
                })

            # Trail not currently logging
            try:
                status = ct.get_trail_status(Name=trail.get("TrailARN", trail_name))
                if not status.get("IsLogging", True):
                    findings.append({
                        "severity":       "HIGH",
                        "domain":         "Monitoring",
                        "resource_id":    trail_name,
                        "resource_type":  "AWS::CloudTrail::Trail",
                        "region":         home_region,
                        "issue":          f"CloudTrail trail '{trail_name}' is NOT currently logging",
                        "recommendation": (
                            "Start CloudTrail logging immediately. "
                            "Without it, API activity is unauditable."
                        ),
                    })
            except Exception:
                pass

    except Exception as exc:
        logger.debug(f"CloudTrail validation check failed: {exc}")

    return findings


# ────────────────────────────────────────────────────────────────────────────
# Unused VPC detection
# ────────────────────────────────────────────────────────────────────────────

def check_vpc_unused(resources, regions, get_client):
    """
    Network: Non-default VPCs with no in-use network interfaces are unused.

    An unused VPC consumes no cost directly, but represents stale networking
    infrastructure that creates confusion, inflates infrastructure inventory,
    and may harbour misconfigured security groups or NACLs.

    Detection method: query EC2 for all non-default VPCs in each region, then
    check whether the VPC contains any ENIs in "in-use" status.  An ENI is
    created for every EC2 instance, RDS instance, ELB, Lambda ENI, ElastiCache
    node, etc., so a VPC with zero in-use ENIs is genuinely empty.
    """
    findings = []
    for region in regions:
        try:
            ec2 = get_client("ec2", region)

            # Only inspect non-default VPCs — the default VPC may be empty
            # but is managed by AWS and should not be flagged here.
            vpcs_resp = ec2.describe_vpcs(
                Filters=[{"Name": "isDefault", "Values": ["false"]}]
            )
            for vpc in vpcs_resp.get("Vpcs", []):
                vpc_id   = vpc["VpcId"]
                vpc_cidr = vpc.get("CidrBlock", "")
                # Derive a human-readable name from the Name tag if present.
                tag_name = next(
                    (t["Value"] for t in vpc.get("Tags", []) if t["Key"] == "Name"),
                    vpc_id,
                )

                # Check for any in-use ENIs in this VPC.
                eni_resp = ec2.describe_network_interfaces(
                    Filters=[
                        {"Name": "vpc-id",  "Values": [vpc_id]},
                        {"Name": "status",  "Values": ["in-use"]},
                    ]
                )
                if eni_resp.get("NetworkInterfaces"):
                    continue  # VPC is in use — skip

                findings.append({
                    "severity":                 "LOW",
                    "domain":                   "Network",
                    "service":                  "Amazon VPC",
                    "resource_id":              vpc_id,
                    "resource_type":            "AWS::EC2::VPC",
                    "region":                   region,
                    "category":                 "unused_resource",
                    "estimated_monthly_cost_usd": 0.0,
                    "issue": (
                        f"VPC '{tag_name}' ({vpc_cidr}) has no in-use network interfaces "
                        f"— it contains no active resources."
                    ),
                    "recommendation": (
                        f"Verify that VPC {vpc_id} is no longer required. "
                        "Delete unused security groups, subnets, and route tables before "
                        "removing the VPC via the console or "
                        "'aws ec2 delete-vpc --vpc-id " + vpc_id + "'."
                    ),
                })
        except Exception as exc:
            logger.debug(f"VPC unused check failed in {region}: {exc}")

    return findings


# ────────────────────────────────────────────────────────────────────────────
# Function dispatch table (must be after all check function definitions)
# ────────────────────────────────────────────────────────────────────────────

CHECK_FUNCTIONS = {
    # ── Core (existing) ───────────────────────────────────────────────────
    "check_iam":                      check_iam,
    "check_s3":                       check_s3,
    "check_security_groups":          check_security_groups,
    "check_ebs_encryption":           check_ebs_encryption,
    "check_rds":                      check_rds,
    "check_dynamodb":                 check_dynamodb,
    "check_elasticache":              check_elasticache,
    "check_eks":                      check_eks,
    "check_monitoring":               check_monitoring,
    "check_redshift":                 check_redshift,
    "check_sqs":                      check_sqs,
    # ── Enterprise IAM ────────────────────────────────────────────────────
    "check_iam_admin_policies":       check_iam_admin_policies,
    "check_iam_unused_credentials":   check_iam_unused_credentials,
    "check_iam_inline_policies":      check_iam_inline_policies,
    # ── Enterprise S3 ─────────────────────────────────────────────────────
    "check_s3_bucket_policies":       check_s3_bucket_policies,
    "check_s3_access_logging":        check_s3_access_logging,
    # ── Enterprise Network / VPC ──────────────────────────────────────────
    "check_vpc_flow_logs":            check_vpc_flow_logs,
    "check_default_vpc":              check_default_vpc,
    "check_sg_unrestricted_egress":   check_sg_unrestricted_egress,
    # ── Enterprise Compute (EC2) ──────────────────────────────────────────
    "check_ec2_imdsv2":               check_ec2_imdsv2,
    "check_ec2_no_instance_profile":  check_ec2_no_instance_profile,
    "check_ec2_stopped_long":         check_ec2_stopped_long,
    # ── Enterprise Database (RDS) ─────────────────────────────────────────
    "check_rds_deletion_protection":  check_rds_deletion_protection,
    "check_rds_public_snapshots":     check_rds_public_snapshots,
    "check_rds_enhanced_monitoring":  check_rds_enhanced_monitoring,
    # ── Enterprise Compute (Lambda) ───────────────────────────────────────
    "check_lambda_public_invoke":     check_lambda_public_invoke,
    "check_lambda_deprecated_runtime": check_lambda_deprecated_runtime,
    # ── Enterprise Secrets & Encryption ──────────────────────────────────
    "check_secrets_rotation":         check_secrets_rotation,
    "check_kms_key_rotation":         check_kms_key_rotation,
    # ── Enterprise Monitoring ─────────────────────────────────────────────
    "check_cloudwatch_log_retention": check_cloudwatch_log_retention,
    "check_cloudtrail_validation":    check_cloudtrail_validation,
    # ── Enterprise Load Balancing ─────────────────────────────────────────
    "check_alb_http_listeners":       check_alb_http_listeners,
    # ── Enterprise Unused Resources ───────────────────────────────────────
    "check_eip_unused":               check_eip_unused,
    "check_ebs_unattached":           check_ebs_unattached,
    # ── Enterprise Idle Resources ─────────────────────────────────────────
    "check_ec2_idle":                    check_ec2_idle,
    "check_rds_no_connections":          check_rds_no_connections,
    "check_lambda_no_invocations":       check_lambda_no_invocations,
    "check_natgw_idle":                  check_natgw_idle,
    "check_elb_idle":                    check_elb_idle,
    "check_elasticache_idle":            check_elasticache_idle,
    "check_sqs_empty_inactive":          check_sqs_empty_inactive,
    "check_redshift_idle":               check_redshift_idle,
    "check_ecs_empty_services":          check_ecs_empty_services,
    "check_kinesis_idle":                check_kinesis_idle,
    "check_apigw_idle":                  check_apigw_idle,
    "check_sagemaker_idle_notebooks":    check_sagemaker_idle_notebooks,
    "check_snstopic_no_publish":         check_snstopic_no_publish,
    # ── Enterprise Unused VPCs ────────────────────────────────────────────
    "check_vpc_unused":                  check_vpc_unused,
}


# ────────────────────────────────────────────────────────────────────────────
# Main entry point
# ────────────────────────────────────────────────────────────────────────────

def run(resources: list, service_map: dict, regions: list) -> list:
    """
    Execute all applicable security checks and return consolidated findings.
    Checks run concurrently (up to 8 workers) so audit time equals the
    slowest parallel batch rather than the sum of all 33 checks.
    """
    logger.info("Starting security audit...")

    # Build a deduplicated, ordered list of check names from the registry
    ordered_checks: list = []
    seen: set = set()
    for check_names in SECURITY_REGISTRY.values():
        for name in check_names:
            if name not in seen:
                ordered_checks.append(name)
                seen.add(name)

    def _run_one(check_name: str) -> list:
        """Execute one named check and return its findings."""
        check_fn = CHECK_FUNCTIONS.get(check_name)
        if not check_fn:
            logger.warning(
                f"Security check '{check_name}' listed in registry but "
                f"not found in CHECK_FUNCTIONS — skipping."
            )
            return []
        try:
            findings = check_fn(resources, regions, aws_client.get_client)
            if findings:
                logger.info(f"  [{check_name}] {len(findings)} finding(s)")
            return findings
        except Exception as exc:
            logger.warning(f"Security check '{check_name}' failed: {exc}")
            return []

    all_findings: list = []
    # Cap at 8 threads to avoid overwhelming AWS API rate limits
    max_workers = min(8, len(ordered_checks))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_run_one, name): name for name in ordered_checks}
        for future in as_completed(futures):
            all_findings.extend(future.result())

    # Sort findings: HIGH first, then MEDIUM, LOW, INFO
    severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2, "INFO": 3}
    all_findings.sort(key=lambda f: severity_order.get(f.get("severity", "INFO"), 4))

    counts = {sev: sum(1 for f in all_findings if f.get("severity") == sev)
              for sev in ("HIGH", "MEDIUM", "LOW", "INFO")}
    logger.info(
        f"Security audit complete: {len(all_findings)} findings "
        f"(HIGH={counts['HIGH']}, MEDIUM={counts['MEDIUM']}, "
        f"LOW={counts['LOW']}, INFO={counts['INFO']})."
    )

    return all_findings
