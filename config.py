"""
config.py
---------
Loads and validates environment variables from .env.
Exposes all configuration constants used throughout the tool.
"""

import os
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── AWS Credentials ─────────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_REGIONS_RAW = os.getenv("AWS_REGIONS", "")
AWS_ACCOUNT_NAME = os.getenv("AWS_ACCOUNT_NAME", "AWS Account")

# ── GCP / Vertex AI ──────────────────────────────────────────────────────────
# Normalise the path so Windows backslash escape sequences in .env don't corrupt it
GCP_SA_KEY_PATH = os.path.normpath(os.getenv("GCP_SA_KEY_PATH", "")) if os.getenv("GCP_SA_KEY_PATH") else ""
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GCP_LOCATION = os.getenv("GCP_LOCATION", "us-central1")
# Stable GA model ID.  Override via GEMINI_MODEL in .env if your GCP project
# has access to a specific preview release.
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-pro")

# ── Analysis time windows (days) ─────────────────────────────────────────────
TIME_WINDOWS = [7, 15, 30]

# ── CloudWatch metric period (seconds) ───────────────────────────────────────
CW_PERIOD = 3600  # 1-hour buckets

# ── Spike detection threshold (Z-score equivalent: std deviations above mean) ─
SPIKE_THRESHOLD_STDDEV = 2.0

# ── Rightsizing thresholds ───────────────────────────────────────────────────
THRESHOLDS = {
    "ec2": {
        "avg_cpu_pct":   10.0,
        "peak_cpu_pct":  30.0,
        "under_cpu_pct": 85.0,   # sustained 30-day average; peak spikes are ignored
    },
    "rds": {
        "avg_cpu_pct": 15.0,
        "free_memory_pct": 80.0,  # FreeableMemory / total memory * 100 > this -> underutilised
    },
    "lambda": {
        "duration_pct_of_timeout": 20.0,  # avg duration < 20% of configured timeout
        "memory_pct_of_configured": 30.0,  # avg memory < 30% of configured
    },
    "elasticache": {
        "avg_cpu_pct": 20.0,
    },
    "redshift": {
        "avg_cpu_pct": 15.0,
    },
    "ecs": {
        "avg_cpu_pct": 20.0,
        "avg_memory_pct": 30.0,
    },
    "dynamodb": {
        "capacity_utilisation_pct": 25.0,  # consumed < 25% of provisioned -> switch to on-demand
    },
}

# ── Security check constants ─────────────────────────────────────────────────
IAM_KEY_MAX_AGE_DAYS = 90
SECRET_MAX_ROTATION_DAYS = 90

# ── Sensitive port list for SG checks ────────────────────────────────────────
SENSITIVE_PORTS = [22, 3389, 3306, 5432, 1433, 27017, 6379, 5601]

# ── Cloud Control API sweep ───────────────────────────────────────────────────
# When True, service_discovery will run a supplementary Cloud Control sweep
# that dynamically enumerates every AWS resource type supported in the account
# (e.g. Glue, Step Functions, MSK, OpenSearch, WAF, CodeBuild …).  This catches
# any service not covered by the built-in direct enumerators, at the cost of
# extra API calls.  Disable if discovery is already fast enough for your needs.
CLOUDCONTROL_SWEEP = True


def get_region_override():
    """Return list of region overrides from .env, or None to auto-discover."""
    if AWS_REGIONS_RAW.strip():
        return [r.strip() for r in AWS_REGIONS_RAW.split(",") if r.strip()]
    return None


def validate():
    """
    Validate required credentials are present.
    Raises EnvironmentError if any required variables are missing.
    """
    missing = []
    if not AWS_ACCESS_KEY_ID:
        missing.append("AWS_ACCESS_KEY_ID")
    if not AWS_SECRET_ACCESS_KEY:
        missing.append("AWS_SECRET_ACCESS_KEY")
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            f"Copy .env.example to .env and fill in your credentials."
        )
    logger.info("Credentials validated successfully.")
    return True
