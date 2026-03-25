"""
modules/pricing_estimator.py
------------------------------
Estimates actual monthly cost savings for every rightsizing recommendation
by looking up real On-Demand prices via the AWS Pricing API or applying
AWS-published fixed unit rates.

Pricing strategies by service
------------------------------
  rds          — Pricing API: current instance class vs actual next-smaller
                 class in the same family, filtered by real engine and
                 deployment model (Single-AZ vs Multi-AZ) from metadata.
  elasticache  — Pricing API: current node type vs next-smaller node type,
                 filtered by cache engine.  Multiplied by num_nodes from
                 metadata so cluster-level savings are accurate.
  redshift     — Pricing API: current node type vs next-smaller node type.
                 Multiplied by node_count from metadata.
  lambda       — GB-second compute model using real 30-day invocation count
                 and average duration from CloudWatch.  No Pricing API call.
  dynamodb     — Wasted provisioned capacity units × AWS-published fixed
                 hourly RCU/WCU rates.  No Pricing API call.
  ec2          — Intentionally excluded: AWS Cost Explorer native
                 recommendations already supply accurate EC2 savings in
                 cost_analyzer.py (including RI/Savings Plan discounts).
  ecs          — Not implemented: requires task-definition CPU/memory values
                 not captured in resource metadata (only ARN is stored).
                 Returns 0.0 until resource_collector is extended.
  eks          — Not implemented: cluster-level metric; underlying node
                 costs reflect EC2 instances, not modelled here.
                 Returns 0.0.

All Pricing API calls are LRU-cached within one tool run so the same
instance-type lookup is never made twice, regardless of how many identical
resources exist in the account.

Adding a new service
---------------------
  1. Implement a _savings_<service>(resource, recommendation_type, **kwargs)
     function that returns a float (USD, rounded to 2 decimal places).
  2. Add one entry to _STRATEGIES at the bottom of this file.
  No other file needs to change.
"""

import functools
import json
import logging

import modules.aws_client as aws_client

logger = logging.getLogger(__name__)

HOURS_PER_MONTH = 730  # AWS standard billing hours per month (365 days / 12 months × 24 hours)


# ─────────────────────────────────────────────────────────────────────────────
# Instance size ordering
#
# Applies to any dot-separated AWS instance/node/class naming scheme:
#   db.r5.2xlarge      prefix="db.r5"    suffix="2xlarge"
#   cache.r6g.xlarge   prefix="cache.r6g" suffix="xlarge"
#   dc2.large          prefix="dc2"       suffix="large"
#   t3.micro           prefix="t3"        suffix="micro"
# ─────────────────────────────────────────────────────────────────────────────

_SIZE_ORDER = [
    "nano", "micro", "small", "medium", "large",
    "xlarge", "2xlarge", "4xlarge", "8xlarge",
    "10xlarge", "12xlarge", "16xlarge", "24xlarge", "32xlarge", "48xlarge",
]


def next_smaller_instance(instance_type: str) -> str:
    """
    Return the next smaller size in the same instance family by splitting
    on the last dot and stepping one position down the _SIZE_ORDER list.

    Returns an empty string when:
      - The instance is already the smallest recognised size.
      - The size suffix is not in the known size list (custom/unknown types).
      - The input has no dot separator.
    """
    parts = instance_type.rsplit(".", 1)  # split on the last dot only
    if len(parts) != 2:
        return ""
    prefix, size = parts[0], parts[1]
    if size not in _SIZE_ORDER:
        return ""
    idx = _SIZE_ORDER.index(size)
    return f"{prefix}.{_SIZE_ORDER[idx - 1]}" if idx > 0 else ""


# ─────────────────────────────────────────────────────────────────────────────
# AWS region name → Pricing API "location" display name
# ─────────────────────────────────────────────────────────────────────────────

_REGION_TO_LOCATION: dict[str, str] = {
    "us-east-1":      "US East (N. Virginia)",
    "us-east-2":      "US East (Ohio)",
    "us-west-1":      "US West (N. California)",
    "us-west-2":      "US West (Oregon)",
    "eu-west-1":      "Europe (Ireland)",
    "eu-west-2":      "Europe (London)",
    "eu-west-3":      "Europe (Paris)",
    "eu-central-1":   "Europe (Frankfurt)",
    "eu-north-1":     "Europe (Stockholm)",
    "ap-southeast-1": "Asia Pacific (Singapore)",
    "ap-southeast-2": "Asia Pacific (Sydney)",
    "ap-northeast-1": "Asia Pacific (Tokyo)",
    "ap-northeast-2": "Asia Pacific (Seoul)",
    "ap-south-1":     "Asia Pacific (Mumbai)",
    "sa-east-1":      "South America (Sao Paulo)",
    "ca-central-1":   "Canada (Central)",
    "me-south-1":     "Middle East (Bahrain)",
    "af-south-1":     "Africa (Cape Town)",
}


# ─────────────────────────────────────────────────────────────────────────────
# Pricing API lookup — LRU-cached per unique combination of inputs so the
# same instance-type price is fetched at most once per tool invocation.
#
# extra_filters must be a tuple of (field_name, value) pairs — tuples are
# hashable and therefore compatible with functools.lru_cache.
# ─────────────────────────────────────────────────────────────────────────────

@functools.lru_cache(maxsize=512)
def _get_on_demand_hourly(service_code: str, instance_type: str,
                           region: str, extra_filters: tuple) -> float:
    """
    Look up the On-Demand hourly USD price for a given instance type and region
    using the AWS Pricing API (global endpoint: us-east-1).

    Returns 0.0 on any error (API unavailable, instance type not recognised,
    no matching price entry, etc.).
    """
    location = _REGION_TO_LOCATION.get(region, "US East (N. Virginia)")

    filters = [
        {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
        {"Type": "TERM_MATCH", "Field": "location",     "Value": location},
        {"Type": "TERM_MATCH", "Field": "termType",     "Value": "OnDemand"},
    ]
    for field, value in extra_filters:
        filters.append({"Type": "TERM_MATCH", "Field": field, "Value": value})

    try:
        pricing = aws_client.get_client("pricing", "us-east-1")
        resp = pricing.get_products(
            ServiceCode=service_code,
            Filters=filters,
            MaxResults=10,
        )
        for price_str in resp.get("PriceList", []):
            price_data = json.loads(price_str)
            terms = price_data.get("terms", {}).get("OnDemand", {})
            for term in terms.values():
                for dim in term.get("priceDimensions", {}).values():
                    usd = float(dim.get("pricePerUnit", {}).get("USD", 0))
                    if usd > 0:
                        return usd
    except Exception as exc:
        logger.debug(
            "Pricing API lookup failed [%s / %s / %s]: %s",
            service_code, instance_type, region, exc,
        )
    return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Pricing API filter builders and downsize target resolution
# ─────────────────────────────────────────────────────────────────────────────

# RDS engine name → Pricing API display name (module-level constant so it is
# shared between _build_rds_extra_filters and resolve_downsize_target).
_RDS_ENGINE_MAP: dict = {
    "mysql":             "MySQL",
    "postgres":          "PostgreSQL",
    "mariadb":           "MariaDB",
    "oracle-se2":        "Oracle",
    "oracle-ee":         "Oracle",
    "sqlserver-se":      "SQL Server",
    "sqlserver-ee":      "SQL Server",
    "sqlserver-ex":      "SQL Server",
    "sqlserver-web":     "SQL Server",
    "aurora-mysql":      "Aurora MySQL",
    "aurora-postgresql": "Aurora PostgreSQL",
}


def _build_rds_extra_filters(metadata: dict) -> tuple:
    """Build Pricing API extra_filters for RDS from resource metadata."""
    engine         = metadata.get("engine", "mysql")
    pricing_engine = _RDS_ENGINE_MAP.get(engine.lower(), engine.capitalize())
    deployment     = "Multi-AZ" if metadata.get("multi_az", False) else "Single-AZ"
    return (("databaseEngine", pricing_engine), ("deploymentOption", deployment))


def _build_elasticache_extra_filters(metadata: dict) -> tuple:
    """Build Pricing API extra_filters for ElastiCache from resource metadata."""
    _ENGINE_MAP = {"redis": "Redis", "memcached": "Memcached"}
    engine     = metadata.get("engine", "redis")
    cache_type = _ENGINE_MAP.get(engine.lower(), "Redis")
    return (("cacheType", cache_type),)


def _find_valid_smaller_via_api(service_code: str, current_type: str,
                                 region: str, extra_filters: tuple) -> str:
    """
    Walk down _SIZE_ORDER one step at a time below current_type and return
    the FIRST candidate whose On-Demand price is confirmed by the AWS
    Pricing API (price > 0).

    This guarantees the returned type actually exists in the service — for
    example, db.t3.nano does not exist in RDS so it is skipped and the
    search continues downward until all possibilities are exhausted.
    Returns "" when no valid smaller type can be confirmed.
    """
    parts = current_type.rsplit(".", 1)
    if len(parts) != 2:
        return ""
    prefix, size = parts[0], parts[1]
    if size not in _SIZE_ORDER:
        return ""
    idx = _SIZE_ORDER.index(size)
    for i in range(idx - 1, -1, -1):
        candidate = f"{prefix}.{_SIZE_ORDER[i]}"
        if _get_on_demand_hourly(service_code, candidate, region, extra_filters) > 0:
            return candidate
    return ""


def _find_valid_smaller_redshift(node_type: str, region: str) -> str:
    """
    Walk down _SIZE_ORDER for a Redshift node type.

    Redshift's Pricing API uses a 'nodetype' attribute filter that must match
    the candidate being looked up, so the filter tuple is rebuilt per candidate
    rather than reusing the original node_type filter.
    Returns the first confirmed smaller node type, or "".
    """
    parts = node_type.rsplit(".", 1)
    if len(parts) != 2:
        return ""
    prefix, size = parts[0], parts[1]
    if size not in _SIZE_ORDER:
        return ""
    idx = _SIZE_ORDER.index(size)
    for i in range(idx - 1, -1, -1):
        candidate = f"{prefix}.{_SIZE_ORDER[i]}"
        extra = (("nodetype", candidate),)
        if _get_on_demand_hourly("AmazonRedshift", candidate, region, extra) > 0:
            return candidate
    return ""


def _find_valid_larger_via_api(service_code: str, current_type: str,
                               region: str, extra_filters: tuple) -> str:
    """
    Walk UP _SIZE_ORDER one step at a time above current_type and return
    the FIRST candidate whose On-Demand price is confirmed by the AWS
    Pricing API (price > 0).  Returns "" when no valid larger type exists
    (i.e. the instance is already the largest in its family, or the Pricing
    API has no entry for any larger size).
    """
    parts = current_type.rsplit(".", 1)
    if len(parts) != 2:
        return ""
    prefix, size = parts[0], parts[1]
    if size not in _SIZE_ORDER:
        return ""
    idx = _SIZE_ORDER.index(size)
    for i in range(idx + 1, len(_SIZE_ORDER)):
        candidate = f"{prefix}.{_SIZE_ORDER[i]}"
        if _get_on_demand_hourly(service_code, candidate, region, extra_filters) > 0:
            return candidate
    return ""


def _find_valid_larger_redshift(node_type: str, region: str) -> str:
    """
    Walk UP _SIZE_ORDER for a Redshift node type, rebuilding the nodetype
    filter per candidate so Redshift's Pricing API attribute matching works.
    Returns the first confirmed larger node type, or "".
    """
    parts = node_type.rsplit(".", 1)
    if len(parts) != 2:
        return ""
    prefix, size = parts[0], parts[1]
    if size not in _SIZE_ORDER:
        return ""
    idx = _SIZE_ORDER.index(size)
    for i in range(idx + 1, len(_SIZE_ORDER)):
        candidate = f"{prefix}.{_SIZE_ORDER[i]}"
        extra = (("nodetype", candidate),)
        if _get_on_demand_hourly("AmazonRedshift", candidate, region, extra) > 0:
            return candidate
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# Per-service savings calculators
#
# Signature convention (all kwargs for forward-compatibility):
#   _savings_<service>(resource, recommendation_type, performance, **_) -> float
# ─────────────────────────────────────────────────────────────────────────────

def _savings_rds(resource: dict, recommendation_type: str, **_) -> float:
    """
    RDS savings via the Pricing API.

    Uses _find_valid_smaller_via_api() which walks down the size list and
    confirms the target class exists in the Pricing API before computing any
    savings.  If the current class is already the smallest AWS offers for this
    engine (e.g. db.t3.micro), 0.0 is returned and the recommendation is
    suppressed upstream by resolve_downsize_target().
    """
    if recommendation_type != "Over-provisioned":
        return 0.0

    metadata = resource.get("metadata", {})
    cls      = metadata.get("instance_class", "")
    region   = resource.get("region", "")
    if not cls or not region:
        return 0.0

    extra         = _build_rds_extra_filters(metadata)
    current_price = _get_on_demand_hourly("AmazonRDS", cls, region, extra)
    if current_price == 0.0:
        return 0.0  # current class price not found — cannot compute diff

    smaller_cls = _find_valid_smaller_via_api("AmazonRDS", cls, region, extra)
    if not smaller_cls:
        return 0.0  # no smaller class confirmed by Pricing API

    rec_price = _get_on_demand_hourly("AmazonRDS", smaller_cls, region, extra)
    # rec_price is guaranteed > 0: _find_valid_smaller_via_api already confirmed it.
    diff = current_price - rec_price
    return round(diff * HOURS_PER_MONTH, 2) if diff > 0 else 0.0


def _savings_elasticache(resource: dict, recommendation_type: str, **_) -> float:
    """
    ElastiCache savings via the Pricing API.

    Uses _find_valid_smaller_via_api() to confirm the target node type exists
    before computing savings.  Multiplied by num_nodes so cluster-level
    savings are accurate.
    """
    if recommendation_type != "Over-provisioned":
        return 0.0

    metadata  = resource.get("metadata", {})
    node_type = metadata.get("node_type", "")
    num_nodes = int(metadata.get("num_nodes", 1) or 1)
    region    = resource.get("region", "")
    if not node_type or not region:
        return 0.0

    extra         = _build_elasticache_extra_filters(metadata)
    current_price = _get_on_demand_hourly("AmazonElastiCache", node_type, region, extra)
    if current_price == 0.0:
        return 0.0  # current node price not found — cannot compute diff

    smaller_type = _find_valid_smaller_via_api("AmazonElastiCache", node_type, region, extra)
    if not smaller_type:
        return 0.0  # no smaller node type confirmed by Pricing API

    rec_price = _get_on_demand_hourly("AmazonElastiCache", smaller_type, region, extra)
    diff = (current_price - rec_price) * num_nodes
    return round(diff * HOURS_PER_MONTH, 2) if diff > 0 else 0.0


def _savings_redshift(resource: dict, recommendation_type: str, **_) -> float:
    """
    Redshift savings via the Pricing API.

    Uses _find_valid_smaller_redshift() which rebuilds the nodetype filter per
    candidate so Redshift's Pricing API attribute matching is correct.
    Multiplied by node_count from metadata.
    """
    if recommendation_type != "Over-provisioned":
        return 0.0

    metadata   = resource.get("metadata", {})
    node_type  = metadata.get("node_type", "")
    node_count = int(metadata.get("node_count", 1) or 1)
    region     = resource.get("region", "")
    if not node_type or not region:
        return 0.0

    extra_current = (("nodetype", node_type),)
    current_price = _get_on_demand_hourly("AmazonRedshift", node_type, region, extra_current)
    if current_price == 0.0:
        return 0.0  # current node price not found — cannot compute diff

    smaller_type = _find_valid_smaller_redshift(node_type, region)
    if not smaller_type:
        return 0.0  # no smaller node type confirmed by Pricing API

    extra_rec = (("nodetype", smaller_type),)
    rec_price = _get_on_demand_hourly("AmazonRedshift", smaller_type, region, extra_rec)
    diff = (current_price - rec_price) * node_count
    return round(diff * HOURS_PER_MONTH, 2) if diff > 0 else 0.0


def _savings_lambda(resource: dict, recommendation_type: str,
                    performance: dict, **_) -> float:
    """
    Lambda savings via the GB-second compute model using real CloudWatch data.

    Formula:
      current_cost = (mem_mb / 1024) × (avg_duration_ms / 1000) × invocations × rate
      savings      = current_cost − cost_at_next_standard_memory_step_below_current

    Lambda memory sizes are constrained to standard steps (128, 256, 512,
    1024, 2048, 4096, 8192, 10240 MB).  The recommended size is the largest
    standard step that is strictly smaller than the current allocation.

    The compute rate ($0.0000166667 / GB-second) is globally uniform — no
    Pricing API call is needed.

    Invocations are sourced from the CloudWatch "Invocations" (Sum) metric
    already collected by the metrics registry over the 30-day window.
    """
    if recommendation_type != "Over-provisioned":
        return 0.0

    metadata = resource.get("metadata", {})
    mem_mb   = int(metadata.get("memory_mb", 0) or 0)
    if mem_mb <= 128:
        return 0.0  # already at or below the smallest useful step

    ns  = "AWS/Lambda"
    rid = resource["arn"].split(":")[-1]

    # Extract 30-day totals from the pre-collected CloudWatch performance data.
    total_invocations = 0.0
    avg_duration_ms   = 0.0
    ns_30d = performance.get("by_namespace", {}).get(ns, {}).get("30d", [])
    for record in ns_30d:
        if record.get("resource_id") == rid:
            inv_dps  = record.get("metrics", {}).get("Invocations", {}).get("30d", [])
            dur_dps  = record.get("metrics", {}).get("Duration",    {}).get("30d", [])
            total_invocations = sum(dp["value"] for dp in inv_dps if dp.get("value"))
            dur_vals = [dp["value"] for dp in dur_dps if dp.get("value")]
            avg_duration_ms   = sum(dur_vals) / len(dur_vals) if dur_vals else 0.0
            break

    if total_invocations == 0 or avg_duration_ms == 0:
        return 0.0

    # Standard Lambda memory allocation steps (MB).
    _LAMBDA_MEM_STEPS = [128, 256, 512, 1024, 2048, 4096, 8192, 10240]
    smaller_steps = [s for s in _LAMBDA_MEM_STEPS if s < mem_mb]
    if not smaller_steps:
        return 0.0
    rec_mem_mb = smaller_steps[-1]  # largest standard step still below current

    _LAMBDA_COMPUTE_RATE = 0.0000166667  # USD per GB-second (globally fixed)
    duration_sec = avg_duration_ms / 1000

    gb_sec_current = (mem_mb    / 1024) * duration_sec * total_invocations
    gb_sec_rec     = (rec_mem_mb / 1024) * duration_sec * total_invocations

    diff = (gb_sec_current - gb_sec_rec) * _LAMBDA_COMPUTE_RATE
    return round(diff, 2) if diff > 0 else 0.0


def _savings_dynamodb(resource: dict, recommendation_type: str,
                      performance: dict, **_) -> float:
    """
    DynamoDB savings via wasted provisioned capacity units.

    AWS-published On-Demand provisioned-capacity hourly rates (globally fixed):
      Provisioned RCU: $0.00013 per RCU per hour
      Provisioned WCU: $0.00065 per WCU per hour

    Wasted capacity = (provisioned average − consumed average) over 30 days.
    Savings = wasted_rcu × rate_rcu × HOURS_PER_MONTH
            + wasted_wcu × rate_wcu × HOURS_PER_MONTH

    Only applies to PROVISIONED billing mode tables.  On-Demand tables pay
    per request and cannot be over-provisioned in this sense.
    """
    if recommendation_type != "Over-provisioned":
        return 0.0

    ns  = "AWS/DynamoDB"
    arn = resource.get("arn", "")
    rid = arn.split("/")[-1] if "/" in arn else arn.split(":")[-1]

    ns_30d = performance.get("by_namespace", {}).get(ns, {}).get("30d", [])
    for record in ns_30d:
        if record.get("resource_id") != rid:
            continue

        def _avg(metric_name: str) -> float:
            dps  = record.get("metrics", {}).get(metric_name, {}).get("30d", [])
            vals = [dp["value"] for dp in dps if dp.get("value")]
            return sum(vals) / len(vals) if vals else 0.0

        prov_rcu = _avg("ProvisionedReadCapacityUnits")
        prov_wcu = _avg("ProvisionedWriteCapacityUnits")
        if prov_rcu <= 0 and prov_wcu <= 0:
            return 0.0

        cons_rcu = _avg("ConsumedReadCapacityUnits")
        cons_wcu = _avg("ConsumedWriteCapacityUnits")

        wasted_rcu = max(0.0, prov_rcu - cons_rcu)
        wasted_wcu = max(0.0, prov_wcu - cons_wcu)

        # AWS published provisioned-capacity rates (USD per unit per hour).
        _RCU_RATE = 0.00013
        _WCU_RATE = 0.00065
        monthly_savings = (wasted_rcu * _RCU_RATE + wasted_wcu * _WCU_RATE) * HOURS_PER_MONTH
        return round(monthly_savings, 2)

    return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Dispatch table
#
# Maps a lowercase service key to its savings calculator.
#
# To add a new service:
#   1. Implement _savings_<service>() above.
#   2. Add one entry here.
#   Nothing else in the codebase needs to change.
#
# Services intentionally absent:
#   ec2  — AWS Cost Explorer native recommendations supply accurate savings
#          (including RI / Savings Plan discounts).  Handled in
#          recommendations.generate() step 2.
#   ecs  — Requires task-definition CPU/memory values not yet captured by
#          resource_collector (only the task_definition ARN is stored).
#          Returns 0.0 until resource_collector is extended.
#   eks  — Cluster-level CPU metric; the actual cost is driven by the
#          underlying EC2 node instances, which are not modelled here.
#          Returns 0.0.
# ─────────────────────────────────────────────────────────────────────────────

_STRATEGIES: dict = {
    "rds":         _savings_rds,
    "elasticache": _savings_elasticache,
    "redshift":    _savings_redshift,
    "lambda":      _savings_lambda,
    "dynamodb":    _savings_dynamodb,
}


def resolve_downsize_target(service_key: str, resource: dict) -> str:
    """
    Return the first smaller instance type for the given resource that is
    confirmed to exist in the AWS Pricing API, or "" if no valid smaller
    target can be found.

    This is the gating function used by recommendations.generate() to decide
    whether an over-provisioned recommendation should be kept or suppressed:
      - "" returned  → suppress the recommendation (no valid downsize target)
      - non-empty    → keep it and update recommended_config with the exact type

    Only applicable to Pricing-API-based services (rds, elasticache, redshift).
    Lambda and DynamoDB use metric-based savings and are not validated here.
    """
    metadata = resource.get("metadata", {})
    region   = resource.get("region", "")
    if not region:
        return ""

    if service_key == "rds":
        cls = metadata.get("instance_class", "")
        if not cls:
            return ""
        extra = _build_rds_extra_filters(metadata)
        return _find_valid_smaller_via_api("AmazonRDS", cls, region, extra)

    if service_key == "elasticache":
        node_type = metadata.get("node_type", "")
        if not node_type:
            return ""
        extra = _build_elasticache_extra_filters(metadata)
        return _find_valid_smaller_via_api("AmazonElastiCache", node_type, region, extra)

    if service_key == "redshift":
        node_type = metadata.get("node_type", "")
        if not node_type:
            return ""
        return _find_valid_smaller_redshift(node_type, region)

    return ""  # not applicable to other services


# ─────────────────────────────────────────────────────────────────────────────
# Per-service cost-increase calculators (under-provisioned resources)
#
# Each returns the additional monthly cost in USD of upsizing to the
# next confirmed larger size.  Uses the same Pricing API helpers and
# LRU-cached lookups as the savings calculators above.
# ─────────────────────────────────────────────────────────────────────────────

def _cost_increase_rds(resource: dict, **_) -> float:
    """Additional monthly cost of upsizing to the next confirmed larger RDS class."""
    metadata = resource.get("metadata", {})
    cls      = metadata.get("instance_class", "")
    region   = resource.get("region", "")
    if not cls or not region:
        return 0.0
    extra         = _build_rds_extra_filters(metadata)
    current_price = _get_on_demand_hourly("AmazonRDS", cls, region, extra)
    if current_price == 0.0:
        return 0.0
    larger_cls = _find_valid_larger_via_api("AmazonRDS", cls, region, extra)
    if not larger_cls:
        return 0.0  # already the largest available class
    larger_price = _get_on_demand_hourly("AmazonRDS", larger_cls, region, extra)
    diff = larger_price - current_price
    return round(diff * HOURS_PER_MONTH, 2) if diff > 0 else 0.0


def _cost_increase_elasticache(resource: dict, **_) -> float:
    """Additional monthly cost of upsizing to the next confirmed larger ElastiCache
    node, multiplied by the actual node count from metadata."""
    metadata  = resource.get("metadata", {})
    node_type = metadata.get("node_type", "")
    num_nodes = int(metadata.get("num_nodes", 1) or 1)
    region    = resource.get("region", "")
    if not node_type or not region:
        return 0.0
    extra         = _build_elasticache_extra_filters(metadata)
    current_price = _get_on_demand_hourly("AmazonElastiCache", node_type, region, extra)
    if current_price == 0.0:
        return 0.0
    larger_type = _find_valid_larger_via_api("AmazonElastiCache", node_type, region, extra)
    if not larger_type:
        return 0.0
    larger_price = _get_on_demand_hourly("AmazonElastiCache", larger_type, region, extra)
    diff = (larger_price - current_price) * num_nodes
    return round(diff * HOURS_PER_MONTH, 2) if diff > 0 else 0.0


def _cost_increase_redshift(resource: dict, **_) -> float:
    """Additional monthly cost of upsizing to the next confirmed larger Redshift
    node, multiplied by node_count from metadata."""
    metadata   = resource.get("metadata", {})
    node_type  = metadata.get("node_type", "")
    node_count = int(metadata.get("node_count", 1) or 1)
    region     = resource.get("region", "")
    if not node_type or not region:
        return 0.0
    extra_current = (("nodetype", node_type),)
    current_price = _get_on_demand_hourly("AmazonRedshift", node_type, region, extra_current)
    if current_price == 0.0:
        return 0.0
    larger_type = _find_valid_larger_redshift(node_type, region)
    if not larger_type:
        return 0.0
    extra_larger = (("nodetype", larger_type),)
    larger_price = _get_on_demand_hourly("AmazonRedshift", larger_type, region, extra_larger)
    diff = (larger_price - current_price) * node_count
    return round(diff * HOURS_PER_MONTH, 2) if diff > 0 else 0.0


def _cost_increase_lambda(resource: dict, performance: dict, **_) -> float:
    """
    Additional monthly cost of upsizing Lambda to the next standard memory step
    above current, using real 30-day CloudWatch Invocations + Duration data.
    Standard steps: 128, 256, 512, 1024, 2048, 4096, 8192, 10240 MB.
    Rate: $0.0000166667 per GB-second (globally fixed).
    """
    metadata = resource.get("metadata", {})
    mem_mb   = int(metadata.get("memory_mb", 0) or 0)
    if mem_mb == 0:
        return 0.0
    ns  = "AWS/Lambda"
    rid = resource["arn"].split(":")[-1]
    total_invocations = 0.0
    avg_duration_ms   = 0.0
    for record in performance.get("by_namespace", {}).get(ns, {}).get("30d", []):
        if record.get("resource_id") == rid:
            inv_dps = record.get("metrics", {}).get("Invocations", {}).get("30d", [])
            dur_dps = record.get("metrics", {}).get("Duration",    {}).get("30d", [])
            total_invocations = sum(dp["value"] for dp in inv_dps if dp.get("value"))
            dur_vals = [dp["value"] for dp in dur_dps if dp.get("value")]
            avg_duration_ms   = sum(dur_vals) / len(dur_vals) if dur_vals else 0.0
            break
    if total_invocations == 0 or avg_duration_ms == 0:
        return 0.0
    _LAMBDA_MEM_STEPS = [128, 256, 512, 1024, 2048, 4096, 8192, 10240]
    larger_steps = [s for s in _LAMBDA_MEM_STEPS if s > mem_mb]
    if not larger_steps:
        return 0.0  # already at maximum standard step
    rec_mem_mb = larger_steps[0]  # smallest standard step above current
    _LAMBDA_COMPUTE_RATE = 0.0000166667
    duration_sec   = avg_duration_ms / 1000
    gb_sec_current = (mem_mb    / 1024) * duration_sec * total_invocations
    gb_sec_larger  = (rec_mem_mb / 1024) * duration_sec * total_invocations
    diff = (gb_sec_larger - gb_sec_current) * _LAMBDA_COMPUTE_RATE
    return round(diff, 2) if diff > 0 else 0.0


# Dispatch table for cost-increase strategies (under-provisioned only).
_COST_INCREASE_STRATEGIES: dict = {
    "rds":         _cost_increase_rds,
    "elasticache": _cost_increase_elasticache,
    "redshift":    _cost_increase_redshift,
    "lambda":      _cost_increase_lambda,
}


def resolve_upsize_target(service_key: str, resource: dict) -> str:
    """
    Return the first larger instance type confirmed to exist in the AWS
    Pricing API for the given resource, or "" if none can be confirmed.

    Under-provisioned recommendations are NEVER suppressed even when "" is
    returned — the performance risk remains real and the recommendation must
    still be shown, just without a specific target type.

    Only applicable to Pricing-API-based services (rds, elasticache, redshift).
    """
    metadata = resource.get("metadata", {})
    region   = resource.get("region", "")
    if not region:
        return ""
    if service_key == "rds":
        cls = metadata.get("instance_class", "")
        if not cls:
            return ""
        extra = _build_rds_extra_filters(metadata)
        return _find_valid_larger_via_api("AmazonRDS", cls, region, extra)
    if service_key == "elasticache":
        node_type = metadata.get("node_type", "")
        if not node_type:
            return ""
        extra = _build_elasticache_extra_filters(metadata)
        return _find_valid_larger_via_api("AmazonElastiCache", node_type, region, extra)
    if service_key == "redshift":
        node_type = metadata.get("node_type", "")
        if not node_type:
            return ""
        return _find_valid_larger_redshift(node_type, region)
    return ""  # not applicable to other services


def estimate_monthly_cost_increase(service_key: str, resource: dict,
                                    recommendation_type: str,
                                    performance: dict = None) -> float:
    """
    Return the estimated additional monthly cost (USD) of upsizing an
    under-provisioned resource to the next confirmed larger size.

    Returns 0.0 when recommendation_type is not Under-provisioned, the
    service has no strategy, required data is missing, or the Pricing API
    cannot confirm a larger type.
    """
    if recommendation_type != "Under-provisioned":
        return 0.0
    strategy = _COST_INCREASE_STRATEGIES.get(service_key)
    if not strategy:
        return 0.0
    try:
        return strategy(resource=resource, performance=performance or {})
    except Exception as exc:
        logger.debug(
            "Cost increase estimation failed [%s / %s]: %s",
            service_key, resource.get("arn", "unknown"), exc,
        )
        return 0.0


def estimate_monthly_savings(service_key: str, resource: dict,
                              recommendation_type: str,
                              performance: dict = None) -> float:
    """
    Return the estimated monthly saving (USD) for a rightsizing finding.

    Args:
        service_key:         Lowercase service identifier, e.g. "rds".
        resource:            Full resource dict from resource_collector,
                             including metadata populated by describe_* calls.
        recommendation_type: "Over-provisioned" | "Under-provisioned" | "Review".
        performance:         Performance data dict from performance_analyzer.
                             Required for the lambda and dynamodb strategies.

    Returns:
        Estimated monthly saving in USD, rounded to 2 decimal places.
        Returns 0.0 when the strategy is not implemented, data is missing,
        or a Pricing API error occurs.  Failures in one service never affect
        estimates for other services.
    """
    strategy = _STRATEGIES.get(service_key)
    if not strategy:
        return 0.0

    try:
        return strategy(
            resource=resource,
            recommendation_type=recommendation_type,
            performance=performance or {},
        )
    except Exception as exc:
        logger.debug(
            "Savings estimation failed [%s / %s]: %s",
            service_key, resource.get("arn", "unknown"), exc,
        )
        return 0.0
