"""
modules/recommendations.py
----------------------------
Phase 6: Generates resource right-sizing recommendations.

Strategy
---------
1. Loads rightsizing_registry.RIGHTSIZING_REGISTRY for thresholds
   and recommendation templates.
2. For each resource in the unified resource list, applies the relevant
   thresholds against the 30-day performance data.
3. Merges with AWS native Cost Explorer recommendations (already in billing).
4. Deduplicates by resource_id (preferring native recommendations when
   available as they have accurate savings estimates).

Returns a list of recommendation dicts:
[
  {
    "resource_id":                  "i-abc123",
    "arn":                          "arn:aws:ec2:...",
    "service":                      "ec2",
    "region":                       "us-east-1",
    "resource_name":                "my-server",
    "current_config":               "t3.large",
    "recommended_config":           "t3.medium",
    "reason":                       "Avg CPU 5.2% over 30 days (threshold: 10%)",
    "estimated_monthly_savings_usd": 12.50,
    "source":                       "metrics_analysis" | "aws_native",
    "severity":                     "LOW" | "MEDIUM" | "HIGH",
  }
]
"""

import logging

import config
from registries.rightsizing_registry import RIGHTSIZING_REGISTRY
from modules import pricing_estimator

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────────
# Metric lookup helpers
# ────────────────────────────────────────────────────────────────────────────

def _avg_for_metric(performance: dict, namespace: str, resource_id: str,
                    metric_name: str, window: str = "30d") -> float:
    """
    Extract the average value for a given namespace/resource/metric/window
    from the performance dict produced by performance_analyzer.
    Returns 0.0 if no data is available.
    """
    ns_data = performance.get("by_namespace", {}).get(namespace, {})
    records = ns_data.get(window, [])
    for record in records:
        if record.get("resource_id") == resource_id:
            datapoints = record.get("metrics", {}).get(metric_name, {}).get(window, [])
            values = [dp["value"] for dp in datapoints if dp.get("value") is not None]
            return sum(values) / len(values) if values else 0.0
    return 0.0


def _peak_for_metric(performance: dict, namespace: str, resource_id: str,
                     metric_name: str, window: str = "30d") -> float:
    """Extract peak (maximum) value for a given metric."""
    ns_data = performance.get("by_namespace", {}).get(namespace, {})
    records = ns_data.get(window, [])
    for record in records:
        if record.get("resource_id") == resource_id:
            datapoints = record.get("metrics", {}).get(metric_name, {}).get(window, [])
            values = [dp["value"] for dp in datapoints if dp.get("value") is not None]
            return max(values) if values else 0.0
    return 0.0


# ────────────────────────────────────────────────────────────────────────────
# Per-service recommendation generators
# ────────────────────────────────────────────────────────────────────────────

def _evaluate_ec2(resource: dict, performance: dict) -> dict:
    """Evaluate an EC2 instance for rightsizing."""
    ns    = "AWS/EC2"
    # Always use the instance ID (from ARN) for CloudWatch lookups, not the Name tag.
    rid_display = resource.get("name", "")
    rid          = resource["arn"].split("/")[-1]   # e.g. i-0abc123def456789
    itype        = resource.get("metadata", {}).get("instance_type", "unknown")

    avg_cpu  = _avg_for_metric(performance, ns, rid, "CPUUtilization")
    peak_cpu = _peak_for_metric(performance, ns, rid, "CPUUtilization")

    thresholds     = config.THRESHOLDS.get("ec2", {})
    cpu_threshold  = thresholds.get("avg_cpu_pct", 10)
    peak_threshold = thresholds.get("peak_cpu_pct", 30)

    # If CloudWatch returned no data, flag large instances as "review needed"
    if avg_cpu == 0.0 and peak_cpu == 0.0:
        large_families = ("xlarge", "2xlarge", "4xlarge", "8xlarge", "16xlarge")
        if any(s in itype for s in large_families):
            return {
                "resource_id":                   rid,
                "arn":                           resource["arn"],
                "service":                       "ec2",
                "region":                        resource.get("region", ""),
                "resource_name":                 rid_display or rid,
                "current_config":                itype,
                "recommended_config":            "Review instance size — no utilization data available",
                "reason":                        f"No CloudWatch utilization data found for {itype}. Verify the instance is actively needed.",
                "estimated_monthly_savings_usd": 0.0,
                "source":                        "no_data_flag",
                "severity":                      "LOW",
            }
        return None

    if avg_cpu <= cpu_threshold and peak_cpu <= peak_threshold:
        reason = (
            f"Average CPU {avg_cpu:.1f}% (threshold {cpu_threshold}%), "
            f"peak CPU {peak_cpu:.1f}% (threshold {peak_threshold}%) over 30 days. "
            f"Instance appears over-provisioned."
        )
        return {
            "resource_id":                   rid,
            "arn":                           resource["arn"],
            "service":                       "ec2",
            "region":                        resource.get("region", ""),
            "resource_name":                 rid_display or rid,
            "current_config":                itype,
            "recommended_config":            "Downsize to smaller instance type",
            "reason":                        reason,
            "estimated_monthly_savings_usd": 0.0,  # Requires pricing API
            "source":                        "metrics_analysis",
            "severity":                      "HIGH" if avg_cpu <= cpu_threshold / 2 else "MEDIUM",
            "recommendation_type":           "Over-provisioned",
        }

    # Under-provisioned: sustained high average CPU over the 30-day window.
    # Peak-only spikes (e.g. a 5-minute burst) are intentionally excluded —
    # only consistent average pressure indicates the instance is too small.
    UNDER_CPU = config.THRESHOLDS.get("ec2", {}).get("under_cpu_pct", 85.0)
    if avg_cpu >= UNDER_CPU:
        return {
            "resource_id":                   rid,
            "arn":                           resource["arn"],
            "service":                       "ec2",
            "region":                        resource.get("region", ""),
            "resource_name":                 rid_display or rid,
            "current_config":                itype,
            "recommended_config":            "Upsize instance type or enable auto-scaling",
            "reason":                        (
                f"Average CPU {avg_cpu:.1f}% over 30 days exceeds {UNDER_CPU}% sustained threshold. "
                f"Instance '{rid_display or rid}' ({itype}) is under-provisioned — "
                f"consistently high CPU indicates the workload needs more capacity."
            ),
            "estimated_monthly_savings_usd": 0.0,
            "source":                        "metrics_analysis",
            "severity":                      "HIGH",
            "recommendation_type":           "Under-provisioned",
        }

    return None


def _evaluate_rds(resource: dict, performance: dict) -> dict:
    """Evaluate an RDS instance for rightsizing."""
    ns   = "AWS/RDS"
    # DBInstanceIdentifier is both the resource name and the CW dimension value.
    rid  = resource["arn"].split(":")[-1]   # last colon-segment of RDS ARN
    rid_display = resource.get("name", rid)
    cls  = resource.get("metadata", {}).get("instance_class", "unknown")

    avg_cpu  = _avg_for_metric(performance, ns, rid, "CPUUtilization")
    peak_cpu = _peak_for_metric(performance, ns, rid, "CPUUtilization")

    thresholds    = config.THRESHOLDS.get("rds", {})
    cpu_threshold = thresholds.get("avg_cpu_pct", 15)

    # No CloudWatch data — flag large/medium instances as needing review.
    if avg_cpu == 0.0:
        non_trivial = cls not in ("unknown", "db.t2.micro", "db.t3.micro", "db.t4g.micro")
        if non_trivial:
            return {
                "resource_id":                   rid,
                "arn":                           resource["arn"],
                "service":                       "rds",
                "region":                        resource.get("region", ""),
                "resource_name":                 rid_display,
                "current_config":                cls,
                "recommended_config":            "Review instance class — no utilization data available",
                "reason":                        (
                    f"No CloudWatch CPU data found for '{rid}' ({cls}). "
                    f"Verify Performance Insights is enabled and the instance is actively used."
                ),
                "estimated_monthly_savings_usd": 0.0,
                "source":                        "no_data_flag",
                "severity":                      "MEDIUM",
                "recommendation_type":           "Review",
            }
        return None

    # Over-provisioned: low CPU utilisation
    if avg_cpu <= cpu_threshold:
        reason = (
            f"Average CPU {avg_cpu:.1f}% (threshold {cpu_threshold}%), "
            f"peak CPU {peak_cpu:.1f}% over 30 days. "
            f"Database may be over-provisioned."
        )
        return {
            "resource_id":                   rid,
            "arn":                           resource["arn"],
            "service":                       "rds",
            "region":                        resource.get("region", ""),
            "resource_name":                 rid_display,
            "current_config":                cls,
            "recommended_config":            "Consider a smaller DB instance class",
            "reason":                        reason,
            "estimated_monthly_savings_usd": 0.0,
            "source":                        "metrics_analysis",
            "severity":                      "HIGH" if avg_cpu <= cpu_threshold / 2 else "MEDIUM",
            "recommendation_type":           "Over-provisioned",
        }

    # Under-provisioned: sustained high DB CPU
    UNDER_CPU = 85.0
    if avg_cpu >= UNDER_CPU:
        return {
            "resource_id":                   rid,
            "arn":                           resource["arn"],
            "service":                       "rds",
            "region":                        resource.get("region", ""),
            "resource_name":                 rid_display,
            "current_config":                cls,
            "recommended_config":            "Upsize DB instance class or enable read replicas",
            "reason":                        (
                f"Average RDS CPU {avg_cpu:.1f}% over 30 days — database is under-provisioned "
                f"and may be causing query latency. Consider upgrading to a larger instance class "
                f"or horizontal scaling via read replicas."
            ),
            "estimated_monthly_savings_usd": 0.0,
            "source":                        "metrics_analysis",
            "severity":                      "HIGH",
            "recommendation_type":           "Under-provisioned",
        }

    return None


def _evaluate_lambda(resource: dict, performance: dict) -> dict:
    """Evaluate a Lambda function for memory rightsizing."""
    ns        = "AWS/Lambda"
    # Lambda ARN format: arn:aws:lambda:region:account:function:FunctionName
    rid       = resource["arn"].split(":")[-1]
    mem_mb    = resource.get("metadata", {}).get("memory_mb", 0)
    timeout   = resource.get("metadata", {}).get("timeout_sec", 0)

    avg_duration = _avg_for_metric(performance, ns, rid, "Duration")

    thresholds = config.THRESHOLDS.get("lambda", {})
    dur_pct    = thresholds.get("duration_pct_of_timeout", 20)

    if avg_duration and timeout and timeout > 0:
        actual_pct = (avg_duration / 1000) / timeout * 100
        if actual_pct < dur_pct and mem_mb > 128:
            reason = (
                f"Average duration {avg_duration:.0f}ms is {actual_pct:.1f}% of "
                f"{timeout}s timeout. Memory is {mem_mb}MB. "
                f"Consider reducing memory allocation."
            )
            return {
                "resource_id":                   rid,
                "arn":                           resource["arn"],
                "service":                       "lambda",
                "region":                        resource.get("region", ""),
                "resource_name":                 resource.get("name", rid),
                "current_config":                f"{mem_mb}MB",
                "recommended_config":            "Reduce memory (use Lambda Power Tuning)",
                "reason":                        reason,
                "estimated_monthly_savings_usd": 0.0,
                "source":                        "metrics_analysis",
                "severity":                      "LOW",
                "recommendation_type":           "Over-provisioned",
            }
        # Under-provisioned: function consistently near timeout
        NEAR_TIMEOUT_PCT = 80.0
        if actual_pct >= NEAR_TIMEOUT_PCT:
            return {
                "resource_id":                   rid,
                "arn":                           resource["arn"],
                "service":                       "lambda",
                "region":                        resource.get("region", ""),
                "resource_name":                 resource.get("name", rid),
                "current_config":                f"{mem_mb}MB, {timeout}s timeout",
                "recommended_config":            "Increase memory and/or timeout, or refactor function logic",
                "reason":                        (
                    f"Average duration {avg_duration:.0f}ms is {actual_pct:.1f}% of the "
                    f"{timeout}s timeout. Function risks timeout failures under load. "
                    f"Increase timeout, add memory (which also increases CPU), or split logic."
                ),
                "estimated_monthly_savings_usd": 0.0,
                "source":                        "metrics_analysis",
                "severity":                      "HIGH" if actual_pct >= 95 else "MEDIUM",
                "recommendation_type":           "Under-provisioned",
            }
    return None


def _evaluate_elasticache(resource: dict, performance: dict) -> dict:
    """Evaluate an ElastiCache cluster for rightsizing."""
    ns       = "AWS/ElastiCache"
    rid      = resource["arn"].split(":")[-1]
    cls      = resource.get("metadata", {}).get("node_type", "unknown")

    avg_cpu = _avg_for_metric(performance, ns, rid, "CPUUtilization")
    thresholds = config.THRESHOLDS.get("elasticache", {})
    cpu_threshold = thresholds.get("avg_cpu_pct", 20)

    if avg_cpu and avg_cpu <= cpu_threshold:
        reason = f"Average CPU {avg_cpu:.1f}% (threshold {cpu_threshold}%) over 30 days."
        return {
            "resource_id":                   rid,
            "arn":                           resource["arn"],
            "service":                       "elasticache",
            "region":                        resource.get("region", ""),
            "resource_name":                 resource.get("name", rid),
            "current_config":                cls,
            "recommended_config":            "Consider a smaller cache node type",
            "reason":                        reason,
            "estimated_monthly_savings_usd": 0.0,
            "source":                        "metrics_analysis",
            "severity":                      "LOW",
            "recommendation_type":           "Over-provisioned",
        }

    # Under-provisioned: sustained high CPU on cache (causes evictions and latency)
    UNDER_CPU_CACHE = 80.0
    if avg_cpu and avg_cpu >= UNDER_CPU_CACHE:
        return {
            "resource_id":                   rid,
            "arn":                           resource["arn"],
            "service":                       "elasticache",
            "region":                        resource.get("region", ""),
            "resource_name":                 resource.get("name", rid),
            "current_config":                cls,
            "recommended_config":            "Upsize to larger cache node or add read replicas",
            "reason":                        (
                f"Average ElastiCache CPU {avg_cpu:.1f}% over 30 days. "
                f"Cache is under-provisioned and may cause evictions and elevated application latency."
            ),
            "estimated_monthly_savings_usd": 0.0,
            "source":                        "metrics_analysis",
            "severity":                      "MEDIUM",
            "recommendation_type":           "Under-provisioned",
        }

    return None


def _evaluate_dynamodb(resource: dict, performance: dict) -> dict | None:
    """
    Evaluate a DynamoDB table for rightsizing.

    Two signals are checked:
    1. Throttled requests  → under-provisioned (highest priority)
    2. Consumed vs Provisioned capacity ratio → over-provisioned when far below threshold
    """
    ns          = "AWS/DynamoDB"
    arn         = resource.get("arn", "")
    # DynamoDB ARN: arn:aws:dynamodb:region:account:table/TableName
    rid         = arn.split("/")[-1] if "/" in arn else arn.split(":")[-1]
    rid_display = resource.get("name", rid)
    billing_mode = resource.get("metadata", {}).get("billing_mode", "PROVISIONED")

    thresholds         = config.THRESHOLDS.get("dynamodb", {})
    capacity_threshold = thresholds.get("capacity_utilisation_pct", 25.0)

    # Signal 1: throttling → table is starved of capacity (under-provisioned)
    throttle_avg = _avg_for_metric(performance, ns, rid, "ThrottledRequests")
    if throttle_avg and throttle_avg > 0:
        return {
            "resource_id":                   rid,
            "arn":                           arn,
            "service":                       "dynamodb",
            "region":                        resource.get("region", ""),
            "resource_name":                 rid_display,
            "current_config":                f"{billing_mode} mode",
            "recommended_config":            "Increase provisioned capacity or switch to on-demand mode",
            "reason":                        (
                f"DynamoDB table is experiencing throttled requests "
                f"(avg {throttle_avg:.2f} throttle events/period). "
                f"The table is under-provisioned — increase RCU/WCU or enable on-demand billing."
            ),
            "estimated_monthly_savings_usd": 0.0,
            "source":                        "metrics_analysis",
            "severity":                      "HIGH",
            "recommendation_type":           "Under-provisioned",
        }

    # Signal 2: provisioned capacity far above consumed → over-provisioned
    if billing_mode == "PROVISIONED":
        consumed_rcu    = _avg_for_metric(performance, ns, rid, "ConsumedReadCapacityUnits")
        consumed_wcu    = _avg_for_metric(performance, ns, rid, "ConsumedWriteCapacityUnits")
        provisioned_rcu = _avg_for_metric(performance, ns, rid, "ProvisionedReadCapacityUnits")
        provisioned_wcu = _avg_for_metric(performance, ns, rid, "ProvisionedWriteCapacityUnits")

        if provisioned_rcu > 0 and provisioned_wcu > 0:
            rcu_util = consumed_rcu / provisioned_rcu * 100
            wcu_util = consumed_wcu / provisioned_wcu * 100
            avg_util = (rcu_util + wcu_util) / 2

            if avg_util < capacity_threshold:
                return {
                    "resource_id":                   rid,
                    "arn":                           arn,
                    "service":                       "dynamodb",
                    "region":                        resource.get("region", ""),
                    "resource_name":                 rid_display,
                    "current_config":                f"PROVISIONED — RCU util {rcu_util:.1f}%, WCU util {wcu_util:.1f}%",
                    "recommended_config":            "Switch to on-demand billing or reduce provisioned capacity",
                    "reason":                        (
                        f"Table consumes only {rcu_util:.1f}% of provisioned RCU and "
                        f"{wcu_util:.1f}% of provisioned WCU. The table is over-provisioned — "
                        f"switching to on-demand mode or reducing provisioned capacity would lower costs."
                    ),
                    "estimated_monthly_savings_usd": 0.0,
                    "source":                        "metrics_analysis",
                    "severity":                      "MEDIUM" if avg_util < capacity_threshold / 2 else "LOW",
                    "recommendation_type":           "Over-provisioned",
                }

    return None


# ────────────────────────────────────────────────────────────────────────────
# Generic evaluator factory
# ────────────────────────────────────────────────────────────────────────────

def _build_generic_evaluator(service_key: str):
    """
    Factory that produces a threshold-based evaluator from registry metadata.

    Used automatically for any service in RIGHTSIZING_REGISTRY that does NOT
    have a hand-crafted dedicated evaluator.  Adding a new service to the
    registry is all that is needed — no code changes here.

    Evaluation logic (CPU / utilisation % model):
    - avg metric ≤ over_threshold_pct  AND  peak ≤ peak_threshold_pct  → Over-provisioned
    - avg metric ≥ under_threshold_pct                                  → Under-provisioned
    - Otherwise                                                         → No recommendation
    """
    cfg = RIGHTSIZING_REGISTRY.get(service_key, {})

    cw_ns           = cfg.get("cw_namespace",           f"AWS/{service_key.capitalize()}")
    arn_parser      = cfg.get("arn_resource_id_parser",  "colon")
    primary_metric  = cfg.get("primary_metric",          "CPUUtilization")
    config_key      = cfg.get("config_metadata_key",     "")
    over_threshold  = cfg.get("over_threshold_pct",
                              config.THRESHOLDS.get(service_key, {}).get("avg_cpu_pct", 10.0))
    peak_threshold  = cfg.get("peak_threshold_pct",      100.0)
    under_threshold = cfg.get("under_threshold_pct",     85.0)
    rec_over        = cfg.get("recommendation_over",     f"Consider downsizing this {service_key} resource")
    rec_under       = cfg.get("recommendation_under",    f"Consider upsizing this {service_key} resource")

    def _evaluate(resource: dict, performance: dict) -> dict | None:
        arn = resource.get("arn", "")

        # Parse resource ID from ARN according to registry instruction
        if arn_parser == "slash":
            rid = arn.split("/")[-1] if "/" in arn else arn.split(":")[-1]
        else:  # "colon"
            rid = arn.split(":")[-1]

        rid_display = resource.get("name", rid)
        current_cfg = (
            resource.get("metadata", {}).get(config_key, "unknown")
            if config_key else "unknown"
        )

        avg_val  = _avg_for_metric(performance, cw_ns, rid, primary_metric)
        peak_val = _peak_for_metric(performance, cw_ns, rid, primary_metric)

        # No CloudWatch data available — skip silently
        if avg_val == 0.0 and peak_val == 0.0:
            return None

        # Over-provisioned: both average and peak are below their thresholds
        if avg_val <= over_threshold and peak_val <= peak_threshold:
            return {
                "resource_id":                   rid,
                "arn":                           arn,
                "service":                       service_key,
                "region":                        resource.get("region", ""),
                "resource_name":                 rid_display,
                "current_config":                current_cfg,
                "recommended_config":            rec_over,
                "reason":                        (
                    f"Average {primary_metric} {avg_val:.1f}% (threshold {over_threshold}%), "
                    f"peak {peak_val:.1f}% (threshold {peak_threshold}%) over 30 days. "
                    f"{service_key.upper()} resource appears over-provisioned."
                ),
                "estimated_monthly_savings_usd": 0.0,
                "source":                        "metrics_analysis",
                "severity":                      "HIGH" if avg_val <= over_threshold / 2 else "MEDIUM",
                "recommendation_type":           "Over-provisioned",
            }

        # Under-provisioned: average exceeds threshold
        if avg_val >= under_threshold:
            return {
                "resource_id":                   rid,
                "arn":                           arn,
                "service":                       service_key,
                "region":                        resource.get("region", ""),
                "resource_name":                 rid_display,
                "current_config":                current_cfg,
                "recommended_config":            rec_under,
                "reason":                        (
                    f"Average {primary_metric} {avg_val:.1f}% over 30 days exceeds "
                    f"{under_threshold}% threshold. Resource is under-provisioned "
                    f"and may be impacting performance."
                ),
                "estimated_monthly_savings_usd": 0.0,
                "source":                        "metrics_analysis",
                "severity":                      "HIGH",
                "recommendation_type":           "Under-provisioned",
            }

        return None

    # Annotate for debuggability
    _evaluate.__name__ = f"_evaluate_{service_key}_generic"
    _evaluate.__doc__  = (
        f"Auto-generated generic evaluator for '{service_key}' "
        f"(built from RIGHTSIZING_REGISTRY at module load time)."
    )
    return _evaluate


# ────────────────────────────────────────────────────────────────────────────
# Dispatcher — fully dynamic, auto-built from RIGHTSIZING_REGISTRY
# ────────────────────────────────────────────────────────────────────────────

# Services with bespoke evaluation logic that cannot be covered by the generic
# CPU/utilisation factory.  All other services in RIGHTSIZING_REGISTRY get a
# generic evaluator automatically — no manual entries required here.
_HAND_CRAFTED_EVALUATORS: dict = {
    "ec2":         _evaluate_ec2,
    "rds":         _evaluate_rds,
    "lambda":      _evaluate_lambda,
    "elasticache": _evaluate_elasticache,
    "dynamodb":    _evaluate_dynamodb,
}

# Build the full evaluator map at module load time.
# ┌─ For each service key in the registry:
# │    • If a hand-crafted function exists for it → use that.
# │    • Otherwise               → auto-generate a generic evaluator.
# └─ Adding a new service to RIGHTSIZING_REGISTRY is all that is needed.
_EVALUATORS: dict = {
    svc: _HAND_CRAFTED_EVALUATORS.get(svc) or _build_generic_evaluator(svc)
    for svc in RIGHTSIZING_REGISTRY
}

logger.debug(
    "Rightsizing evaluators loaded: %s (hand-crafted: %s, generic: %s)",
    list(_EVALUATORS.keys()),
    [s for s in _EVALUATORS if s in _HAND_CRAFTED_EVALUATORS],
    [s for s in _EVALUATORS if s not in _HAND_CRAFTED_EVALUATORS],
)


def generate(resources: list, performance: dict, billing: dict) -> list:
    """
    Generate all rightsizing recommendations.
    Merges metrics-driven recommendations with AWS native recommendations.
    """
    logger.info("Generating rightsizing recommendations...")

    recommendations = []
    seen_resource_ids = set()

    # Build ARN → full resource lookup used by the pricing estimator pass below.
    resource_by_arn: dict = {r["arn"]: r for r in resources if r.get("arn")}

    # 1. Metrics-driven recommendations
    for resource in resources:
        service  = resource.get("service", "").lower()
        evaluator = _EVALUATORS.get(service)
        if not evaluator:
            continue
        try:
            rec = evaluator(resource, performance)
            if rec:
                recommendations.append(rec)
                seen_resource_ids.add(rec["resource_id"])
        except Exception as e:
            logger.debug(f"Evaluator failed for {resource.get('arn')}: {e}")

    # 1b. Validate and enrich recommendations for Pricing-API-based services
    #     (rds, elasticache, redshift).
    #
    #     Over-provisioned: confirm a smaller class exists via the Pricing API
    #     before keeping the recommendation.  If the current class is already
    #     the minimum AWS offers (e.g. db.t3.micro → no db.t3.nano in RDS),
    #     the recommendation is suppressed entirely.  When a valid smaller type
    #     is found, recommended_config is set to the exact confirmed type.
    #
    #     Under-provisioned: resolve the next confirmed larger type from the
    #     Pricing API and set it as recommended_config.  Under-provisioned recs
    #     are NEVER suppressed — the performance risk is real regardless of
    #     whether a specific target can be priced.
    _PRICING_API_SERVICES = {"rds", "elasticache", "redshift"}
    validated_recs = []
    for rec in recommendations:
        svc      = rec.get("service", "")
        rec_type = rec.get("recommendation_type", "")
        if svc in _PRICING_API_SERVICES and rec_type == "Over-provisioned":
            full_resource = resource_by_arn.get(rec.get("arn", ""), {})
            target = pricing_estimator.resolve_downsize_target(svc, full_resource)
            if not target:
                # No smaller instance confirmed by the AWS Pricing API.
                # Keep the recommendation visible so the analyst sees it, but
                # mark it clearly and lock savings to $0.00.
                logger.debug(
                    "No smaller instance confirmed for %s (%s): flagging as "
                    "'No smaller resource available'.",
                    rec.get("resource_name", rec.get("resource_id", "")),
                    rec.get("current_config", ""),
                )
                rec["recommended_config"] = "No smaller resource available"
                rec["estimated_monthly_savings_usd"] = 0.0
            else:
                # Replace the vague evaluator placeholder with the exact confirmed target.
                rec["recommended_config"] = target
        elif svc in _PRICING_API_SERVICES and rec_type == "Under-provisioned":
            # Resolve the confirmed larger type — update if found, never suppress.
            full_resource = resource_by_arn.get(rec.get("arn", ""), {})
            target = pricing_estimator.resolve_upsize_target(svc, full_resource)
            if target:
                rec["recommended_config"] = target
        validated_recs.append(rec)
    recommendations = validated_recs

    # 1a. Inject accurate monthly cost figures for non-EC2 services.
    #     Over-provisioned  → estimated_monthly_savings_usd      (money saved)
    #     Under-provisioned → estimated_monthly_cost_increase_usd (investment to fix)
    #     EC2 savings come from AWS Cost Explorer in step 2.
    for rec in recommendations:
        svc      = rec.get("service", "")
        rec_type = rec.get("recommendation_type", "")
        if svc == "ec2":
            continue  # handled by Cost Explorer in step 2
        full_resource = resource_by_arn.get(rec.get("arn", ""), {})
        if rec_type == "Over-provisioned":
            # Skip recs that step 1b has already validated and locked.
            # "No smaller resource available" means the Pricing API confirmed
            # there is no cheaper tier → savings MUST remain 0.00, never
            # re-estimated.
            if rec.get("recommended_config") == "No smaller resource available":
                rec["estimated_monthly_savings_usd"] = 0.0  # enforce; belt-and-suspenders
            elif rec.get("estimated_monthly_savings_usd", 0.0) == 0.0:
                rec["estimated_monthly_savings_usd"] = pricing_estimator.estimate_monthly_savings(
                    service_key=svc,
                    resource=full_resource,
                    recommendation_type=rec_type,
                    performance=performance,
                )
        elif rec_type == "Under-provisioned":
            if rec.get("estimated_monthly_cost_increase_usd", 0.0) == 0.0:
                rec["estimated_monthly_cost_increase_usd"] = (
                    pricing_estimator.estimate_monthly_cost_increase(
                        service_key=svc,
                        resource=full_resource,
                        recommendation_type=rec_type,
                        performance=performance,
                    )
                )

    # 2. Merge AWS native recommendations from billing data
    native_recs = billing.get("native_recommendations", [])
    for native in native_recs:
        rid = native.get("resource_id", "")
        if rid in seen_resource_ids:
            # Update the existing recommendation with accurate savings
            for rec in recommendations:
                if rec["resource_id"] == rid:
                    rec["estimated_monthly_savings_usd"] = native.get(
                        "estimated_monthly_savings", 0.0)
                    rec["source"] = "metrics_analysis+aws_native"
                    if native.get("recommended_instance_type"):
                        rec["recommended_config"] = native["recommended_instance_type"]
        else:
            # Add native recommendation not found by metrics analysis
            recommendations.append({
                "resource_id":                   rid,
                "arn":                           "",
                "service":                       "ec2",
                "region":                        native.get("region", ""),
                "resource_name":                 rid,
                "current_config":                native.get("current_instance_type", ""),
                "recommended_config":            native.get("recommended_instance_type", native.get("action", "")),
                "reason":                        f"AWS native recommendation: {native.get('action', '')}",
                "estimated_monthly_savings_usd": native.get("estimated_monthly_savings", 0.0),
                "source":                        "aws_native",
                "severity":                      "MEDIUM",
            })

    # Sort by estimated savings descending
    recommendations.sort(
        key=lambda r: r.get("estimated_monthly_savings_usd", 0.0),
        reverse=True
    )

    total_savings = sum(r.get("estimated_monthly_savings_usd", 0.0) for r in recommendations)
    logger.info(
        f"Recommendations complete: {len(recommendations)} recommendations, "
        f"estimated monthly savings: ${total_savings:.2f}."
    )
    return recommendations
