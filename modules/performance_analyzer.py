"""
modules/performance_analyzer.py
---------------------------------
Phase 5: Collects CloudWatch performance metrics for every discovered
namespace.

Strategy
--------
1. Uses metrics_registry.METRICS_REGISTRY to know which metrics to
   pull per namespace.
2. For each namespace + resource ARN, builds the appropriate CloudWatch
   dimensions and calls get_metric_statistics() for each of the three
   time windows (7d / 15d / 30d).
3. Falls back to list_metrics() for unknown namespaces and collects
   whatever is available.
4. Performs basic spike correlation: flags resources whose metric
   averages were ≥ SPIKE_THRESHOLD_STDDEV standard deviations above
   the mean of all resources in the same namespace/metric.

Returns
-------
{
  "by_namespace": {
    "AWS/EC2": {
      "7d":  [ { "resource_id": "i-abc", "metrics": { "CPUUtilization": [...] } } ],
      "15d": [...],
      "30d": [...]
    },
    ...
  },
  "spike_correlation": [
    { "namespace": "AWS/EC2", "metric": "CPUUtilization", "window": "7d",
      "resource_id": "i-abc", "avg": 88.5, "z_score": 3.2, "severity": "HIGH" }
  ]
}
"""

import logging
import math
import statistics
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import modules.aws_client as aws_client
import config
from registries.metrics_registry import METRICS_REGISTRY

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────────────────────────────────
# Utility helpers
# ────────────────────────────────────────────────────────────────────────────

def _date_range(days: int):
    """Return (start_dt, end_dt) UTC datetimes for the last N days."""
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    return start, end


def _get_statistics(cw_client, namespace: str, metric_name: str,
                    dimensions: list, stat: str, start: datetime,
                    end: datetime, period: int = None) -> list:
    """
    Call CloudWatch get_metric_statistics.
    Returns a list of data-point dicts sorted by Timestamp.
    """
    period = period or config.CW_PERIOD
    try:
        resp = cw_client.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=dimensions,
            StartTime=start,
            EndTime=end,
            Period=period,
            Statistics=[stat],
        )
        datapoints = resp.get("Datapoints", [])
        datapoints.sort(key=lambda d: d["Timestamp"])
        return [
            {
                "timestamp": d["Timestamp"].isoformat(),
                "value":     round(d.get(stat, 0), 4),
                "unit":      d.get("Unit", ""),
            }
            for d in datapoints
        ]
    except Exception as e:
        logger.debug(f"CloudWatch stats failed [{namespace}/{metric_name}]: {e}")
        return []


def _compute_avg(datapoints: list) -> float:
    values = [dp["value"] for dp in datapoints if dp.get("value") is not None]
    return sum(values) / len(values) if values else 0.0


# ────────────────────────────────────────────────────────────────────────────
# Per-resource metric collection
# ────────────────────────────────────────────────────────────────────────────

def _collect_resource_metrics(namespace: str, arn: str, region: str,
                               metric_defs: list) -> dict:
    """
    Collect all metrics for a single resource across all time windows.
    Returns { "resource_id": ..., "arn": ..., "region": ...,
              "metrics": { metric_name: { "7d": [...], "15d": [...], "30d": [...] } } }
    """
    resource_id = arn.split("/")[-1].split(":")[-1]

    # Build dimensions from the metric definitions' dim_key
    # dim_key is a field name like "InstanceId", "DBInstanceIdentifier", etc.
    result = {
        "resource_id": resource_id,
        "arn":         arn,
        "region":      region,
        "metrics":     {},
    }

    try:
        cw = aws_client.get_client("cloudwatch", region)
    except Exception as e:
        logger.debug(f"CW client creation failed for {region}: {e}")
        return result

    for metric_def in metric_defs:
        metric_name = metric_def["name"]
        stat        = metric_def.get("stat", "Average")
        dim_key     = metric_def.get("dim_key", "")

        if not dim_key:
            continue

        dimensions = [{"Name": dim_key, "Value": resource_id}]
        metric_data = {}

        for days in config.TIME_WINDOWS:
            start, end = _date_range(days)
            # Use a finer period for shorter windows
            period = 3600 if days <= 7 else config.CW_PERIOD
            datapoints = _get_statistics(
                cw, namespace, metric_name, dimensions,
                stat, start, end, period
            )
            metric_data[f"{days}d"] = datapoints

        if any(metric_data.values()):
            result["metrics"][metric_name] = metric_data

    return result


# ────────────────────────────────────────────────────────────────────────────
# Spike correlation
# ────────────────────────────────────────────────────────────────────────────

def _detect_spikes(by_namespace: dict) -> list:
    """
    Across all resources in a namespace/metric/window triple, compute
    z-scores and flag resources significantly above the mean.
    """
    spikes = []
    threshold = config.SPIKE_THRESHOLD_STDDEV

    for namespace, windows_data in by_namespace.items():
        for window_key, resource_list in windows_data.items():
            # Collect average per-resource per metric
            metric_avgs = {}  # metric_name -> {resource_id: avg}
            for rec in resource_list:
                rid = rec["resource_id"]
                for metric_name, windows in rec.get("metrics", {}).items():
                    datapoints = windows.get(window_key, [])
                    avg = _compute_avg(datapoints)
                    metric_avgs.setdefault(metric_name, {})[rid] = avg

            for metric_name, avgs_by_resource in metric_avgs.items():
                values = list(avgs_by_resource.values())
                if len(values) < 2:
                    continue
                mean = statistics.mean(values)
                try:
                    stdev = statistics.stdev(values)
                except StatisticsError:
                    continue
                if stdev == 0:
                    continue

                for rid, avg in avgs_by_resource.items():
                    z = (avg - mean) / stdev
                    if z >= threshold:
                        severity = "HIGH" if z >= threshold * 1.5 else "MEDIUM"
                        spikes.append({
                            "namespace":   namespace,
                            "metric":      metric_name,
                            "window":      window_key,
                            "resource_id": rid,
                            "avg":         round(avg, 4),
                            "z_score":     round(z, 2),
                            "severity":    severity,
                        })

    return sorted(spikes, key=lambda s: s["z_score"], reverse=True)


# ────────────────────────────────────────────────────────────────────────────
# Unknown-namespace fallback
# ────────────────────────────────────────────────────────────────────────────

def _collect_unknown_namespace(namespace: str, arns: list, regions: list) -> list:
    """
    For namespaces not in the registry, use list_metrics to discover
    available metrics and pull the first few.
    """
    records = []
    # Use a representative region from the ARN list
    seen_regions = set()
    for arn in arns:
        parts = arn.split(":")
        if len(parts) > 3 and parts[3]:
            seen_regions.add(parts[3])

    for region in (seen_regions or [regions[0]] if regions else ["us-east-1"]):
        try:
            cw = aws_client.get_client("cloudwatch", region)
            paginator = cw.get_paginator("list_metrics")
            metrics_found = []
            for page in paginator.paginate(Namespace=namespace):
                metrics_found.extend(page.get("Metrics", []))
                if len(metrics_found) >= 5:
                    break

            for metric in metrics_found[:5]:
                metric_name = metric["MetricName"]
                dims        = metric.get("Dimensions", [])
                if not dims:
                    continue
                resource_id = dims[0]["Value"]

                metric_data = {}
                for days in config.TIME_WINDOWS:
                    start, end = _date_range(days)
                    metric_data[f"{days}d"] = _get_statistics(
                        cw, namespace, metric_name, dims, "Average", start, end
                    )

                records.append({
                    "resource_id": resource_id,
                    "arn":         f"unknown::{resource_id}",
                    "region":      region,
                    "metrics":     {metric_name: metric_data},
                })
        except Exception as e:
            logger.debug(f"Unknown namespace fallback failed [{namespace}]: {e}")

    return records


# ────────────────────────────────────────────────────────────────────────────
# Main entry point
# ────────────────────────────────────────────────────────────────────────────

def analyze(service_map: dict, regions: list) -> dict:
    """
    Collect CloudWatch metrics for all discovered services.
    Returns the full performance and spike-correlation dict.
    """
    logger.info("Starting performance analysis...")

    # Build work items: (namespace, arn, region)
    work_items = []  # (namespace, arn, region, metric_defs)

    for resource_type, info in service_map.items():
        if resource_type.startswith("__namespace__"):
            continue
        namespace  = info.get("namespace", "")
        arns       = info.get("arns", [])
        metric_defs = METRICS_REGISTRY.get(namespace, [])

        if not namespace or not arns:
            continue

        for arn in arns:
            parts  = arn.split(":")
            region = parts[3] if len(parts) > 3 and parts[3] else "us-east-1"
            work_items.append((namespace, arn, region, metric_defs))

    logger.info(f"Collecting CloudWatch metrics for {len(work_items)} resources...")

    # Collect all metrics in parallel
    raw_results = []  # list of (namespace, record)
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {}
        for namespace, arn, region, metric_defs in work_items:
            if not metric_defs:
                # Queue the unknown-namespace path instead
                continue
            f = executor.submit(_collect_resource_metrics, namespace, arn, region, metric_defs)
            futures[f] = namespace

        for future in as_completed(futures):
            ns = futures[future]
            try:
                record = future.result()
                if record.get("metrics"):
                    raw_results.append((ns, record))
            except Exception as e:
                logger.debug(f"Performance collection future failed: {e}")

    # Handle unknown namespaces (those not in registry but discovered by CW)
    unknown_ns_work = {}
    for resource_type, info in service_map.items():
        if resource_type.startswith("__namespace__"):
            ns = resource_type.replace("__namespace__", "")
            if ns not in METRICS_REGISTRY:
                unknown_ns_work.setdefault(ns, {"arns": [], "regions": regions})
                unknown_ns_work[ns]["arns"].extend(info.get("arns", []))

    for ns, ns_info in unknown_ns_work.items():
        records = _collect_unknown_namespace(ns, ns_info["arns"], regions)
        for rec in records:
            raw_results.append((ns, rec))

    # Organise into by_namespace structure
    by_namespace = {}
    for ns, record in raw_results:
        if ns not in by_namespace:
            by_namespace[ns] = {f"{d}d": [] for d in config.TIME_WINDOWS}
        for window_key in by_namespace[ns]:
            # Build a per-window snapshot of just this window's datapoints
            window_record = {
                "resource_id": record["resource_id"],
                "arn":         record["arn"],
                "region":      record["region"],
                "metrics":     {
                    m: {window_key: pts[window_key]}
                    for m, pts in record.get("metrics", {}).items()
                    if window_key in pts
                },
            }
            by_namespace[ns][window_key].append(window_record)

    # Spike correlation
    logger.info("Computing spike correlation...")
    spikes = _detect_spikes(by_namespace)

    total_resources = len(set(r.get("resource_id", "") for _, r in raw_results))
    logger.info(
        f"Performance analysis complete: {len(by_namespace)} namespaces, "
        f"{len(spikes)} anomalies detected."
    )

    return {
        "by_namespace":     by_namespace,
        "spike_correlation": spikes,
    }
