"""
modules/trend_analyzer.py
--------------------------
Pure-computation trend and pattern recognition engine.

Operates entirely on the already-collected consolidated data — makes
NO additional AWS API calls.  All four analysis domains derive their
insights from the data structures built earlier in the pipeline:

  cost_trends        — week-over-week cost deltas, service growth rates,
                       projected monthly spend
  performance_trends — metric direction (INCREASING / DECREASING) across
                       the 7d / 15d / 30d CloudWatch windows per resource
  fleet_patterns     — systemic over/under-provisioning across the fleet,
                       savings concentration, resource distribution
  security_patterns  — recurring issue types, hotspot regions/domains,
                       cross-service encryption and public-access patterns

Result dict is stored under key "trends" in the consolidated data dict
and consumed by report_generator to produce the Trends & Patterns section.
"""

import logging
import re
import statistics
from collections import Counter, defaultdict

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe_avg(datapoints: list) -> float:
    """Mean of a CloudWatch datapoint list; returns 0.0 when empty."""
    vals = [
        float(d.get("Average", d.get("Sum", d.get("Maximum", 0.0))))
        for d in (datapoints or [])
        if d
    ]
    return statistics.mean(vals) if vals else 0.0


def _trend_direction(early: float, late: float, threshold_pct: float = 5.0) -> str:
    """
    Classify the direction of change from *early* (older average) to
    *late* (most-recent average).

    Returns:
        "INCREASING"  — late > early by more than threshold_pct %
        "DECREASING"  — late < early by more than threshold_pct %
        "STABLE"      — change within threshold
    """
    if early == 0:
        return "INCREASING" if late > 0 else "STABLE"
    change_pct = (late - early) / abs(early) * 100
    if change_pct > threshold_pct:
        return "INCREASING"
    if change_pct < -threshold_pct:
        return "DECREASING"
    return "STABLE"


def _trend_label(direction: str, change_pct: float) -> str:
    """Human-readable label for a cost direction + magnitude."""
    if direction == "INCREASING":
        return "Fast growing" if abs(change_pct) > 25 else "Growing"
    if direction == "DECREASING":
        return "Fast declining" if abs(change_pct) > 25 else "Declining"
    return "Stable"


# ─────────────────────────────────────────────────────────────────────────────
# 1. Cost Trends
# ─────────────────────────────────────────────────────────────────────────────

def _analyze_cost_trends(billing: dict) -> dict:
    """
    Derive week-over-week and service-level billing trends from the three
    rolling-window totals (7d / 15d / 30d) already fetched by cost_analyzer.

    Week-over-week technique (no extra API call needed):
        current_7d  = billing["7d"]["total"]          <- last 7 days
        prior_7d    = billing["15d"]["total"] - billing["7d"]["total"]  <- days 8-15

    Spend acceleration measures whether the current week is ramping faster
    (+) or slower (-) than the prior week, giving early-warning of spend
    momentum regardless of absolute dollar amounts.
    """
    b7  = billing.get("7d",  {})
    b15 = billing.get("15d", {})
    b30 = billing.get("30d", {})
    bm  = billing.get("current_month", {})

    t7  = float(b7.get("total",  0) or 0)
    t15 = float(b15.get("total", 0) or 0)
    t30 = float(b30.get("total", 0) or 0)

    # Daily averages per rolling window
    daily_7d  = t7  / 7  if t7  else 0.0
    daily_15d = t15 / 15 if t15 else 0.0
    daily_30d = t30 / 30 if t30 else 0.0

    # Week-over-week: current 7d vs the prior 7d (days 8-15 extracted from 15d total)
    prior_7d_total = max(t15 - t7, 0.0)
    prior_7d_daily = prior_7d_total / 7 if prior_7d_total > 0 else 0.0
    wow_delta      = t7 - prior_7d_total
    wow_pct        = (wow_delta / prior_7d_total * 100) if prior_7d_total > 0 else 0.0

    # Spend acceleration: % change in daily rate from prior week to current week
    spend_acceleration = round(
        (daily_7d - prior_7d_daily) / prior_7d_daily * 100
        if prior_7d_daily > 0 else 0.0,
        1,
    )

    # Overall direction based on 7d daily vs 30d daily (3% threshold)
    direction     = _trend_direction(daily_30d, daily_7d, threshold_pct=3.0)
    chg_30_to_7   = ((daily_7d - daily_30d) / daily_30d * 100) if daily_30d else 0.0
    overall_label = _trend_label(direction, chg_30_to_7)

    # Projected full month from the current 7d daily rate
    projected_monthly = daily_7d * 30
    baseline_monthly  = daily_30d * 30      # 30d-based baseline for comparison
    vs_baseline_usd   = round(projected_monthly - baseline_monthly, 2)
    vs_baseline_pct   = round(
        (vs_baseline_usd / baseline_monthly * 100) if baseline_monthly > 0 else 0.0, 1
    )

    # Current-month projection from actual MTD spend (calendar-aware)
    cm_elapsed  = int(bm.get("days_elapsed",  0) or 0)
    cm_in_month = int(bm.get("days_in_month", 30) or 30)
    cm_total    = float(bm.get("total", 0) or 0)
    if cm_elapsed > 0:
        cm_daily_rate = cm_total / cm_elapsed
        cm_remaining  = max(cm_in_month - cm_elapsed, 0)
        # Hybrid projection: actual MTD spend (fixed) + 7-day daily rate × remaining days.
        # Using the most recent 7-day rate for the unknown future is more accurate than
        # extrapolating the MTD average, especially when spend has recently shifted.
        # Falls back to MTD-average projection when no 7-day data is available.
        cm_projected  = round(cm_total + (daily_7d * cm_remaining), 2) if daily_7d > 0 \
                        else round(cm_daily_rate * cm_in_month, 2)
    else:
        cm_daily_rate = 0.0
        cm_remaining  = cm_in_month
        cm_projected  = 0.0

    mom_delta_pct = ((cm_daily_rate - daily_30d) / daily_30d * 100) if daily_30d > 0 else 0.0

    # Projection confidence: higher when more historical windows are populated
    window_count          = sum(1 for t in [t7, t15, t30] if t > 0)
    projection_confidence = "HIGH" if window_count == 3 else ("MEDIUM" if window_count == 2 else "LOW")

    overall = {
        "daily_avg_7d":          round(daily_7d,  4),
        "daily_avg_15d":         round(daily_15d, 4),
        "daily_avg_30d":         round(daily_30d, 4),
        "prior_7d_daily_avg":    round(prior_7d_daily, 4),
        "direction":             direction,
        "trend_label":           overall_label,
        "prior_7d_total":        round(prior_7d_total, 2),
        "current_7d_total":      round(t7, 2),
        "wow_delta_usd":         round(wow_delta, 2),
        "wow_delta_pct":         round(wow_pct, 1),
        "spend_acceleration":    spend_acceleration,
        "projected_monthly":     round(projected_monthly, 2),
        "baseline_monthly":      round(baseline_monthly, 2),
        "vs_baseline_usd":       vs_baseline_usd,
        "vs_baseline_pct":       vs_baseline_pct,
        "cm_projected":          cm_projected,
        "mom_delta_pct":         round(mom_delta_pct, 1),
        # MoM delta in USD — numerically consistent with mom_delta_pct (both compare
        # cm_daily_rate vs daily_30d, normalised to a 30-day month so that
        #   mom_delta_usd / baseline_monthly == mom_delta_pct / 100
        "mom_delta_usd":         round((cm_daily_rate - daily_30d) * 30, 2),
        # Calendar-aware projected bill vs 30d rolling baseline.
        # Preferred over vs_baseline_pct / vs_baseline_usd when the last-7d
        # daily rate is a poor proxy for the current month's run-rate.
        "cm_vs_baseline_pct":    round(
            ((cm_projected - baseline_monthly) / baseline_monthly * 100)
            if baseline_monthly > 0 else 0.0, 1
        ),
        "cm_vs_baseline_usd":    round(cm_projected - baseline_monthly, 2),
        "cm_remaining":          cm_remaining,
        "projection_confidence": projection_confidence,
    }

    # ── Service-level trends ──────────────────────────────────────────────────
    def _svc_map(window_data: dict) -> dict:
        return {s["service"]: float(s["cost"]) for s in window_data.get("by_service", [])}

    svc_7d  = _svc_map(b7)
    svc_15d = _svc_map(b15)
    svc_30d = _svc_map(b30)

    all_services = set(svc_7d) | set(svc_30d)
    by_service: list = []
    for svc in all_services:
        cost_7d  = svc_7d.get(svc,  0.0)
        cost_15d = svc_15d.get(svc, 0.0)
        cost_30d = svc_30d.get(svc, 0.0)

        d7  = cost_7d  / 7  if cost_7d  else 0.0
        d15 = cost_15d / 15 if cost_15d else 0.0
        d30 = cost_30d / 30 if cost_30d else 0.0

        # Suppress sub-cent-per-day noise (avoids cluttering the table with
        # microservice allocations that have no meaningful cost signal).
        if max(d7, d30) < 0.01:
            continue

        dir_  = _trend_direction(d30, d7, threshold_pct=5.0)
        chg   = ((d7 - d30) / d30 * 100) if d30 else (100.0 if d7 > 0 else 0.0)

        # Consistent trend: does the 15d window sit monotonically between 30d and 7d?
        # A consistent trend has higher predictive confidence than a reversal midway.
        consistent_trend = (
            (d30 <= d15 <= d7) or (d30 >= d15 >= d7)
        ) if d15 > 0 else False

        by_service.append({
            "service":          svc,
            "cost_7d":          round(cost_7d,  2),
            "cost_30d":         round(cost_30d, 2),
            "daily_avg_7d":     round(d7,  4),
            "daily_avg_15d":    round(d15, 4),
            "daily_avg_30d":    round(d30, 4),
            "direction":        dir_,
            "change_pct":       round(chg, 1),
            "trend_label":      _trend_label(dir_, chg),
            "consistent_trend": consistent_trend,
        })

    # Biggest movers first; within each direction bucket, sort by magnitude
    by_service.sort(key=lambda s: (
        {"INCREASING": 0, "DECREASING": 1, "STABLE": 2}.get(s["direction"], 3),
        -abs(s["change_pct"]),
    ))

    fastest_growing = [
        s["service"] for s in by_service
        if s["direction"] == "INCREASING" and s["daily_avg_30d"] > 0
    ][:5]
    biggest_savers  = [
        s["service"] for s in by_service
        if s["direction"] == "DECREASING" and s["daily_avg_7d"] > 0
    ][:5]

    return {
        "overall":         overall,
        "by_service":      by_service,
        "fastest_growing": fastest_growing,
        "biggest_savers":  biggest_savers,
    }


# ── Metric interpretation catalogue ──────────────────────────────────────────
# Keyed by "MetricName/DIR" where DIR is the first 3 chars of the direction.
_METRIC_INTERPRETATION: dict = {
    "CPUUtilization/INC":              "CPU utilisation rising — potential under-provisioning",
    "CPUUtilization/DEC":              "Low CPU utilisation — downsize / rightsizing opportunity",
    "MemoryUtilization/INC":           "Memory pressure detected — resize or review workload",
    "MemoryUtilization/DEC":           "Low memory utilisation — downsize opportunity",
    "DatabaseConnections/INC":         "DB connection count growing — check pool configuration",
    "DatabaseConnections/DEC":         "DB connections declining — workload reduced",
    "ReadLatency/INC":                 "Read I/O degradation — review IOPS or storage tier",
    "WriteLatency/INC":                "Write I/O degradation — review IOPS or storage tier",
    "ReadLatency/DEC":                 "Read latency improving — performance nominal",
    "WriteLatency/DEC":                "Write latency improving — performance nominal",
    "FreeStorageSpace/DEC":            "Storage shrinking — plan expansion to avoid capacity incident",
    "FreeStorageSpace/INC":            "Storage space recovering — capacity risk resolved",
    "NetworkIn/INC":                   "Inbound network traffic growing",
    "NetworkOut/INC":                  "Outbound network traffic growing",
    "Latency/INC":                     "API / LB latency degradation — investigate backend",
    "RequestCount/INC":                "Request volume growing — review capacity plan",
    "StatusCheckFailed/INC":           "EC2 status check failures increasing — investigate instance health",
    "UnHealthyHostCount/INC":          "Unhealthy LB targets rising — check target health",
    "HTTPCode_ELB_5XX_Count/INC":      "5xx error rate rising — service reliability at risk",
    "ConsumedWriteCapacityUnits/INC":  "DynamoDB write throughput growing — check capacity mode",
    "ConsumedReadCapacityUnits/INC":   "DynamoDB read throughput growing — check capacity mode",
    "ThrottledRequests/INC":           "Throttling increasing — scale capacity or reduce request rate",
    "BurstBalance/DEC":                "EBS burst balance depleting — consider gp3 upgrade",
    "VolumeQueueLength/INC":           "EBS queue length rising — I/O bottleneck",
    "ActiveConnectionCount/INC":       "Active connections growing — review listener capacity",
    "TargetResponseTime/INC":          "Target response time increasing — investigate application layer",
}

# Metric criticality weights — multiplies change_pct when calculating severity tier.
# Latency, failure, and error metrics escalate to HIGH at a lower threshold.
_METRIC_WEIGHT: dict = {
    "CPUUtilization":             1.2,
    "MemoryUtilization":          1.4,
    "ReadLatency":                2.2,
    "WriteLatency":               2.2,
    "FreeStorageSpace":           2.5,
    "StatusCheckFailed":          3.5,
    "UnHealthyHostCount":         3.5,
    "HTTPCode_ELB_5XX_Count":     3.0,
    "ThrottledRequests":          2.0,
    "DatabaseConnections":        1.5,
    "Latency":                    2.0,
    "TargetResponseTime":         2.0,
    "VolumeQueueLength":          2.0,
    "BurstBalance":               2.0,
}


def _perf_interpret(metric_name: str, direction: str) -> str:
    """Return a human-readable interpretation for a (metric, direction) pair."""
    key = f"{metric_name}/{direction[:3]}"
    return _METRIC_INTERPRETATION.get(
        key,
        f"{metric_name} {'rising' if direction == 'INCREASING' else 'declining'}",
    )


# ─────────────────────────────────────────────────────────────────────────────
# 2. Performance Trends
# ─────────────────────────────────────────────────────────────────────────────

def _analyze_performance_trends(performance: dict) -> list:
    """
    For every (namespace, resource, metric) triple, compare the average
    value across the 7d / 15d / 30d CloudWatch windows and classify the
    trend direction.

    Only non-STABLE results are returned.  Severity is calculated using
    metric-criticality weights so that latency and health check metrics
    surface as HIGH even with smaller percentage changes, while generic
    utilisation metrics require a larger shift.

    A ``consistent_trend`` flag is set when the 15d average sits
    monotonically between the 30d and 7d averages — confirming that the
    shift is sustained rather than a single-window spike.
    """
    by_namespace = performance.get("by_namespace", {})
    trends: list = []

    for namespace, windows_data in by_namespace.items():
        resource_metric_avgs: defaultdict = defaultdict(lambda: defaultdict(dict))

        for window_key, resource_list in windows_data.items():
            for rec in resource_list:
                rid = rec.get("resource_id", "")
                if not rid:
                    continue
                for metric_name, window_pts in rec.get("metrics", {}).items():
                    pts = window_pts.get(window_key, [])
                    if pts:
                        avg = _safe_avg(pts)
                        resource_metric_avgs[rid][metric_name][window_key] = avg

        for rid, metrics_data in resource_metric_avgs.items():
            for metric_name, window_avgs in metrics_data.items():
                avg_7d  = window_avgs.get("7d")
                avg_15d = window_avgs.get("15d")
                avg_30d = window_avgs.get("30d")

                if avg_7d is None or avg_30d is None:
                    continue

                # Suppress truly-zero noise: both windows must be non-trivial.
                # Threshold is intentionally very low (0.001) so that latency
                # metrics measured in seconds (e.g. RDS ReadLatency ≈ 0.002 s,
                # Lambda duration ≈ 0.3 s, API Gateway latency ≈ 0.1 s) are
                # NOT suppressed — they represent real-world millisecond values
                # that can change significantly in absolute terms.
                if avg_7d < 0.001 and avg_30d < 0.001:
                    continue

                # 5 % change threshold — catches meaningful shifts (e.g. CPU 70 → 74 %)
                # without generating noise from random micro-fluctuations.
                direction  = _trend_direction(avg_30d, avg_7d, threshold_pct=5.0)
                if direction == "STABLE":
                    continue

                change_pct = ((avg_7d - avg_30d) / avg_30d * 100) if avg_30d else 0.0
                abs_chg    = abs(change_pct)

                # Weighted severity: critical metrics escalate faster.
                # Thresholds lowered from 40/20 → 30/15 so that real-world changes
                # (e.g. +25 % CPU × 1.2 weight = 30 → HIGH) surface correctly.
                weight        = _METRIC_WEIGHT.get(metric_name, 1.0)
                effective_chg = abs_chg * weight
                severity      = (
                    "HIGH"   if effective_chg >= 30
                    else "MEDIUM" if effective_chg >= 15
                    else "LOW"
                )

                # Trend consistency: does the 15d window sit monotonically
                # between 30d and 7d?  A consistent signal has higher confidence.
                consistent_trend: bool | None = None
                if avg_15d is not None:
                    consistent_trend = (
                        (avg_30d <= avg_15d <= avg_7d)
                        or (avg_30d >= avg_15d >= avg_7d)
                    )

                trends.append({
                    "resource_id":      rid,
                    "namespace":        namespace,
                    "metric":           metric_name,
                    "avg_7d":           round(avg_7d,  3),
                    "avg_15d":          round(avg_15d, 3) if avg_15d is not None else None,
                    "avg_30d":          round(avg_30d, 3),
                    "direction":        direction,
                    "change_pct":       round(change_pct, 1),
                    "severity":         severity,
                    "consistent_trend": consistent_trend,
                    "interpretation":   _perf_interpret(metric_name, direction),
                })

    # PRIMARY sort: severity (HIGH first); SECONDARY: absolute magnitude
    trends.sort(key=lambda t: (
        {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(t["severity"], 3),
        -abs(t["change_pct"]),
    ))
    return trends


# ─────────────────────────────────────────────────────────────────────────────
# 3. Fleet Patterns
# ─────────────────────────────────────────────────────────────────────────────

def _analyze_fleet_patterns(resources: list, recommendations: list) -> dict:
    """
    Identify systemic provisioning patterns across the whole resource fleet.

    Uses the recommendations list (carrying CloudWatch-backed reason and
    change_pct data) rather than re-analysing raw metrics.

    The ``efficiency_score`` reflects the proportion of resources that are
    neither flagged as over-provisioned nor under-provisioned — a useful
    single-number health indicator for the fleet.
    """
    total = len(resources)

    type_counter = Counter(r.get("recommendation_type", "Unknown") for r in recommendations)
    over_count   = (
        type_counter.get("Over-provisioned",   0)
        + type_counter.get("No-downsize target", 0)
    )
    under_count  = type_counter.get("Under-provisioned",  0)
    review_count = type_counter.get("Review",             0)
    over_pct     = round(over_count  / total * 100, 1) if total else 0.0
    under_pct    = round(under_count / total * 100, 1) if total else 0.0

    # Efficiency score: fraction of resources NOT flagged for over- or under-provisioning.
    # 100 % = entire fleet is right-sized; lower scores signal broader waste or risk.
    right_sized_count = max(total - over_count - under_count, 0)
    efficiency_score  = round(right_sized_count / total * 100, 1) if total else 100.0

    # Group recommendations by service
    svc_recs: defaultdict = defaultdict(lambda: {"count": 0, "savings": 0.0, "types": Counter()})
    for r in recommendations:
        svc = r.get("service", "unknown").upper()
        svc_recs[svc]["count"]   += 1
        svc_recs[svc]["savings"] += float(r.get("estimated_monthly_savings_usd") or 0)
        svc_recs[svc]["types"][r.get("recommendation_type", "Unknown")] += 1

    svc_resource_count = Counter(r.get("service", "unknown").upper() for r in resources)

    services_with_most_recs = sorted(
        [
            {
                "service":        svc,
                "count":          d["count"],
                "savings":        round(d["savings"], 2),
                "dominant_type":  d["types"].most_common(1)[0][0] if d["types"] else "Unknown",
                "resource_total": svc_resource_count.get(svc, 0),
                "rec_pct":        round(
                    d["count"] / svc_resource_count.get(svc, 1) * 100
                ) if svc_resource_count.get(svc, 0) else 0,
            }
            for svc, d in svc_recs.items()
        ],
        key=lambda x: -x["count"],
    )[:8]

    # Systemic issue narratives with actionable language
    # Only generated when ≥ 2 resources of a service are affected.
    systemic_issues: list = []
    for svc, d in svc_recs.items():
        svc_total = svc_resource_count.get(svc, 0)
        if svc_total < 2 or d["count"] < 2:
            continue
        pct     = round(d["count"] / svc_total * 100)
        over_n  = (
            d["types"].get("Over-provisioned",   0)
            + d["types"].get("No-downsize target", 0)
        )
        under_n = d["types"].get("Under-provisioned", 0)
        sav_str = (
            f" — potential savings ${d['savings']:,.2f}/mo"
            if d["savings"] > 1 else ""
        )
        if over_n / d["count"] >= 0.7:
            systemic_issues.append(
                f"{svc}: {over_n}/{svc_total} resources ({pct}%) are over-provisioned{sav_str}."
                f" Schedule a batch rightsizing review."
            )
        elif under_n / d["count"] >= 0.7:
            systemic_issues.append(
                f"{svc}: {under_n}/{svc_total} resources ({pct}%) are under-provisioned."
                f" Scale up or enable auto-scaling to eliminate performance risk."
            )
        elif pct >= 60:
            systemic_issues.append(
                f"{svc}: {d['count']}/{svc_total} resources ({pct}%) have rightsizing"
                f" opportunities{sav_str}."
            )

    resource_distribution = [
        {
            "service": svc,
            "count":   cnt,
            "pct":     round(cnt / total * 100, 1) if total else 0,
        }
        for svc, cnt in svc_resource_count.most_common(10)
    ]

    total_savings = sum(
        float(r.get("estimated_monthly_savings_usd") or 0) for r in recommendations
    )

    return {
        "total_resources":         total,
        "overprovisioned_count":   over_count,
        "overprovisioned_pct":     over_pct,
        "underprovisioned_count":  under_count,
        "underprovisioned_pct":    under_pct,
        "review_count":            review_count,
        "efficiency_score":        efficiency_score,
        "total_savings_potential": round(total_savings, 2),
        "services_with_most_recs": services_with_most_recs,
        "systemic_issues":         systemic_issues[:6],
        "resource_distribution":   resource_distribution,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 4. Security Patterns
# ─────────────────────────────────────────────────────────────────────────────

def _dominant_severity_for(findings: list, issue_snippet: str) -> str:
    """Highest severity among findings whose issue text contains *issue_snippet*."""
    order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2, "INFO": 3}
    best  = "INFO"
    for f in findings:
        if issue_snippet[:30].lower() in str(f.get("issue", "")).lower():
            sev = f.get("severity", "INFO")
            if order.get(sev, 3) < order.get(best, 3):
                best = sev
    return best


def _analyze_security_patterns(security_findings: list) -> dict:
    """
    Identify recurring security patterns, hotspot regions, and impacted domains.

    Strips resource-specific identifiers (names, IDs) from issue strings so
    the same class of problem clusters under one label regardless of the
    specific resource it affects.
    """
    total_findings = len(security_findings)

    if not security_findings:
        return {
            "total_findings":         0,
            "severity_distribution":  {},
            "top_issue_types":        [],
            "regions_by_findings":    [],
            "services_by_findings":   [],
            "high_risk_domains":      [],
            "recurring_patterns":     [],
        }

    severity_dist = Counter(f.get("severity", "INFO") for f in security_findings)

    # ── Issue pattern grouping ────────────────────────────────────────────────
    # Remove quoted names and AWS IDs to expose the structural issue pattern
    _strip_re = re.compile(
        r"'[^']*'|\"[^\"]*\""           # remove quoted strings
        r"|\b(sg|i|vpc|db|vol|subnet)-[a-z0-9\-]+\b"  # remove resource IDs
        r"|\d{4}-\d{2}-\d{2}( \d{2}:\d{2}(:\d{2})?)?( GMT)?"  # remove timestamps
        r"|\b\d+\b"                      # remove bare numbers
    )

    issue_types: Counter = Counter()
    for f in security_findings:
        issue  = str(f.get("issue", ""))
        domain = f.get("domain", "Unknown")
        clean  = _strip_re.sub("", issue)
        clean  = re.sub(r"\s{2,}", " ", clean).strip()
        words  = clean.split()
        # Build a 6-word summary key prefixed by domain
        key = f"{domain}: {' '.join(words[:6])}" if words else domain
        issue_types[key] += 1

    top_issue_types = [
        {
            "issue_type": issue,
            "count":      cnt,
            "severity":   _dominant_severity_for(security_findings, issue.split(": ", 1)[-1]),
        }
        for issue, cnt in issue_types.most_common(10)
        if cnt >= 2          # only patterns (≥2 occurrences)
    ]

    # ── Hotspot regions ───────────────────────────────────────────────────────
    regions_by_findings = [
        {"region": region, "count": cnt}
        for region, cnt in Counter(
            f.get("region", "global") for f in security_findings
        ).most_common(10)
    ]

    # ── Hotspot domains ───────────────────────────────────────────────────────
    domain_counts = Counter(f.get("domain", "Unknown") for f in security_findings)
    services_by_findings = [
        {
            "domain":      domain,
            "count":       cnt,
            "high_count":  sum(
                1 for f in security_findings
                if f.get("domain") == domain and f.get("severity") == "HIGH"
            ),
        }
        for domain, cnt in domain_counts.most_common(8)
    ]

    # ── Human-readable recurring pattern narratives ───────────────────────────
    recurring_patterns = []

    # Cross-service encryption gap
    enc_findings = [
        f for f in security_findings
        if "encrypt" in str(f.get("issue", "")).lower()
    ]
    if len(enc_findings) >= 3:
        enc_domains = sorted({f.get("domain", "") for f in enc_findings})
        recurring_patterns.append(
            f"[PATTERN] Encryption gaps across {len(enc_domains)} domain(s) "
            f"({len(enc_findings)} findings): {', '.join(enc_domains)}"
        )

    # Public / internet exposure
    public_findings = [
        f for f in security_findings
        if "public" in str(f.get("issue", "")).lower()
        or "publicly accessible" in str(f.get("issue", "")).lower()
        or "0.0.0.0/0" in str(f.get("issue", ""))
    ]
    if len(public_findings) >= 2:
        recurring_patterns.append(
            f"[PATTERN] {len(public_findings)} resource(s) exposed to the "
            "internet — review public-endpoint configuration"
        )

    # Unused / idle resources that represent wasted spend
    idle_findings = [
        f for f in security_findings
        if f.get("category") in ("idle_resource", "unused_resource")
    ]
    if idle_findings:
        est_waste = sum(
            float(f.get("estimated_monthly_cost_usd", 0) or 0) for f in idle_findings
        )
        recurring_patterns.append(
            f"[PATTERN] {len(idle_findings)} idle/unused resource(s) costing "
            f"~${est_waste:,.2f}/month — deletion or shutdown recommended"
        )

    # Top issue types as narrative patterns
    for item in top_issue_types[:5]:
        cnt  = item["count"]
        sev  = item["severity"]
        text = item["issue_type"]
        recurring_patterns.append(
            f"[{sev}] {text} — {cnt} occurrence{'s' if cnt > 1 else ''}"
        )

    # Domains that have at least one HIGH-severity finding — require immediate attention
    high_risk_domains = sorted({
        f.get("domain", "Unknown")
        for f in security_findings
        if f.get("severity") == "HIGH" and f.get("domain")
    })

    return {
        "total_findings":         total_findings,
        "severity_distribution":  dict(severity_dist),
        "top_issue_types":        top_issue_types,
        "regions_by_findings":    regions_by_findings,
        "services_by_findings":   services_by_findings,
        "high_risk_domains":      high_risk_domains,
        "recurring_patterns":     recurring_patterns[:10],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def analyze(billing: dict, performance: dict, resources: list,
            recommendations: list, security_findings: list) -> dict:
    """
    Run all four trend / pattern analyses.

    Parameters
    ----------
    billing           : output of cost_analyzer.analyze()
    performance       : output of performance_analyzer.analyze()
    resources         : flat list of resource dicts (from service_discovery)
    recommendations   : list produced by recommendations.generate()
    security_findings : list produced by security_auditor.run()

    Returns
    -------
    dict with keys:
        cost_trends        — overall direction, WoW deltas, per-service trends
        performance_trends — per-resource metric trend directions
        fleet_patterns     — systemic provisioning patterns
        security_patterns  — recurring finding patterns and hotspots
    """
    logger.info("Running trend and pattern analysis...")

    cost_trends        = _analyze_cost_trends(billing)
    performance_trends = _analyze_performance_trends(performance)
    fleet_patterns     = _analyze_fleet_patterns(resources, recommendations)
    security_patterns  = _analyze_security_patterns(security_findings)

    logger.info(
        f"Trend analysis complete: cost={cost_trends['overall']['direction']}, "
        f"{len(performance_trends)} metric trends, "
        f"{len(fleet_patterns.get('systemic_issues', []))} fleet issues, "
        f"{len(security_patterns.get('recurring_patterns', []))} security patterns."
    )

    return {
        "cost_trends":        cost_trends,
        "performance_trends": performance_trends,
        "fleet_patterns":     fleet_patterns,
        "security_patterns":  security_patterns,
    }
