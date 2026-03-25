"""
modules/cost_analyzer.py
--------------------------
Phase 4: Retrieves AWS billing data using the Cost Explorer API.

Fetches:
  - Total and per-service costs for the last 7, 15, and 30 days
  - AWS native right-sizing recommendations (where available)

Returns a billing dict:
  {
    "7d":  { "total": 1234.56, "by_service": [{"service": "...", "cost": 0.0}, ...] },
    "15d": { ... },
    "30d": { ... },
    "native_recommendations": [ ... ]
  }

Note: Cost Explorer is a global service (us-east-1) and requires the
      Cost Explorer API to be enabled on the account.
"""

import logging
import calendar
from datetime import datetime, timedelta, timezone

import modules.aws_client as aws_client

logger = logging.getLogger(__name__)


def _get_cost_for_current_month(ce_client) -> dict:
    """
    Fetch per-service costs for the current calendar month only.

    Uses DAILY granularity so Cost Explorer returns exactly one bucket per day
    within the current month — no bleed from previous months regardless of
    where in the month the query runs.

    start : first day of the current month  (e.g. 2026-03-01)
    end   : today midnight UTC (exclusive)  — today's costs are still accruing
            and not yet finalised by AWS, so they are intentionally excluded.
    """
    now   = datetime.now(timezone.utc)
    year  = now.year
    month = now.month

    start = datetime(year, month, 1, tzinfo=timezone.utc)
    end   = now.replace(hour=0, minute=0, second=0, microsecond=0)

    days_in_month  = calendar.monthrange(year, month)[1]
    days_elapsed   = (end - start).days
    days_remaining = days_in_month - days_elapsed
    month_name     = now.strftime("%B %Y")
    start_str      = start.strftime("%Y-%m-%d")
    end_str        = end.strftime("%Y-%m-%d")

    # On the 1st of the month no full day has completed yet — return empty.
    if start >= end:
        return {
            "total": 0.0, "by_service": [],
            "month_name": month_name, "days_in_month": days_in_month,
            "days_elapsed": 0, "days_remaining": days_in_month,
            "period": {"start": start_str, "end": end_str},
        }

    # Accumulate all daily buckets into one total per service, following
    # NextPageToken pagination — AWS returns at most 100 groups per page.
    service_totals: dict[str, float] = {}
    total = 0.0
    next_token = None

    while True:
        kwargs: dict = dict(
            TimePeriod={"Start": start_str, "End": end_str},
            # DAILY granularity: one bucket per calendar day — every bucket
            # falls strictly within the current month, zero cross-month mixing.
            Granularity="DAILY",
            Metrics=["UnblendedCost"],
            GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
        )
        if next_token:
            kwargs["NextPageToken"] = next_token

        try:
            response = ce_client.get_cost_and_usage(**kwargs)
        except Exception as e:
            logger.warning(f"Current-month cost query failed: {e}")
            return {
                "total": 0.0, "by_service": [],
                "month_name": month_name, "days_in_month": days_in_month,
                "days_elapsed": days_elapsed, "days_remaining": days_remaining,
                "period": {"start": start_str, "end": end_str},
            }

        for result in response.get("ResultsByTime", []):
            for group in result.get("Groups", []):
                svc_name = group["Keys"][0]
                amount   = float(group["Metrics"]["UnblendedCost"]["Amount"])
                if amount > 0.0:
                    service_totals[svc_name] = service_totals.get(svc_name, 0.0) + amount
                    total += amount

        next_token = response.get("NextPageToken")
        if not next_token:
            break

    by_service = [
        {"service": name, "cost": round(cost, 4)}
        for name, cost in service_totals.items()
    ]
    by_service.sort(key=lambda x: x["cost"], reverse=True)

    return {
        "total":          round(total, 4),
        "by_service":     by_service,
        "month_name":     month_name,
        "days_in_month":  days_in_month,
        "days_elapsed":   days_elapsed,
        "days_remaining": days_remaining,
        "period":         {"start": start_str, "end": end_str},
    }


def _get_cost_for_window(ce_client, days: int) -> dict:
    """Fetch total and per-service cost for the given number of past days."""
    end   = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)

    start_str = start.strftime("%Y-%m-%d")
    end_str   = end.strftime("%Y-%m-%d")

    # Accumulate per-service costs across ALL ResultsByTime buckets, following
    # NextPageToken pagination — AWS returns at most 100 groups per page.
    # Cost Explorer with MONTHLY granularity can also return multiple buckets
    # when the query window spans two calendar months (e.g. Feb 18 → Mar 17).
    service_totals: dict[str, float] = {}
    total = 0.0
    next_token = None

    while True:
        kwargs: dict = dict(
            TimePeriod={"Start": start_str, "End": end_str},
            Granularity="MONTHLY",
            Metrics=["UnblendedCost"],
            GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
        )
        if next_token:
            kwargs["NextPageToken"] = next_token

        try:
            response = ce_client.get_cost_and_usage(**kwargs)
        except Exception as e:
            logger.warning(f"Cost Explorer query failed for {days}d window: {e}")
            return {"total": 0.0, "by_service": [], "period": {"start": start_str, "end": end_str}}

        for result in response.get("ResultsByTime", []):
            for group in result.get("Groups", []):
                service_name = group["Keys"][0]
                amount = float(group["Metrics"]["UnblendedCost"]["Amount"])
                if amount > 0.0:
                    service_totals[service_name] = service_totals.get(service_name, 0.0) + amount
                    total += amount

        next_token = response.get("NextPageToken")
        if not next_token:
            break

    # Build sorted list — one entry per service, no duplicates
    by_service = [
        {"service": name, "cost": round(cost, 4)}
        for name, cost in service_totals.items()
    ]
    by_service.sort(key=lambda x: x["cost"], reverse=True)

    # Compute the actual number of days in the queried period from the real
    # datetime boundary values so report code never has to hardcode a divisor.
    days_elapsed = (end - start).days  # always equals the `days` parameter

    return {
        "total":        round(total, 4),
        "by_service":   by_service,
        "days_elapsed": days_elapsed,
        "period":       {"start": start_str, "end": end_str},
    }


def _get_native_recommendations(ce_client) -> list:
    """
    Retrieve AWS native right-sizing recommendations from Cost Explorer.
    Returns a list of recommendation dicts (may be empty if none available).
    """
    recommendations = []
    try:
        paginator = ce_client.get_paginator("get_rightsizing_recommendation")
        for page in paginator.paginate(
            Service="AmazonEC2",
            Configuration={
                "RecommendationTarget": "SAME_INSTANCE_FAMILY",
                "BenefitsConsidered": True,
            },
        ):
            for rec in page.get("RightsizingRecommendations", []):
                current = rec.get("CurrentInstance", {})
                action  = rec.get("RightsizingType", "Terminate")

                entry = {
                    "resource_id":              current.get("ResourceId", ""),
                    "region":                   current.get("ResourceDetails", {}).get(
                                                    "EC2ResourceDetails", {}).get("Region", ""),
                    "current_instance_type":    current.get("ResourceDetails", {}).get(
                                                    "EC2ResourceDetails", {}).get("InstanceType", ""),
                    "action":                   action,
                    "estimated_monthly_savings": 0.0,
                }

                if action == "Modify":
                    modify = rec.get("ModifyRecommendationDetail", {})
                    target = modify.get("TargetInstances", [{}])[0]
                    savings = target.get("EstimatedMonthlySavings", {})
                    entry["recommended_instance_type"] = target.get(
                        "ResourceDetails", {}).get("EC2ResourceDetails", {}).get("InstanceType", "")
                    entry["estimated_monthly_savings"] = float(savings.get("Value", 0))
                elif action == "Terminate":
                    terminate = rec.get("TerminateRecommendationDetail", {})
                    savings = terminate.get("EstimatedMonthlySavings", {})
                    entry["estimated_monthly_savings"] = float(savings.get("Value", 0))

                recommendations.append(entry)
    except Exception as e:
        logger.info(f"Could not fetch native recommendations (may not be enabled): {e}")

    return recommendations


def analyze() -> dict:
    """
    Main entry point.  Fetches billing data for all three time windows
    plus native AWS right-sizing recommendations.
    """
    logger.info("Starting cost analysis...")

    # Cost Explorer is a global service - always use us-east-1
    try:
        ce = aws_client.get_client("ce", "us-east-1")
    except Exception as e:
        logger.error(f"Failed to create Cost Explorer client: {e}")
        return {
            "7d": {"total": 0.0, "by_service": [], "period": {}},
            "15d": {"total": 0.0, "by_service": [], "period": {}},
            "30d": {"total": 0.0, "by_service": [], "period": {}},
            "native_recommendations": [],
        }

    billing = {}
    for days in [7, 15, 30]:
        logger.info(f"Fetching {days}-day cost data...")
        billing[f"{days}d"] = _get_cost_for_window(ce, days)

    logger.info("Fetching current-month cost data...")
    billing["current_month"] = _get_cost_for_current_month(ce)

    logger.info("Fetching native right-sizing recommendations...")
    billing["native_recommendations"] = _get_native_recommendations(ce)

    total_services = len(billing["30d"].get("by_service", []))
    logger.info(
        f"Cost analysis complete: 30d total=${billing['30d']['total']:.2f} "
        f"across {total_services} services."
    )
    return billing
