"""
modules/query_analyzer.py
--------------------------
Phase 5 (RDS-specific): Analyses database query performance using
RDS Performance Insights.

For each RDS instance:
  - If Performance Insights is enabled, calls pi.describe_dimension_keys()
    to get the top N SQL statements ranked by db.load for each time window.
  - If Performance Insights is not enabled, records an advisory notice.

Returns a list of per-instance analysis dicts:
[
  {
    "instance_id":   "mydb",
    "region":        "us-east-1",
    "pi_enabled":    True,
    "top_queries": {
      "7d":  [{"sql": "...", "db_load": 0.4, "calls": 120}, ...],
      "15d": [...],
      "30d": [...]
    },
    "advisory": ""
  },
  ...
]
"""

import logging
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import modules.aws_client as aws_client
import config

logger = logging.getLogger(__name__)

# Maximum number of top queries to retrieve per window
TOP_N = 10

# db.sql.statement dimension key
SQL_DIMENSION = "db.sql.statement"
LOAD_METRIC   = "db.load.avg"


def _date_range(days: int):
    """Return (start_dt, end_dt) UTC for the last N days."""
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    return start, end


def _analyse_instance(resource: dict) -> dict:
    """
    Run Performance Insights analysis for a single RDS instance.
    resource must have keys: name, region, metadata.
    """
    instance_id = resource.get("name") or resource["arn"].split(":")[-1]
    region      = resource.get("region", "us-east-1")
    pi_enabled  = resource.get("metadata", {}).get("performance_insights", False)

    result = {
        "instance_id": instance_id,
        "region":      region,
        "pi_enabled":  pi_enabled,
        "top_queries": {},
        "advisory":    "",
    }

    if not pi_enabled:
        result["advisory"] = (
            "Performance Insights is not enabled for this instance. "
            "Enable it to gain query-level visibility into database load. "
            "Navigate to RDS Console -> Modify -> Performance Insights -> Enable."
        )
        return result

    try:
        pi = aws_client.get_client("pi", region)

        # Retrieve resource metrics endpoint identifier (DBInstanceIdentifier)
        db_identifier = f"db:{instance_id}"

        for days in config.TIME_WINDOWS:
            start, end = _date_range(days)
            # Period: use 1-hour granularity
            period_in_seconds = 3600

            try:
                response = pi.describe_dimension_keys(
                    ServiceType="RDS",
                    Identifier=db_identifier,
                    StartTime=start,
                    EndTime=end,
                    Metric=LOAD_METRIC,
                    PeriodInSeconds=period_in_seconds,
                    GroupBy={
                        "Group":   "db.sql",
                        "Dimensions": [SQL_DIMENSION],
                        "Limit":  TOP_N,
                    },
                )

                top_queries = []
                for key in response.get("Keys", []):
                    dimensions = key.get("Dimensions", {})
                    sql_text   = dimensions.get(SQL_DIMENSION, "<unknown>")
                    total      = key.get("Total", 0.0)
                    top_queries.append({
                        "sql":      sql_text[:500],   # truncate very long statements
                        "db_load":  round(total, 4),
                    })

                result["top_queries"][f"{days}d"] = top_queries

            except pi.exceptions.InvalidArgumentException as e:
                logger.debug(f"PI query failed for {instance_id} [{days}d]: {e}")
                result["top_queries"][f"{days}d"] = []
            except Exception as e:
                logger.debug(f"PI error for {instance_id} [{days}d]: {e}")
                result["top_queries"][f"{days}d"] = []

    except Exception as e:
        logger.warning(f"Could not create Performance Insights client for {instance_id}: {e}")
        result["advisory"] = f"Performance Insights API call failed: {e}"

    return result


def analyze(resources: list) -> list:
    """
    Analyse all RDS resources using Performance Insights.
    Returns a list of per-instance analysis results.
    """
    # Filter to RDS instances only
    rds_resources = [
        r for r in resources
        if r.get("type") in ("AWS::RDS::DBInstance", "AWS::RDS::DBCluster")
    ]

    if not rds_resources:
        logger.info("No RDS instances found - skipping query analysis.")
        return []

    logger.info(f"Analysing {len(rds_resources)} RDS instance(s) via Performance Insights...")

    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(_analyse_instance, r): r
            for r in rds_resources
        }
        for future in as_completed(futures):
            resource = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.debug(f"Query analysis future failed for {resource.get('name')}: {e}")

    pi_enabled_count = sum(1 for r in results if r["pi_enabled"])
    logger.info(
        f"Query analysis complete: {pi_enabled_count}/{len(results)} instances "
        f"have Performance Insights enabled."
    )
    return results
