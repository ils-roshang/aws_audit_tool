"""
main.py
--------
Entry point for the AWS Infrastructure Dashboard Tool.

Usage:
    python main.py [options]

Options:
    --format  {pdf,excel,both}   Output format (default: both)
    --no-ai                      Skip Gemini AI analysis
    --region  REGION[,REGION]    Comma-separated region override (default: auto-discover)
    --output  DIR                Output directory (default: ./output)
    --verbose                    Enable DEBUG logging

Example:
    python main.py --format both --region us-east-1,eu-west-1
    python main.py --no-ai --format excel
"""

import argparse
import logging
import sys
import os
from datetime import datetime, timezone

# ── Configure logging before any module imports ───────────────────────────
def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Quiet noisy third-party loggers
    for lib in ("botocore", "boto3", "urllib3", "matplotlib", "PIL"):
        logging.getLogger(lib).setLevel(logging.WARNING)


def _parse_args():
    parser = argparse.ArgumentParser(
        description="AWS Infrastructure Dashboard — generates PDF and/or Excel reports."
    )
    parser.add_argument(
        "--format",
        choices=["pdf", "excel", "both"],
        default="both",
        help="Output format (default: both)",
    )
    parser.add_argument(
        "--no-ai",
        action="store_true",
        dest="no_ai",
        help="Skip Gemini AI analysis",
    )
    parser.add_argument(
        "--region",
        default="",
        help="Comma-separated list of AWS regions to scan (default: auto-discover all)",
    )
    parser.add_argument(
        "--output",
        default="output",
        help="Output directory for generated reports (default: ./output)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging",
    )
    return parser.parse_args()


def _banner(message: str) -> None:
    """Print a prominent console banner."""
    bar = "=" * 70
    print(f"\n{bar}")
    print(f"  {message}")
    print(f"{bar}")


def main() -> int:
    args = _parse_args()
    _setup_logging(args.verbose)

    logger = logging.getLogger("main")

    _banner("AWS Infrastructure Dashboard  —  Starting")
    print(f"  Format  : {args.format}")
    print(f"  AI      : {'disabled' if args.no_ai else 'enabled'}")
    print(f"  Regions : {args.region if args.region else 'auto-discover'}")
    print(f"  Output  : {args.output}")

    # ── Validate config ────────────────────────────────────────────────────
    print("\n[1/8] Validating configuration...")
    try:
        import config
        config.validate()
    except EnvironmentError as e:
        logger.error(f"Configuration error: {e}")
        print(f"\nERROR: {e}")
        print("Please copy .env.example to .env and fill in your credentials.")
        return 1

    region_override = [r.strip() for r in args.region.split(",") if r.strip()] or None

    # ── Phase 2: Service discovery ─────────────────────────────────────────
    print("\n[2/8] Discovering active AWS services and regions...")
    from modules.service_discovery import run as discover_services
    try:
        service_map, regions = discover_services(region_override)
    except Exception as e:
        logger.error(f"Service discovery failed: {e}", exc_info=True)
        print(f"\nERROR during service discovery: {e}")
        return 1

    total_types = sum(1 for k in service_map if not k.startswith("__"))
    total_arns  = sum(len(v.get("arns", [])) for k, v in service_map.items()
                      if not k.startswith("__"))
    print(f"  Found {total_types} resource types, {total_arns} total resources "
          f"across {len(regions)} region(s).")

    # ── Phase 3: Resource collection ──────────────────────────────────────
    print("\n[3/8] Collecting resource metadata...")
    from modules.resource_collector import collect as collect_resources
    resources = collect_resources(service_map, regions)
    print(f"  Collected metadata for {len(resources)} resources.")

    # ── Phase 4: Cost analysis ─────────────────────────────────────────────
    print("\n[4/8] Analysing billing and cost data...")
    from modules.cost_analyzer import analyze as analyze_costs
    billing = analyze_costs()
    total_30d = billing.get("30d", {}).get("total", 0.0)
    print(f"  30-day total spend: ${total_30d:,.2f}")

    # ── Phase 5a: Performance analysis ────────────────────────────────────
    print("\n[5/8] Collecting CloudWatch performance metrics (this may take a while)...")
    from modules.performance_analyzer import analyze as analyze_performance
    performance = analyze_performance(service_map, regions)
    ns_count    = len(performance.get("by_namespace", {}))
    spike_count = len(performance.get("spike_correlation", []))
    print(f"  Metrics collected for {ns_count} namespace(s). "
          f"{spike_count} anomalies detected.")

    # ── Phase 5b: RDS Query analysis ──────────────────────────────────────
    print("\n[5b/8] Analysing RDS query performance...")
    from modules.query_analyzer import analyze as analyze_queries
    query_analysis = analyze_queries(resources)
    pi_count = sum(1 for r in query_analysis if r.get("pi_enabled"))
    print(f"  Analysed {len(query_analysis)} RDS instance(s) "
          f"({pi_count} with Performance Insights).")

    # ── Phase 6a: Right-sizing recommendations ────────────────────────────
    print("\n[6/8] Generating right-sizing recommendations...")
    from modules.recommendations import generate as generate_recommendations
    recommendations = generate_recommendations(resources, performance, billing)
    total_savings = sum(r.get("estimated_monthly_savings_usd", 0) for r in recommendations)
    print(f"  {len(recommendations)} recommendation(s) identified. "
          f"Potential monthly savings: ${total_savings:,.2f}.")

    # ── Phase 6b: Security audit ───────────────────────────────────────────
    print("\n[6b/8] Running security audit...")
    from modules.security_auditor import run as run_security_audit
    security_findings = run_security_audit(resources, service_map, regions)
    high_count = sum(1 for f in security_findings if f.get("severity") == "HIGH")
    print(f"  {len(security_findings)} finding(s) "
          f"({high_count} HIGH severity).")

    # ── Phase 6c: IAM credential report ───────────────────────────────────
    print("       Collecting IAM credential report...")
    iam_credential_report = []
    try:
        import csv as _csv
        import io as _sio
        import time as _time2
        _iam_cr = aws_client.get_client("iam", config.AWS_DEFAULT_REGION)
        for _attempt in range(5):
            if _iam_cr.generate_credential_report().get("State") == "COMPLETE":
                break
            _time2.sleep(2)
        _raw_cr  = _iam_cr.get_credential_report()["Content"]
        _content = _raw_cr.decode("utf-8") if isinstance(_raw_cr, bytes) else _raw_cr
        for _row in _csv.DictReader(_sio.StringIO(_content)):
            iam_credential_report.append(dict(_row))
    except Exception as _exc:
        logger.debug(f"IAM credential report collection failed: {_exc}")

    # ── Phase 6d: Open ports map (parallel per region) ────────────────────
    print("       Building open ports map...")
    open_ports_map = []
    try:
        from concurrent.futures import ThreadPoolExecutor as _SGTP

        def _ports_for_region(_rgn):
            _entries = []
            try:
                _ec2_sg = aws_client.get_client("ec2", _rgn)
                for _page in _ec2_sg.get_paginator("describe_security_groups").paginate():
                    for _sg in _page.get("SecurityGroups", []):
                        _sg_id   = _sg["GroupId"]
                        _sg_name = _sg.get("GroupName", _sg_id)
                        for _direction, _pkey in [("inbound",  "IpPermissions"),
                                                  ("outbound", "IpPermissionsEgress")]:
                            for _perm in _sg.get(_pkey, []):
                                _fp    = _perm.get("FromPort", 0)
                                _tp    = _perm.get("ToPort",   65535)
                                _proto = _perm.get("IpProtocol", "-1")
                                for _cidr in (
                                    [r.get("CidrIp",   "") for r in _perm.get("IpRanges",   [])] +
                                    [r.get("CidrIpv6", "") for r in _perm.get("Ipv6Ranges", [])]
                                ):
                                    if _cidr:
                                        _entries.append({
                                            "sg_id":       _sg_id,
                                            "sg_name":     _sg_name,
                                            "region":      _rgn,
                                            "protocol":    _proto,
                                            "from_port":   _fp if _proto != "-1" else 0,
                                            "to_port":     _tp if _proto != "-1" else 65535,
                                            "source_cidr": _cidr,
                                            "direction":   _direction,
                                        })
            except Exception as _re:
                logger.debug(f"Open ports map failed for {_rgn}: {_re}")
            return _entries

        with _SGTP(max_workers=min(8, len(regions))) as _pool:
            for _rgn_entries in _pool.map(_ports_for_region, regions):
                open_ports_map.extend(_rgn_entries)
    except Exception as _exc:
        logger.debug(f"Open ports map collection failed: {_exc}")

    # ── Phase 7a: Trend & pattern analysis ──────────────────────────────────
    print("\n[7a/8] Running trend and pattern analysis...")
    from modules.trend_analyzer import analyze as analyze_trends
    trends = analyze_trends(
        billing=billing,
        performance=performance,
        resources=resources,
        recommendations=recommendations,
        security_findings=security_findings,
    )
    cost_dir = trends.get("cost_trends", {}).get("overall", {}).get("direction", "STABLE")
    p_trend_cnt = len(trends.get("performance_trends", []))
    sys_cnt = len(trends.get("fleet_patterns", {}).get("systemic_issues", []))
    print(f"  Cost trend: {cost_dir}   ·   {p_trend_cnt} metric trend(s)   ·   {sys_cnt} fleet pattern(s).")

    # ── Phase 7: AI analysis ───────────────────────────────────────────────
    print(f"\n[7/8] {'Running Gemini AI analysis...' if not args.no_ai else 'Skipping AI analysis (--no-ai).'}")
    from modules.ai_analyzer import analyze as analyze_ai
    ai_insights = analyze_ai(
        billing=billing,
        performance=performance,
        query_analysis=query_analysis,
        recommendations=recommendations,
        security_findings=security_findings,
        resources=resources,
        enabled=not args.no_ai,
    )
    if not args.no_ai:
        ai_errors = ai_insights.get("errors", [])
        if ai_errors:
            print(f"  AI analysis completed with {len(ai_errors)} error(s): "
                  f"{'; '.join(ai_errors[:2])}")
        else:
            print(f"  AI analysis complete (model: {ai_insights.get('model_used')}).")

    # ── Retrieve account identity ──────────────────────────────────────────
    account_id   = "unknown"
    account_name = config.AWS_ACCOUNT_NAME or "unknown"
    try:
        import modules.aws_client as aws_client
        sts     = aws_client.get_client("sts", config.AWS_DEFAULT_REGION)
        identity = sts.get_caller_identity()
        account_id = identity.get("Account", "unknown")
    except Exception as e:
        logger.debug(f"Could not retrieve account identity: {e}")

    # ── Assemble consolidated data dict ───────────────────────────────────
    consolidated = {
        "account_id":            account_id,
        "account_name":          account_name,
        "generated_at":          datetime.now(timezone.utc).isoformat(),
        "regions_scanned":       regions,
        "service_map":           service_map,
        "resources":             resources,
        "billing":               billing,
        "performance":           performance,
        "query_analysis":        query_analysis,
        "recommendations":       recommendations,
        "security_findings":     security_findings,
        "trends":                trends,
        "iam_credential_report": iam_credential_report,
        "open_ports_map":        open_ports_map,
        "ai_insights":           ai_insights,
    }

    # ── Phase 8: Report generation ─────────────────────────────────────────
    print(f"\n[8/8] Generating {args.format.upper()} report(s)...")
    from modules.report_generator import generate as generate_reports
    output_paths = generate_reports(consolidated, args.output, fmt=args.format)

    # ── Summary ────────────────────────────────────────────────────────────
    _banner("Report Generation Complete")
    if output_paths.get("pdf"):
        print(f"  PDF   : {os.path.abspath(output_paths['pdf'])}")
    if output_paths.get("excel"):
        print(f"  Excel : {os.path.abspath(output_paths['excel'])}")

    print("\nKey findings:")
    print(f"  • 30-day spend    : ${total_30d:,.2f}")
    print(f"  • Resources found : {len(resources)}")
    print(f"  • Savings found   : ${total_savings:,.2f}/month")
    print(f"  • Security HIGH   : {high_count} findings")
    print(f"  • Anomalies       : {spike_count} detected")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
