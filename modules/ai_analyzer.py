"""
modules/ai_analyzer.py
------------------------
Phase 7: Augments the analysis output with Gemini 2.5 Pro insights
via Google Vertex AI.

Makes 5 focused API calls:
  1. Executive summary for the C-suite
  2. Root-cause analysis for detected performance spikes
  3. Enhanced query analysis with optimisation suggestions
  4. Prioritised and expanded rightsizing recommendations
  5. Security risk prioritisation with business-context explanations

Each call is wrapped in a try/except so analysis degrades gracefully
if the user passes --no-ai, the GCP credentials are missing, or any
single call fails.

Returns:
{
  "executive_summary":         "...",
  "root_cause_analysis":       [...],
  "query_analysis_enhanced":   [...],
  "recommendations_enhanced":  [...],
  "security_prioritised":      [...],
  "model_used":                "gemini-2.5-pro",
  "errors":                    [...],
}
"""

import json
import logging

import config

logger = logging.getLogger(__name__)

# Ordered list of model IDs to try.  The first one that responds without a
# 404 is used for all calls in the session.  This lets the tool fall back
# gracefully when a preview model is not yet available in the project while
# still preferring the configured (newer) model when it is available.
_MODEL_FALLBACKS = [
    config.GEMINI_MODEL,                    # primary: from config / .env
    "gemini-2.5-pro",                        # stable GA
    "gemini-1.5-pro-002",                   # previous stable GA
]
# De-duplicate while preserving order (config may already equal a fallback).
_MODEL_FALLBACKS = list(dict.fromkeys(_MODEL_FALLBACKS))

# The resolved model ID is set during _build_client() and used in logging.
MODEL_ID = config.GEMINI_MODEL


def _build_client():
    """Initialise the Vertex AI Generative Model client.

    Probes each model in _MODEL_FALLBACKS with a cheap token-free preflight
    call and returns the first one that responds successfully.  Raises if
    none of the candidates are accessible in the configured GCP project.
    """
    global MODEL_ID
    import vertexai
    from vertexai.generative_models import GenerativeModel, GenerationConfig
    import os
    from google.oauth2 import service_account

    if not config.GCP_PROJECT_ID or not config.GCP_SA_KEY_PATH:
        raise ValueError(
            "GCP_PROJECT_ID and GCP_SA_KEY_PATH must be set in .env for AI analysis."
        )

    credentials = service_account.Credentials.from_service_account_file(
        config.GCP_SA_KEY_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    vertexai.init(
        project=config.GCP_PROJECT_ID,
        location=config.GCP_LOCATION,
        credentials=credentials,
    )

    generation_config = GenerationConfig(
        temperature=0.0,
        max_output_tokens=8192,
        candidate_count=1,
    )

    last_exc = None
    for candidate in _MODEL_FALLBACKS:
        try:
            model = GenerativeModel(candidate, generation_config=generation_config)
            # Lightweight probe: count tokens only — no content generated, no cost.
            model.count_tokens("ping")
            MODEL_ID = candidate
            if candidate != config.GEMINI_MODEL:
                logger.info(
                    "Primary model '%s' unavailable; using fallback '%s'.",
                    config.GEMINI_MODEL, candidate,
                )
            else:
                logger.info("Using Gemini model: %s", candidate)
            return model
        except Exception as exc:  # noqa: BLE001
            logger.debug("Model probe failed for '%s': %s", candidate, exc)
            last_exc = exc

    raise RuntimeError(
        f"None of the configured Gemini models are accessible in project "
        f"'{config.GCP_PROJECT_ID}' (tried: {_MODEL_FALLBACKS}). "
        f"Last error: {last_exc}"
    )


def _call_gemini(model, prompt: str, label: str) -> str:
    """Send a prompt and return the response text.

    Raises on API failure so the per-call try/except blocks in analyze()
    correctly record the error in result['errors'] and report an accurate
    error count rather than silently returning empty strings.
    """
    response = model.generate_content(prompt)
    return response.text or ""


def _summarise_for_prompt(data_subset, max_items: int = 20) -> str:
    """
    Convert a data subset to a compact JSON string for inclusion in prompts.
    Truncates list items to avoid excessive token usage.
    """
    if isinstance(data_subset, list):
        data_subset = data_subset[:max_items]
    return json.dumps(data_subset, default=str, indent=2)


def _extract_json(raw: str) -> str:
    """
    Extract clean JSON from a Gemini response, handling all common formats:

    - Plain JSON with no fences
    - ```json ... ``` / ``` ... ``` fenced blocks
    - Text prose before the first [ or {
    - Wrapper objects like {"plan": [...]} where the value is the desired array
    """
    raw = raw.strip()

    # 1. Strip Markdown fences when present
    if "```" in raw:
        parts = raw.split("```")
        if len(parts) >= 3:
            inner = parts[1]
            newline = inner.find("\n")
            if newline != -1:
                tag = inner[:newline].strip().lower()
                if tag and not tag.startswith(("{", "[")):
                    inner = inner[newline + 1:]
            raw = inner.strip()

    # 2. Find the first JSON-start character and trim any leading prose
    first_bracket = raw.find("[")
    first_brace   = raw.find("{")
    if first_bracket == -1 and first_brace == -1:
        return raw  # no JSON found — return as-is for the caller to handle

    if first_bracket == -1 or (first_brace != -1 and first_brace < first_bracket):
        # Starts with an object — check if it wraps an array in any value
        raw = raw[first_brace:]
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                # Return the first list-valued key (unwrap the wrapper)
                for v in obj.values():
                    if isinstance(v, list):
                        return json.dumps(v)
        except Exception:
            pass
    elif first_bracket >= 0:
        raw = raw[first_bracket:]

    # 3. Trim any trailing prose after the final ] or }
    for end_char, start_char in (("]", "["), ("}", "{")):
        last = raw.rfind(end_char)
        if last != -1:
            candidate = raw[:last + 1]
            try:
                json.loads(candidate)
                return candidate
            except Exception:
                pass

    return raw


# ────────────────────────────────────────────────────────────────────────────
# Individual analysis functions
# ────────────────────────────────────────────────────────────────────────────

def _executive_summary(model, billing: dict, resources: list,
                       recommendations: list, security_findings: list) -> str:
    """Generate a professional C-suite executive summary."""
    total_30d    = billing.get("30d", {}).get("total", 0)
    top_services = billing.get("30d", {}).get("by_service", [])[:5]
    regions_count = len(set(r.get("region", "") for r in resources))
    account_name  = config.AWS_ACCOUNT_NAME or "the audited account"

    # Savings only for over-provisioned recs that have a confirmed downsize target;
    # "No smaller resource available" recs are locked at $0.00 by design.
    total_savings  = sum(
        r.get("estimated_monthly_savings_usd", 0)
        for r in recommendations
        if r.get("recommended_config") != "No smaller resource available"
    )
    annual_savings       = total_savings * 12
    total_cost_increase  = sum(r.get("estimated_monthly_cost_increase_usd", 0) for r in recommendations)

    over_count   = sum(1 for r in recommendations
                       if r.get("recommendation_type") == "Over-provisioned"
                       and r.get("recommended_config") != "No smaller resource available")
    under_count  = sum(1 for r in recommendations
                       if r.get("recommendation_type") == "Under-provisioned")
    flag_count   = sum(1 for r in recommendations
                       if r.get("recommendation_type") == "Review"
                       or r.get("recommended_config") == "No smaller resource available")

    high_findings = sum(1 for f in security_findings if f.get("severity") == "HIGH")
    med_findings  = sum(1 for f in security_findings if f.get("severity") == "MEDIUM")

    prompt = f"""You are a senior AWS cloud consultant writing an executive summary for {account_name}'s infrastructure audit report.

Audit data (USE ONLY THESE FIGURES — do not invent or adjust numbers):
- Total AWS spend (last 30 days): ${total_30d:,.2f}
- Top cost drivers: {json.dumps([s.get('service') + ' $' + str(round(s.get('cost',0),2)) for s in top_services], default=str)}
- Resources inventoried: {len(resources)} across {regions_count} regions
- Right-sizing: {over_count} over-provisioned (saves ${total_savings:,.2f}/month · ${annual_savings:,.2f}/year), \
{under_count} under-provisioned (${total_cost_increase:,.2f}/month investment required to fix), \
{flag_count} flagged for manual review
- Security findings: {high_findings} HIGH-severity, {med_findings} MEDIUM-severity

Write exactly 2 paragraphs of plain prose (no markdown, no bullets, no headers).
Paragraph 1 (3–4 sentences): Current cost state — total spend, top cost driver, right-sizing savings opportunity with annualised value, and any under-provisioned risk.
Paragraph 2 (3–4 sentences): Security posture — number of HIGH-severity findings, most critical risk category, business/compliance exposure, and the single highest-priority remediation action.
Tone: professional, direct, client-ready. Every number cited must exactly match the audit data above.""".strip()

    return _call_gemini(model, prompt, "executive_summary")


def _root_cause_analysis(model, spikes: list, query_analysis: list) -> list:
    """Provide AI-driven root cause analysis for detected performance anomalies."""
    if not spikes and not query_analysis:
        return []

    prompt = f"""You are a cloud performance engineer. Analyse the following AWS performance anomalies and return a JSON array.

Performance spikes:
{_summarise_for_prompt(spikes, 15)}

RDS queries:
{_summarise_for_prompt(query_analysis, 5)}

For each significant anomaly return an object with keys:
- resource_id, metric, likely_cause (1 sentence), action (1 sentence)

Return ONLY valid JSON array, no prose.""".strip()

    raw = _call_gemini(model, prompt, "root_cause_analysis")
    if not raw:
        return []
    try:
        return json.loads(_extract_json(raw))
    except Exception:
        logger.debug("Root cause JSON parse failed, returning raw text")
        return [{"raw_analysis": raw}]


def _enhanced_query_analysis(model, query_analysis: list) -> list:
    """Enhance RDS query analysis with optimisation suggestions."""
    if not query_analysis:
        return []

    prompt = f"""You are a database performance expert. Review this RDS Performance Insights query data and give 2-3 bullet optimisation tips per instance.

Query data:
{_summarise_for_prompt(query_analysis, 5)}

Return a JSON array. Each object: instance_id (string), optimisation_suggestions (array of short strings, max 3 items, each under 15 words).
Return ONLY valid JSON.""".strip()

    raw = _call_gemini(model, prompt, "enhanced_query_analysis")
    if not raw:
        return []
    try:
        return json.loads(_extract_json(raw))
    except Exception:
        return [{"raw_analysis": raw}]


def _enhanced_recommendations(model, recommendations: list, billing: dict) -> list:
    """Enhance right-sizing and scaling recommendations with expert implementation guidance."""
    if not recommendations:
        return []

    # Build a compact per-recommendation payload for the prompt.
    # "No smaller resource available" recs are re-typed as "No-downsize target"
    # so Gemini produces appropriate review steps instead of downsize steps.
    compact = [
        {
            "resource_id":         r.get("resource_name", r.get("resource_id", "")),
            "service":             r.get("service", "").upper(),
            "region":              r.get("region", ""),
            "recommendation_type": (
                "No-downsize target"
                if r.get("recommended_config") == "No smaller resource available"
                else r.get("recommendation_type", "Over-provisioned")
            ),
            "severity":            r.get("severity", "LOW"),
            "current_config":      r.get("current_config", ""),
            # Send None for no-downsize recs so the model doesn't cite the
            # placeholder string as a target instance type.
            "recommended_config":  (
                None
                if r.get("recommended_config") == "No smaller resource available"
                else r.get("recommended_config", "")
            ),
            # Financial fields — let the model cite these verbatim.
            "savings_usd":         round(r.get("estimated_monthly_savings_usd", 0), 2),
            "cost_increase_usd":   round(r.get("estimated_monthly_cost_increase_usd", 0), 2),
            # Pass the measured metric evidence so the model can cite real numbers.
            "reason":              r.get("reason", ""),
        }
        for r in recommendations[:30]
    ]

    prompt = f"""You are a senior AWS cloud architect writing specific implementation guidance for a client infrastructure audit report.

Recommendations to enhance:
{json.dumps(compact, indent=2)}

"recommendation_type" definitions:
- "Over-provisioned": Measured utilisation is far below threshold — downsize to cut cost.
  savings_usd is the exact confirmed monthly saving from the AWS Pricing API.
- "Under-provisioned": Measured utilisation is near or above threshold — upsize to prevent failures.
  cost_increase_usd is the estimated additional monthly spend.
- "No-downsize target": Metrics confirm over-provisioning but no smaller class exists in this instance
  family. savings_usd is $0.00. Focus: identify a better-fit family or commitment discount.
- "Review": No utilisation data — confirm whether the resource is still in active use.

For EACH item return a JSON object with exactly these keys:

"resource_id": copy verbatim from input.

"recommendation_type": copy verbatim from input.

"risk_level": map input severity HIGH→"High", MEDIUM→"Medium", LOW→"Low".

"implementation_steps": array of exactly 2 actionable steps, each up to 25 words.
  Derive steps from the actual service, current_config, recommended_config, and reason values — not generic advice.
  Over-provisioned:    step 1 = create a service-appropriate backup or snapshot first;
                       step 2 = resize using the correct AWS console path or CLI command for this service,
                       citing the exact transition e.g. db.r5.large → db.r5.medium via RDS Modify.
  Under-provisioned:   step 1 = schedule a maintenance window aligned to lowest-traffic hours for this workload;
                       step 2 = resize to recommended_config using the service-specific console workflow,
                       noting any service-restart window if applicable.
  No-downsize target:  step 1 = check whether a Graviton-based equivalent class (e.g. db.t4g for RDS,
                       m7g for EC2, cache.t4g for ElastiCache) is supported for this workload;
                       step 2 = if workload is predictable, purchase a 1-year Reserved Instance or
                       Compute Savings Plan for current_config to cut the bill by ~35-40 %% immediately.
  Review:              step 1 = open CloudWatch and check the service-appropriate activity metric
                       (e.g. DatabaseConnections for RDS, Invocations for Lambda) over the last 30 days;
                       step 2 = if activity is zero or negligible, create a final snapshot and
                       schedule decommission; otherwise collect baseline to right-size.

CRITICAL: Do NOT mention the resource_id or resource name anywhere inside implementation_steps or
validation — it is displayed in a separate column and repeating it wastes space and reads badly.

"validation": one sentence stating the exact CloudWatch metric name AND a concrete pass/fail threshold
  derived from the reason field (e.g. if reason says "Avg CPU 2.1%", write "Verify CPUUtilization
  stays below 10 %% for 7 days post-resize before decommissioning the snapshot"). Must be specific.

"business_impact": one sentence citing the precise financial outcome:
  Over-provisioned:   state exact savings_usd/month and the annualised equivalent.
  Under-provisioned:  state cost_increase_usd/month as the investment and the performance risk avoided.
  No-downsize target: state that no class-change saving is available; a 1-year RI or Savings Plan
                      at standard rates saves ~35 %% on current_config on-demand cost.
  Review:             state that retaining an idle resource wastes its full on-demand cost each month
                      until decommissioned.

Strict rules:
- Write service-specific, concrete guidance — NEVER copy the generic template phrases from this prompt
- Do NOT invent or modify savings_usd or cost_increase_usd values
- Do NOT use placeholder brackets like [metric_name] — fill in the real value
- Return ONLY a valid JSON array — no prose, no markdown fences""".strip()

    raw = _call_gemini(model, prompt, "enhanced_recommendations")
    if not raw:
        return []
    try:
        return json.loads(_extract_json(raw))
    except Exception:
        return [{"raw_analysis": raw}]


def _prioritised_security(model, security_findings: list) -> list:
    """Produce an expert, client-ready security remediation priority plan."""
    if not security_findings:
        return []

    # Exclude idle/unused resource findings — they are surfaced in a dedicated
    # section and are not security vulnerabilities requiring remediation order.
    sec_only = [
        f for f in security_findings
        if f.get("category") not in ("idle_resource", "unused_resource")
    ]
    if not sec_only:
        return []

    # Compact representation grouped by severity.
    # Include 'service' and 'recommendation' so Gemini produces specific
    # remediation guidance rather than generic domain-level advice.
    high_findings = [{"resource_id":    f.get("resource_id"),
                      "domain":         f.get("domain"),
                      "service":        f.get("service", ""),
                      "issue":          f.get("issue"),
                      "recommendation": f.get("recommendation", "")}
                     for f in sec_only if f.get("severity") == "HIGH"][:20]
    med_findings  = [{"resource_id":    f.get("resource_id"),
                      "domain":         f.get("domain"),
                      "service":        f.get("service", ""),
                      "issue":          f.get("issue"),
                      "recommendation": f.get("recommendation", "")}
                     for f in sec_only if f.get("severity") == "MEDIUM"][:15]
    low_findings  = [{"resource_id":    f.get("resource_id"),
                      "domain":         f.get("domain"),
                      "issue":          f.get("issue")}
                     for f in sec_only if f.get("severity") == "LOW"][:10]

    total_high = sum(1 for f in sec_only if f.get("severity") == "HIGH")
    total_med  = sum(1 for f in sec_only if f.get("severity") == "MEDIUM")
    total_low  = sum(1 for f in sec_only if f.get("severity") == "LOW")

    prompt = f"""You are a senior AWS cloud security architect producing a client-facing remediation priority plan.

Security findings summary:
- HIGH severity ({total_high} total, sample): {json.dumps(high_findings, indent=2)}
- MEDIUM severity ({total_med} total, sample): {json.dumps(med_findings, indent=2)}
- LOW severity ({total_low} total, sample): {json.dumps(low_findings, indent=2)}

Produce a remediation priority plan with exactly 4 groups. Return a JSON array where each object has:
- "priority_group": string — one of: "Critical – Fix Within 24 Hours", "High – Fix Within 1 Week", "Medium – Fix Within 1 Month", "Low – Address Next Quarter"
- "findings_count": integer — number of findings in this group
- "business_risk": string — 1 precise sentence describing the business/compliance risk if not remediated
- "remediation_summary": string — 2-3 sentences of concrete, expert remediation guidance specific to the findings listed (e.g. reference specific AWS services, CLI commands, or console paths)
- "affected_resources": array of strings — resource_ids from the findings that belong to this group (include all, not just samples)

Rules:
- ALL High-severity findings must map to either Critical or High groups
- Be specific: reference the actual domain names and resource types found in the data
- remediation_summary must contain actionable steps, not generic advice
- Return ONLY a valid JSON array, no markdown, no prose outside the JSON""".strip()

    raw = _call_gemini(model, prompt, "prioritised_security")
    if not raw:
        return []
    try:
        return json.loads(_extract_json(raw))
    except Exception:
        return [{"raw_analysis": raw}]


# ────────────────────────────────────────────────────────────────────────────
# Main entry point
# ────────────────────────────────────────────────────────────────────────────

def analyze(billing: dict, performance: dict, query_analysis: list,
            recommendations: list, security_findings: list,
            resources: list, enabled: bool = True) -> dict:
    """
    Run all AI analysis calls.

    Args:
        enabled: Set to False when --no-ai flag is passed.
    """
    result = {
        "executive_summary":        "",
        "root_cause_analysis":      [],
        "query_analysis_enhanced":  [],
        "recommendations_enhanced": [],
        "security_prioritised":     [],
        "model_used":               MODEL_ID if enabled else "disabled",
        "errors":                   [],
    }

    if not enabled:
        logger.info("AI analysis skipped (--no-ai flag).")
        return result

    logger.info("Starting AI analysis with Gemini 2.5 Pro...")

    try:
        model = _build_client()
    except Exception as e:
        msg = f"Could not initialise Vertex AI client: {e}"
        logger.warning(msg)
        result["errors"].append(msg)
        return result

    spikes = performance.get("spike_correlation", [])

    # Call 1: Executive summary
    logger.info("AI call 1/5: Executive summary...")
    try:
        result["executive_summary"] = _executive_summary(
            model, billing, resources, recommendations, security_findings
        )
    except Exception as e:
        result["errors"].append(f"Executive summary: {e}")

    # Call 2: Root cause analysis
    logger.info("AI call 2/5: Root cause analysis...")
    try:
        result["root_cause_analysis"] = _root_cause_analysis(model, spikes, query_analysis)
    except Exception as e:
        result["errors"].append(f"Root cause analysis: {e}")

    # Call 3: Enhanced query analysis
    logger.info("AI call 3/5: Query analysis enhancement...")
    try:
        result["query_analysis_enhanced"] = _enhanced_query_analysis(model, query_analysis)
    except Exception as e:
        result["errors"].append(f"Query analysis enhancement: {e}")

    # Call 4: Enhanced recommendations
    logger.info("AI call 4/5: Recommendation enhancement...")
    try:
        result["recommendations_enhanced"] = _enhanced_recommendations(
            model, recommendations, billing
        )
    except Exception as e:
        result["errors"].append(f"Recommendation enhancement: {e}")

    # Call 5: Security prioritisation
    logger.info("AI call 5/5: Security prioritisation...")
    try:
        result["security_prioritised"] = _prioritised_security(model, security_findings)
    except Exception as e:
        result["errors"].append(f"Security prioritisation: {e}")

    logger.info(
        f"AI analysis complete. Errors: {len(result['errors'])}. "
        f"Model: {MODEL_ID}."
    )
    return result
