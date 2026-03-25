"""
registries/rightsizing_registry.py
------------------------------------
Single source of truth for all rightsizing configuration.

Each service entry is self-describing: it carries every field the generic
evaluator factory in modules/recommendations.py needs to work without any
additional code changes.

Adding a brand-new AWS service here is sufficient — no code change in
recommendations.py is required.  If a service warrants bespoke evaluation
logic (e.g. Lambda's duration-timeout model, DynamoDB's capacity-unit model)
a dedicated hand-crafted evaluator can be registered in the
_HAND_CRAFTED_EVALUATORS dict in recommendations.py, which takes precedence.

Schema per entry
----------------
cw_namespace            str   CloudWatch namespace  (e.g. "AWS/EC2")
arn_resource_id_parser  str   "slash" → split ARN by "/" take last segment
                              "colon" → split ARN by ":" take last segment
primary_metric          str   CloudWatch MetricName for the primary signal
primary_stat            str   Statistic to query  ("Average" | "Sum" | ...)
config_metadata_key     str   Key inside resource["metadata"] that holds the
                              current instance/node/class size string
over_threshold_pct      float avg primary metric below this  → over-provisioned
peak_threshold_pct      float peak primary metric below this (combined with avg)
under_threshold_pct     float avg primary metric above this  → under-provisioned
recommendation_over     str   Human-readable action for over-provisioned signal
recommendation_under    str   Human-readable action for under-provisioned signal

Legacy display-only fields (kept for backwards compatibility):
  metrics, reason_template, recommendation
"""

RIGHTSIZING_REGISTRY = {

    # ── EC2 ──────────────────────────────────────────────────────────────────
    "ec2": {
        # Generic-evaluator fields
        "cw_namespace":             "AWS/EC2",
        "arn_resource_id_parser":   "slash",   # ARN: arn:...:instance/i-0abc → "i-0abc"
        "primary_metric":           "CPUUtilization",
        "primary_stat":             "Average",
        "config_metadata_key":      "instance_type",
        "over_threshold_pct":       10.0,
        "peak_threshold_pct":       30.0,
        "under_threshold_pct":      85.0,
        "recommendation_over":      "Downsize to a smaller instance type",
        "recommendation_under":     "Upsize instance type or enable auto-scaling",
        # Legacy display fields
        "metrics":          ["CPUUtilization_Average", "CPUUtilization_Maximum"],
        "reason_template":  "Average CPU {avg_cpu:.1f}% (peak {peak_cpu:.1f}%) over 30 days is below threshold.",
        "recommendation":   "Consider downsizing to the next smaller instance type.",
    },

    # ── RDS ──────────────────────────────────────────────────────────────────
    "rds": {
        "cw_namespace":             "AWS/RDS",
        "arn_resource_id_parser":   "colon",   # ARN: arn:...:db:my-db-id → "my-db-id"
        "primary_metric":           "CPUUtilization",
        "primary_stat":             "Average",
        "config_metadata_key":      "instance_class",
        "over_threshold_pct":       15.0,
        "peak_threshold_pct":       50.0,
        "under_threshold_pct":      85.0,
        "recommendation_over":      "Downsize DB instance class",
        "recommendation_under":     "Upsize DB instance class or add read replicas",
        # Legacy display fields
        "metrics":          ["CPUUtilization_Average", "FreeableMemory_Average"],
        "reason_template":  "Average CPU {avg_cpu:.1f}%, free memory {free_mem_pct:.1f}% of total.",
        "recommendation":   "Consider a smaller DB instance class.",
    },

    # ── Lambda ───────────────────────────────────────────────────────────────
    # Hand-crafted evaluator uses duration-timeout logic; generic fields are
    # here for documentation and as fallback if the hand-crafted entry is
    # ever removed.
    "lambda": {
        "cw_namespace":             "AWS/Lambda",
        "arn_resource_id_parser":   "colon",   # ARN last segment = FunctionName
        "primary_metric":           "Duration",
        "primary_stat":             "Average",
        "config_metadata_key":      "memory_mb",
        "over_threshold_pct":       20.0,      # avg duration < 20 % of timeout
        "peak_threshold_pct":       100.0,
        "under_threshold_pct":      80.0,      # avg duration ≥ 80 % of timeout
        "recommendation_over":      "Reduce memory allocation (run Lambda Power Tuning)",
        "recommendation_under":     "Increase memory/timeout or refactor function logic",
        # Legacy display fields
        "metrics":          ["Duration_Average", "Duration_Maximum"],
        "reason_template":  "Average duration {avg_dur_pct:.1f}% of configured timeout.",
        "recommendation":   "Reduce memory allocation and timeout to match actual usage.",
    },

    # ── ElastiCache ──────────────────────────────────────────────────────────
    "elasticache": {
        "cw_namespace":             "AWS/ElastiCache",
        "arn_resource_id_parser":   "colon",
        "primary_metric":           "CPUUtilization",
        "primary_stat":             "Average",
        "config_metadata_key":      "node_type",
        "over_threshold_pct":       20.0,
        "peak_threshold_pct":       50.0,
        "under_threshold_pct":      80.0,
        "recommendation_over":      "Consider a smaller cache node type",
        "recommendation_under":     "Upsize cache node or add read replicas",
        # Legacy display fields
        "metrics":          ["CPUUtilization_Average", "FreeableMemory_Average"],
        "reason_template":  "Average CPU {avg_cpu:.1f}%, cache hit rate {hit_rate:.1f}%.",
        "recommendation":   "Consider a smaller node type or fewer nodes.",
    },

    # ── DynamoDB ─────────────────────────────────────────────────────────────
    # Hand-crafted evaluator uses capacity-unit comparison and throttle signals;
    # generic fields below are still populated so the factory could fall back
    # if the hand-crafted entry is removed.
    "dynamodb": {
        "cw_namespace":             "AWS/DynamoDB",
        "arn_resource_id_parser":   "slash",   # ARN: arn:...:table/TableName → "TableName"
        "primary_metric":           "ConsumedReadCapacityUnits",
        "primary_stat":             "Sum",
        "config_metadata_key":      "billing_mode",
        "over_threshold_pct":       25.0,      # consumed < 25 % of provisioned
        "peak_threshold_pct":       100.0,
        "under_threshold_pct":      85.0,
        "recommendation_over":      "Switch to on-demand billing or reduce provisioned capacity",
        "recommendation_under":     "Increase provisioned capacity or switch to on-demand mode",
        # Legacy display fields
        "metrics": [
            "ConsumedReadCapacityUnits_Sum",
            "ConsumedWriteCapacityUnits_Sum",
            "ProvisionedReadCapacityUnits_Average",
            "ProvisionedWriteCapacityUnits_Average",
        ],
        "reason_template":  "Consumed {rcu_pct:.1f}% of provisioned RCU, {wcu_pct:.1f}% of provisioned WCU.",
        "recommendation":   "Switch to on-demand billing mode or reduce provisioned capacity.",
    },

    # ── ECS ──────────────────────────────────────────────────────────────────
    # Generic CPU evaluator is sufficient for ECS services.
    "ecs": {
        "cw_namespace":             "AWS/ECS",
        "arn_resource_id_parser":   "slash",   # ARN: arn:...:service/cluster/SvcName → "SvcName"
        "primary_metric":           "CPUUtilization",
        "primary_stat":             "Average",
        "config_metadata_key":      "task_cpu",
        "over_threshold_pct":       20.0,
        "peak_threshold_pct":       40.0,
        "under_threshold_pct":      80.0,
        "recommendation_over":      "Reduce task CPU/memory limits in the task definition",
        "recommendation_under":     "Increase task CPU/memory limits or scale out horizontally",
        # Legacy display fields
        "metrics":          ["CPUUtilization_Average", "MemoryUtilization_Average"],
        "reason_template":  "Average CPU {avg_cpu:.1f}%, average memory {avg_mem:.1f}%.",
        "recommendation":   "Reduce task CPU/memory limits in the task definition.",
    },

    # ── Redshift ─────────────────────────────────────────────────────────────
    # Generic CPU evaluator is sufficient for Redshift clusters.
    "redshift": {
        "cw_namespace":             "AWS/Redshift",
        "arn_resource_id_parser":   "colon",   # ARN last segment = ClusterIdentifier
        "primary_metric":           "CPUUtilization",
        "primary_stat":             "Average",
        "config_metadata_key":      "node_type",
        "over_threshold_pct":       15.0,
        "peak_threshold_pct":       40.0,
        "under_threshold_pct":      85.0,
        "recommendation_over":      "Downsize cluster node type or reduce node count",
        "recommendation_under":     "Upsize cluster nodes or increase node count",
        # Legacy display fields
        "metrics":          ["CPUUtilization_Average", "PercentageDiskSpaceUsed_Average"],
        "reason_template":  "Average CPU {avg_cpu:.1f}%, disk used {disk_pct:.1f}%.",
        "recommendation":   "Consider a smaller node type or fewer nodes.",
    },

    # ─────────────────────────────────────────────────────────────────────────
    # To add support for a NEW AWS service, simply add an entry here following
    # the schema above.  No changes to recommendations.py are needed — a
    # generic evaluator will be auto-generated at startup from these fields.
    # ─────────────────────────────────────────────────────────────────────────
}
