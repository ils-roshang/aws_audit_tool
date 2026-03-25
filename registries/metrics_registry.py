"""
registries/metrics_registry.py
-------------------------------
Maps CloudWatch namespaces to the key metrics to collect.
Each entry is a list of metric definition dicts:
  {
      "name":      CloudWatch metric name,
      "stat":      "Average" | "Maximum" | "Sum" | "SampleCount",
      "unit":      CloudWatch unit string (optional, used for display),
      "dim_key":   primary dimension key name (e.g. "InstanceId"),
  }
For namespaces NOT present here, the tool falls back to calling
list_metrics() and pulling every available metric automatically.
"""

METRICS_REGISTRY = {
    "AWS/EC2": [
        {"name": "CPUUtilization",    "stat": "Average", "unit": "Percent",      "dim_key": "InstanceId"},
        {"name": "CPUUtilization",    "stat": "Maximum", "unit": "Percent",      "dim_key": "InstanceId"},
        {"name": "NetworkIn",         "stat": "Sum",     "unit": "Bytes",        "dim_key": "InstanceId"},
        {"name": "NetworkOut",        "stat": "Sum",     "unit": "Bytes",        "dim_key": "InstanceId"},
        {"name": "DiskReadOps",       "stat": "Sum",     "unit": "Count",        "dim_key": "InstanceId"},
        {"name": "DiskWriteOps",      "stat": "Sum",     "unit": "Count",        "dim_key": "InstanceId"},
        {"name": "StatusCheckFailed", "stat": "Sum",     "unit": "Count",        "dim_key": "InstanceId"},
    ],
    "AWS/RDS": [
        {"name": "CPUUtilization",       "stat": "Average", "unit": "Percent",   "dim_key": "DBInstanceIdentifier"},
        {"name": "CPUUtilization",       "stat": "Maximum", "unit": "Percent",   "dim_key": "DBInstanceIdentifier"},
        {"name": "FreeableMemory",       "stat": "Average", "unit": "Bytes",     "dim_key": "DBInstanceIdentifier"},
        {"name": "DatabaseConnections",  "stat": "Average", "unit": "Count",     "dim_key": "DBInstanceIdentifier"},
        {"name": "DatabaseConnections",  "stat": "Maximum", "unit": "Count",     "dim_key": "DBInstanceIdentifier"},
        {"name": "ReadIOPS",             "stat": "Average", "unit": "Count/Second", "dim_key": "DBInstanceIdentifier"},
        {"name": "WriteIOPS",            "stat": "Average", "unit": "Count/Second", "dim_key": "DBInstanceIdentifier"},
        {"name": "ReadLatency",          "stat": "Average", "unit": "Seconds",   "dim_key": "DBInstanceIdentifier"},
        {"name": "WriteLatency",         "stat": "Average", "unit": "Seconds",   "dim_key": "DBInstanceIdentifier"},
        {"name": "FreeStorageSpace",     "stat": "Average", "unit": "Bytes",     "dim_key": "DBInstanceIdentifier"},
    ],
    "AWS/Lambda": [
        {"name": "Invocations",    "stat": "Sum",     "unit": "Count",        "dim_key": "FunctionName"},
        {"name": "Errors",         "stat": "Sum",     "unit": "Count",        "dim_key": "FunctionName"},
        {"name": "Throttles",      "stat": "Sum",     "unit": "Count",        "dim_key": "FunctionName"},
        {"name": "Duration",       "stat": "Average", "unit": "Milliseconds", "dim_key": "FunctionName"},
        {"name": "Duration",       "stat": "Maximum", "unit": "Milliseconds", "dim_key": "FunctionName"},
        {"name": "ConcurrentExecutions", "stat": "Maximum", "unit": "Count",  "dim_key": "FunctionName"},
    ],
    "AWS/S3": [
        {"name": "NumberOfRequests",    "stat": "Sum",     "unit": "Count",        "dim_key": "BucketName"},
        {"name": "4xxErrors",           "stat": "Sum",     "unit": "Count",        "dim_key": "BucketName"},
        {"name": "5xxErrors",           "stat": "Sum",     "unit": "Count",        "dim_key": "BucketName"},
        {"name": "FirstByteLatency",    "stat": "Average", "unit": "Milliseconds", "dim_key": "BucketName"},
        {"name": "TotalRequestLatency", "stat": "Average", "unit": "Milliseconds", "dim_key": "BucketName"},
        {"name": "BytesDownloaded",     "stat": "Sum",     "unit": "Bytes",        "dim_key": "BucketName"},
        {"name": "BytesUploaded",       "stat": "Sum",     "unit": "Bytes",        "dim_key": "BucketName"},
    ],
    "AWS/DynamoDB": [
        {"name": "ConsumedReadCapacityUnits",  "stat": "Sum",     "unit": "Count", "dim_key": "TableName"},
        {"name": "ConsumedWriteCapacityUnits", "stat": "Sum",     "unit": "Count", "dim_key": "TableName"},
        {"name": "ProvisionedReadCapacityUnits",  "stat": "Average", "unit": "Count", "dim_key": "TableName"},
        {"name": "ProvisionedWriteCapacityUnits", "stat": "Average", "unit": "Count", "dim_key": "TableName"},
        {"name": "SuccessfulRequestLatency",  "stat": "Average", "unit": "Milliseconds", "dim_key": "TableName"},
        {"name": "SystemErrors",              "stat": "Sum",     "unit": "Count", "dim_key": "TableName"},
        {"name": "ThrottledRequests",         "stat": "Sum",     "unit": "Count", "dim_key": "TableName"},
    ],
    "AWS/ECS": [
        {"name": "CPUUtilization",    "stat": "Average", "unit": "Percent", "dim_key": "ServiceName"},
        {"name": "CPUUtilization",    "stat": "Maximum", "unit": "Percent", "dim_key": "ServiceName"},
        {"name": "MemoryUtilization", "stat": "Average", "unit": "Percent", "dim_key": "ServiceName"},
        {"name": "MemoryUtilization", "stat": "Maximum", "unit": "Percent", "dim_key": "ServiceName"},
    ],
    "AWS/ElastiCache": [
        {"name": "CPUUtilization",  "stat": "Average", "unit": "Percent", "dim_key": "CacheClusterId"},
        {"name": "FreeableMemory",  "stat": "Average", "unit": "Bytes",   "dim_key": "CacheClusterId"},
        {"name": "CacheHits",       "stat": "Sum",     "unit": "Count",   "dim_key": "CacheClusterId"},
        {"name": "CacheMisses",     "stat": "Sum",     "unit": "Count",   "dim_key": "CacheClusterId"},
        {"name": "NetworkBytesIn",  "stat": "Sum",     "unit": "Bytes",   "dim_key": "CacheClusterId"},
        {"name": "NetworkBytesOut", "stat": "Sum",     "unit": "Bytes",   "dim_key": "CacheClusterId"},
        {"name": "Evictions",       "stat": "Sum",     "unit": "Count",   "dim_key": "CacheClusterId"},
    ],
    "AWS/SQS": [
        {"name": "NumberOfMessagesSent",                   "stat": "Sum",     "unit": "Count", "dim_key": "QueueName"},
        {"name": "NumberOfMessagesReceived",               "stat": "Sum",     "unit": "Count", "dim_key": "QueueName"},
        {"name": "NumberOfMessagesDeleted",                "stat": "Sum",     "unit": "Count", "dim_key": "QueueName"},
        {"name": "ApproximateNumberOfMessagesVisible",     "stat": "Maximum", "unit": "Count", "dim_key": "QueueName"},
        {"name": "ApproximateAgeOfOldestMessage",          "stat": "Maximum", "unit": "Seconds","dim_key": "QueueName"},
    ],
    "AWS/ApplicationELB": [
        {"name": "RequestCount",              "stat": "Sum",     "unit": "Count",        "dim_key": "LoadBalancer"},
        {"name": "TargetResponseTime",        "stat": "Average", "unit": "Seconds",      "dim_key": "LoadBalancer"},
        {"name": "HTTPCode_Target_5XX_Count", "stat": "Sum",     "unit": "Count",        "dim_key": "LoadBalancer"},
        {"name": "HTTPCode_Target_4XX_Count", "stat": "Sum",     "unit": "Count",        "dim_key": "LoadBalancer"},
        {"name": "ActiveConnectionCount",     "stat": "Average", "unit": "Count",        "dim_key": "LoadBalancer"},
        {"name": "HealthyHostCount",          "stat": "Minimum", "unit": "Count",        "dim_key": "TargetGroup"},
        {"name": "UnHealthyHostCount",        "stat": "Maximum", "unit": "Count",        "dim_key": "TargetGroup"},
    ],
    "AWS/NetworkELB": [
        {"name": "ActiveFlowCount",           "stat": "Average", "unit": "Count",  "dim_key": "LoadBalancer"},
        {"name": "ProcessedBytes",            "stat": "Sum",     "unit": "Bytes",  "dim_key": "LoadBalancer"},
        {"name": "HealthyHostCount",          "stat": "Minimum", "unit": "Count",  "dim_key": "TargetGroup"},
        {"name": "UnHealthyHostCount",        "stat": "Maximum", "unit": "Count",  "dim_key": "TargetGroup"},
    ],
    "AWS/ApiGateway": [
        {"name": "Count",       "stat": "Sum",     "unit": "Count",        "dim_key": "ApiName"},
        {"name": "Latency",     "stat": "Average", "unit": "Milliseconds", "dim_key": "ApiName"},
        {"name": "5XXError",    "stat": "Sum",     "unit": "Count",        "dim_key": "ApiName"},
        {"name": "4XXError",    "stat": "Sum",     "unit": "Count",        "dim_key": "ApiName"},
        {"name": "IntegrationLatency", "stat": "Average", "unit": "Milliseconds", "dim_key": "ApiName"},
    ],
    "AWS/Redshift": [
        {"name": "CPUUtilization",        "stat": "Average", "unit": "Percent", "dim_key": "ClusterIdentifier"},
        {"name": "DatabaseConnections",   "stat": "Average", "unit": "Count",   "dim_key": "ClusterIdentifier"},
        {"name": "PercentageDiskSpaceUsed","stat": "Average","unit": "Percent", "dim_key": "ClusterIdentifier"},
        {"name": "ReadIOPS",              "stat": "Average", "unit": "Count/Second", "dim_key": "ClusterIdentifier"},
        {"name": "WriteIOPS",             "stat": "Average", "unit": "Count/Second", "dim_key": "ClusterIdentifier"},
    ],
    "AWS/Kinesis": [
        {"name": "IncomingRecords",      "stat": "Sum",     "unit": "Count", "dim_key": "StreamName"},
        {"name": "GetRecords.IteratorAgeMilliseconds", "stat": "Maximum", "unit": "Milliseconds", "dim_key": "StreamName"},
        {"name": "ReadProvisionedThroughputExceeded",  "stat": "Sum",     "unit": "Count", "dim_key": "StreamName"},
        {"name": "WriteProvisionedThroughputExceeded", "stat": "Sum",     "unit": "Count", "dim_key": "StreamName"},
    ],
    "AWS/SNS": [
        {"name": "NumberOfMessagesPublished",    "stat": "Sum", "unit": "Count", "dim_key": "TopicName"},
        {"name": "NumberOfNotificationsDelivered","stat": "Sum","unit": "Count", "dim_key": "TopicName"},
        {"name": "NumberOfNotificationsFailed",  "stat": "Sum", "unit": "Count", "dim_key": "TopicName"},
    ],
    "AWS/CloudFront": [
        {"name": "Requests",       "stat": "Sum",     "unit": "None",    "dim_key": "DistributionId"},
        {"name": "BytesDownloaded","stat": "Sum",     "unit": "Bytes",   "dim_key": "DistributionId"},
        {"name": "4xxErrorRate",   "stat": "Average", "unit": "Percent", "dim_key": "DistributionId"},
        {"name": "5xxErrorRate",   "stat": "Average", "unit": "Percent", "dim_key": "DistributionId"},
        {"name": "TotalErrorRate", "stat": "Average", "unit": "Percent", "dim_key": "DistributionId"},
    ],
    "AWS/States": [
        {"name": "ExecutionsStarted",   "stat": "Sum", "unit": "Count", "dim_key": "StateMachineArn"},
        {"name": "ExecutionsFailed",    "stat": "Sum", "unit": "Count", "dim_key": "StateMachineArn"},
        {"name": "ExecutionThrottled",  "stat": "Sum", "unit": "Count", "dim_key": "StateMachineArn"},
        {"name": "ExecutionTime",       "stat": "Average", "unit": "Milliseconds", "dim_key": "StateMachineArn"},
    ],
    "AWS/EKS": [
        {"name": "cluster_failed_node_count", "stat": "Maximum", "unit": "Count", "dim_key": "ClusterName"},
        {"name": "node_cpu_utilization",      "stat": "Average", "unit": "Percent","dim_key": "ClusterName"},
        {"name": "node_memory_utilization",   "stat": "Average", "unit": "Percent","dim_key": "ClusterName"},
    ],
}
