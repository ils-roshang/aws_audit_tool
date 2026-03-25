"""
modules/service_discovery.py
-----------------------------
Phase 2: Discovers all enabled AWS regions and all active services
across the entire account using three sources:
  1. AWS Resource Groups Tagging API  - all tagged resource ARNs
  2. CloudWatch list_metrics()        - all active metric namespaces
  3. AWS Config                       - untagged/config-tracked resources

Also collects global resources (IAM, S3, CloudFront, Route53) once.

Returns a canonical service_map and the list of active regions.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import config
import modules.aws_client as aws_client

logger = logging.getLogger(__name__)

# Maps AWS resource type prefix to CloudWatch namespace
RESOURCE_TYPE_TO_NAMESPACE = {
    "AWS::EC2::Instance":                "AWS/EC2",
    "AWS::RDS::DBInstance":              "AWS/RDS",
    "AWS::RDS::DBCluster":               "AWS/RDS",
    "AWS::Lambda::Function":             "AWS/Lambda",
    "AWS::S3::Bucket":                   "AWS/S3",
    "AWS::DynamoDB::Table":              "AWS/DynamoDB",
    "AWS::ECS::Service":                 "AWS/ECS",
    "AWS::ECS::Cluster":                 "AWS/ECS",
    "AWS::EKS::Cluster":                 "AWS/EKS",
    "AWS::ElastiCache::CacheCluster":    "AWS/ElastiCache",
    "AWS::ElastiCache::ReplicationGroup":"AWS/ElastiCache",
    "AWS::Redshift::Cluster":            "AWS/Redshift",
    "AWS::Kinesis::Stream":              "AWS/Kinesis",
    "AWS::SQS::Queue":                   "AWS/SQS",
    "AWS::SNS::Topic":                   "AWS/SNS",
    "AWS::ElasticLoadBalancingV2::LoadBalancer": "AWS/ApplicationELB",
    "AWS::ElasticLoadBalancing::LoadBalancer":   "AWS/ELB",
    "AWS::ApiGateway::RestApi":          "AWS/ApiGateway",
    "AWS::ApiGatewayV2::Api":            "AWS/ApiGateway",
    "AWS::StepFunctions::StateMachine":  "AWS/States",
    "AWS::Glue::Job":                    "AWS/Glue",
    "AWS::CloudFront::Distribution":     "AWS/CloudFront",
    "AWS::SecretsManager::Secret":       None,
    "AWS::IAM::User":                    None,
    "AWS::IAM::Role":                    None,
    # Networking
    "AWS::EC2::VPC":                             None,
    "AWS::EC2::Subnet":                          None,
    "AWS::EC2::InternetGateway":                 None,
    "AWS::EC2::NatGateway":                      None,
    "AWS::EC2::SecurityGroup":                   None,
    "AWS::EC2::RouteTable":                      None,
    "AWS::EC2::EIP":                             None,
    # Storage
    "AWS::EC2::Volume":                          None,
    "AWS::EC2::Snapshot":                        None,
    # Load Balancing — already present as ElasticLoadBalancingV2; alias for elbv2 ARN style
    # Identity / Security
    "AWS::ECR::Repository":                      None,
    "AWS::ACM::Certificate":                     None,
    # Messaging
    "AWS::SNS::Topic":                           None,
    # Compute scaling
    "AWS::AutoScaling::AutoScalingGroup":         "AWS/AutoScaling",
    # Systems Manager (user-owned only)
    "AWS::SSM::Document":                        None,
    "AWS::SSM::PatchBaseline":                   None,
    # Developer tools / Workflow
    "AWS::CloudTrail::Trail":                    None,
    "AWS::ApiGateway::RestApi":                  "AWS/ApiGateway",
    "AWS::Kinesis::Stream":                      "AWS/Kinesis",
    "AWS::Events::Rule":                         None,
    "AWS::StepFunctions::StateMachine":          "AWS/States",
}


# ────────────────────────────────────────────────────────────────────────────
# Cloud Control sweep — curated allowlist
# ────────────────────────────────────────────────────────────────────────────
#
# Only types that are:
#   (a) genuinely user-created resources with real audit value
#   (b) NOT covered by native direct enumeration (sources 1-4)
#   (c) have reliable Cloud Control support
#
# This replaces the previous dynamic cloudformation.list_types() approach
# which returned 400+ types including hundreds of AWS-managed SSM Documents,
# Patch Baselines, XRay defaults, and other noise resources.  The curated
# list reduces API calls by >95% and cuts discovery time from ~30 min to
# seconds for the CC sweep portion.
_CC_SWEEP_TYPES: tuple = (
    # AI / ML ───────────────────────────────────────────────
    "AWS::SageMaker::NotebookInstance",
    "AWS::SageMaker::Model",
    "AWS::SageMaker::Endpoint",
    # Analytics ──────────────────────────────────────────────
    "AWS::Glue::Job",
    "AWS::Glue::Database",
    "AWS::Glue::Crawler",
    "AWS::MSK::Cluster",
    "AWS::OpenSearchService::Domain",
    "AWS::Elasticsearch::Domain",
    "AWS::KinesisFirehose::DeliveryStream",
    "AWS::Athena::WorkGroup",
    # Application Integration ──────────────────────────────
    "AWS::AppSync::GraphQLApi",
    "AWS::ApiGatewayV2::Api",
    # Compute / Batch ──────────────────────────────────────
    "AWS::Batch::JobQueue",
    "AWS::Batch::ComputeEnvironment",
    # Database ──────────────────────────────────────────────
    "AWS::Neptune::DBCluster",
    "AWS::DocDB::DBCluster",
    "AWS::ElastiCache::ReplicationGroup",
    "AWS::MemoryDB::Cluster",
    "AWS::QLDB::Ledger",
    "AWS::Timestream::Database",
    # Developer Tools ─────────────────────────────────────
    "AWS::CodeBuild::Project",
    "AWS::CodePipeline::Pipeline",
    "AWS::CodeCommit::Repository",
    # Management & Operations ─────────────────────────────
    "AWS::Backup::BackupVault",
    # Messaging / Streaming ──────────────────────────────
    "AWS::MQ::Broker",
    # Migration ────────────────────────────────────────────
    "AWS::DMS::ReplicationInstance",
    # Networking ───────────────────────────────────────────
    "AWS::NetworkFirewall::Firewall",
    "AWS::Route53Resolver::ResolverEndpoint",
    # Security ────────────────────────────────────────────
    "AWS::WAFv2::WebACL",
    "AWS::GuardDuty::Detector",
    "AWS::SecurityHub::Hub",
    "AWS::Cognito::UserPool",
    # Transfer / Endpoints ─────────────────────────────
    "AWS::Transfer::Server",
    "AWS::Amplify::App",
)


def _discover_regions(region_override: list = None) -> list:
    """Return list of all enabled AWS regions, or the override list."""
    if region_override:
        logger.info(f"Using region override: {region_override}")
        return region_override
    try:
        ec2 = aws_client.get_client("ec2", config.AWS_DEFAULT_REGION)
        response = ec2.describe_regions(
            Filters=[
                {"Name": "opt-in-status", "Values": ["opt-in-not-required", "opted-in"]}
            ]
        )
        regions = [r["RegionName"] for r in response["Regions"]]
        logger.info(f"Discovered {len(regions)} enabled regions.")
        return sorted(regions)
    except Exception as e:
        logger.warning(f"Could not auto-discover regions, falling back to default: {e}")
        return [config.AWS_DEFAULT_REGION]


def _scan_region(region: str) -> dict:
    """
    Scan a single region using all three discovery sources.
    Returns dict: {resource_type: {arns: [...], namespace: str}}
    """
    result = {}

    # Source 1: Resource Groups Tagging API
    try:
        tagging = aws_client.get_client("resourcegroupstaggingapi", region)
        paginator = tagging.get_paginator("get_resources")
        for page in paginator.paginate(ResourcesPerPage=100):
            for resource in page.get("ResourceTagMappingList", []):
                arn = resource.get("ResourceARN", "")
                resource_type = _parse_resource_type(arn)
                if resource_type:
                    if resource_type not in result:
                        result[resource_type] = {
                            "arns": [],
                            "namespace": RESOURCE_TYPE_TO_NAMESPACE.get(resource_type),
                            "regions": set(),
                        }
                    result[resource_type]["arns"].append(arn)
                    result[resource_type]["regions"].add(region)
    except Exception as e:
        logger.debug(f"Tagging API scan failed in {region}: {e}")

    # Source 2: CloudWatch list_metrics (discovers active namespaces)
    try:
        cw = aws_client.get_client("cloudwatch", region)
        seen_namespaces = set()
        paginator = cw.get_paginator("list_metrics")
        for page in paginator.paginate():
            for metric in page.get("Metrics", []):
                ns = metric.get("Namespace", "")
                if ns.startswith("AWS/") and ns not in seen_namespaces:
                    seen_namespaces.add(ns)
                    # Synthesise a resource type entry for namespace-level tracking
                    ns_key = f"__namespace__{ns}"
                    if ns_key not in result:
                        result[ns_key] = {"arns": [], "namespace": ns, "regions": set()}
                    result[ns_key]["regions"].add(region)
    except Exception as e:
        logger.debug(f"CloudWatch list_metrics failed in {region}: {e}")

    # Source 3: AWS Config (if enabled)
    try:
        cfg = aws_client.get_client("config", region)
        paginator = cfg.get_paginator("list_discovered_resources")
        for resource_type in list(result.keys()):
            if resource_type.startswith("__namespace__"):
                continue
            try:
                for page in paginator.paginate(resourceType=resource_type):
                    for res in page.get("resourceIdentifiers", []):
                        arn = res.get("resourceId", "")
                        if arn and arn not in result.get(resource_type, {}).get("arns", []):
                            result.setdefault(resource_type, {"arns": [], "namespace": None, "regions": set()})
                            result[resource_type]["arns"].append(arn)
                            result[resource_type]["regions"].add(region)
            except Exception:
                pass
    except Exception as e:
        logger.debug(f"AWS Config not available in {region}: {e}")

    # Source 4: Direct enumeration for core services (always catches untagged resources)
    _direct_enumerate(region, result)

    # Source 5: Cloud Control API sweep — dynamically enumerates every AWS
    # resource type not already covered by sources 1-4.  Controlled by
    # config.CLOUDCONTROL_SWEEP so it can be disabled for faster local runs.
    if getattr(config, "CLOUDCONTROL_SWEEP", True):
        _cloud_control_sweep(region, result)

    return region, result


def _direct_enumerate(region: str, result: dict) -> None:
    """
    Directly enumerate core services via their native Describe APIs.
    Merges discovered ARNs into result without overwriting Tagging API results.
    This guarantees untagged resources are always discovered.
    """
    def _add_arn(resource_type: str, arn: str) -> None:
        existing = result.setdefault(resource_type, {
            "arns": [],
            "namespace": RESOURCE_TYPE_TO_NAMESPACE.get(resource_type),
            "regions": set(),
        })
        if arn and arn not in existing["arns"]:
            existing["arns"].append(arn)
            existing.setdefault("regions", set()).add(region)

    # EC2 instances
    try:
        ec2 = aws_client.get_client("ec2", region)
        paginator = ec2.get_paginator("describe_instances")
        for page in paginator.paginate(Filters=[{"Name": "instance-state-name", "Values": ["running", "stopped"]}]):
            for reservation in page.get("Reservations", []):
                for inst in reservation.get("Instances", []):
                    iid = inst.get("InstanceId", "")
                    # Build ARN from instance ID (account ID embedded in reservation owner)
                    owner = reservation.get("OwnerId", "")
                    arn = f"arn:aws:ec2:{region}:{owner}:instance/{iid}"
                    _add_arn("AWS::EC2::Instance", arn)
    except Exception as e:
        logger.debug(f"Direct EC2 enumeration failed in {region}: {e}")

    # RDS instances
    try:
        rds = aws_client.get_client("rds", region)
        paginator = rds.get_paginator("describe_db_instances")
        for page in paginator.paginate():
            for db in page.get("DBInstances", []):
                arn = db.get("DBInstanceArn", "")
                _add_arn("AWS::RDS::DBInstance", arn)
    except Exception as e:
        logger.debug(f"Direct RDS enumeration failed in {region}: {e}")

    # Lambda functions
    try:
        lmb = aws_client.get_client("lambda", region)
        paginator = lmb.get_paginator("list_functions")
        for page in paginator.paginate():
            for fn in page.get("Functions", []):
                arn = fn.get("FunctionArn", "")
                _add_arn("AWS::Lambda::Function", arn)
    except Exception as e:
        logger.debug(f"Direct Lambda enumeration failed in {region}: {e}")

    # ElastiCache clusters
    try:
        ec = aws_client.get_client("elasticache", region)
        paginator = ec.get_paginator("describe_cache_clusters")
        for page in paginator.paginate():
            for cluster in page.get("CacheClusters", []):
                arn = cluster.get("ARN", "")
                _add_arn("AWS::ElastiCache::CacheCluster", arn)
    except Exception as e:
        logger.debug(f"Direct ElastiCache enumeration failed in {region}: {e}")

    # Redshift clusters
    try:
        rs = aws_client.get_client("redshift", region)
        paginator = rs.get_paginator("describe_clusters")
        for page in paginator.paginate():
            for cluster in page.get("Clusters", []):
                arn = cluster.get("ClusterNamespaceArn", "")
                if not arn:
                    cid = cluster.get("ClusterIdentifier", "")
                    arn = f"arn:aws:redshift:{region}::cluster:{cid}"
                _add_arn("AWS::Redshift::Cluster", arn)
    except Exception as e:
        logger.debug(f"Direct Redshift enumeration failed in {region}: {e}")

    # ── Networking ────────────────────────────────────────────────────────

    # VPCs (all — including default so audit report is complete)
    try:
        ec2 = aws_client.get_client("ec2", region)
        for page in ec2.get_paginator("describe_vpcs").paginate():
            for vpc in page.get("Vpcs", []):
                vid  = vpc["VpcId"]
                oid  = vpc.get("OwnerId", "")
                arn  = f"arn:aws:ec2:{region}:{oid}:vpc/{vid}"
                _add_arn("AWS::EC2::VPC", arn)
    except Exception as e:
        logger.debug(f"Direct VPC enumeration failed in {region}: {e}")

    # Subnets
    try:
        ec2 = aws_client.get_client("ec2", region)
        for page in ec2.get_paginator("describe_subnets").paginate():
            for sn in page.get("Subnets", []):
                sid = sn["SubnetId"]
                oid = sn.get("OwnerId", "")
                arn = sn.get("SubnetArn", f"arn:aws:ec2:{region}:{oid}:subnet/{sid}")
                _add_arn("AWS::EC2::Subnet", arn)
    except Exception as e:
        logger.debug(f"Direct Subnet enumeration failed in {region}: {e}")

    # Internet Gateways
    try:
        ec2 = aws_client.get_client("ec2", region)
        for page in ec2.get_paginator("describe_internet_gateways").paginate():
            for igw in page.get("InternetGateways", []):
                igw_id = igw["InternetGatewayId"]
                oid    = igw.get("OwnerId", "")
                arn    = f"arn:aws:ec2:{region}:{oid}:internet-gateway/{igw_id}"
                _add_arn("AWS::EC2::InternetGateway", arn)
    except Exception as e:
        logger.debug(f"Direct IGW enumeration failed in {region}: {e}")

    # NAT Gateways (available + pending only)
    try:
        ec2 = aws_client.get_client("ec2", region)
        for page in ec2.get_paginator("describe_nat_gateways").paginate(
            Filter=[{"Name": "state", "Values": ["available", "pending"]}]
        ):
            for nat in page.get("NatGateways", []):
                nat_id = nat["NatGatewayId"]
                arn    = f"arn:aws:ec2:{region}::natgateway/{nat_id}"
                _add_arn("AWS::EC2::NatGateway", arn)
    except Exception as e:
        logger.debug(f"Direct NAT Gateway enumeration failed in {region}: {e}")

    # Security Groups (skip default to reduce noise, but include all for completeness)
    try:
        ec2 = aws_client.get_client("ec2", region)
        for page in ec2.get_paginator("describe_security_groups").paginate():
            for sg in page.get("SecurityGroups", []):
                sg_id = sg["GroupId"]
                oid   = sg.get("OwnerId", "")
                arn   = f"arn:aws:ec2:{region}:{oid}:security-group/{sg_id}"
                _add_arn("AWS::EC2::SecurityGroup", arn)
    except Exception as e:
        logger.debug(f"Direct Security Group enumeration failed in {region}: {e}")

    # Elastic IP Addresses (all allocated)
    try:
        ec2   = aws_client.get_client("ec2", region)
        addrs = ec2.describe_addresses().get("Addresses", [])
        for addr in addrs:
            alloc_id = addr.get("AllocationId", "")
            if not alloc_id:
                continue
            oid = addr.get("NetworkInterfaceOwnerId", "")
            arn = f"arn:aws:ec2:{region}:{oid}:elastic-ip/{alloc_id}"
            _add_arn("AWS::EC2::EIP", arn)
    except Exception as e:
        logger.debug(f"Direct EIP enumeration failed in {region}: {e}")

    # ── Storage ───────────────────────────────────────────────────────────

    # EBS Volumes
    try:
        ec2 = aws_client.get_client("ec2", region)
        for page in ec2.get_paginator("describe_volumes").paginate():
            for vol in page.get("Volumes", []):
                vol_id = vol["VolumeId"]
                oid    = vol.get("OwnerId", "")
                arn    = f"arn:aws:ec2:{region}:{oid}:volume/{vol_id}"
                _add_arn("AWS::EC2::Volume", arn)
    except Exception as e:
        logger.debug(f"Direct EBS Volume enumeration failed in {region}: {e}")

    # EBS Snapshots (account-owned, completed only)
    try:
        ec2 = aws_client.get_client("ec2", region)
        for page in ec2.get_paginator("describe_snapshots").paginate(
            OwnerIds=["self"],
            Filters=[{"Name": "status", "Values": ["completed"]}],
        ):
            for snap in page.get("Snapshots", []):
                snap_id = snap["SnapshotId"]
                oid     = snap.get("OwnerId", "")
                arn     = f"arn:aws:ec2:{region}:{oid}:snapshot/{snap_id}"
                _add_arn("AWS::EC2::Snapshot", arn)
    except Exception as e:
        logger.debug(f"Direct EBS Snapshot enumeration failed in {region}: {e}")

    # ── Load Balancers (ALB / NLB via ELBv2) ─────────────────────────────
    try:
        elbv2 = aws_client.get_client("elbv2", region)
        for page in elbv2.get_paginator("describe_load_balancers").paginate():
            for lb in page.get("LoadBalancers", []):
                arn = lb.get("LoadBalancerArn", "")
                if arn:
                    _add_arn("AWS::ElasticLoadBalancingV2::LoadBalancer", arn)
    except Exception as e:
        logger.debug(f"Direct ELBv2 enumeration failed in {region}: {e}")

    # ── Messaging ─────────────────────────────────────────────────────────

    # SNS Topics
    try:
        sns = aws_client.get_client("sns", region)
        for page in sns.get_paginator("list_topics").paginate():
            for topic in page.get("Topics", []):
                arn = topic.get("TopicArn", "")
                if arn:
                    _add_arn("AWS::SNS::Topic", arn)
    except Exception as e:
        logger.debug(f"Direct SNS enumeration failed in {region}: {e}")

    # Secrets Manager Secrets
    try:
        sm = aws_client.get_client("secretsmanager", region)
        for page in sm.get_paginator("list_secrets").paginate():
            for secret in page.get("SecretList", []):
                arn = secret.get("ARN", "")
                if arn:
                    _add_arn("AWS::SecretsManager::Secret", arn)
    except Exception as e:
        logger.debug(f"Direct Secrets Manager enumeration failed in {region}: {e}")

    # ── Containers ────────────────────────────────────────────────────────

    # ECR Repositories
    try:
        ecr = aws_client.get_client("ecr", region)
        for page in ecr.get_paginator("describe_repositories").paginate():
            for repo in page.get("repositories", []):
                arn = repo.get("repositoryArn", "")
                if arn:
                    _add_arn("AWS::ECR::Repository", arn)
    except Exception as e:
        logger.debug(f"Direct ECR enumeration failed in {region}: {e}")

    # ── Security / Compliance ─────────────────────────────────────────────

    # ACM Certificates (issued + pending validation)
    try:
        acm = aws_client.get_client("acm", region)
        for page in acm.get_paginator("list_certificates").paginate(
            CertificateStatuses=["ISSUED", "PENDING_VALIDATION"]
        ):
            for cert in page.get("CertificateSummaryList", []):
                arn = cert.get("CertificateArn", "")
                if arn:
                    _add_arn("AWS::ACM::Certificate", arn)
    except Exception as e:
        logger.debug(f"Direct ACM enumeration failed in {region}: {e}")

    # ── Compute Scaling ───────────────────────────────────────────────────

    # Auto Scaling Groups
    try:
        asg_client = aws_client.get_client("autoscaling", region)
        for page in asg_client.get_paginator("describe_auto_scaling_groups").paginate():
            for asg in page.get("AutoScalingGroups", []):
                arn = asg.get("AutoScalingGroupARN", "")
                if arn:
                    _add_arn("AWS::AutoScaling::AutoScalingGroup", arn)
    except Exception as e:
        logger.debug(f"Direct ASG enumeration failed in {region}: {e}")

    # ── Systems Manager (self-owned only) ────────────────────────────────
    # SSM Documents — filter Owner=Self so AWS-managed docs (~thousands) are excluded
    try:
        ssm = aws_client.get_client("ssm", region)
        for page in ssm.get_paginator("list_documents").paginate(
            Filters=[{"Key": "Owner", "Values": ["Self"]}]
        ):
            for doc in page.get("DocumentIdentifiers", []):
                name = doc.get("Name", "")
                oid  = doc.get("Owner", "")
                if name and oid:
                    arn = f"arn:aws:ssm:{region}:{oid}:document/{name}"
                    _add_arn("AWS::SSM::Document", arn)
    except Exception as e:
        logger.debug(f"Direct SSM Document enumeration failed in {region}: {e}")

    # SSM Patch Baselines — filter Owner=Self to skip AWS-managed baselines
    try:
        ssm = aws_client.get_client("ssm", region)
        for page in ssm.get_paginator("describe_patch_baselines").paginate(
            Filters=[{"Key": "OWNER", "Values": ["Self"]}]
        ):
            for baseline in page.get("BaselineIdentities", []):
                bid = baseline.get("BaselineId", "")
                if bid:
                    arn = f"arn:aws:ssm:{region}::patchbaseline/{bid}"
                    _add_arn("AWS::SSM::PatchBaseline", arn)
    except Exception as e:
        logger.debug(f"Direct SSM Patch Baseline enumeration failed in {region}: {e}")

    # ── Developer Tools / Workflow ──────────────────────────────────
    # CloudTrail trails (no shadow trails — avoids duplicate multi-region entries)
    try:
        ct     = aws_client.get_client("cloudtrail", region)
        trails = ct.describe_trails(includeShadowTrails=False).get("trailList", [])
        for trail in trails:
            arn = trail.get("TrailARN", "")
            if arn:
                _add_arn("AWS::CloudTrail::Trail", arn)
    except Exception as e:
        logger.debug(f"Direct CloudTrail enumeration failed in {region}: {e}")

    # API Gateway REST APIs
    try:
        agw = aws_client.get_client("apigateway", region)
        for page in agw.get_paginator("get_rest_apis").paginate():
            for api in page.get("items", []):
                api_id = api.get("id", "")
                if api_id:
                    arn = f"arn:aws:apigateway:{region}::/restapis/{api_id}"
                    _add_arn("AWS::ApiGateway::RestApi", arn)
    except Exception as e:
        logger.debug(f"Direct API Gateway enumeration failed in {region}: {e}")

    # Kinesis Data Streams
    try:
        ks = aws_client.get_client("kinesis", region)
        for page in ks.get_paginator("list_streams").paginate():
            for stream_summary in page.get("StreamSummaries", []):
                arn = stream_summary.get("StreamARN", "")
                if arn:
                    _add_arn("AWS::Kinesis::Stream", arn)
    except Exception as e:
        logger.debug(f"Direct Kinesis enumeration failed in {region}: {e}")

    # EventBridge / CloudWatch Events rules
    try:
        events = aws_client.get_client("events", region)
        for page in events.get_paginator("list_rules").paginate():
            for rule in page.get("Rules", []):
                arn = rule.get("Arn", "")
                if arn:
                    _add_arn("AWS::Events::Rule", arn)
    except Exception as e:
        logger.debug(f"Direct EventBridge enumeration failed in {region}: {e}")

    # Step Functions state machines
    try:
        sfn = aws_client.get_client("stepfunctions", region)
        for page in sfn.get_paginator("list_state_machines").paginate():
            for sm in page.get("stateMachines", []):
                arn = sm.get("stateMachineArn", "")
                if arn:
                    _add_arn("AWS::StepFunctions::StateMachine", arn)
    except Exception as e:
        logger.debug(f"Direct Step Functions enumeration failed in {region}: {e}")


def _cloud_control_sweep(region: str, result: dict) -> None:
    """
    Source 5 — Cloud Control API targeted sweep.

    Enumerates 34 curated resource types that are:
      - Genuinely user-created resources with real audit value
      - NOT covered by direct enumeration (sources 1-4)
      - Reliably supported by Cloud Control

    Using a fixed curated allowlist instead of dynamically fetching all 400+
    supported types avoids AWS-managed noise (SSM Documents, Patch Baselines,
    XRay defaults, Scheduler default groups) and reduces API calls by >95%,
    cutting the CC sweep from ~30 minutes to ~30 seconds.

    Concurrency is 10 workers; each type call is independently fault-tolerant.
    """
    # Skip types already discovered by sources 1-4
    already_present = frozenset(k for k in result if not k.startswith("__"))
    candidates = [t for t in _CC_SWEEP_TYPES if t not in already_present]

    if not candidates:
        return

    cc = aws_client.get_client("cloudcontrol", region)

    def _sweep_one(type_name: str) -> tuple:
        """List all resources of type_name via Cloud Control.  Returns (type_name, [arns])."""
        arns: list = []
        try:
            kwargs: dict = {"TypeName": type_name}
            while True:
                resp        = cc.list_resources(**kwargs)
                for desc in resp.get("ResourceDescriptions", []):
                    identifier = desc.get("Identifier", "")
                    if not identifier:
                        continue
                    # Use the raw ARN when available; otherwise synthesise a
                    # deterministic ARN so resource_collector can call get_resource.
                    if identifier.startswith("arn:"):
                        arn = identifier
                    else:
                        svc = type_name.split("::")[1].lower()
                        arn = (
                            f"arn:aws:{svc}:{region}::"
                            f"{type_name.replace('::', '/').lower()}/{identifier}"
                        )
                    arns.append(arn)
                next_token = resp.get("NextToken", "")
                if not next_token:
                    break
                kwargs["NextToken"] = next_token
        except Exception as exc:
            err = str(exc).lower()
            # Silently skip types not supported in this region / account
            if not any(s in err for s in ("unsupported", "not found", "not supported",
                                          "typeconfiguration", "does not support")):
                logger.debug(f"CC sweep [{region}] {type_name}: {exc}")
        return type_name, arns

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_sweep_one, t): t for t in candidates}
        for future in as_completed(futures):
            type_name, arns = future.result()
            if not arns:
                continue
            entry = result.setdefault(type_name, {
                "arns":      [],
                "namespace": RESOURCE_TYPE_TO_NAMESPACE.get(type_name),
                "regions":   set(),
            })
            existing_set = set(entry["arns"])
            new_arns     = [a for a in arns if a not in existing_set]
            entry["arns"].extend(new_arns)
            entry.setdefault("regions", set()).add(region)
            if new_arns:
                logger.info(
                    f"  [{region}] CC sweep found {len(new_arns)} {type_name} resource(s)"
                )


def _discover_global_services() -> dict:
    """
    Collect global resources (IAM, S3, CloudFront, Route53).
    Called once - not per region.
    """
    global_map = {}

    # IAM users and roles
    try:
        iam = aws_client.get_client("iam", "us-east-1")
        users_resp = iam.list_users()
        user_arns = [u["Arn"] for u in users_resp.get("Users", [])]
        if user_arns:
            global_map["AWS::IAM::User"] = {
                "arns": user_arns,
                "namespace": None,
                "regions": {"global"},
            }
        logger.info(f"IAM: found {len(user_arns)} users.")
    except Exception as e:
        logger.warning(f"IAM discovery failed: {e}")

    # S3 buckets (global namespace, but each has a home region)
    try:
        s3 = aws_client.get_client("s3", "us-east-1")
        buckets = s3.list_buckets().get("Buckets", [])
        bucket_arns = [f"arn:aws:s3:::{b['Name']}" for b in buckets]
        if bucket_arns:
            global_map["AWS::S3::Bucket"] = {
                "arns": bucket_arns,
                "namespace": "AWS/S3",
                "regions": {"global"},
            }
        logger.info(f"S3: found {len(bucket_arns)} buckets.")
    except Exception as e:
        logger.warning(f"S3 discovery failed: {e}")

    # CloudFront distributions
    try:
        cf = aws_client.get_client("cloudfront", "us-east-1")
        dists = cf.list_distributions().get("DistributionList", {}).get("Items", [])
        cf_arns = [d.get("ARN", d.get("Id", "")) for d in dists]
        if cf_arns:
            global_map["AWS::CloudFront::Distribution"] = {
                "arns": cf_arns,
                "namespace": "AWS/CloudFront",
                "regions": {"global"},
            }
        logger.info(f"CloudFront: found {len(cf_arns)} distributions.")
    except Exception as e:
        logger.debug(f"CloudFront discovery failed: {e}")

    # Route53 hosted zones
    try:
        r53 = aws_client.get_client("route53", "us-east-1")
        zones = r53.list_hosted_zones().get("HostedZones", [])
        zone_arns = [z["Id"] for z in zones]
        if zone_arns:
            global_map["AWS::Route53::HostedZone"] = {
                "arns": zone_arns,
                "namespace": None,
                "regions": {"global"},
            }
        logger.info(f"Route53: found {len(zone_arns)} hosted zones.")
    except Exception as e:
        logger.debug(f"Route53 discovery failed: {e}")

    return global_map


def _parse_resource_type(arn: str) -> str:
    """
    Extract a resource type string from an ARN.
    e.g. arn:aws:ec2:us-east-1:123:instance/i-abc -> AWS::EC2::Instance
    Returns None for unrecognised ARNs.
    """
    # Check against known resource type patterns in RESOURCE_TYPE_TO_NAMESPACE
    arn_lower = arn.lower()
    if ":instance/" in arn_lower and ":ec2:" in arn_lower:
        return "AWS::EC2::Instance"
    if ":db:" in arn_lower and ":rds:" in arn_lower:
        return "AWS::RDS::DBInstance"
    if ":cluster:" in arn_lower and ":rds:" in arn_lower:
        return "AWS::RDS::DBCluster"
    if ":function:" in arn_lower and ":lambda:" in arn_lower:
        return "AWS::Lambda::Function"
    if arn_lower.startswith("arn:aws:s3:::"):
        return "AWS::S3::Bucket"
    if ":table/" in arn_lower and ":dynamodb:" in arn_lower:
        return "AWS::DynamoDB::Table"
    if ":service/" in arn_lower and ":ecs:" in arn_lower:
        return "AWS::ECS::Service"
    if ":cluster/" in arn_lower and ":ecs:" in arn_lower:
        return "AWS::ECS::Cluster"
    if ":cluster/" in arn_lower and ":eks:" in arn_lower:
        return "AWS::EKS::Cluster"
    if ":replicationgroup:" in arn_lower and ":elasticache:" in arn_lower:
        return "AWS::ElastiCache::ReplicationGroup"
    if ":cluster:" in arn_lower and ":elasticache:" in arn_lower:
        return "AWS::ElastiCache::CacheCluster"
    if ":cluster:" in arn_lower and ":redshift:" in arn_lower:
        return "AWS::Redshift::Cluster"
    if ":stream/" in arn_lower and ":kinesis:" in arn_lower:
        return "AWS::Kinesis::Stream"
    if ":sqs:" in arn_lower:
        return "AWS::SQS::Queue"
    if ":sns:" in arn_lower:
        return "AWS::SNS::Topic"
    if "loadbalancer" in arn_lower and ":elasticloadbalancingv2:" in arn_lower:
        return "AWS::ElasticLoadBalancingV2::LoadBalancer"
    if ":restapi/" in arn_lower and ":apigateway:" in arn_lower:
        return "AWS::ApiGateway::RestApi"
    if ":stateMachine:" in arn and ":states:" in arn_lower:
        return "AWS::StepFunctions::StateMachine"
    if ":distribution/" in arn_lower and ":cloudfront:" in arn_lower:
        return "AWS::CloudFront::Distribution"
    if ":secret:" in arn_lower and ":secretsmanager:" in arn_lower:
        return "AWS::SecretsManager::Secret"
    # Networking resources
    if ":vpc/" in arn_lower and ":ec2:" in arn_lower:
        return "AWS::EC2::VPC"
    if ":subnet/" in arn_lower and ":ec2:" in arn_lower:
        return "AWS::EC2::Subnet"
    if ":internet-gateway/" in arn_lower and ":ec2:" in arn_lower:
        return "AWS::EC2::InternetGateway"
    if ":natgateway/" in arn_lower and ":ec2:" in arn_lower:
        return "AWS::EC2::NatGateway"
    if ":security-group/" in arn_lower and ":ec2:" in arn_lower:
        return "AWS::EC2::SecurityGroup"
    if ":route-table/" in arn_lower and ":ec2:" in arn_lower:
        return "AWS::EC2::RouteTable"
    if ":elastic-ip/" in arn_lower and ":ec2:" in arn_lower:
        return "AWS::EC2::EIP"
    # Storage resources
    if ":volume/" in arn_lower and ":ec2:" in arn_lower:
        return "AWS::EC2::Volume"
    if ":snapshot/" in arn_lower and ":ec2:" in arn_lower:
        return "AWS::EC2::Snapshot"
    # Containers / Security / Messaging / Scaling
    if ":repository/" in arn_lower and ":ecr:" in arn_lower:
        return "AWS::ECR::Repository"
    if ":certificate/" in arn_lower and ":acm:" in arn_lower:
        return "AWS::ACM::Certificate"
    if ":autoscalinggroup:" in arn_lower and ":autoscaling:" in arn_lower:
        return "AWS::AutoScaling::AutoScalingGroup"
    # Systems Manager
    if ":document/" in arn_lower and ":ssm:" in arn_lower:
        return "AWS::SSM::Document"
    if ":patchbaseline/" in arn_lower and ":ssm:" in arn_lower:
        return "AWS::SSM::PatchBaseline"
    # CloudTrail
    if ":trail/" in arn_lower and ":cloudtrail:" in arn_lower:
        return "AWS::CloudTrail::Trail"
    # API Gateway — ARNs are arn:aws:apigateway:{region}::/restapis/{id}
    if "/restapis/" in arn_lower and ":apigateway:" in arn_lower:
        return "AWS::ApiGateway::RestApi"
    # EventBridge / CloudWatch Events
    if ":rule/" in arn_lower and ":events:" in arn_lower:
        return "AWS::Events::Rule"
    return None


def _merge_maps(regional_results: list, global_map: dict) -> dict:
    """Merge per-region discovery results and global results into one service map."""
    merged = {}
    for _region, region_map in regional_results:
        for resource_type, data in region_map.items():
            if resource_type not in merged:
                merged[resource_type] = {
                    "arns": [],
                    "namespace": data.get("namespace"),
                    "regions": set(),
                }
            # Deduplicate ARNs
            existing_arns = set(merged[resource_type]["arns"])
            for arn in data.get("arns", []):
                if arn not in existing_arns:
                    merged[resource_type]["arns"].append(arn)
                    existing_arns.add(arn)
            merged[resource_type]["regions"].update(data.get("regions", set()))

    for resource_type, data in global_map.items():
        if resource_type not in merged:
            merged[resource_type] = {"arns": [], "namespace": data.get("namespace"), "regions": set()}
        existing_arns = set(merged[resource_type]["arns"])
        for arn in data.get("arns", []):
            if arn not in existing_arns:
                merged[resource_type]["arns"].append(arn)
                existing_arns.add(arn)
        merged[resource_type]["regions"].update(data.get("regions", set()))

    # Convert region sets to sorted lists for JSON-serializability
    for rt in merged:
        merged[rt]["regions"] = sorted(merged[rt]["regions"])

    return merged


def run(region_override: list = None) -> tuple:
    """
    Main entry point.  Discovers regions then scans all of them in parallel.
    Returns (service_map, regions) where service_map is the canonical dict.
    """
    regions = _discover_regions(region_override)
    logger.info(f"Scanning {len(regions)} regions in parallel...")

    regional_results = []
    with ThreadPoolExecutor(max_workers=min(len(regions), 20)) as executor:
        futures = {executor.submit(_scan_region, region): region for region in regions}
        for future in as_completed(futures):
            region = futures[future]
            try:
                result = future.result()
                regional_results.append(result)
                logger.info(f"  [{region}] discovery complete.")
            except Exception as e:
                logger.warning(f"  [{region}] discovery failed: {e}")
                regional_results.append((region, {}))

    logger.info("Collecting global resources (IAM, S3, CloudFront, Route53)...")
    global_map = _discover_global_services()

    service_map = _merge_maps(regional_results, global_map)

    # Log summary
    real_services = {k: v for k, v in service_map.items() if not k.startswith("__namespace__")}
    logger.info(f"Discovery complete: {len(real_services)} resource types, "
                f"{sum(len(v['arns']) for v in real_services.values())} total resources.")

    return service_map, regions
