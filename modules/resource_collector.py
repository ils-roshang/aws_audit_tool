"""
modules/resource_collector.py
-------------------------------
Phase 3: Collects detailed metadata for every resource discovered
by service_discovery.py.  Uses an ARN-driven dispatcher - each ARN
is parsed, the service type is identified, and the appropriate
describe_* AWS API is called.

Unknown resource types are stored as generic records so nothing is
silently dropped.

Returns a flat unified resource list:
  [{ arn, service, type, region, name, tags, metadata }]
"""

import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import modules.aws_client as aws_client

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────────
# Cloud Control fallback helpers
# ────────────────────────────────────────────────────────────────────────────

# Mutable set of resource types known to not work with Cloud Control get_resource.
# Pre-seeded with types handled by global-service collectors or that reliably fail.
# Populated lazily at runtime when a get_resource call returns an unsupported-type
# error — subsequent calls for that type skip Cloud Control entirely.
_CLOUDCONTROL_UNSUPPORTED: set = {
    "AWS::S3::Bucket",              # dedicated collector
    "AWS::IAM::User",               # global collector
    "AWS::IAM::Role",               # global collector
    "AWS::CloudFront::Distribution",# global collector
    "AWS::Route53::HostedZone",     # global collector
    "AWS::CloudFormation::Stack",   # meta, not a billable resource
}


# ────────────────────────────────────────────────────────────────────────────
# Per-service collector functions
# Each takes (arn, region) and returns a metadata dict or None on failure.
# ────────────────────────────────────────────────────────────────────────────

def _collect_ec2(arn: str, region: str) -> dict:
    instance_id = arn.split("/")[-1]
    try:
        ec2 = aws_client.get_client("ec2", region)
        resp = ec2.describe_instances(InstanceIds=[instance_id])
        reservations = resp.get("Reservations", [])
        if not reservations:
            return None
        inst = reservations[0]["Instances"][0]
        name = next((t["Value"] for t in inst.get("Tags", []) if t["Key"] == "Name"), instance_id)
        return {
            "name": name,
            "tags": {t["Key"]: t["Value"] for t in inst.get("Tags", [])},
            "metadata": {
                "instance_type":    inst.get("InstanceType"),
                "state":            inst.get("State", {}).get("Name"),
                "az":               inst.get("Placement", {}).get("AvailabilityZone"),
                "launch_time":      str(inst.get("LaunchTime", "")),
                "public_ip":        inst.get("PublicIpAddress", ""),
                "key_name":         inst.get("KeyName", ""),
                "vpc_id":           inst.get("VpcId", ""),
                "security_groups":  [sg["GroupId"] for sg in inst.get("SecurityGroups", [])],
            },
        }
    except Exception as e:
        logger.debug(f"EC2 collect failed {arn}: {e}")
        return None


def _collect_rds(arn: str, region: str) -> dict:
    db_id = arn.split(":")[-1]
    try:
        rds = aws_client.get_client("rds", region)
        resp = rds.describe_db_instances(DBInstanceIdentifier=db_id)
        instances = resp.get("DBInstances", [])
        if not instances:
            return None
        inst = instances[0]
        return {
            "name": inst.get("DBInstanceIdentifier", db_id),
            "tags": {},
            "metadata": {
                "engine":                   inst.get("Engine"),
                "engine_version":           inst.get("EngineVersion"),
                "instance_class":           inst.get("DBInstanceClass"),
                "status":                   inst.get("DBInstanceStatus"),
                "multi_az":                 inst.get("MultiAZ", False),
                "storage_type":             inst.get("StorageType"),
                "allocated_storage_gb":     inst.get("AllocatedStorage"),
                "publicly_accessible":      inst.get("PubliclyAccessible", False),
                "storage_encrypted":        inst.get("StorageEncrypted", False),
                "performance_insights":     inst.get("PerformanceInsightsEnabled", False),
                "auto_minor_version_upgrade": inst.get("AutoMinorVersionUpgrade", True),
                "backup_retention_period":  inst.get("BackupRetentionPeriod", 0),
                "endpoint":                 inst.get("Endpoint", {}).get("Address", ""),
            },
        }
    except Exception as e:
        logger.debug(f"RDS collect failed {arn}: {e}")
        return None


def _collect_lambda(arn: str, region: str) -> dict:
    func_name = arn.split(":")[-1]
    try:
        lmb = aws_client.get_client("lambda", region)
        resp = lmb.get_function(FunctionName=func_name)
        cfg = resp.get("Configuration", {})
        return {
            "name": cfg.get("FunctionName", func_name),
            "tags": resp.get("Tags", {}),
            "metadata": {
                "runtime":          cfg.get("Runtime"),
                "memory_mb":        cfg.get("MemorySize"),
                "timeout_sec":      cfg.get("Timeout"),
                "handler":          cfg.get("Handler"),
                "code_size_bytes":  cfg.get("CodeSize"),
                "last_modified":    cfg.get("LastModified", ""),
                "role":             cfg.get("Role", ""),
                "architectures":    cfg.get("Architectures", []),
            },
        }
    except Exception as e:
        logger.debug(f"Lambda collect failed {arn}: {e}")
        return None


def _collect_s3(arn: str, region: str) -> dict:
    bucket_name = arn.replace("arn:aws:s3:::", "")
    try:
        s3 = aws_client.get_client("s3", "us-east-1")
        # Get location
        try:
            loc_resp = s3.get_bucket_location(Bucket=bucket_name)
            bucket_region = loc_resp.get("LocationConstraint") or "us-east-1"
        except Exception:
            bucket_region = "unknown"
        # Get encryption status
        try:
            enc_resp = s3.get_bucket_encryption(Bucket=bucket_name)
            encryption = enc_resp.get("ServerSideEncryptionConfiguration", {}).get("Rules", [{}])[0]
            enc_type = encryption.get("ApplyServerSideEncryptionByDefault", {}).get("SSEAlgorithm", "none")
        except Exception:
            enc_type = "none"
        # Get public access block
        try:
            pab = s3.get_public_access_block(Bucket=bucket_name)
            pab_cfg = pab.get("PublicAccessBlockConfiguration", {})
            fully_blocked = all(pab_cfg.get(k, False) for k in
                                ["BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets"])
        except Exception:
            fully_blocked = False
        # Get versioning
        try:
            ver_resp = s3.get_bucket_versioning(Bucket=bucket_name)
            versioning = ver_resp.get("Status", "Disabled")
        except Exception:
            versioning = "Unknown"
        # Get tags
        try:
            tag_resp = s3.get_bucket_tagging(Bucket=bucket_name)
            tags = {t["Key"]: t["Value"] for t in tag_resp.get("TagSet", [])}
        except Exception:
            tags = {}

        return {
            "name": bucket_name,
            "tags": tags,
            "metadata": {
                "bucket_region":    bucket_region,
                "encryption":       enc_type,
                "public_access_blocked": fully_blocked,
                "versioning":       versioning,
            },
        }
    except Exception as e:
        logger.debug(f"S3 collect failed {arn}: {e}")
        return None


def _collect_dynamodb(arn: str, region: str) -> dict:
    table_name = arn.split("/")[-1]
    try:
        ddb = aws_client.get_client("dynamodb", region)
        resp = ddb.describe_table(TableName=table_name)
        tbl = resp.get("Table", {})
        pitr = False
        try:
            pitr_resp = ddb.describe_continuous_backups(TableName=table_name)
            pitr_status = pitr_resp.get("ContinuousBackupsDescription", {}).get(
                "PointInTimeRecoveryDescription", {}).get("PointInTimeRecoveryStatus", "DISABLED")
            pitr = pitr_status == "ENABLED"
        except Exception:
            pass
        return {
            "name": table_name,
            "tags": {},
            "metadata": {
                "status":           tbl.get("TableStatus"),
                "billing_mode":     tbl.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED"),
                "item_count":       tbl.get("ItemCount", 0),
                "size_bytes":       tbl.get("TableSizeBytes", 0),
                "rcu_provisioned":  tbl.get("ProvisionedThroughput", {}).get("ReadCapacityUnits", 0),
                "wcu_provisioned":  tbl.get("ProvisionedThroughput", {}).get("WriteCapacityUnits", 0),
                "encryption_type":  tbl.get("SSEDescription", {}).get("Status", "DISABLED"),
                "pitr_enabled":     pitr,
            },
        }
    except Exception as e:
        logger.debug(f"DynamoDB collect failed {arn}: {e}")
        return None


def _collect_elasticache(arn: str, region: str) -> dict:
    cluster_id = arn.split(":")[-1]
    try:
        ec = aws_client.get_client("elasticache", region)
        resp = ec.describe_cache_clusters(CacheClusterId=cluster_id, ShowCacheNodeInfo=True)
        clusters = resp.get("CacheClusters", [])
        if not clusters:
            return None
        cluster = clusters[0]
        return {
            "name": cluster_id,
            "tags": {},
            "metadata": {
                "engine":           cluster.get("Engine"),
                "engine_version":   cluster.get("EngineVersion"),
                "node_type":        cluster.get("CacheNodeType"),
                "status":           cluster.get("CacheClusterStatus"),
                "num_nodes":        cluster.get("NumCacheNodes"),
                "encryption_at_rest": cluster.get("AtRestEncryptionEnabled", False),
                "encryption_transit": cluster.get("TransitEncryptionEnabled", False),
                "auth_token_enabled": cluster.get("AuthTokenEnabled", False),
            },
        }
    except Exception as e:
        logger.debug(f"ElastiCache collect failed {arn}: {e}")
        return None


def _collect_ecs_service(arn: str, region: str) -> dict:
    parts = arn.split("/")
    cluster_name = parts[-2] if len(parts) >= 3 else "unknown"
    service_name = parts[-1]
    try:
        ecs = aws_client.get_client("ecs", region)
        resp = ecs.describe_services(cluster=cluster_name, services=[arn])
        services = resp.get("services", [])
        if not services:
            return None
        svc = services[0]
        return {
            "name": service_name,
            "tags": {t["key"]: t["value"] for t in svc.get("tags", [])},
            "metadata": {
                "cluster":          cluster_name,
                "status":           svc.get("status"),
                "desired_count":    svc.get("desiredCount"),
                "running_count":    svc.get("runningCount"),
                "launch_type":      svc.get("launchType", ""),
                "task_definition":  svc.get("taskDefinition", ""),
            },
        }
    except Exception as e:
        logger.debug(f"ECS service collect failed {arn}: {e}")
        return None


def _collect_eks(arn: str, region: str) -> dict:
    cluster_name = arn.split("/")[-1]
    try:
        eks = aws_client.get_client("eks", region)
        resp = eks.describe_cluster(name=cluster_name)
        cluster = resp.get("cluster", {})
        resources_vpc = cluster.get("resourcesVpcConfig", {})
        return {
            "name": cluster_name,
            "tags": cluster.get("tags", {}),
            "metadata": {
                "status":                   cluster.get("status"),
                "version":                  cluster.get("version"),
                "endpoint_public_access":   resources_vpc.get("endpointPublicAccess", True),
                "endpoint_private_access":  resources_vpc.get("endpointPrivateAccess", False),
                "encryption_config":        bool(cluster.get("encryptionConfig")),
                "role_arn":                 cluster.get("roleArn", ""),
            },
        }
    except Exception as e:
        logger.debug(f"EKS collect failed {arn}: {e}")
        return None


def _collect_redshift(arn: str, region: str) -> dict:
    cluster_id = arn.split(":")[-1]
    try:
        rs = aws_client.get_client("redshift", region)
        resp = rs.describe_clusters(ClusterIdentifier=cluster_id)
        clusters = resp.get("Clusters", [])
        if not clusters:
            return None
        c = clusters[0]
        return {
            "name": cluster_id,
            "tags": {t["Key"]: t["Value"] for t in c.get("Tags", [])},
            "metadata": {
                "node_type":            c.get("NodeType"),
                "num_nodes":            c.get("NumberOfNodes"),
                "status":               c.get("ClusterStatus"),
                "publicly_accessible":  c.get("PubliclyAccessible", False),
                "encrypted":            c.get("Encrypted", False),
                "database_name":        c.get("DBName"),
            },
        }
    except Exception as e:
        logger.debug(f"Redshift collect failed {arn}: {e}")
        return None


def _collect_sqs(arn: str, region: str) -> dict:
    queue_name = arn.split(":")[-1]
    account_id = arn.split(":")[4]
    queue_url = f"https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}"
    try:
        sqs = aws_client.get_client("sqs", region)
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["All"],
        ).get("Attributes", {})
        return {
            "name": queue_name,
            "tags": {},
            "metadata": {
                "visibility_timeout":   attrs.get("VisibilityTimeout"),
                "message_retention_sec":attrs.get("MessageRetentionPeriod"),
                "approx_messages":      attrs.get("ApproximateNumberOfMessages", "0"),
                "kms_key":              attrs.get("KmsMasterKeyId", ""),
                "policy":               attrs.get("Policy", ""),
            },
        }
    except Exception as e:
        logger.debug(f"SQS collect failed {arn}: {e}")
        return None


def _extract_cc_tags(props: dict) -> dict:
    """
    Extract tags from a Cloud Control ResourceModel properties dict.
    Handles both list-of-{Key,Value} form and plain dict form.
    """
    tags_raw = props.get("Tags", [])
    if isinstance(tags_raw, list):
        return {
            t.get("Key", ""): t.get("Value", "")
            for t in tags_raw
            if isinstance(t, dict)
        }
    if isinstance(tags_raw, dict):
        return tags_raw
    return {}


def _collect_generic(arn: str, region: str, resource_type: str) -> dict:
    """
    Fallback collector for resource types that don't have a dedicated handler.

    Strategy:
      1. Try AWS Cloud Control get_resource — returns a full JSON property bag
         for any type that supports the Cloud Control API (~400 types).
      2. If Cloud Control is unsupported or fails, fall back to a minimal
         record derived from the ARN so the resource is never silently dropped.
    """
    # Derive the best-guess identifier from the ARN.
    # For ARNs like  arn:aws:states:eu-west-1:123:stateMachine:MyMachine
    # the identifier is the last colon-segment; for arn:...:volume/vol-abc it
    # is the last slash-segment.  Cloud Control primary keys are usually one
    # of those two forms.
    raw_last  = arn.split(":")[-1]
    identifier = raw_last.split("/")[-1] if "/" in raw_last else raw_last
    name       = identifier  # may be overridden by CC response

    if resource_type not in _CLOUDCONTROL_UNSUPPORTED:
        try:
            cc     = aws_client.get_client("cloudcontrol", region)
            result = cc.get_resource(
                TypeName=resource_type,
                Identifier=identifier,
            )
            props_raw = (
                result.get("ResourceDescription", {})
                      .get("Properties", "{}")
            )
            props: dict = json.loads(props_raw) if props_raw else {}

            # Resolve a human-readable name from the most common name fields
            # across AWS resource types — first non-empty value wins.
            name = next(
                filter(None, [
                    props.get("Name"),
                    props.get("FunctionName"),
                    props.get("TableName"),
                    props.get("BucketName"),
                    props.get("DomainName"),
                    props.get("ClusterName"),
                    props.get("GroupName"),
                    props.get("RepositoryName"),
                    props.get("TopicName"),
                    props.get("StreamName"),
                    props.get("StateMachineName"),
                    props.get("JobName"),
                    props.get("PipelineName"),
                    props.get("AlarmName"),
                    props.get("RuleName"),
                    props.get("WebACLName"),
                    props.get("ProjectName"),
                    props.get("DatabaseName"),
                    props.get("CrawlerName"),
                    identifier,  # final fallback
                ]),
                identifier,
            )

            return {
                "name":     str(name)[:120],
                "tags":     _extract_cc_tags(props),
                "metadata": props,
            }

        except Exception as exc:
            err = str(exc).lower()
            # Permanently mark types that Cloud Control cannot service to
            # prevent repeated failed calls on every resource of that type.
            if any(s in err for s in (
                "type not found", "unsupported", "not supported",
                "typeconfiguration", "does not support list",
                "handlerexception",
            )):
                _CLOUDCONTROL_UNSUPPORTED.add(resource_type)
                logger.debug(
                    f"Cloud Control: marked {resource_type} as unsupported"
                )
            else:
                logger.debug(
                    f"Cloud Control get_resource [{resource_type}/{identifier}]: {exc}"
                )

    # Minimal fallback — resource is recorded even if no metadata is available
    return {
        "name":     name,
        "tags":     {},
        "metadata": {"raw_arn": arn},
    }


def _collect_vpc(arn: str, region: str) -> dict:
    """Describe a single VPC and return its key attributes."""
    vpc_id = arn.split("/")[-1]
    try:
        ec2  = aws_client.get_client("ec2", region)
        vpcs = ec2.describe_vpcs(VpcIds=[vpc_id]).get("Vpcs", [])
        if not vpcs:
            return None
        vpc  = vpcs[0]
        name = next((t["Value"] for t in vpc.get("Tags", []) if t["Key"] == "Name"), vpc_id)
        sn_count = len(
            ec2.describe_subnets(
                Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
            ).get("Subnets", [])
        )
        return {
            "name": name,
            "tags": {t["Key"]: t["Value"] for t in vpc.get("Tags", [])},
            "metadata": {
                "cidr_block":       vpc.get("CidrBlock", ""),
                "state":            vpc.get("State", ""),
                "is_default":       vpc.get("IsDefault", False),
                "subnet_count":     sn_count,
                "dhcp_options_id":  vpc.get("DhcpOptionsId", ""),
            },
        }
    except Exception as e:
        logger.debug(f"VPC collect failed {arn}: {e}")
        return None


def _collect_subnet(arn: str, region: str) -> dict:
    """Describe a single Subnet."""
    subnet_id = arn.split("/")[-1]
    try:
        ec2     = aws_client.get_client("ec2", region)
        subnets = ec2.describe_subnets(SubnetIds=[subnet_id]).get("Subnets", [])
        if not subnets:
            return None
        sn   = subnets[0]
        name = next((t["Value"] for t in sn.get("Tags", []) if t["Key"] == "Name"), subnet_id)
        return {
            "name": name,
            "tags": {t["Key"]: t["Value"] for t in sn.get("Tags", [])},
            "metadata": {
                "cidr_block":           sn.get("CidrBlock", ""),
                "availability_zone":    sn.get("AvailabilityZone", ""),
                "vpc_id":               sn.get("VpcId", ""),
                "available_ips":        sn.get("AvailableIpAddressCount", 0),
                "auto_assign_public_ip":sn.get("MapPublicIpOnLaunch", False),
                "state":                sn.get("State", ""),
            },
        }
    except Exception as e:
        logger.debug(f"Subnet collect failed {arn}: {e}")
        return None


def _collect_igw(arn: str, region: str) -> dict:
    """Describe an Internet Gateway."""
    igw_id = arn.split("/")[-1]
    try:
        ec2  = aws_client.get_client("ec2", region)
        igws = ec2.describe_internet_gateways(InternetGatewayIds=[igw_id]).get("InternetGateways", [])
        if not igws:
            return None
        igw  = igws[0]
        name = next((t["Value"] for t in igw.get("Tags", []) if t["Key"] == "Name"), igw_id)
        attachments = igw.get("Attachments", [])
        attached_vpc = attachments[0].get("VpcId", "") if attachments else ""
        state        = attachments[0].get("State", "detached") if attachments else "detached"
        return {
            "name": name,
            "tags": {t["Key"]: t["Value"] for t in igw.get("Tags", [])},
            "metadata": {
                "attached_vpc": attached_vpc,
                "state":        state,
            },
        }
    except Exception as e:
        logger.debug(f"IGW collect failed {arn}: {e}")
        return None


def _collect_nat_gateway(arn: str, region: str) -> dict:
    """Describe a NAT Gateway."""
    nat_id = arn.split("/")[-1]
    try:
        ec2  = aws_client.get_client("ec2", region)
        nats = ec2.describe_nat_gateways(NatGatewayIds=[nat_id]).get("NatGateways", [])
        if not nats:
            return None
        nat  = nats[0]
        name = next((t["Value"] for t in nat.get("Tags", []) if t["Key"] == "Name"), nat_id)
        addrs      = nat.get("NatGatewayAddresses", [])
        public_ip  = addrs[0].get("PublicIp", "") if addrs else ""
        return {
            "name": name,
            "tags": {t["Key"]: t["Value"] for t in nat.get("Tags", [])},
            "metadata": {
                "state":       nat.get("State", ""),
                "subnet_id":   nat.get("SubnetId", ""),
                "vpc_id":      nat.get("VpcId", ""),
                "nat_type":    nat.get("ConnectivityType", "public"),
                "public_ip":   public_ip,
            },
        }
    except Exception as e:
        logger.debug(f"NAT Gateway collect failed {arn}: {e}")
        return None


def _collect_ebs_volume(arn: str, region: str) -> dict:
    """Describe an EBS Volume."""
    vol_id = arn.split("/")[-1]
    try:
        ec2  = aws_client.get_client("ec2", region)
        vols = ec2.describe_volumes(VolumeIds=[vol_id]).get("Volumes", [])
        if not vols:
            return None
        vol  = vols[0]
        name = next((t["Value"] for t in vol.get("Tags", []) if t["Key"] == "Name"), vol_id)
        # Determine attachment
        attachments = vol.get("Attachments", [])
        attached_to = attachments[0].get("InstanceId", "") if attachments else ""
        return {
            "name": name,
            "tags": {t["Key"]: t["Value"] for t in vol.get("Tags", [])},
            "metadata": {
                "volume_type":  vol.get("VolumeType", ""),
                "size_gb":      vol.get("Size", 0),
                "state":        vol.get("State", ""),
                "iops":         vol.get("Iops", 0),
                "encrypted":    vol.get("Encrypted", False),
                "az":           vol.get("AvailabilityZone", ""),
                "attached_to":  attached_to,
            },
        }
    except Exception as e:
        logger.debug(f"EBS Volume collect failed {arn}: {e}")
        return None


def _collect_ebs_snapshot(arn: str, region: str) -> dict:
    """Describe an EBS Snapshot."""
    snap_id = arn.split("/")[-1]
    try:
        ec2   = aws_client.get_client("ec2", region)
        snaps = ec2.describe_snapshots(SnapshotIds=[snap_id]).get("Snapshots", [])
        if not snaps:
            return None
        snap = snaps[0]
        name = next((t["Value"] for t in snap.get("Tags", []) if t["Key"] == "Name"), snap_id)
        return {
            "name": name,
            "tags": {t["Key"]: t["Value"] for t in snap.get("Tags", [])},
            "metadata": {
                "volume_id":     snap.get("VolumeId", ""),
                "volume_size_gb":snap.get("VolumeSize", 0),
                "state":         snap.get("State", ""),
                "encrypted":     snap.get("Encrypted", False),
                "description":   snap.get("Description", "")[:80],
                "start_time":    str(snap.get("StartTime", "")),
            },
        }
    except Exception as e:
        logger.debug(f"EBS Snapshot collect failed {arn}: {e}")
        return None


def _collect_security_group(arn: str, region: str) -> dict:
    """Describe a Security Group."""
    sg_id = arn.split("/")[-1]
    try:
        ec2 = aws_client.get_client("ec2", region)
        sgs = ec2.describe_security_groups(GroupIds=[sg_id]).get("SecurityGroups", [])
        if not sgs:
            return None
        sg   = sgs[0]
        name = sg.get("GroupName", sg_id)
        return {
            "name": name,
            "tags": {t["Key"]: t["Value"] for t in sg.get("Tags", [])},
            "metadata": {
                "vpc_id":          sg.get("VpcId", ""),
                "description":     sg.get("Description", "")[:80],
                "inbound_rules":   len(sg.get("IpPermissions", [])),
                "outbound_rules":  len(sg.get("IpPermissionsEgress", [])),
            },
        }
    except Exception as e:
        logger.debug(f"Security Group collect failed {arn}: {e}")
        return None


def _collect_eip(arn: str, region: str) -> dict:
    """Describe an Elastic IP address."""
    alloc_id = arn.split("/")[-1]
    try:
        ec2   = aws_client.get_client("ec2", region)
        addrs = ec2.describe_addresses(AllocationIds=[alloc_id]).get("Addresses", [])
        if not addrs:
            return None
        addr = addrs[0]
        pub_ip = addr.get("PublicIp", alloc_id)
        name   = next((t["Value"] for t in addr.get("Tags", []) if t["Key"] == "Name"), pub_ip)
        return {
            "name": name,
            "tags": {t["Key"]: t["Value"] for t in addr.get("Tags", [])},
            "metadata": {
                "public_ip":    pub_ip,
                "instance_id":  addr.get("InstanceId", ""),
                "domain":       addr.get("Domain", "vpc"),
                "association_id": addr.get("AssociationId", ""),
                "state":        "associated" if addr.get("AssociationId") else "unassociated",
            },
        }
    except Exception as e:
        logger.debug(f"EIP collect failed {arn}: {e}")
        return None


def _collect_elb(arn: str, region: str) -> dict:
    """Describe an ALB or NLB (ELBv2)."""
    try:
        elbv2 = aws_client.get_client("elbv2", region)
        lbs   = elbv2.describe_load_balancers(LoadBalancerArns=[arn]).get("LoadBalancers", [])
        if not lbs:
            return None
        lb   = lbs[0]
        name = lb.get("LoadBalancerName", arn.split("/")[-2])
        return {
            "name": name,
            "tags": {},
            "metadata": {
                "lb_type":      lb.get("Type", ""),
                "scheme":       lb.get("Scheme", ""),
                "state":        lb.get("State", {}).get("Code", ""),
                "dns_name":     lb.get("DNSName", ""),
                "vpc_id":       lb.get("VpcId", ""),
                "az_count":     len(lb.get("AvailabilityZones", [])),
            },
        }
    except Exception as e:
        logger.debug(f"ELB collect failed {arn}: {e}")
        return None


def _collect_sns_topic(arn: str, region: str) -> dict:
    """Get attributes for an SNS Topic."""
    try:
        sns   = aws_client.get_client("sns", region)
        attrs = sns.get_topic_attributes(TopicArn=arn).get("Attributes", {})
        name  = arn.split(":")[-1]
        return {
            "name": name,
            "tags": {},
            "metadata": {
                "subscriptions_confirmed": attrs.get("SubscriptionsConfirmed", "0"),
                "subscriptions_pending":  attrs.get("SubscriptionsPending", "0"),
                "kms_key":                attrs.get("KmsMasterKeyId", ""),
            },
        }
    except Exception as e:
        logger.debug(f"SNS Topic collect failed {arn}: {e}")
        return None


def _collect_secret(arn: str, region: str) -> dict:
    """Describe a Secrets Manager secret."""
    try:
        sm     = aws_client.get_client("secretsmanager", region)
        secret = sm.describe_secret(SecretId=arn)
        name   = secret.get("Name", arn.split(":")[-1])
        return {
            "name": name,
            "tags": {t["Key"]: t["Value"] for t in secret.get("Tags", [])},
            "metadata": {
                "rotation_enabled": secret.get("RotationEnabled", False),
                "last_rotated":     str(secret.get("LastRotatedDate", "")),
                "last_accessed":    str(secret.get("LastAccessedDate", "")),
            },
        }
    except Exception as e:
        logger.debug(f"Secret collect failed {arn}: {e}")
        return None


def _collect_ecr_repo(arn: str, region: str) -> dict:
    """Describe an ECR Repository."""
    repo_name = arn.split("/")[-1]
    try:
        ecr   = aws_client.get_client("ecr", region)
        repos = ecr.describe_repositories(repositoryNames=[repo_name]).get("repositories", [])
        if not repos:
            return None
        repo = repos[0]
        # Get image count (may fail if no perms)
        try:
            img_count = len(
                ecr.describe_images(repositoryName=repo_name).get("imageDetails", [])
            )
        except Exception:
            img_count = 0
        return {
            "name": repo_name,
            "tags": {},
            "metadata": {
                "uri":             repo.get("repositoryUri", ""),
                "image_count":     img_count,
                "scan_on_push":    repo.get("imageScanningConfiguration", {}).get("scanOnPush", False),
                "encryption":      repo.get("encryptionConfiguration", {}).get("encryptionType", "AES256"),
            },
        }
    except Exception as e:
        logger.debug(f"ECR Repo collect failed {arn}: {e}")
        return None


def _collect_acm_cert(arn: str, region: str) -> dict:
    """Describe an ACM Certificate."""
    try:
        acm  = aws_client.get_client("acm", region)
        cert = acm.describe_certificate(CertificateArn=arn).get("Certificate", {})
        name = cert.get("DomainName", arn.split("/")[-1])
        return {
            "name": name,
            "tags": {},
            "metadata": {
                "domain":         cert.get("DomainName", ""),
                "status":         cert.get("Status", ""),
                "expiry":         str(cert.get("NotAfter", "")),
                "renewal_status": cert.get("RenewalSummary", {}).get("RenewalStatus", ""),
                "key_algorithm":  cert.get("KeyAlgorithm", ""),
            },
        }
    except Exception as e:
        logger.debug(f"ACM Cert collect failed {arn}: {e}")
        return None


def _collect_asg(arn: str, region: str) -> dict:
    """Describe an Auto Scaling Group."""
    asg_name = arn.split("/")[-1] if "/" in arn else arn.split(":")[-1]
    try:
        asgc   = aws_client.get_client("autoscaling", region)
        groups = asgc.describe_auto_scaling_groups(
            AutoScalingGroupNames=[asg_name]
        ).get("AutoScalingGroups", [])
        if not groups:
            return None
        asg = groups[0]
        return {
            "name": asg_name,
            "tags": {t["Key"]: t["Value"] for t in asg.get("Tags", [])},
            "metadata": {
                "min_size":          asg.get("MinSize", 0),
                "max_size":          asg.get("MaxSize", 0),
                "desired_capacity":  asg.get("DesiredCapacity", 0),
                "health_check_type": asg.get("HealthCheckType", ""),
                "az_count":          len(asg.get("AvailabilityZones", [])),
                "status":            asg.get("Status", "Active"),
            },
        }
    except Exception as e:
        logger.debug(f"ASG collect failed {arn}: {e}")
        return None


def _collect_ssm_doc(arn: str, region: str) -> dict:
    """Describe a self-owned SSM Document."""
    doc_name = arn.split("/")[-1]
    try:
        ssm  = aws_client.get_client("ssm", region)
        resp = ssm.describe_document(Name=doc_name)
        doc  = resp.get("Document", {})
        return {
            "name": doc_name,
            "tags": {},
            "metadata": {
                "document_type":    doc.get("DocumentType", ""),
                "document_format":  doc.get("DocumentFormat", ""),
                "schema_version":   doc.get("SchemaVersion", ""),
                "status":           doc.get("Status", ""),
                "platform_types":   ", ".join(doc.get("PlatformTypes", [])),
                "owner":            doc.get("Owner", ""),
            },
        }
    except Exception as e:
        logger.debug(f"SSM Document collect failed {arn}: {e}")
        return None


def _collect_cloudtrail(arn: str, region: str) -> dict:
    """Describe a CloudTrail trail."""
    trail_name = arn.split("/")[-1]
    try:
        ct      = aws_client.get_client("cloudtrail", region)
        trails  = ct.describe_trails(trailNameList=[trail_name]).get("trailList", [])
        if not trails:
            return None
        trail = trails[0]
        # Get trail status (logging on/off)
        try:
            status   = ct.get_trail_status(Name=arn)
            logging_ = status.get("IsLogging", False)
        except Exception:
            logging_ = None
        return {
            "name": trail_name,
            "tags": {},
            "metadata": {
                "is_multi_region":    trail.get("IsMultiRegionTrail", False),
                "is_logging":         logging_,
                "log_validation":     trail.get("LogFileValidationEnabled", False),
                "s3_bucket":          trail.get("S3BucketName", ""),
                "cloudwatch_logs":    bool(trail.get("CloudWatchLogsLogGroupArn")),
                "home_region":        trail.get("HomeRegion", region),
            },
        }
    except Exception as e:
        logger.debug(f"CloudTrail collect failed {arn}: {e}")
        return None


def _collect_apigateway(arn: str, region: str) -> dict:
    """Describe an API Gateway REST API."""
    # ARN format: arn:aws:apigateway:{region}::/restapis/{api_id}
    api_id = arn.split("/restapis/")[-1].split("/")[0] if "/restapis/" in arn else arn.split("/")[-1]
    try:
        agw = aws_client.get_client("apigateway", region)
        api = agw.get_rest_api(restApiId=api_id)
        return {
            "name": api.get("name", api_id),
            "tags": api.get("tags", {}),
            "metadata": {
                "api_id":              api_id,
                "endpoint_types":      ", ".join(
                    api.get("endpointConfiguration", {}).get("types", [])
                ),
                "created":             str(api.get("createdDate", "")),
                "disable_execute_api": api.get("disableExecuteApiEndpoint", False),
            },
        }
    except Exception as e:
        logger.debug(f"API Gateway collect failed {arn}: {e}")
        return None


def _collect_kinesis(arn: str, region: str) -> dict:
    """Describe a Kinesis Data Stream."""
    stream_name = arn.split("/")[-1]
    try:
        ks   = aws_client.get_client("kinesis", region)
        resp = ks.describe_stream_summary(StreamName=stream_name)
        sd   = resp.get("StreamDescriptionSummary", {})
        return {
            "name": stream_name,
            "tags": {},
            "metadata": {
                "status":          sd.get("StreamStatus", ""),
                "shard_count":     sd.get("OpenShardCount", 0),
                "retention_hours": sd.get("RetentionPeriodHours", 24),
                "stream_mode":     sd.get("StreamModeDetails", {}).get("StreamMode", "PROVISIONED"),
                "encryption_type": sd.get("EncryptionType", "NONE"),
            },
        }
    except Exception as e:
        logger.debug(f"Kinesis collect failed {arn}: {e}")
        return None


def _collect_events_rule(arn: str, region: str) -> dict:
    """Describe an EventBridge / CloudWatch Events rule."""
    # ARN: arn:aws:events:{region}:{acct}:rule/{optional-bus}/{rule-name}
    parts     = arn.split(":rule/")
    rule_name = parts[-1].split("/")[-1] if parts else arn.split("/")[-1]
    try:
        events = aws_client.get_client("events", region)
        rule   = events.describe_rule(Name=rule_name)
        return {
            "name": rule_name,
            "tags": {},
            "metadata": {
                "state":           rule.get("State", ""),
                "schedule":        rule.get("ScheduleExpression", ""),
                "event_pattern":   "Yes" if rule.get("EventPattern") else "No",
                "event_bus":       rule.get("EventBusName", "default"),
                "description":     rule.get("Description", "")[:80],
            },
        }
    except Exception as e:
        logger.debug(f"EventBridge rule collect failed {arn}: {e}")
        return None


def _collect_sfn(arn: str, region: str) -> dict:
    """Describe a Step Functions state machine."""
    try:
        sfn  = aws_client.get_client("stepfunctions", region)
        resp = sfn.describe_state_machine(stateMachineArn=arn)
        name = resp.get("name", arn.split(":")[-1])
        return {
            "name": name,
            "tags": {},
            "metadata": {
                "type":        resp.get("type", ""),
                "status":      resp.get("status", ""),
                "created":     str(resp.get("creationDate", "")),
                "logging":     resp.get("loggingConfiguration", {}).get("level", "OFF"),
                "tracing":     resp.get("tracingConfiguration", {}).get("enabled", False),
                "role_arn":    resp.get("roleArn", ""),
            },
        }
    except Exception as e:
        logger.debug(f"Step Functions collect failed {arn}: {e}")
        return None


# Dispatcher mapping resource type -> collector function
_COLLECTORS = {
    "AWS::EC2::Instance":                   _collect_ec2,
    "AWS::RDS::DBInstance":                 _collect_rds,
    "AWS::RDS::DBCluster":                  _collect_rds,
    "AWS::Lambda::Function":                _collect_lambda,
    "AWS::S3::Bucket":                      _collect_s3,
    "AWS::DynamoDB::Table":                 _collect_dynamodb,
    "AWS::ElastiCache::CacheCluster":       _collect_elasticache,
    "AWS::ElastiCache::ReplicationGroup":   _collect_elasticache,
    "AWS::ECS::Service":                    _collect_ecs_service,
    "AWS::EKS::Cluster":                    _collect_eks,
    "AWS::Redshift::Cluster":               _collect_redshift,
    "AWS::SQS::Queue":                      _collect_sqs,
    # Networking
    "AWS::EC2::VPC":                        _collect_vpc,
    "AWS::EC2::Subnet":                     _collect_subnet,
    "AWS::EC2::InternetGateway":            _collect_igw,
    "AWS::EC2::NatGateway":                 _collect_nat_gateway,
    "AWS::EC2::SecurityGroup":              _collect_security_group,
    "AWS::EC2::EIP":                        _collect_eip,
    "AWS::ElasticLoadBalancingV2::LoadBalancer": _collect_elb,
    # Storage
    "AWS::EC2::Volume":                     _collect_ebs_volume,
    "AWS::EC2::Snapshot":                   _collect_ebs_snapshot,
    # Messaging / Security / Containers / Scaling
    "AWS::SNS::Topic":                      _collect_sns_topic,
    "AWS::SecretsManager::Secret":          _collect_secret,
    "AWS::ECR::Repository":                 _collect_ecr_repo,
    "AWS::ACM::Certificate":                _collect_acm_cert,
    "AWS::AutoScaling::AutoScalingGroup":   _collect_asg,
    # Systems Manager
    "AWS::SSM::Document":                   _collect_ssm_doc,
    # Developer Tools / Workflow (direct-enumerated with specific metadata)
    "AWS::CloudTrail::Trail":               _collect_cloudtrail,
    "AWS::ApiGateway::RestApi":             _collect_apigateway,
    "AWS::Kinesis::Stream":                 _collect_kinesis,
    "AWS::Events::Rule":                    _collect_events_rule,
    "AWS::StepFunctions::StateMachine":     _collect_sfn,
}


# Maps resource types that share the same service prefix to a more descriptive
# service label used for grouping in reports.  Unlisted types fall back to the
# middle segment of the CFN type string (e.g. AWS::ECR::Repository -> ecr).
_TYPE_TO_SERVICE = {
    # EC2 compute
    "AWS::EC2::Instance":                        "ec2",
    # Networking
    "AWS::EC2::VPC":                             "vpc",
    "AWS::EC2::Subnet":                          "subnet",
    "AWS::EC2::InternetGateway":                 "igw",
    "AWS::EC2::NatGateway":                      "nat-gateway",
    "AWS::EC2::SecurityGroup":                   "security-group",
    "AWS::EC2::EIP":                             "eip",
    # Storage
    "AWS::EC2::Volume":                          "ebs",
    "AWS::EC2::Snapshot":                        "ebs-snapshot",
    # Load Balancing
    "AWS::ElasticLoadBalancingV2::LoadBalancer": "loadbalancer",
    "AWS::ElasticLoadBalancing::LoadBalancer":   "loadbalancer",
    # Developer Tools / Workflow
    "AWS::SSM::Document":                        "ssm",
    "AWS::SSM::PatchBaseline":                   "ssm",
    "AWS::CloudTrail::Trail":                    "cloudtrail",
    "AWS::ApiGateway::RestApi":                  "apigateway",
    "AWS::Kinesis::Stream":                      "kinesis",
    "AWS::Events::Rule":                         "eventbridge",
    "AWS::StepFunctions::StateMachine":          "stepfunctions",
}


def _collect_one(arn: str, resource_type: str, region: str) -> dict:
    """Collect metadata for a single resource and return a unified record."""
    collector = _COLLECTORS.get(resource_type)
    if collector:
        data = collector(arn, region)
    else:
        data = _collect_generic(arn, region, resource_type)

    if data is None:
        return None

    # Determine region from ARN if not provided
    if not region or region == "global":
        arn_parts = arn.split(":")
        region = arn_parts[3] if len(arn_parts) > 3 and arn_parts[3] else "global"

    # Use fine-grained service label where possible, otherwise derive from type string
    service = _TYPE_TO_SERVICE.get(
        resource_type,
        resource_type.split("::")[1].lower() if "::" in resource_type else "unknown",
    )

    return {
        "arn":      arn,
        "service":  service,
        "type":     resource_type,
        "region":   region,
        "name":     data.get("name", arn.split("/")[-1]),
        "tags":     data.get("tags", {}),
        "metadata": data.get("metadata", {}),
    }


def collect(service_map: dict, regions: list) -> list:
    """
    Collect detailed metadata for all resources in service_map.
    Returns a flat unified resource list.
    """
    tasks = []
    for resource_type, info in service_map.items():
        if resource_type.startswith("__namespace__"):
            continue
        for arn in info.get("arns", []):
            # Determine region from ARN
            parts = arn.split(":")
            region = parts[3] if len(parts) > 3 and parts[3] else "us-east-1"
            if region == "" or region == "global":
                region = "us-east-1"
            tasks.append((arn, resource_type, region))

    logger.info(f"Collecting metadata for {len(tasks)} resources...")
    resources = []

    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = {
            executor.submit(_collect_one, arn, rt, reg): (arn, rt)
            for arn, rt, reg in tasks
        }
        for future in as_completed(futures):
            arn, rt = futures[future]
            try:
                result = future.result()
                if result:
                    resources.append(result)
            except Exception as e:
                logger.debug(f"Collection failed for {arn}: {e}")

    logger.info(f"Resource collection complete: {len(resources)} resources collected.")
    return resources
