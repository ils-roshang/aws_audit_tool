"""
modules/aws_client.py
---------------------
boto3 client factory.
All modules obtain clients through this module so credentials
are injected from config in one place.
"""

import logging
import boto3
import botocore.config
import config

logger = logging.getLogger(__name__)

# Shared botocore config: retry with exponential back-off
_BOTO_CONFIG = botocore.config.Config(
    retries={"max_attempts": 5, "mode": "adaptive"},
    max_pool_connections=50,
)


def get_client(service: str, region: str = None):
    """Return a boto3 client for the given service and region."""
    region = region or config.AWS_DEFAULT_REGION
    return boto3.client(
        service,
        region_name=region,
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        config=_BOTO_CONFIG,
    )


def get_resource(service: str, region: str = None):
    """Return a boto3 resource for the given service and region."""
    region = region or config.AWS_DEFAULT_REGION
    return boto3.resource(
        service,
        region_name=region,
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        config=_BOTO_CONFIG,
    )


def get_all_regional_clients(service: str, regions: list):
    """Return dict of {region: client} for a service across multiple regions."""
    return {region: get_client(service, region) for region in regions}
