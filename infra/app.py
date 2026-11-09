#!/usr/bin/env python3
"""CDK entrypoint: two stacks so the (billable) EMR cluster can be destroyed and
recreated independently of the VPC/NAT, bucket and ECR repo.

  EmrSdpPlatformStack  VPC + NAT, S3 bucket, ECR repo, IAM roles, Glue database
  EmrSdpClusterStack   the EMR on EC2 cluster (needs the Spark bundle already in S3)

All settings live in cdk.json "context" and can be overridden with -c key=value.
"""
import os

import aws_cdk as cdk

from stacks.config import DeploymentConfig
from stacks.emr_cluster_stack import EmrClusterStack
from stacks.platform_stack import PlatformStack

app = cdk.App()
config = DeploymentConfig.from_context(app.node)

env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=config.region,
)

platform = PlatformStack(app, "EmrSdpPlatformStack", config=config, env=env)
cluster = EmrClusterStack(
    app, "EmrSdpClusterStack", config=config, platform=platform, env=env
)
cluster.add_stack_dependency(
    platform, "Cluster needs the platform's VPC, bucket, roles and Spark bundle location"
)

cdk.Tags.of(app).add("project", config.project)
cdk.Tags.of(app).add("owner", config.owner_tag)

app.synth()
