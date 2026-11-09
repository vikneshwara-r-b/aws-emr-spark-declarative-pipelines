import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template

from stacks.config import DeploymentConfig
from stacks.emr_cluster_stack import EmrClusterStack
from stacks.platform_stack import PlatformStack

CONTEXT = {
    "project": "emr-sdp",
    "region": "us-east-1",
    "prefix": "f1-pipeline",
    "glue_database": "f1_sdp_db",
    "emr_release_label": "emr-spark-8.0.0",
    "spark_version": "4.3.0",
    "primary_instance_type": "m5.xlarge",
    "core_instance_type": "m5.xlarge",
    "core_instance_count": 2,
    "core_market": "ON_DEMAND",
    "primary_market": "ON_DEMAND",
    "idle_timeout_seconds": 3600,
    "vpc_cidr": "10.20.0.0/16",
    "owner_tag": "de-team",
}


def synth(overrides=None):
    app = cdk.App(context={**CONTEXT, **(overrides or {})})
    config = DeploymentConfig.from_context(app.node)
    env = cdk.Environment(account="111111111111", region="us-east-1")
    platform = PlatformStack(app, "Platform", config=config, env=env)
    cluster = EmrClusterStack(app, "Cluster", config=config, platform=platform, env=env)
    return Template.from_stack(platform), Template.from_stack(cluster)


def test_platform_network_has_single_nat_s3_endpoint_and_private_dns_emr_endpoint():
    platform, _ = synth()
    platform.resource_count_is("AWS::EC2::NatGateway", 1)
    platform.has_resource_properties("AWS::EC2::VPCEndpoint", {"VpcEndpointType": "Gateway"})
    # PrivateDnsEnabled: otherwise EMR creates a second endpoint for the same service and fails.
    platform.has_resource_properties(
        "AWS::EC2::VPCEndpoint",
        {"ServiceName": "aws.api.us-east-1.emr-service-cell01", "VpcEndpointType": "Interface", "PrivateDnsEnabled": True},
    )


def test_platform_creates_private_ecr_repo_bucket_and_glue_database():
    platform, _ = synth()
    platform.has_resource_properties(
        "AWS::ECR::Repository",
        {"RepositoryName": "emr-sdp-builder", "ImageScanningConfiguration": {"ScanOnPush": True}},
    )
    platform.has_resource_properties(
        "AWS::S3::Bucket",
        {"PublicAccessBlockConfiguration": Match.object_like({"BlockPublicAcls": True})},
    )
    platform.has_resource_properties(
        "AWS::Glue::Database", {"DatabaseInput": Match.object_like({"Name": "f1_sdp_db"})}
    )


def test_service_role_can_create_the_emr_vpc_endpoint_and_pass_the_instance_role():
    platform, _ = synth()
    statements = Match.array_with(
        [
            Match.object_like({"Action": ["ec2:CreateVpcEndpoint", "ec2:ModifyVpcEndpoint"]}),
            Match.object_like(
                {"Action": "iam:PassRole", "Condition": {"StringEquals": {"iam:PassedToService": "ec2.amazonaws.com"}}}
            ),
        ]
    )
    platform.has_resource_properties("AWS::IAM::Policy", {"PolicyDocument": {"Statement": statements}})


def test_cluster_shape_security_groups_and_spark_bootstrap():
    _, cluster = synth()
    cluster.has_resource_properties(
        "AWS::EMR::Cluster",
        {
            "ReleaseLabel": "emr-spark-8.0.0",
            "AutoTerminationPolicy": {"IdleTimeout": 3600},
            "Instances": Match.object_like(
                {
                    "MasterInstanceGroup": Match.object_like({"Market": "ON_DEMAND", "InstanceCount": 1}),
                    "CoreInstanceGroup": Match.object_like({"Market": "ON_DEMAND", "InstanceCount": 2}),
                    "KeepJobFlowAliveWhenNoSteps": True,
                    # all three are mandatory together for a cluster in a private subnet
                    "EmrManagedMasterSecurityGroup": Match.any_value(),
                    "EmrManagedSlaveSecurityGroup": Match.any_value(),
                    "ServiceAccessSecurityGroup": Match.any_value(),
                }
            ),
            "BootstrapActions": [
                Match.object_like({"ScriptBootstrapAction": Match.object_like({"Args": [Match.any_value()]})})
            ],
        },
    )


def test_context_overrides_apply_and_an_empty_core_group_is_rejected():
    _, cluster = synth({"core_market": "SPOT", "core_instance_count": 3})
    cluster.has_resource_properties(
        "AWS::EMR::Cluster",
        {"Instances": Match.object_like({"CoreInstanceGroup": Match.object_like({"Market": "SPOT", "InstanceCount": 3})})},
    )
    with pytest.raises(ValueError, match="core_instance_count"):
        synth({"core_instance_count": 0})
