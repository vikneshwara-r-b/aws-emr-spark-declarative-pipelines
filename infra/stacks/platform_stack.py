from __future__ import annotations

from pathlib import Path

import aws_cdk as cdk
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3deploy
from constructs import Construct

from .config import DeploymentConfig

REPO_ROOT = Path(__file__).resolve().parents[2]

# EMR's AmazonEMRServicePolicy_v2 only acts on VPC resources carrying this tag.
EMR_MANAGED_TAG = ("for-use-with-amazon-emr-managed-policies", "true")


class PlatformStack(cdk.Stack):
    """Long-lived resources: network, storage, image registry and IAM."""

    def __init__(self, scope: Construct, construct_id: str, *, config: DeploymentConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.config = config

        # --- Network: one private subnet (cluster) + one public subnet (NAT) -------------
        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            ip_addresses=ec2.IpAddresses.cidr(config.vpc_cidr),
            max_azs=1,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(name="public", subnet_type=ec2.SubnetType.PUBLIC, cidr_mask=24),
                ec2.SubnetConfiguration(
                    name="private", subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS, cidr_mask=24
                ),
            ],
            gateway_endpoints={"S3": ec2.GatewayVpcEndpointOptions(service=ec2.GatewayVpcEndpointAwsService.S3)},
        )
        cdk.Tags.of(self.vpc).add(*EMR_MANAGED_TAG)
        self.private_subnet = self.vpc.private_subnets[0]
        cdk.Tags.of(self.private_subnet).add(*EMR_MANAGED_TAG)

        # emr-spark-8.0.0 clusters in private subnets talk to the EMR service through an
        # interface endpoint; its SG must allow inbound HTTPS from the VPC CIDR.
        self.service_access_sg = ec2.SecurityGroup(
            self,
            "EmrServiceAccessSg",
            vpc=self.vpc,
            description="EMR service access (VPC endpoint) - HTTPS from the VPC",
            allow_all_outbound=False,
        )
        self.service_access_sg.add_ingress_rule(
            ec2.Peer.ipv4(config.vpc_cidr), ec2.Port.tcp(443), "HTTPS from VPC"
        )
        cdk.Tags.of(self.service_access_sg).add(*EMR_MANAGED_TAG)

        # In a private subnet EMR requires the primary and core/task security groups to be
        # supplied together with the service access one (EMR only auto-creates and populates
        # them for public subnets). Because we own them, we must also open the intra-cluster
        # traffic EMR normally adds itself: all traffic between and within the two groups.
        self.primary_sg = ec2.SecurityGroup(
            self, "EmrPrimarySg", vpc=self.vpc, description="EMR primary (master) nodes"
        )
        self.core_sg = ec2.SecurityGroup(
            self, "EmrCoreSg", vpc=self.vpc, description="EMR core/task nodes"
        )
        for group in (self.primary_sg, self.core_sg):
            for peer in (self.primary_sg, self.core_sg):
                group.add_ingress_rule(peer, ec2.Port.all_traffic(), "Intra-cluster traffic")
            cdk.Tags.of(group).add(*EMR_MANAGED_TAG)

        ec2.InterfaceVpcEndpoint(
            self,
            "EmrServiceEndpoint",
            vpc=self.vpc,
            service=ec2.InterfaceVpcEndpointService(f"aws.api.{config.region}.emr-service-cell01", 443),
            subnets=ec2.SubnetSelection(subnets=[self.private_subnet]),
            security_groups=[self.service_access_sg],
            # Private DNS must be on: with it off, EMR does not recognise this endpoint and creates
            # a second one itself (not removed on cluster termination, which then blocks VPC deletion).
            private_dns_enabled=True,
        )

        # --- Storage ---------------------------------------------------------------------
        self.bucket = s3.Bucket(
            self,
            "Bucket",
            bucket_name=f"{config.project}-{self.account}-{self.region}",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,  # dev/test environment
            auto_delete_objects=True,
        )
        # Bootstrap + step scripts the cluster downloads at launch/run time.
        s3deploy.BucketDeployment(
            self,
            "BootstrapScripts",
            sources=[s3deploy.Source.asset(str(REPO_ROOT / "bootstrap"))],
            destination_bucket=self.bucket,
            destination_key_prefix=config.bootstrap_prefix,
            prune=True,
        )

        # --- Image registry (holds the Docker builder image) -----------------------------
        self.repository = ecr.Repository(
            self,
            "BuilderRepo",
            repository_name=config.ecr_repo_name,
            image_scan_on_push=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            empty_on_delete=True,
            lifecycle_rules=[ecr.LifecycleRule(max_image_count=5)],
        )

        # --- Glue Data Catalog database used by the pipeline's Iceberg catalog -----------
        glue.CfnDatabase(
            self,
            "GlueDb",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=config.glue_database,
                location_uri=f"s3://{self.bucket.bucket_name}/{config.prefix}/warehouse/",
                description="Tables written by the F1 Spark Declarative Pipeline on EMR",
            ),
        )

        # --- IAM -------------------------------------------------------------------------
        self.service_role = iam.Role(
            self,
            "EmrServiceRole",
            assumed_by=iam.ServicePrincipal("elasticmapreduce.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonEMRServicePolicy_v2")
            ],
        )
        # Required by emr-spark-8.0.0 to create its service VPC endpoint when the cluster
        # starts in a private subnet.
        self.service_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ec2:CreateVpcEndpoint", "ec2:ModifyVpcEndpoint"],
                resources=["*"],
            )
        )

        self.instance_role = iam.Role(
            self,
            "EmrInstanceRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore")],
        )
        self.bucket.grant_read_write(self.instance_role)
        self.instance_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:CreateDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:BatchGetPartition",
                ],
                resources=[
                    f"arn:{self.partition}:glue:{self.region}:{self.account}:catalog",
                    f"arn:{self.partition}:glue:{self.region}:{self.account}:database/{config.glue_database}",
                    f"arn:{self.partition}:glue:{self.region}:{self.account}:database/default",
                    f"arn:{self.partition}:glue:{self.region}:{self.account}:table/{config.glue_database}/*",
                ],
            )
        )
        # AmazonEMRServicePolicy_v2 does not cover our custom instance role, so without this
        # EMR's RunInstances call fails with "not authorized to perform iam:PassRole" and the
        # cluster terminates with "Service role ... has insufficient EC2 permissions".
        self.service_role.add_to_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[self.instance_role.role_arn],
                conditions={"StringEquals": {"iam:PassedToService": "ec2.amazonaws.com"}},
            )
        )
        # The first Spot request in an account needs the EC2 Spot service-linked role. Accounts that
        # never used Spot don't have it, and the cluster then fails with "Service-linked role
        # 'AWSServiceRoleForEC2Spot' for EC2 Spot Instances is required". Let EMR create it once
        # (a no-op if it already exists); scoped to exactly that role and service.
        self.service_role.add_to_policy(
            iam.PolicyStatement(
                actions=["iam:CreateServiceLinkedRole"],
                resources=[
                    f"arn:{self.partition}:iam::{self.account}:role/aws-service-role/"
                    "spot.amazonaws.com/AWSServiceRoleForEC2Spot"
                ],
                conditions={"StringEquals": {"iam:AWSServiceName": "spot.amazonaws.com"}},
            )
        )
        self.instance_profile = iam.CfnInstanceProfile(
            self, "EmrInstanceProfile", roles=[self.instance_role.role_name]
        )

        for name, value in {
            "BucketName": self.bucket.bucket_name,
            "EcrRepositoryUri": self.repository.repository_uri,
            "EcrRepositoryName": self.repository.repository_name,
            "PrivateSubnetId": self.private_subnet.subnet_id,
            "ServiceAccessSecurityGroupId": self.service_access_sg.security_group_id,
            "PrimarySecurityGroupId": self.primary_sg.security_group_id,
            "CoreSecurityGroupId": self.core_sg.security_group_id,
            "GlueDatabase": config.glue_database,
            "BundleS3Uri": f"s3://{self.bucket.bucket_name}/{config.bundle_key}",
        }.items():
            cdk.CfnOutput(self, name, value=value)
