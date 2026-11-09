from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import aws_emr as emr
from constructs import Construct

from .config import DeploymentConfig
from .platform_stack import PlatformStack


class EmrClusterStack(cdk.Stack):
    """The EMR on EC2 cluster. Spark 4.3 is installed by the bootstrap action from a
    bundle that scripts/build_bundle.sh uploaded to S3 -- build that before deploying."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        config: DeploymentConfig,
        platform: PlatformStack,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = platform.bucket.bucket_name
        bootstrap_uri = f"s3://{bucket}/{config.bootstrap_prefix}/install_spark.sh"
        bundle_uri = f"s3://{bucket}/{config.bundle_key}"

        def group(role: str, count: int, instance_type: str, market: str):
            return emr.CfnCluster.InstanceGroupConfigProperty(
                instance_count=count,
                instance_type=instance_type,
                market=market,
                name=role,
            )

        cluster = emr.CfnCluster(
            self,
            "Cluster",
            name=f"{config.project}-spark-{config.spark_version.replace('.', '-')}",
            release_label=config.emr_release_label,
            applications=[emr.CfnCluster.ApplicationProperty(name="Spark")],
            service_role=platform.service_role.role_name,
            job_flow_role=platform.instance_profile.ref,
            log_uri=f"s3://{bucket}/{config.prefix}/emr-logs/",
            visible_to_all_users=True,
            ebs_root_volume_size=40,
            instances=emr.CfnCluster.JobFlowInstancesConfigProperty(
                ec2_subnet_id=platform.private_subnet.subnet_id,
                # All three groups are mandatory together for a cluster in a private subnet.
                emr_managed_master_security_group=platform.primary_sg.security_group_id,
                emr_managed_slave_security_group=platform.core_sg.security_group_id,
                service_access_security_group=platform.service_access_sg.security_group_id,
                keep_job_flow_alive_when_no_steps=True,
                termination_protected=False,
                master_instance_group=group("Primary", 1, config.primary_instance_type, config.primary_market),
                core_instance_group=group("Core", config.core_instance_count, config.core_instance_type, config.core_market),
            ),
            auto_termination_policy=emr.CfnCluster.AutoTerminationPolicyProperty(
                idle_timeout=config.idle_timeout_seconds
            ),
            bootstrap_actions=[
                emr.CfnCluster.BootstrapActionConfigProperty(
                    name="install-spark-" + config.spark_version,
                    script_bootstrap_action=emr.CfnCluster.ScriptBootstrapActionConfigProperty(
                        path=bootstrap_uri,
                        args=[bundle_uri],
                    ),
                )
            ],
            tags=[
                cdk.CfnTag(key="for-use-with-amazon-emr-managed-policies", value="true"),
            ],
        )
        cluster.node.add_dependency(platform.instance_profile)

        cdk.CfnOutput(self, "ClusterId", value=cluster.ref)
