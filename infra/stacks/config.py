from __future__ import annotations

from dataclasses import dataclass

from constructs import Node


@dataclass(frozen=True)
class DeploymentConfig:
    project: str
    region: str
    prefix: str
    glue_database: str
    emr_release_label: str
    spark_version: str
    primary_instance_type: str
    core_instance_type: str
    core_instance_count: int
    primary_market: str
    core_market: str
    idle_timeout_seconds: int
    vpc_cidr: str
    owner_tag: str

    @classmethod
    def from_context(cls, node: Node) -> "DeploymentConfig":
        def get(key: str):
            value = node.try_get_context(key)
            if value is None:
                raise ValueError(f"missing CDK context key '{key}' (see infra/cdk.json)")
            return value

        if int(get("core_instance_count")) < 1:
            raise ValueError("core_instance_count must be at least 1")

        for key in ("primary_market", "core_market"):
            if str(get(key)).upper() not in ("ON_DEMAND", "SPOT"):
                raise ValueError(f"{key} must be ON_DEMAND or SPOT")

        return cls(
            project=get("project"),
            region=get("region"),
            prefix=get("prefix").strip("/"),
            glue_database=get("glue_database"),
            emr_release_label=get("emr_release_label"),
            spark_version=get("spark_version"),
            primary_instance_type=get("primary_instance_type"),
            core_instance_type=get("core_instance_type"),
            core_instance_count=int(get("core_instance_count")),
            primary_market=str(get("primary_market")).upper(),
            core_market=str(get("core_market")).upper(),
            idle_timeout_seconds=int(get("idle_timeout_seconds")),
            vpc_cidr=get("vpc_cidr"),
            owner_tag=get("owner_tag"),
        )

    # -- derived names, defined once so stacks and scripts agree -----------------
    @property
    def ecr_repo_name(self) -> str:
        return f"{self.project}-builder"

    @property
    def bundle_key(self) -> str:
        return f"{self.prefix}/bundles/spark-{self.spark_version}-bundle.tar.gz"

    @property
    def bootstrap_prefix(self) -> str:
        return f"{self.prefix}/bootstrap"
