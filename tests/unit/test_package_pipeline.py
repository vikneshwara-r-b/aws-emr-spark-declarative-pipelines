from pathlib import Path

from scripts.package_pipeline import render_package

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PIPELINE_SRC_DIR = REPO_ROOT / "pipeline" / "f1"


def _render(out_dir):
    return render_package(
        bucket="my-bucket",
        prefix="my-prefix",
        database="my_db",
        api_base_url="https://api.jolpi.ca/ergast/f1",
        src_dir=PIPELINE_SRC_DIR,
        out_dir=out_dir,
    )


def test_render_package_generates_manifest_from_props(tmp_path):
    out_dir = tmp_path / "package"
    _render(out_dir)

    manifest_text = (out_dir / "spark-pipeline.yml").read_text()
    assert "database: my_db" in manifest_text
    assert "catalog: glue_catalog" in manifest_text
    assert "storage: s3://my-bucket/my-prefix/state/" in manifest_text
    assert 'spark.sql.catalog.glue_catalog.warehouse: "s3://my-bucket/my-prefix/warehouse"' in manifest_text
    assert 'spark.f1.api.base_url: "https://api.jolpi.ca/ergast/f1"' in manifest_text
    assert 'spark.f1.api.user_agent: "aws-emr-spark-declarative-pipelines/1.0"' in manifest_text
    assert "staging" not in manifest_text  # the raw zone reads the API directly, nothing is staged in S3
    assert (
        'spark.f1.reference.circuit_track_type.path: '
        '"s3://my-bucket/my-prefix/reference-data/circuit_track_type/"'
    ) in manifest_text
    # _sys_path_bootstrap.py's own glob entry must precede transformations/**'s
    # so SDP execs it first (see spark-pipeline.yml's comment + that file's
    # docstring) -- this ordering is what the whole mechanism depends on.
    bootstrap_idx = manifest_text.index("include: _sys_path_bootstrap.py")
    transformations_idx = manifest_text.index("include: transformations/**")
    assert bootstrap_idx < transformations_idx


def test_render_package_root_layout(tmp_path):
    out_dir = tmp_path / "package"
    _render(out_dir)

    assert (out_dir / "spark-pipeline.yml").is_file()
    assert (out_dir / "_sys_path_bootstrap.py").is_file()
    assert (out_dir / "transformations").is_dir()
    assert (out_dir / "transformations" / "00_bronze_circuit_track_type.py").is_file()
    assert (out_dir / "transformations" / "01_bronze_raw.py").is_file()
    assert (out_dir / "transformations" / "02_bronze_prepared.py").is_file()
    assert (out_dir / "transformations" / "03_curated.py").is_file()
    assert (out_dir / "transformations" / "04_gold.sql").is_file()
    assert (out_dir / "f1_pipeline_lib").is_dir()
    assert (out_dir / "f1_pipeline_lib" / "schema.py").is_file()
    assert (out_dir / "f1_pipeline_lib" / "f1_source.py").is_file()


def test_render_package_transformation_files_carry_no_deploy_literals(tmp_path):
    # Bucket/prefix/database values reach bronze only via spark.conf (the rendered
    # manifest's `configuration:` block), never as literals baked into the copied
    # transformation/library files.
    out_dir = tmp_path / "package"
    _render(out_dir)

    for path in (out_dir / "transformations").glob("*"):
        if path.suffix == ".py":
            text = path.read_text()
            assert "getResolvedOptions" not in text
            assert "my-bucket" not in text
    for path in (out_dir / "f1_pipeline_lib").glob("*.py"):
        assert "my-bucket" not in path.read_text()


def test_render_package_is_idempotent(tmp_path):
    out_dir = tmp_path / "package"
    _render(out_dir)
    _render(out_dir)

    manifest_text = (out_dir / "spark-pipeline.yml").read_text()
    # Re-rendering must not duplicate or stack substitutions. "my-bucket" appears
    # 3 times in a correctly-rendered manifest: storage, Iceberg warehouse, and the
    # circuit_track_type reference data path.
    assert manifest_text.count("my-bucket") == 3
