"""Makes f1_pipeline_lib importable from every transformation file, in one place.

SDP's registration loop (pyspark/pipelines/cli.py::register_definitions) execs
every .py file matched by the manifest's `libraries:` glob entries, in the
order those entries appear in spark-pipeline.yml, via raw
importlib.util.spec_from_file_location -- which does NOT add the executed
file's directory to sys.path. Without this, each transformation file that
imports from f1_pipeline_lib needs its own sys.path fix (confirmed necessary:
a real Glue run failed with ModuleNotFoundError: No module named
'f1_pipeline_lib' both without any fix, and when relying on Glue's
--extra-py-files job parameter instead -- that mechanism doesn't propagate to
the SDP CLI's subprocess either).

This file has its own dedicated `libraries:` glob entry in spark-pipeline.yml,
listed BEFORE the transformations/** entry, so it always execs first -- making
this the one place sys.path is fixed, instead of every transformation file
repeating it. It deliberately imports nothing from f1_pipeline_lib itself
(only stdlib), so there's no chicken-and-egg ordering problem.
"""
import sys
from pathlib import Path

_ZIP_ROOT = Path(__file__).resolve().parent
if str(_ZIP_ROOT) not in sys.path:
    sys.path.insert(0, str(_ZIP_ROOT))
