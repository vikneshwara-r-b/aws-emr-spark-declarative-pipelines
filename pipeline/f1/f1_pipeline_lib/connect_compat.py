"""Works around a Spark 4.3.0 Spark Connect client bug that crashes `spark-pipelines run` AFTER a successful run.

Symptom (the step ends with exit code 1 although the log says "Run is COMPLETED"):

    pyspark.errors.exceptions.base.PySparkValueError: [UNKNOWN_RESPONSE] Unknown response:
    Plan observed(__python_accumulator__=[binary: ...

Cause: when a Python worker ran during the update (here: the `f1_api` Python data source), the server attaches
"observed metrics" named `__python_accumulator__` to its responses. `SparkConnectClient._execute_and_fetch_as_iterator`
yields them as PlanObservedMetrics objects (after applying them itself), but `execute_command_as_iterator`, which
`spark-pipelines` uses to stream pipeline events, only accepts dict responses and raises UNKNOWN_RESPONSE for anything else.

Fix: the same method with those already-handled metrics skipped. It is installed from the pipeline definition files,
which `spark-pipelines` imports in the same Python process that later streams the events.

Upstream: fixed by apache/spark PR #58435 (SPARK-59135, "Handle metadata in command response streams"), merged to
master on 2026-09-11. As of 2026-11-10 it is NOT in Spark 4.3.0 (the release this project bundles) and there is no
4.3.1 yet, so the patch is still needed. `install()` switches itself off when the client no longer raises UNKNOWN_RESPONSE.

Removal: once the bundled Spark contains SPARK-59135, run one race with this patch disabled and check that the step exits
0; then delete this module, its test, and the `connect_compat.install()` call in transformations/01_bronze_raw.py.
Not verified: whether the `spark-pipelines` event loop copes with the non-dict items a fixed client yields.
"""
from __future__ import annotations

import inspect


def _still_has_the_bug(method) -> bool:
    """True unless the method's source is readable and no longer raises UNKNOWN_RESPONSE (i.e. Spark fixed it).
    When the source cannot be read, assume the bug is present: that is the Spark 4.3.0 behaviour."""
    try:
        return "UNKNOWN_RESPONSE" in inspect.getsource(method)
    except (OSError, TypeError):
        return True


def install(core=None) -> bool:
    """Patches SparkConnectClient.execute_command_as_iterator. Returns True when patched, False when it was not
    needed or not possible (an unexpected Spark layout must not break the pipeline: it just keeps the old behaviour)."""
    try:
        if core is None:
            from pyspark.sql.connect.client import core
        client_cls = core.SparkConnectClient
        observed_type = core.PlanObservedMetrics
        original = client_cls.execute_command_as_iterator
    except (ImportError, AttributeError):
        return False
    if getattr(original, "_f1_skips_observed_metrics", False):
        return False
    if not _still_has_the_bug(original):
        return False  # the bundled Spark already yields every response: leave the fixed client alone

    def execute_command_as_iterator(self, command, observations=None):
        req = self._execute_plan_request_with_metadata()
        self._set_command_in_plan(req.plan, command)
        for response in self._execute_and_fetch_as_iterator(req, observations or {}):
            if isinstance(response, dict):
                yield response
            elif isinstance(response, observed_type):
                continue  # already applied by _execute_and_fetch_as_iterator; not a pipeline event
            else:
                from pyspark.errors import PySparkValueError

                raise PySparkValueError(errorClass="UNKNOWN_RESPONSE", messageParameters={"response": str(response)})

    execute_command_as_iterator._f1_skips_observed_metrics = True
    client_cls.execute_command_as_iterator = execute_command_as_iterator
    return True
