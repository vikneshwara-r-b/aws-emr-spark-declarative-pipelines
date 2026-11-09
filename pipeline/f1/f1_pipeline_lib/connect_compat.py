"""Works around a Spark 4.3.0 Spark Connect client bug that crashes `spark-pipelines run` AFTER a successful run.

Symptom (the step ends with exit code 1 although the log says "Run is COMPLETED"):

    pyspark.errors.exceptions.base.PySparkValueError: [UNKNOWN_RESPONSE] Unknown response:
    Plan observed(__python_accumulator__=[binary: ...

Cause: when a Python worker ran during the update (here: the `f1_api` Python data source), the server attaches
"observed metrics" named `__python_accumulator__` to its responses. `SparkConnectClient._execute_and_fetch_as_iterator`
yields them as PlanObservedMetrics objects (after applying them itself), but `execute_command_as_iterator`, which
`spark-pipelines` uses to stream pipeline events, only accepts dict responses and raises UNKNOWN_RESPONSE for anything else.

Fix: the same method with those already-handled metrics skipped. It is installed from the pipeline definition files,
which `spark-pipelines` imports in the same Python process that later streams the events. Remove this when the
bundled Spark has the upstream fix.
"""
from __future__ import annotations


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
