"""connect_compat.install(): the Spark 4.3.0 spark-pipelines client crash on __python_accumulator__ observed metrics."""
from types import SimpleNamespace

import pytest

from f1_pipeline_lib import connect_compat


class PlanObservedMetrics:
    """Stands in for pyspark.sql.connect.client.core.PlanObservedMetrics."""

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return f"Plan observed({self.name}=[...])"


def fake_core(responses):
    class Client:
        def _execute_plan_request_with_metadata(self):
            return SimpleNamespace(plan=SimpleNamespace())

        def _set_command_in_plan(self, plan, command):
            plan.command = command

        def _execute_and_fetch_as_iterator(self, req, observations):
            yield from responses

        def execute_command_as_iterator(self, command, observations=None):
            # the Spark 4.3.0 behaviour: anything that is not a dict is an error
            for response in self._execute_and_fetch_as_iterator(self._execute_plan_request_with_metadata(), {}):
                if isinstance(response, dict):
                    yield response
                else:
                    raise ValueError(f"UNKNOWN_RESPONSE {response}")

    return SimpleNamespace(SparkConnectClient=Client, PlanObservedMetrics=PlanObservedMetrics)


EVENTS = [{"pipeline_event_result": 1}, PlanObservedMetrics("__python_accumulator__"), {"pipeline_event_result": 2}]


def test_the_unpatched_client_crashes_on_observed_metrics():
    core = fake_core(EVENTS)
    with pytest.raises(ValueError, match="UNKNOWN_RESPONSE"):
        list(core.SparkConnectClient().execute_command_as_iterator("cmd"))


def test_patched_client_yields_the_pipeline_events_and_skips_observed_metrics():
    core = fake_core(EVENTS)
    assert connect_compat.install(core) is True
    assert list(core.SparkConnectClient().execute_command_as_iterator("cmd")) == [
        {"pipeline_event_result": 1},
        {"pipeline_event_result": 2},
    ]


def test_installing_twice_patches_once():
    core = fake_core(EVENTS)
    assert connect_compat.install(core) is True
    assert connect_compat.install(core) is False


def test_other_unexpected_responses_still_raise():
    from pyspark.errors import PySparkValueError

    core = fake_core([{"a": 1}, "something else"])
    connect_compat.install(core)
    with pytest.raises(PySparkValueError, match="UNKNOWN_RESPONSE"):
        list(core.SparkConnectClient().execute_command_as_iterator("cmd"))


def test_a_spark_that_already_has_the_upstream_fix_is_left_alone():
    # SPARK-59135 (fixed on master): execute_command_as_iterator yields every response and never raises UNKNOWN_RESPONSE
    class FixedClient:
        def _execute_and_fetch_as_iterator(self, req, observations):
            yield from EVENTS

        def execute_command_as_iterator(self, command, observations=None):
            yield from self._execute_and_fetch_as_iterator(None, observations or {})

    core = SimpleNamespace(SparkConnectClient=FixedClient, PlanObservedMetrics=PlanObservedMetrics)
    original = FixedClient.execute_command_as_iterator
    assert connect_compat.install(core) is False
    assert FixedClient.execute_command_as_iterator is original
    assert len(list(FixedClient().execute_command_as_iterator("cmd"))) == 3  # the metadata item still reaches the caller


def test_unreadable_source_still_gets_the_patch_because_4_3_0_has_the_bug():
    core = fake_core(EVENTS)
    original = core.SparkConnectClient.execute_command_as_iterator
    # builtins have no Python source: inspect raises TypeError, which must count as "bug present"
    assert connect_compat._still_has_the_bug(len) is True
    assert connect_compat._still_has_the_bug(original) is True


def test_an_unexpected_spark_layout_keeps_the_old_behaviour_instead_of_failing():
    assert connect_compat.install(SimpleNamespace()) is False


def test_raw_zone_installs_the_fix_before_registering_the_data_source():
    from pathlib import Path

    text = (Path(__file__).resolve().parents[2] / "pipeline/f1/transformations/01_bronze_raw.py").read_text()
    assert "connect_compat.install()" in text
    assert text.index("connect_compat.install()") < text.index("register_data_source(SparkSession.active())")
