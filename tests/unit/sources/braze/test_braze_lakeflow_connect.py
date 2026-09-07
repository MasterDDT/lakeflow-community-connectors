"""Tests for the Braze LakeflowConnect connector.

Runs against the in-process source simulator described by
``source_simulator/specs/braze/`` (``endpoints.yaml`` + ``corpus/``). No live
credentials and no network: the simulator stands in for the Braze REST
Export & Analytics API and serves the bootstrapped corpus with real
filter / sort / paginate semantics.

Braze extends both ``LakeflowConnect`` and ``SupportsPartitionedStream``:

  * Snapshot list streams (``/campaigns/list`` …) and details fan-out streams
    (``/…/details``) are NOT partitioned — they use ``read_table``.
  * Date-windowed analytics streams (fan-out ``/…/data_series`` and
    workspace-level ``/kpi/*`` / ``/purchases/*`` series) ARE partitioned —
    ``get_partitions`` fans the read out and ``read_partition`` runs per
    partition. ``latest_offset`` returns a frozen init-time boundary so the
    micro-batch loop converges (Trigger.AvailableNow termination).

The stand-in credentials below are values of the right shape; the simulator
never validates them.
"""

from __future__ import annotations

import json

import pytest
from pyspark.sql.types import StringType

from databricks.labs.community_connector.sources.braze.braze import (
    BrazeLakeflowConnect,
)
from tests.unit.sources.test_partition_suite import (
    SupportsPartitionedStreamTests,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestBrazeConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = BrazeLakeflowConnect
    simulator_source = "braze"

    # Stand-in credentials. ``__init__`` makes no HTTP calls (it only parses
    # options and freezes the init-time boundary), and the simulator ignores
    # auth, so any values of the right shape work.
    replay_config = {
        "rest_endpoint": "https://rest.iad-01.braze.com",
        "api_key": "test-api-key",
    }

    # ``sends_analytics`` fans out over (campaign_id, send_id) pairs. ``send_id``
    # is not exposed by ``/campaigns/list`` (a documented Braze quirk), so
    # without explicit pairs the stream produces no partitions. Supply pairs
    # via the ``send_ids`` table option (comma-separated ``campaign_id:send_id``
    # tokens) so the partitioned-read tests have parents to fan out over.
    # The pair below is a real send registered on the EU test workspace via
    # /sends/id/create + /campaigns/trigger/send (campaign "Test Email Campaign"),
    # so /sends/data_series returns live data in record mode. The simulator
    # ignores the value in simulate mode and serves the corpus.
    table_configs = {
        "sends_analytics": {
            "send_ids": "21a019c7-e024-4790-b8ae-76622c9c3e09:lakeflow_test_send_001",
        },
    }

    # Lifecycle-timestamp columns the live source genuinely returns as null for
    # the recorded corpus record. ``/…/details`` are ``single_entity`` streams,
    # so the corpus holds exactly one detail object per stream (the one recorded
    # from the live workspace). The recorded campaign was never sent
    # (``first_sent``/``last_sent`` = null) and the recorded Canvas is an unsent
    # draft (``first_entry``/``last_entry`` = null); these fields only populate
    # once the entity has been sent/entered. No corpus record can exercise them,
    # so they are exempt from the column-population invariant.
    allow_null_columns = {
        "campaigns_details": {"first_sent", "last_sent"},
        "canvases_details": {"first_entry", "last_entry"},
    }

    # ------------------------------------------------------------------
    # messages-column JSON-encoding contract (braze-specific)
    # ------------------------------------------------------------------

    def _collect_records(self, table: str, cap: int = 200) -> list[dict]:
        """Read up to ``cap`` records from ``table`` via whichever read path
        it uses — partitioned fan-out (``get_partitions`` + ``read_partition``)
        or the non-partitioned ``read_table``."""
        opts = self._opts(table)
        out: list[dict] = []
        if self._is_partitioned(table):
            for partition in self.connector.get_partitions(table, opts):
                for rec in self.connector.read_partition(table, partition, opts):
                    out.append(rec)
                    if len(out) >= cap:
                        return out
        else:
            iterator, _ = self.connector.read_table(table, {}, opts)
            for rec in iterator:
                out.append(rec)
                if len(out) >= cap:
                    break
        return out

    def test_messages_column_is_valid_json(self):
        """Every non-null ``messages`` value is a JSON-encoded object/array.

        Braze nests per-channel / per-message metrics in a ``messages`` column
        that the connector declares as ``StringType`` and populates via
        ``json.dumps`` (see ``braze.py``). The harness's structural tests check
        that the column is populated, but not that its contents round-trip as
        JSON. This asserts the encoding contract directly: each non-null value
        must be a ``str`` that parses to a JSON object or array — guarding
        against a regression that yields a raw ``dict`` / ``list`` (a Spark
        schema mismatch) or emits malformed JSON. Discovered from the schema,
        so it auto-covers any table that later gains a ``messages`` column
        (today: ``campaigns_details``, ``campaigns_analytics``,
        ``sends_analytics``).
        """
        targets = [
            t for t in self._tables()
            if any(
                f.name == "messages" and isinstance(f.dataType, StringType)
                for f in self.connector.get_table_schema(t, self._opts(t)).fields
            )
        ]
        if not targets:
            pytest.skip("No table declares a StringType `messages` column")

        seen = 0
        errors = []
        for table in targets:
            for rec in self._collect_records(table):
                value = rec.get("messages")
                if value is None:
                    continue
                seen += 1
                if not isinstance(value, str):
                    errors.append(
                        f"[{table}] messages is {type(value).__name__}, expected a "
                        "JSON string (StringType). Fix: json.dumps() before yielding."
                    )
                    continue
                try:
                    parsed = json.loads(value)
                except (ValueError, TypeError) as exc:
                    errors.append(
                        f"[{table}] messages is not valid JSON: {exc} "
                        f"— value={value[:200]!r}"
                    )
                    continue
                if not isinstance(parsed, (dict, list)):
                    errors.append(
                        f"[{table}] messages decodes to {type(parsed).__name__}, "
                        "expected a JSON object or array."
                    )
        if errors:
            pytest.fail("\n\n".join(errors))
        assert seen > 0, (
            "No non-null `messages` values were produced by any messages-bearing "
            "stream; the JSON-encoding contract went unexercised. Re-seed the "
            "corpus so at least one record populates `messages`."
        )
