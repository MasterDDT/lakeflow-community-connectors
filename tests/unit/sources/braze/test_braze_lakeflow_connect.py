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
