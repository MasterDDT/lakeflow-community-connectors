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
    table_configs = {
        "sends_analytics": {"send_ids": "1001:s1,1002:s2"},
    }
