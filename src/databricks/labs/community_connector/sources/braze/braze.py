"""Braze source connector (read-only).

Reads data FROM Braze into Databricks via the Braze REST Export & Analytics
APIs. Implements ``LakeflowConnect`` plus ``SupportsPartitionedStream`` so the
date-windowed analytics streams parallelise across Spark executors.

Auth
    ``Authorization: Bearer <api_key>`` header. The single required secret is
    the Braze REST API key (``api_key``; ``access_token`` / ``token`` are
    accepted aliases). Braze is multi-tenant: the instance REST base URL
    (e.g. ``https://rest.iad-01.braze.com``) is a REQUIRED connection option
    (``rest_endpoint``; ``base_url`` / ``instance_url`` aliases).

Stream shapes (see ``braze_schemas.py`` for the full catalogue):
    * snapshot_list    — paginated list endpoints (``/campaigns/list`` …),
      full-refresh snapshots. Not partitioned; read on the single driver.
    * details          — two-step fan-out (list ids -> ``/…/details`` once per
      id), snapshots. Not partitioned.
    * fanout_analytics — ``/…/data_series`` fanned out per parent id/name; one
      partition per parent, run in parallel.
    * workspace_series — workspace-level ``/kpi/*`` and ``/purchases/*``
      series; a single date-windowed partition.

Why partitioned?
    The analytics/data_series endpoints accept range queries (``length`` days +
    ``ending_at``). ``latest_offset`` returns a fixed init-time snapshot
    (midnight UTC today, so only complete days are ingested and consecutive
    intraday triggers can't double-count a partial day), and ``get_partitions``
    fans the read out per parent. This is the parallel evolution of the
    sliding-time-window pattern.

Termination (Trigger.AvailableNow)
    ``latest_offset`` is a constant frozen at ``__init__`` (``self._until_iso``).
    The first micro-batch reads ``(since, until]`` and commits ``until``; the
    next micro-batch sees ``latest_offset == committed`` and the trigger stops.
    Snapshot streams return offset ``{}`` and terminate after one emitted batch.
"""

import json
import math
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Sequence

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.braze.braze_schemas import (
    DEPRECATED_TABLES,
    DETAILS_STREAMS,
    FANOUT_STREAMS,
    INITIAL_BACKOFF,
    KIND_DETAILS,
    KIND_FANOUT_ANALYTICS,
    KIND_SNAPSHOT_LIST,
    KIND_WORKSPACE_SERIES,
    LIST_STREAMS,
    MAX_RETRIES,
    MAX_WINDOW_DAYS,
    REQUEST_TIMEOUT,
    RETRIABLE_STATUS_CODES,
    SENDS_DATA_PATH,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    WORKSPACE_STREAMS,
    table_kind,
)

# Fields whose value is a genuinely dynamic nested object/array (no fixed Spark
# shape) and therefore stored as a JSON-encoded string column. See
# braze_schemas.py for the rationale.
_COMPLEX_JSON_FIELDS: dict[str, tuple[str, ...]] = {
    "campaigns_details": ("messages", "conversion_behaviors"),
    "campaigns_analytics": ("messages",),
    "canvases_details": ("variants", "steps"),
    "canvases_analytics": ("variant_stats", "step_stats"),
    "sends_analytics": ("messages",),
}

# Fields kept as an explicit StructType — coerce an empty/absent object to None
# (an empty dict is illegal for a Spark StructType field).
_STRUCT_FIELDS: dict[str, tuple[str, ...]] = {
    "canvases_analytics": ("total_stats",),
}

# Safety cap on list-endpoint pagination so a misbehaving source can never spin
# forever. 100k pages * 100/page is far beyond any real Braze workspace.
_MAX_LIST_PAGES = 100_000

# Optional passthrough query params, restricted to the endpoints that document
# each (see _apply_optional_params).
_APP_ID_TABLES = frozenset(
    {
        "events_analytics",
        "kpi_daily_new_users",
        "kpi_daily_active_users",
        "kpi_monthly_active_users",
        "kpi_daily_app_uninstalls",
        "purchases_quantity_series",
        "purchases_revenue_series",
    }
)
_UNIT_TABLES = frozenset(
    {"events_analytics", "purchases_quantity_series", "purchases_revenue_series"}
)
_PRODUCT_TABLES = frozenset(
    {"purchases_quantity_series", "purchases_revenue_series"}
)


class BrazeLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect + SupportsPartitionedStream implementation for Braze."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        base = (
            options.get("rest_endpoint")
            or options.get("base_url")
            or options.get("instance_url")
        )
        if not base:
            raise ValueError(
                "Braze connector requires 'rest_endpoint' — the instance REST "
                "base URL (e.g. 'https://rest.iad-01.braze.com'). Find it in the "
                "Braze dashboard under Settings > APIs and Identifiers."
            )
        self._base_url = base.rstrip("/")

        api_key = (
            options.get("api_key")
            or options.get("access_token")
            or options.get("token")
        )
        if not api_key:
            raise ValueError(
                "Braze connector requires 'api_key' — a Braze REST API key "
                "(created under Settings > APIs and Identifiers > Rest API Keys)."
            )
        self._api_key = api_key

        self._include_deprecated = str(
            options.get("include_deprecated_streams", "true")
        ).strip().lower() in ("true", "1", "yes")

        # Freeze the upper bound at init time: midnight (UTC) of today. Using
        # the day boundary (not wall-clock now) means only COMPLETE days are
        # ingested, so consecutive intraday triggers never re-emit a partial
        # day's analytics (append tables have no upsert to dedup them). It is
        # also the constant returned by ``latest_offset`` that lets a
        # Trigger.AvailableNow micro-batch loop terminate.
        now = datetime.now(timezone.utc)
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        self._until_iso = midnight.strftime("%Y-%m-%dT%H:%M:%SZ")

    # ------------------------------------------------------------------ #
    # LakeflowConnect interface
    # ------------------------------------------------------------------ #

    def list_tables(self) -> list[str]:
        tables = list(SUPPORTED_TABLES)
        if not self._include_deprecated:
            tables = [t for t in tables if t not in DEPRECATED_TABLES]
        return tables

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Single-driver read.

        Used by ``simpleStreamReader`` for the non-partitioned snapshot/details
        tables, and as a safe direct-call path for the partitioned append
        tables. Snapshot/details return offset ``{}`` (one full-refresh batch);
        append tables drain ``(since, until]`` and return ``{"cursor": until}``.
        """
        self._validate_table(table_name)
        kind = table_kind(table_name)
        if kind == KIND_SNAPSHOT_LIST:
            return self._read_list(table_name, table_options), {}
        if kind == KIND_DETAILS:
            return self._read_details(table_name, table_options), {}
        return self._read_analytics_batch(table_name, start_offset, table_options)

    # ------------------------------------------------------------------ #
    # SupportsPartitionedStream interface
    # ------------------------------------------------------------------ #

    def is_partitioned(self, table_name: str) -> bool:
        """Only date-windowed analytics streams are partitioned.

        Snapshot list and details streams have no server-side range query to
        split, so they fall back to ``simpleStreamReader`` (``read_table``).
        """
        if table_name not in SUPPORTED_TABLES:
            return False
        return table_kind(table_name) in (
            KIND_FANOUT_ANALYTICS,
            KIND_WORKSPACE_SERIES,
        )

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Return the init-time snapshot boundary (constant across the run).

        Returning a fixed value is what guarantees termination: once the
        committed offset reaches it, ``latest_offset == committed`` and the
        trigger stops. The next trigger builds a fresh connector with a newer
        boundary and ingests the delta.
        """
        self._validate_table(table_name)
        return {"cursor": self._until_iso}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        """Return partition descriptors.

        * Snapshot list / details tables: a single ``{"full": True}`` descriptor
          — the batch reader routes it to a full ``read_partition`` read (these
          tables never reach here on the streaming path; ``is_partitioned`` is
          False).
        * Append tables (batch: no offsets; streaming: ``(start, end]``): one
          partition per parent (fan-out) or a single windowed partition
          (workspace). Empty when there is no new data.
        """
        self._validate_table(table_name)
        kind = table_kind(table_name)
        if kind in (KIND_SNAPSHOT_LIST, KIND_DETAILS):
            return [{"full": True}]

        # Streaming quiescence: no new data since the last commit.
        if (
            start_offset is not None
            and end_offset is not None
            and start_offset == end_offset
        ):
            return []

        window = self._resolve_window(
            table_name, start_offset, end_offset, table_options
        )
        if window is None:
            return []
        since_iso, until_iso = window
        length = self._window_length(
            since_iso, until_iso, MAX_WINDOW_DAYS[table_name]
        )

        if table_name == "sends_analytics":
            return self._sends_partitions(length, until_iso, table_options)
        if kind == KIND_FANOUT_ANALYTICS:
            return self._fanout_partitions(table_name, length, until_iso, table_options)
        # workspace series -> a single windowed partition.
        return [{"length": length, "ending_at": until_iso}]

    def read_partition(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        """Read the records for one partition (runs on an executor)."""
        self._validate_table(table_name)
        kind = table_kind(table_name)
        if kind == KIND_SNAPSHOT_LIST:
            return self._read_list(table_name, table_options)
        if kind == KIND_DETAILS:
            return self._read_details(table_name, table_options)
        if table_name == "sends_analytics":
            return self._read_sends_partition(partition, table_options)
        if kind == KIND_FANOUT_ANALYTICS:
            return self._read_fanout_partition(table_name, partition, table_options)
        return self._read_workspace_partition(table_name, partition, table_options)

    # ------------------------------------------------------------------ #
    # Snapshot list reads
    # ------------------------------------------------------------------ #

    def _iter_list_items(
        self, list_table: str, table_options: dict[str, str]
    ) -> Iterator[Any]:
        """Yield raw items from a paginated list endpoint (0-indexed page).

        Stops on an empty page OR on the first short (< page_size) page — the
        latter is the documented last-page signal and also keeps the connector
        from issuing a redundant trailing page request.
        """
        cfg = LIST_STREAMS[list_table]
        path = cfg["path"]
        records_key = cfg["records_key"]
        page_size = cfg["page_size"]
        page = 0
        while page < _MAX_LIST_PAGES:
            body = self._get_json(path, params={"page": page})
            items = body.get(records_key) if isinstance(body, dict) else None
            if not items:
                break
            yield from items
            if len(items) < page_size:
                break
            page += 1

    def _read_list(
        self, table_name: str, table_options: dict[str, str]
    ) -> Iterator[dict]:
        cfg = LIST_STREAMS[table_name]
        records: list[dict] = []
        if cfg.get("string_items"):
            field = cfg["string_field"]
            for item in self._iter_list_items(table_name, table_options):
                # Real API returns bare strings; the simulator corpus holds
                # {field: value} dicts — accept both.
                records.append(item if isinstance(item, dict) else {field: item})
        else:
            for item in self._iter_list_items(table_name, table_options):
                if isinstance(item, dict):
                    records.append(item)
        return iter(records)

    def _discover_parents(
        self, list_table: str, table_options: dict[str, str]
    ) -> list[str]:
        """Return the parent ids/names driving a fan-out."""
        cfg = LIST_STREAMS[list_table]
        ids: list[str] = []
        if cfg.get("string_items"):
            field = cfg["string_field"]
            for item in self._iter_list_items(list_table, table_options):
                value = item if isinstance(item, str) else item.get(field)
                if value:
                    ids.append(value)
        else:
            for item in self._iter_list_items(list_table, table_options):
                value = item.get("id") if isinstance(item, dict) else item
                if value:
                    ids.append(value)
        return ids

    # ------------------------------------------------------------------ #
    # Details fan-out reads
    # ------------------------------------------------------------------ #

    def _read_details(
        self, table_name: str, table_options: dict[str, str]
    ) -> Iterator[dict]:
        cfg = DETAILS_STREAMS[table_name]
        id_param = cfg["id_param"]
        id_field = cfg["id_field"]
        json_fields = _COMPLEX_JSON_FIELDS.get(table_name, ())
        records: list[dict] = []
        for parent_id in self._discover_parents(cfg["list_table"], table_options):
            body = self._get_json(cfg["detail_path"], params={id_param: parent_id})
            # Real API: a single detail object at the top level (plus a
            # "message" envelope field). Simulator corpus: a JSON array — take
            # the first element and stamp the parent id so each fanned-out call
            # yields a distinct primary key.
            detail = body[0] if isinstance(body, list) and body else body
            if not isinstance(detail, dict):
                continue
            rec = dict(detail)
            rec[id_field] = parent_id
            for f in json_fields:
                if f in rec:
                    rec[f] = _json_str(rec[f])
            records.append(rec)
        return iter(records)

    # ------------------------------------------------------------------ #
    # Fan-out analytics reads
    # ------------------------------------------------------------------ #

    def _fanout_partitions(
        self,
        table_name: str,
        length: int,
        until_iso: str,
        table_options: dict[str, str],
    ) -> list[dict]:
        cfg = FANOUT_STREAMS[table_name]
        parents = self._discover_parents(cfg["list_table"], table_options)
        return [
            {"id": pid, "length": length, "ending_at": until_iso} for pid in parents
        ]

    def _read_fanout_partition(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        cfg = FANOUT_STREAMS[table_name]
        parent_id = partition["id"]
        params = {
            cfg["id_param"]: parent_id,
            "length": partition["length"],
            "ending_at": partition["ending_at"],
        }
        self._apply_optional_params(table_name, params, table_options)
        body = self._get_json(cfg["data_path"], params=params)
        stamp = {cfg["id_field"]: parent_id}
        records = _extract_records(body, cfg["records_key"])
        return iter(
            self._shape_analytics(table_name, raw, stamp) for raw in records
        )

    # ------------------------------------------------------------------ #
    # Workspace-level series reads
    # ------------------------------------------------------------------ #

    def _read_workspace_partition(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        cfg = WORKSPACE_STREAMS[table_name]
        params = {
            "length": partition["length"],
            "ending_at": partition["ending_at"],
        }
        self._apply_optional_params(table_name, params, table_options)
        body = self._get_json(cfg["data_path"], params=params)
        records = _extract_records(body, cfg["records_key"])
        # Workspace series carry only scalar fields — pass through as dicts.
        return iter(dict(raw) for raw in records if isinstance(raw, dict))

    # ------------------------------------------------------------------ #
    # sends_analytics (fan-out over campaign_id + send_id pairs)
    # ------------------------------------------------------------------ #

    def _sends_partitions(
        self, length: int, until_iso: str, table_options: dict[str, str]
    ) -> list[dict]:
        pairs = self._discover_send_pairs(table_options)
        return [
            {
                "campaign_id": campaign_id,
                "send_id": send_id,
                "length": length,
                "ending_at": until_iso,
            }
            for (campaign_id, send_id) in pairs
        ]

    def _discover_send_pairs(
        self, table_options: dict[str, str]
    ) -> list[tuple[str, str]]:
        """Resolve (campaign_id, send_id) pairs for ``/sends/data_series``.

        ``send_id`` is not exposed by ``/campaigns/list`` (Braze doc quirk #4),
        so unless the caller supplies pairs explicitly via the ``send_ids``
        table option (comma-separated ``campaign_id:send_id`` tokens), or a
        campaign record happens to carry a ``send_id`` field, no partitions are
        produced and the stream yields nothing — the documented behaviour when
        no send_id is available.
        """
        pairs: list[tuple[str, str]] = []
        raw = table_options.get("send_ids")
        if raw:
            for token in raw.split(","):
                token = token.strip()
                if ":" in token:
                    campaign_id, send_id = token.split(":", 1)
                    campaign_id, send_id = campaign_id.strip(), send_id.strip()
                    if campaign_id and send_id:
                        pairs.append((campaign_id, send_id))
        for item in self._iter_list_items("campaigns", table_options):
            if isinstance(item, dict) and item.get("send_id") and item.get("id"):
                pairs.append((item["id"], item["send_id"]))
        return pairs

    def _read_sends_partition(
        self, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        params = {
            "campaign_id": partition["campaign_id"],
            "send_id": partition["send_id"],
            "length": partition["length"],
            "ending_at": partition["ending_at"],
        }
        body = self._get_json(SENDS_DATA_PATH, params=params)
        stamp = {
            "campaign_id": partition["campaign_id"],
            "send_id": partition["send_id"],
        }
        records = _extract_records(body, "data")
        return iter(
            self._shape_analytics("sends_analytics", raw, stamp) for raw in records
        )

    # ------------------------------------------------------------------ #
    # read_table drain for append tables
    # ------------------------------------------------------------------ #

    def _read_analytics_batch(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Drain an append table's ``(since, until]`` window in one batch.

        Reuses ``get_partitions`` / ``read_partition`` so the single-driver path
        and the parallel path stay identical. Terminates when the committed
        cursor has reached the init-time boundary.
        """
        start_offset = start_offset or {}
        since_cursor = start_offset.get("cursor")
        if since_cursor and _cmp_iso(since_cursor, self._until_iso) >= 0:
            return iter([]), start_offset

        end_offset = {"cursor": self._until_iso}
        partitions = self.get_partitions(
            table_name, table_options, start_offset, end_offset
        )
        records: list[dict] = []
        for partition in partitions:
            records.extend(self.read_partition(table_name, partition, table_options))
        return iter(records), end_offset

    # ------------------------------------------------------------------ #
    # Record shaping
    # ------------------------------------------------------------------ #

    def _shape_analytics(
        self, table_name: str, raw: Any, stamp: dict[str, str]
    ) -> dict:
        """Stamp fan-out ids, JSON-encode dynamic objects, null empty structs."""
        rec = dict(raw) if isinstance(raw, dict) else {}
        rec.update(stamp)
        for f in _COMPLEX_JSON_FIELDS.get(table_name, ()):
            if f in rec:
                rec[f] = _json_str(rec[f])
        for f in _STRUCT_FIELDS.get(table_name, ()):
            value = rec.get(f)
            rec[f] = value if isinstance(value, dict) and value else None
        return rec

    @staticmethod
    def _apply_optional_params(
        table_name: str, params: dict, table_options: dict[str, str]
    ) -> None:
        """Attach optional passthrough filters, only where the endpoint documents them.

        ``app_id`` is accepted by /events, /kpi/* and /purchases/* series (not by
        the campaign/canvas/segment/card analytics endpoints); ``unit`` by
        /events and /purchases/* series; ``segment_id`` by /events; ``product``
        by /purchases/* series.
        """
        if table_name in _APP_ID_TABLES and table_options.get("app_id"):
            params["app_id"] = table_options["app_id"]
        if table_name in _UNIT_TABLES and table_options.get("unit"):
            params["unit"] = table_options["unit"]
        if table_name == "events_analytics" and table_options.get("segment_id"):
            params["segment_id"] = table_options["segment_id"]
        if table_name in _PRODUCT_TABLES and table_options.get("product"):
            params["product"] = table_options["product"]

    # ------------------------------------------------------------------ #
    # Window resolution
    # ------------------------------------------------------------------ #

    def _resolve_window(
        self,
        table_name: str,
        start_offset: dict | None,
        end_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[str, str] | None:
        """Resolve the ``(since, until]`` ISO window for an append read.

        ``until`` is the init-time boundary (capped). ``since`` is the committed
        cursor, or — on the first read — ``until`` minus ``history_days``
        (default: the endpoint's maximum window, so the first call is bounded
        and a single API request covers it). Returns None when the window is
        empty.
        """
        max_days = MAX_WINDOW_DAYS[table_name]
        history_days = _parse_int(table_options.get("history_days"), max_days, minimum=1)

        until_iso = (end_offset or {}).get("cursor") or self._until_iso
        if _cmp_iso(until_iso, self._until_iso) > 0:
            until_iso = self._until_iso

        start_cursor = (start_offset or {}).get("cursor")
        if start_cursor:
            since_iso = start_cursor
        else:
            since_iso = _format_iso(
                _parse_iso(self._until_iso) - timedelta(days=history_days)
            )

        if _cmp_iso(since_iso, until_iso) >= 0:
            return None
        return since_iso, until_iso

    @staticmethod
    def _window_length(since_iso: str, until_iso: str, max_days: int) -> int:
        """Whole-day ``length`` for one API call, clamped to [1, max_days]."""
        delta = _parse_iso(until_iso) - _parse_iso(since_iso)
        days = math.ceil(delta.total_seconds() / 86400.0)
        return max(1, min(days, max_days))

    # ------------------------------------------------------------------ #
    # HTTP layer
    # ------------------------------------------------------------------ #

    @property
    def _session(self) -> requests.Session:
        """Lazily-built session so the connector carries no non-picklable state.

        Created on first use (driver or executor). Holding no threading.Lock
        keeps the instance cheap to serialise to Spark executors.
        """
        session = getattr(self, "_session_obj", None)
        if session is None:
            session = requests.Session()
            session.headers.update(
                {
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }
            )
            self._session_obj = session
        return session

    def _request_with_retry(
        self, path: str, params: dict | None = None
    ) -> requests.Response:
        """GET with retry on 429/5xx, honouring ``X-RateLimit-Reset`` on 429."""
        url = f"{self._base_url}{path}"
        backoff = INITIAL_BACKOFF
        resp: requests.Response | None = None
        for attempt in range(MAX_RETRIES):
            resp = self._session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp
            if attempt < MAX_RETRIES - 1:
                time.sleep(self._retry_wait(resp, backoff))
                backoff *= 2
        return resp  # type: ignore[return-value]

    @staticmethod
    def _retry_wait(resp: requests.Response, backoff: float) -> float:
        """Seconds to wait before a retry.

        On 429 respect ``X-RateLimit-Reset`` (UTC epoch seconds), capped so a
        stale/absurd header can't stall a run; otherwise exponential backoff.
        """
        if resp.status_code == 429:
            reset = resp.headers.get("X-RateLimit-Reset")
            if reset:
                try:
                    delta = float(reset) - time.time()
                except (TypeError, ValueError):
                    delta = 0.0
                if delta > 0:
                    return min(delta, 60.0)
        return backoff

    def _get_json(self, path: str, params: dict | None = None) -> Any:
        """Issue a GET and return parsed JSON (dict or list); raise on error."""
        resp = self._request_with_retry(path, params=params)
        if not 200 <= resp.status_code < 300:
            raise RuntimeError(
                f"Braze API error {resp.status_code} for {path}: "
                f"{_error_detail(resp)}"
            )
        try:
            return resp.json()
        except ValueError:
            return {}

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Unsupported Braze table {table_name!r}; supported: "
                f"{list(SUPPORTED_TABLES)}"
            )


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _extract_records(body: Any, records_key: str) -> list:
    """Extract the record array at a dotted key (``data`` / ``data.stats``)."""
    cur: Any = body
    for part in records_key.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return []
    return cur if isinstance(cur, list) else []


def _json_str(value: Any) -> str | None:
    """Encode a dynamic nested value as a JSON string.

    ``None`` stays ``None``; an existing string is passed through unchanged (the
    simulator corpus supplies bare strings for these fields); everything else is
    JSON-encoded so the column stays a plain ``StringType``.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return None


def _error_detail(resp: requests.Response) -> str:
    """Best-effort extraction of Braze's ``{"message", "errors"}`` envelope."""
    try:
        body = resp.json()
    except ValueError:
        return (resp.text or "")[:256]
    if isinstance(body, dict):
        message = body.get("message")
        errors = body.get("errors")
        if errors:
            return f"{message}: {errors}"
        if message:
            return str(message)
    return str(body)[:256]


def _parse_int(value: Any, default: int, *, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= minimum else default


def _parse_iso(iso_ts: str) -> datetime:
    """Parse an ISO-8601 timestamp (``Z`` or offset) as timezone-aware UTC."""
    normalised = iso_ts.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalised)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _cmp_iso(a: str, b: str) -> int:
    """Chronologically compare two ISO strings: -1 / 0 / 1."""
    da, db = _parse_iso(a), _parse_iso(b)
    if da < db:
        return -1
    if da > db:
        return 1
    return 0
