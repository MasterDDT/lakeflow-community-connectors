"""Schemas, metadata, and per-stream routing config for the Braze connector.

Everything data-shaped (Spark schemas, table metadata, endpoint paths,
per-stream fan-out/pagination behaviour, and the maximum date-window each
analytics endpoint accepts) lives here so ``braze.py`` stays pure logic.

Field-type conventions (per ``braze_api_doc.md`` Field Type Mapping):

* Counts / integers  -> ``LongType`` (avoid overflow on large tallies).
* Revenue / money    -> ``DoubleType``.
* ISO-8601 date/datetime strings are kept as ``StringType`` (the framework
  and downstream jobs can parse them; keeping them strings avoids lossy /
  timezone-fragile coercion at read time).
* ``tags`` / ``teams`` / ``channels`` -> ``ArrayType(StringType)``.
* Genuinely dynamic nested objects that are keyed by runtime values
  (``messages`` keyed by channel or message-variation id, Canvas
  ``variant_stats`` / ``step_stats``, and the ``variants`` / ``steps`` /
  ``conversion_behaviors`` arrays) have no fixed Spark shape, so they are
  stored as JSON-encoded ``StringType`` columns. The connector JSON-encodes
  them at read time (see ``braze.py``). This mirrors the ADME connector's
  ``*_json`` approach but keeps the documented field names.
* Canvas ``total_stats`` has a documented fixed shape, so it is an explicit
  ``StructType`` rather than a JSON blob.
* Every optional field is nullable.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Retry / rate-limit constants
# ---------------------------------------------------------------------------
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry
REQUEST_TIMEOUT = 30  # seconds; every HTTP call must set an explicit timeout

# ---------------------------------------------------------------------------
# Stream "kinds" — how a table is read.
# ---------------------------------------------------------------------------
#   snapshot_list    : paginated list endpoint, full-refresh snapshot.
#   details          : two-step fan-out (list -> details, one call per id),
#                      snapshot; not date-windowed.
#   fanout_analytics : date-windowed ``data_series`` endpoint fanned out per
#                      parent id/name discovered from a list endpoint.
#   workspace_series : date-windowed ``data_series`` endpoint at the
#                      workspace/app level (no fan-out).
KIND_SNAPSHOT_LIST = "snapshot_list"
KIND_DETAILS = "details"
KIND_FANOUT_ANALYTICS = "fanout_analytics"
KIND_WORKSPACE_SERIES = "workspace_series"

# Deprecated News Feed streams (Braze /feed/* endpoints). Kept for parity with
# the documented catalogue; can be dropped from ``list_tables`` via the
# ``include_deprecated_streams`` connection option.
DEPRECATED_TABLES = ("cards", "cards_analytics")


# ---------------------------------------------------------------------------
# Reusable field groups
# ---------------------------------------------------------------------------
def _tags() -> StructField:
    return StructField("tags", ArrayType(StringType()), True)


# ===========================================================================
# Schemas
# ===========================================================================
CAMPAIGNS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("is_api_campaign", BooleanType(), True),
        StructField("last_edited", StringType(), True),
        _tags(),
    ]
)

CAMPAIGNS_DETAILS_SCHEMA = StructType(
    [
        StructField("campaign_id", StringType(), False),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("archived", BooleanType(), True),
        StructField("draft", BooleanType(), True),
        StructField("enabled", BooleanType(), True),
        StructField("has_post_launch_draft", BooleanType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("schedule_type", StringType(), True),
        StructField("channels", ArrayType(StringType()), True),
        StructField("first_sent", StringType(), True),
        StructField("last_sent", StringType(), True),
        _tags(),
        StructField("teams", ArrayType(StringType()), True),
        # Dynamic (keyed by message-variation id / channel) -> JSON string.
        StructField("messages", StringType(), True),
        # Array of conversion-behaviour objects -> JSON string.
        StructField("conversion_behaviors", StringType(), True),
    ]
)

CAMPAIGNS_ANALYTICS_SCHEMA = StructType(
    [
        StructField("campaign_id", StringType(), False),
        StructField("time", StringType(), False),
        StructField("conversions", LongType(), True),
        StructField("conversions1", LongType(), True),
        StructField("conversions2", LongType(), True),
        StructField("conversions3", LongType(), True),
        StructField("conversions_by_send_time", LongType(), True),
        StructField("conversions1_by_send_time", LongType(), True),
        StructField("unique_recipients", LongType(), True),
        StructField("revenue", DoubleType(), True),
        # Channel-keyed metrics -> JSON string.
        StructField("messages", StringType(), True),
    ]
)

CANVASES_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("last_edited", StringType(), True),
        _tags(),
    ]
)

CANVASES_DETAILS_SCHEMA = StructType(
    [
        StructField("canvas_id", StringType(), False),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("archived", BooleanType(), True),
        StructField("draft", BooleanType(), True),
        StructField("enabled", BooleanType(), True),
        StructField("has_post_launch_draft", BooleanType(), True),
        StructField("schedule_type", StringType(), True),
        StructField("first_entry", StringType(), True),
        StructField("last_entry", StringType(), True),
        StructField("channels", ArrayType(StringType()), True),
        # Array of variant objects -> JSON string.
        StructField("variants", StringType(), True),
        _tags(),
        StructField("teams", ArrayType(StringType()), True),
        # Array of step objects -> JSON string.
        StructField("steps", StringType(), True),
    ]
)

_CANVAS_TOTAL_STATS = StructType(
    [
        StructField("revenue", DoubleType(), True),
        StructField("conversions", LongType(), True),
        StructField("conversions_by_entry_time", LongType(), True),
        StructField("entries", LongType(), True),
    ]
)

CANVASES_ANALYTICS_SCHEMA = StructType(
    [
        StructField("canvas_id", StringType(), False),
        StructField("time", StringType(), False),
        StructField("total_stats", _CANVAS_TOTAL_STATS, True),
        # Per-variant / per-step stats (keyed by variant/step id) -> JSON.
        StructField("variant_stats", StringType(), True),
        StructField("step_stats", StringType(), True),
    ]
)

SEGMENTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("analytics_tracking_enabled", BooleanType(), True),
        _tags(),
    ]
)

SEGMENTS_DETAILS_SCHEMA = StructType(
    [
        StructField("segment_id", StringType(), False),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("text_description", StringType(), True),
        _tags(),
        StructField("teams", ArrayType(StringType()), True),
    ]
)

SEGMENTS_ANALYTICS_SCHEMA = StructType(
    [
        StructField("segment_id", StringType(), False),
        StructField("time", StringType(), False),
        StructField("size", LongType(), True),
    ]
)

EVENTS_SCHEMA = StructType(
    [
        StructField("event", StringType(), False),
    ]
)

EVENTS_ANALYTICS_SCHEMA = StructType(
    [
        StructField("event", StringType(), False),
        StructField("time", StringType(), False),
        StructField("count", LongType(), True),
    ]
)

CARDS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        _tags(),
    ]
)

CARDS_ANALYTICS_SCHEMA = StructType(
    [
        StructField("card_id", StringType(), False),
        StructField("time", StringType(), False),
        StructField("clicks", LongType(), True),
        StructField("impressions", LongType(), True),
        StructField("unique_clicks", LongType(), True),
        StructField("unique_impressions", LongType(), True),
    ]
)

KPI_NEW_USERS_SCHEMA = StructType(
    [
        StructField("time", StringType(), False),
        StructField("new_users", LongType(), True),
    ]
)

KPI_DAU_SCHEMA = StructType(
    [
        StructField("time", StringType(), False),
        StructField("dau", LongType(), True),
    ]
)

KPI_MAU_SCHEMA = StructType(
    [
        StructField("time", StringType(), False),
        StructField("mau", LongType(), True),
    ]
)

KPI_UNINSTALLS_SCHEMA = StructType(
    [
        StructField("time", StringType(), False),
        StructField("uninstalls", LongType(), True),
    ]
)

PURCHASES_PRODUCT_LIST_SCHEMA = StructType(
    [
        StructField("product", StringType(), False),
    ]
)

PURCHASES_QUANTITY_SERIES_SCHEMA = StructType(
    [
        StructField("time", StringType(), False),
        StructField("purchase_quantity", LongType(), True),
    ]
)

PURCHASES_REVENUE_SERIES_SCHEMA = StructType(
    [
        StructField("time", StringType(), False),
        # Doc shows int, but revenue can be fractional in practice -> Double.
        StructField("revenue", DoubleType(), True),
    ]
)

SENDS_ANALYTICS_SCHEMA = StructType(
    [
        StructField("campaign_id", StringType(), False),
        StructField("send_id", StringType(), False),
        StructField("time", StringType(), False),
        StructField("messages", StringType(), True),  # channel-keyed -> JSON
        StructField("sent", LongType(), True),
        StructField("delivered", LongType(), True),
        StructField("undelivered", LongType(), True),
        StructField("delivery_failed", LongType(), True),
        StructField("direct_opens", LongType(), True),
        StructField("total_opens", LongType(), True),
        StructField("bounces", LongType(), True),
        StructField("body_clicks", LongType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("unique_recipients", LongType(), True),
        StructField("conversions", LongType(), True),
        StructField("conversions_by_send_time", LongType(), True),
        StructField("conversions1", LongType(), True),
        StructField("conversions2", LongType(), True),
        StructField("conversions3", LongType(), True),
    ]
)


TABLE_SCHEMAS: dict[str, StructType] = {
    "campaigns": CAMPAIGNS_SCHEMA,
    "campaigns_details": CAMPAIGNS_DETAILS_SCHEMA,
    "campaigns_analytics": CAMPAIGNS_ANALYTICS_SCHEMA,
    "canvases": CANVASES_SCHEMA,
    "canvases_details": CANVASES_DETAILS_SCHEMA,
    "canvases_analytics": CANVASES_ANALYTICS_SCHEMA,
    "segments": SEGMENTS_SCHEMA,
    "segments_details": SEGMENTS_DETAILS_SCHEMA,
    "segments_analytics": SEGMENTS_ANALYTICS_SCHEMA,
    "events": EVENTS_SCHEMA,
    "events_analytics": EVENTS_ANALYTICS_SCHEMA,
    "cards": CARDS_SCHEMA,
    "cards_analytics": CARDS_ANALYTICS_SCHEMA,
    "kpi_daily_new_users": KPI_NEW_USERS_SCHEMA,
    "kpi_daily_active_users": KPI_DAU_SCHEMA,
    "kpi_monthly_active_users": KPI_MAU_SCHEMA,
    "kpi_daily_app_uninstalls": KPI_UNINSTALLS_SCHEMA,
    "purchases_product_list": PURCHASES_PRODUCT_LIST_SCHEMA,
    "purchases_quantity_series": PURCHASES_QUANTITY_SERIES_SCHEMA,
    "purchases_revenue_series": PURCHASES_REVENUE_SERIES_SCHEMA,
    "sends_analytics": SENDS_ANALYTICS_SCHEMA,
}

# Static catalogue ordering used by ``list_tables``.
SUPPORTED_TABLES: tuple[str, ...] = tuple(TABLE_SCHEMAS.keys())


# ===========================================================================
# Metadata (primary_keys / cursor_field / ingestion_type)
# ===========================================================================
_SNAPSHOT = "snapshot"
_APPEND = "append"

TABLE_METADATA: dict[str, dict] = {
    "campaigns": {
        "primary_keys": ["id"], "cursor_field": None,
        "ingestion_type": _SNAPSHOT,
    },
    "campaigns_details": {
        "primary_keys": ["campaign_id"], "cursor_field": None,
        "ingestion_type": _SNAPSHOT,
    },
    "campaigns_analytics": {
        "primary_keys": ["campaign_id", "time"], "cursor_field": "time",
        "ingestion_type": _APPEND,
    },
    "canvases": {
        "primary_keys": ["id"], "cursor_field": None,
        "ingestion_type": _SNAPSHOT,
    },
    "canvases_details": {
        "primary_keys": ["canvas_id"], "cursor_field": None,
        "ingestion_type": _SNAPSHOT,
    },
    "canvases_analytics": {
        "primary_keys": ["canvas_id", "time"], "cursor_field": "time",
        "ingestion_type": _APPEND,
    },
    "segments": {
        "primary_keys": ["id"], "cursor_field": None,
        "ingestion_type": _SNAPSHOT,
    },
    "segments_details": {
        "primary_keys": ["segment_id"], "cursor_field": None,
        "ingestion_type": _SNAPSHOT,
    },
    "segments_analytics": {
        "primary_keys": ["segment_id", "time"], "cursor_field": "time",
        "ingestion_type": _APPEND,
    },
    "events": {
        "primary_keys": ["event"], "cursor_field": None,
        "ingestion_type": _SNAPSHOT,
    },
    "events_analytics": {
        "primary_keys": ["event", "time"], "cursor_field": "time",
        "ingestion_type": _APPEND,
    },
    "cards": {
        "primary_keys": ["id"], "cursor_field": None,
        "ingestion_type": _SNAPSHOT,
    },
    "cards_analytics": {
        "primary_keys": ["card_id", "time"], "cursor_field": "time",
        "ingestion_type": _APPEND,
    },
    "kpi_daily_new_users": {
        "primary_keys": ["time"], "cursor_field": "time",
        "ingestion_type": _APPEND,
    },
    "kpi_daily_active_users": {
        "primary_keys": ["time"], "cursor_field": "time",
        "ingestion_type": _APPEND,
    },
    "kpi_monthly_active_users": {
        "primary_keys": ["time"], "cursor_field": "time",
        "ingestion_type": _APPEND,
    },
    "kpi_daily_app_uninstalls": {
        "primary_keys": ["time"], "cursor_field": "time",
        "ingestion_type": _APPEND,
    },
    "purchases_product_list": {
        "primary_keys": ["product"], "cursor_field": None,
        "ingestion_type": _SNAPSHOT,
    },
    "purchases_quantity_series": {
        "primary_keys": ["time"], "cursor_field": "time",
        "ingestion_type": _APPEND,
    },
    "purchases_revenue_series": {
        "primary_keys": ["time"], "cursor_field": "time",
        "ingestion_type": _APPEND,
    },
    "sends_analytics": {
        "primary_keys": ["campaign_id", "send_id", "time"],
        "cursor_field": "time", "ingestion_type": _APPEND,
    },
}


# ===========================================================================
# Per-stream routing config.
# ===========================================================================
# Braze list-endpoint page sizes (documented). Used only to decide when the
# last (short) page has been reached; the connector paginates 0-indexed.
_PAGE_SIZE_DEFAULT = 100
_PAGE_SIZE_EVENTS = 250

# Maximum date-window (in days) each analytics endpoint accepts in one call.
MAX_WINDOW_DAYS: dict[str, int] = {
    "campaigns_analytics": 100,
    "canvases_analytics": 14,
    "segments_analytics": 100,
    "events_analytics": 100,
    "cards_analytics": 100,
    "kpi_daily_new_users": 100,
    "kpi_daily_active_users": 100,
    "kpi_monthly_active_users": 100,
    "kpi_daily_app_uninstalls": 100,
    "purchases_quantity_series": 100,
    "purchases_revenue_series": 100,
    # Braze only retains send-level analytics for 14 days.
    "sends_analytics": 14,
}

# Snapshot list streams: path + envelope key + whether the array holds bare
# strings (event names / product names) rather than objects.
LIST_STREAMS: dict[str, dict] = {
    "campaigns": {
        "path": "/campaigns/list", "records_key": "campaigns",
        "page_size": _PAGE_SIZE_DEFAULT, "string_items": False,
    },
    "canvases": {
        "path": "/canvas/list", "records_key": "canvases",
        "page_size": _PAGE_SIZE_DEFAULT, "string_items": False,
    },
    "segments": {
        "path": "/segments/list", "records_key": "segments",
        "page_size": _PAGE_SIZE_DEFAULT, "string_items": False,
    },
    "events": {
        "path": "/events/list", "records_key": "events",
        "page_size": _PAGE_SIZE_EVENTS, "string_items": True,
        "string_field": "event",
    },
    "cards": {
        "path": "/feed/list", "records_key": "cards",
        "page_size": _PAGE_SIZE_DEFAULT, "string_items": False,
    },
    "purchases_product_list": {
        "path": "/purchases/product_list", "records_key": "products",
        "page_size": _PAGE_SIZE_DEFAULT, "string_items": True,
        "string_field": "product",
    },
}

# Details streams: parent list source + per-id detail endpoint.
DETAILS_STREAMS: dict[str, dict] = {
    "campaigns_details": {
        "list_table": "campaigns", "detail_path": "/campaigns/details",
        "id_param": "campaign_id", "id_field": "campaign_id",
    },
    "canvases_details": {
        "list_table": "canvases", "detail_path": "/canvas/details",
        "id_param": "canvas_id", "id_field": "canvas_id",
    },
    "segments_details": {
        "list_table": "segments", "detail_path": "/segments/details",
        "id_param": "segment_id", "id_field": "segment_id",
    },
}

# Fan-out analytics streams: parent list source + per-parent data_series call.
FANOUT_STREAMS: dict[str, dict] = {
    "campaigns_analytics": {
        "list_table": "campaigns", "data_path": "/campaigns/data_series",
        "records_key": "data", "id_param": "campaign_id", "id_field": "campaign_id",
    },
    "canvases_analytics": {
        "list_table": "canvases", "data_path": "/canvas/data_series",
        "records_key": "data.stats", "id_param": "canvas_id", "id_field": "canvas_id",
    },
    "segments_analytics": {
        "list_table": "segments", "data_path": "/segments/data_series",
        "records_key": "data", "id_param": "segment_id", "id_field": "segment_id",
    },
    "events_analytics": {
        "list_table": "events", "data_path": "/events/data_series",
        "records_key": "data", "id_param": "event", "id_field": "event",
    },
    "cards_analytics": {
        "list_table": "cards", "data_path": "/feed/data_series",
        "records_key": "data", "id_param": "card_id", "id_field": "card_id",
    },
}

# Workspace-level date-windowed series (no fan-out).
WORKSPACE_STREAMS: dict[str, dict] = {
    "kpi_daily_new_users": {"data_path": "/kpi/new_users/data_series", "records_key": "data"},
    "kpi_daily_active_users": {"data_path": "/kpi/dau/data_series", "records_key": "data"},
    "kpi_monthly_active_users": {"data_path": "/kpi/mau/data_series", "records_key": "data"},
    "kpi_daily_app_uninstalls": {"data_path": "/kpi/uninstalls/data_series", "records_key": "data"},
    "purchases_quantity_series": {"data_path": "/purchases/quantity_series", "records_key": "data"},
    "purchases_revenue_series": {"data_path": "/purchases/revenue_series", "records_key": "data"},
}

# sends_analytics is special: it fans out over (campaign_id, send_id) pairs
# which /campaigns/list does not expose. Handled explicitly in braze.py.
SENDS_DATA_PATH = "/sends/data_series"


def table_kind(table_name: str) -> str:
    """Return the read "kind" for a table."""
    if table_name in LIST_STREAMS:
        return KIND_SNAPSHOT_LIST
    if table_name in DETAILS_STREAMS:
        return KIND_DETAILS
    if table_name in FANOUT_STREAMS or table_name == "sends_analytics":
        return KIND_FANOUT_ANALYTICS
    if table_name in WORKSPACE_STREAMS:
        return KIND_WORKSPACE_SERIES
    raise ValueError(f"Unknown Braze table: {table_name!r}")
