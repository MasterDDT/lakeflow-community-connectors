# Lakeflow Braze Community Connector

This documentation provides setup instructions and reference information for the Braze source connector. The connector reads data **from** Braze into Databricks via the Braze REST Export & Analytics APIs, using the Spark Python Data Source API and Spark Declarative Pipelines (SDP). It ingests campaign, Canvas, segment, custom-event, purchase, send, and workspace-level KPI data as tables in Unity Catalog.

The connector is read-only: it never writes to or modifies your Braze workspace.

## Prerequisites

- A Braze account with access to the **Settings > APIs and Identifiers** dashboard page.
- A Braze **REST API key** with read permissions enabled for the endpoints you want to sync (see [Authentication](#authentication)).
- Your Braze instance **REST endpoint** URL (Braze operates 15+ regional instances; the correct one for your workspace is shown alongside your API keys).

## Setup

### Authentication

Braze authenticates every request with a REST API key sent as an HTTP Bearer token:

```
Authorization: Bearer <YOUR_REST_API_KEY>
```

This is the only authentication method — there is no OAuth flow, client secret, or refresh token. The connector stores only the API key.

**Create the key** in the Braze Dashboard under **Settings > APIs and Identifiers > Rest API Keys**. When creating the key, grant it the per-endpoint permissions for every stream you plan to sync. Braze keys are scoped to a subset of endpoints, so a key that works for one stream may return `403 Forbidden` for another that it lacks permission for.

Minimum permissions per stream:

| Stream(s) | Required permission |
|---|---|
| `campaigns` | `campaigns.list` |
| `campaigns_details` | `campaigns.details` |
| `campaigns_analytics` | `campaigns.data_series` |
| `canvases` | `canvas.list` |
| `canvases_details` | `canvas.details` |
| `canvases_analytics` | `canvas.data_series` |
| `segments` | `segments.list` |
| `segments_details` | `segments.details` |
| `segments_analytics` | `segments.data_series` |
| `events` | `events.list` |
| `events_analytics` | `events.data_series` |
| `kpi_daily_new_users` | `kpi.new_users.data_series` |
| `kpi_daily_active_users` | `kpi.dau.data_series` |
| `kpi_monthly_active_users` | `kpi.mau.data_series` |
| `kpi_daily_app_uninstalls` | `kpi.uninstalls.data_series` |
| `purchases_product_list` | `purchases.product_list` |
| `purchases_quantity_series` | `purchases.quantity_series` |
| `purchases_revenue_series` | `purchases.revenue_series` |
| `sends_analytics` | `sends.data_series` |
| `cards`, `cards_analytics` | News Feed (`/feed/*`) — deprecated; see [Known Limitations](#known-limitations-and-quirks) |

A `401 Unauthorized` response means the key is missing, malformed, or deactivated; a `403 Forbidden` means the key is valid but lacks the permission for that endpoint.

### Required Connection Parameters

To configure the connector, provide the following parameters in your connector options:

| Parameter | Type | Required | Description | Example |
|---|---|---|---|---|
| `rest_endpoint` | String | Yes | Your Braze instance REST base URL. Braze is multi-tenant; this must match the region where your workspace is hosted. | `https://rest.iad-01.braze.com` |
| `api_key` | String | Yes | Braze REST API key used for Bearer authentication. Store securely; it is a secret. | `00000000-0000-0000-0000-000000000000` |

Your REST endpoint is shown in the Braze Dashboard under **Settings > APIs and Identifiers**, next to your API keys. It is not a global URL — you must supply the one for your instance. Common examples:

| Region | REST Base URL |
|---|---|
| US-01 | `https://rest.iad-01.braze.com` |
| US-03 | `https://rest.iad-03.braze.com` |
| US-05 | `https://rest.iad-05.braze.com` |
| US-08 | `https://rest.iad-08.braze.com` |
| US-10 | `https://rest.us-10.braze.com` |
| EU-01 | `https://rest.fra-01.braze.eu` |
| EU-02 | `https://rest.fra-02.braze.eu` |
| AU-01 | `https://rest.au-01.braze.com` |
| ID-01 | `https://rest.id-01.braze.com` |
| JP-01 | `https://rest.jp-01.braze.com` |
| KR-01 | `https://rest.kr-01.braze.com` |

See the full list of 15 regional endpoints in the [Braze API documentation](https://www.braze.com/docs/api/basics/#endpoints).

### Table-Specific Options (`externalOptionsAllowList`)

This connector supports several table-specific options that must be explicitly allowed on the Unity Catalog connection. `externalOptionsAllowList` is a **required** connection option, and it must be set to the following comma-separated list of all supported options:

```
include_deprecated_streams,send_ids,history_days,app_id,unit,segment_id,product
```

These options are described in [Special `table_configuration` options](#special-table_configuration-options). Only options included in `externalOptionsAllowList` are passed through to the connector.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `include_deprecated_streams,send_ids,history_days,app_id,unit,segment_id,product` to enable per-table configuration of these options.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The connector supports the following 21 objects. Object names are case-sensitive; use the exact names shown below (lowercase with underscores).

Two ingestion modes are used:

- **Snapshot** (full refresh): every sync fetches the complete set of records and replaces the destination table. Used for list and detail objects.
- **Append** (incremental): each sync fetches only new data points in a date window, keyed by the `time` cursor. Used for the `data_series` / KPI / purchase-series objects. The connector freezes its upper bound at the start of each run to midnight UTC of the current day, so only **complete days** are ingested and repeated triggers within a day never double-count a partial day.

Several objects use a **two-step fan-out**: a list endpoint is first queried to discover parent IDs (or names), then a details or analytics endpoint is called once per parent. The analytics fan-out and workspace-level series objects are **partitioned streams** — the connector emits one partition per parent ID (or a single windowed partition for workspace-level series), so these reads parallelize across Spark executors. Snapshot list and detail objects are not partitioned and are read on a single driver.

| Object | Group | Endpoint | Primary Key | Ingestion Mode | Cursor | Partitioned | Notes |
|---|---|---|---|---|---|---|---|
| `campaigns` | Campaigns | `/campaigns/list` | `id` | Snapshot | — | No | Parent list for campaign fan-outs |
| `campaigns_details` | Campaigns | `/campaigns/details` | `campaign_id` | Snapshot | — | No | Fan-out: one call per campaign |
| `campaigns_analytics` | Campaigns | `/campaigns/data_series` | `campaign_id`, `time` | Append | `time` | Yes | Fan-out per campaign; up to 100-day window |
| `canvases` | Canvases | `/canvas/list` | `id` | Snapshot | — | No | Parent list for Canvas fan-outs |
| `canvases_details` | Canvases | `/canvas/details` | `canvas_id` | Snapshot | — | No | Fan-out: one call per Canvas |
| `canvases_analytics` | Canvases | `/canvas/data_series` | `canvas_id`, `time` | Append | `time` | Yes | Fan-out per Canvas; **max 14-day window** |
| `segments` | Segments | `/segments/list` | `id` | Snapshot | — | No | Parent list for segment fan-outs |
| `segments_details` | Segments | `/segments/details` | `segment_id` | Snapshot | — | No | Fan-out: one call per segment |
| `segments_analytics` | Segments | `/segments/data_series` | `segment_id`, `time` | Append | `time` | Yes | Fan-out per segment; up to 100-day window; returns estimated size |
| `events` | Events | `/events/list` | `event` | Snapshot | — | No | Custom-event names; parent list for `events_analytics` |
| `events_analytics` | Events | `/events/data_series` | `event`, `time` | Append | `time` | Yes | Fan-out per event name; up to 100-day window |
| `kpi_daily_new_users` | KPI | `/kpi/new_users/data_series` | `time` | Append | `time` | Yes | Workspace-level; one record per day |
| `kpi_daily_active_users` | KPI | `/kpi/dau/data_series` | `time` | Append | `time` | Yes | Workspace-level; one record per day |
| `kpi_monthly_active_users` | KPI | `/kpi/mau/data_series` | `time` | Append | `time` | Yes | Workspace-level; 30-day rolling window |
| `kpi_daily_app_uninstalls` | KPI | `/kpi/uninstalls/data_series` | `time` | Append | `time` | Yes | Workspace-level; one record per day |
| `purchases_product_list` | Purchases | `/purchases/product_list` | `product` | Snapshot | — | No | Product names |
| `purchases_quantity_series` | Purchases | `/purchases/quantity_series` | `time` | Append | `time` | Yes | Workspace-level; up to 100-day window |
| `purchases_revenue_series` | Purchases | `/purchases/revenue_series` | `time` | Append | `time` | Yes | Workspace-level; up to 100-day window |
| `sends_analytics` | Sends | `/sends/data_series` | `campaign_id`, `send_id`, `time` | Append | `time` | Yes | Fan-out per `(campaign_id, send_id)`; **14-day retention** |
| `cards` | News Feed (deprecated) | `/feed/list` | `id` | Snapshot | — | No | Deprecated; see quirks |
| `cards_analytics` | News Feed (deprecated) | `/feed/data_series` | `card_id`, `time` | Append | `time` | Yes | Deprecated; see quirks |

This connector does not support delete synchronization; append objects are never updated or tombstoned.

### Object groups

- **Campaigns** — `campaigns` lists all campaigns; `campaigns_details` fans out to per-campaign configuration; `campaigns_analytics` fans out to per-campaign daily performance metrics.
- **Canvases** — `canvases`, `canvases_details`, and `canvases_analytics` mirror the campaigns pattern for Braze Canvases. Note that `canvases_analytics` is limited to a 14-day window per request.
- **Segments** — `segments`, `segments_details`, and `segments_analytics`. Segment analytics reports an **estimated** segment size, not exact membership.
- **Custom Events** — `events` returns the list of custom-event names; `events_analytics` fans out to per-event occurrence counts.
- **KPI daily/monthly series** — workspace-level daily new users, daily active users, monthly (30-day rolling) active users, and daily app uninstalls.
- **Purchases** — `purchases_product_list` returns product names; `purchases_quantity_series` and `purchases_revenue_series` return workspace-level purchase quantity and revenue over time.
- **Sends** — `sends_analytics` returns send-level analytics keyed by `(campaign_id, send_id, time)`. See the `send_id` caveat in [Known Limitations](#known-limitations-and-quirks).
- **News Feed cards** — `cards` and `cards_analytics` are **deprecated** and disabled-able via `include_deprecated_streams`.

### Special columns

Some columns hold genuinely dynamic nested objects that have no fixed shape (for example channel-keyed `messages`, Canvas `variant_stats` / `step_stats`, and the `variants` / `steps` / `conversion_behaviors` arrays). These are stored as **JSON-encoded string columns**, so downstream jobs can parse them with `from_json` or similar. The Canvas `total_stats` column has a fixed, documented shape and is kept as a nested struct. Timestamps and dates are stored as ISO-8601 strings.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system (one of the objects above) |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Special `table_configuration` options

These source-specific options are set inside the `table_configuration` map. Every option used here must also appear in the connection's `externalOptionsAllowList`.

| Option | Applicable Objects | Required | Description | Default |
|---|---|---|---|---|
| `include_deprecated_streams` | Global (connection-wide) | No | Whether the deprecated News Feed streams (`cards`, `cards_analytics`) are listed as available objects. Set to `false` to hide them. | `true` |
| `history_days` | All append objects | No | Number of days of history to backfill on the first sync. Capped at the endpoint's maximum window. | Endpoint's maximum window (14 or 100 days) |
| `send_ids` | `sends_analytics` | No | Comma-separated `campaign_id:send_id` tokens used to fan out send analytics (see the `send_id` caveat below). | — |
| `app_id` | `events_analytics`, all `kpi_*` series, `purchases_quantity_series`, `purchases_revenue_series` | No | Restrict results to a single Braze app. | — |
| `unit` | `events_analytics`, `purchases_quantity_series`, `purchases_revenue_series` | No | Time granularity: `day` (default) or `hour`. | `day` |
| `segment_id` | `events_analytics` | No | Restrict event counts to a single segment. | — |
| `product` | `purchases_quantity_series`, `purchases_revenue_series` | No | Restrict purchase series to a single product name. | — |

## Data Type Mapping

The connector maps Braze API field types to Spark SQL types as follows:

| Source (Braze API) | Spark SQL Type | Notes |
|---|---|---|
| `string` (general) | `StringType` | — |
| `string` (ISO-8601 date / datetime) | `StringType` | Kept as a string to avoid lossy or timezone-fragile coercion; parse downstream if needed |
| `integer` / count | `LongType` | `Long` avoids overflow on large tallies |
| `float` / revenue | `DoubleType` | Revenue and other monetary fields |
| `boolean` | `BooleanType` | — |
| `array[string]` (`tags`, `teams`, `channels`) | `ArrayType(StringType)` | — |
| dynamic nested object/array (`messages`, `variant_stats`, `step_stats`, `variants`, `steps`, `conversion_behaviors`) | `StringType` (JSON-encoded) | No fixed shape; stored as a JSON string for downstream parsing |
| fixed-shape object (Canvas `total_stats`) | `StructType` | Documented fixed shape kept as nested struct |
| `null` / absent field | nullable | All optional fields are nullable |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Configure each object, adding table-specific options where needed. For example:

```json
{
  "pipeline_spec": {
    "connection_name": "my_braze_connection",
    "object": [
      {
        "table": {
          "source_table": "campaigns"
        }
      },
      {
        "table": {
          "source_table": "campaigns_analytics",
          "table_configuration": {
            "history_days": "90"
          }
        }
      },
      {
        "table": {
          "source_table": "events_analytics",
          "table_configuration": {
            "unit": "day",
            "app_id": "01234567-89ab-cdef-0123-456789abcdef"
          }
        }
      },
      {
        "table": {
          "source_table": "purchases_revenue_series",
          "table_configuration": {
            "product": "premium_subscription"
          }
        }
      },
      {
        "table": {
          "source_table": "sends_analytics",
          "table_configuration": {
            "send_ids": "campaign_abc:send_123,campaign_def:send_456"
          }
        }
      }
    ]
  }
}
```

3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a few objects (e.g., `campaigns`, `kpi_daily_active_users`) to validate your pipeline before adding the full analytics fan-outs.
- **Use Incremental Sync**: The append objects (`*_analytics`, `kpi_*`, `purchases_*_series`) only fetch new complete days on each run, reducing API calls and improving performance.
- **Mind the fan-outs**: `campaigns_analytics`, `canvases_analytics`, `segments_analytics`, and `events_analytics` issue one request per parent ID/name. Workspaces with many campaigns or events will make proportionally more API calls; the connector parallelizes these across executors.
- **Backfill deliberately**: Use `history_days` to control how far back the first sync reaches. Windows are capped at each endpoint's maximum (14 days for Canvas analytics and sends, 100 days for the others).
- **Set Appropriate Schedules**: Balance data freshness against Braze's rate limits (see below).
- **Rate limit handling**: The connector automatically retries on HTTP 429, 500, 502, 503, and 504 with exponential backoff (up to 5 attempts), and honors the `X-RateLimit-Reset` header on 429 responses. Braze rate limits vary by endpoint:
  - `/campaigns/data_series`: 50,000 requests/minute.
  - `/events/list`, `/purchases/product_list`, `/purchases/quantity_series`, `/purchases/revenue_series` (and related): a shared pool of 1,000 requests/hour.
  - All other export/analytics endpoints: 250,000 requests/hour (default).
  - Limits reset at clock-hour boundaries, not on a rolling window.

#### Troubleshooting

**Common Issues:**

- **Authentication errors (`401`)**: Verify the `api_key` is a current, active Braze REST API key.
- **Permission errors (`403`)**: The key is valid but lacks the permission for that endpoint. Enable the required per-endpoint permission on the key (see [Authentication](#authentication)).
- **Wrong instance / `404` or empty results**: Confirm `rest_endpoint` matches your Braze region exactly (e.g., `https://rest.fra-01.braze.eu` for EU-01).
- **`sends_analytics` returns no rows**: `send_id` is not exposed by the campaigns list endpoint. Supply `(campaign_id, send_id)` pairs via the `send_ids` option, or the stream will produce nothing. See the caveat below.
- **Missing historical analytics**: Send analytics are only retained for 14 days, and Canvas analytics accept at most a 14-day window per request. Older data cannot be backfilled.
- **Rate limiting (`429`)**: Handled automatically with backoff. If it persists, reduce sync frequency or the number of fan-out objects synced per run.

## Known Limitations and Quirks

- **Canvas analytics — 14-day maximum window**: `/canvas/data_series` accepts a maximum `length` of 14 days per request, unlike other analytics endpoints that accept up to 100 days. The connector caps `canvases_analytics` windows accordingly.
- **Send analytics — 14-day retention**: Braze retains send-level analytics for only 14 days after the send date. Requesting older data returns empty results (not an error); historical backfill beyond 14 days is not possible.
- **`send_id` availability**: `sends_analytics` requires both a `campaign_id` and a `send_id`. Braze does not expose `send_id` through `/campaigns/list`, so unless you supply `campaign_id:send_id` pairs via the `send_ids` table option (or a campaign record happens to carry a `send_id`), the stream yields no rows.
- **News Feed (`/feed/*`) deprecation**: The `cards` and `cards_analytics` objects use the deprecated Braze News Feed endpoints (`/feed/list`, `/feed/data_series`). Braze recommends migrating to Content Cards. These endpoints remain callable but their schemas are best-effort. Set `include_deprecated_streams` to `false` to hide these objects. If your workspace does not use News Feed, they return empty lists.
- **Per-endpoint rate limits**: Rate limits differ sharply by endpoint (see Best Practices). The `/events/*` and `/purchases/*` list/series endpoints share a low 1,000 requests/hour pool, so syncing those alongside large event or product catalogs can throttle sooner than the default endpoints.
- **Segment analytics is estimated**: `segments_analytics` reports an estimated segment `size` for each day, not exact membership.
- **Complete-days only**: Append objects freeze their upper bound at the start of each run to midnight UTC of the current day. The current (partial) day is not ingested until it completes, so the latest day appears on the following sync.

## References

- [Braze API Basics (base URLs, auth, rate limits)](https://www.braze.com/docs/api/basics/)
- [Braze REST instance endpoints](https://www.braze.com/docs/api/basics/#endpoints)
- [Braze API rate limits](https://www.braze.com/docs/api/api_limits/)
- [Braze API errors](https://www.braze.com/docs/api/errors/)
- [Braze Export API endpoints](https://www.braze.com/docs/api/endpoints/export/)
- [Lakeflow Community Connectors Repository](https://github.com/databrickslabs/lakeflow-community-connectors)
