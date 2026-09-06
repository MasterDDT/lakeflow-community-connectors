# Braze API Documentation

Source for the Lakeflow Community Connector — reads data FROM Braze into Databricks via the Braze REST Export and Analytics APIs.

---

## Authorization

**Method**: REST API Key via Bearer token header (single method; no OAuth flow).

Every request must include:

```
Authorization: Bearer YOUR_REST_API_KEY
```

The key is created in the Braze Dashboard under **Settings > APIs and Identifiers > Rest API Keys**. Each key can be scoped to a subset of endpoints (permissions). The connector stores only the API key (no client secret or refresh token).

**Per-endpoint permissions** (key must have the corresponding permission enabled):

| Stream | Required permission |
|--------|-------------------|
| campaigns (list) | `campaigns.list` |
| campaigns_details | `campaigns.details` |
| campaigns_analytics | `campaigns.data_series` |
| canvases (list) | `canvas.list` |
| canvases_details | `canvas.details` |
| canvases_analytics | `canvas.data_series` |
| segments (list) | `segments.list` |
| segments_details | `segments.details` |
| segments_analytics | `segments.data_series` |
| events (list) | `events.list` |
| events_analytics | `events.data_series` |
| cards (list) | TBD: not documented (News Feed deprecated) |
| cards_analytics | TBD: not documented (News Feed deprecated) |
| kpi_daily_new_users | `kpi.new_users.data_series` |
| kpi_daily_active_users | `kpi.dau.data_series` |
| kpi_monthly_active_users | `kpi.mau.data_series` |
| kpi_daily_app_uninstalls | `kpi.uninstalls.data_series` |
| purchases_product_list | `purchases.product_list` |
| purchases_quantity_series | `purchases.quantity_series` |
| purchases_revenue_series | `purchases.revenue_series` |
| sends_analytics | `sends.data_series` |
| sessions_analytics | `sessions.data_series` |

**Error responses for auth failures**:
- `401 Unauthorized` — key is missing, malformed, or deactivated.
- `403 Forbidden` — key exists but lacks permission for the requested endpoint.

---

## Base URL / Instance Endpoints

Braze is multi-tenant with per-instance REST endpoints. The instance is a **required connection parameter**. Customers find their REST endpoint in the Braze Dashboard under **Settings > APIs and Identifiers > API Keys**.

| Instance | Dashboard URL | REST Base URL |
|----------|--------------|---------------|
| US-01 | `https://dashboard-01.braze.com` | `https://rest.iad-01.braze.com` |
| US-02 | `https://dashboard-02.braze.com` | `https://rest.iad-02.braze.com` |
| US-03 | `https://dashboard-03.braze.com` | `https://rest.iad-03.braze.com` |
| US-04 | `https://dashboard-04.braze.com` | `https://rest.iad-04.braze.com` |
| US-05 | `https://dashboard-05.braze.com` | `https://rest.iad-05.braze.com` |
| US-06 | `https://dashboard-06.braze.com` | `https://rest.iad-06.braze.com` |
| US-07 | `https://dashboard-07.braze.com` | `https://rest.iad-07.braze.com` |
| US-08 | `https://dashboard-08.braze.com` | `https://rest.iad-08.braze.com` |
| US-10 | `https://dashboard.us-10.braze.com` | `https://rest.us-10.braze.com` |
| EU-01 | `https://dashboard-01.braze.eu` | `https://rest.fra-01.braze.eu` |
| EU-02 | `https://dashboard-02.braze.eu` | `https://rest.fra-02.braze.eu` |
| AU-01 | `https://dashboard.au-01.braze.com` | `https://rest.au-01.braze.com` |
| ID-01 | `https://dashboard.id-01.braze.com` | `https://rest.id-01.braze.com` |
| JP-01 | `https://dashboard.jp-01.braze.com` | `https://rest.jp-01.braze.com` |
| KR-01 | `https://dashboard.kr-01.braze.com` | `https://rest.kr-01.braze.com` |

All endpoint paths below are appended to the instance's REST base URL, e.g. `GET https://rest.iad-01.braze.com/campaigns/list`.

---

## Rate Limits

Braze enforces **workspace-level** rate limits. Exceeding a limit returns `HTTP 429 Too Many Requests`.

**Response headers** included on every API response:

| Header | Description |
|--------|-------------|
| `X-RateLimit-Limit` | Maximum requests allowed in the current window |
| `X-RateLimit-Remaining` | Requests remaining in the current window |
| `X-RateLimit-Reset` | UTC epoch seconds when the window resets |

Rate limits reset at **clock-hour boundaries**, not as rolling windows.

**Per-endpoint rate limits**:

| Endpoint(s) | Rate Limit |
|-------------|------------|
| `/campaigns/data_series` | **50,000 requests/minute** |
| `/events/list`, `/events`, `/purchases/product_list`, `/custom_attributes`, `/purchases/revenue_series`, `/purchases/quantity_series` | **1,000 requests/hour** (shared pool) |
| All other export/analytics endpoints (campaigns list/details, canvas list/details/analytics, segments, KPI, sessions, sends, events_analytics, etc.) | **250,000 requests/hour** (default) |

---

## Error Model

All responses share the same envelope:

```json
{
  "message": "success",
  "errors": ["optional non-fatal error message"]
}
```

On fatal errors:

```json
{
  "message": "<fatal error description>",
  "errors": ["optional detail"]
}
```

| HTTP Status | Meaning | Retryable |
|-------------|---------|-----------|
| 2XX | Success (request accepted) | N/A |
| 400 | Bad syntax, invalid JSON, missing required params | No |
| 401 | Invalid or missing API key | No |
| 403 | API key lacks required permission | No |
| 404 | Invalid URL / resource not found | No |
| 415 | Missing or incorrect `Content-Type` header | No |
| 429 | Rate limited | Yes (exponential backoff) |
| 5XX | Server error | Yes (exponential backoff) |

---

## Pagination

Most list endpoints use a **0-indexed `page` integer** query parameter.

| Endpoint | Items per page |
|----------|---------------|
| `/campaigns/list` | 100 |
| `/canvas/list` | 100 |
| `/segments/list` | 100 |
| `/feed/list` | 100 (TBD: assumed based on pattern; not in current docs) |
| `/events/list` | 250 |
| `/purchases/product_list` | TBD: not explicitly stated in docs |
| `/custom_attributes` | 50 (uses `cursor` string, not `page`) |
| `/events` | 50 (uses `cursor` string, not `page`) |

Analytics/data-series endpoints are **not paginated** — they return all data points for the requested time window in a single response.

---

## Incremental / Date-Window Support

Analytics and data-series endpoints take:
- `length` (integer, required): number of days (or hours) to look back
- `ending_at` (ISO-8601 datetime, optional): end of window; defaults to request time

**Maximum `length` values**:

| Endpoint | Max `length` | Unit |
|----------|-------------|------|
| `/campaigns/data_series` | 100 | days |
| `/canvas/data_series` | 14 | days |
| `/segments/data_series` | 100 | days |
| `/events/data_series` | 100 | days or hours |
| `/kpi/*/data_series` | 100 | days |
| `/purchases/quantity_series` | 100 | days or hours |
| `/purchases/revenue_series` | 100 | days or hours |
| `/sends/data_series` | 100 | days |
| `/sessions/data_series` | 100 | days or hours |
| `/feed/data_series` | 100 (TBD: assumed) | days |

List endpoints (`/campaigns/list`, `/canvas/list`, `/segments/list`) support **incremental filtering** via the `last_edit.time[gt]` query parameter (ISO-8601 timestamp), which returns only records edited after that time.

For **sends analytics** specifically: Braze retains send-level analytics for only **14 days** after the send date, regardless of `length`.

---

## Object List

Objects/streams are a mix of static (determined by connector logic) and discoverable:

- **Static structure**: All streams listed below have fixed endpoints. There is no single "list all streams" API — the connector hardcodes the stream catalog.
- **Discoverable sub-entities**: Campaign IDs, Canvas IDs, Segment IDs, and Event names are discovered at runtime via their respective list endpoints before fetching per-entity details/analytics.

### Common Two-Step Fan-Out Pattern

Several streams require a two-step fetch:
1. **List** endpoint → returns IDs/names
2. **Details or analytics** endpoint → called once per ID/name

```
/campaigns/list  →  /campaigns/details  (one call per campaign_id)
                 →  /campaigns/data_series (one call per campaign_id per time window)
/canvas/list     →  /canvas/details
                 →  /canvas/data_series
/segments/list   →  /segments/details
                 →  /segments/data_series
/events/list     →  /events/data_series  (one call per event name per time window)
/feed/list       →  /feed/data_series    (one call per card_id per time window)
```

---

## Object Schema

### Stream: campaigns

**Endpoint**: `GET /campaigns/list`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/campaigns/get_campaigns/
**Rate limit**: 250,000 req/hr
**Paginated**: Yes (`page`, 0-indexed, 100 per page)
**Incremental filter**: `last_edit.time[gt]` (ISO-8601)
**Response envelope field**: `campaigns`
**Primary key**: `id`

**Request parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `page` | No | integer | 0-indexed page number; defaults to 0 |
| `include_archived` | No | boolean | Include archived campaigns; default false |
| `sort_direction` | No | string | `asc` (default) or `desc` |
| `last_edit.time[gt]` | No | ISO-8601 datetime | Filter to campaigns edited after this time |

**Response**:

```json
{
  "message": "success",
  "campaigns": [
    {
      "id": "string",
      "name": "string",
      "is_api_campaign": "boolean",
      "last_edited": "string (ISO 8601)",
      "tags": ["string"]
    }
  ]
}
```

**Schema**:

| Field | Type | Notes |
|-------|------|-------|
| `id` | string | Campaign API identifier; primary key |
| `name` | string | Campaign name |
| `is_api_campaign` | boolean | Whether campaign uses API triggering |
| `last_edited` | string (ISO 8601) | Last modification timestamp; use for incremental |
| `tags` | array[string] | Associated tag names |

**Ingestion type**: `snapshot` (full refresh with incremental filter via `last_edit.time[gt]`)

---

### Stream: campaigns_details

**Endpoint**: `GET /campaigns/details`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/campaigns/get_campaign_details/
**Rate limit**: 250,000 req/hr
**Paginated**: No (single record per `campaign_id`)
**Fan-out dependency**: Requires `campaign_id` from `campaigns` list stream
**Primary key**: `campaign_id` (synthetic; added by connector from parent)

**Request parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `campaign_id` | Yes | string | Campaign API identifier |
| `post_launch_draft_version` | No | boolean | Show draft changes; default false |
| `include_has_translatable_content` | No | boolean | Include translation flags; default false |

**Response**:

```json
{
  "message": "success",
  "created_at": "string (ISO 8601)",
  "updated_at": "string (ISO 8601)",
  "archived": "boolean",
  "draft": "boolean",
  "enabled": "boolean",
  "has_post_launch_draft": "boolean",
  "name": "string",
  "description": "string",
  "schedule_type": "string",
  "channels": ["string"],
  "first_sent": "string (ISO 8601)",
  "last_sent": "string (ISO 8601)",
  "tags": ["string"],
  "teams": ["string"],
  "messages": {},
  "conversion_behaviors": []
}
```

**Schema**:

| Field | Type | Notes |
|-------|------|-------|
| `campaign_id` | string | Injected from parent list; primary key |
| `created_at` | string (ISO 8601) | Creation timestamp |
| `updated_at` | string (ISO 8601) | Last update timestamp |
| `archived` | boolean | Archive status |
| `draft` | boolean | Draft status |
| `enabled` | boolean | Active status |
| `has_post_launch_draft` | boolean | Whether post-launch draft exists |
| `name` | string | Campaign name |
| `description` | string | Campaign description |
| `schedule_type` | string | Scheduling action type |
| `channels` | array[string] | Delivery channels (email, push, sms, etc.) |
| `first_sent` | string (ISO 8601) | First send timestamp |
| `last_sent` | string (ISO 8601) | Most recent send timestamp |
| `tags` | array[string] | Associated tags |
| `teams` | array[string] | Associated teams |
| `messages` | object | Message variations keyed by variation ID; channel-specific subfields |
| `conversion_behaviors` | array[object] | Conversion event definitions with type and window |

**`messages` object sub-fields (varies by channel)**:

| Field | Channel | Type |
|-------|---------|------|
| `channel` | all | string |
| `name` | all | string |
| `subject` | email | string |
| `body` | email, SMS, webhook | string |
| `from` | email, SMS | string |
| `reply_to` | email | string |
| `alert` | push | string |
| `title` | push, email | string |
| `action` | push | string |
| `image_url` | push | string |
| `url` | webhook | string |
| `method` | webhook | string |
| `headers` | webhook | object |
| `subscription_group_id` | SMS, WhatsApp | string |
| `template_name` | WhatsApp | string |
| `extras` | content cards, email, in-app | hash/array |

**Ingestion type**: `snapshot` (no date filtering; full re-fetch per campaign on each sync)

---

### Stream: campaigns_analytics

**Endpoint**: `GET /campaigns/data_series`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/campaigns/get_campaign_analytics/
**Rate limit**: **50,000 req/min** (much higher than default)
**Paginated**: No
**Fan-out dependency**: Requires `campaign_id` from `campaigns` list stream
**Incremental**: Yes — cursor field `time`, window up to 100 days per request
**Response envelope field**: `data`
**Primary key**: Composite `(campaign_id, time)` (best-effort; `time` is the per-record key within a campaign)

**Request parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `campaign_id` | Yes | string | Campaign API identifier |
| `length` | Yes | integer | Days to include (1–100) |
| `ending_at` | No | ISO-8601 datetime | End of window; defaults to now |

**Response**:

```json
{
  "message": "success",
  "data": [
    {
      "time": "string (ISO 8601 date)",
      "conversions": "integer",
      "conversions1": "integer (optional)",
      "conversions2": "integer (optional)",
      "conversions3": "integer (optional)",
      "conversions_by_send_time": "integer (optional)",
      "conversions1_by_send_time": "integer (optional)",
      "unique_recipients": "integer",
      "revenue": "float (optional)",
      "messages": {}
    }
  ]
}
```

**Schema** (per `data[]` element):

| Field | Type | Notes |
|-------|------|-------|
| `time` | string (ISO 8601 date) | Date of the data point; cursor for incremental |
| `conversions` | integer | Primary conversion event count |
| `conversions1` | integer | Secondary conversion (optional) |
| `conversions2` | integer | Tertiary conversion (optional) |
| `conversions3` | integer | Quaternary conversion (optional) |
| `conversions_by_send_time` | integer | Conversions attributed to send time (optional) |
| `unique_recipients` | integer | Unique message recipients |
| `revenue` | float | Revenue in USD (optional) |
| `messages` | object | Channel-specific metrics keyed by channel name |

**`messages` sub-fields by channel**:

| Channel | Fields |
|---------|--------|
| `ios_push`, `android_push` | `sent`, `direct_opens`, `total_opens`, `bounces`, `body_clicks` |
| `email` | `sent`, `opens`, `unique_opens`, `clicks`, `unique_clicks`, `unsubscribes`, `bounces`, `delivered`, `reported_spam` |
| `sms` | `sent`, `sent_to_carrier`, `delivered`, `rejected`, `delivery_failed`, `clicks`, `opt_out`, `help` |
| `webhook` | `sent`, `errors` |
| `whatsapp` | `sent`, `delivered`, `failed`, `read` |
| `content_card` | `sent`, `total_clicks`, `total_dismissals`, `total_impressions` |

**Ingestion type**: `append` (date-windowed; request windows of up to 100 days using `length` + `ending_at`)

---

### Stream: canvases

**Endpoint**: `GET /canvas/list`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/canvas/get_canvases/
**Rate limit**: 250,000 req/hr
**Paginated**: Yes (`page`, 0-indexed, 100 per page)
**Incremental filter**: `last_edit.time[gt]`
**Response envelope field**: `canvases`
**Primary key**: `id`

**Request parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `page` | No | integer | 0-indexed page; defaults to 0 |
| `include_archived` | No | boolean | Include archived Canvases; default false |
| `sort_direction` | No | string | `asc` (default) or `desc` |
| `last_edit.time[gt]` | No | ISO-8601 datetime | Filter to Canvases edited after this time |

**Response**:

```json
{
  "canvases": [
    {
      "id": "string",
      "last_edited": "string (ISO 8601)",
      "name": "string",
      "tags": ["string"]
    }
  ],
  "message": "success"
}
```

**Schema**:

| Field | Type | Notes |
|-------|------|-------|
| `id` | string | Canvas API identifier; primary key |
| `name` | string | Canvas name |
| `last_edited` | string (ISO 8601) | Last modification timestamp; use for incremental |
| `tags` | array[string] | Associated tag names |

**Ingestion type**: `snapshot`

---

### Stream: canvases_details

**Endpoint**: `GET /canvas/details`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/canvas/get_canvas_details/
**Rate limit**: 250,000 req/hr
**Paginated**: No
**Fan-out dependency**: Requires `canvas_id` from `canvases` list stream
**Primary key**: `canvas_id` (synthetic; injected by connector)

**Request parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `canvas_id` | Yes | string | Canvas API identifier |
| `post_launch_draft_version` | No | boolean | Show draft changes; default false |
| `include_has_translatable_content` | No | boolean | Include translation flags; default false |

**Response**:

```json
{
  "message": "success",
  "created_at": "string (ISO 8601)",
  "updated_at": "string (ISO 8601)",
  "name": "string",
  "description": "string",
  "archived": "boolean",
  "draft": "boolean",
  "enabled": "boolean",
  "has_post_launch_draft": "boolean",
  "schedule_type": "string",
  "first_entry": "string (ISO 8601)",
  "last_entry": "string (ISO 8601)",
  "channels": ["string"],
  "variants": [],
  "tags": ["string"],
  "teams": ["string"],
  "steps": []
}
```

**Schema**:

| Field | Type | Notes |
|-------|------|-------|
| `canvas_id` | string | Injected from parent; primary key |
| `created_at` | string (ISO 8601) | Creation timestamp |
| `updated_at` | string (ISO 8601) | Last update timestamp |
| `name` | string | Canvas name |
| `description` | string | Canvas description |
| `archived` | boolean | Archive status |
| `draft` | boolean | Draft status |
| `enabled` | boolean | Active status |
| `has_post_launch_draft` | boolean | Post-launch draft existence |
| `schedule_type` | string | Scheduling type |
| `first_entry` | string (ISO 8601) | First entry timestamp |
| `last_entry` | string (ISO 8601) | Most recent entry timestamp |
| `channels` | array[string] | Channels used in canvas |
| `variants` | array[object] | Variant definitions: `{name, id, first_step_ids, first_step_id}` |
| `tags` | array[string] | Associated tags |
| `teams` | array[string] | Associated teams |
| `steps` | array[object] | Step definitions: `{name, type, id, next_step_ids, next_paths, channels, messages}` |

**Ingestion type**: `snapshot`

---

### Stream: canvases_analytics

**Endpoint**: `GET /canvas/data_series`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/canvas/get_canvas_analytics/
**Rate limit**: 250,000 req/hr
**Paginated**: No
**Fan-out dependency**: Requires `canvas_id` from `canvases` list stream
**Incremental**: Yes — cursor field `time`, **max window 14 days** per request
**Response envelope field**: `data.stats` (records array is nested as `response["data"]["stats"]`)
**Primary key**: Composite `(canvas_id, time)`

**Request parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `canvas_id` | Yes | string | Canvas API identifier |
| `ending_at` | Yes | ISO-8601 datetime | End of export window |
| `starting_at` | Conditional | ISO-8601 datetime | Start of window; required if `length` not provided |
| `length` | Conditional | integer | Days before `ending_at` (1–14); required if `starting_at` not provided |
| `include_variant_breakdown` | No | boolean | Include variant stats; default false |
| `include_step_breakdown` | No | boolean | Include step stats; default false |
| `include_deleted_step_data` | No | boolean | Include deleted step stats; default false |

**Response**:

```json
{
  "data": {
    "name": "string",
    "stats": [
      {
        "time": "string (ISO 8601)",
        "total_stats": {
          "revenue": "float",
          "conversions": "integer",
          "conversions_by_entry_time": "integer",
          "entries": "integer"
        },
        "variant_stats": {},
        "step_stats": {}
      }
    ]
  },
  "message": "success"
}
```

**Schema** (per `data.stats[]` element):

| Field | Type | Notes |
|-------|------|-------|
| `time` | string (ISO 8601) | Date of the data point; incremental cursor |
| `total_stats.revenue` | float | Total revenue for the day |
| `total_stats.conversions` | integer | Total conversions |
| `total_stats.conversions_by_entry_time` | integer | Conversions attributed to entry time |
| `total_stats.entries` | integer | Total Canvas entries |
| `variant_stats` | object | Per-variant stats (optional): `{name, revenue, conversions, conversions_by_entry_time, entries}` |
| `step_stats` | object | Per-step stats (optional): `{name, revenue, conversions, entries, messages}` |

**Ingestion type**: `append` (max 14-day window per request; much shorter than other analytics endpoints)

---

### Stream: segments

**Endpoint**: `GET /segments/list`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/segments/get_segment/
**Rate limit**: 250,000 req/hr
**Paginated**: Yes (`page`, 0-indexed, 100 per page)
**Incremental filter**: None (no `last_edit.time[gt]` for segments)
**Response envelope field**: `segments`
**Primary key**: `id`

**Request parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `page` | No | integer | 0-indexed page; defaults to 0 |
| `sort_direction` | No | string | `asc` (default) or `desc` |

Note: Archived segments are not included in results.

**Response**:

```json
{
  "message": "success",
  "segments": [
    {
      "id": "string",
      "name": "string",
      "analytics_tracking_enabled": "boolean",
      "tags": ["string"]
    }
  ]
}
```

**Schema**:

| Field | Type | Notes |
|-------|------|-------|
| `id` | string | Segment API identifier; primary key |
| `name` | string | Segment name |
| `analytics_tracking_enabled` | boolean | Whether analytics tracking is enabled |
| `tags` | array[string] | Associated tag names |

**Ingestion type**: `snapshot`

---

### Stream: segments_details

**Endpoint**: `GET /segments/details`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/segments/get_segment_details/
**Rate limit**: 250,000 req/hr
**Paginated**: No
**Fan-out dependency**: Requires `segment_id` from `segments` list stream
**Primary key**: `segment_id` (synthetic; injected by connector)

**Request parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `segment_id` | Yes | string | Segment API identifier |

**Response**:

```json
{
  "message": "success",
  "created_at": "string (ISO 8601)",
  "updated_at": "string (ISO 8601)",
  "name": "string",
  "description": "string",
  "text_description": "string",
  "tags": ["string"],
  "teams": ["string"]
}
```

**Schema**:

| Field | Type | Notes |
|-------|------|-------|
| `segment_id` | string | Injected from parent; primary key |
| `created_at` | string (ISO 8601) | Creation timestamp |
| `updated_at` | string (ISO 8601) | Last update timestamp |
| `name` | string | Segment name |
| `description` | string | Human-readable filter description |
| `text_description` | string | Segment description text |
| `tags` | array[string] | Associated tags |
| `teams` | array[string] | Associated teams |

**Ingestion type**: `snapshot`

---

### Stream: segments_analytics

**Endpoint**: `GET /segments/data_series`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/segments/get_segment_analytics/
**Rate limit**: 250,000 req/hr
**Paginated**: No
**Fan-out dependency**: Requires `segment_id` from `segments` list stream
**Incremental**: Yes — cursor field `time`, max 100 days per request
**Response envelope field**: `data`
**Primary key**: Composite `(segment_id, time)`

**Request parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `segment_id` | Yes | string | Segment API identifier |
| `length` | Yes | integer | Days to include (1–100) |
| `ending_at` | No | ISO-8601 datetime | End of window; defaults to now |

**Response**:

```json
{
  "message": "success",
  "data": [
    {
      "time": "string (ISO 8601 date)",
      "size": "integer"
    }
  ]
}
```

**Schema** (per `data[]` element):

| Field | Type | Notes |
|-------|------|-------|
| `time` | string (ISO 8601 date) | Date of the measurement; incremental cursor |
| `size` | integer | Estimated segment size on that date |

Note: Returns estimated size. For exact membership, use `/users/export/segment` (not a connector stream due to async nature).

**Ingestion type**: `append`

---

### Stream: events

**Endpoint**: `GET /events/list`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/custom_events/get_custom_events/
**Rate limit**: 1,000 req/hr (shared with `/custom_attributes`, `/events`, `/purchases/product_list`)
**Paginated**: Yes (`page`, 0-indexed, **250 per page** — larger than other list endpoints)
**Response envelope field**: `events`
**Primary key**: The event name string itself (no `id` field; each element is a name string)

**Request parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `page` | No | integer | 0-indexed page; defaults to 0 |

**Response**:

```json
{
  "message": "success",
  "events": [
    "event_name_1",
    "event_name_2"
  ]
}
```

**Schema** (each element of `events[]` is a string):

| Field | Type | Notes |
|-------|------|-------|
| `event` | string | Custom event name; serves as the identifier for fan-out to `events_analytics` |

Note: There is also a richer `/events` endpoint (paginated via `cursor`, 50 per page) that returns event objects with `name`, `description`, `status`, `data_type`, `tag_names`. That endpoint shares the same 1,000 req/hr pool. Document as a possible enhancement (see `events_details` in Deferred Tables).

**Ingestion type**: `snapshot`

---

### Stream: events_analytics

**Endpoint**: `GET /events/data_series`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/custom_events/get_custom_events_analytics/
**Rate limit**: 250,000 req/hr
**Paginated**: No
**Fan-out dependency**: Requires event name from `events` list stream
**Incremental**: Yes — cursor field `time`, max 100 days per request
**Response envelope field**: `data`
**Primary key**: Composite `(event, time)`

**Request parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `event` | Yes | string | Custom event name |
| `length` | Yes | integer | Units to include (1–100) |
| `unit` | No | string | `day` (default) or `hour` |
| `ending_at` | No | ISO-8601 datetime | End of window; defaults to now |
| `app_id` | No | string | Filter to specific app |
| `segment_id` | No | string | Filter to specific segment |

**Response**:

```json
{
  "message": "success",
  "data": [
    {
      "time": "string (ISO 8601)",
      "count": "integer"
    }
  ]
}
```

**Schema** (per `data[]` element):

| Field | Type | Notes |
|-------|------|-------|
| `time` | string (ISO 8601) | Date/hour of the data point; incremental cursor |
| `count` | integer | Number of times the event occurred |

**Ingestion type**: `append`

---

### Stream: cards (News Feed — Deprecated)

**Endpoint**: `GET /feed/list`
**Official doc**: TBD — News Feed is deprecated; Braze official docs pages for `/feed/*` return 404 or redirect. Endpoint confirmed functional by Airbyte source connector (v0.4.20, manifest.yaml).
**Rate limit**: TBD (assumed 250,000 req/hr based on default pattern)
**Paginated**: Yes (`page`, 0-indexed, assumed 100 per page)
**Response envelope field**: `cards`
**Primary key**: `id`

**Request parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `page` | No | integer | 0-indexed page |
| `include_archived` | No | boolean | TBD |
| `sort_direction` | No | string | TBD |

**Response** (best-effort from Airbyte connector; official schema not publicly documented):

```json
{
  "message": "success",
  "cards": [
    {
      "id": "string",
      "name": "string",
      "tags": ["string"]
    }
  ]
}
```

**Schema** (best-effort):

| Field | Type | Notes |
|-------|------|-------|
| `id` | string | Card API identifier; primary key |
| `name` | string | Card name (best-effort) |
| `tags` | array[string] | Associated tags (best-effort) |

**Ingestion type**: `snapshot`

**Known quirk**: News Feed is a deprecated Braze feature. Braze recommends migrating to Content Cards. The `/feed/*` endpoints are still callable but undocumented. If the customer does not use News Feed, this stream will return an empty list.

---

### Stream: cards_analytics (News Feed — Deprecated)

**Endpoint**: `GET /feed/data_series`
**Official doc**: TBD — deprecated; confirmed by Airbyte manifest.
**Rate limit**: TBD (assumed 250,000 req/hr)
**Paginated**: No
**Fan-out dependency**: Requires `card_id` from `cards` list stream
**Incremental**: Yes — cursor `time`, assumed max 100 days
**Response envelope field**: `data`
**Primary key**: Composite `(card_id, time)` (best-effort)

**Request parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `card_id` | Yes | string | Card API identifier |
| `length` | Yes | integer | Days to include |
| `ending_at` | No | ISO-8601 datetime | End of window |

**Response** (best-effort):

```json
{
  "message": "success",
  "data": [
    {
      "time": "string (ISO 8601)",
      "clicks": "integer (best-effort)",
      "impressions": "integer (best-effort)",
      "unique_clicks": "integer (best-effort)",
      "unique_impressions": "integer (best-effort)"
    }
  ]
}
```

**Ingestion type**: `append`

---

### Stream: kpi_daily_new_users

**Endpoint**: `GET /kpi/new_users/data_series`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/kpi/get_kpi_daily_new_users_date/
**Rate limit**: 250,000 req/hr
**Paginated**: No
**Incremental**: Yes — cursor `time`, max 100 days per request
**Response envelope field**: `data`
**Primary key**: `time` (one record per day)

**Request parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `length` | Yes | integer | Days to include (1–100) |
| `ending_at` | No | ISO-8601 datetime | End of window; defaults to now |
| `app_id` | No | string | Filter to specific app |

**Response**:

```json
{
  "message": "success",
  "data": [
    {
      "time": "string (ISO 8601 date)",
      "new_users": "integer"
    }
  ]
}
```

**Schema**:

| Field | Type | Notes |
|-------|------|-------|
| `time` | string (ISO 8601 date) | Date; incremental cursor |
| `new_users` | integer | New user count for that day |

**Ingestion type**: `append`

---

### Stream: kpi_daily_active_users

**Endpoint**: `GET /kpi/dau/data_series`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/kpi/get_kpi_dau_date/
**Rate limit**: 250,000 req/hr
**Paginated**: No
**Incremental**: Yes — cursor `time`, max 100 days per request
**Response envelope field**: `data`
**Primary key**: `time`

**Request parameters**: Same as `kpi_daily_new_users` (length, ending_at, app_id).

**Response**:

```json
{
  "message": "success",
  "data": [
    {
      "time": "string (ISO 8601 date)",
      "dau": "integer"
    }
  ]
}
```

**Schema**:

| Field | Type | Notes |
|-------|------|-------|
| `time` | string (ISO 8601 date) | Date; incremental cursor |
| `dau` | integer | Daily active users for that day |

**Ingestion type**: `append`

---

### Stream: kpi_monthly_active_users

**Endpoint**: `GET /kpi/mau/data_series`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/kpi/get_kpi_mau_30_days/
**Rate limit**: 250,000 req/hr
**Paginated**: No
**Incremental**: Yes — cursor `time`, max 100 days per request
**Response envelope field**: `data`
**Primary key**: `time`

**Request parameters**: Same as `kpi_daily_new_users` (length, ending_at, app_id).

**Response**:

```json
{
  "message": "success",
  "data": [
    {
      "time": "string (ISO 8601 date)",
      "mau": "integer"
    }
  ]
}
```

**Schema**:

| Field | Type | Notes |
|-------|------|-------|
| `time` | string (ISO 8601 date) | Date; incremental cursor |
| `mau` | integer | 30-day rolling window monthly active users |

**Ingestion type**: `append`

---

### Stream: kpi_daily_app_uninstalls

**Endpoint**: `GET /kpi/uninstalls/data_series`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/kpi/get_kpi_uninstalls_date/
**Rate limit**: 250,000 req/hr
**Paginated**: No
**Incremental**: Yes — cursor `time`, max 100 days per request
**Response envelope field**: `data`
**Primary key**: `time`

**Request parameters**: Same as `kpi_daily_new_users` (length, ending_at, app_id).

**Response**:

```json
{
  "message": "success",
  "data": [
    {
      "time": "string (ISO 8601 date)",
      "uninstalls": "integer"
    }
  ]
}
```

**Schema**:

| Field | Type | Notes |
|-------|------|-------|
| `time` | string (ISO 8601 date) | Date; incremental cursor |
| `uninstalls` | integer | App uninstall count for that day |

**Ingestion type**: `append`

---

### Stream: purchases_product_list

**Endpoint**: `GET /purchases/product_list`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/purchases/get_list_product_id/
**Rate limit**: 1,000 req/hr (shared with `/custom_attributes`, `/events`, `/events/list`)
**Paginated**: Yes (`page`, 0-indexed; page size not explicitly documented)
**Response envelope field**: `products`
**Primary key**: The product name string itself

**Request parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `page` | No | string | Page to retrieve |

**Response**:

```json
{
  "products": ["product_name_1", "product_name_2"],
  "message": "success"
}
```

**Schema** (each element of `products[]` is a string):

| Field | Type | Notes |
|-------|------|-------|
| `product` | string | Product identifier name; also used as fan-out key for purchases analytics |

**Ingestion type**: `snapshot`

---

### Stream: purchases_quantity_series

**Endpoint**: `GET /purchases/quantity_series`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/purchases/get_number_of_purchases/
**Rate limit**: 1,000 req/hr (shared pool)
**Paginated**: No
**Incremental**: Yes — cursor `time`, max 100 days per request
**Response envelope field**: `data`
**Primary key**: Composite `(product, time)` if filtered; `time` otherwise

**Request parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `length` | Yes | integer | Days to include (1–100) |
| `ending_at` | No | ISO-8601 datetime | End of window; defaults to now |
| `unit` | No | string | `day` (default) or `hour` |
| `app_id` | No | string | Filter to specific app |
| `product` | No | string | Filter to specific product name |

**Response**:

```json
{
  "message": "success",
  "data": [
    {
      "time": "string (ISO 8601 date)",
      "purchase_quantity": "integer"
    }
  ]
}
```

**Schema**:

| Field | Type | Notes |
|-------|------|-------|
| `time` | string (ISO 8601) | Date/hour; incremental cursor |
| `purchase_quantity` | integer | Number of purchases in period |

**Ingestion type**: `append`

---

### Stream: purchases_revenue_series

**Endpoint**: `GET /purchases/revenue_series`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/purchases/get_revenue_series/
**Rate limit**: 1,000 req/hr (shared pool)
**Paginated**: No
**Incremental**: Yes — cursor `time`, max 100 days per request
**Response envelope field**: `data`
**Primary key**: Composite `(product, time)` if filtered; `time` otherwise

**Request parameters**: Same as `purchases_quantity_series` (length, ending_at, unit, app_id, product).

**Response**:

```json
{
  "message": "success",
  "data": [
    {
      "time": "string (ISO 8601 date)",
      "revenue": "integer"
    }
  ]
}
```

**Schema**:

| Field | Type | Notes |
|-------|------|-------|
| `time` | string (ISO 8601) | Date/hour; incremental cursor |
| `revenue` | integer | Revenue in USD for period |

Note: Docs show `revenue` as `int` but could be `float` in practice — best-effort; treat as numeric.

**Ingestion type**: `append`

---

### Stream: sends_analytics

**Endpoint**: `GET /sends/data_series`
**Official doc**: https://www.braze.com/docs/api/endpoints/export/campaigns/get_send_analytics/
**Rate limit**: 250,000 req/hr
**Paginated**: No
**Fan-out dependency**: Requires both `campaign_id` (from campaigns list) and `send_id` (from campaign metadata — see Known Quirks)
**Incremental**: Yes — cursor `time`, max 100 days per request; but **data only retained for 14 days after send**
**Response envelope field**: `data`
**Primary key**: Composite `(campaign_id, send_id, time)`

**Request parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `campaign_id` | Yes | string | Campaign API identifier |
| `send_id` | Yes | string | Send API identifier |
| `length` | Yes | integer | Days to include (1–100) |
| `ending_at` | No | ISO-8601 datetime | End of window; defaults to now |

**Important constraint**: Braze retains send analytics for **only 14 days** after the send date, regardless of `length` value.

**Response**:

```json
{
  "message": "success",
  "data": [
    {
      "time": "string (ISO 8601 date)",
      "messages": {},
      "sent": "integer",
      "delivered": "integer",
      "undelivered": "integer",
      "delivery_failed": "integer",
      "direct_opens": "integer",
      "total_opens": "integer",
      "bounces": "integer",
      "body_clicks": "integer",
      "revenue": "float",
      "unique_recipients": "integer",
      "conversions": "integer",
      "conversions_by_send_time": "integer",
      "conversions1": "integer (optional)",
      "conversions2": "integer (optional)",
      "conversions3": "integer (optional)"
    }
  ]
}
```

**Schema** (per `data[]` element):

| Field | Type | Notes |
|-------|------|-------|
| `time` | string (ISO 8601 date) | Date; incremental cursor |
| `messages` | object | Channel-specific metrics (same structure as campaigns_analytics) |
| `sent` | integer | Messages sent |
| `delivered` | integer | Messages delivered |
| `undelivered` | integer | Messages not delivered |
| `delivery_failed` | integer | Delivery failures |
| `direct_opens` | integer | Direct opens |
| `total_opens` | integer | Total opens |
| `bounces` | integer | Bounce count |
| `body_clicks` | integer | Body clicks |
| `revenue` | float | Revenue in USD |
| `unique_recipients` | integer | Unique recipients |
| `conversions` | integer | Conversion count |
| `conversions_by_send_time` | integer | Conversions by send time |
| `conversions1`, `conversions2`, `conversions3` | integer | Secondary/tertiary conversions (optional) |

**Ingestion type**: `append`

---

## Get Object Primary Keys

| Stream | Primary Key | Notes |
|--------|-------------|-------|
| `campaigns` | `id` | Campaign API identifier |
| `campaigns_details` | `campaign_id` | Synthetic; injected from parent |
| `campaigns_analytics` | `(campaign_id, time)` | Composite |
| `canvases` | `id` | Canvas API identifier |
| `canvases_details` | `canvas_id` | Synthetic; injected from parent |
| `canvases_analytics` | `(canvas_id, time)` | Composite |
| `segments` | `id` | Segment API identifier |
| `segments_details` | `segment_id` | Synthetic; injected from parent |
| `segments_analytics` | `(segment_id, time)` | Composite |
| `events` | `event` (name string) | No dedicated ID; name is the identifier |
| `events_analytics` | `(event, time)` | Composite |
| `cards` | `id` | Card API identifier (best-effort; deprecated) |
| `cards_analytics` | `(card_id, time)` | Composite (best-effort; deprecated) |
| `kpi_daily_new_users` | `time` | One record per day per workspace/app |
| `kpi_daily_active_users` | `time` | One record per day |
| `kpi_monthly_active_users` | `time` | One record per day (30-day rolling) |
| `kpi_daily_app_uninstalls` | `time` | One record per day |
| `purchases_product_list` | `product` (name string) | Product name is identifier |
| `purchases_quantity_series` | `time` | One record per day (or hour) per product |
| `purchases_revenue_series` | `time` | One record per day (or hour) per product |
| `sends_analytics` | `(campaign_id, send_id, time)` | Composite |

---

## Object Ingestion Type Summary

| Stream | Ingestion Type | Cursor / Filter | Notes |
|--------|---------------|-----------------|-------|
| `campaigns` | `snapshot` | `last_edit.time[gt]` | Server-side filter; reduces payload |
| `campaigns_details` | `snapshot` | none | Full re-fetch per campaign |
| `campaigns_analytics` | `append` | `time` (date-window) | Up to 100-day window per request |
| `canvases` | `snapshot` | `last_edit.time[gt]` | Server-side filter |
| `canvases_details` | `snapshot` | none | Full re-fetch |
| `canvases_analytics` | `append` | `time` (date-window) | Max 14-day window; more frequent requests needed |
| `segments` | `snapshot` | none | No edit-time filter available |
| `segments_details` | `snapshot` | none | Full re-fetch |
| `segments_analytics` | `append` | `time` (date-window) | Up to 100-day window |
| `events` | `snapshot` | none | Small list; full refresh is cheap |
| `events_analytics` | `append` | `time` (date-window) | Up to 100-day window |
| `cards` | `snapshot` | none | Deprecated endpoint |
| `cards_analytics` | `append` | `time` (date-window) | Deprecated endpoint |
| `kpi_daily_new_users` | `append` | `time` (date-window) | Workspace-level KPI |
| `kpi_daily_active_users` | `append` | `time` (date-window) | Workspace-level KPI |
| `kpi_monthly_active_users` | `append` | `time` (date-window) | 30-day rolling window |
| `kpi_daily_app_uninstalls` | `append` | `time` (date-window) | Workspace-level KPI |
| `purchases_product_list` | `snapshot` | none | Product names rarely change |
| `purchases_quantity_series` | `append` | `time` (date-window) | Up to 100-day window |
| `purchases_revenue_series` | `append` | `time` (date-window) | Up to 100-day window |
| `sends_analytics` | `append` | `time` (date-window) | 14-day retention limit |

---

## Read API for Data Retrieval

### General read pattern

```python
import requests

BASE_URL = "https://rest.iad-01.braze.com"  # from connector config
API_KEY  = "<REST API KEY>"                  # from connector config

headers = {"Authorization": f"Bearer {API_KEY}"}

# Example: paginated list
page = 0
while True:
    resp = requests.get(
        f"{BASE_URL}/campaigns/list",
        headers=headers,
        params={"page": page, "last_edit.time[gt]": "2024-01-01T00:00:00"}
    )
    resp.raise_for_status()
    body = resp.json()
    records = body["campaigns"]
    if not records:
        break
    yield from records
    page += 1
```

### Date-window analytics pattern

```python
from datetime import datetime, timedelta

def fetch_analytics_window(campaign_id, start_dt, end_dt):
    """Fetches analytics in <=100-day windows."""
    cursor = end_dt
    while cursor > start_dt:
        window_start = max(cursor - timedelta(days=100), start_dt)
        length = (cursor - window_start).days or 1
        resp = requests.get(
            f"{BASE_URL}/campaigns/data_series",
            headers=headers,
            params={
                "campaign_id": campaign_id,
                "length": length,
                "ending_at": cursor.isoformat()
            }
        )
        resp.raise_for_status()
        yield from resp.json()["data"]
        cursor = window_start
```

### Rate limit handling

```python
import time

def request_with_retry(url, headers, params, max_retries=5):
    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 429:
            reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
            sleep_secs = max(reset - time.time(), 1)
            time.sleep(sleep_secs)
            continue
        elif resp.status_code >= 500:
            time.sleep(2 ** attempt)  # exponential backoff
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Exhausted retries for {url}")
```

---

## Recommended Stream Table

| Stream | Endpoint | Primary Key | Paginated? | Incremental? | Fan-out Dependency |
|--------|----------|-------------|------------|-------------|-------------------|
| `campaigns` | `GET /campaigns/list` | `id` | Yes (page, 100/pg) | Yes (`last_edit.time[gt]`) | — |
| `campaigns_details` | `GET /campaigns/details` | `campaign_id` | No | No | `campaigns.id` |
| `campaigns_analytics` | `GET /campaigns/data_series` | `(campaign_id, time)` | No | Yes (time cursor, 100d) | `campaigns.id` |
| `canvases` | `GET /canvas/list` | `id` | Yes (page, 100/pg) | Yes (`last_edit.time[gt]`) | — |
| `canvases_details` | `GET /canvas/details` | `canvas_id` | No | No | `canvases.id` |
| `canvases_analytics` | `GET /canvas/data_series` | `(canvas_id, time)` | No | Yes (time cursor, **14d max**) | `canvases.id` |
| `segments` | `GET /segments/list` | `id` | Yes (page, 100/pg) | No | — |
| `segments_details` | `GET /segments/details` | `segment_id` | No | No | `segments.id` |
| `segments_analytics` | `GET /segments/data_series` | `(segment_id, time)` | No | Yes (time cursor, 100d) | `segments.id` |
| `events` | `GET /events/list` | `event` (name) | Yes (page, 250/pg) | No | — |
| `events_analytics` | `GET /events/data_series` | `(event, time)` | No | Yes (time cursor, 100d) | `events.event` |
| `cards` | `GET /feed/list` | `id` | Yes (page, ~100/pg) | No | — |
| `cards_analytics` | `GET /feed/data_series` | `(card_id, time)` | No | Yes (time cursor, 100d) | `cards.id` |
| `kpi_daily_new_users` | `GET /kpi/new_users/data_series` | `time` | No | Yes (time cursor, 100d) | — |
| `kpi_daily_active_users` | `GET /kpi/dau/data_series` | `time` | No | Yes (time cursor, 100d) | — |
| `kpi_monthly_active_users` | `GET /kpi/mau/data_series` | `time` | No | Yes (time cursor, 100d) | — |
| `kpi_daily_app_uninstalls` | `GET /kpi/uninstalls/data_series` | `time` | No | Yes (time cursor, 100d) | — |
| `purchases_product_list` | `GET /purchases/product_list` | `product` (name) | Yes (page) | No | — |
| `purchases_quantity_series` | `GET /purchases/quantity_series` | `(time[, product])` | No | Yes (time cursor, 100d) | — |
| `purchases_revenue_series` | `GET /purchases/revenue_series` | `(time[, product])` | No | Yes (time cursor, 100d) | — |
| `sends_analytics` | `GET /sends/data_series` | `(campaign_id, send_id, time)` | No | Yes (time cursor, 14d retention) | `campaigns.id` + `send_id` |

---

## Field Type Mapping

| API type | Spark/Python type | Notes |
|----------|------------------|-------|
| `string` (general) | `StringType` | — |
| `string` (ISO 8601 date `YYYY-MM-DD`) | `DateType` or `StringType` | Parse to date if needed |
| `string` (ISO 8601 datetime) | `TimestampType` or `StringType` | Parse with `datetime.fromisoformat()` |
| `integer` | `LongType` | Use Long to avoid overflow on counts |
| `float` | `DoubleType` | Revenue fields |
| `boolean` | `BooleanType` | — |
| `array[string]` | `ArrayType(StringType)` | tags, teams, channels |
| `object` (nested) | `MapType` or `StructType` | messages, conversion_behaviors, steps |
| `null` / absent field | nullable | All optional fields should be nullable |

---

## Known Quirks

1. **News Feed deprecation**: The `/feed/list` and `/feed/data_series` endpoints (used by `cards` and `cards_analytics` streams) are deprecated. Braze's official documentation pages for these endpoints 404 or redirect. The endpoints remain callable but schemas are unverified against current docs. The Airbyte connector (v0.4.20) still uses them. Recommend making these streams optional in the connector config.

2. **Canvas analytics max window is only 14 days**: Unlike all other analytics endpoints that accept up to 100 days, `/canvas/data_series` accepts a maximum `length` of 14 days. The connector must issue more frequent requests to cover historical data.

3. **Send analytics 14-day retention**: `/sends/data_series` only holds data for 14 days after the send. Requesting older data returns empty results, not an error. Historical backfill beyond 14 days is not possible.

4. **`send_id` availability**: The `sends_analytics` stream requires a `send_id`, which is an optional field on campaigns. Not all campaigns have `send_id` values. The connector should skip fan-out to `sends/data_series` when no `send_id` is available.

5. **Campaigns data_series rate limit anomaly**: `/campaigns/data_series` has a rate limit of 50,000 req/min (much higher than the default 250,000/hour). This is intentional for high-volume analytics consumers but means the per-minute bucket is actually the constraint for bursty workloads.

6. **Rate limit window resets at clock-hour boundaries**: The `X-RateLimit-Reset` header gives the exact reset time. When close to the limit, the connector should sleep until reset rather than using fixed sleep intervals.

7. **Segment analytics returns estimated size**: `/segments/data_series` returns an estimated segment size (`size`), not exact membership. The exact user list is available via `/users/export/segment` (async, not suitable for this connector).

8. **Events list vs. events detail**: `/events/list` returns only event names (strings). The newer `/events` endpoint (with cursor pagination) returns full event metadata (description, status, tags). Airbyte uses the simpler `/events/list`; consider using `/events` for richer data.

9. **`last_edit.time[gt]` filter on campaigns/canvases**: This parameter filters server-side. When used, the connector still gets up to 100 records per page, but only records edited after the timestamp. This enables efficient incremental list sync without scanning the full catalog.

10. **Braze instance endpoint is required at connection time**: Unlike APIs with a single global base URL, Braze requires the customer to supply their specific REST endpoint (e.g., `https://rest.iad-05.braze.com`). This must be a required field in the connector configuration.

---

## Deferred Tables

The following streams exist in the Braze API and are relevant for analytics ingestion but are deferred due to complexity, async behavior, or lower priority:

| Stream | Endpoint | Why Deferred |
|--------|----------|-------------|
| `sessions_analytics` | `GET /sessions/data_series` | Low priority; covered by KPI streams for most use cases. Simple pattern (identical to KPI streams). Defer until requested. |
| `events_details` | `GET /events` (cursor-paginated) | Returns richer event metadata than `/events/list`. Different pagination (cursor vs page). Low priority for initial build. |
| `custom_attributes` | `GET /custom_attributes` | Cursor-paginated (not page-indexed). Metadata-only; no analytics. Low priority. |
| `user_export_segment` | `POST /users/export/segment` | Async endpoint: initiates export, must poll for completion, then download from S3/Azure/GCS. Completely different pattern from synchronous endpoints. High complexity; requires separate async job management. |
| `user_export_ids` | `POST /users/export/ids` | Same async pattern as `user_export_segment`. |
| `canvas_data_summary` | `GET /canvas/data_summary` | Aggregate/rollup endpoint (not a time series). Useful for single-canvas summaries but overlaps with `canvases_analytics`. Defer until requested. |
| `content_cards` | TBD | Content Cards (the modern replacement for News Feed) do not have a direct export API in the same REST pattern. Data is available via Braze Currents (streaming) or user-level export only. High complexity; defer. |

---

## Sources and References

### Research Log

| Source Type | URL | Accessed | Confidence | What it confirmed |
|-------------|-----|----------|------------|-------------------|
| Official Docs | https://www.braze.com/docs/api/basics/ | 2026-09-06 | High | Base URLs per instance, auth method, rate limit overview |
| Official Docs | https://www.braze.com/docs/api/basics/#endpoints | 2026-09-06 | High | Full list of 15 REST instance endpoints with dashboard URLs |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/campaigns/get_campaigns/ | 2026-09-06 | High | /campaigns/list endpoint path, params, response schema, rate limit |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/campaigns/get_campaign_analytics/ | 2026-09-06 | High | /campaigns/data_series params, response, 50K/min rate limit |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/campaigns/get_campaign_details/ | 2026-09-06 | High | /campaigns/details full response schema including messages object |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/canvas/get_canvases/ | 2026-09-06 | High | /canvas/list endpoint params and response |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/canvas/get_canvas_details/ | 2026-09-06 | High | /canvas/details full response schema |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/canvas/get_canvas_analytics/ | 2026-09-06 | High | /canvas/data_series params, 14-day max window, nested data.stats |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/canvas/get_canvas_analytics_summary/ | 2026-09-06 | High | /canvas/data_summary confirmed as separate rollup endpoint |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/segments/get_segment/ | 2026-09-06 | High | /segments/list endpoint params, response, no last_edit filter |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/segments/get_segment_details/ | 2026-09-06 | High | /segments/details full response schema |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/segments/get_segment_analytics/ | 2026-09-06 | High | /segments/data_series params, size field, 100-day max |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/custom_events/get_custom_events/ | 2026-09-06 | High | /events/list endpoint, 250 per page, 1K/hr shared rate limit |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/custom_events/get_custom_events_analytics/ | 2026-09-06 | High | /events/data_series params, count field, unit (day/hour) |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/kpi/get_kpi_daily_new_users_date/ | 2026-09-06 | High | /kpi/new_users/data_series endpoint, params, new_users field |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/kpi/get_kpi_dau_date/ | 2026-09-06 | High | /kpi/dau/data_series endpoint, dau field |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/kpi/get_kpi_mau_30_days/ | 2026-09-06 | High | /kpi/mau/data_series endpoint, mau field, 30-day rolling window |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/kpi/get_kpi_uninstalls_date/ | 2026-09-06 | High | /kpi/uninstalls/data_series endpoint, uninstalls field |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/purchases/get_list_product_id/ | 2026-09-06 | High | /purchases/product_list params, products array, 1K/hr shared limit |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/purchases/get_number_of_purchases/ | 2026-09-06 | High | /purchases/quantity_series params, purchase_quantity field |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/purchases/get_revenue_series/ | 2026-09-06 | High | /purchases/revenue_series params, revenue field |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/campaigns/get_send_analytics/ | 2026-09-06 | High | /sends/data_series params, response, 14-day retention constraint |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/sessions/get_sessions_analytics/ | 2026-09-06 | High | /sessions/data_series params and response |
| Official Docs | https://www.braze.com/docs/api/errors/ | 2026-09-06 | High | HTTP error codes, response envelope, retryable vs non-retryable |
| Official Docs | https://www.braze.com/docs/api/api_limits/ | 2026-09-06 | High | Per-endpoint rate limits, X-RateLimit-* headers, hourly reset |
| Official Docs | https://www.braze.com/docs/api/endpoints/export/custom_attributes/get_custom_attributes/ | 2026-09-06 | High | /custom_attributes cursor pagination pattern |
| Official Docs | https://www.braze.com/docs/api/identifier_types/ | 2026-09-06 | High | All identifier types (campaign, canvas, segment, send, subscription group) |
| Airbyte OSS | https://docs.airbyte.com/integrations/sources/braze | 2026-09-06 | High | Confirmed 13 streams, incremental support, required config fields |
| Airbyte OSS | https://raw.githubusercontent.com/airbytehq/airbyte/master/airbyte-integrations/connectors/source-braze/manifest.yaml | 2026-09-06 | High | All stream endpoints, pagination config, cursor fields, response field paths; confirmed /feed/list and /feed/data_series for cards streams |
| Official Docs (404) | https://www.braze.com/docs/api/endpoints/export/news_feed/ | 2026-09-06 | Low | News Feed endpoint pages 404/redirect — confirms deprecation |
