# Superleap CRM - Airbyte Source Connector

An Airbyte source connector for [Superleap CRM](https://app.superleap.com/) that syncs all your CRM entities (leads, opportunities, users, call logs, etc.) into any Airbyte destination.

The connector dynamically discovers all available entities and their schemas from your Superleap instance — no manual stream configuration needed.

## Features

- Dynamic entity discovery — automatically detects all CRM objects available in your account
- Supports **full refresh** and **incremental sync** (via `updated_at` cursor)
- Schema inference from Superleap field definitions
- Cursor-based pagination

## Prerequisites

- A Superleap CRM account
- An API key (generate one at https://app.superleap.com/settings/apiAccess)
- An Airbyte instance (OSS, Cloud, or self-hosted)

## Setup

### 1. Add the connector to Airbyte

In your Airbyte instance, go to **Settings > Sources > + New Connector** and add a custom Docker connector:

| Field | Value |
|---|---|
| Connector name | `source-superleap-crm` |
| Docker image | `ghcr.io/superleapai/source-superleap-crm` |
| Docker tag | `0.3.6` |
| Connector type | Source |

> **Important:** Always pin to a specific version tag (e.g. `0.3.6`). Avoid using `latest` as breaking changes between versions can disrupt your syncs.

### 2. Configure the source

When creating a new source connection, provide:

| Parameter | Required | Description |
|---|---|---|
| `api_key` | Yes | Your Superleap API key |
| `base_url` | No | Base URL of your Superleap instance (defaults to `https://app.superleap.com/`) |
| `start_date` | No | ISO 8601 start date for incremental syncs (defaults to `2024-01-01T00:00:00Z`). Only records updated after this date will be synced on the first run. |

Example config:

```json
{
  "api_key": "YOUR_API_KEY_HERE",
  "base_url": "https://app.superleap.com/",
  "start_date": "2024-01-01T00:00:00Z"
}
```

### 3. Select streams and sync

After configuring the source, Airbyte will discover all available streams from your Superleap account. Select the ones you want to sync, choose a sync mode (full refresh or incremental), and start syncing.

## Building from source

If you want to build the Docker image yourself:

```bash
git clone https://github.com/superleap-ai/superleap-airbyte-connector-public.git
cd superleap-airbyte-connector-public
docker build -t source-superleap-crm:0.3.6 .
```

Then use your locally built image name when adding the connector to Airbyte.

## Sync modes

| Mode | Supported |
|---|---|
| Full Refresh | Yes |
| Incremental | Yes (for entities with an `updated_at` field) |

## Version history

| Version | Notes |
|---|---|
| 0.3.6 | Skip /objects/list during read, create streams directly from catalog |
| 0.3.5 | Simplify state management to just prior_cursor and sync_start_ts |
| 0.3.4 | Fix incremental filter to use prior cursor, state advances on 0 records |
| 0.3.3 | Remove deprecated get_updated_state, use CheckpointMixin state getter/setter |
| 0.3.2 | Use sync start time as cursor, simplify state management |
| 0.3.1 | Make start_date optional, fetch all records on first run if not provided |
| 0.3.0 | Lazy field loading, catalog schema reuse, replication lag grace period |
| 0.2.1 | Fix datetime normalization for consistent fractional seconds |
| 0.2.0 | Public release |

## API documentation

https://docs.superleap.com/api-reference/airbyte/introduction
