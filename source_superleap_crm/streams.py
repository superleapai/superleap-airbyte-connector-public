import logging
from datetime import datetime, timedelta

from dateutil.parser import isoparse
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_cdk.models import SyncMode
import requests as req

logger = logging.getLogger("airbyte")

# Map Superleap data types to JSON Schema types
TYPE_MAPPING = {
    "String": {"type": ["null", "string"]},
    "Integer": {"type": ["null", "integer"]},
    "Float": {"type": ["null", "number"]},
    "Double": {"type": ["null", "number"]},
    "Long": {"type": ["null", "integer"]},
    "Short": {"type": ["null", "integer"]},
    "BigInteger": {"type": ["null", "integer"]},
    "BigDecimal": {"type": ["null", "number"]},
    "Boolean": {"type": ["null", "boolean"]},
    "Date": {"type": ["null", "string"], "format": "date"},
    "DateTime": {"type": ["null", "string"], "format": "date-time"},
}


class SuperleapStream(HttpStream):
    primary_key = "id"
    cursor_field = "updated_at"

    def __init__(
        self,
        config: Mapping[str, Any],
        authenticator: TokenAuthenticator,
        entity_identifier: str,
        field_names: Optional[List[str]] = None,
        field_definitions: Optional[List[dict]] = None,
        **kwargs,
    ):
        self._config = config
        self._entity_identifier = entity_identifier
        self._field_names = field_names
        self._field_definitions = field_definitions
        self._fields_loaded = field_definitions is not None
        self._cursor_value: Optional[str] = None
        self._prior_cursor: Optional[str] = None
        now = datetime.utcnow()
        self._sync_start_ts: str = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"
        self._datetime_fields: Optional[List[str]] = None
        self._catalog_json_schema: Optional[Mapping[str, Any]] = None
        super().__init__(authenticator=authenticator, **kwargs)

    def set_catalog_schema(self, json_schema: Mapping[str, Any]) -> None:
        """Derive field names and datetime info from the configured catalog schema."""
        self._catalog_json_schema = json_schema
        properties = json_schema.get("properties", {})
        self._field_names = list(properties.keys())
        self._datetime_fields = [
            fname for fname, schema in properties.items()
            if schema.get("format") == "date-time"
        ]
        self._fields_loaded = True
        logger.info(f"[{self.name}] Using catalog schema ({len(self._field_names)} fields)")

    def _ensure_fields_loaded(self) -> None:
        """Lazily fetch field definitions from the API on first access."""
        if self._fields_loaded:
            return
        base = self._config.get("base_url", "https://app.superleap.com/").rstrip("/")
        url = f"{base}/api/v1/airbyte/objects/{self._entity_identifier}"
        headers = {"Authorization": f"Bearer {self._config['api_key']}"}
        resp = req.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        fields = []
        if data.get("success") and data.get("data"):
            fields = data["data"].get("fields", [])
        self._field_definitions = fields
        self._field_names = [f["field_name"] for f in fields if f.get("field_name")]
        self._fields_loaded = True
        logger.info(f"[{self.name}] Loaded {len(self._field_names)} fields on demand")

    @property
    def name(self) -> str:
        return self._entity_identifier

    @property
    def url_base(self) -> str:
        base = self._config.get("base_url", "https://app.superleap.com/").rstrip("/")
        return f"{base}/api/v1/airbyte/"

    def path(self, **kwargs) -> str:
        return f"objects/{self._entity_identifier}/records/"

    @property
    def http_method(self) -> str:
        return "POST"

    # -- Retry -----------------------------------------------------------------

    @property
    def max_retries(self) -> int:
        return 3

    @property
    def retry_factor(self) -> float:
        return 1.0

    def should_retry(self, response: req.Response) -> bool:
        return response.status_code in (429, 500, 502, 503, 504)

    # -- Sync modes ------------------------------------------------------------

    @property
    def supports_incremental(self) -> bool:
        if self._field_names is not None:
            return "updated_at" in self._field_names
        self._ensure_fields_loaded()
        return any(
            f.get("field_name") == "updated_at"
            for f in self._field_definitions
        )

    @property
    def source_defined_cursor(self) -> bool:
        return True

    @property
    def supported_sync_modes(self) -> List[SyncMode]:
        modes = [SyncMode.full_refresh]
        if self.supports_incremental:
            modes.append(SyncMode.incremental)
        return modes

    # -- Schema ----------------------------------------------------------------

    def get_json_schema(self) -> Mapping[str, Any]:
        if self._catalog_json_schema is not None:
            return self._catalog_json_schema

        self._ensure_fields_loaded()
        properties = {}
        for field in self._field_definitions:
            fname = field.get("field_name")
            if not fname:
                continue
            dtype = field.get("data_type", "String")
            properties[fname] = TYPE_MAPPING.get(dtype, {"type": ["null", "string"]})

        return {
            "type": "object",
            "additionalProperties": True,
            "properties": properties,
        }

    # -- Request body ----------------------------------------------------------

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Any] = None,
    ) -> Optional[Mapping[str, Any]]:
        self._ensure_fields_loaded()
        query: dict = {"fields": self._field_names}

        # Build incremental filter using prior cursor (not stream_state, which
        # may already reflect _sync_start_ts from the state getter)
        cursor_ts = self._prior_cursor or self._config.get("start_date")

        if cursor_ts:
            epoch_ms = self._to_epoch_ms(cursor_ts)
            if epoch_ms:
                # Subtract grace period to account for replication lag
                grace_minutes = self._config.get("replication_lag_minutes", 5)
                grace_ms = grace_minutes * 60 * 1000
                epoch_ms = max(0, epoch_ms - grace_ms)
                query["filter"] = {
                    "and": [
                        {
                            "field": "updated_at",
                            "operator": "gte",
                            "value": epoch_ms,
                        }
                    ]
                }

        return {
            "query": query,
            "next_token": next_page_token,
        }

    # -- Pagination ------------------------------------------------------------

    def next_page_token(
        self, response: req.Response
    ) -> Optional[Mapping[str, Any]]:
        data = response.json()
        if data.get("success") and data.get("data"):
            token = data["data"].get("next_token")
            if token and str(token) not in ("None", ""):
                return token
        return None

    # -- Record parsing --------------------------------------------------------

    def parse_response(
        self,
        response: req.Response,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Any] = None,
        **kwargs,
    ) -> Iterable[Mapping[str, Any]]:
        data = response.json()
        if not data.get("success") or not data.get("data"):
            logger.warning(
                f"[{self.name}] Unexpected response: {data}"
            )
            return

        records = data["data"].get("records", [])
        if records is None:
            return

        for record in records:
            self._normalize_datetimes(record)
            yield record

    # -- State management (incremental) ----------------------------------------

    @property
    def state(self) -> MutableMapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._sync_start_ts}
        return {}

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self._cursor_value = value.get(self.cursor_field)
        self._prior_cursor = self._cursor_value

    # -- Helpers ---------------------------------------------------------------

    def _normalize_datetimes(self, record: dict) -> None:
        """Normalize datetime strings to consistent 3-digit millisecond format.

        Some destinations fail to parse fractional seconds with fewer than 3 digits
        (e.g. '2026-04-02T15:26:58.25Z' vs '2026-04-02T15:26:58.250Z').
        """
        # Determine which fields are datetime — from catalog or API field definitions
        if self._datetime_fields is not None:
            dt_fields = self._datetime_fields
        else:
            dt_fields = [
                f.get("field_name") for f in (self._field_definitions or [])
                if f.get("data_type") in ("DateTime", "Date") and f.get("field_name")
            ]

        for fname in dt_fields:
            if fname not in record or record[fname] is None:
                continue
            try:
                dt = isoparse(str(record[fname]))
                record[fname] = dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"
            except (ValueError, TypeError):
                pass

    @staticmethod
    def _to_epoch_ms(timestamp_str) -> Optional[int]:
        if isinstance(timestamp_str, (int, float)):
            return int(timestamp_str)
        try:
            ts = str(timestamp_str)
            if ts.endswith("Z"):
                dt = datetime.fromisoformat(ts[:-1] + "+00:00")
            else:
                dt = datetime.fromisoformat(ts)
            return int(dt.timestamp() * 1000)
        except (ValueError, TypeError):
            logger.warning(f"Could not parse timestamp: {timestamp_str}")
            return None
