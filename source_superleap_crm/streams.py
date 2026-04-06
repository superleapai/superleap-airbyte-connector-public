import logging
from datetime import datetime

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
        field_names: List[str],
        field_definitions: List[dict],
        **kwargs,
    ):
        self._config = config
        self._entity_identifier = entity_identifier
        self._field_names = field_names
        self._field_definitions = field_definitions
        self._cursor_value: Optional[str] = None
        super().__init__(authenticator=authenticator, **kwargs)

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
        # Only support incremental if entity has an updated_at field
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
        query: dict = {"fields": self._field_names}

        # Build incremental filter
        cursor_ts = None
        if stream_state and self.cursor_field in stream_state:
            cursor_ts = stream_state[self.cursor_field]
        elif self._config.get("start_date"):
            cursor_ts = self._config["start_date"]

        if cursor_ts:
            epoch_ms = self._to_epoch_ms(cursor_ts)
            if epoch_ms:
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

            # Track cursor for state
            record_cursor = record.get(self.cursor_field)
            if record_cursor:
                if self._cursor_value is None or record_cursor > self._cursor_value:
                    self._cursor_value = record_cursor

            yield record

    # -- State management (incremental) ----------------------------------------

    @property
    def state(self) -> MutableMapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value}
        return {}

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self._cursor_value = value.get(self.cursor_field)

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> MutableMapping[str, Any]:
        latest_cursor = latest_record.get(self.cursor_field, "")
        current_cursor = current_stream_state.get(self.cursor_field, "")
        return {self.cursor_field: max(latest_cursor, current_cursor)}

    # -- Helpers ---------------------------------------------------------------

    def _normalize_datetimes(self, record: dict) -> None:
        """Normalize datetime strings to consistent 3-digit millisecond format.

        Some destinations fail to parse fractional seconds with fewer than 3 digits
        (e.g. '2026-04-02T15:26:58.25Z' vs '2026-04-02T15:26:58.250Z').
        """
        for field in self._field_definitions:
            if field.get("data_type") not in ("DateTime", "Date"):
                continue
            fname = field.get("field_name")
            if not fname or fname not in record or record[fname] is None:
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
