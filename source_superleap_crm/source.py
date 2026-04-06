import logging
from typing import Any, List, Mapping, Optional, Tuple

import requests as req
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

from source_superleap_crm.streams import SuperleapStream

logger = logging.getLogger("airbyte")


class SourceSuperleapCrm(AbstractSource):

    @staticmethod
    def _base_url(config: Mapping[str, Any]) -> str:
        return config.get("base_url", "https://app.superleap.com/").rstrip("/")

    @staticmethod
    def _headers(config: Mapping[str, Any]) -> dict:
        return {"Authorization": f"Bearer {config['api_key']}"}

    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, Optional[str]]:
        url = f"{self._base_url(config)}/api/v1/airbyte/verify/"
        try:
            resp = req.get(url, headers=self._headers(config), timeout=30)
            if resp.status_code == 200:
                body = resp.json()
                if body.get("status") == "ok":
                    return True, None
                return False, f"Unexpected response: {body}"
            return False, f"Connection check failed with status {resp.status_code}: {resp.text}"
        except Exception as e:
            return False, f"Connection error: {str(e)}"

    def _list_entities(self, config: Mapping[str, Any]) -> List[dict]:
        url = f"{self._base_url(config)}/api/v1/airbyte/objects/list/"
        resp = req.post(url, headers=self._headers(config), json={}, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if data.get("success") and data.get("data"):
            return data["data"]
        return []

    def _get_entity_fields(self, config: Mapping[str, Any], entity_id: str) -> List[dict]:
        url = f"{self._base_url(config)}/api/v1/airbyte/objects/{entity_id}"
        resp = req.get(url, headers=self._headers(config), timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if data.get("success") and data.get("data"):
            return data["data"].get("fields", [])
        return []

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = TokenAuthenticator(token=config["api_key"])
        entities = self._list_entities(config)
        streams = []

        for entity in entities:
            entity_id = entity.get("entity_identifier")
            if not entity_id:
                continue

            fields = self._get_entity_fields(config, entity_id)

            # Extract field names, skip any with null/empty field_name
            field_names = [
                f["field_name"]
                for f in fields
                if f.get("field_name")
            ]

            if not field_names:
                logger.warning(f"Skipping entity '{entity_id}': no valid fields found")
                continue

            stream = SuperleapStream(
                config=config,
                authenticator=authenticator,
                entity_identifier=entity_id,
                field_names=field_names,
                field_definitions=fields,
            )
            streams.append(stream)

        logger.info(f"Discovered {len(streams)} streams: {[s.name for s in streams]}")
        return streams
