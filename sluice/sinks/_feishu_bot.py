"""Feishu Bot API client (app_id + app_secret auth)."""

from __future__ import annotations

import json
import time

import httpx

_FEISHU_BASE = "https://open.feishu.cn/open-apis"


class FeishuBotClient:
    """Async Feishu Bot API client with automatic tenant_access_token caching."""

    def __init__(
        self,
        app_id: str,
        app_secret: str,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._app_id = app_id
        self._app_secret = app_secret
        self._token: str | None = None
        self._token_exp: float = 0.0
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=30.0)

    async def _get_token(self) -> str:
        """Return a valid tenant_access_token, refreshing 60 s before expiry."""
        now = time.monotonic()
        if self._token is not None and now < self._token_exp - 60:
            return self._token

        resp = await self._client.post(
            f"{_FEISHU_BASE}/auth/v3/tenant_access_token/internal",
            json={"app_id": self._app_id, "app_secret": self._app_secret},
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code", -1) != 0:
            code = data.get("code")
            msg = data.get("msg")
            raise RuntimeError(
                f"feishu get tenant_access_token failed: code={code} msg={msg}"
            )

        self._token = data["tenant_access_token"]
        self._token_exp = now + float(data.get("expire", 7200))
        return self._token  # type: ignore[return-value]

    async def send_post(
        self,
        receive_id: str,
        receive_id_type: str,
        post_content: dict,
    ) -> str:
        """Send a rich-text post message. Returns message_id."""
        token = await self._get_token()
        resp = await self._client.post(
            f"{_FEISHU_BASE}/im/v1/messages",
            params={"receive_id_type": receive_id_type},
            headers={"Authorization": f"Bearer {token}"},
            json={
                "receive_id": receive_id,
                "msg_type": "post",
                "content": json.dumps(post_content),
            },
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code", -1) != 0:
            raise RuntimeError(
                f"feishu send_post failed: code={data.get('code')} msg={data.get('msg')}"
            )
        return data["data"]["message_id"]

    async def send_text(
        self,
        receive_id: str,
        receive_id_type: str,
        text: str,
    ) -> str:
        """Send a plain-text message. Returns message_id."""
        token = await self._get_token()
        resp = await self._client.post(
            f"{_FEISHU_BASE}/im/v1/messages",
            params={"receive_id_type": receive_id_type},
            headers={"Authorization": f"Bearer {token}"},
            json={
                "receive_id": receive_id,
                "msg_type": "text",
                "content": json.dumps({"text": text}),
            },
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code", -1) != 0:
            raise RuntimeError(
                f"feishu send_text failed: code={data.get('code')} msg={data.get('msg')}"
            )
        return data["data"]["message_id"]

    async def aclose(self) -> None:
        """Close the underlying httpx client if owned by this instance."""
        if self._owns_client:
            await self._client.aclose()
