"""Telegram remote-control adapter.

The generic "integrations" store is prompt-visible API plumbing. Bot tokens
are process control credentials, so they live in their own config file and are
only exposed through this manager.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import httpx
from sqlalchemy import or_

from core.atomic_io import atomic_write_json
from core.database import ModelEndpoint, ScheduledTask, SessionLocal
from src.endpoint_resolver import build_chat_url, build_headers, normalize_base, resolve_endpoint

logger = logging.getLogger(__name__)

VALID_PROVIDERS = {"telegram"}
DEFAULT_DATA_FILE = os.path.join("data", "remote_control.json")
TELEGRAM_MAX_CHARS = 3900


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _default_provider_config(provider: str) -> Dict[str, Any]:
    _clean_provider(provider)
    return {
        "enabled": False,
        "token": "",
        "allow_all": False,
        "allowed_chat_ids": [],
        "sessions": {},
    }


def _default_config() -> Dict[str, Any]:
    return {
        "version": 1,
        "providers": {
            "telegram": _default_provider_config("telegram"),
        },
    }


def _as_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        parts = value.replace("\r", "\n").replace(",", "\n").split("\n")
        return [p.strip() for p in parts if p.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(v).strip() for v in value if str(v).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _clean_provider(provider: str) -> str:
    provider = (provider or "").strip().lower()
    if provider not in VALID_PROVIDERS:
        raise ValueError(f"Unsupported remote-control provider: {provider}")
    return provider


def _mask_token(token: str) -> str:
    token = token or ""
    if not token:
        return ""
    if len(token) <= 8:
        return "*" * len(token)
    return f"{token[:4]}...{token[-4:]}"


def _chunk_text(text: str, limit: int) -> List[str]:
    text = (text or "").strip() or "(no response)"
    chunks = []
    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut < max(200, limit // 3):
            cut = text.rfind(" ", 0, limit)
        if cut < max(200, limit // 3):
            cut = limit
        chunks.append(text[:cut].rstrip())
        text = text[cut:].lstrip()
    if text:
        chunks.append(text)
    return chunks or ["(no response)"]


class RemoteControlManager:
    def __init__(
        self,
        *,
        session_manager,
        task_scheduler,
        auth_manager=None,
        api_key_manager=None,
        app=None,
        data_file: str = DEFAULT_DATA_FILE,
    ):
        self.session_manager = session_manager
        self.task_scheduler = task_scheduler
        self.auth_manager = auth_manager
        self.api_key_manager = api_key_manager
        self.app = app
        self.data_file = data_file
        self._config = self._load_config()
        self._status: Dict[str, Dict[str, Any]] = {
            "telegram": self._empty_status(),
        }
        self._tasks: Dict[str, asyncio.Task] = {}
        self._stop = asyncio.Event()
        self._lock = asyncio.Lock()
        self._http: Optional[httpx.AsyncClient] = None

    def _empty_status(self) -> Dict[str, Any]:
        return {
            "running": False,
            "last_error": "",
            "last_message_at": "",
            "identity": "",
        }

    def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(timeout=httpx.Timeout(connect=8, read=45, write=10, pool=5))
        return self._http

    def _load_config(self) -> Dict[str, Any]:
        cfg = _default_config()
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, "r", encoding="utf-8") as f:
                    stored = json.load(f)
                if isinstance(stored, dict):
                    cfg.update({k: v for k, v in stored.items() if k != "providers"})
                    providers = stored.get("providers") or {}
                    for provider in VALID_PROVIDERS:
                        merged = _default_provider_config(provider)
                        if isinstance(providers.get(provider), dict):
                            merged.update(providers[provider])
                        merged.pop("mode", None)
                        merged.pop("owner", None)
                        merged["allow_all"] = bool(merged.get("allow_all"))
                        if not isinstance(merged.get("sessions"), dict):
                            merged["sessions"] = {}
                        cfg["providers"][provider] = merged
        except Exception as exc:
            logger.warning("Failed to load remote-control config: %s", exc)
        return cfg

    def _save_config(self) -> None:
        atomic_write_json(self.data_file, self._config, indent=2)

    def _encrypt_token(self, token: str) -> str:
        if not token:
            return ""
        if self.api_key_manager:
            return self.api_key_manager.encrypt_api_key(token)
        return token

    def _decrypt_token(self, cfg: Dict[str, Any]) -> str:
        raw = cfg.get("token") or ""
        if not raw:
            return ""
        if not self.api_key_manager:
            return raw
        try:
            return self.api_key_manager.decrypt_api_key(raw)
        except Exception:
            return raw

    def _provider_cfg(self, provider: str) -> Dict[str, Any]:
        provider = _clean_provider(provider)
        self._config.setdefault("providers", {})
        if provider not in self._config["providers"]:
            self._config["providers"][provider] = _default_provider_config(provider)
        return self._config["providers"][provider]

    def _status_for(self, provider: str) -> Dict[str, Any]:
        provider = _clean_provider(provider)
        self._status.setdefault(provider, self._empty_status())
        return self._status[provider]

    def _set_status(self, provider: str, **values: Any) -> None:
        status = self._status_for(provider)
        status.update(values)

    def _safe_provider(self, provider: str) -> Dict[str, Any]:
        cfg = deepcopy(self._provider_cfg(provider))
        token = self._decrypt_token(cfg)
        cfg.pop("token", None)
        cfg.pop("owner", None)
        cfg.pop("mode", None)
        cfg.pop("sessions", None)
        cfg["configured"] = bool(token)
        cfg["token_mask"] = _mask_token(token)
        cfg["status"] = deepcopy(self._status_for(provider))
        return cfg

    def safe_config(self) -> Dict[str, Any]:
        return {
            "version": self._config.get("version", 1),
            "providers": {
                provider: self._safe_provider(provider)
                for provider in sorted(VALID_PROVIDERS)
            },
        }

    async def start(self) -> None:
        self._stop.clear()
        for provider in sorted(VALID_PROVIDERS):
            await self.reload(provider)

    async def close(self) -> None:
        self._stop.set()
        tasks = list(self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    async def reload(self, provider: Optional[str] = None) -> Dict[str, Any]:
        providers = [_clean_provider(provider)] if provider else sorted(VALID_PROVIDERS)
        cancelled: List[asyncio.Task] = []
        async with self._lock:
            for name in providers:
                task = self._tasks.pop(name, None)
                if task:
                    task.cancel()
                    cancelled.append(task)
                cfg = self._provider_cfg(name)
                token = self._decrypt_token(cfg)
                if not cfg.get("enabled") or not token:
                    self._set_status(name, running=False, last_error="")
                    continue
                self._tasks[name] = asyncio.create_task(self._telegram_loop(), name="remote-control-telegram")
        if cancelled:
            await asyncio.gather(*cancelled, return_exceptions=True)
        return self.safe_config()

    async def update_provider(self, provider: str, patch: Dict[str, Any], actor: Optional[str] = None) -> Dict[str, Any]:
        provider = _clean_provider(provider)
        patch = patch or {}
        async with self._lock:
            if patch.get("reset"):
                self._config["providers"][provider] = _default_provider_config(provider)
                cfg = self._provider_cfg(provider)
            else:
                cfg = self._provider_cfg(provider)
                if "enabled" in patch:
                    cfg["enabled"] = bool(patch.get("enabled"))
                if "allow_all" in patch:
                    cfg["allow_all"] = bool(patch.get("allow_all"))
                if patch.get("clear_token"):
                    cfg["token"] = ""
                token = str(patch.get("token") or "").strip()
                if token:
                    cfg["token"] = self._encrypt_token(token)
                if "allowed_chat_ids" in patch:
                    cfg["allowed_chat_ids"] = _as_list(patch.get("allowed_chat_ids"))
            self._save_config()
        await self.reload(provider)
        return self.safe_config()

    async def test_provider(self, provider: str, patch: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        provider = _clean_provider(provider)
        patch = patch or {}
        token = str(patch.get("token") or "").strip()
        if not token:
            token = self._decrypt_token(self._provider_cfg(provider))
        if not token:
            return {"ok": False, "error": "Bot token is required"}
        return await self._test_telegram(token)

    async def _test_telegram(self, token: str) -> Dict[str, Any]:
        try:
            r = await self._client().get(f"https://api.telegram.org/bot{token}/getMe", timeout=15)
            data = r.json()
            if not r.is_success or not data.get("ok"):
                return {"ok": False, "error": data.get("description") or f"HTTP {r.status_code}"}
            user = data.get("result") or {}
            name = user.get("username") or user.get("first_name") or str(user.get("id") or "")
            return {"ok": True, "identity": name}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    async def _telegram_loop(self) -> None:
        provider = "telegram"
        offset = None
        backoff = 2.0
        while not self._stop.is_set():
            cfg = deepcopy(self._provider_cfg(provider))
            token = self._decrypt_token(cfg)
            if not cfg.get("enabled") or not token:
                self._set_status(provider, running=False, last_error="")
                return
            try:
                params: Dict[str, Any] = {"timeout": 30, "allowed_updates": ["message"]}
                if offset is not None:
                    params["offset"] = offset
                r = await self._client().get(
                    f"https://api.telegram.org/bot{token}/getUpdates",
                    params=params,
                    timeout=40,
                )
                data = r.json()
                if not r.is_success or not data.get("ok"):
                    raise RuntimeError(data.get("description") or f"HTTP {r.status_code}")
                self._set_status(provider, running=True, last_error="")
                backoff = 2.0
                for update in data.get("result") or []:
                    update_id = update.get("update_id")
                    if update_id is not None:
                        offset = max(offset or 0, int(update_id) + 1)
                    await self._handle_telegram_update(token, update)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._set_status(provider, running=False, last_error=str(exc)[:500])
                logger.warning("Telegram remote-control loop error: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 30.0)

    async def _handle_telegram_update(self, token: str, update: Dict[str, Any]) -> None:
        msg = update.get("message") or update.get("edited_message") or {}
        text = (msg.get("text") or msg.get("caption") or "").strip()
        if not text:
            return
        chat = msg.get("chat") or {}
        from_user = msg.get("from") or {}
        chat_id = str(chat.get("id") or "")
        user_id = str(from_user.get("id") or "")
        if not chat_id:
            return
        if not self._telegram_authorized(chat_id):
            await self._telegram_send(token, chat_id, self._unauthorized_message(chat_id, user_id))
            return
        self._set_status("telegram", last_message_at=_utc_now_iso())
        surface_id = f"telegram:{chat_id}"

        async def reply(body: str) -> None:
            await self._telegram_send(token, chat_id, body)

        async def typing() -> None:
            try:
                await self._client().post(
                    f"https://api.telegram.org/bot{token}/sendChatAction",
                    json={"chat_id": chat_id, "action": "typing"},
                    timeout=10,
                )
            except Exception:
                pass

        await self._handle_message(
            surface_id=surface_id,
            actor_id=user_id,
            text=text,
            reply=reply,
            typing=typing,
            identity_text=f"Telegram chat id: {chat_id}\nTelegram user id: {user_id or 'unknown'}",
        )

    async def _telegram_send(self, token: str, chat_id: str, text: str) -> None:
        for chunk in _chunk_text(text, TELEGRAM_MAX_CHARS):
            await self._client().post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": chunk},
                timeout=20,
            )

    def _telegram_authorized(self, chat_id: str) -> bool:
        cfg = self._provider_cfg("telegram")
        if cfg.get("allow_all"):
            return True
        return str(chat_id) in set(_as_list(cfg.get("allowed_chat_ids")))

    def _unauthorized_message(self, chat_id: str, user_id: str) -> str:
        return (
            "Remote control is not allowed from this Telegram chat yet.\n"
            f"Chat ID: {chat_id}\n"
            f"User ID: {user_id or 'unknown'}\n"
            "Add the chat ID in Settings > Integrations > Remote Control."
        )

    async def _handle_message(
        self,
        *,
        surface_id: str,
        actor_id: str,
        text: str,
        reply: Callable[[str], Awaitable[None]],
        typing: Callable[[], Awaitable[None]],
        identity_text: str,
    ) -> None:
        try:
            command, rest = self._parse_command(text)
            if command in {"start", "help"}:
                await reply(self._help_text())
                return
            if command == "id":
                await reply(identity_text)
                return
            if command == "status":
                await reply(self._status_text())
                return
            if command == "reset":
                self._forget_session(surface_id)
                await reply("Remote conversation reset.")
                return
            if command == "tasks":
                await reply(self._list_tasks_text(self._owner_for()))
                return
            if command == "run":
                await reply(await self._run_task_text(rest, self._owner_for()))
                return
            if command in {"chat", "agent"}:
                if not rest:
                    await reply(f"Usage: /{command} your message")
                    return
                mode = command
                prompt = rest
            elif command:
                await reply(f"Unknown command: /{command}\n\n{self._help_text()}")
                return
            else:
                mode = "chat"
                prompt = text

            await typing()
            response = await self._dispatch_to_chat_front_door(surface_id, actor_id, prompt, mode)
            await reply(response)
        except Exception as exc:
            logger.exception("Telegram remote-control message failed")
            await reply(f"Remote control error: {type(exc).__name__}: {exc}")

    def _parse_command(self, text: str) -> Tuple[str, str]:
        stripped = (text or "").strip()
        if not stripped.startswith("/"):
            return "", stripped
        first, _, rest = stripped.partition(" ")
        command = first[1:].split("@", 1)[0].strip().lower()
        return command, rest.strip()

    def _help_text(self) -> str:
        return (
            "Odysseus remote control via Telegram\n"
            "/id - show IDs for allowlisting\n"
            "/status - show adapter status\n"
            "/reset - start a fresh remote chat session\n"
            "/tasks - list active scheduled tasks\n"
            "/run <task-id> - run a scheduled task now\n"
            "/chat <message> - send a plain chat turn\n"
            "/agent <message> - send an agent turn with tools\n"
            "Otherwise, send a message and Odysseus auto-routes chat vs tool requests."
        )

    def _status_text(self) -> str:
        cfg = self._provider_cfg("telegram")
        status = self._status_for("telegram")
        lines = [
            "Telegram remote control",
            f"Enabled: {'yes' if cfg.get('enabled') else 'no'}",
            f"Configured: {'yes' if self._decrypt_token(cfg) else 'no'}",
            f"Running: {'yes' if status.get('running') else 'no'}",
        ]
        if status.get("identity"):
            lines.append(f"Bot: {status['identity']}")
        if status.get("last_message_at"):
            lines.append(f"Last message: {status['last_message_at']}")
        if status.get("last_error"):
            lines.append(f"Last error: {status['last_error']}")
        return "\n".join(lines)

    def _owner_for(self) -> str:
        if self.auth_manager:
            try:
                users = self.auth_manager.users or {}
                for username, data in users.items():
                    if data.get("is_admin") is True:
                        return username
                if users:
                    return next(iter(users.keys()))
            except Exception:
                pass
        return ""

    def _forget_session(self, surface_id: str) -> None:
        cfg = self._provider_cfg("telegram")
        sessions = cfg.setdefault("sessions", {})
        if surface_id in sessions:
            sessions.pop(surface_id, None)
            self._save_config()

    def _list_tasks_text(self, owner: str) -> str:
        db = SessionLocal()
        try:
            q = db.query(ScheduledTask).filter(ScheduledTask.status == "active")
            if owner:
                q = q.filter(ScheduledTask.owner == owner)
            tasks = q.order_by(ScheduledTask.created_at.desc()).limit(10).all()
            if not tasks:
                return "No active scheduled tasks found."
            lines = ["Active scheduled tasks:"]
            for task in tasks:
                name = task.name or "Untitled Task"
                detail = task.schedule or task.trigger_type or "manual"
                lines.append(f"{task.id} - {name} ({detail})")
            return "\n".join(lines)
        finally:
            db.close()

    async def _run_task_text(self, task_id: str, owner: str) -> str:
        task_id = (task_id or "").strip()
        if not task_id:
            return "Usage: /run <task-id>"
        db = SessionLocal()
        try:
            q = db.query(ScheduledTask).filter(ScheduledTask.id == task_id)
            if owner:
                q = q.filter(ScheduledTask.owner == owner)
            task = q.first()
            if not task:
                return f"Task not found: {task_id}"
            if task.status != "active":
                return f"Task '{task.name}' is {task.status or 'not active'}."
        finally:
            db.close()
        ok = await self.task_scheduler.run_task_now(task_id)
        return f"Started task {task_id}." if ok else f"Task {task_id} is already running."

    async def _dispatch_to_chat_front_door(
        self,
        surface_id: str,
        actor_id: str,
        prompt: str,
        mode: str,
    ) -> str:
        if self.app is None:
            return "Remote control is not attached to the Odysseus chat runtime."
        owner = self._owner_for()
        endpoint_url, model, _headers = self._resolve_chat_endpoint(owner)
        if not endpoint_url or not model:
            return "No default chat model is configured. Set one in Settings > AI."
        session = self._get_or_create_session(surface_id, owner, endpoint_url, model)
        session.endpoint_url = endpoint_url
        session.model = model
        form_data = {
            "message": prompt,
            "session": session.id,
            "mode": mode,
            "remote_provider": "telegram",
            "remote_surface_id": surface_id,
            "remote_actor_id": actor_id,
        }
        from core.middleware import INTERNAL_TOOL_HEADER, INTERNAL_TOOL_TOKEN
        headers = {
            INTERNAL_TOOL_HEADER: INTERNAL_TOOL_TOKEN,
            "X-Odysseus-Owner": owner or "",
        }
        transport = httpx.ASGITransport(app=self.app, client=("127.0.0.1", 0))
        parts: List[str] = []
        last_error = ""
        timeout = httpx.Timeout(420.0, connect=10.0, write=10.0, pool=10.0)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:7000", timeout=timeout) as client:
            async with client.stream("POST", "/api/chat_stream", data=form_data, headers=headers) as response:
                if response.status_code >= 400:
                    body = await response.aread()
                    return f"Remote chat request failed ({response.status_code}): {body.decode(errors='replace')[:500]}"
                async for line in response.aiter_lines():
                    line = (line or "").strip()
                    if not line.startswith("data:"):
                        continue
                    payload = line[5:].strip()
                    if payload == "[DONE]":
                        break
                    if not payload:
                        continue
                    try:
                        data = json.loads(payload)
                    except Exception:
                        continue
                    if data.get("delta"):
                        parts.append(str(data.get("delta")))
                    elif data.get("type") == "research_done":
                        parts.append("Research finished in Odysseus.")
                    elif data.get("type") == "error":
                        last_error = str(data.get("error") or data.get("message") or data)
        reply = "".join(parts).strip()
        return reply or last_error or "(no response)"

    def _resolve_chat_endpoint(self, owner: str) -> Tuple[Optional[str], Optional[str], Dict[str, str]]:
        url, model, headers = resolve_endpoint("default", owner=owner or None)
        if url and model:
            return url, model, headers or {}

        db = SessionLocal()
        try:
            q = db.query(ModelEndpoint).filter(
                ModelEndpoint.is_enabled == True,
                or_(ModelEndpoint.model_type == None, ModelEndpoint.model_type != "image"),
            )
            if owner:
                q = q.filter((ModelEndpoint.owner == owner) | (ModelEndpoint.owner == None))
            ep = q.order_by(ModelEndpoint.created_at.desc()).first()
            if not ep:
                return None, None, {}
            base = normalize_base(ep.base_url)
            model_name = ""
            if ep.cached_models:
                try:
                    models = json.loads(ep.cached_models) or []
                    if models:
                        model_name = str(models[0])
                except Exception:
                    pass
            if not model_name:
                model_name = ep.name or ""
            return build_chat_url(base), model_name, build_headers(ep.api_key, base)
        finally:
            db.close()

    def _get_or_create_session(
        self,
        surface_id: str,
        owner: str,
        endpoint_url: str,
        model: str,
    ):
        cfg = self._provider_cfg("telegram")
        sessions = cfg.setdefault("sessions", {})
        session_id = sessions.get(surface_id)
        if session_id:
            try:
                return self.session_manager.get_session(session_id)
            except Exception:
                sessions.pop(surface_id, None)
        session_id = str(uuid.uuid4())
        name = f"Remote Telegram {surface_id.split(':', 1)[-1]}"
        session = self.session_manager.create_session(
            session_id=session_id,
            name=name,
            endpoint_url=endpoint_url,
            model=model,
            rag=False,
            owner=owner or None,
        )
        sessions[surface_id] = session_id
        self._save_config()
        return session
