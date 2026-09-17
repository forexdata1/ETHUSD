"""Telegram channel notifications for finished packs (Bot API).

Secrets come from the environment:
    TELEGRAM_BOT_TOKEN  bot token from @BotFather
    TELEGRAM_CHAT_ID    channel id (e.g. -1001234567890) or @channel_username

Files up to ~49 MB are uploaded directly (the Bot API upload limit is 50 MB).
Bigger packs fall back to a message containing the GitHub release download URL.
"""
from __future__ import annotations

import os
from pathlib import Path

import requests

API_BASE = "https://api.telegram.org"
DIRECT_UPLOAD_LIMIT_BYTES = 49 * 1024 * 1024  # Bot API upload limit is 50 MB


class TelegramNotifyError(RuntimeError):
    pass


def resolve_bot_token() -> str:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise TelegramNotifyError(
            "no Telegram bot token: set TELEGRAM_BOT_TOKEN (from @BotFather)"
        )
    return token


def resolve_chat_id() -> str:
    chat = os.environ.get("TELEGRAM_CHAT_ID")
    if not chat:
        raise TelegramNotifyError(
            "no Telegram chat: set TELEGRAM_CHAT_ID "
            "(channel id like -1001234567890, or @channel_username)"
        )
    return chat


def _api_url(token: str, method: str) -> str:
    return f"{API_BASE}/bot{token}/{method}"


def _check(resp, method: str) -> None:
    try:
        payload = resp.json()
    except ValueError:
        raise TelegramNotifyError(f"telegram {method}: HTTP {resp.status_code}")
    if not payload.get("ok"):
        raise TelegramNotifyError(
            f"telegram {method} failed: {payload.get('description', '')}"
        )


def send_message(token: str, chat_id: str, text: str) -> None:
    resp = requests.post(
        _api_url(token, "sendMessage"),
        json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
        timeout=30,
    )
    _check(resp, "sendMessage")


def send_document(token: str, chat_id: str, path: Path, caption: str) -> None:
    size_mb = path.stat().st_size / (1024 * 1024)
    with open(path, "rb") as fin:
        resp = requests.post(
            _api_url(token, "sendDocument"),
            data={"chat_id": chat_id, "caption": caption},
            files={"document": (path.name, fin, "application/zip")},
            timeout=max(120, int(size_mb) * 10),
        )
    _check(resp, "sendDocument")


def notify_pack(
    token: str,
    chat_id: str,
    zip_path: Path,
    caption: str,
    download_url: str | None = None,
) -> str:
    """Send one pack zip to the channel. Returns 'file' or 'link'.

    Small packs go as a direct file upload; packs over the bot limit fall back
    to a message with the release download URL.
    """
    size = zip_path.stat().st_size
    if size <= DIRECT_UPLOAD_LIMIT_BYTES:
        send_document(token, chat_id, zip_path, caption)
        return "file"
    if not download_url:
        raise TelegramNotifyError(
            f"{zip_path.name} is {size / 1e6:.0f} MB — over the 50 MB bot "
            f"upload limit and no download URL was provided"
        )
    size_mb = size / (1024 * 1024)
    send_message(
        token,
        chat_id,
        f"{caption}\n\n📦 {zip_path.name} ({size_mb:,.0f} MB)\n⬇️ {download_url}",
    )
    return "link"
