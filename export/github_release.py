"""Minimal GitHub Releases uploader (used by `download --yearly --upload-release`).

Auth: GITHUB_TOKEN (or GH_TOKEN) with contents write access.
Repo: --repo owner/name, or GITHUB_REPOSITORY — both are provided
automatically inside GitHub Actions.
"""
from __future__ import annotations

import os
from pathlib import Path

import requests

API_BASE = "https://api.github.com"
API_VERSION = "2022-11-28"
GITHUB_ASSET_LIMIT_BYTES = 2 * 1024**3  # GitHub release asset hard limit


class ReleaseUploadError(RuntimeError):
    pass


def resolve_token() -> str:
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        raise ReleaseUploadError(
            "no GitHub token: set GITHUB_TOKEN (with repo contents write access)"
        )
    return token


def resolve_repository(explicit: str | None = None) -> str:
    repo = explicit or os.environ.get("GITHUB_REPOSITORY", "")
    if not repo or "/" not in repo:
        raise ReleaseUploadError(
            "unknown GitHub repository: pass --repo owner/name "
            "or set GITHUB_REPOSITORY"
        )
    return repo


def _headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": API_VERSION,
    }


def get_or_create_release(
    repo: str,
    tag: str,
    token: str,
    name: str | None = None,
    body: str = "",
) -> dict:
    """Return the existing release for `tag`, or create it."""
    headers = _headers(token)
    resp = requests.get(
        f"{API_BASE}/repos/{repo}/releases/tags/{tag}", headers=headers, timeout=30
    )
    if resp.status_code == 200:
        return resp.json()
    resp = requests.post(
        f"{API_BASE}/repos/{repo}/releases",
        headers=headers,
        json={
            "tag_name": tag,
            "name": name or tag,
            "body": body,
            "draft": False,
            "prerelease": False,
        },
        timeout=30,
    )
    if resp.status_code not in (200, 201):
        raise ReleaseUploadError(
            f"create release {tag} failed: {resp.status_code} {resp.text[:300]}"
        )
    return resp.json()


def upload_asset(repo: str, release: dict, path: Path, token: str) -> str:
    """Upload `path` as a release asset, replacing any asset with the same name.

    Returns the browser download URL.
    """
    size = path.stat().st_size
    if size >= GITHUB_ASSET_LIMIT_BYTES:
        raise ReleaseUploadError(
            f"{path.name} is {size / 1e9:.1f} GB — GitHub release assets "
            f"are limited to 2 GB"
        )

    headers = _headers(token)
    release_id = release["id"]

    resp = requests.get(
        f"{API_BASE}/repos/{repo}/releases/{release_id}/assets",
        headers=headers,
        params={"per_page": 100},
        timeout=30,
    )
    if resp.status_code == 200:
        for asset in resp.json():
            if asset.get("name") == path.name:
                requests.delete(
                    f"{API_BASE}/repos/{repo}/releases/assets/{asset['id']}",
                    headers=headers,
                    timeout=30,
                )

    upload_url = release["upload_url"].split("{")[0]
    size_mb = max(1, size // (1024 * 1024))
    with open(path, "rb") as fin:
        resp = requests.post(
            upload_url,
            headers={**headers, "Content-Type": "application/zip"},
            params={"name": path.name},
            data=fin,
            timeout=max(120, size_mb * 5),
        )
    if resp.status_code not in (200, 201):
        raise ReleaseUploadError(
            f"upload {path.name} failed: {resp.status_code} {resp.text[:300]}"
        )
    return resp.json().get("browser_download_url", "")
