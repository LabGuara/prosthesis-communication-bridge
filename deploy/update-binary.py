#!/usr/bin/env python3

from __future__ import annotations

import subprocess
import json
import os
import tarfile
from pathlib import Path
import shlex
import shutil
import urllib.request


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if not key:
            continue

        if value and value[0] in {'"', "'"}:
            try:
                value = shlex.split(value, posix=True)[0]
            except ValueError:
                value = value.strip('"\'')

        os.environ.setdefault(key, value)


load_env_file(Path(__file__).resolve().with_name(".env"))


REPO = os.environ.get("REPO", "mateus-sartorio/prothesis-logger")
INSTALL_ROOT = Path(os.environ.get("INSTALL_ROOT", "/opt/prothesis-logger-wrapper"))
SERVICE_NAME = os.environ.get("SERVICE_NAME", "prothesis-logger")
ASSET_NAME = os.environ.get("ASSET_NAME", "prothesis-logger-wrapper-linux-aarch64.tar.gz")
GITHUB_API = os.environ.get("GITHUB_API", "https://api.github.com")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")


def github_headers(accept: str) -> dict[str, str]:
    headers = {
        "User-Agent": "prothesis-logger-updater",
        "Accept": accept,
    }

    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    return headers


def fetch_json(url: str) -> dict:
    request = urllib.request.Request(url, headers=github_headers("application/vnd.github+json"))
    with urllib.request.urlopen(request) as response:
        return json.load(response)


def download_file(url: str, destination: Path) -> None:
    request = urllib.request.Request(url, headers=github_headers("application/octet-stream"))
    with urllib.request.urlopen(request) as response, destination.open("wb") as file_handle:
        shutil.copyfileobj(response, file_handle)


def main() -> int:
    release = fetch_json(f"{GITHUB_API}/repos/{REPO}/releases/latest")
    release_tag = release["tag_name"]

    asset_id = None
    for asset in release.get("assets", []):
        if asset.get("name") == ASSET_NAME:
            asset_id = asset.get("id")
            break

    if asset_id is None:
        raise SystemExit(f"asset not found: {ASSET_NAME}")

    release_dir = INSTALL_ROOT
    release_dir.mkdir(parents=True, exist_ok=True)

    installed_release_path = release_dir / "prothesis-logger-release" 
    if installed_release_path.exists() and installed_release_path.read_text(encoding="utf-8").strip() == release_tag:
        print(f"Already at latest release: {release_tag}")
        return 0

    archive_path = release_dir / ASSET_NAME
    download_file(f"{GITHUB_API}/repos/{REPO}/releases/assets/{asset_id}", archive_path)

    with tarfile.open(archive_path, mode="r:gz") as archive:
        archive.extractall(release_dir)

    archive_path.unlink(missing_ok=True)

    installed_release_path.write_text(f"{release_tag}\n", encoding="utf-8")

    print(f"Downloaded and extracted {REPO}@{release_tag} to {release_dir}")

    # Restart the service to pick up the new binary
    subprocess.run(["systemctl", "restart", "prothesis-logger-wrapper"], check=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())