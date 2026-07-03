#!/usr/bin/env python3
"""MCP server exposing Google Drive only for the SIGA allowed folders."""

from __future__ import annotations

import base64
import io
import json
import mimetypes
import os
from pathlib import Path
from typing import Any

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from mcp.server.fastmcp import FastMCP


SCOPES = ["https://www.googleapis.com/auth/drive"]
FOLDER_NAMES = {
    "SIGA": os.environ.get("SIGA_FOLDER_ID", ""),
    "SIGA_TEMP": os.environ.get("SIGA_TEMP_FOLDER_ID", ""),
}
TOKEN_PATH = Path(os.environ.get("GDRIVE_CREDENTIALS_PATH", ""))
CLIENT_PATH = Path(os.environ.get("GDRIVE_OAUTH_PATH", ""))
MAX_TEXT_BYTES = int(os.environ.get("SIGA_DRIVE_MAX_TEXT_BYTES", "1048576"))
MAX_DOWNLOAD_BYTES = int(os.environ.get("SIGA_DRIVE_MAX_DOWNLOAD_BYTES", "52428800"))

mcp = FastMCP("siga-drive")


def _require_config() -> None:
    missing = [name for name, value in FOLDER_NAMES.items() if not value]
    if missing:
        raise RuntimeError(f"Missing folder ids: {', '.join(missing)}")
    if not TOKEN_PATH.exists():
        raise RuntimeError(f"Missing Google Drive credentials: {TOKEN_PATH}")


def _service():
    _require_config()
    creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _folder_id(folder: str, service=None) -> str:
    if folder in FOLDER_NAMES:
        return FOLDER_NAMES[folder]
    if folder in FOLDER_NAMES.values():
        return folder
    if service is not None:
        try:
            meta = _file(service, folder, "id,mimeType")
        except RuntimeError as exc:
            raise ValueError("folder must be SIGA, SIGA_TEMP, or a folder ID under them") from exc
        if meta.get("mimeType") != "application/vnd.google-apps.folder":
            raise ValueError("folder must reference a Google Drive folder")
        if any(_is_descendant(service, folder, root) for root in _allowed_roots("both")):
            return folder
    raise ValueError("folder must be SIGA, SIGA_TEMP, or a folder ID under them")


def _quote_query(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _file(service, file_id: str, fields: str = "id,name,mimeType,parents,size,webViewLink,modifiedTime") -> dict[str, Any]:
    try:
        return service.files().get(fileId=file_id, fields=fields, supportsAllDrives=True).execute()
    except HttpError as exc:
        raise RuntimeError(f"Unable to read Drive file metadata for {file_id}: {exc}") from exc


def _is_descendant(service, file_id: str, root_id: str) -> bool:
    if file_id == root_id:
        return True
    seen: set[str] = set()
    stack = list(_file(service, file_id, "id,parents").get("parents", []))
    while stack:
        current = stack.pop()
        if current == root_id:
            return True
        if current in seen:
            continue
        seen.add(current)
        try:
            stack.extend(_file(service, current, "id,parents").get("parents", []))
        except RuntimeError:
            continue
    return False


def _allowed_roots(folder: str = "both") -> list[str]:
    if folder == "both":
        return [FOLDER_NAMES["SIGA"], FOLDER_NAMES["SIGA_TEMP"]]
    return [_folder_id(folder)]


def _assert_allowed(service, file_id: str, folder: str = "both") -> dict[str, Any]:
    meta = _file(service, file_id)
    if not any(_is_descendant(service, file_id, root) for root in _allowed_roots(folder)):
        raise PermissionError("Refusing access: file is outside SIGA/SIGA_TEMP allowed folders")
    return meta


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)


@mcp.tool()
def list_drive_files(folder: str = "SIGA_TEMP", page_size: int = 50) -> str:
    """List direct children of SIGA/SIGA_TEMP, or any allowed subfolder by ID."""
    service = _service()
    parent_id = _folder_id(folder, service)
    query = f"'{parent_id}' in parents and trashed = false"
    fields = "nextPageToken, files(id,name,mimeType,size,modifiedTime,webViewLink,parents)"
    result = service.files().list(
        q=query,
        pageSize=max(1, min(page_size, 200)),
        fields=fields,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    return _json(result.get("files", []))


@mcp.tool()
def search_drive_files(text: str, folder: str = "both", page_size: int = 50) -> str:
    """Search files by name/full text, then return only files under SIGA/SIGA_TEMP."""
    service = _service()
    safe = _quote_query(text)
    query = "trashed = false"
    if safe:
        query += f" and (name contains '{safe}' or fullText contains '{safe}')"
    fields = "nextPageToken, files(id,name,mimeType,size,modifiedTime,webViewLink,parents)"
    raw = service.files().list(
        q=query,
        pageSize=max(1, min(page_size * 4, 400)),
        fields=fields,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute().get("files", [])
    roots = _allowed_roots(folder)
    allowed = []
    for item in raw:
        if any(_is_descendant(service, item["id"], root) for root in roots):
            allowed.append(item)
            if len(allowed) >= page_size:
                break
    return _json(allowed)


@mcp.tool()
def get_drive_file_metadata(file_id: str) -> str:
    """Return metadata for a file only if it is inside SIGA/SIGA_TEMP."""
    service = _service()
    return _json(_assert_allowed(service, file_id))


@mcp.tool()
def download_drive_file(file_id: str, output_path: str | None = None) -> str:
    """Download a Drive binary file inside SIGA/SIGA_TEMP to a local path."""
    service = _service()
    meta = _assert_allowed(service, file_id)
    if meta["mimeType"].startswith("application/vnd.google-apps."):
        raise ValueError("Use export_google_workspace_file for Google Docs/Sheets/Slides")
    size = int(meta.get("size", "0") or 0)
    if size > MAX_DOWNLOAD_BYTES:
        raise ValueError(f"File too large: {size} bytes")
    out = Path(output_path or f"/workspace/{meta['name']}")
    if not str(out).startswith(("/workspace/", "/tmp/")):
        raise PermissionError("output_path must be under /workspace or /tmp")
    out.parent.mkdir(parents=True, exist_ok=True)
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    with out.open("wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return _json({"file_id": file_id, "name": meta["name"], "path": str(out), "bytes": out.stat().st_size})


@mcp.tool()
def read_drive_file_base64(file_id: str) -> str:
    """Return a small binary file from SIGA/SIGA_TEMP as base64."""
    service = _service()
    meta = _assert_allowed(service, file_id)
    size = int(meta.get("size", "0") or 0)
    if size > MAX_TEXT_BYTES:
        raise ValueError(f"File too large for inline base64: {size} bytes")
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return _json({"metadata": meta, "base64": base64.b64encode(buf.getvalue()).decode("ascii")})


@mcp.tool()
def export_google_workspace_file(file_id: str, mime_type: str = "text/plain") -> str:
    """Export a Google Docs/Sheets/Slides file inside SIGA/SIGA_TEMP."""
    service = _service()
    meta = _assert_allowed(service, file_id)
    request = service.files().export_media(fileId=file_id, mimeType=mime_type)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
        if buf.tell() > MAX_TEXT_BYTES:
            raise ValueError("Export too large for inline response")
    data = buf.getvalue()
    try:
        content: str | dict[str, str] = data.decode("utf-8")
    except UnicodeDecodeError:
        content = {"base64": base64.b64encode(data).decode("ascii")}
    return _json({"metadata": meta, "mime_type": mime_type, "content": content})


@mcp.tool()
def upload_file_to_drive(local_path: str, target_folder: str = "SIGA_TEMP", name: str | None = None) -> str:
    """Upload a local file to SIGA or SIGA_TEMP."""
    service = _service()
    parent_id = _folder_id(target_folder, service)
    if not any(_is_descendant(service, parent_id, root) for root in _allowed_roots("both")):
        raise PermissionError("Target folder is outside SIGA/SIGA_TEMP")
    path = Path(local_path)
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(local_path)
    mime_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    media = MediaFileUpload(str(path), mimetype=mime_type, resumable=True)
    body = {"name": name or path.name, "parents": [parent_id]}
    created = service.files().create(
        body=body,
        media_body=media,
        fields="id,name,mimeType,size,parents,webViewLink,modifiedTime",
        supportsAllDrives=True,
    ).execute()
    return _json(created)


@mcp.tool()
def create_drive_folder(name: str, parent_folder: str = "SIGA_TEMP") -> str:
    """Create a folder under SIGA or SIGA_TEMP."""
    service = _service()
    parent_id = _folder_id(parent_folder, service)
    body = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    created = service.files().create(
        body=body,
        fields="id,name,mimeType,parents,webViewLink,modifiedTime",
        supportsAllDrives=True,
    ).execute()
    return _json(created)


@mcp.tool()
def move_drive_file(file_id: str, target_folder: str = "SIGA") -> str:
    """Move a file already in SIGA/SIGA_TEMP to another allowed folder."""
    service = _service()
    meta = _assert_allowed(service, file_id)
    parent_id = _folder_id(target_folder, service)
    previous = ",".join(meta.get("parents", []))
    updated = service.files().update(
        fileId=file_id,
        addParents=parent_id,
        removeParents=previous,
        fields="id,name,mimeType,parents,webViewLink,modifiedTime",
        supportsAllDrives=True,
    ).execute()
    return _json(updated)


@mcp.tool()
def create_text_file(name: str, content: str, target_folder: str = "SIGA_TEMP", mime_type: str = "text/plain") -> str:
    """Create a small text file under SIGA or SIGA_TEMP."""
    if len(content.encode("utf-8")) > MAX_TEXT_BYTES:
        raise ValueError("content is too large")
    tmp = Path("/tmp") / name
    tmp.write_text(content, encoding="utf-8")
    try:
        return upload_file_to_drive(str(tmp), target_folder=target_folder, name=name)
    finally:
        tmp.unlink(missing_ok=True)


if __name__ == "__main__":
    mcp.run()
