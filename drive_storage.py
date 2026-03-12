"""
Google Drive CSV Storage Utility

Stores data as Google Sheets in Google Drive using OAuth credentials.
Files are uploaded as CSV and converted to Google Sheets format.

Required environment variables:
  GOOGLE_OAUTH_CLIENT_ID      — OAuth client ID
  GOOGLE_OAUTH_CLIENT_SECRET  — OAuth client secret
  GOOGLE_OAUTH_REFRESH_TOKEN  — OAuth refresh token (obtained via auth_setup.py)
  GOOGLE_DRIVE_FOLDER_ID      — ID of the Google Drive folder to store files in
"""

import os
import io
import csv
import json
import logging

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload

log = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive"]

SHEETS_MIME = "application/vnd.google-apps.spreadsheet"

_service = None


def get_drive_service():
    """Authenticate via OAuth refresh token and return a Drive API service (cached)."""
    global _service
    if _service:
        return _service

    client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "")
    client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "")
    refresh_token = os.environ.get("GOOGLE_OAUTH_REFRESH_TOKEN", "")

    if not all([client_id, client_secret, refresh_token]):
        raise RuntimeError(
            "Missing Google OAuth env vars. Need: "
            "GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET, GOOGLE_OAUTH_REFRESH_TOKEN"
        )

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=SCOPES,
    )
    creds.refresh(Request())

    _service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _service


def get_folder_id():
    """Get the target Google Drive folder ID from environment."""
    folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")
    if not folder_id:
        raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID is not set")
    return folder_id


def find_file(filename):
    """Find a file by name in the target folder. Returns file ID or None."""
    service = get_drive_service()
    folder_id = get_folder_id()

    query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)", pageSize=1).execute()
    files = results.get("files", [])
    return files[0]["id"] if files else None


def _rows_to_csv_bytes(rows, headers):
    """Convert a list of dicts to CSV bytes."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buf.getvalue().encode("utf-8")


def upload_csv(filename, rows, headers):
    """Upload (or replace) data as a Google Sheet in Google Drive.

    Creates a new Google Sheet if one doesn't exist, or updates the existing one.

    Args:
        filename: Display name of the file (e.g. "news_raw.csv")
        rows: List of dicts to write
        headers: List of column names
    """
    service = get_drive_service()
    folder_id = get_folder_id()

    content = _rows_to_csv_bytes(rows, headers)
    media = MediaInMemoryUpload(content, mimetype="text/csv")

    existing_id = find_file(filename)

    if existing_id:
        service.files().update(
            fileId=existing_id,
            media_body=media,
        ).execute()
        print(f"  Updated {filename} ({len(rows)} rows)")
    else:
        metadata = {
            "name": filename,
            "parents": [folder_id],
            "mimeType": SHEETS_MIME,
        }
        service.files().create(
            body=metadata,
            media_body=media,
        ).execute()
        print(f"  Created {filename} ({len(rows)} rows)")

    return len(rows)


def read_csv(filename):
    """Read a Google Sheet from Drive as CSV. Returns list of dicts, or empty list if not found."""
    service = get_drive_service()
    file_id = find_file(filename)

    if not file_id:
        print(f"  {filename} not found in Drive")
        return []

    # Export Google Sheet as CSV
    csv_bytes = service.files().export(fileId=file_id, mimeType="text/csv").execute()

    text = csv_bytes.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    print(f"  Read {len(rows)} rows from {filename}")
    return rows


def append_csv(filename, new_rows, headers):
    """Append rows to an existing sheet (or create if it doesn't exist).

    Deduplicates by the first column in headers (assumed to be a unique key like 'url').

    Args:
        filename: File name on Drive
        new_rows: List of dicts to append
        headers: List of column names (first one used as dedup key)

    Returns:
        Number of new rows actually appended
    """
    existing = read_csv(filename)

    # Deduplicate using the first header as the key
    key_field = headers[0]
    existing_keys = {row.get(key_field) for row in existing}

    unique_new = [r for r in new_rows if r.get(key_field) not in existing_keys]

    if not unique_new:
        print(f"  No new rows to append to {filename}")
        return 0

    all_rows = existing + unique_new
    upload_csv(filename, all_rows, headers)
    print(f"  Appended {len(unique_new)} new rows to {filename} (total: {len(all_rows)})")
    return len(unique_new)
