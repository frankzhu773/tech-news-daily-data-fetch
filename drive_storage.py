"""
Google Drive CSV Storage Utility

Provides helpers to upload, read, and append CSV files on Google Drive
using a service account. Used by all fetcher scripts as a replacement
for Supabase storage.

Required environment variables:
  GOOGLE_SERVICE_ACCOUNT_KEY  — JSON key content for a Google Cloud service account
  GOOGLE_DRIVE_FOLDER_ID      — ID of the Google Drive folder to store files in
"""

import os
import io
import csv
import json
import logging

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload, MediaIoBaseDownload

log = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive"]

_service = None


def get_drive_service():
    """Authenticate and return a Google Drive API service instance (cached)."""
    global _service
    if _service:
        return _service

    key_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_KEY", "")
    if not key_json:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_KEY is not set")

    creds_info = json.loads(key_json)
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
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


def upload_csv(filename, rows, headers):
    """Upload (or replace) a CSV file in Google Drive.

    Args:
        filename: Name of the CSV file (e.g. "news_raw.csv")
        rows: List of dicts to write
        headers: List of column names
    """
    service = get_drive_service()
    folder_id = get_folder_id()

    # Build CSV content in memory
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)

    content = buf.getvalue().encode("utf-8")
    media = MediaInMemoryUpload(content, mimetype="text/csv")

    # Check if file already exists
    existing_id = find_file(filename)

    if existing_id:
        # Update existing file
        service.files().update(
            fileId=existing_id,
            media_body=media,
        ).execute()
        log.info(f"Updated {filename} ({len(rows)} rows)")
    else:
        # Create new file
        metadata = {
            "name": filename,
            "parents": [folder_id],
            "mimeType": "text/csv",
        }
        service.files().create(
            body=metadata,
            media_body=media,
        ).execute()
        log.info(f"Created {filename} ({len(rows)} rows)")

    return len(rows)


def read_csv(filename):
    """Read a CSV file from Google Drive. Returns list of dicts, or empty list if not found."""
    service = get_drive_service()
    file_id = find_file(filename)

    if not file_id:
        log.info(f"{filename} not found in Drive")
        return []

    request = service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    buf.seek(0)
    text = buf.read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    log.info(f"Read {len(rows)} rows from {filename}")
    return rows


def append_csv(filename, new_rows, headers):
    """Append rows to an existing CSV (or create if it doesn't exist).

    Deduplicates by the first column in headers (assumed to be a unique key like 'url').

    Args:
        filename: CSV filename on Drive
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
        log.info(f"No new rows to append to {filename}")
        return 0

    all_rows = existing + unique_new
    upload_csv(filename, all_rows, headers)
    log.info(f"Appended {len(unique_new)} new rows to {filename} (total: {len(all_rows)})")
    return len(unique_new)
