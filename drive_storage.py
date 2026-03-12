"""
Google Drive CSV Storage Utility

Stores data as Google Sheets in Google Drive using OAuth credentials.
Data is organized by year/month folders with monthly files.

Folder structure:
  GOOGLE_DRIVE_FOLDER_ID/
    2026/
      Mar/
        news_raw_2026_Mar          (Google Sheet)
        download_rank_7d_2026_Mar  (Google Sheet)
        ...
      Apr/
        ...

Required environment variables:
  GOOGLE_OAUTH_CLIENT_ID      — OAuth client ID
  GOOGLE_OAUTH_CLIENT_SECRET  — OAuth client secret
  GOOGLE_OAUTH_REFRESH_TOKEN  — OAuth refresh token
  GOOGLE_DRIVE_FOLDER_ID      — Root folder ID
"""

import os
import io
import csv
import json
import logging
from datetime import datetime, timezone

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload

log = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive"]
SHEETS_MIME = "application/vnd.google-apps.spreadsheet"
FOLDER_MIME = "application/vnd.google-apps.folder"

MONTH_ABBRS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

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
    """Get the root Google Drive folder ID from environment."""
    folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")
    if not folder_id:
        raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID is not set")
    return folder_id


# ─── Folder management ──────────────────────────────────────────────────────

def ensure_subfolder(parent_id, folder_name):
    """Find or create a subfolder under parent_id. Returns the folder ID."""
    service = get_drive_service()

    query = (f"name = '{folder_name}' and '{parent_id}' in parents "
             f"and mimeType = '{FOLDER_MIME}' and trashed = false")
    results = service.files().list(q=query, fields="files(id)", pageSize=1).execute()
    files = results.get("files", [])

    if files:
        return files[0]["id"]

    metadata = {
        "name": folder_name,
        "parents": [parent_id],
        "mimeType": FOLDER_MIME,
    }
    folder = service.files().create(body=metadata, fields="id").execute()
    print(f"  Created folder: {folder_name}")
    return folder["id"]


def get_monthly_folder(year=None, month=None):
    """Get (or create) the monthly subfolder. Returns folder ID.

    Structure: root / {year} / {month_abbr}
    Example:   root / 2026 / Mar
    """
    if year is None or month is None:
        now = datetime.now(timezone.utc)
        year = now.year
        month = now.month

    month_abbr = MONTH_ABBRS[month - 1]

    root = get_folder_id()
    year_folder = ensure_subfolder(root, str(year))
    month_folder = ensure_subfolder(year_folder, month_abbr)
    return month_folder


def get_monthly_filename(base_name, year=None, month=None):
    """Generate monthly filename with suffix.

    Example: news_raw.csv -> news_raw_2026_Mar
    """
    if year is None or month is None:
        now = datetime.now(timezone.utc)
        year = now.year
        month = now.month

    month_abbr = MONTH_ABBRS[month - 1]
    base = base_name.removesuffix(".csv")
    return f"{base}_{year}_{month_abbr}"


# ─── File operations ─────────────────────────────────────────────────────────

def find_file_in_folder(filename, folder_id):
    """Find a file by name in a specific folder. Returns file ID or None."""
    service = get_drive_service()
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


def _read_csv_by_id(file_id):
    """Read a Google Sheet by file ID, return list of dicts."""
    service = get_drive_service()
    csv_bytes = service.files().export(fileId=file_id, mimeType="text/csv").execute()
    text = csv_bytes.decode("utf-8")
    if not text.strip():
        return []
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def _write_csv_to_folder(filename, rows, headers, folder_id):
    """Write rows as a Google Sheet in the specified folder (create or update)."""
    service = get_drive_service()
    content = _rows_to_csv_bytes(rows, headers)
    media = MediaInMemoryUpload(content, mimetype="text/csv")

    existing_id = find_file_in_folder(filename, folder_id)

    if existing_id:
        service.files().update(fileId=existing_id, media_body=media).execute()
        print(f"  Updated {filename} ({len(rows)} rows)")
    else:
        metadata = {
            "name": filename,
            "parents": [folder_id],
            "mimeType": SHEETS_MIME,
        }
        service.files().create(body=metadata, media_body=media).execute()
        print(f"  Created {filename} ({len(rows)} rows)")


# ─── High-level storage functions ────────────────────────────────────────────

def upsert_by_date(base_filename, rows, headers, date_field="fetch_date"):
    """Store rows in the monthly file, replacing existing data for the same date.

    Used for: Sensor Tower rankings, Product Hunt top products.
    If data for that date already exists, it is deleted and replaced.

    Args:
        base_filename: Base name (e.g. "download_rank_7d.csv")
        rows: List of dicts to store
        headers: Column names
        date_field: Field used to identify the day's data
    """
    if not rows:
        print(f"  No rows to save for {base_filename}")
        return 0

    folder_id = get_monthly_folder()
    filename = get_monthly_filename(base_filename)

    file_id = find_file_in_folder(filename, folder_id)

    if file_id:
        existing = _read_csv_by_id(file_id)
        new_dates = {r[date_field] for r in rows if date_field in r}
        filtered = [r for r in existing if r.get(date_field) not in new_dates]
        replaced = len(existing) - len(filtered)
        all_rows = filtered + rows
        if replaced:
            print(f"  Replacing {replaced} existing rows for date(s): {new_dates}")
    else:
        all_rows = rows

    _write_csv_to_folder(filename, all_rows, headers, folder_id)
    print(f"  Saved {len(rows)} rows to {filename} (total: {len(all_rows)})")
    return len(rows)


def append_by_url(base_filename, rows, headers, url_field="url"):
    """Append rows to the monthly file, skipping rows with duplicate URLs.

    Used for: News articles.
    Each article is identified by its URL. Duplicates are skipped.

    Args:
        base_filename: Base name (e.g. "news_raw.csv")
        rows: List of dicts to append
        headers: Column names
        url_field: Field used for deduplication

    Returns:
        Number of new rows actually appended
    """
    if not rows:
        return 0

    folder_id = get_monthly_folder()
    filename = get_monthly_filename(base_filename)

    file_id = find_file_in_folder(filename, folder_id)

    if file_id:
        existing = _read_csv_by_id(file_id)
        existing_urls = {r.get(url_field) for r in existing}
        unique_new = [r for r in rows if r.get(url_field) not in existing_urls]

        if not unique_new:
            print(f"  No new rows to append to {filename}")
            return 0

        all_rows = existing + unique_new
    else:
        unique_new = rows
        all_rows = rows

    _write_csv_to_folder(filename, all_rows, headers, folder_id)
    print(f"  Appended {len(unique_new)} new rows to {filename} (total: {len(all_rows)})")
    return len(unique_new)


def read_monthly_csv(base_filename, year=None, month=None):
    """Read a monthly CSV file. Returns list of dicts or empty list."""
    folder_id = get_monthly_folder(year, month)
    filename = get_monthly_filename(base_filename, year, month)

    file_id = find_file_in_folder(filename, folder_id)
    if not file_id:
        print(f"  {filename} not found")
        return []

    rows = _read_csv_by_id(file_id)
    print(f"  Read {len(rows)} rows from {filename}")
    return rows


# ─── Legacy functions (for weekly digest, etc.) ──────────────────────────────

def find_file(filename):
    """Find a file by name in the root folder. Returns file ID or None."""
    return find_file_in_folder(filename, get_folder_id())


def read_csv(filename):
    """Read CSV from root folder."""
    file_id = find_file(filename)
    if not file_id:
        print(f"  {filename} not found in Drive")
        return []
    rows = _read_csv_by_id(file_id)
    print(f"  Read {len(rows)} rows from {filename}")
    return rows


def upload_csv(filename, rows, headers):
    """Upload CSV to the root folder."""
    _write_csv_to_folder(filename, rows, headers, get_folder_id())
    return len(rows)
