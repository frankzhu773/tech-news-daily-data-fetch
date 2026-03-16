"""
Google Drive Native Sheets Storage Utility (gws CLI version)

Stores data as native Google Sheets in Google Drive using the gws CLI.
Data is organized into Latest (overwritten) and Cumulative (upserted) folders.

Folder structure:
  GOOGLE_DRIVE_FOLDER_ID/
    2026/
      Latest/
        news_raw_2026_latest        (native Google Sheet)
        download_rank_7d_2026_latest
        ...
      Cumulative/
        news_raw_2026               (native Google Sheet)
        download_rank_7d_2026
        ...

Uses the pre-configured gws CLI for all Google Drive and Sheets operations.
No OAuth credentials needed — gws handles authentication.
"""

import os
import io
import json
import logging
import subprocess
import tempfile
from datetime import datetime, timezone

log = logging.getLogger(__name__)

GOOGLE_DRIVE_FOLDER_ID = "1hzvd_SkU3z2oP-op9LtYn3Q50Op7qY_P"
SHEETS_MIME = "application/vnd.google-apps.spreadsheet"
FOLDER_MIME = "application/vnd.google-apps.folder"

# ─── Known spreadsheet IDs (pre-existing) ──────────────────────────────────
# These are the native Google Sheets already created in the Drive folder.
# We look them up dynamically, but cache them here for performance.
_sheet_id_cache = {}


# ─── gws CLI helpers ───────────────────────────────────────────────────────

def _run_gws(args: list[str], input_json: dict = None) -> dict:
    """Run a gws CLI command and return parsed JSON output.
    
    Args:
        args: Command arguments (e.g., ['drive', 'files', 'list', '--params', '...'])
        input_json: If provided, passed as --json argument
    
    Returns:
        Parsed JSON response dict
    """
    cmd = ["gws"] + args
    if input_json is not None:
        cmd.extend(["--json", json.dumps(input_json)])
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=120,
    )
    
    if result.returncode != 0:
        log.error(f"gws command failed: {' '.join(cmd[:6])}...")
        log.error(f"  stderr: {result.stderr[:500]}")
        raise RuntimeError(f"gws error: {result.stderr[:200]}")
    
    if not result.stdout.strip():
        return {}
    
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        log.warning(f"Could not parse gws output as JSON: {result.stdout[:200]}")
        return {}


# ─── Folder management ──────────────────────────────────────────────────────

def ensure_subfolder(parent_id: str, folder_name: str) -> str:
    """Find or create a subfolder under parent_id. Returns the folder ID."""
    query = f"name = '{folder_name}' and '{parent_id}' in parents and mimeType = '{FOLDER_MIME}' and trashed = false"
    result = _run_gws([
        "drive", "files", "list",
        "--params", json.dumps({"q": query, "fields": "files(id)", "pageSize": 1})
    ])
    
    files = result.get("files", [])
    if files:
        return files[0]["id"]
    
    # Create the folder
    metadata = {
        "name": folder_name,
        "parents": [parent_id],
        "mimeType": FOLDER_MIME,
    }
    create_result = _run_gws([
        "drive", "files", "create",
        "--params", json.dumps({"fields": "id"}),
    ], input_json=metadata)
    
    folder_id = create_result.get("id", "")
    print(f"  Created folder: {folder_name} ({folder_id})")
    return folder_id


def _get_year_folder(year=None) -> str:
    """Get (or create) the year subfolder under root. Returns folder ID."""
    if year is None:
        year = datetime.now(timezone.utc).year
    return ensure_subfolder(GOOGLE_DRIVE_FOLDER_ID, str(year))


def get_latest_folder(year=None) -> str:
    """Get (or create) the Latest folder: root/{year}/Latest."""
    year_folder = _get_year_folder(year)
    return ensure_subfolder(year_folder, "Latest")


def get_cumulative_folder(year=None) -> str:
    """Get (or create) the Cumulative folder: root/{year}/Cumulative."""
    year_folder = _get_year_folder(year)
    return ensure_subfolder(year_folder, "Cumulative")


def _latest_filename(base_name: str, year=None) -> str:
    """Generate Latest filename: news_raw -> news_raw_2026_latest"""
    if year is None:
        year = datetime.now(timezone.utc).year
    base = base_name.removesuffix(".xlsx").removesuffix(".csv")
    return f"{base}_{year}_latest"


def _cumulative_filename(base_name: str, year=None) -> str:
    """Generate Cumulative filename: news_raw -> news_raw_2026"""
    if year is None:
        year = datetime.now(timezone.utc).year
    base = base_name.removesuffix(".xlsx").removesuffix(".csv")
    return f"{base}_{year}"


# ─── Spreadsheet operations ────────────────────────────────────────────────

def find_sheet_in_folder(name: str, folder_id: str) -> str | None:
    """Find a native Google Sheet by name in a specific folder. Returns spreadsheet ID or None."""
    cache_key = f"{folder_id}/{name}"
    if cache_key in _sheet_id_cache:
        return _sheet_id_cache[cache_key]
    
    query = f"name = '{name}' and '{folder_id}' in parents and mimeType = '{SHEETS_MIME}' and trashed = false"
    result = _run_gws([
        "drive", "files", "list",
        "--params", json.dumps({"q": query, "fields": "files(id,name)", "pageSize": 1})
    ])
    
    files = result.get("files", [])
    if files:
        sheet_id = files[0]["id"]
        _sheet_id_cache[cache_key] = sheet_id
        return sheet_id
    return None


def _create_sheet_in_folder(name: str, folder_id: str) -> str:
    """Create a new native Google Sheet in the specified folder. Returns spreadsheet ID."""
    metadata = {
        "name": name,
        "parents": [folder_id],
        "mimeType": SHEETS_MIME,
    }
    result = _run_gws([
        "drive", "files", "create",
        "--params", json.dumps({"fields": "id"}),
    ], input_json=metadata)
    
    sheet_id = result.get("id", "")
    cache_key = f"{folder_id}/{name}"
    _sheet_id_cache[cache_key] = sheet_id
    print(f"  Created sheet: {name} ({sheet_id})")
    return sheet_id


def _read_sheet_values(spreadsheet_id: str) -> list[list[str]]:
    """Read all values from a Google Sheet. Returns list of rows (each row is a list of strings)."""
    try:
        result = _run_gws([
            "sheets", "spreadsheets", "values", "get",
            "--params", json.dumps({
                "spreadsheetId": spreadsheet_id,
                "range": "Sheet1",
            })
        ])
        return result.get("values", [])
    except Exception as e:
        log.warning(f"Failed to read sheet {spreadsheet_id}: {e}")
        return []


def _write_sheet_values(spreadsheet_id: str, rows: list[list[str]]) -> None:
    """Clear and write all values to a Google Sheet."""
    # First clear the sheet
    try:
        _run_gws([
            "sheets", "spreadsheets", "values", "clear",
            "--params", json.dumps({
                "spreadsheetId": spreadsheet_id,
                "range": "Sheet1",
            }),
        ], input_json={})
    except Exception as e:
        log.warning(f"Failed to clear sheet (may be empty): {e}")
    
    if not rows:
        return
    
    # Write in batches of 500 rows to avoid request size limits
    BATCH_SIZE = 500
    for batch_start in range(0, len(rows), BATCH_SIZE):
        batch = rows[batch_start:batch_start + BATCH_SIZE]
        
        if batch_start == 0:
            range_str = "Sheet1!A1"
        else:
            range_str = f"Sheet1!A{batch_start + 1}"
        
        # For first batch, use update; for subsequent, use append
        if batch_start == 0:
            _run_gws([
                "sheets", "spreadsheets", "values", "update",
                "--params", json.dumps({
                    "spreadsheetId": spreadsheet_id,
                    "range": range_str,
                    "valueInputOption": "RAW",
                }),
            ], input_json={"values": batch})
        else:
            _run_gws([
                "sheets", "spreadsheets", "values", "append",
                "--params", json.dumps({
                    "spreadsheetId": spreadsheet_id,
                    "range": "Sheet1!A1",
                    "valueInputOption": "RAW",
                    "insertDataOption": "INSERT_ROWS",
                }),
            ], input_json={"values": batch})


def _rows_to_values(rows: list[dict], headers: list[str]) -> list[list[str]]:
    """Convert a list of dicts to a list of lists (with header row)."""
    values = [headers]
    for row in rows:
        values.append([str(row.get(h, "")) for h in headers])
    return values


def _values_to_rows(values: list[list[str]]) -> list[dict]:
    """Convert sheet values (list of lists) back to list of dicts."""
    if not values or len(values) < 2:
        return []
    headers = values[0]
    rows = []
    for row_values in values[1:]:
        row = {}
        for i, h in enumerate(headers):
            row[h] = row_values[i] if i < len(row_values) else ""
        rows.append(row)
    return rows


def _write_rows_to_sheet(name: str, rows: list[dict], headers: list[str], folder_id: str) -> None:
    """Write rows to a native Google Sheet (create if needed, overwrite if exists)."""
    sheet_id = find_sheet_in_folder(name, folder_id)
    
    if not sheet_id:
        sheet_id = _create_sheet_in_folder(name, folder_id)
    
    values = _rows_to_values(rows, headers)
    _write_sheet_values(sheet_id, values)
    print(f"  Updated {name} ({len(rows)} rows)")


# ─── High-level storage functions ────────────────────────────────────────────

def save_latest_and_cumulative(base_filename: str, rows: list[dict], headers: list[str], dedup_keys: list[str]) -> int:
    """Two-step save: overwrite Latest, then upsert into Cumulative.

    Step 1 — Latest: Overwrite the sheet with only the new rows.
    Step 2 — Cumulative: Read existing data, merge with new rows
             deduplicating by dedup_keys (new rows win on conflict).

    Args:
        base_filename: File name (e.g. "download_rank_7d.xlsx")
        rows: List of dicts to store
        headers: Column names
        dedup_keys: List of field names that together form a unique key
    """
    if not rows:
        print(f"  No rows to save for {base_filename}")
        return 0

    latest_name = _latest_filename(base_filename)
    cumulative_name = _cumulative_filename(base_filename)

    # Step 1: Overwrite Latest
    latest_folder = get_latest_folder()
    _write_rows_to_sheet(latest_name, rows, headers, latest_folder)
    print(f"  [Latest] Wrote {len(rows)} rows to {latest_name}")

    # Step 2: Upsert into Cumulative
    cumulative_folder = get_cumulative_folder()
    sheet_id = find_sheet_in_folder(cumulative_name, cumulative_folder)

    if sheet_id:
        # Read existing data
        existing_values = _read_sheet_values(sheet_id)
        existing = _values_to_rows(existing_values)
        
        # Build a set of composite keys from the new rows
        def _make_key(row):
            return tuple(str(row.get(k, "")) for k in dedup_keys)

        new_keys = {_make_key(r) for r in rows}
        # Keep existing rows whose key is NOT in the new batch
        filtered = [r for r in existing if _make_key(r) not in new_keys]
        replaced = len(existing) - len(filtered)
        all_rows = filtered + rows
        if replaced:
            print(f"  [Cumulative] Replacing {replaced} existing rows")
    else:
        all_rows = rows

    _write_rows_to_sheet(cumulative_name, all_rows, headers, cumulative_folder)
    print(f"  [Cumulative] Saved {len(rows)} new rows to {cumulative_name} (total: {len(all_rows)})")
    return len(rows)


# ─── Read helpers ─────────────────────────────────────────────────────────────

def read_latest(base_filename: str, year=None) -> list[dict]:
    """Read a sheet from the Latest folder. Returns list of dicts."""
    folder_id = get_latest_folder(year)
    name = _latest_filename(base_filename, year)

    sheet_id = find_sheet_in_folder(name, folder_id)
    if not sheet_id:
        print(f"  {name} not found in Latest")
        return []

    values = _read_sheet_values(sheet_id)
    rows = _values_to_rows(values)
    print(f"  Read {len(rows)} rows from Latest/{name}")
    return rows


def read_cumulative(base_filename: str, year=None) -> list[dict]:
    """Read a sheet from the Cumulative folder. Returns list of dicts."""
    folder_id = get_cumulative_folder(year)
    name = _cumulative_filename(base_filename, year)

    sheet_id = find_sheet_in_folder(name, folder_id)
    if not sheet_id:
        print(f"  {name} not found in Cumulative")
        return []

    values = _read_sheet_values(sheet_id)
    rows = _values_to_rows(values)
    print(f"  Read {len(rows)} rows from Cumulative/{name}")
    return rows
