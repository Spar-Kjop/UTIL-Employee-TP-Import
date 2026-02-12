"""
UTIL-Employee-TP-Import
=======================
Polls a SharePoint folder for CSV files, validates & transforms them,
uploads the result to an SFTP server, and archives the originals.

SharePoint path : Lønn / Automatisering/Import/TimePlan Lønnssats
Ignored folders : Behandlet, Feil
Output file     : TimeplanEmployeeIntegrationOutput.txt
SFTP destination: /Import til TP
"""

import csv
import io
import logging
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import msal
import paramiko
import requests
from dotenv import load_dotenv

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
load_dotenv()

AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")

SHAREPOINT_HOSTNAME = os.getenv("SHAREPOINT_HOSTNAME", "sparkjop.sharepoint.com")
SHAREPOINT_SITE_NAME = os.getenv("SHAREPOINT_SITE_NAME", "Sparfile")
SHAREPOINT_DRIVE_NAME = os.getenv("SHAREPOINT_DRIVE_NAME", "Lønn")
SHAREPOINT_FOLDER_PATH = os.getenv(
    "SHAREPOINT_FOLDER_PATH", "Automatisering/Import/TimePlan Lønnssats"
)

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USERNAME = os.getenv("SFTP_USERNAME")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")
SFTP_REMOTE_PATH = os.getenv("SFTP_REMOTE_PATH", "/Import til TP")

IGNORED_FOLDERS = {"Behandlet", "Feil"}
EXPECTED_COLUMNS = [
    "EMPLID",
    "FIRSTREPETITIONDATE",
    "HOURLYSALARY",
    "HOURSPERWEEK",
    "MONTHLYSALARY",
    "SOCIALSECURITYNUM",
]
OUTPUT_FILENAME = "TimeplanEmployeeIntegrationOutput.txt"

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# Graph API helpers
# ──────────────────────────────────────────────
def get_access_token() -> str:
    """Acquire an application-only access token via MSAL."""
    authority = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}"
    app = msal.ConfidentialClientApplication(
        AZURE_CLIENT_ID,
        authority=authority,
        client_credential=AZURE_CLIENT_SECRET,
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in result:
        raise RuntimeError(f"Failed to acquire token: {result.get('error_description', result)}")
    return result["access_token"]


def graph_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def get_site_id(token: str) -> str:
    """Resolve the SharePoint site-id for the configured hostname + site name."""
    url = f"{GRAPH_BASE}/sites/{SHAREPOINT_HOSTNAME}:/sites/{SHAREPOINT_SITE_NAME}"
    resp = requests.get(url, headers=graph_headers(token), timeout=30)
    resp.raise_for_status()
    return resp.json()["id"]


def get_drive_id(token: str, site_id: str) -> str:
    """Find the document-library drive by SHAREPOINT_DRIVE_NAME."""
    url = f"{GRAPH_BASE}/sites/{site_id}/drives"
    resp = requests.get(url, headers=graph_headers(token), timeout=30)
    resp.raise_for_status()
    for drive in resp.json().get("value", []):
        if drive["name"] == SHAREPOINT_DRIVE_NAME:
            log.info("Found drive '%s' (id=%s)", SHAREPOINT_DRIVE_NAME, drive["id"])
            return drive["id"]
    available = [d["name"] for d in resp.json().get("value", [])]
    raise RuntimeError(
        f"Drive '{SHAREPOINT_DRIVE_NAME}' not found. Available drives: {available}"
    )


def list_folder_children(token: str, drive_id: str, folder_path: str) -> list[dict]:
    """Return the children items of a folder given its path inside the drive."""
    encoded_path = requests.utils.quote(folder_path)
    url = f"{GRAPH_BASE}/drives/{drive_id}/root:/{encoded_path}:/children"
    resp = requests.get(url, headers=graph_headers(token), timeout=30)
    resp.raise_for_status()
    return resp.json().get("value", [])


def download_file(token: str, drive_id: str, item_id: str) -> bytes:
    """Download file content by item id."""
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/content"
    resp = requests.get(url, headers=graph_headers(token), timeout=60)
    resp.raise_for_status()
    return resp.content


def upload_file_to_sharepoint(
    token: str, drive_id: str, folder_path: str, filename: str, content: bytes
) -> None:
    """Upload (or overwrite) a file into the given folder path."""
    encoded = requests.utils.quote(f"{folder_path}/{filename}")
    url = f"{GRAPH_BASE}/drives/{drive_id}/root:/{encoded}:/content"
    headers = graph_headers(token)
    headers["Content-Type"] = "application/octet-stream"
    resp = requests.put(url, headers=headers, data=content, timeout=60)
    resp.raise_for_status()


def create_sharepoint_folder(token: str, drive_id: str, parent_path: str, folder_name: str) -> None:
    """Create a folder inside a parent path (no-op if it already exists)."""
    encoded_parent = requests.utils.quote(parent_path)
    url = f"{GRAPH_BASE}/drives/{drive_id}/root:/{encoded_parent}:/children"
    body = {
        "name": folder_name,
        "folder": {},
        "@microsoft.graph.conflictBehavior": "fail",
    }
    resp = requests.post(url, headers=graph_headers(token), json=body, timeout=30)
    if resp.status_code == 409:
        # Folder already exists – that's fine
        return
    resp.raise_for_status()


def move_file(token: str, drive_id: str, item_id: str, dest_folder_path: str, new_name: str | None = None) -> None:
    """Move a file to a new parent folder (resolved by path)."""
    # Resolve destination folder item-id
    encoded = requests.utils.quote(dest_folder_path)
    url = f"{GRAPH_BASE}/drives/{drive_id}/root:/{encoded}"
    resp = requests.get(url, headers=graph_headers(token), timeout=30)
    resp.raise_for_status()
    dest_id = resp.json()["id"]

    # PATCH the item to move it
    patch_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}"
    body: dict = {"parentReference": {"id": dest_id}}
    if new_name:
        body["name"] = new_name
    resp = requests.patch(patch_url, headers=graph_headers(token), json=body, timeout=30)
    resp.raise_for_status()


def delete_file(token: str, drive_id: str, item_id: str) -> None:
    """Delete a file by item id."""
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}"
    resp = requests.delete(url, headers=graph_headers(token), timeout=30)
    resp.raise_for_status()


# ──────────────────────────────────────────────
# CSV validation & transformation
# ──────────────────────────────────────────────
def _is_zero_salary(value: str) -> bool:
    """Return True if the salary string represents zero / empty."""
    return value in ("", ".00", "0", "0.00")


def _format_salary(value: str) -> str:
    """Format a salary value to 2 decimals, or return empty if zero."""
    if _is_zero_salary(value):
        return ""
    try:
        d = Decimal(value).quantize(Decimal("0.01"))
        return str(d)
    except InvalidOperation:
        return value  # pass through; row-level validation will catch truly bad data


def _first_of_month(date_str: str) -> str:
    """Parse 'YYYY-MM-DD HH:MM:SS' (or 'YYYY-MM-DD') and return '01-MM-YYYY'."""
    date_part = date_str.split(" ")[0] if " " in date_str else date_str
    dt = datetime.strptime(date_part, "%Y-%m-%d")
    return dt.replace(day=1).strftime("%d-%m-%Y")


def validate_and_transform_csv(
    raw: bytes, filename: str
) -> tuple[list[str], list[str]]:
    """
    Validate the CSV header, then validate each row individually.

    Row-level checks (skip & log on failure):
      - EMPLID must be present
      - SOCIALSECURITYNUM must be present
      - Must have HOURLYSALARY *or* MONTHLYSALARY, not both and not neither

    Returns:
        (output_lines, row_errors)
        output_lines - transformed lines ready for the output file
        row_errors   - human-readable error strings for skipped rows
    """
    try:
        text = raw.decode("utf-8-sig")  # handles BOM if present
    except UnicodeDecodeError:
        text = raw.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text))

    # --- Column check (file-level) ---
    if reader.fieldnames is None:
        raise ValueError(f"{filename}: CSV file is empty or has no header row.")

    actual = [c.strip() for c in reader.fieldnames]
    if actual != EXPECTED_COLUMNS:
        raise ValueError(
            f"{filename}: Unexpected columns.\n"
            f"  Expected: {EXPECTED_COLUMNS}\n"
            f"  Got:      {actual}"
        )

    output_lines: list[str] = []
    row_errors: list[str] = []

    for line_no, row in enumerate(reader, start=2):
        row = {k: v.strip() for k, v in row.items()}

        emplid = row.get("EMPLID", "").strip()
        ssn = row.get("SOCIALSECURITYNUM", "").strip()
        hourly = row.get("HOURLYSALARY", "").strip()
        monthly = row.get("MONTHLYSALARY", "").strip()
        hours = row.get("HOURSPERWEEK", "").strip()
        raw_date = row.get("FIRSTREPETITIONDATE", "").strip()

        hourly_has_value = not _is_zero_salary(hourly)
        monthly_has_value = not _is_zero_salary(monthly)

        # ── Row-level validation ──
        errors: list[str] = []
        if not emplid or not emplid.isdigit() or int(emplid) <= 0:
            errors.append(f"invalid EMPLID ({emplid!r})")
        if not ssn or not ssn.isdigit() or int(ssn) <= 0:
            errors.append(f"invalid SOCIALSECURITYNUM ({ssn!r})")
        if hourly_has_value and monthly_has_value:
            errors.append(
                f"has both HOURLYSALARY ({hourly}) and MONTHLYSALARY ({monthly})"
            )
        if not hourly_has_value and not monthly_has_value:
            errors.append("has neither HOURLYSALARY nor MONTHLYSALARY")

        if errors:
            msg = f"{filename} line {line_no}: {'; '.join(errors)}"
            log.warning("Skipping row: %s", msg)
            row_errors.append(msg)
            continue

        # ── Transform ──
        line = ";".join(
            f'"{v}"'
            for v in [
                emplid,
                _first_of_month(raw_date),
                _format_salary(hourly),
                _format_salary(hours) if not _is_zero_salary(hours) else "",
                _format_salary(monthly),
                ssn,
            ]
        )
        output_lines.append(line)

    if not output_lines and not row_errors:
        raise ValueError(f"{filename}: CSV file contains a header but no data rows.")

    return output_lines, row_errors


# ──────────────────────────────────────────────
# SFTP upload
# ──────────────────────────────────────────────
def upload_to_sftp(content: bytes) -> None:
    """Upload the transformed CSV to the SFTP server."""
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    try:
        transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)

        remote_file = f"{SFTP_REMOTE_PATH}/{OUTPUT_FILENAME}"
        log.info("Uploading %s to SFTP %s:%s", OUTPUT_FILENAME, SFTP_HOST, remote_file)

        with sftp.file(remote_file, "w") as f:
            f.write(content)

        log.info("SFTP upload complete.")
    finally:
        transport.close()


# ──────────────────────────────────────────────
# Archival helpers
# ──────────────────────────────────────────────
def _timestamp_folder_name() -> str:
    """Return a folder name like '12-02-2026 14.35'."""
    return datetime.now(timezone.utc).strftime("%d-%m-%Y %H.%M")


def archive_failure(
    token: str, drive_id: str, item_id: str, filename: str, error_log: str,
    feil_dest_path: str,
) -> None:
    """Move the original file + a log file into the run's Feil/<timestamp>/ folder."""
    # Ensure the Feil timestamp folder exists (created lazily)
    feil_parent = "/".join(feil_dest_path.rsplit("/", 1)[:-1])
    feil_folder_name = feil_dest_path.rsplit("/", 1)[-1]
    create_sharepoint_folder(token, drive_id, feil_parent, feil_folder_name)

    # Upload the error log into the timestamped folder
    log_filename = f"{os.path.splitext(filename)[0]}_error.log"
    upload_file_to_sharepoint(
        token, drive_id, feil_dest_path, log_filename, error_log.encode("utf-8")
    )

    # Move the original file alongside the log
    move_file(token, drive_id, item_id, feil_dest_path)
    log.info("Archived %s → Feil/  (with error log)", filename)


# ──────────────────────────────────────────────
# Main orchestration
# ──────────────────────────────────────────────
def process_file(
    token: str, drive_id: str, item: dict,
    behandlet_dest_path: str, feil_dest_path: str,
) -> tuple[list[str], list[str]]:
    """
    Validate & transform a single CSV file.

    Returns:
        (output_lines, row_errors)
        output_lines – transformed lines (may be empty if whole file failed)
        row_errors   – list of error messages for this file

    Side-effects:
        - Archives file to behandlet_dest_path on success (even partial)
        - Archives file + error log to feil_dest_path on total failure
    """
    filename: str = item["name"]
    item_id: str = item["id"]
    log.info("Processing file: %s", filename)

    try:
        # 1. Download from SharePoint
        raw = download_file(token, drive_id, item_id)

        # 2. Validate & transform (row-level)
        output_lines, row_errors = validate_and_transform_csv(raw, filename)

        if row_errors:
            log.warning(
                "%s: %d row(s) skipped due to validation errors.",
                filename,
                len(row_errors),
            )

        if not output_lines:
            # Every single row failed → treat as full failure
            raise ValueError(
                f"All rows failed validation:\n" + "\n".join(row_errors)
            )

        # 3. Archive original → Behandlet/<timestamp>/
        #    (includes a row-error log if there were partial failures)
        if row_errors:
            error_log_name = f"{os.path.splitext(filename)[0]}_skipped_rows.log"
            error_log_content = (
                f"File: {filename}\n"
                f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n"
                f"Skipped {len(row_errors)} row(s):\n\n"
                + "\n".join(row_errors)
                + "\n"
            )
            upload_file_to_sharepoint(
                token, drive_id, behandlet_dest_path, error_log_name,
                error_log_content.encode("utf-8"),
            )

        move_file(token, drive_id, item_id, behandlet_dest_path)
        log.info(
            "Archived %s → Behandlet/  (%d valid rows, %d skipped)",
            filename, len(output_lines), len(row_errors),
        )
        return output_lines, row_errors

    except Exception as exc:
        error_msg = f"Error processing {filename}: {exc}"
        log.error(error_msg, exc_info=True)

        file_error = f"{filename}: {exc}"

        error_log = (
            f"File: {filename}\n"
            f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n"
            f"Error: {exc}\n"
        )
        try:
            archive_failure(token, drive_id, item_id, filename, error_log, feil_dest_path)
        except Exception as archive_exc:
            log.error("Failed to archive error files: %s", archive_exc, exc_info=True)

        return [], [file_error]


def main() -> None:
    log.info("=== UTIL-Employee-TP-Import started ===")

    # Validate required env vars
    missing = [
        name
        for name, val in {
            "AZURE_TENANT_ID": AZURE_TENANT_ID,
            "AZURE_CLIENT_ID": AZURE_CLIENT_ID,
            "AZURE_CLIENT_SECRET": AZURE_CLIENT_SECRET,
            "SFTP_HOST": SFTP_HOST,
            "SFTP_USERNAME": SFTP_USERNAME,
            "SFTP_PASSWORD": SFTP_PASSWORD,
        }.items()
        if not val
    ]
    if missing:
        log.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)

    # 1. Authenticate
    log.info("Acquiring Graph API access token …")
    token = get_access_token()

    # 2. Resolve site & drive
    log.info("Resolving SharePoint site and drive …")
    site_id = get_site_id(token)
    drive_id = get_drive_id(token, site_id)

    # 3. List files in the target folder
    log.info("Listing files in: %s", SHAREPOINT_FOLDER_PATH)
    children = list_folder_children(token, drive_id, SHAREPOINT_FOLDER_PATH)

    csv_files = [
        item
        for item in children
        if "file" in item  # is a file, not a folder
        and item["name"].lower().endswith(".csv")
    ]

    if not csv_files:
        log.info("No CSV files found. Nothing to do.")
        return

    log.info("Found %d CSV file(s) to process.", len(csv_files))

    # 4. Create a single timestamped folder for this run
    ts = _timestamp_folder_name()
    base = SHAREPOINT_FOLDER_PATH

    behandlet_path = f"{base}/Behandlet"
    behandlet_dest = f"{behandlet_path}/{ts}"
    create_sharepoint_folder(token, drive_id, behandlet_path, ts)

    feil_path = f"{base}/Feil"
    feil_dest = f"{feil_path}/{ts}"
    # Feil folder is created lazily – only when a file actually fails

    log.info("Run folder: %s", ts)

    # 5. Process each CSV – collect all valid output lines
    all_output_lines: list[str] = []
    for item in csv_files:
        lines, _file_errors = process_file(token, drive_id, item, behandlet_dest, feil_dest)
        all_output_lines.extend(lines)

    if not all_output_lines:
        log.warning("No valid output rows from any file. Skipping SFTP upload.")
        return

    # 6. Merge into a single output and upload to SFTP
    merged_output = "\n".join(all_output_lines) + "\n"
    upload_to_sftp(merged_output.encode("utf-8"))

    log.info(
        "Uploaded %d total rows to SFTP as %s",
        len(all_output_lines),
        OUTPUT_FILENAME,
    )
    log.info("=== UTIL-Employee-TP-Import finished ===")


if __name__ == "__main__":
    main()
