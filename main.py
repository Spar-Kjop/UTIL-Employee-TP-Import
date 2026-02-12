"""
UTIL-Employee-TP-Import
=======================
Polls a SharePoint folder for CSV files, validates & transforms them,
uploads the result to an SFTP server, and archives the originals.

SharePoint path : Lnn/Automatisering/Import/TimePlan Lønnssats
Ignored folders : Behandlet, Feil
Output file     : TimeplanEmployeeIntegrationOutput.csv
SFTP destination: /Import til TP
"""

import csv
import io
import logging
import os
import sys
from datetime import datetime, timezone

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
OUTPUT_FILENAME = "TimeplanEmployeeIntegrationOutput.csv"

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
def validate_csv(raw: bytes, filename: str) -> list[dict]:
    """
    Validate that the CSV has the expected columns and that each row
    contains either HOURLYSALARY or MONTHLYSALARY, but not both
    (a non-zero value in both columns is an error).

    Returns parsed rows as list[dict].
    """
    try:
        text = raw.decode("utf-8-sig")  # handles BOM if present
    except UnicodeDecodeError:
        text = raw.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text))

    # --- Column check ---
    if reader.fieldnames is None:
        raise ValueError(f"{filename}: CSV file is empty or has no header row.")

    actual = [c.strip() for c in reader.fieldnames]
    if actual != EXPECTED_COLUMNS:
        raise ValueError(
            f"{filename}: Unexpected columns.\n"
            f"  Expected: {EXPECTED_COLUMNS}\n"
            f"  Got:      {actual}"
        )

    rows: list[dict] = []
    for line_no, row in enumerate(reader, start=2):
        hourly = row.get("HOURLYSALARY", "").strip()
        monthly = row.get("MONTHLYSALARY", "").strip()

        hourly_has_value = hourly not in ("", ".00", "0", "0.00")
        monthly_has_value = monthly not in ("", ".00", "0", "0.00")

        if hourly_has_value and monthly_has_value:
            raise ValueError(
                f"{filename} line {line_no}: Row has both HOURLYSALARY ({hourly}) "
                f"and MONTHLYSALARY ({monthly}). Only one is allowed."
            )

        rows.append({k: v.strip() for k, v in row.items()})

    if not rows:
        raise ValueError(f"{filename}: CSV file contains a header but no data rows.")

    return rows


def transform_value(value: str) -> str:
    """Clean up a single field value."""
    if value in (".00", "0.00", "0"):
        return ""
    return value


def transform_date(date_str: str) -> str:
    """Convert 'YYYY-MM-DD HH:MM:SS' → 'YYYY-MM-DD'."""
    return date_str.split(" ")[0] if " " in date_str else date_str


def transform_csv(rows: list[dict]) -> str:
    """
    Transform validated rows into the target format:
    - No header
    - Fields enclosed in double-quotes
    - Semicolon separated
    - .00 values → empty
    - Date truncated to YYYY-MM-DD
    """
    output_lines: list[str] = []
    for row in rows:
        emplid = transform_value(row["EMPLID"])
        date = transform_date(row["FIRSTREPETITIONDATE"])
        hourly = transform_value(row["HOURLYSALARY"])
        hours = transform_value(row["HOURSPERWEEK"])
        monthly = transform_value(row["MONTHLYSALARY"])
        ssn = transform_value(row["SOCIALSECURITYNUM"])

        line = ";".join(
            f'"{v}"' for v in [emplid, date, hourly, hours, monthly, ssn]
        )
        output_lines.append(line)

    return "\n".join(output_lines) + "\n"


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
    """Return a folder name like '12-02-2026 14.35.07'."""
    return datetime.now(timezone.utc).strftime("%d-%m-%Y %H.%M.%S")


def archive_success(
    token: str, drive_id: str, item_id: str, filename: str
) -> None:
    """Move the original file into  Behandlet/<timestamp>/."""
    ts = _timestamp_folder_name()
    base = SHAREPOINT_FOLDER_PATH
    behandlet_path = f"{base}/Behandlet"
    dest_path = f"{behandlet_path}/{ts}"

    create_sharepoint_folder(token, drive_id, behandlet_path, ts)
    move_file(token, drive_id, item_id, dest_path)
    log.info("Archived %s → Behandlet/%s/", filename, ts)


def archive_failure(
    token: str, drive_id: str, item_id: str, filename: str, error_log: str
) -> None:
    """Move the original file + a log file into  Feil/<timestamp>/."""
    ts = _timestamp_folder_name()
    base = SHAREPOINT_FOLDER_PATH
    feil_path = f"{base}/Feil"
    dest_path = f"{feil_path}/{ts}"

    create_sharepoint_folder(token, drive_id, feil_path, ts)

    # Upload the error log into the timestamped folder
    log_filename = f"{os.path.splitext(filename)[0]}_error.log"
    upload_file_to_sharepoint(
        token, drive_id, dest_path, log_filename, error_log.encode("utf-8")
    )

    # Move the original file alongside the log
    move_file(token, drive_id, item_id, dest_path)
    log.info("Archived %s → Feil/%s/  (with error log)", filename, ts)


# ──────────────────────────────────────────────
# Main orchestration
# ──────────────────────────────────────────────
def process_file(
    token: str, drive_id: str, item: dict
) -> None:
    """Validate, transform, upload, and archive a single CSV file."""
    filename: str = item["name"]
    item_id: str = item["id"]
    log.info("Processing file: %s", filename)

    try:
        # 1. Download from SharePoint
        raw = download_file(token, drive_id, item_id)

        # 2. Validate
        rows = validate_csv(raw, filename)

        # 3. Transform
        output = transform_csv(rows)
        output_bytes = output.encode("utf-8")

        # 4. Upload to SFTP
        upload_to_sftp(output_bytes)

        # 5. Archive original file → Behandlet/<timestamp>/
        archive_success(token, drive_id, item_id, filename)

        log.info("Successfully processed %s", filename)

    except Exception as exc:
        error_msg = f"Error processing {filename}: {exc}"
        log.error(error_msg, exc_info=True)

        # Build a detailed error log
        error_log = (
            f"File: {filename}\n"
            f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n"
            f"Error: {exc}\n"
        )

        try:
            archive_failure(token, drive_id, item_id, filename, error_log)
        except Exception as archive_exc:
            log.error("Failed to archive error files: %s", archive_exc, exc_info=True)


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

    # 4. Process each CSV file one-by-one
    for item in csv_files:
        process_file(token, drive_id, item)

    log.info("=== UTIL-Employee-TP-Import finished ===")


if __name__ == "__main__":
    main()
