# UTIL-Employee-TP-Import

Polls a SharePoint folder for CSV files containing employee salary data, validates and transforms them, uploads the result to an SFTP server, and archives the originals.

## SharePoint Folder Structure

```
Lnn/Automatisering/Import/TimePlan Lønnssats/
├── <incoming CSV files>      ← script picks these up
├── Behandlet/                ← successfully processed files go here
│   └── DD-MM-YYYY HH.MM.SS/ ← timestamped subfolder per file
└── Feil/                     ← failed files + error logs go here
    └── DD-MM-YYYY HH.MM.SS/ ← timestamped subfolder per failure
```

## What the Script Does

1. **Authenticates** with Microsoft Graph API using Azure AD client credentials.
2. **Lists CSV files** in the SharePoint folder (ignores `Behandlet` and `Feil` subfolders).
3. **Validates** each CSV:
   - Checks that the header matches the expected columns: `EMPLID, FIRSTREPETITIONDATE, HOURLYSALARY, HOURSPERWEEK, MONTHLYSALARY, SOCIALSECURITYNUM`.
   - Ensures each row has either `HOURLYSALARY` **or** `MONTHLYSALARY` — not both.
   - Catches any other parsing or unexpected errors.
4. **Transforms** valid CSVs:
   - Removes the header row.
   - Encloses every field in double quotes.
   - Uses `;` as the delimiter.
   - Converts `.00` / `0.00` values to empty strings.
   - Truncates dates from `YYYY-MM-DD HH:MM:SS` to `YYYY-MM-DD`.
5. **Uploads** the output as `TimeplanEmployeeIntegrationOutput.csv` to the SFTP server at `/Import til TP`.
6. **Archives** the original file:
   - On **success** → `Behandlet/<DD-MM-YYYY HH.MM.SS>/`
   - On **failure** → `Feil/<DD-MM-YYYY HH.MM.SS>/` (original file + error log)

## Prerequisites

- Python 3.11+
- An Azure AD **App Registration** with `Sites.ReadWrite.All` application permission (Graph API).
- SFTP server credentials.

## Setup

```bash
# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp .env.example .env
# Edit .env with your actual credentials
```

## Environment Variables

| Variable | Description |
|---|---|
| `AZURE_TENANT_ID` | Azure AD tenant ID |
| `AZURE_CLIENT_ID` | App registration client ID |
| `AZURE_CLIENT_SECRET` | App registration client secret |
| `SHAREPOINT_HOSTNAME` | SharePoint hostname (default: `sparkjop.sharepoint.com`) |
| `SHAREPOINT_SITE_NAME` | SharePoint site name (default: `Sparfile`) |
| `SHAREPOINT_FOLDER_PATH` | Path within the document library (default: `Lnn/Automatisering/Import/TimePlan Lønnssats`) |
| `SFTP_HOST` | SFTP server hostname |
| `SFTP_PORT` | SFTP server port (default: `22`) |
| `SFTP_USERNAME` | SFTP username |
| `SFTP_PASSWORD` | SFTP password |
| `SFTP_REMOTE_PATH` | Remote directory for uploads (default: `/Import til TP`) |

## Usage

```bash
python main.py
```
