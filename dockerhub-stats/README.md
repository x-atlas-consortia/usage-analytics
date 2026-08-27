# Docker Hub Pull Stats Tracker

A lightweight CLI tool to fetch and report all-time pull counts for all public repositories under a Docker Hub organization or user account. It includes clean terminal formatting and a structured CSV export option for spreadsheet reporting.

---

## Features

- Automated Pagination: Handles large accounts seamlessly by pulling 100 repositories per page.
- Descending Sort: Automatically orders repositories from highest to lowest pull counts.
- Summary Totals: Displays individual counts, repository totals, and cumulative pull counts.
- CSV Export: Generates a clean CSV reporting format that mirrors the console layout.

---

## Installation & Requirements

Requires Python 3.13+ and the `requests` library.

1. Install dependencies: `pip install requests`
2. Make executable (macOS/Linux): `chmod +x dockerhub-stats.py`

---

## Usage

### 1. View Stats in Terminal
```
python dockerhub-stats.py <organization_name>
```

Example Output:

```
Fetching public repositories for 'organization_name'...

Public Repository     Created Date     Last Updated     All-Time Pulls
----------------------------------------------------------------------
api-service           2024-03-10       2026-07-15            1,240,500
frontend-app          2025-01-12       2026-05-20              450,210
----------------------------------------------------------------------
TOTAL (Count: 2)                                             1,690,710
```

### 2. Export to CSV for Spreadsheets

```
python dockerhub-stats.py <organization_name> -o report.csv
```

---

## CLI Arguments Reference

| Argument | Flag | Description | Required |
| :--- | :--- | :--- | :--- |
| organization | (Positional) | The Docker Hub organization or username to query. | Yes |
| output | -o, --output | The file path to save the generated CSV report. | No |