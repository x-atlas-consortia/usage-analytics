"""
Daily record count and total bytes_transferred for dtn02+dtn03 in
December 2025 specifically -- to see whether ES's ~95% shortfall that
month is a contiguous block (pointing at an outage) or scattered across
many days (pointing at something more like a flaky/partial ingestion
issue).
"""

import duckdb
from loader_config import load_shared_config

QUERY = """
    SELECT
        download_date,
        COUNT(*) AS total_rows,
        COALESCE(SUM(bytes_transferred), 0) AS total_bytes
    FROM file_download
    WHERE source_node IN ('dtn02', 'dtn03')
      AND download_year = 2025
      AND download_month = 12
    GROUP BY download_date
    ORDER BY download_date
"""

shared_config = load_shared_config()
con = duckdb.connect(shared_config["DUCKDB_PATH"], read_only=True)
try:
    relation = con.sql(QUERY)
    print(" | ".join(relation.columns))
    for row in relation.fetchall():
        print(" | ".join(str(v) for v in row))
finally:
    con.close()
