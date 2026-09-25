"""
Count and total bytes_transferred for dtn02+dtn03 combined (app001
excluded), through end of July -- for comparison against the equivalent
ES query, now that app001 itself is reconciled to a small residual.
"""

import duckdb
from loader_config import load_shared_config

QUERY = """
    SELECT
        COUNT(*) AS total_rows,
        COALESCE(SUM(bytes_transferred), 0) AS total_bytes
    FROM file_download
    WHERE source_node IN ('dtn02', 'dtn03')
      AND download_date <= '2026-07-31'
"""

shared_config = load_shared_config()
con = duckdb.connect(shared_config["DUCKDB_PATH"], read_only=True)
try:
    print(con.execute(QUERY).fetchone())
finally:
    con.close()
