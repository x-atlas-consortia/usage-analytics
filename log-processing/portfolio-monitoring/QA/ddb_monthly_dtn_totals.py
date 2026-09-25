"""
Monthly record count and total bytes_transferred for dtn02+dtn03 combined
(app001 excluded), through end of July -- for comparing bucket-by-bucket
against the equivalent ES date_histogram aggregation, to find where the
~32.77 TB residual is concentrated rather than just that it exists.
"""

import duckdb
from loader_config import load_shared_config

QUERY = """
    SELECT
        download_year,
        download_month,
        COUNT(*) AS total_rows,
        COALESCE(SUM(bytes_transferred), 0) AS total_bytes
    FROM file_download
    WHERE source_node IN ('dtn02', 'dtn03')
      AND download_date <= '2026-07-31'
    GROUP BY download_year, download_month
    ORDER BY download_year, download_month
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
