"""
Row count from file_download only, through end of July -- for comparing
against ES's non-loopback record count (57,298,790) over the same range.
"""

import duckdb
from loader_config import load_shared_config

QUERY = """
    SELECT COUNT(*) AS total_rows
    FROM file_download
    WHERE download_date <= '2026-07-31'
"""

shared_config = load_shared_config()
con = duckdb.connect(shared_config["DUCKDB_PATH"], read_only=True)
try:
    print(con.execute(QUERY).fetchone())
finally:
    con.close()
