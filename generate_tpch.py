import duckdb
import pyarrow.parquet as pq
from pathlib import Path
import tempfile

Path("tests/tpch").mkdir(parents=True, exist_ok=True)

with tempfile.TemporaryDirectory() as temp_dir:
    db = f"{temp_dir}/test.db"
    con = duckdb.connect(db)
    con.execute("INSTALL tpch; LOAD tpch")
    con.execute("CALL dbgen(sf=1)")
    tables = [t[0] for t in con.execute("show tables").fetchall()]
    for t in tables:
        res = con.query("SELECT * FROM " + t)
        pq.write_table(res.to_arrow_table(), f"tests/tpch/{t}.parquet")
        print(f"Wrote dataset to tests/tpch/{t}.parquet")
