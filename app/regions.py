from typing import List
from app.db import get_connection

def list_regions() -> List[str]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("select distinct region from trips_raw order by region")
    rows = cur.fetchall()
    conn.close()
    return [r["region"] for r in rows]
