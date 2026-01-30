from typing import Optional, Dict, Any
from app.db import get_connection

def get_run(run_id: str) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("select * from ingestion_runs where run_id = ?", (run_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return dict(row)
