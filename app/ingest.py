import csv
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Union

from app.db import get_connection
from app.events import publish
from app.aggregate import rebuild_trips_agg

POINT_RE = re.compile(r"POINT\s*\(\s*([-\d\.]+)\s+([-\d\.]+)\s*\)")

def _parse_point(point_str: str) -> tuple[float, float]:
    """
    Recibe: 'POINT (lng lat)'
    Devuelve: (lng, lat)
    """
    m = POINT_RE.match(point_str.strip())
    if not m:
        raise ValueError(f"Formato invalido: {point_str}")
    return float(m.group(1)), float(m.group(2))

def ingest_csv(csv_path: Union[str, Path]) -> str:
    """
    Ingesta trips.csv hacia BD (trips_raw) y se registra el run en ingestion_runs.
    Retorna run_id.
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    run_id = str(uuid.uuid4())
    started = datetime.now(timezone.utc).isoformat()

    conn = get_connection()
    cur = conn.cursor()

    # Crea run en estado queued/running
    cur.execute(
        """
        insert into ingestion_runs(run_id, status, started_at)
        values (?, ?, ?)
        """,
        (run_id, "running", started),
    )
    conn.commit()
    
    publish(run_id, {"run_id": run_id, "status": "running"})
    
    # Recalcular agregados para consultas rapidas
    rebuild_trips_agg()

    rows = 0
    try:
        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            batch = []

            for r in reader:
                region = r["region"].strip()
                datasource = r["datasource"].strip()
                trip_ts = r["datetime"].strip()

                o_lng, o_lat = _parse_point(r["origin_coord"])
                d_lng, d_lat = _parse_point(r["destination_coord"])

                batch.append((region, datasource, trip_ts, o_lng, o_lat, d_lng, d_lat))
                rows += 1

                # Insertamos por batch para mejorar performance
                if len(batch) >= 10_000:
                    cur.executemany(
                        """
                        insert into trips_raw(region, datasource, trip_ts, origin_lng, origin_lat, dest_lng, dest_lat)
                        values (?, ?, ?, ?, ?, ?, ?)
                        """,
                        batch,
                    )
                    conn.commit()
                    batch.clear()

            if batch:
                cur.executemany(
                    """
                    insert into trips_raw(region, datasource, trip_ts, origin_lng, origin_lat, dest_lng, dest_lat)
                    values (?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch,
                )
                conn.commit()

        finished = datetime.now(timezone.utc).isoformat()
        cur.execute(
            """
            update ingestion_runs
            set status=?, finished_at=?, rows_loaded=?
            where run_id=?
            """,
            ("done", finished, rows, run_id),
        )
        conn.commit()
        
        publish(run_id, {"run_id": run_id, "status": "done", "rows_loaded": rows})
        
        return run_id

    except Exception as e:
        finished = datetime.now(timezone.utc).isoformat()
        cur.execute(
            """
            update ingestion_runs
            set status=?, finished_at=?, error_message=?
            where run_id=?
            """,
            ("failed", finished, str(e), run_id),
        )
        conn.commit()
        
        publish(run_id, {"run_id": run_id, "status": "failed", "error": str(e)})
        
        raise

    finally:
        conn.close()
