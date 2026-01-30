import math
from datetime import datetime, timedelta

from app.db import get_connection

# Tamaño de celda en grados
CELL_SIZE_DEG = 0.01

def _week_start_iso(dt_str: str) -> str:
    """
    dt_str viene como texto del CSV.
    Se devuelve la fecha del lunes de esa semana en formato YYYY-MM-DD.
    """
    
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    monday = dt.date() - timedelta(days=dt.weekday())
    return monday.isoformat()

def _cell_x(lng: float) -> int:
    return int(math.floor(lng / CELL_SIZE_DEG))

def _cell_y(lat: float) -> int:
    return int(math.floor(lat / CELL_SIZE_DEG))

def rebuild_trips_agg() -> None:
    """
    Recalcula trips_agg completo.
    (Para MVP. En producción sería incremental/particionado.)
    """
    conn = get_connection()
    cur = conn.cursor()

    # Limpiamos agregados
    cur.execute("delete from trips_agg")
    conn.commit()

    # Traemos crudo en stream
    cur.execute("""
        select region, trip_ts, origin_lng, origin_lat, dest_lng, dest_lat
        from trips_raw
    """)

    # Agregación en memoria por key
    agg = {}

    for row in cur.fetchall():
        region = row["region"]
        trip_ts = row["trip_ts"]
        origin_lng = float(row["origin_lng"])
        origin_lat = float(row["origin_lat"])
        dest_lng = float(row["dest_lng"])
        dest_lat = float(row["dest_lat"])

        dt = datetime.fromisoformat(trip_ts.replace("Z", "+00:00"))
        hour = dt.hour
        week_start = (dt.date() - timedelta(days=dt.weekday())).isoformat()

        key = (
            region,
            week_start,
            hour,
            _cell_x(origin_lng),
            _cell_y(origin_lat),
            _cell_x(dest_lng),
            _cell_y(dest_lat),
        )
        agg[key] = agg.get(key, 0) + 1

    # Insert batch
    rows = [
        (k[0], k[1], k[2], k[3], k[4], k[5], k[6], v)
        for k, v in agg.items()
    ]

    cur.executemany(
        """
        insert into trips_agg(
            region, week_start, hour_of_day,
            origin_cell_x, origin_cell_y,
            dest_cell_x, dest_cell_y,
            trips_count
        )
        values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    conn.commit()
    conn.close()