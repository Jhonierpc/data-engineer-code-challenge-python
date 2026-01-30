import math
from typing import Dict, Any

from app.db import get_connection
from app.aggregate import CELL_SIZE_DEG

def _cell_x(lng: float) -> int:
    return int(math.floor(lng / CELL_SIZE_DEG))

def _cell_y(lat: float) -> int:
    return int(math.floor(lat / CELL_SIZE_DEG))

def weekly_average_trips(region: str, min_lat: float, min_lng: float, max_lat: float, max_lng: float) -> Dict[str, Any]:
    # Convertir bbox a rango de celdas
    x_min = _cell_x(min_lng)
    x_max = _cell_x(max_lng)
    y_min = _cell_y(min_lat)
    y_max = _cell_y(max_lat)

    # normalizar por si vienen invertidos
    if x_min > x_max:
        x_min, x_max = x_max, x_min
    if y_min > y_max:
        y_min, y_max = y_max, y_min

    conn = get_connection()
    cur = conn.cursor()

    # Total por semana dentro del bbox, usamos origin_cell como “ubicación del viaje”
    cur.execute(
        """
        select
          week_start,
          SUM(trips_count) AS trips_in_week
        from trips_agg
        where region = ?
          and origin_cell_x between ? and ?
          and origin_cell_y between ? and ?
        group by week_start
        """,
        (region, x_min, x_max, y_min, y_max),
    )

    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {
            "region": region,
            "bbox": {"min_lat": min_lat, "min_lng": min_lng, "max_lat": max_lat, "max_lng": max_lng},
            "weeks_count": 0,
            "weekly_avg_trips": 0.0,
            "weekly_totals": [],
        }

    weekly_totals = [{"week_start": r["week_start"], "trips": int(r["trips_in_week"])} for r in rows]
    avg = sum(x["trips"] for x in weekly_totals) / len(weekly_totals)

    return {
        "region": region,
        "bbox": {"min_lat": min_lat, "min_lng": min_lng, "max_lat": max_lat, "max_lng": max_lng},
        "weeks_count": len(weekly_totals),
        "weekly_avg_trips": float(avg),
        "weekly_totals": weekly_totals,
    }
