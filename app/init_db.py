from app.db import get_connection

def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    create table if not exists ingestion_runs (
        run_id TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT,
        rows_loaded INTEGER DEFAULT 0,
        error_message TEXT
    )
    """)

    cur.execute("""
    create table if not exists trips_raw (
        region TEXT NOT NULL,
        datasource TEXT NOT NULL,
        trip_ts TEXT NOT NULL,
        origin_lng REAL NOT NULL,
        origin_lat REAL NOT NULL,
        dest_lng REAL NOT NULL,
        dest_lat REAL NOT NULL
    )
    """)
    
    cur.execute("""
    create index if not exists idx_trips_raw_region_ts
    on trips_raw(region, trip_ts)
    """)
    
    cur.execute("""
    create table if not exists trips_agg (
        region TEXT NOT NULL,
        week_start TEXT NOT NULL,
        hour_of_day INTEGER NOT NULL,
        origin_cell_x INTEGER NOT NULL,
        origin_cell_y INTEGER NOT NULL,
        dest_cell_x INTEGER NOT NULL,
        dest_cell_y INTEGER NOT NULL,
        trips_count INTEGER NOT NULL
    )
    """)

    cur.execute("""
    create index if not exists idx_trips_agg_query
    on trips_agg(region, week_start, hour_of_day, origin_cell_x, origin_cell_y)
    """)

    conn.commit()
    conn.close()
