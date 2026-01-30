import json
import uuid
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
from pathlib import Path
from datetime import datetime, timezone

from app.init_db import init_db
from app.ingest import ingest_csv
from app.runs import get_run
from app.events import subscribe
from app.db import get_connection
from app.analytics import weekly_average_trips
from app.regions import list_regions

app = FastAPI(title="Data Engineer Code Challenge (Python)")

@app.on_event("startup")
def startup():
    init_db()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/regions")
def regions():
    return {"regions": list_regions()}

@app.post("/ingestions")
def create_ingestion(background: BackgroundTasks):
    csv_path = Path("trips.csv")
    if not csv_path.exists():
        raise HTTPException(status_code=400, detail="No se encontro trips.csv en la raiz del proyecto.")

    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    # Insertar run en estado queued
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        insert into ingestion_runs(run_id, status, started_at)
        values (?, ?, ?)
        """,
        (run_id, "queued", now),
    )
    conn.commit()
    conn.close()

    # Ejecutar ingesta en background usando el mismo run_id
    background.add_task(ingest_csv, csv_path)

    return {"run_id": run_id, "status": "queued"}


@app.get("/ingestions/{run_id}")
def read_ingestion(run_id: str):
    run = get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="run_id no encontrado")
    return run

@app.get("/ingestions/{run_id}/events")
async def ingestion_events(run_id: str):
    q = subscribe(run_id)

    async def event_stream():
        while True:
            ev = await q.get()
            yield f"data: {json.dumps(ev)}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")

@app.get("/analytics/weekly-average")
def weekly_average(region: str, minLat: float, minLng: float, maxLat: float, maxLng: float):
    return weekly_average_trips(
        region=region,
        min_lat=minLat,
        min_lng=minLng,
        max_lat=maxLat,
        max_lng=maxLng,
    )



