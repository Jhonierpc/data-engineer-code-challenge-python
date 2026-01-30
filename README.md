# Data Engineer Code Challenge (Python)

Prueba técnica Jhonier Riascos Zapata.

Este repo implementa una solución tipo MVP para la prueba técnica de Data Engineering enfocada en:
- ingesta on-demand de un CSV de viajes (origin/destination trips)
- agrupación de “similar trips” por origen/destino/hora
- servicio para calcular promedio semanal de viajes por región + bounding box
- estado de ingesta sin polling (SSE)

---

## Requisitos cubiertos

### 1) Ingesta on-demand
- `POST /ingestions` dispara la ingesta del archivo `trips.csv` (en la raíz del repo).
- Se registra un `run_id` y se actualiza su estado.

### 2) Agrupación de “similar trips”
Definición usada:
- “Similar” = misma `region` + misma `hour_of_day` + origen/destino dentro de la misma celda espacial (grid).
- Se usa un bucketing simple por grilla (cell_size en grados) para convertir lat/lng a `cell_x`, `cell_y`.
- Resultado: tabla agregada `trips_agg` con conteos por `week_start` + `hour_of_day` + celdas (origen/destino).

### 3) Weekly average por bounding box + region
- `GET /analytics/weekly-average` recibe `region` y bbox (`minLat,minLng,maxLat,maxLng`)
- El bbox se transforma a rangos de celdas y se consulta `trips_agg` para sumar trips por semana.
- Se retorna el promedio semanal y los totales por semana.

### 4) Status sin polling
- `GET /ingestions/{run_id}/events` expone un stream SSE.
- Durante la ingesta se publican eventos `running`, `done` o `failed`.

---

## Arquitectura y modelo de datos

### Tablas principales

- **`trips_raw`**
  - Almacena los viajes crudos tal como llegan del CSV.
  - Funciona como capa de ingesta.

- **`trips_agg`**
  - Tabla analítica agregada.
  - Optimizada para consultas de BI y métricas.

- **`ingestion_runs`**
  - Controla el estado y metadata de cada ingesta.

---

## Para ejecución (local)

### 1) Instalar dependencias
```bash
pip3 install -r requirements.txt
```

### 2) Levantar la API
```bash
python3 -m uvicorn app.main:app --reload
```
- La API queda disponible en:
```bash
http://127.0.0.1:8000
```

### 3) Health check
```bash
curl http://127.0.0.1:8000/health
```

## Flujo de uso

### A) Disparar una ingesta
```bash
curl -s -X POST http://127.0.0.1:8000/ingestions
```

### B) Consultar estado puntual de la ingesta
```bash
curl http://127.0.0.1:8000/ingestions/<RUN_ID>
```

### C) Consultar regiones disponibles
```bash
curl http://127.0.0.1:8000/regions
```

### D) Calcular promedio semanal
```bash
curl "http://127.0.0.1:8000/analytics/weekly-average?region=<REGION>&minLat=52.4&minLng=13.3&maxLat=52.6&maxLng=13.6"
```