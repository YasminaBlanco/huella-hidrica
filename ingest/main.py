import os
from datetime import datetime, timezone

from fastapi import FastAPI

from routers import ingest, kpi

app = FastAPI(title="Huella Hídrica LATAM", version="0.0.1")

app.include_router(ingest.router)
app.include_router(kpi.router)


@app.get("/health", tags=["Health"])
def health_check():
    service_status = "ok"

    return {
        "status": service_status,
        "service": "Agua Latam API",
        "version": os.getenv("API_VERSION", "1.0.0"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
