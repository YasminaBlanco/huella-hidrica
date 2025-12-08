from fastapi import APIRouter, Query
from services.kpi_svc import get_gold_files_service, get_gold_data_service

router = APIRouter(
    prefix="/kpis",
    tags=["KPI", "Gold"]
)

@router.get("/files")
def files():
    return get_gold_files_service()

@router.get("/{kpi}")
def kpi_data(kpi: str, limit: int = Query(None)):
    return get_gold_data_service(kpi=kpi, limit=limit)
