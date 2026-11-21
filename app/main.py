"""FastAPI application: CSV upload, station-scoped SQL rounds and downloads."""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel

from .config import MAX_CONCURRENT_ROUNDS, MAX_UPLOAD_BYTES, QUERY_TIMEOUT_SECONDS
from .csv_io import CsvError, parse_observations_csv
from .registry import (
    CANCELLED,
    FAILED,
    SUCCEEDED,
    RoundNotFound,
    RoundRegistry,
    SlotsFullError,
)
from .sql_guard import SqlRejected, validate_sql
from .store import DataStore

STATIC_DIR = Path(__file__).resolve().parent / "static"

registry = RoundRegistry()
store = DataStore()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    registry.shutdown()


app = FastAPI(
    title="观测数据 SQL 清洗沙箱",
    description=(
        "上传观测 CSV，按站点提交只读 SELECT/WITH 查询。每轮独立隔离、"
        "最多两轮并行、5 秒超时、结果上限 8 MiB。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


class SqlSubmission(BaseModel):
    station_id: str
    sql: str


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html", media_type="text/html")


@app.get("/api/health")
def health() -> dict:
    dataset = store.current()
    return {
        "status": "ok",
        "max_concurrent_rounds": MAX_CONCURRENT_ROUNDS,
        "query_timeout_seconds": QUERY_TIMEOUT_SECONDS,
        "active_rounds": registry.active_count(),
        "dataset_loaded": dataset is not None,
        "stations": dataset.stations if dataset else [],
    }


@app.post("/api/upload")
async def upload_csv(file: UploadFile = File(...)) -> dict:
    raw = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(raw) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"文件超过 1 MiB 上限（{len(raw)} bytes）",
        )
    try:
        parsed = parse_observations_csv(raw)
    except CsvError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    dataset = store.replace(parsed, file.filename or "upload.csv")
    return {
        "filename": dataset.filename,
        "total_rows": dataset.total_rows,
        "stations": dataset.stations,
        "station_counts": {
            station: len(cols["station_id"]) for station, cols in dataset.by_station.items()
        },
        "message": "上传成功，请选择站点并提交 SQL",
    }


@app.post("/api/rounds")
async def create_round(submission: SqlSubmission) -> JSONResponse:
    station_id = submission.station_id.strip()
    if not station_id:
        raise HTTPException(status_code=400, detail="station_id 不能为空")
    try:
        validated = validate_sql(submission.sql)
    except SqlRejected as exc:
        raise HTTPException(status_code=400, detail=f"SQL 校验失败：{exc}") from exc

    columns = store.station_columns(station_id)
    if columns is None:
        dataset = store.current()
        if dataset is None:
            raise HTTPException(status_code=409, detail="尚未上传观测 CSV，请先上传数据")
        raise HTTPException(
            status_code=404,
            detail=f"站点 {station_id!r} 不存在；可选站点：{dataset.stations}",
        )

    try:
        round_obj = registry.submit(station_id, validated.text, columns)
    except SlotsFullError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return JSONResponse(status_code=201, content=round_obj.public_dict())


@app.get("/api/rounds")
def list_rounds() -> dict:
    return {"rounds": registry.list_rounds(), "active": registry.active_count()}


@app.get("/api/rounds/{round_id}")
def get_round(round_id: str) -> dict:
    try:
        return registry.snapshot(round_id)
    except RoundNotFound:
        raise HTTPException(status_code=404, detail="轮次不存在或已被清理") from None


@app.post("/api/rounds/{round_id}/cancel")
def cancel_round(round_id: str) -> dict:
    try:
        accepted, message = registry.request_cancel(round_id)
    except RoundNotFound:
        raise HTTPException(status_code=404, detail="轮次不存在或已被清理") from None
    if not accepted:
        # Succeeded first -> cannot cancel.
        raise HTTPException(status_code=409, detail=message)
    return {"accepted": True, "message": message, **registry.snapshot(round_id)}


@app.get("/api/rounds/{round_id}/result")
def download_result(round_id: str) -> Response:
    try:
        round_obj = registry.get(round_id)
    except RoundNotFound:
        raise HTTPException(status_code=404, detail="轮次不存在或已被清理") from None
    if round_obj.state != SUCCEEDED or round_obj.result_csv is None:
        raise HTTPException(
            status_code=409,
            detail=f"轮次当前状态为 {round_obj.state}，没有可下载的结果；失败/超时不提供部分结果",
        )
    headers = {
        "Content-Disposition": (
            f'attachment; filename="result_{round_id[:8]}.csv"'
        )
    }
    return Response(
        content=round_obj.result_csv,
        media_type="text/csv; charset=utf-8",
        headers=headers,
    )
