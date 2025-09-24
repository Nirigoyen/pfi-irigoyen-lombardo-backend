import os
import math
import json
import uuid
import asyncio
from io import BytesIO
from typing import Any, Dict, List, Tuple
import anyio
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import FastAPI, Form, HTTPException, BackgroundTasks  
from fastapi.responses import JSONResponse
from pypdf import PdfReader, PdfWriter

# APScheduler (async)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

# =======================
# Config desde entorno
# =======================
DIFY_BASE_URL = os.getenv("DIFY_BASE_URL", "http://110.238.68.181/v1").rstrip("/")
DIFY_API_KEY = os.getenv("DIFY_API_KEY")  # puede ser dataset-xxxx o api key de workspace
DATASET_ID = os.getenv("DATASET_ID")      # Knowledge/Dataset ID (estático)

# Ajustes de dataset
DOC_FORM = "hierarchical_model" #os.getenv("DOC_FORM", "hierarchical_model")
INDEXING_TECHNIQUE = "high_quality" #os.getenv("INDEXING_TECHNIQUE", "high_quality")

# Reglas por defecto de procesamiento
PR_PRE_REMOVE_EXTRA_SPACES = "true"
PR_PRE_REMOVE_URLS_EMAILS  = "true"
PR_SEG_SEPARATOR = "\\n\\n"  # doble salto de línea
PR_SEG_MAXTOK    = 1024
PR_PARENT_MODE   = "paragraph"
PR_SUB_SEPARATOR = "\\n"
PR_SUB_MAXTOK    = 1024

# Concurrencia de subidas
UPLOAD_CONCURRENCY = 4
AUTO_UPDATE_METADATA = 0

# IDs de metadata
METADATA_ISBN_ID  = "9287565e-43a3-48fd-ac74-45872ede0176"
METADATA_LEIDO_ID = "a3d9bc30-7696-4210-a934-81b6b375bd6a"

METADATA_DELAY_SECONDS = int(os.getenv("METADATA_DELAY_SECONDS", "60"))
METADATA_MAX_RETRIES   = int(os.getenv("METADATA_MAX_RETRIES", "5"))
METADATA_BACKOFF_BASE  = float(os.getenv("METADATA_BACKOFF_BASE", "2"))
SCHEDULE_METADATA_AFTER_UPLOAD = os.getenv("SCHEDULE_METADATA_AFTER_UPLOAD", "1") == "1"

SCHED_TZ = os.getenv("SCHED_TZ", "UTC")  # UTC por defecto

if not DIFY_API_KEY:
    raise RuntimeError("Falta DIFY_API_KEY en el entorno.")
if not DATASET_ID:
    raise RuntimeError("Falta DATASET_ID en el entorno.")

app = FastAPI(title="OBS PDF → Dify uploader (PDF-only, hierarchical, Postman-exact)")

# Scheduler global
scheduler = AsyncIOScheduler(timezone=SCHED_TZ)


# =======================
# Utilidades (todo en memoria)
# =======================
async def download_pdf_bytes(url: str, timeout_s: float = 120.0) -> bytes:
    headers = {"User-Agent": "obs-dify-uploader/2.2"}
    try:
        limits = httpx.Limits(max_connections=5, max_keepalive_connections=5, keepalive_expiry=30)
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(connect=30, read=180, write=180, pool=180),
            headers=headers,
            limits=limits,
            http2=False,
            trust_env=False
        ) as client:
            r = await client.get(url)
            r.raise_for_status()
            return r.content
    except httpx.HTTPStatusError as e:
        raise HTTPException(e.response.status_code, f"Error descargando PDF: {e.response.text[:400]}") from e
    except Exception as e:
        raise HTTPException(400, f"No se pudo descargar el PDF: {e}") from e


def _split_pdf_bytes_to_parts_sync(src_bytes: bytes, desired_parts: int = 20) -> List[Tuple[Tuple[int, int], bytes]]:
    """
    CPU-bound: partir PDF en memoria. Lo invocamos desde un thread.
    Devuelve [((start,end), pdf_bytes_chunk)].
    """
    reader = PdfReader(BytesIO(src_bytes))
    total_pages = len(reader.pages)
    if total_pages == 0:
        raise ValueError("El PDF no tiene páginas.")

    parts = min(max(1, desired_parts), total_pages)
    pages_per_part = math.ceil(total_pages / parts)

    outputs: List[Tuple[Tuple[int, int], bytes]] = []
    for i in range(parts):
        start = i * pages_per_part
        end = min(start + pages_per_part, total_pages)
        if start >= end:
            continue

        writer = PdfWriter()
        for p in range(start, end):
            writer.add_page(reader.pages[p])
        buf = BytesIO()
        writer.write(buf)
        outputs.append(((start + 1, end), buf.getvalue()))
    return outputs


def build_process_rule_payload() -> Dict[str, Any]:
    """
    'data' EXACTO como en tu Postman (estructura y tipos).
    """
    def _unescape(s: str) -> str:
        return s.encode("utf-8").decode("unicode_escape")

    return {
        "indexing_technique": INDEXING_TECHNIQUE,  # "high_quality"
        "process_rule": {
            "mode": "hierarchical",
            "rules": {
                "pre_processing_rules": [
                    {"id": "remove_extra_spaces", "enabled": PR_PRE_REMOVE_EXTRA_SPACES},
                    {"id": "remove_urls_emails",  "enabled": PR_PRE_REMOVE_URLS_EMAILS},
                ],
                "segmentation": {
                    "separator": _unescape(PR_SEG_SEPARATOR),  # "\n\n"
                    "max_tokens": PR_SEG_MAXTOK,
                },
                "parent_mode": PR_PARENT_MODE,                 # "paragraph"
                "subchunk_segmentation": {
                    "separator": _unescape(PR_SUB_SEPARATOR),  # "\n"
                    "max_tokens": PR_SUB_MAXTOK,
                },
            },
        },
        "doc_form": DOC_FORM,              # "hierarchical_model"    
    }


async def upload_pdf_chunk_to_dify(
    client: httpx.AsyncClient,
    filename_for_name: str,
    file_bytes: bytes,
) -> Dict[str, Any]:
    """
    Multipart EXACTO:
      - 'data': application/json
      - 'file': application/pdf
    """
    url = f"{DIFY_BASE_URL}/datasets/{DATASET_ID}/document/create-by-file"
    data_json = build_process_rule_payload()
    files = {
        "data": (None, json.dumps(data_json), "application/json"),
        "file": (filename_for_name, file_bytes, "application/pdf"),
    }
    r = await client.post(url, files=files)
    if r.status_code >= 400:
        try:
            err = r.json()
        except Exception:
            err = {"raw": r.text}
        raise HTTPException(status_code=r.status_code, detail={"dify_error": err})
    return r.json()


def build_metadata_operation_data(
    isbn: str,
    chunks: List[Tuple[Tuple[int, int], bytes]],
    uploads_json: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Arma el JSON para /datasets/{dataset_id}/documents/metadata:
      - isbn: parámetro recibido
      - leido: % acumulado al FIN de cada parte
    """
    total_pages = chunks[-1][0][1] if chunks else 0
    op_data: List[Dict[str, Any]] = []
    for i, up in enumerate(uploads_json, start=1):
        if isinstance(up, Exception):
            continue
        doc = up.get("document", {})
        doc_id = doc.get("id")
        if not doc_id:
            continue
        start_page, end_page = chunks[i - 1][0]
        leido_pct = 0
        if total_pages > 0:
            leido_pct = round(end_page / total_pages * 100)
            leido_pct = min(leido_pct, 100)

        md_list: List[Dict[str, Any]] = []
        md_list.append({
            **({"id": METADATA_ISBN_ID} if METADATA_ISBN_ID else {}),
            "name": "isbn",
            "value": str(isbn),
        })
        md_list.append({
            **({"id": METADATA_LEIDO_ID} if METADATA_LEIDO_ID else {}),
            "name": "leido",
            "value": str(leido_pct),
        })
        op_data.append({"document_id": doc_id, "metadata_list": md_list})
    return {"operation_data": op_data}


async def post_metadata_batch(operation_data: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{DIFY_BASE_URL}/datasets/{DATASET_ID}/documents/metadata"
    headers = {
        "Authorization": f"Bearer {DIFY_API_KEY}",
        "Content-Type": "application/json",
    }
    limits = httpx.Limits(max_connections=5, max_keepalive_connections=5, keepalive_expiry=30)
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(connect=30, read=180, write=180, pool=180),
        headers=headers,
        limits=limits,
        http2=False,
        trust_env=False
    ) as client:
        r = await client.post(url, json=operation_data)
        r.raise_for_status()
        return r.json()


# =======================
# Scheduler: job
# =======================
async def metadata_job(operation_data: Dict[str, Any]) -> None:
    """
    Job programado: envía metadata con backoff.
    """
    delay = 1.0
    last_err = None
    for attempt in range(1, METADATA_MAX_RETRIES + 1):
        try:
            resp = await post_metadata_batch(operation_data)
            print(f"[scheduler][metadata] OK intento {attempt} -> {len(operation_data.get('operation_data', []))} items")
            # Podrías loguear resp si querés
            return
        except Exception as e:
            last_err = str(e)
            print(f"[scheduler][metadata] Error intento {attempt}: {last_err}")
            await asyncio.sleep(delay)
            delay *= METADATA_BACKOFF_BASE
    print(f"[scheduler][metadata] Falló tras {METADATA_MAX_RETRIES} intentos. Último error: {last_err}")


def enqueue_metadata_job(operation_data: Dict[str, Any], delay_seconds: int) -> str:
    """
    Programa el job para dentro de 'delay_seconds'. Devuelve job_id.
    """
    # Usamos UTC para evitar problemas de TZ
    run_dt = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
    trigger = DateTrigger(run_date=run_dt)
    job = scheduler.add_job(metadata_job, trigger=trigger, args=[operation_data])
    print(f"[scheduler] Job programado {job.id} para {run_dt.isoformat()}")
    return job.id


# =======================
# Endpoints
# =======================
@app.post("/ingest/from_url")
async def ingest_from_url(
    pdf_url: str = Form(..., description="URL pública/estática del PDF en OBS"),
    isbn: str = Form(..., description="ISBN que nombra los subdocumentos: ISBN-1, ISBN-2, ..."),
    parts: int = Form(20, description="Cantidad aproximada de partes"),
    schedule_metadata_flag: int = Form(1, description="1: programa metadata; 0: no programa"),
):
    """
    Descarga PDF (OBS) → parte en ~N sub-PDFs (en thread) → sube cada chunk a Dify.
    Al finalizar, arma metadata y, si se indica, programa un job para enviarla en diferido.
    """
    if not isbn.strip():
        raise HTTPException(400, "El campo 'isbn' es obligatorio.")
    if not pdf_url.lower().startswith(("http://", "https://")):
        raise HTTPException(400, "pdf_url debe ser http(s).")

    # 1) Descargar PDF a memoria
    src_bytes = await download_pdf_bytes(pdf_url)

    # 2) Partir en un thread (no bloquea el loop)
    try:
        pdf_chunks: List[Tuple[Tuple[int, int], bytes]] = await anyio.to_thread.run_sync(
            _split_pdf_bytes_to_parts_sync, src_bytes, parts
        )
    except Exception as e:
        raise HTTPException(400, f"No se pudo procesar el PDF: {e}")

    # 3) Subir concurrentemente
    headers = {"Authorization": f"Bearer {DIFY_API_KEY}"}
    limits = httpx.Limits(max_connections=max(UPLOAD_CONCURRENCY, 2), max_keepalive_connections=2, keepalive_expiry=30)
    sem = asyncio.Semaphore(UPLOAD_CONCURRENCY)

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(connect=30, read=180, write=180, pool=180),
        headers=headers,
        limits=limits,
        http2=False,
        trust_env=False
    ) as client:

        async def _task_pdf(idx: int, blob: bytes) -> Dict[str, Any]:
            async with sem:
                fname = f"{isbn}-{idx}.pdf"
                return await upload_pdf_chunk_to_dify(client, filename_for_name=fname, file_bytes=blob)

        tasks = [asyncio.create_task(_task_pdf(i + 1, blob)) for i, ((_, _), blob) in enumerate(pdf_chunks)]
        uploads = await asyncio.gather(*tasks, return_exceptions=True)

    # 4) Resumen por parte
    results = []
    for i, up in enumerate(uploads, start=1):
        start_page, end_page = pdf_chunks[i - 1][0]
        if isinstance(up, Exception):
            results.append({
                "part": i, "name": f"{isbn}-{i}",
                "pages": {"from": start_page, "to": end_page},
                "status": "error", "error": str(up),
            })
        else:
            doc = up.get("document", {})
            results.append({
                "part": i, "name": f"{isbn}-{i}",
                "pages": {"from": start_page, "to": end_page},
                "status": "ok",
                "document_id": doc.get("id"),
                "indexing_status": doc.get("indexing_status"),
                "doc_form": doc.get("doc_form"),
            })

    # 5) Payload de metadata
    operation_data = build_metadata_operation_data(isbn, pdf_chunks, uploads)

    # 6) Programar metadata si aplica
    job_info = None
    if SCHEDULE_METADATA_AFTER_UPLOAD and schedule_metadata_flag == 1 and operation_data["operation_data"]:
        job_id = enqueue_metadata_job(operation_data, METADATA_DELAY_SECONDS)
        job_info = {
            "job_id": job_id,
            "delay_seconds": METADATA_DELAY_SECONDS,
            "items": len(operation_data["operation_data"]),
        }

    return JSONResponse({
        "dataset_id": DATASET_ID,
        "isbn": isbn,
        "uploaded": results,
        "metadata_operation_data": operation_data,  # por si querés dispararlo manualmente
        "metadata_job": job_info,                   # info del job si se programó
    })


@app.post("/metadata/batch")
async def update_metadata_batch(operation_data: Dict[str, Any]):
    """
    Envío manual (alternativo): reenvía la metadata a Dify ahora.
    """
    if "operation_data" not in operation_data or not isinstance(operation_data["operation_data"], list):
        raise HTTPException(400, "El cuerpo debe incluir 'operation_data' (lista).")
    try:
        out = await post_metadata_batch(operation_data)
        return JSONResponse(out)
    except httpx.HTTPStatusError as e:
        raise HTTPException(e.response.status_code, f"Dify error: {e.response.text[:400]}") from e


@app.get("/healthz")
def healthz():
    return {"ok": True}


# =======================
# Lifecycle: iniciar / parar scheduler
# =======================
@app.on_event("startup")
async def on_startup():
    if not scheduler.running:
        scheduler.start()
        print("[scheduler] started")


@app.on_event("shutdown")
async def on_shutdown():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        print("[scheduler] stopped")