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
import threading
import traceback
import pika
import ssl

# APScheduler (async)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

# =======================
# Config desde entorno
# =======================
DIFY_BASE_URL = os.getenv("DIFY_BASE_URL", "http://110.238.68.181/v1").rstrip("/")
DIFY_API_KEY = os.getenv("DIFY_API_KEY")  # puede ser dataset-xxxx o api key de workspace
DATASET_ID = os.getenv("DATASET_ID")      # Knowledge/Dataset ID (estático)

# ==== RabbitMQ ====
RABBIT_HOST = os.getenv("RABBIT_HOST", "")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))   # 5671 si TLS
RABBIT_USER = os.getenv("RABBIT_USER", "")
RABBIT_PASS = os.getenv("RABBIT_PASS", "")
RABBIT_VHOST = os.getenv("RABBIT_VHOST", "/")
RABBIT_USE_TLS = os.getenv("RABBIT_USE_TLS", "false").lower() in ("1","true","yes")

RABBIT_EXCHANGE = os.getenv("RABBIT_EXCHANGE", "gen.topic")
RABBIT_QUEUE_EMBEDDINGS = os.getenv("RABBIT_QUEUE_EMBEDDINGS", "gen_embeddings")
RABBIT_RK_EMBEDDINGS = os.getenv("RABBIT_RK_EMBEDDINGS", "gen.embeddings")
RABBIT_PREFETCH = int(os.getenv("RABBIT_PREFETCH", "4"))

RABBIT_QUEUE_PROMPTS = os.getenv("RABBIT_QUEUE_PROMPTS", "gen_prompts")
RABBIT_RK_PROMPTS_PERSONAJE = os.getenv("RABBIT_RK_PROMPTS_PERSONAJE", "gen.prompts.personaje")
RABBIT_RK_PROMPTS_LUGAR     = os.getenv("RABBIT_RK_PROMPTS_LUGAR", "gen.prompts.lugar")


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
async def metadata_job(operation_data: Dict[str, Any], followup: Dict[str, Any] | None = None) -> None:
    """
    Job programado: envía metadata con backoff.
    Si 'followup' viene con {'isbn', 'personajes', 'lugares'}, al terminar publica los prompts 1-a-1.
    """
    delay = 1.0
    last_err = None
    for attempt in range(1, METADATA_MAX_RETRIES + 1):
        try:
            resp = await post_metadata_batch(operation_data)
            print(f"[scheduler][metadata] OK intento {attempt} -> {len(operation_data.get('operation_data', []))} items")

            # Follow-up: publicar prompts
            if followup and followup.get("isbn"):
                await anyio.to_thread.run_sync(lambda: publish_prompts_for_entities(
                    isbn=followup["isbn"],
                    autor=followup.get("autor"),
                    titulo=followup.get("titulo"),
                    genero=followup.get("genero"),
                    personajes=followup.get("personajes") or [],
                    lugares=followup.get("lugares") or [],
        ))
            return
        except Exception as e:
            last_err = str(e)
            print(f"[scheduler][metadata] Error intento {attempt}: {last_err}")
            await asyncio.sleep(delay)
            delay *= METADATA_BACKOFF_BASE
    print(f"[scheduler][metadata] Falló tras {METADATA_MAX_RETRIES} intentos. Último error: {last_err}")

def enqueue_metadata_job(operation_data: Dict[str, Any], delay_seconds: int, followup: Dict[str, Any] | None = None) -> str:
    """
    Programa el job para dentro de 'delay_seconds'. Devuelve job_id.
    Cuando el job se ejecute y la metadata se envíe OK, publicará eventos de prompts.
    """
    run_dt = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
    trigger = DateTrigger(run_date=run_dt)
    job = scheduler.add_job(metadata_job, trigger=trigger, args=[operation_data, followup])
    print(f"[scheduler] Job programado {job.id} para {run_dt.isoformat()}")
    return job.id

async def process_embeddings_event_async(
    pdf_url: str,
    isbn: str,
    personajes: List[Any],
    lugares: List[Any],
    author: str | None,         
    genre: str | None,          
    title: str | None = None,   
    parts: int = 20,
) -> Dict[str, Any]:
    # 1) Descargar PDF
    src_bytes = await download_pdf_bytes(pdf_url)

    # 2) Partir en thread
    pdf_chunks: List[Tuple[Tuple[int, int], bytes]] = await anyio.to_thread.run_sync(
        _split_pdf_bytes_to_parts_sync, src_bytes, parts
    )

    # 3) Subir concurrente a Dify
    headers = {"Authorization": f"Bearer {DIFY_API_KEY}"}
    limits = httpx.Limits(max_connections=max(max(UPLOAD_CONCURRENCY, 2), len(pdf_chunks)),
                          max_keepalive_connections=2, keepalive_expiry=30)
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

    # 4) Resumen
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

    # 5) Metadata
    operation_data = build_metadata_operation_data(isbn, pdf_chunks, uploads)

    followup = {
        "isbn": isbn,
        "autor": author,     
        "genero": genre,     
        "titulo": title,     
        "personajes": personajes,
        "lugares": lugares,
    }

    job_info = None
    if operation_data.get("operation_data"):
        if SCHEDULE_METADATA_AFTER_UPLOAD:
            job_id = enqueue_metadata_job(operation_data, METADATA_DELAY_SECONDS, followup=followup)
            job_info = {"job_id": job_id, "delay_seconds": METADATA_DELAY_SECONDS,
                        "items": len(operation_data["operation_data"])}
        else:
            await metadata_job(operation_data, followup=followup)

    return {
        "dataset_id": DATASET_ID,
        "isbn": isbn,
        "uploaded": results,
        "metadata_operation_data": operation_data,
        "metadata_job": job_info,
    }

# ========= RabbitMQ consumer: gen_embeddings =========

def _mk_rabbit_params() -> Optional[pika.ConnectionParameters]:
    if not (RABBIT_HOST and RABBIT_USER and RABBIT_PASS):
        print("[rabbit] faltan variables de entorno para conectar, listener deshabilitado.")
        return None
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    if RABBIT_USE_TLS:
        ctx = ssl.create_default_context()
        # Para pruebas con IP privada y cert CN distinto, descomentar bajo tu responsabilidad:
        # ctx.check_hostname = False
        # ctx.verify_mode = ssl.CERT_NONE
        return pika.ConnectionParameters(
            host=RABBIT_HOST, port=RABBIT_PORT, virtual_host=RABBIT_VHOST,
            credentials=creds,
            ssl_options=pika.SSLOptions(ctx, server_hostname=RABBIT_HOST),
            heartbeat=30, blocked_connection_timeout=60
        )
    else:
        return pika.ConnectionParameters(
            host=RABBIT_HOST, port=RABBIT_PORT, virtual_host=RABBIT_VHOST,
            credentials=creds, heartbeat=30, blocked_connection_timeout=60
        )

def start_rabbit_listener(loop: asyncio.AbstractEventLoop):
    
    params = _mk_rabbit_params()
    if params is None:
        return

    def _run():
        while True:
            try:
                conn = pika.BlockingConnection(params)
                ch = conn.channel()

                # Asegurar topología mínima (por si no existe)
                ch.exchange_declare(RABBIT_EXCHANGE, exchange_type="topic", durable=True)
                ch.queue_declare(RABBIT_QUEUE_EMBEDDINGS, durable=True, exclusive=False, auto_delete=False)
                ch.queue_bind(RABBIT_QUEUE_EMBEDDINGS, RABBIT_EXCHANGE, RABBIT_RK_EMBEDDINGS)
                ch.basic_qos(prefetch_count=RABBIT_PREFETCH)

                print(f"[rabbit] Escuchando {RABBIT_QUEUE_EMBEDDINGS} (rk={RABBIT_RK_EMBEDDINGS})")

                def _on_msg(chx, method, props, body: bytes):
                    try:
                        payload = json.loads(body.decode("utf-8"))
                        pdf_url    = payload.get("url_libro")
                        isbn       = payload.get("isbn")
                        personajes = payload.get("personajes") or []
                        lugares    = payload.get("lugares") or []
                        author     = payload.get("autor") or payload.get("author")   
                        genre      = payload.get("genero") or payload.get("genre")   
                        title      = payload.get("titulo") or payload.get("title")   

                        if not (isinstance(isbn, str) and isbn.strip()):
                            raise ValueError("isbn faltante/ inválido")
                        if not (isinstance(pdf_url, str) and pdf_url.lower().startswith(("http://", "https://"))):
                            raise ValueError("url_libro faltante/ inválida")

                        fut = asyncio.run_coroutine_threadsafe(
                            process_embeddings_event_async(
                                pdf_url=pdf_url,
                                isbn=isbn,
                                personajes=personajes,
                                lugares=lugares,
                                author=author,
                                genre=genre,
                                title=title,
                                parts=20
                            ),
                            loop
                        )
                        chx.basic_ack(delivery_tag=method.delivery_tag)
                    except Exception as e:
                        print("[rabbit] Error al procesar mensaje:", e)
                        traceback.print_exc()
                        chx.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

                ch.basic_consume(queue=RABBIT_QUEUE_EMBEDDINGS, on_message_callback=_on_msg, auto_ack=False)
                ch.start_consuming()
            except Exception as e:
                print(f"[rabbit] conexión/loop caído: {e}. Reintentando en 5s...")
                try:
                    conn.close()
                except Exception:
                    pass
                import time as _t
                _t.sleep(5)

    th = threading.Thread(target=_run, name="rabbit-listener", daemon=True)
    th.start()
    print("[rabbit] listener thread iniciado.")

def _ensure_prompts_topology(ch: pika.channel.Channel) -> None:
    ch.exchange_declare(RABBIT_EXCHANGE, exchange_type="topic", durable=True)
    ch.queue_declare(RABBIT_QUEUE_PROMPTS, durable=True, exclusive=False, auto_delete=False)
    ch.queue_bind(RABBIT_QUEUE_PROMPTS, RABBIT_EXCHANGE, "gen.prompts.*")

def _normalize_entities(lst: List[Any]) -> List[dict]:
    # Acepta [{'id':..,'nombre':..}] o ['Ana','Bruno'] → [{id:1,nombre:'Ana'},...]
    out = []
    for i, it in enumerate(lst or []):
        if isinstance(it, dict) and "nombre" in it:
            _id = it.get("id", i+1)
            out.append({"id": int(_id), "nombre": str(it["nombre"]).strip()})
        else:
            out.append({"id": i+1, "nombre": str(it)})
    return out

def publish_prompts_for_entities (isbn: str,
    autor: str | None,
    titulo: str | None,
    genero: str | None,
    personajes: List[Any],
    lugares: List[Any],
) -> dict:
    res = {"ok": False, "count_personajes": 0, "count_lugares": 0, "reason": None}
    params = _mk_rabbit_params()
    if params is None:
        res["reason"] = "rabbit_not_configured"
        return res
    conn = None
    try:
        conn = pika.BlockingConnection(params)
        ch = conn.channel()
        _ensure_prompts_topology(ch)

        props = pika.BasicProperties(content_type="application/json", delivery_mode=2)

        pj = _normalize_entities(personajes)
        lg = _normalize_entities(lugares)

        for e in pj:
            payload = {"isbn": isbn, "tipo": "personaje", "id": int(e["id"]), "nombre": e["nombre"]}
            ch.basic_publish(RABBIT_EXCHANGE, RABBIT_RK_PROMPTS_PERSONAJE, json.dumps(payload).encode("utf-8"), properties=props)
        for e in lg:
            payload = {"isbn": isbn, "tipo": "lugar", "id": int(e["id"]), "nombre": e["nombre"]}
            ch.basic_publish(RABBIT_EXCHANGE, RABBIT_RK_PROMPTS_LUGAR, json.dumps(payload).encode("utf-8"), properties=props)

        res["ok"] = True
        res["count_personajes"] = len(pj)
        res["count_lugares"] = len(lg)
        return res
    except Exception as e:
        res["reason"] = f"{type(e).__name__}: {e}"
        return res
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass


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
    loop = asyncio.get_event_loop()
    start_rabbit_listener(loop)


@app.on_event("shutdown")
async def on_shutdown():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        print("[scheduler] stopped")