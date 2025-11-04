import os
import json
import ssl
import time
import threading
import traceback
import urllib.parse
from typing import Any, Dict, Optional, Tuple

import requests
import pika
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from obs import ObsClient

# =========================
# FastAPI
# =========================
app = FastAPI(title="Lugares → Dify → BlockadeLabs → OBS (Scenes)")

# =========================
# Entorno / Config
# =========================
# Dify
DIFY_BASE_URL   = os.getenv("DIFY_BASE_URL", "http://110.238.68.181/v1").rstrip("/")
DIFY_API_KEY    = os.getenv("DIFY_API_KEY", "")  # REQUIRED
WORKFLOW_USER   = os.getenv("WORKFLOW_USER", "abc-123")
DIFY_RESPONSE_MODE = os.getenv("DIFY_RESPONSE_MODE", "blocking")  # usar "blocking" para recibir el prompt directo
DIFY_HTTP_TIMEOUT = float(os.getenv("DIFY_HTTP_TIMEOUT", "90"))

# RabbitMQ
RABBIT_HOST     = os.getenv("RABBIT_HOST", "127.0.0.1")
RABBIT_PORT     = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER     = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS     = os.getenv("RABBIT_PASS", "guest")
RABBIT_VHOST    = os.getenv("RABBIT_VHOST", "/")
RABBIT_USE_TLS  = os.getenv("RABBIT_USE_TLS", "false").lower() in ("1","true","yes")

EXCHANGE_NAME   = os.getenv("RABBIT_EXCHANGE", "gen.topic")
QUEUE_PROMPTS   = os.getenv("RABBIT_QUEUE_PROMPTS", "gen_prompts")
RK_LUGAR        = os.getenv("RABBIT_RK_PROMPTS_LUGAR", "gen.prompts.lugar")
PREFETCH        = int(os.getenv("RABBIT_PREFETCH", "3"))

# BlockadeLabs Skybox
BLOCKADE_BASE           = os.getenv("BLOCKADE_BASE", "https://backend.blockadelabs.com/api/v1").rstrip("/")
BLOCKADE_API_KEY        = os.getenv("BLOCKADE_API_KEY", "")  # REQUIRED (x-api-key)
SKYBOX_STYLE_ID         = int(os.getenv("SKYBOX_STYLE_ID", "102"))  # 102 como en tu ejemplo
SKYBOX_NEGATIVE_TEXT    = os.getenv("SKYBOX_NEGATIVE_TEXT",
    "No people/animals; no blood or body; no modern signs, cars, neon, power lines; "
    "no text/logos; no smoke/fire; no sci-fi/steampunk props."
)
SKYBOX_SEED             = int(os.getenv("SKYBOX_SEED", "42"))
SKYBOX_EXPORT_TYPE_ID   = int(os.getenv("SKYBOX_EXPORT_TYPE_ID", "1"))
SKYBOX_WAIT_SECONDS     = int(os.getenv("SKYBOX_WAIT_SECONDS", "20"))
BLOCKADE_HTTP_TIMEOUT   = float(os.getenv("BLOCKADE_HTTP_TIMEOUT", "120"))

# OBS Huawei
OBS_ENDPOINT = os.getenv("OBS_ENDPOINT")  # p.ej. https://obs.la-south-2.myhuaweicloud.com
OBS_AK = os.getenv("OBS_AK")
OBS_SK = os.getenv("OBS_SK")
OBS_BUCKET = os.getenv("OBS_BUCKET", "livrario-books")

# Retries
RETRY_MAX       = int(os.getenv("RETRY_MAX", "3"))
RETRY_BACKOFF   = float(os.getenv("RETRY_BACKOFF", "2.0"))

# =========================
# Rabbit helpers
# =========================
def _mk_rabbit_params() -> pika.ConnectionParameters:
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    if RABBIT_USE_TLS:
        ctx = ssl.create_default_context()
        return pika.ConnectionParameters(
            host=RABBIT_HOST, port=RABBIT_PORT, virtual_host=RABBIT_VHOST,
            credentials=creds, heartbeat=30, blocked_connection_timeout=60,
            ssl_options=pika.SSLOptions(ctx, server_hostname=RABBIT_HOST)
        )
    return pika.ConnectionParameters(
        host=RABBIT_HOST, port=RABBIT_PORT, virtual_host=RABBIT_VHOST,
        credentials=creds, heartbeat=30, blocked_connection_timeout=60
    )

def _bootstrap_topology(ch: pika.adapters.blocking_connection.BlockingChannel) -> None:
    ch.exchange_declare(EXCHANGE_NAME, exchange_type="topic", durable=True)
    ch.queue_declare(QUEUE_PROMPTS, durable=True, exclusive=False, auto_delete=False)
    ch.queue_bind(queue=QUEUE_PROMPTS, exchange=EXCHANGE_NAME, routing_key=RK_LUGAR)
    ch.basic_qos(prefetch_count=PREFETCH)

# =========================
# Validación / mapping
# =========================
def _validate_lugar_msg(msg: Dict[str, Any]) -> Optional[str]:
    # Requeridos para el pipeline
    # { isbn, nombre, autor, titulo, [id (numero de escena)] }
    for k in ("isbn", "nombre", "autor", "titulo"):
        if not msg.get(k):
            return f"Falta '{k}'"
    if msg.get("tipo") and msg["tipo"] != "lugar":
        return "tipo distinto de 'lugar'"
    return None

def _scene_number_from_msg(msg: Dict[str, Any]) -> int:
    try:
        val = int(msg.get("id", 1))
        return max(1, val)
    except Exception:
        return 1

def _dify_inputs_from_lugar(msg: Dict[str, Any]) -> Dict[str, Any]:
    # Ajustá las keys a lo que tu workflow Dify espere para generar el prompt de escena
    return {
        "isbn": msg["isbn"],
        "autor": msg["autor"],
        "titulo": msg["titulo"],
        "lugar_nombre": msg["nombre"],
        "lugar_id": msg.get("id"),
    }

# =========================
# Dify
# =========================
def _call_dify_workflow(inputs: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    if not DIFY_API_KEY:
        return False, None, "Missing DIFY_API_KEY"
    url = f"{DIFY_BASE_URL}/workflows/run"
    headers = {
        "Authorization": f"Bearer {DIFY_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "inputs": inputs,
        "response_mode": DIFY_RESPONSE_MODE,  # "blocking"
        "user": WORKFLOW_USER,
    }

    backoff = 1.0
    last_err = ""
    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=DIFY_HTTP_TIMEOUT)
            if 200 <= r.status_code < 300:
                try:
                    return True, r.json(), "ok"
                except Exception:
                    return False, None, "Dify body no-JSON"
            last_err = f"HTTP {r.status_code}: {r.text[:600]}"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
        if attempt < RETRY_MAX:
            time.sleep(backoff)
            backoff *= RETRY_BACKOFF
    return False, None, last_err

def _extract_prompt_from_dify(resp_json: Dict[str, Any]) -> Optional[str]:
    # Intenta varias rutas típicas
    try_paths = [
        ["data", "outputs", "prompt"],
        ["data", "outputs", "result"],
        ["data", "text"],
        ["result"],
        ["outputs", "prompt"],
    ]
    for path in try_paths:
        cur = resp_json
        ok = True
        for k in path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                ok = False
                break
        if ok and isinstance(cur, str) and cur.strip():
            return cur.strip()
    return None

# =========================
# BlockadeLabs Skybox
# =========================
def _blockade_create_skybox(prompt: str) -> Tuple[bool, Optional[str], str]:
    if not BLOCKADE_API_KEY:
        return False, None, "Missing BLOCKADE_API_KEY"
    url = f"{BLOCKADE_BASE}/skybox"
    headers = {
        "x-api-key": BLOCKADE_API_KEY,
        "Content-Type": "application/json",
    }
    body = {
        "skybox_style_id": SKYBOX_STYLE_ID,
        "prompt": prompt,
        "negative_text": SKYBOX_NEGATIVE_TEXT,
        "enhance_prompt": True,
        "seed": SKYBOX_SEED,
    }
    backoff = 1.0
    last_err = ""
    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = requests.post(url, headers=headers, json=body, timeout=BLOCKADE_HTTP_TIMEOUT)
            if 200 <= r.status_code < 300:
                try:
                    data = r.json()
                except Exception:
                    return False, None, "Skybox create: body no-JSON"
                # buscar skybox_id
                skybox_id = data.get("id") or data.get("skybox") or data.get("skybox_id")
                if isinstance(skybox_id, dict):
                    skybox_id = skybox_id.get("id")
                if not skybox_id:
                    # intenta paths comunes
                    skybox_id = (data.get("data") or {}).get("id")
                if skybox_id:
                    return True, str(skybox_id), "ok"
                return False, None, f"Skybox create OK sin id. Resp: {str(data)[:500]}"
            last_err = f"HTTP {r.status_code}: {r.text[:600]}"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
        if attempt < RETRY_MAX:
            time.sleep(backoff)
            backoff *= RETRY_BACKOFF
    return False, None, last_err

def _blockade_export_skybox(skybox_id: str) -> Tuple[bool, Optional[str], str]:
    if not BLOCKADE_API_KEY:
        return False, None, "Missing BLOCKADE_API_KEY"
    url = f"{BLOCKADE_BASE}/skybox/export"
    headers = {
        "x-api-key": BLOCKADE_API_KEY,
        "Content-Type": "application/json",
    }
    body = {
        "skybox_id": skybox_id,
        "type_id": SKYBOX_EXPORT_TYPE_ID
    }
    # Espera solicitada por el usuario
    time.sleep(max(0, SKYBOX_WAIT_SECONDS))

    backoff = 1.0
    last_err = ""
    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = requests.post(url, headers=headers, json=body, timeout=BLOCKADE_HTTP_TIMEOUT)
            if 200 <= r.status_code < 300:
                try:
                    data = r.json()
                except Exception:
                    return False, None, "Skybox export: body no-JSON"
                # buscar URL
                # Blockade suele devolver 'file_url' o algo equivalente
                file_url = data.get("file_url") or data.get("url") or (data.get("data") or {}).get("file_url")
                if file_url:
                    return True, str(file_url), "ok"
                return False, None, f"Skybox export OK sin file_url. Resp: {str(data)[:500]}"
            last_err = f"HTTP {r.status_code}: {r.text[:600]}"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
        if attempt < RETRY_MAX:
            time.sleep(backoff)
            backoff *= RETRY_BACKOFF
    return False, None, last_err

def _download_file(url: str, timeout_s: float = 180.0) -> Tuple[bool, Optional[bytes], str]:
    try:
        r = requests.get(url, timeout=timeout_s)
        if r.status_code == 200:
            return True, r.content, "ok"
        return False, None, f"HTTP {r.status_code}: {r.text[:400]}"
    except Exception as e:
        return False, None, f"{type(e).__name__}: {e}"

# =========================
# OBS
# =========================
def _upload_obs_scene(bytes_img: bytes, isbn: str, scene_num: int) -> Tuple[bool, str, str]:
    """
    Sube a: scenes/<ISBN>/sceneX.jpg
    """
    if not (OBS_ENDPOINT and OBS_AK and OBS_SK and OBS_BUCKET):
        return False, "", "OBS config incompleta (endpoint/AK/SK/bucket)"

    object_key = f"scenes/{isbn}/scene{scene_num}.jpg"
    client = ObsClient(access_key_id=OBS_AK, secret_access_key=OBS_SK, server=OBS_ENDPOINT)
    try:
        resp = client.putContent(
            bucketName=OBS_BUCKET,
            objectKey=object_key,
            content=bytes_img,
            contentType="image/jpeg"
        )
        status = getattr(resp, "status", None) if hasattr(resp, "status") else resp.get("status", None)  # type: ignore
        if status == 200:
            return True, object_key, "ok"
        msg = getattr(resp, "errorMessage", "") if hasattr(resp, "errorMessage") else ""
        return False, object_key, f"OBS status {status} {msg}"
    except Exception as e:
        return False, object_key, f"{type(e).__name__}: {e}"
    finally:
        try:
            client.close()
        except Exception:
            pass

# =========================
# Pipeline por mensaje (Lugar)
# =========================
def _process_message_lugar(msg: Dict[str, Any]) -> Tuple[bool, str]:
    err = _validate_lugar_msg(msg)
    if err:
        return False, err

    isbn = str(msg["isbn"])
    scene_num = _scene_number_from_msg(msg)

    # 1) Dify -> prompt
    dify_inputs = _dify_inputs_from_lugar(msg)
    ok_dify, dify_json, info_dify = _call_dify_workflow(dify_inputs)
    if not ok_dify or not dify_json:
        return False, f"Dify error: {info_dify}"

    prompt = _extract_prompt_from_dify(dify_json)
    if not prompt:
        snippet = json.dumps(dify_json)[:800]
        return False, f"No se encontró 'prompt' en respuesta Dify. Resp: {snippet}"

    # 2) Blockade create -> id
    ok_create, skybox_id, info_create = _blockade_create_skybox(prompt)
    if not ok_create or not skybox_id:
        return False, f"Blockade create error: {info_create}"

    # 3) Blockade export -> url fichero
    ok_export, file_url, info_export = _blockade_export_skybox(skybox_id)
    if not ok_export or not file_url:
        return False, f"Blockade export error: {info_export}"

    # 4) Descargar imagen
    ok_dl, img_bytes, info_dl = _download_file(file_url)
    if not ok_dl or not img_bytes:
        return False, f"Descarga imagen error: {info_dl}"

    # 5) Subir a OBS
    ok_up, obj_key, info_up = _upload_obs_scene(img_bytes, isbn, scene_num)
    if not ok_up:
        return False, f"OBS error ({obj_key}): {info_up}"

    public_url = f"https://{OBS_BUCKET}.obs.la-south-2.myhuaweicloud.com/{urllib.parse.quote(obj_key)}"
    return True, public_url

# =========================
# Consumer loop
# =========================
def _consumer_thread():
    params = _mk_rabbit_params()
    while True:
        conn = None
        try:
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            _bootstrap_topology(ch)

            print(f"[miniapi-scenes] Escuchando cola='{QUEUE_PROMPTS}' rk='{RK_LUGAR}' → Dify→Blockade→OBS")

            def _on_msg(chx, method, props, body: bytes):
                try:
                    rk = method.routing_key or ""
                    if rk != RK_LUGAR:
                        chx.basic_ack(delivery_tag=method.delivery_tag)
                        return
                    payload = json.loads(body.decode("utf-8"))
                    ok, info = _process_message_lugar(payload)
                    if ok:
                        print(f"[miniapi-scenes] OK lugar='{payload.get('nombre')}' isbn={payload.get('isbn')} → {info}")
                        chx.basic_ack(delivery_tag=method.delivery_tag)
                    else:
                        print(f"[miniapi-scenes] ERROR lugar='{payload.get('nombre')}' isbn={payload.get('isbn')} → {info}")
                        chx.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                except Exception as e:
                    print("[miniapi-scenes] Excepción procesando mensaje:", e)
                    traceback.print_exc()
                    chx.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            ch.basic_consume(queue=QUEUE_PROMPTS, on_message_callback=_on_msg, auto_ack=False)
            ch.start_consuming()
        except Exception as e:
            print(f"[miniapi-scenes] Conexión/loop caído: {e}. Reintento en 5s…")
            try:
                if conn: conn.close()
            except Exception:
                pass
            time.sleep(5)

# =========================
# Endpoints
# =========================
@app.get("/health")
def health():
    return {
        "ok": True,
        "rabbit": {"host": RABBIT_HOST, "queue": QUEUE_PROMPTS, "rk": RK_LUGAR},
        "dify": DIFY_BASE_URL,
        "blockade": BLOCKADE_BASE,
        "obs": {"bucket": OBS_BUCKET, "endpoint": OBS_ENDPOINT},
        "wait_seconds": SKYBOX_WAIT_SECONDS,
        "response_mode": DIFY_RESPONSE_MODE,
    }

@app.post("/test/pipeline")
def test_pipeline(body: Dict[str, Any]):
    ok, info = _process_message_lugar(body)
    return JSONResponse({"ok": ok, "info": info}, status_code=200 if ok else 502)

# =========================
# Startup
# =========================
@app.on_event("startup")
def on_startup():
    th = threading.Thread(target=_consumer_thread, name="prompts-lugar-listener", daemon=True)
    th.start()
    print("[miniapi-scenes] listener iniciado")
