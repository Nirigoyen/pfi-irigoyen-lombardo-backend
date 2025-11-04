
import os
import json
import ssl
import time
import urllib.parse
import threading
import traceback
from typing import Any, Dict, Optional, Tuple

import requests
import pika
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from obs import ObsClient

# =========================
# FastAPI
# =========================
app = FastAPI(title="Prompts → Dify → Pollinations → OBS (Personajes)")

# =========================
# Configuración por entorno
# =========================
# Dify
DIFY_BASE_URL   = os.getenv("DIFY_BASE_URL", "http://110.238.68.181/v1").rstrip("/")
DIFY_API_KEY    = os.getenv("DIFY_API_KEY", "")  # REQUIRED
WORKFLOW_USER   = os.getenv("WORKFLOW_USER", "abc-123")
RESPONSE_MODE   = os.getenv("RESPONSE_MODE", "blocking")  # "blocking" recomendado para obtener el prompt directo
HTTP_TIMEOUT_S  = float(os.getenv("HTTP_TIMEOUT_S", "90"))

# RabbitMQ
RABBIT_HOST     = os.getenv("RABBIT_HOST", "127.0.0.1")
RABBIT_PORT     = int(os.getenv("RABBIT_PORT", "5672"))     # 5671 si TLS
RABBIT_USER     = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS     = os.getenv("RABBIT_PASS", "guest")
RABBIT_VHOST    = os.getenv("RABBIT_VHOST", "/")
RABBIT_USE_TLS  = os.getenv("RABBIT_USE_TLS", "false").lower() in ("1","true","yes")

EXCHANGE_NAME   = os.getenv("RABBIT_EXCHANGE", "gen.topic")
QUEUE_PROMPTS   = os.getenv("RABBIT_QUEUE_PROMPTS", "gen_prompts")
RK_PERSONAJE    = os.getenv("RABBIT_RK_PROMPTS_PERSONAJE", "gen.prompts.personaje")
PREFETCH        = int(os.getenv("RABBIT_PREFETCH", "3"))

# Pollinations
POLLINATIONS_BASE   = os.getenv("POLLINATIONS_BASE", "https://image.pollinations.ai/prompt").rstrip("/")
POLLINATIONS_WIDTH  = int(os.getenv("POLLINATIONS_WIDTH", "1024"))
POLLINATIONS_HEIGHT = int(os.getenv("POLLINATIONS_HEIGHT", "1024"))
POLLINATIONS_SEED   = int(os.getenv("POLLINATIONS_SEED", "1024"))
POLLINATIONS_MODEL  = os.getenv("POLLINATIONS_MODEL", "flux")
POLLINATIONS_NOLOGO = os.getenv("POLLINATIONS_NOLOGO", "true").lower() in ("1","true","yes")
POLLINATIONS_REFERRER = os.getenv("POLLINATIONS_REFERRER", "livrario-books")
POLLINATIONS_TIMEOUT = float(os.getenv("POLLINATIONS_TIMEOUT", "180"))

# OBS Huawei
OBS_ENDPOINT = os.getenv("OBS_ENDPOINT")  # p.ej. https://obs.la-south-2.myhuaweicloud.com
OBS_AK = os.getenv("OBS_AK")
OBS_SK = os.getenv("OBS_SK")
OBS_BUCKET = os.getenv("OBS_BUCKET", "livrario-books")

# Retries genéricos
RETRY_MAX       = int(os.getenv("RETRY_MAX", "3"))
RETRY_BACKOFF   = float(os.getenv("RETRY_BACKOFF", "2.0"))  # exponencial


# =========================
# Helpers
# =========================
def _mk_rabbit_params() -> pika.ConnectionParameters:
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    if RABBIT_USE_TLS:
        ctx = ssl.create_default_context()
        # Para entornos de prueba con CN/IP no coincidente (usar con cuidado):
        # ctx.check_hostname = False
        # ctx.verify_mode = ssl.CERT_NONE
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
    ch.queue_bind(queue=QUEUE_PROMPTS, exchange=EXCHANGE_NAME, routing_key=RK_PERSONAJE)
    ch.basic_qos(prefetch_count=PREFETCH)

def _slug_no_spaces_lower(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace(" ", "")
    s = s.replace("/", "-").replace("\\", "-")
    return s

def _validate_personaje_msg(msg: Dict[str, Any]) -> Optional[str]:
    # Requisitos para arrancar el flujo:
    # { isbn, tipo:'personaje', nombre, autor, titulo, genero } ; id opcional
    needed = ["isbn", "nombre", "autor", "titulo", "genero"]
    for k in needed:
        if not msg.get(k):
            return f"Falta '{k}'"
    if msg.get("tipo") and msg["tipo"] != "personaje":
        return "tipo distinto de 'personaje'"
    return None


# =========================
# Dify
# =========================
def _call_dify_workflow(inputs: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    """POST /workflows/run (blocking recomendado). Devuelve (ok, json, err)."""
    if not DIFY_API_KEY:
        return False, None, "Missing DIFY_API_KEY"
    url = f"{DIFY_BASE_URL}/workflows/run"
    headers = {
        "Authorization": f"Bearer {DIFY_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "inputs": inputs,
        "response_mode": RESPONSE_MODE,  # "blocking" esperado
        "user": WORKFLOW_USER,
    }

    backoff = 1.0
    last_err = ""
    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=HTTP_TIMEOUT_S)
            if 200 <= r.status_code < 300:
                try:
                    return True, r.json(), "ok"
                except Exception:
                    return False, None, "Dify devolvió cuerpo no-JSON"
            last_err = f"HTTP {r.status_code}: {r.text[:600]}"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
        if attempt < RETRY_MAX:
            time.sleep(backoff)
            backoff *= RETRY_BACKOFF
    return False, None, last_err

def _extract_prompt_from_dify(resp_json: Dict[str, Any]) -> Optional[str]:
    """
    Intenta extraer el prompt del JSON de Dify en forma robusta.
    Ajustá si tu workflow entrega en otra key.
    Heurísticas típicas (bloqueante):
      - resp_json["data"]["outputs"]["prompt"]
      - resp_json["data"]["outputs"]["result"]
      - resp_json["data"]["text"]
      - resp_json["result"]
    """
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
# Pollinations
# =========================
def _build_pollinations_url(prompt: str) -> str:
    q = urllib.parse.quote(prompt, safe="")
    params = {
        "width":  str(POLLINATIONS_WIDTH),
        "height": str(POLLINATIONS_HEIGHT),
        "seed":   str(POLLINATIONS_SEED),
        "model":  POLLINATIONS_MODEL,
        "nologo": "true" if POLLINATIONS_NOLOGO else "false",
        "referrer": POLLINATIONS_REFERRER,
    }
    return f"{POLLINATIONS_BASE}/{q}?{urllib.parse.urlencode(params)}"

def _download_image_bytes(prompt: str) -> Tuple[bool, Optional[bytes], str]:
    url = _build_pollinations_url(prompt)
    backoff = 1.0
    last_err = ""
    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = requests.get(url, timeout=POLLINATIONS_TIMEOUT)
            if r.status_code == 200 and r.content:
                return True, r.content, "ok"
            last_err = f"HTTP {r.status_code}: {r.text[:400]}"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
        if attempt < RETRY_MAX:
            time.sleep(backoff)
            backoff *= RETRY_BACKOFF
    return False, None, last_err


# =========================
# OBS Upload
# =========================
def _upload_obs_jpg(bytes_img: bytes, isbn: str, character_name: str) -> Tuple[bool, str, str]:
    """
    Sube a: characters/<ISBN>/<CHARACTER NAME>/<character_name>_0.jpg
    Retorna (ok, object_key, err)
    """
    if not (OBS_ENDPOINT and OBS_AK and OBS_SK and OBS_BUCKET):
        return False, "", "OBS config incompleta (endpoint/AK/SK/bucket)"

    folder_name = (character_name or "").strip()
    file_slug = _slug_no_spaces_lower(character_name)
    object_key = f"characters/{isbn}/{folder_name}/{file_slug}_0.jpg"

    client = ObsClient(
        access_key_id=OBS_AK,
        secret_access_key=OBS_SK,
        server=OBS_ENDPOINT
    )
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
# Pipeline (por mensaje)
# =========================
def _build_dify_inputs_from_msg(msg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mapea directamente lo que tu workflow necesita para construir el prompt final.
    Ajustá los nombres si tu flujo en Dify espera otras keys.
    """
    return {
        "isbn": msg["isbn"],
        "autor": msg["autor"],
        "titulo": msg["titulo"],
        "genero": msg["genero"],
        "personaje_nombre": msg["nombre"],
        "personaje_id": msg.get("id"),
    }

def _process_message_personaje(msg: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Pipeline:
      evento personaje -> Dify -> prompt -> Pollinations -> OBS
    Devuelve (ok, info).
    """
    # 1) Validar
    err = _validate_personaje_msg(msg)
    if err:
        return False, err

    isbn = str(msg["isbn"])
    nombre = str(msg["nombre"])

    # 2) Dify: obtener prompt final
    dify_inputs = _build_dify_inputs_from_msg(msg)
    ok_dify, dify_json, info_dify = _call_dify_workflow(dify_inputs)
    if not ok_dify or not dify_json:
        return False, f"Dify error: {info_dify}"

    prompt = _extract_prompt_from_dify(dify_json)
    if not prompt:
        # Para debug útil:
        snippet = json.dumps(dify_json)[:800]
        return False, f"No se encontró 'prompt' en respuesta Dify. Resp: {snippet}"

    # 3) Pollinations: descargar JPG del prompt
    ok_img, img_bytes, info_img = _download_image_bytes(prompt)
    if not ok_img or not img_bytes:
        return False, f"Pollinations error: {info_img}"

    # 4) OBS: subir al path requerido
    ok_up, obj_key, info_up = _upload_obs_jpg(img_bytes, isbn, nombre)
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

            print(f"[miniapi] Escuchando cola='{QUEUE_PROMPTS}' rk='{RK_PERSONAJE}' → Dify→Pollinations→OBS")

            def _on_msg(chx, method, props, body: bytes):
                try:
                    payload = json.loads(body.decode("utf-8"))
                    rk = method.routing_key or ""
                    if rk != RK_PERSONAJE:
                        # Si la cola está bindeada a más RKs, ignoramos aquí
                        chx.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    ok, info = _process_message_personaje(payload)
                    if ok:
                        print(f"[miniapi] OK personaje='{payload.get('nombre')}' isbn={payload.get('isbn')} → {info}")
                        chx.basic_ack(delivery_tag=method.delivery_tag)
                    else:
                        print(f"[miniapi] ERROR personaje='{payload.get('nombre')}' isbn={payload.get('isbn')} → {info}")
                        # NACK sin requeue para evitar loops (ideal tener DLQ configurada)
                        chx.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                except Exception as e:
                    print("[miniapi] Excepción procesando mensaje:", e)
                    traceback.print_exc()
                    chx.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            ch.basic_consume(queue=QUEUE_PROMPTS, on_message_callback=_on_msg, auto_ack=False)
            ch.start_consuming()
        except Exception as e:
            print(f"[miniapi] conexión/loop caído: {e}. Reintento en 5s...")
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
        "rabbit": {
            "host": RABBIT_HOST,
            "queue": QUEUE_PROMPTS,
            "rk": RK_PERSONAJE,
        },
        "dify": DIFY_BASE_URL,
        "pollinations": POLLINATIONS_BASE,
        "obs": {
            "bucket": OBS_BUCKET,
            "endpoint": OBS_ENDPOINT,
        },
        "response_mode": RESPONSE_MODE,
    }

@app.post("/test/pipeline")
def test_pipeline(body: Dict[str, Any]):
    ok, info = _process_message_personaje(body)
    return JSONResponse({"ok": ok, "info": info}, status_code=200 if ok else 502)


# =========================
# Startup
# =========================
@app.on_event("startup")
def on_startup():
    th = threading.Thread(target=_consumer_thread, name="prompts-personaje-listener", daemon=True)
    th.start()
    print("[miniapi] listener iniciado")
