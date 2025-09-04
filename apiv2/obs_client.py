# app/obs_client.py
import os
from typing import Optional
from obs import ObsClient, PutObjectHeader

OBS_AK = "HPUANXF5VB9ZAHS96FX6" #os.getenv("OBS_AK")
OBS_SK = "UYtG4LB7r9h69DCXGhMrrER2cZFO3YhHRpw3b0hW" #os.getenv("OBS_SK")
OBS_SERVER = "obs.la-south-2.myhuaweicloud.com" #os.getenv("OBS_SERVER")   # p.ej. obs.la-south-2.myhuaweicloud.com
OBS_BUCKET = "livrario-books"  #os.getenv("OBS_BUCKET")   # p.ej. books-bucket

def _client() -> ObsClient:
    if not (OBS_AK and OBS_SK and OBS_SERVER and OBS_BUCKET):
        raise RuntimeError("OBS env vars faltantes (OBS_AK/OBS_SK/OBS_SERVER/OBS_BUCKET)")
    return ObsClient(access_key_id=OBS_AK, secret_access_key=OBS_SK, server=OBS_SERVER, is_secure=True)

def upload_bytes(key: str, data: bytes, content_type: Optional[str] = None) -> None:
    client = _client()
    try:
        headers = PutObjectHeader()
        if content_type:
            headers.contentType = content_type
        resp = client.putContent(OBS_BUCKET, key, data, headers)
        if resp.status >= 300:
            raise RuntimeError(f"OBS putContent failed: {resp.errorCode} {resp.errorMessage}")
    finally:
        client.close()

def download_bytes(key: str) -> bytes:
    """Descarga bytes desde OBS; lanza excepción si no existe."""
    client = _client()
    try:
        resp = client.getObject(OBS_BUCKET, key, loadStreamInMemory=True)
        if resp.status < 300:
            # esdk-obs-python deja los bytes en resp.body.buffer cuando loadStreamInMemory=True
            return resp.body.buffer
        raise RuntimeError(f"OBS getObject failed: {resp.errorCode} {resp.errorMessage}")
    finally:
        client.close()
