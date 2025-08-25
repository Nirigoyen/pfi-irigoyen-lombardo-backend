# app/main.py
import os
import io
import traceback
import requests
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse, Response
from lt_client import try_get_characters_and_places
from openlibrary_client import _get_work_json_safe  # importa helpers si los dejaste públicos


from fastapi import Query
from lt_client import fetch_ck_work_xml  # ya lo tenés con cloudscraper
from parsers import debug_parse

from obs_client import upload_bytes, download_bytes
from db import (
    upsert_book, attach_characters, attach_places, attach_genres,
    get_author_by_isbn, get_cover_key_by_isbn, get_book_info,
    update_author_description  # <-- nuevo helper
)
from parsers import parse_librarything_xml
from openlibrary_client import fetch_with_olclient  # <-- NUEVO

app = FastAPI(title="Book Ingest API")

LIBRARYTHING_ENDPOINT = "https://www.librarything.com/services/rest/1.1/"

# ---------- Helpers externos (ya existentes) ----------
LIBRARYTHING_ENDPOINT = "https://www.librarything.com/services/rest/1.1/"
UA = {"User-Agent": os.getenv("OPENLIBRARY_USER_AGENT", "Livrario/1.0 (+contact)")}

def fetch_librarything_xml(isbn: str, api_key: str) -> bytes:
    # LibraryThing doc indica 'api_key' (con guion bajo). Probamos con ese.
    params = {
        "method": "librarything.ck.getwork",
        "isbn": isbn,
        "apikey": api_key,   # <-- clave
    }
    r = requests.get(LIBRARYTHING_ENDPOINT, params=params, headers=UA, timeout=20)
    r.raise_for_status()
    return r.content

def try_fetch_lt_chars_places(isbn: str, api_key: str) -> tuple[list[str], list[str], str]:
    """Best-effort: si LT falla (403 u otro), devolvemos listas vacías y un status legible."""
    try:
        xml_bytes = fetch_librarything_xml(isbn, api_key)
        chars, places = parse_librarything_xml(xml_bytes)
        return chars, places, "ok"
    except requests.HTTPError as e:
        # 403 es frecuente por Turnstile / API temporalmente off
        return [], [], f"lt_error_http_{e.response}"
    except Exception as e:
        return [], [], f"lt_error_{type(e).__name__}"

def fetch_cover_bytes(isbn: str) -> bytes:
    # Mantenemos la API de covers que preferís
    url = f"https://bookcover.longitood.com/bookcover/{isbn}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.content

# ---------- Endpoint principal de ingesta (actualizado) ----------
@app.post("/ingest")
async def ingest_book(
    isbn: str = Form(...),
    libr_api_key: str = Form(...),
    pdf: UploadFile = File(...),
    force_title: str | None = Form(None),
    force_author: str | None = Form(None),
):
    if pdf.content_type != "application/pdf":
        raise HTTPException(400, "El archivo debe ser PDF")

    try:
        # 1) Subir PDF a OBS
        pdf_bytes = await pdf.read()
        pdf_key = f"books/{isbn}/original.pdf"
        upload_bytes(pdf_key, pdf_bytes, content_type="application/pdf")

        # 2) LibraryThing: personajes + lugares
        # lt_xml = fetch_librarything_xml(isbn, libr_api_key)
        # chars, places = parse_librarything_xml(lt_xml)
        chars, places, lt_status = try_get_characters_and_places(isbn, libr_api_key)


        # 3) Cover -> OBS (longitood)
        cover_data = fetch_cover_bytes(isbn)
        cover_key = f"covers/{isbn}.jpg"
        upload_bytes(cover_key, cover_data, content_type="image/jpeg")

        # 4) Open Library → título/autor/bio/sinopsis/géneros
        ol_meta = fetch_with_olclient(isbn)
        title_from_ol = ol_meta.get("title")
        author_from_ol = ol_meta.get("author")
        author_bio = ol_meta.get("author_description")
        synopsis_from_ol = ol_meta.get("synopsis")
        genres_from_ol = ol_meta.get("genres") or []

        # Respeta force_* si vienen
        title_to_use = force_title or title_from_ol
        author_to_use = force_author or author_from_ol
        synopsis_to_use = synopsis_from_ol
        genres_to_use: list[str] = list(genres_from_ol)[:3]

        # 5) Persistencia principal (crea libro + autor por nombre si aplica)
        upsert_book(
            isbn=isbn,
            title=title_to_use,
            author_name=author_to_use,
            synopsis=synopsis_to_use,
            cover_obs_key=cover_key,
        )

        # 5.1) Bio del autor (si tenemos nombre + descripción)
        if author_to_use and author_bio:
            update_author_description(author_to_use, author_bio)

        # 5.2) Personajes / Lugares / Géneros
        n_chars = attach_characters(isbn, chars)
        n_places = attach_places(isbn, places, top_n=5)
        n_genres = attach_genres(isbn, genres_to_use, top_n=3)

        return JSONResponse({
            "isbn": isbn,
            "pdf_obs_key": pdf_key,
            "cover_obs_key": cover_key,
            "characters_inserted": n_chars,
            "places_inserted": n_places,
            "genres_inserted": n_genres,
            "lt_status": lt_status,
            "metadata": {
                "title": title_to_use,
                "author": author_to_use,
                "author_description": author_bio,
                "synopsis": synopsis_to_use,
                "genres": genres_to_use,
                "openlibrary_raw": ol_meta.get("raw", {})
            }
        })

    except requests.HTTPError as rexc:
        traceback.print_exc()
        raise HTTPException(502, f"Error HTTP externo: {rexc}")
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(500, f"Error interno: {exc}")

@app.get("/books/{isbn}/author")
def get_author(isbn: str):
    author = get_author_by_isbn(isbn)
    if not author:
        raise HTTPException(404, f"No hay autor asociado al ISBN {isbn}")
    return {"isbn": isbn, "author": author}

@app.get("/books/{isbn}/cover")
def get_cover(isbn: str):
    key = get_cover_key_by_isbn(isbn)
    if not key:
        raise HTTPException(404, f"No hay cover asociado al ISBN {isbn}")
    try:
        data = download_bytes(key)
    except Exception as e:
        raise HTTPException(404, f"No se pudo descargar la portada: {e}")
    return Response(content=data, media_type="image/jpeg")

@app.get("/books/{isbn}")
def get_book(isbn: str):
    info = get_book_info(isbn)
    if not info:
        raise HTTPException(404, f"Libro ISBN {isbn} no encontrado")
    return info



@app.get("/debug/librarything")
def debug_librarything(
    isbn: str = Query(...),
    apikey: str = Query(...),
    raw: bool = Query(False)
):
    xml_bytes = fetch_ck_work_xml(isbn, apikey)
    info = debug_parse(xml_bytes)

    if raw:
        # ¡CUIDADO! esto puede ser grande; sólo para inspección rápida
        return {
            "info": info,
            "xml_first_500": xml_bytes[:500].decode("utf-8", errors="replace"),
        }
    return info

@app.get("/debug/openlibrary")
def debug_openlibrary(isbn: str = Query(...)):
    data = fetch_with_olclient(isbn)
    wj = _get_work_json_safe(f"/works/{data['raw'].get('work_olid')}")
    return {
        "resolved": data["raw"],
        "title": data["title"],
        "author": data["author"],
        "has_synopsis": bool(data["synopsis"]),
        "subjects_count": len(wj.get("subjects", [])) if wj else None,
    }