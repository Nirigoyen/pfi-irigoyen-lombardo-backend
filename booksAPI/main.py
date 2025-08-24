# app/main.py
import os
import io
import traceback
import requests
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse, Response

from obs_client import upload_bytes, download_bytes
from db import (
    upsert_book, attach_characters, attach_places, attach_genres,
    get_author_by_isbn, get_cover_key_by_isbn, get_book_info
)
from parsers import parse_librarything_xml

app = FastAPI(title="Book Ingest API")

LIBRARYTHING_ENDPOINT = "https://www.librarything.com/services/rest/1.1/"

# ---------- Helpers externos (ya existentes) ----------
def fetch_librarything_xml(isbn: str, api_key: str) -> bytes:
    params = {
        "method": "librarything.ck.getwork",
        "isbn": isbn,
        "apikey": api_key,
    }
    r = requests.get(LIBRARYTHING_ENDPOINT, params=params, timeout=20)
    r.raise_for_status()
    return r.content

def fetch_cover_bytes(isbn: str) -> bytes:
    url = f"https://bookcover.longitood.com/bookcover/{isbn}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.content

# ---------- Endpoint principal de ingesta (ya existente) ----------
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
        lt_xml = fetch_librarything_xml(isbn, libr_api_key)
        chars, places = parse_librarything_xml(lt_xml)

        # 3) Cover -> OBS
        cover_data = fetch_cover_bytes(isbn)
        cover_key = f"covers/{isbn}.jpg"
        upload_bytes(cover_key, cover_data, content_type="image/jpeg")

        # 4) TODOs externos (título, autor, sinopsis, géneros)
        title_to_use = force_title or None  # TODO: reemplazar con otra API
        author_to_use = force_author or None  # TODO: reemplazar con otra API
        synopsis_to_use = None               # TODO
        genres_to_use: list[str] = []        # TODO (top 3)

        # 5) Persistencia
        upsert_book(
            isbn=isbn,
            title=title_to_use,
            author_name=author_to_use,
            synopsis=synopsis_to_use,
            cover_obs_key=cover_key,
        )
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
            "todos": {
                "title": "TODO desde otra API (o usar force_title)",
                "author": "TODO desde otra API (o usar force_author)",
                "author_description": "TODO desde otra API",
                "synopsis": "TODO desde otra API",
                "genres": "TODO desde otra API (top 3)"
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
    """
    Devuelve autor y su descripción (si existe), según el libro (isbn).
    """
    author = get_author_by_isbn(isbn)
    if not author:
        # Puede ser que no haya autor asignado todavía (TODO pendiente)
        raise HTTPException(404, f"No hay autor asociado al ISBN {isbn}")
    return {"isbn": isbn, "author": author}

@app.get("/books/{isbn}/cover")
def get_cover(isbn: str):
    """
    Devuelve la portada como bytes (image/jpeg) desde OBS.
    Si preferís sólo la key, podés devolver JSON usando get_cover_key_by_isbn.
    """
    key = get_cover_key_by_isbn(isbn)
    if not key:
        raise HTTPException(404, f"No hay cover asociado al ISBN {isbn}")
    try:
        data = download_bytes(key)
    except Exception as e:
        raise HTTPException(404, f"No se pudo descargar la portada: {e}")
    # asumimos .jpg (si guardás otro mime, podés detectarlo por extensión)
    return Response(content=data, media_type="image/jpeg")

@app.get("/books/{isbn}")
def get_book(isbn: str):
    """
    Devuelve info completa del libro:
      - isbn, title, synopsis, cover_obs_key
      - author { id, name, description }
      - genres [hasta 3], places [hasta 5], characters [lista]
    """
    info = get_book_info(isbn)
    if not info:
        raise HTTPException(404, f"Libro ISBN {isbn} no encontrado")
    return info
