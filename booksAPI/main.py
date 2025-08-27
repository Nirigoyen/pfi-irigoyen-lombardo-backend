# booksAPI/main.py
import os
import traceback
import requests
import logging
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query
from fastapi.responses import JSONResponse, Response

from obs_client import upload_bytes, download_bytes
from db import (
    upsert_book, attach_characters, attach_places, attach_genres,
    get_author_by_isbn, get_cover_key_by_isbn, get_book_info,
    update_author_description
)

# LibraryThing (cloudscraper) y parser
from lt_client import try_get_characters_and_places, fetch_ck_work_xml
from parsers import debug_parse

# Open Library + normalizador de géneros
from openlibrary_client import fetch_with_olclient, normalize_genres_from_subjects

# Google Books
from google_books import gb_by_isbn_es, gb_pick_fields

app = FastAPI(title="Book Ingest API")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("ingest")


# ---------- Helpers externos ----------
def fetch_cover_bytes(isbn: str) -> bytes:
    """
    Descarga portada desde longitood (no cachea). Subís el resultado a OBS luego.
    """
    url = f"https://bookcover.longitood.com/bookcover/{isbn}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.content


# ---------- Endpoint principal de ingesta (sin Wikipedia/Wikidata) ----------
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

        # 2) LibraryThing (cloudscraper): personajes + lugares (best-effort)
        chars, places, lt_status = try_get_characters_and_places(isbn, libr_api_key)
        log.info("LT status=%s chars=%d places=%d", lt_status, len(chars), len(places))

        # 3) Cover -> OBS (longitood)
        cover_data = fetch_cover_bytes(isbn)
        cover_key = f"covers/{isbn}.jpg"
        upload_bytes(cover_key, cover_data, content_type="image/jpeg")

        # 4) Open Library (base)
        ol_meta = fetch_with_olclient(isbn)
        title_from_ol = ol_meta.get("title")
        author_from_ol = ol_meta.get("author")
        author_bio = ol_meta.get("author_description")
        synopsis_from_ol = ol_meta.get("synopsis")
        genres_from_ol = ol_meta.get("genres") or []

        # 5) Google Books (ES) — sinopsis y categorías
        gb_item = None
        gb_desc_es = None
        gb_cats = []
        gb_title_es = None
        try:
            gb_item = gb_by_isbn_es(isbn)  # reintenta sin key si 403
            if gb_item:
                gb_desc_es, gb_cats, gb_title_es = gb_pick_fields(gb_item)
        except Exception as e:
            log.warning("Google Books ES error: %s", e)

        # 6) Fusión / prioridad (preferimos español cuando exista, sin Wikipedia)
        title_to_use = force_title or title_from_ol or gb_title_es
        author_to_use = force_author or author_from_ol

        # Sinopsis: OL → GB(ES)
        synopsis_to_use = synopsis_from_ol or gb_desc_es

        # Géneros: OL → GB(categorías normalizadas)
        if genres_from_ol:
            genres_to_use = list(genres_from_ol)[:3]
            genres_source = "openlibrary"
        else:
            genres_to_use = normalize_genres_from_subjects(gb_cats, limit=3) if gb_cats else []
            genres_source = "googlebooks_normalized" if genres_to_use else "none"

        # 7) Persistencia
        upsert_book(
            isbn=isbn,
            title=title_to_use,
            author_name=author_to_use,
            synopsis=synopsis_to_use,
            cover_obs_key=cover_key,
        )

        if author_to_use and author_bio:
            update_author_description(author_to_use, author_bio)

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
                "genres_source": genres_source,
                "sources_used": {
                    "openlibrary": True,
                    "googlebooks_es": bool(gb_item),
                },
                "openlibrary_raw": ol_meta.get("raw", {})
            }
        })

    except requests.HTTPError as rexc:
        traceback.print_exc()
        raise HTTPException(502, f"Error HTTP externo: {rexc}")
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(500, f"Error interno: {exc}")


# ----------- Endpoints existentes -----------
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


# =========================
# Endpoints de DEBUG/TEST (sin Wikipedia/Wikidata)
# =========================
@app.get("/debug/librarything")
def debug_librarything(
    isbn: str = Query(..., description="ISBN-10 o ISBN-13"),
    apikey: str = Query(..., description="API key de LibraryThing"),
    raw: bool = Query(False, description="Incluir preview crudo del XML")
):
    xml_bytes = fetch_ck_work_xml(isbn, apikey)
    info = debug_parse(xml_bytes)
    if raw:
        return {
            "info": info,
            "xml_first_800": xml_bytes[:800].decode("utf-8", errors="replace"),
        }
    return info

@app.get("/debug/openlibrary")
def debug_openlibrary(
    isbn: str = Query(..., description="ISBN-10 o ISBN-13")
):
    data = fetch_with_olclient(isbn)
    return {
        "resolved_raw": data.get("raw"),
        "title": data.get("title"),
        "author": data.get("author"),
        "has_author_description": bool(data.get("author_description")),
        "has_synopsis": bool(data.get("synopsis")),
        "genres": data.get("genres") or [],
    }

@app.get("/debug/googlebooks")
def debug_googlebooks(
    isbn: str = Query(..., description="ISBN-10 o ISBN-13")
):
    try:
        item = gb_by_isbn_es(isbn)
    except requests.HTTPError as e:
        return JSONResponse(
            status_code=502,
            content={"error": f"Google Books HTTP {getattr(e.response,'status_code',None)}",
                     "body": getattr(e.response, "text", "")[:300]}
        )
    if not item:
        return {"found": False}
    desc_es, cats, title_es = gb_pick_fields(item)
    return {
        "found": True,
        "title_es": title_es,
        "has_description_es": bool(desc_es),
        "description_es_preview": (desc_es or "")[:300],
        "categories_raw": cats,
    }

@app.get("/debug/cover")
def debug_cover(
    isbn: str = Query(..., description="ISBN de la portada (longitood)"),
    download: bool = Query(False, description="Si true, devuelve la imagen como image/jpeg")
):
    url = f"https://bookcover.longitood.com/bookcover/{isbn}"
    r = requests.get(url, timeout=15)
    if not download:
        return {
            "status_code": r.status_code,
            "content_type": r.headers.get("Content-Type"),
            "content_length": int(r.headers.get("Content-Length", "0")),
            "url": url
        }
    if r.status_code != 200:
        raise HTTPException(r.status_code, f"No se pudo descargar la portada: HTTP {r.status_code}")
    return Response(content=r.content, media_type="image/jpeg")
