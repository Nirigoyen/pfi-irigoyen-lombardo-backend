# main.py — API unificada (ingesta por título, covers a OBS, LT scraper, OL/GB/Wiki/LibreTranslate)
import os
import io
import re
import json
import time
import html
import urllib.parse
import traceback
import logging
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query
from fastapi.responses import JSONResponse, Response
from PIL import Image  # pip install Pillow

# === Tus dependencias locales existentes ===
from obs_client import upload_bytes  # download_bytes no se usa aquí
from db import (
    upsert_book, attach_characters, attach_places, attach_genres,
    get_author_by_isbn, get_cover_key_by_isbn, get_book_info,
    update_author_description
)
from lt_client import try_get_characters_and_places, fetch_ck_work_xml
from parsers import debug_parse

# -----------------------------------------------------------------------------
# Config general
# -----------------------------------------------------------------------------
app = FastAPI(title="Book Ingest API (Unificada)")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("ingest")

# URL pública fija del bucket OBS
OBS_PUBLIC_BASE = "https://livrario-books.obs.la-south-2.myhuaweicloud.com"

# Endpoints externos (Código 2)
GOOGLE_API_BASE = "https://www.googleapis.com/books/v1/volumes"
WIKI_API_SEARCH_ES = "https://es.wikipedia.org/w/api.php"
WIKI_API_SUMMARY_ES = "https://es.wikipedia.org/api/rest_v1/page/summary/"
WIKI_API_SUMMARY_EN = "https://en.wikipedia.org/api/rest_v1/page/summary/"
OPENLIB_ISBN = "https://openlibrary.org/isbn/{}.json"
OPENLIB_WORK = "https://openlibrary.org{}.json"
OPENLIB_WORK_EDITIONS = "https://openlibrary.org{}/editions.json?limit={limit}&offset={offset}"
OPENLIB_SEARCH = "https://openlibrary.org/search.json"


# -----------------------------------------------------------------------------
# Helpers HTTP (silenciosos)
# -----------------------------------------------------------------------------
def request_json(url: str, params: Optional[dict] = None, timeout: int = 15,
                 method: str = "GET", data: Optional[dict] = None,
                 headers: Optional[dict] = None) -> Optional[dict]:
    try:
        base_headers = {"User-Agent": "book-ingest/2.0"}
        if headers:
            base_headers.update(headers)
        if method.upper() == "GET":
            r = requests.get(url, params=params, timeout=timeout, headers=base_headers)
        else:
            r = requests.post(url, params=params, data=data, timeout=timeout, headers=base_headers)
        if r.status_code == 200:
            return r.json()
    except requests.RequestException:
        pass
    return None


def strip_html(text: str) -> str:
    text = html.unescape(text or "")
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_isbn13(s: str) -> bool:
    s = re.sub(r"[^0-9Xx]", "", s or "")
    return len(s) == 13 and s.isdigit()


def dedup(seq: List[Any]) -> List[Any]:
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


# -----------------------------------------------------------------------------
# Google Books (búsquedas + heurística)
# -----------------------------------------------------------------------------
def google_search(q: str, lang: Optional[str], max_results: int = 20) -> Optional[dict]:
    params = {
        "q": q,
        "printType": "books",
        "projection": "full",
        "maxResults": max_results,
        "fields": "items(id,volumeInfo(title,subtitle,authors,description,categories,industryIdentifiers,language,canonicalVolumeLink,publishedDate))",
    }
    if lang:
        params["langRestrict"] = lang
    api_key = os.getenv("GOOGLE_BOOKS_API_KEY")
    if api_key:
        params["key"] = api_key
    return request_json(GOOGLE_API_BASE, params=params)


def google_search_by_isbn(isbn: str, lang: Optional[str] = "es") -> Optional[dict]:
    return google_search(f"isbn:{isbn}", lang=lang)


def google_search_title_author(title: str, author: Optional[str], lang: Optional[str] = "es") -> Optional[dict]:
    q = f'intitle:"{title.strip()}"'
    if author:
        q += f' inauthor:"{author.strip()}"'
    return google_search(q, lang=lang)


def google_search_title_variants(title: str, lang: Optional[str] = "es") -> List[dict]:
    items: List[dict] = []
    data = google_search(f'intitle:"{title}"', lang)
    if data and data.get("items"): items += data["items"]
    data = google_search(f"intitle:{title}", lang)
    if data and data.get("items"): items += data["items"]
    data = google_search(f"intitle:{title}", None)
    if data and data.get("items"): items += data["items"]
    by_id: Dict[str, dict] = {it.get("id"): it for it in items}
    return list(by_id.values())


def extract_isbn13s(item: dict) -> List[str]:
    vi = item.get("volumeInfo", {}) or {}
    ids = vi.get("industryIdentifiers", []) or []
    out = []
    for obj in ids:
        if obj.get("type") == "ISBN_13":
            v = re.sub(r"[^0-9]", "", obj.get("identifier", ""))
            if is_isbn13(v):
                out.append(v)
    return dedup(out)


def _has_rich_metadata(it: dict) -> Tuple[bool, bool]:
    vi = it.get("volumeInfo", {}) or {}
    return bool(vi.get("description")), bool(vi.get("categories"))


def choose_best_item(items: List[dict], prefer_lang: str = "es", title_query: Optional[str] = None) -> Tuple[Optional[dict], Optional[dict]]:
    if not items:
        return None, None

    def score(it: dict, prefer_lang_opt: Optional[str]) -> int:
        vi = it.get("volumeInfo", {}) or {}
        lang = vi.get("language")
        title = (vi.get("title") or "").lower()
        has_desc, has_cats = _has_rich_metadata(it)
        isbn_count = len(extract_isbn13s(it))
        cats_len = len(vi.get("categories") or [])
        s = 0
        if has_desc: s += 6
        if has_cats: s += 5 + min(cats_len, 3)
        if isbn_count: s += min(isbn_count, 2)
        if vi.get("publishedDate"): s += 1
        if prefer_lang_opt and lang == prefer_lang_opt: s += 4
        if title_query:
            tq = title_query.lower()
            if tq == title: s += 3
            elif tq in title: s += 2
        return s

    rich_es = [it for it in items if (it.get("volumeInfo", {}) or {}).get("language") == prefer_lang and any(_has_rich_metadata(it))]
    if rich_es:
        primary = sorted(rich_es, key=lambda it: score(it, prefer_lang), reverse=True)[0]
    else:
        rich_any = [it for it in items if any(_has_rich_metadata(it))]
        primary = sorted(rich_any, key=lambda it: score(it, None), reverse=True)[0] if rich_any else items[0]

    rich_any = [it for it in items if any(_has_rich_metadata(it))]
    fallback_any = sorted(rich_any, key=lambda it: score(it, None), reverse=True)[0] if rich_any else None
    return primary, fallback_any


# -----------------------------------------------------------------------------
# Open Library (works/ediciones/subjects)
# -----------------------------------------------------------------------------
def openlibrary_search_title(title: str, prefer_lang: str = "es", limit: int = 5) -> Tuple[Optional[str], List[str]]:
    params = {"title": title, "limit": limit}
    data = request_json(OPENLIB_SEARCH, params=params)
    if not data or not data.get("docs"):
        return None, []
    docs = data["docs"]

    def score(doc):
        langs = doc.get("language") or []
        has_spa = any((l or "").startswith("spa") for l in langs)
        has_isbn = bool(doc.get("isbn"))
        year = doc.get("first_publish_year") or 0
        return (1 if has_spa else 0, 1 if has_isbn else 0, year)

    docs_sorted = sorted(docs, key=score, reverse=True)
    best = docs_sorted[0]
    work_key = best.get("key")  # "/works/OL...W"

    isbn13s: List[str] = []
    for d in docs_sorted[:limit]:
        for isbn in d.get("isbn", []) or []:
            s = re.sub(r"[^0-9]", "", str(isbn))
            if is_isbn13(s):
                isbn13s.append(s)
    return work_key, dedup(isbn13s)


def openlibrary_work_from_isbn(isbn13: str) -> Optional[str]:
    data = request_json(OPENLIB_ISBN.format(isbn13))
    if not data:
        return None
    works = data.get("works") or []
    if works:
        return works[0].get("key")
    return None


def openlibrary_work_stats(work_key: str) -> Tuple[Optional[int], Optional[int]]:
    edition_count = None
    first_publish_year = None
    eds = request_json(OPENLIB_WORK_EDITIONS.format(work_key, limit=1, offset=0))
    if eds and isinstance(eds.get("size"), int):
        edition_count = eds["size"]
    wk = request_json(OPENLIB_WORK.format(work_key))
    if wk:
        fpd = wk.get("first_publish_date")
        if fpd and re.search(r"\d{4}", fpd):
            try:
                first_publish_year = int(re.search(r"(\d{4})", fpd).group(1))
            except Exception:
                pass
    return edition_count, first_publish_year


def openlibrary_work_subjects_and_description(work_key: str) -> Tuple[List[str], Optional[str]]:
    wk = request_json(OPENLIB_WORK.format(work_key))
    if not wk:
        return [], None
    subjects = wk.get("subjects") or []
    desc = wk.get("description")
    if isinstance(desc, dict):
        desc = desc.get("value")
    if isinstance(desc, str):
        desc = desc.strip()
    else:
        desc = None
    subjects = [s for s in subjects if isinstance(s, str)]
    return subjects, desc


def openlibrary_collect_subjects_from_editions(work_key: str, scan_limit: int = 60) -> List[str]:
    if scan_limit <= 0:
        return []
    collected: List[str] = []
    remaining = scan_limit
    offset = 0
    step = 50 if scan_limit > 50 else scan_limit
    while remaining > 0:
        lim = min(step, remaining)
        url = OPENLIB_WORK_EDITIONS.format(work_key, limit=lim, offset=offset)
        eds = request_json(url)
        if not eds:
            break
        entries = eds.get("entries") or eds.get("docs") or eds.get("editions") or []
        if not entries:
            break
        for e in entries:
            subs = e.get("subjects") or []
            for s in subs:
                if isinstance(s, str):
                    collected.append(s)
        got = len(entries)
        if got < lim:
            break
        remaining -= got
        offset += got
    return dedup(collected)


def openlibrary_search_works_docs_for_title(title: str, limit: int = 10) -> List[dict]:
    params = {
        "q": title,
        "fields": "key,subject,edition_count,first_publish_year",
        "limit": limit,
    }
    data = request_json(OPENLIB_SEARCH, params=params)
    if not data:
        return []
    docs = [d for d in data.get("docs", []) if isinstance(d.get("key", ""), str) and d["key"].startswith("/works/")]
    return docs


def gather_ol_subjects_enriched(primary_work_key: Optional[str], title: Optional[str], editions_scan_total: int = 60) -> Tuple[List[str], Optional[str]]:
    subjects: List[str] = []
    ol_desc: Optional[str] = None
    remaining = max(0, int(editions_scan_total))
    seen_works: set = set()

    def add_from_work(wkey: str, per_work_scan: int) -> None:
        nonlocal subjects, ol_desc, remaining
        if not wkey or wkey in seen_works or remaining < 0:
            return
        seen_works.add(wkey)
        subs_w, desc_w = openlibrary_work_subjects_and_description(wkey)
        if subs_w:
            subjects += subs_w
        if (not ol_desc) and desc_w:
            ol_desc = desc_w
        scan = max(0, min(per_work_scan, remaining))
        if scan > 0:
            subs_ed = openlibrary_collect_subjects_from_editions(wkey, scan_limit=scan)
            if subs_ed:
                subjects += subs_ed
            remaining -= scan

    if primary_work_key:
        add_from_work(primary_work_key, per_work_scan=remaining or editions_scan_total)

    if title:
        for d in openlibrary_search_works_docs_for_title(title, limit=10):
            add_from_work(d.get("key"), per_work_scan=min(10, remaining))

    subjects = dedup([s for s in subjects if isinstance(s, str) and s.strip()])
    return subjects, ol_desc


# -----------------------------------------------------------------------------
# Wikipedia (bio autor)
# -----------------------------------------------------------------------------
def fetch_wikipedia_bio_es_or_en(name: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not name:
        return None, None, None
    params = {"action": "query", "list": "search", "srsearch": name, "format": "json"}
    data = request_json(WIKI_API_SEARCH_ES, params=params)
    if data and data.get("query", {}).get("search"):
        title = data["query"]["search"][0]["title"]
        slug = title.replace(" ", "_")
        es = request_json(WIKI_API_SUMMARY_ES + urllib.parse.quote(slug))
        if es and es.get("extract"):
            return es.get("extract"), "es", f"https://es.wikipedia.org/wiki/{slug}"
    slug_en = name.replace(" ", "_")
    en = request_json(WIKI_API_SUMMARY_EN + urllib.parse.quote(slug_en))
    if en and en.get("extract"):
        return en.get("extract"), "en", f"https://en.wikipedia.org/wiki/{slug_en}"
    return None, None, None


# -----------------------------------------------------------------------------
# Mapeo de categorías/subjects a ES
# -----------------------------------------------------------------------------
def map_categories_to_es(categories: Optional[List[str]]) -> Tuple[List[str], List[str]]:
    if not categories:
        return [], []
    mapping = {
        "fiction": "Ficción",
        "fantasy": "Fantasía",
        "epic": "Épica",
        "epic fantasy": "Fantasía épica",
        "high fantasy": "Alta fantasía",
        "science fiction": "Ciencia ficción",
        "sci-fi": "Ciencia ficción",
        "magic": "Magia",
        "young adult": "Juvenil",
        "romance": "Romance",
        "mystery": "Misterio",
        "thrillers": "Suspenso",
        "thriller": "Suspenso",
        "horror": "Terror",
        "history": "Historia",
        "biography": "Biografía",
        "biography & autobiography": "Biografía y autobiografía",
        "comics & graphic novels": "Cómic y novela gráfica",
        "literary criticism": "Crítica literaria",
        "poetry": "Poesía",
        "religion": "Religión",
        "philosophy": "Filosofía",
        "life on other planets": "Vida en otros planetas",
        "imaginary wars and battles": "Guerras y batallas imaginarias",
        "new york times bestseller": "Best seller NYT",
    }
    raw = []
    tokens = []
    for c in categories:
        if not c:
            continue
        raw.append(c)
        parts = re.split(r"[\/;–—\-]+", c)
        flat_parts = []
        for p in parts:
            flat_parts += [x.strip() for x in p.split(",")]
        for token in flat_parts:
            if not token:
                continue
            low = token.lower()
            if low.startswith("nyt:"):
                tokens.append(token)
                continue
            if low in mapping:
                tokens.append(mapping[low])
                continue
            words = [w.strip() for w in low.split() if w.strip()]
            if "science" in words and "fiction" in words:
                tokens.append(mapping["science fiction"])
                continue
            mapped_any = False
            for key, val in mapping.items():
                if key in low:
                    tokens.append(val)
                    mapped_any = True
                    break
            if not mapped_any:
                tokens.append(token[:1].upper() + token[1:])
    return dedup(tokens), dedup(raw)


# -----------------------------------------------------------------------------
# Traducción (LibreTranslate)
# -----------------------------------------------------------------------------
def translate_text(text: str, source_lang: Optional[str], target_lang: str = "es") -> Tuple[str, str]:
    text = (text or "").strip()
    if not text:
        return text, "empty"
    url = os.getenv("LIBRETRANSLATE_URL")
    api_key = os.getenv("LIBRETRANSLATE_API_KEY")
    if not url:
        return text, "no-provider"
    payload = {"q": text, "source": (source_lang or "auto"), "target": target_lang, "format": "text"}
    if api_key:
        payload["api_key"] = api_key
    try:
        resp = requests.post(url, data=payload, timeout=20, headers={"Accept": "application/json"})
        if resp.status_code == 200:
            data = resp.json()
            translated = data.get("translatedText") or data.get("translated_text") or text
            return translated, "libretranslate"
        return text, f"provider-error:{resp.status_code}"
    except Exception as e:
        return text, f"provider-exc:{e}"


# -----------------------------------------------------------------------------
# Construcción de salida (Google primario + OL enriquecido)
# -----------------------------------------------------------------------------
def build_output_json(
    primary_item: dict,
    fallback_item: Optional[dict],
    work_key: Optional[str],
    author_bios: Dict[str, Dict[str, Optional[str]]],
    preferred_isbn13: Optional[str],
    translate_flag: bool = True,
    target_lang: str = "es",
) -> Dict[str, Any]:
    vi_p = primary_item.get("volumeInfo", {}) or {}
    title = vi_p.get("title")
    authors = vi_p.get("authors") or []
    description = vi_p.get("description") or ""
    lang_desc = vi_p.get("language")
    categories_google = vi_p.get("categories") or []

    if fallback_item:
        vi_f = fallback_item.get("volumeInfo", {}) or {}
        if not description and vi_f.get("description"):
            description = vi_f.get("description")
            lang_desc = vi_f.get("language") or lang_desc
        if not categories_google and vi_f.get("categories"):
            categories_google = vi_f.get("categories")

    all_isbn13 = extract_isbn13s(primary_item)
    if fallback_item:
        all_isbn13 = dedup(all_isbn13 + extract_isbn13s(fallback_item))
    preferred = preferred_isbn13 if (preferred_isbn13 and preferred_isbn13 in all_isbn13) else (all_isbn13[0] if all_isbn13 else preferred_isbn13)

    out_authors = []
    for a in authors:
        bio = author_bios.get(a, {})
        out_authors.append({
            "id": None,
            "name": a,
            "bio": bio.get("bio"),
            "bio_lang": bio.get("lang"),
            "wikipedia_url": bio.get("url"),
        })

    final_id = work_key.replace("/works/", "") if work_key else f"GB:{primary_item.get('id')}"

    synopsis_raw = strip_html(description)
    synopsis_es = synopsis_raw
    synopsis_src_lang = (lang_desc or "").lower() if lang_desc else None
    synopsis_translation_provider = None
    if translate_flag and synopsis_raw and (synopsis_src_lang and synopsis_src_lang != target_lang):
        synopsis_es, synopsis_translation_provider = translate_text(synopsis_raw, source_lang=synopsis_src_lang, target_lang=target_lang)

    genres_google, genres_google_raw = map_categories_to_es(categories_google)

    out = {
        "id": final_id,
        "title": title,
        "authors": out_authors,
        "preferred_isbn13": preferred,
        "all_isbn13": all_isbn13,
        "genres": [],                 # se llenan con OL en merge
        "genres_raw": [],
        "genres_google": genres_google,
        "genres_google_raw": genres_google_raw,
        "synopsis": synopsis_es or "",
        "synopsis_source_lang": synopsis_src_lang,
        "synopsis_translation": synopsis_translation_provider,
        "google_volume_id": primary_item.get("id"),
        "language": vi_p.get("language"),
        "canonical_volume_link": vi_p.get("canonicalVolumeLink"),
        "publishedDate": vi_p.get("publishedDate"),
    }
    return out


def merge_with_openlibrary(out_json: Dict[str, Any], work_key: Optional[str],
                           translate_flag: bool = True, target_lang: str = "es",
                           ol_editions_scan: int = 60, title_hint: Optional[str] = None) -> Dict[str, Any]:
    subjects_raw, ol_desc = gather_ol_subjects_enriched(
        primary_work_key=work_key,
        title=title_hint,
        editions_scan_total=ol_editions_scan,
    )

    # Completar sinopsis desde OL si aún no hay
    if (not out_json.get("synopsis")) and ol_desc:
        cleaned = strip_html(ol_desc)
        syn = cleaned
        prov = None
        if translate_flag:
            syn, prov = translate_text(cleaned, source_lang="auto", target_lang=target_lang)
        out_json["synopsis"] = syn
        out_json["synopsis_source_lang"] = out_json.get("synopsis_source_lang") or "unknown"
        out_json["synopsis_translation"] = out_json.get("synopsis_translation") or prov

    # Subjects → géneros principales si existen
    if subjects_raw:
        mapped_es, _ = map_categories_to_es(subjects_raw)
        out_json["genres"] = dedup(mapped_es + subjects_raw)   # mapeados + raw
        out_json["genres_raw"] = subjects_raw
    else:
        out_json["genres"] = out_json.get("genres_google") or []
        out_json["genres_raw"] = out_json.get("genres_google_raw") or []

    return out_json


# -----------------------------------------------------------------------------
# Longitood → imagen JPG + subida a OBS
# -----------------------------------------------------------------------------
def _image_bytes_to_jpeg(src_bytes: bytes, quality: int = 90) -> Optional[bytes]:
    try:
        with Image.open(io.BytesIO(src_bytes)) as im:
            if im.mode not in ("RGB", "L"):
                im = im.convert("RGB")
            elif im.mode == "L":
                im = im.convert("RGB")
            out = io.BytesIO()
            im.save(out, format="JPEG", quality=quality, optimize=True)
            return out.getvalue()
    except Exception as e:
        log.warning("Fallo convirtiendo cover a JPEG: %s", e)
        return None


def fetch_cover_from_longitood_as_jpg(isbn: str) -> bytes:
    api_url = f"https://bookcover.longitood.com/bookcover/{isbn}"
    r = requests.get(api_url, timeout=20)
    r.raise_for_status()
    data = r.json()
    img_url = data.get("url")
    if not img_url:
        raise ValueError("longitood: respuesta sin 'url'")
    r2 = requests.get(img_url, timeout=30)
    r2.raise_for_status()
    src_bytes = r2.content
    jpg_bytes = _image_bytes_to_jpeg(src_bytes)
    return jpg_bytes if jpg_bytes is not None else src_bytes


# -----------------------------------------------------------------------------
# Endpoint principal de INGESTA (AHORA por TÍTULO; autor opcional)
# -----------------------------------------------------------------------------
@app.post("/ingest")
async def ingest_book_by_title(
    title: str = Form(..., description="Título del libro (obligatorio)"),
    author_opt: Optional[str] = Form(None, description="Autor (opcional, ayuda a desambiguar)"),
    libr_api_key: str = Form(..., description="API key de LibraryThing"),
    pdf: UploadFile = File(...),
    force_title: Optional[str] = Form(None, description="Forzar título final"),
    force_author: Optional[str] = Form(None, description="Forzar autor final"),
    force_reingest: bool = Form(False, description="Reprocesar aunque exista"),
    translate_synopsis: bool = Form(True, description="Traducir sinopsis si hace falta"),
    ol_editions_scan: int = Form(60, description="Ediciones OL a escanear p/subjects (default 60)"),
):
    try:
        if pdf.content_type != "application/pdf":
            raise HTTPException(400, "El archivo debe ser PDF")

        # 1) Google Books por título (+ variantes) y heurísticas de elección
        items: List[dict] = []
        d = google_search_title_author(title, author_opt, lang="es")
        if d and d.get("items"): items += d["items"]
        items += google_search_title_variants(title, lang="es")

        # Fallback: OpenLibrary title → ISBN candidates → Google by ISBN
        prefer_isbn = None
        work_key_hint = None
        if not items:
            wk, isbn_candidates = openlibrary_search_title(title, prefer_lang="es", limit=5)
            if wk: work_key_hint = wk
            for cand in isbn_candidates:
                d2 = google_search_by_isbn(cand, lang="es")
                if d2 and d2.get("items"):
                    items += d2["items"]
                    prefer_isbn = cand
                    break

        if not items:
            raise HTTPException(404, "No se encontraron resultados para ese título.")

        primary, fallback_any = choose_best_item(items, prefer_lang="es", title_query=title)
        if not primary:
            raise HTTPException(500, "No se pudo elegir un resultado adecuado.")

        # 2) Determinar ISBN-13 preferido y Work de OpenLibrary
        isbns13 = extract_isbn13s(primary)
        if fallback_any:
            isbns13 = dedup(isbns13 + extract_isbn13s(fallback_any))

        work_key = work_key_hint
        if isbns13 and not work_key:
            candidates = [prefer_isbn] + isbns13 if prefer_isbn else isbns13
            for candidate in candidates:
                if candidate and is_isbn13(candidate):
                    wk = openlibrary_work_from_isbn(candidate)
                    if wk:
                        work_key = wk
                        break

        # 3) Bios de autores (Wikipedia ES→EN)
        vi = (primary.get("volumeInfo") or {})
        author_bios: Dict[str, Dict[str, Optional[str]]] = {}
        for a in (vi.get("authors") or []):
            bio, bio_lang, wiki_url = fetch_wikipedia_bio_es_or_en(a)
            author_bios[a] = {"bio": bio, "lang": bio_lang, "url": wiki_url}
            time.sleep(0.2)  # cortesía

        # 4) Construcción JSON base y merge con OpenLibrary (subjects→géneros)
        out = build_output_json(
            primary_item=primary,
            fallback_item=fallback_any,
            work_key=work_key,
            author_bios=author_bios,
            preferred_isbn13=prefer_isbn,
            translate_flag=translate_synopsis,
            target_lang="es",
        )
        out = merge_with_openlibrary(
            out,
            work_key,
            translate_flag=translate_synopsis,
            target_lang="es",
            ol_editions_scan=ol_editions_scan,
            title_hint=title,
        )

        isbn = out.get("preferred_isbn13")
        if not (isbn and is_isbn13(isbn)):
            raise HTTPException(422, "No se pudo determinar un ISBN-13 válido a partir del título. No es posible almacenar ni generar cover.")

        # 5) Early-exit si ya existe y no pediste reingesta
        if not force_reingest:
            existing = get_book_info(isbn)
            if existing:
                return JSONResponse({
                    "isbn": isbn,
                    "skipped": True,
                    "reason": "already_exists",
                    "cover_public_url": f"{OBS_PUBLIC_BASE}/covers/{isbn}.jpg",
                    "book": existing,
                })

        # 6) Subir PDF a OBS
        pdf_bytes = await pdf.read()
        pdf_key = f"books/{isbn}/original.pdf"
        upload_bytes(pdf_key, pdf_bytes, content_type="application/pdf")

        # 7) LibraryThing → personajes y lugares
        chars, places, lt_status = try_get_characters_and_places(isbn, libr_api_key)
        log.info("LT status=%s chars=%d places=%d", lt_status, len(chars), len(places))

        # 8) Cover desde longitood → JPG → OBS
        cover_bytes = fetch_cover_from_longitood_as_jpg(isbn)
        cover_key = f"covers/{isbn}.jpg"
        upload_bytes(cover_key, cover_bytes, content_type="image/jpeg")

        # 9) Persistencia principal (DB)
        #    Forzar título/autor si vienen por Form
        final_title = force_title or out.get("title")
        authors_list = out.get("authors") or []
        detected_author = authors_list[0]["name"] if authors_list else None
        final_author = force_author or detected_author
        synopsis = out.get("synopsis")

        upsert_book(
            isbn=isbn,
            title=final_title,
            author_name=final_author,
            synopsis=synopsis,
            cover_obs_key=cover_key,
        )

        # 10) Guardar bio principal si existe
        primary_bio = None
        if final_author:
            # buscar bio del autor final (si coincide con alguno encontrado)
            for a in authors_list:
                if a.get("name") == final_author and a.get("bio"):
                    primary_bio = a["bio"]
                    break
            if not primary_bio and authors_list and authors_list[0].get("bio"):
                primary_bio = authors_list[0]["bio"]
        if final_author and primary_bio:
            update_author_description(final_author, primary_bio)

        # 11) Attach secundarios (chars/places/genres)
        n_chars = attach_characters(isbn, chars)
        n_places = attach_places(isbn, places, top_n=5)

        # Preferir géneros mapeados a ES (primeros 3)
        genres_all = out.get("genres") or []
        # intentar priorizar los que provienen del mapeo (presentes también en genres_google)
        genres_es = out.get("genres_google") or []
        if genres_es:
            preferred = [g for g in genres_all if g in set(genres_es)]
            if preferred:
                genres_to_use = dedup(preferred)[:3]
            else:
                genres_to_use = dedup(genres_all)[:3]
        else:
            genres_to_use = dedup(genres_all)[:3]
        n_genres = attach_genres(isbn, genres_to_use, top_n=3)

        # 12) (Opcional) Info de OL (stats del work)
        work_stats = {}
        if work_key:
            ed_count, fpy = openlibrary_work_stats(work_key)
            work_stats = {"edition_count": ed_count, "first_publish_year": fpy, "work_id": work_key.replace("/works/", "")}

        return JSONResponse({
            "isbn": isbn,
            "pdf_obs_key": pdf_key,
            "cover_obs_key": cover_key,
            "cover_public_url": f"{OBS_PUBLIC_BASE}/covers/{isbn}.jpg",
            "characters_inserted": n_chars,
            "places_inserted": n_places,
            "genres_inserted": n_genres,
            "lt_status": lt_status,
            "metadata": {
                "id": out.get("id"),
                "title": final_title,
                "author": final_author,
                "authors": out.get("authors"),
                "synopsis": synopsis,
                "synopsis_source_lang": out.get("synopsis_source_lang"),
                "synopsis_translation": out.get("synopsis_translation"),
                "genres": genres_to_use,
                "genres_raw": out.get("genres_raw"),
                "preferred_isbn13": out.get("preferred_isbn13"),
                "all_isbn13": out.get("all_isbn13"),
                "google_volume_id": out.get("google_volume_id"),
                "language": out.get("language"),
                "canonical_volume_link": out.get("canonical_volume_link"),
                "publishedDate": out.get("publishedDate"),
                "openlibrary": work_stats,
                "sources_used": {
                    "google_books": True,
                    "openlibrary": True,
                    "wikipedia": any(a.get("bio") for a in (out.get("authors") or [])),
                    "libretranslate": (out.get("synopsis_translation") == "libretranslate"),
                },
            }
        })

    except requests.HTTPError as rexc:
        traceback.print_exc()
        raise HTTPException(502, f"Error HTTP externo: {rexc}")
    except HTTPException:
        raise
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(500, f"Error interno: {exc}")


# -----------------------------------------------------------------------------
# Endpoints de lectura (se mantienen)
# -----------------------------------------------------------------------------
@app.get("/book")
def get_book_by_isbn(
    isbn: str = Query(..., description="ISBN-10 o ISBN-13"),
    include_cover_url: bool = Query(True, description="Adjuntar URL pública del cover")
):
    info = get_book_info(isbn)
    if not info:
        raise HTTPException(404, f"Libro ISBN {isbn} no encontrado")
    if include_cover_url:
        info = dict(info)
        info["cover_public_url"] = f"{OBS_PUBLIC_BASE}/covers/{isbn}.jpg"
    return info


@app.get("/books/{isbn}/author")
def get_author(isbn: str):
    author = get_author_by_isbn(isbn)
    if not author:
        raise HTTPException(404, f"No hay autor asociado al ISBN {isbn}")
    return {"isbn": isbn, "author": author}


@app.get("/books/{isbn}/cover")
def get_cover(isbn: str):
    return {"url": f"{OBS_PUBLIC_BASE}/covers/{isbn}.jpg"}


@app.get("/books/{isbn}")
def get_book(isbn: str):
    info = get_book_info(isbn)
    if not info:
        raise HTTPException(404, f"Libro ISBN {isbn} no encontrado")
    return info


# -----------------------------------------------------------------------------
# Endpoints de DEBUG / TEST: para cada integración
# -----------------------------------------------------------------------------
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
def debug_openlibrary_by_isbn(
    isbn: str = Query(..., description="ISBN-10 o ISBN-13")
):
    wk = openlibrary_work_from_isbn(re.sub(r"[^0-9]", "", isbn))
    if not wk:
        return {"found": False}
    subs, desc = openlibrary_work_subjects_and_description(wk)
    ed_count, fpy = openlibrary_work_stats(wk)
    return {
        "found": True,
        "work_key": wk,
        "subjects_count": len(subs),
        "subjects_sample": subs[:15],
        "has_description": bool(desc),
        "description_preview": (strip_html(desc) if desc else "")[:300],
        "edition_count": ed_count,
        "first_publish_year": fpy,
    }


@app.get("/debug/openlibrary_title")
def debug_openlibrary_by_title(
    title: str = Query(..., description="Título del libro"),
    scan: int = Query(30, description="Ediciones a escanear")
):
    wk, candidates = openlibrary_search_title(title, prefer_lang="es", limit=5)
    subs, desc = gather_ol_subjects_enriched(wk, title, editions_scan_total=scan)
    ed_count, fpy = (None, None)
    if wk:
        ed_count, fpy = openlibrary_work_stats(wk)
    return {
        "work_key": wk,
        "isbn_candidates": candidates,
        "subjects_count": len(subs),
        "subjects_sample": subs[:20],
        "has_description": bool(desc),
        "edition_count": ed_count,
        "first_publish_year": fpy,
    }


@app.get("/debug/googlebooks")
def debug_googlebooks_by_isbn(
    isbn: str = Query(..., description="ISBN-10 o ISBN-13")
):
    d = google_search_by_isbn(re.sub(r"[^0-9]", "", isbn), lang="es")
    if not (d and d.get("items")):
        d = google_search_by_isbn(re.sub(r"[^0-9]", "", isbn), lang=None)
    if not (d and d.get("items")):
        return {"found": False}
    it = d["items"][0]
    vi = it.get("volumeInfo", {}) or {}
    return {
        "found": True,
        "id": it.get("id"),
        "title": vi.get("title"),
        "authors": vi.get("authors"),
        "has_description": bool(vi.get("description")),
        "description_preview": (strip_html(vi.get("description") or "")[:300]),
        "categories": vi.get("categories") or [],
        "language": vi.get("language"),
        "isbns13": extract_isbn13s(it),
    }


@app.get("/debug/googlebooks_title")
def debug_googlebooks_by_title(
    title: str = Query(..., description="Título"),
    author: Optional[str] = Query(None, description="Autor (opcional)")
):
    items: List[dict] = []
    d = google_search_title_author(title, author, lang="es")
    if d and d.get("items"): items += d["items"]
    items += google_search_title_variants(title, lang="es")
    if not items:
        return {"found": False}
    primary, fallback_any = choose_best_item(items, prefer_lang="es", title_query=title)
    if not primary:
        return {"found": False}
    vi = primary.get("volumeInfo", {}) or {}
    return {
        "found": True,
        "picked_id": primary.get("id"),
        "title": vi.get("title"),
        "authors": vi.get("authors"),
        "language": vi.get("language"),
        "isbns13": extract_isbn13s(primary),
        "has_description": bool(vi.get("description")),
        "categories": vi.get("categories") or [],
        "fallback_used": bool(fallback_any is not None),
    }


@app.get("/debug/wikipedia")
def debug_wikipedia(
    author: str = Query(..., description="Nombre del autor/a")
):
    bio, lang, url = fetch_wikipedia_bio_es_or_en(author)
    return {
        "author": author,
        "has_bio": bool(bio),
        "bio_lang": lang,
        "wikipedia_url": url,
        "bio_preview": (bio or "")[:400],
    }


@app.get("/debug/cover")
def debug_cover(
    isbn: str = Query(..., description="ISBN para resolver cover (longitood)"),
    download: bool = Query(False, description="Si true, devuelve image/jpeg")
):
    api_url = f"https://bookcover.longitood.com/bookcover/{isbn}"
    r = requests.get(api_url, timeout=15)
    r.raise_for_status()
    data = r.json()
    img_url = data.get("url")

    if not download:
        return {"resolver_status": r.status_code, "resolver_url": api_url, "image_url": img_url}

    if not img_url:
        raise HTTPException(404, "longitood no devolvió 'url'")

    r2 = requests.get(img_url, timeout=20)
    if r2.status_code != 200:
        raise HTTPException(r2.status_code, f"No se pudo descargar la imagen real: HTTP {r2.status_code}")

    jpg_bytes = _image_bytes_to_jpeg(r2.content) or r2.content
    return Response(content=jpg_bytes, media_type="image/jpeg")


@app.get("/debug/translate")
def debug_translate(
    text: str = Query(..., description="Texto a traducir"),
    source_lang: Optional[str] = Query(None, description="Idioma origen (auto si None)"),
    target_lang: str = Query("es", description="Idioma destino (default es)")
):
    translated, prov = translate_text(text, source_lang=source_lang, target_lang=target_lang)
    return {"provider": prov, "translated": translated[:800]}
