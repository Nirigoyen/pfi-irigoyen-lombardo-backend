#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FastAPI para metadatos de libros (ES) con:
- Google Books (ediciones, sinopsis, ISBNs)
- Open Library (Work ID global + géneros enriquecidos desde subjects del work, ediciones y works candidatos)
- Wikipedia (bio del autor en ES, fallback EN)
- Traducción opcional de sinopsis con LibreTranslate

Instalación:
  pip install fastapi uvicorn requests

Ejecución:
  uvicorn app:app --reload --port 8000

Uso:
  GET http://localhost:8000/book?title=Palabras%20radiantes&lang=es
  GET http://localhost:8000/book?isbn=9788466657549
  # Parámetros opcionales:
  #   author=..., translate=true|false, ol_editions_scan=60
"""
from __future__ import annotations

import html
import json
import os
import re
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware


# =========================
# Endpoints / Constantes
# =========================
GOOGLE_API_BASE = "https://www.googleapis.com/books/v1/volumes"
WIKI_API_SEARCH_ES = "https://es.wikipedia.org/w/api.php"
WIKI_API_SUMMARY_ES = "https://es.wikipedia.org/api/rest_v1/page/summary/"
WIKI_API_SUMMARY_EN = "https://en.wikipedia.org/api/rest_v1/page/summary/"
OPENLIB_ISBN = "https://openlibrary.org/isbn/{}.json"
OPENLIB_WORK = "https://openlibrary.org{}.json"
OPENLIB_WORK_EDITIONS = "https://openlibrary.org{}/editions.json?limit={limit}&offset={offset}"
OPENLIB_SEARCH = "https://openlibrary.org/search.json"


# =========================
# HTTP helpers (silencioso)
# =========================
def request_json(url: str, params: Optional[dict] = None, timeout: int = 15,
                 method: str = "GET", data: Optional[dict] = None,
                 headers: Optional[dict] = None) -> Optional[dict]:
    """GET/POST JSON, silencioso si no es 200."""
    try:
        base_headers = {"User-Agent": "book-fetch-api/1.0"}
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


# =========================
# Google Books
# =========================
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

    def score(it: dict, prefer_lang: Optional[str]) -> int:
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
        if lang == prefer_lang: s += 4
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


# =========================
# Open Library (work/ediciones)
# =========================
def openlibrary_search_title(title: str, prefer_lang: str = "es", limit: int = 5) -> Tuple[Optional[str], List[str]]:
    params = {"title": title, "limit": limit}
    data = request_json(OPENLIB_SEARCH, params=params)
    if not data or not data.get("docs"):
        return None, []
    docs = data["docs"]
    def score(doc):
        langs = doc.get("language") or []
        has_spa = any(l.startswith("spa") for l in langs)
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
        return works[0].get("key")  # "/works/OL...W"
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
    """Escanea hasta 'scan_limit' ediciones y recolecta todos los subjects a nivel edición."""
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


# =========================
# Wikipedia (bio autor)
# =========================
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


# =========================
# Mapeo categorías/subjects -> ES
# =========================
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
        # etiquetas especiales (nyt:...) se preservan tal cual
    }
    raw = []
    tokens = []
    for c in categories:
        if not c:
            continue
        raw.append(c)
        # dividir por slash, guiones y comas (captura "Fiction, fantasy, epic")
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


# =========================
# Traducción (LibreTranslate)
# =========================
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


# =========================
# Helpers de subjects (ENRIQUECIDOS)
# =========================
def openlibrary_search_works_docs_for_title(title: str, limit: int = 10) -> List[dict]:
    """Busca works por título (Open Library search) y devuelve docs /works/* con algunos campos básicos."""
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
    """
    Junta subjects del work raíz (si existe) + ediciones del work + (si hay title) works candidatos del search,
    incluidas algunas de sus ediciones (hasta 'editions_scan_total' en total).
    Devuelve (subjects_raw_unificados, descripcion_del_work_si_hay).
    """
    subjects: List[str] = []
    ol_desc: Optional[str] = None
    remaining = max(0, int(editions_scan_total))
    seen_works: set = set()

    def add_from_work(wkey: str, per_work_scan: int) -> None:
        nonlocal subjects, ol_desc, remaining
        if not wkey or wkey in seen_works or remaining < 0:
            return
        seen_works.add(wkey)
        # subjects del work + description
        subs_w, desc_w = openlibrary_work_subjects_and_description(wkey)
        if subs_w:
            subjects += subs_w
        if (not ol_desc) and desc_w:
            ol_desc = desc_w
        # subjects desde ediciones (cupos)
        scan = max(0, min(per_work_scan, remaining))
        if scan > 0:
            subs_ed = openlibrary_collect_subjects_from_editions(wkey, scan_limit=scan)
            if subs_ed:
                subjects += subs_ed
            remaining -= scan

    # 1) Work raíz
    if primary_work_key:
        add_from_work(primary_work_key, per_work_scan=remaining or editions_scan_total)

    # 2) Works candidatos por título
    if title:
        for d in openlibrary_search_works_docs_for_title(title, limit=10):
            add_from_work(d.get("key"), per_work_scan=min(10, remaining))

    subjects = dedup([s for s in subjects if isinstance(s, str) and s.strip()])
    return subjects, ol_desc


# =========================
# Construcción de salida
# =========================
def build_output_json(
    primary_item: dict,
    fallback_item: Optional[dict],
    work_key: Optional[str],
    author_bios: Dict[str, Dict[str, Optional[str]]],
    preferred_isbn13: Optional[str],
    translate: bool = True,
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
    if translate and synopsis_raw and (synopsis_src_lang and synopsis_src_lang != target_lang):
        synopsis_es, synopsis_translation_provider = translate_text(synopsis_raw, source_lang=synopsis_src_lang, target_lang=target_lang)

    genres_google, genres_google_raw = map_categories_to_es(categories_google)

    out = {
        "id": final_id,
        "title": title,
        "authors": out_authors,
        "preferred_isbn13": preferred,
        "all_isbn13": all_isbn13,
        # géneros OL se colocan luego
        "genres": [],
        "genres_raw": [],
        # guardamos Google por si sirve de fallback
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
                           translate: bool = True, target_lang: str = "es",
                           ol_editions_scan: int = 60, title_hint: Optional[str] = None) -> Dict[str, Any]:
    """
    PRIORIDAD: subjects de Open Library enriquecidos:
      - work raíz (si hay) + ediciones del work
      - si hay título, también works candidatos del search (y algunas ediciones)
    Si OL no trae nada, fallback a categorías de Google ya presentes en out_json.
    """
    all_subjects_raw: List[str] = []

    subjects_raw, ol_desc = gather_ol_subjects_enriched(
        primary_work_key=work_key,
        title=title_hint,
        editions_scan_total=ol_editions_scan,
    )
    if subjects_raw:
        all_subjects_raw = subjects_raw

    # Completar sinopsis desde OL si aún no hay
    if (not out_json.get("synopsis")) and ol_desc:
        cleaned = strip_html(ol_desc)
        syn = cleaned
        prov = None
        if translate:
            syn, prov = translate_text(cleaned, source_lang="auto", target_lang=target_lang)
        out_json["synopsis"] = syn
        out_json["synopsis_source_lang"] = out_json.get("synopsis_source_lang") or "unknown"
        out_json["synopsis_translation"] = out_json.get("synopsis_translation") or prov

    # Si conseguimos subjects de OL, los usamos como principales (mapeados + crudos)
    if all_subjects_raw:
        mapped_es, _ = map_categories_to_es(all_subjects_raw)
        out_json["genres"] = dedup(mapped_es + all_subjects_raw)   # como tu ejemplo: mapeados + raw
        out_json["genres_raw"] = all_subjects_raw
    else:
        # Fallback: categorías de Google
        out_json["genres"] = out_json.get("genres_google") or []
        out_json["genres_raw"] = out_json.get("genres_google_raw") or []

    return out_json


# =========================
# Agregador para API
# =========================
def aggregate_book_api(isbn: Optional[str], title: Optional[str], author: Optional[str], lang: str = "es",
                       translate_flag: bool = True, ol_editions_scan: int = 60) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    items: List[dict] = []
    prefer_isbn = None
    work_key_hint = None

    if isbn:
        prefer_isbn = re.sub(r"[^0-9]", "", isbn)
        d = google_search_by_isbn(prefer_isbn, lang=lang)
        if d and d.get("items"): items += d["items"]
        else:
            d = google_search_by_isbn(prefer_isbn, lang=None)
            if d and d.get("items"): items += d["items"]
    elif title:
        d = google_search_title_author(title, author, lang=lang)
        if d and d.get("items"): items += d["items"]
        items += google_search_title_variants(title, lang=lang)
        if not items:
            wk, isbn_candidates = openlibrary_search_title(title, prefer_lang=lang, limit=5)
            if wk: work_key_hint = wk
            for cand in isbn_candidates:
                d2 = google_search_by_isbn(cand, lang=lang)
                if d2 and d2.get("items"):
                    items += d2["items"]
                    prefer_isbn = cand
                    break

    if not items:
        raise HTTPException(status_code=404, detail="No se encontraron resultados en Google Books.")

    primary, fallback_any = choose_best_item(items, prefer_lang=lang, title_query=title)
    if not primary:
        raise HTTPException(status_code=404, detail="No se pudo elegir un item adecuado.")

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

    # Bios de autores
    vi = primary.get("volumeInfo", {}) or {}
    author_bios: Dict[str, Dict[str, Optional[str]]] = {}
    for a in (vi.get("authors") or []):
        bio, bio_lang, wiki_url = fetch_wikipedia_bio_es_or_en(a)
        author_bios[a] = {"bio": bio, "lang": bio_lang, "url": wiki_url}
        time.sleep(0.2)

    # Google (+fallback edición)
    out = build_output_json(
        primary_item=primary,
        fallback_item=fallback_any,
        work_key=work_key,
        author_bios=author_bios,
        preferred_isbn13=prefer_isbn,
        translate=translate_flag,
        target_lang=lang or "es",
    )

    # Subjects de Open Library enriquecidos como fuente principal de géneros
    out = merge_with_openlibrary(
        out,
        work_key,
        translate=translate_flag,
        target_lang=lang or "es",
        ol_editions_scan=ol_editions_scan,
        title_hint=title,
    )

    # Info adicional (antes se imprimía como [INFO] ...)
    info = {}
    if work_key:
        ed_count, fpy = openlibrary_work_stats(work_key)
        info = {
            "work_id": work_key.replace("/works/", ""),
            "edition_count": ed_count,
            "first_publish_year": fpy,
        }

    # Limpiar campos auxiliares de Google para no “ensuciar” la salida si no se quieren
    out.pop("genres_google", None)
    out.pop("genres_google_raw", None)

    return out, info


# =========================
# FastAPI app
# =========================
app = FastAPI(title="Book Metadata API", version="1.0.0", description="Google Books + Open Library + Wikipedia (+traducción)")

# CORS (ajusta orígenes según tu front)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # cámbialo en producción
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/book")
def get_book(
    isbn: Optional[str] = Query(None, description="ISBN-13 del libro"),
    title: Optional[str] = Query(None, description="Título del libro"),
    author: Optional[str] = Query(None, description="Autor (opcional, mejora precisión)"),
    lang: str = Query("es", description="Idioma preferido para Google Books y traducción"),
    translate: bool = Query(True, description="Traducir sinopsis si no está en el idioma destino"),
    ol_editions_scan: int = Query(60, ge=0, le=300, description="Cuántas ediciones de OL escanear para subjects"),
):
    if not isbn and not title:
        raise HTTPException(status_code=400, detail="Debes enviar 'isbn' o 'title'.")
    out, info = aggregate_book_api(
        isbn=isbn,
        title=title,
        author=author,
        lang=lang,
        translate_flag=translate,
        ol_editions_scan=ol_editions_scan,
    )
    return {"info": info, "data": out}


# Ejecución con: uvicorn app:app --reload
