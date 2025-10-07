# app/main.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FastAPI con persistencia (PostgreSQL) + enriquecimiento:
- Google Books (búsqueda base, sinopsis, ISBNs)
- Open Library (Work ID + géneros desde 'subjects')
- Wikipedia (bio del autor ES/EN)
- LibraryThing (personajes y lugares) vía cloudscraper (best-effort)
- Traducción opcional de sinopsis (LibreTranslate)
- OBS Huawei: subida de PDF y cover (Longitood -> JPG)
- Persistencia usando app/db.py

Ejecutar:
  uvicorn app.main:app --reload --port 8080
"""
from __future__ import annotations

import html
import io
import json
import os
import re
import time
import urllib.parse
from functools import lru_cache
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fastapi import Body, FastAPI, HTTPException, Query, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware

# === DB helpers (tuyos) ===
try:
    from db import (
        upsert_book,
        attach_genres,
        attach_characters,
        attach_places,
        update_author_description,
    )
except Exception:
    from db import (
        upsert_book,
        attach_genres,
        attach_characters,
        attach_places,
        update_author_description,
    )

# === LibraryThing (tus helpers) ===
try:
    from lt_client import try_get_characters_and_places
except Exception:
    from lt_client import try_get_characters_and_places

# === OBS (tus helpers) ===
try:
    from obs_client import upload_bytes
except Exception:
    from obs_client import upload_bytes


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
LONGITOOD_API = "https://bookcover.longitood.com/bookcover/"


_session_lock = Lock()
_http_session: Optional[requests.Session] = None


def _get_http_session() -> requests.Session:
    global _http_session
    if _http_session is None:
        with _session_lock:
            if _http_session is None:
                session = requests.Session()
                retries = Retry(
                    total=2,
                    backoff_factor=0.3,
                    status_forcelist=(429, 500, 502, 503, 504),
                    allowed_methods=frozenset(["GET", "POST"]),
                )
                adapter = HTTPAdapter(pool_connections=8, pool_maxsize=16, max_retries=retries)
                session.mount("https://", adapter)
                session.mount("http://", adapter)
                _http_session = session
    return _http_session


# =========================
# HTTP helpers (silencioso)
# =========================
def request_json(url: str, params: Optional[dict] = None, timeout: int = 15,
                 method: str = "GET", data: Optional[dict] = None,
                 headers: Optional[dict] = None) -> Optional[dict]:
    """GET/POST JSON, silencioso si no es 200."""
    method_upper = method.upper()
    try:
        base_headers = {"User-Agent": "book-fetch-api/1.3"}
        if headers:
            base_headers.update(headers)
        session = _get_http_session()
        if method_upper == "GET":
            resp = session.get(url, params=params, timeout=timeout, headers=base_headers)
        elif method_upper == "POST":
            resp = session.post(url, params=params, data=data, timeout=timeout, headers=base_headers)
        else:
            resp = session.request(method_upper, url, params=params, data=data, timeout=timeout, headers=base_headers)
        if resp.status_code == 200:
            return resp.json()
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
        return works[0].get("key")
    return None


@lru_cache(maxsize=128)
def _get_openlibrary_work_cached(work_key: Optional[str]) -> Optional[dict]:
    if not work_key:
        return None
    data = request_json(OPENLIB_WORK.format(work_key))
    return data if isinstance(data, dict) else None


@lru_cache(maxsize=128)
def openlibrary_work_stats(work_key: str) -> Tuple[Optional[int], Optional[int]]:
    if not work_key:
        return None, None
    edition_count = None
    first_publish_year = None
    eds = request_json(OPENLIB_WORK_EDITIONS.format(work_key, limit=1, offset=0))
    if eds and isinstance(eds.get("size"), int):
        edition_count = eds["size"]
    wk = _get_openlibrary_work_cached(work_key)
    if wk:
        fpd = wk.get("first_publish_date")
        if fpd and re.search(r"\d{4}", fpd):
            try:
                first_publish_year = int(re.search(r"(\d{4})", fpd).group(1))
            except Exception:
                pass
    return edition_count, first_publish_year


def openlibrary_work_subjects_and_description(work_key: str) -> Tuple[List[str], Optional[str]]:
    wk = _get_openlibrary_work_cached(work_key)
    if not wk:
        return [], None
    subjects = [s for s in (wk.get("subjects") or []) if isinstance(s, str)]
    desc = wk.get("description")
    if isinstance(desc, dict):
        desc = desc.get("value")
    if isinstance(desc, str):
        desc = desc.strip()
    else:
        desc = None
    return subjects, desc


def openlibrary_collect_subjects_from_editions(work_key: str, scan_limit: int = 60) -> List[str]:
    """Escanea hasta 'scan_limit' ediciones y recolecta 'subjects' de cada edición."""
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
@lru_cache(maxsize=128)
def _fetch_wikipedia_bio_cached(query: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    params = {"action": "query", "list": "search", "srsearch": query, "format": "json"}
    data = request_json(WIKI_API_SEARCH_ES, params=params)
    if data and data.get("query", {}).get("search"):
        title = data["query"]["search"][0]["title"]
        slug = title.replace(" ", "_")
        es = request_json(WIKI_API_SUMMARY_ES + urllib.parse.quote(slug))
        if es and es.get("extract"):
            return es.get("extract"), "es", f"https://es.wikipedia.org/wiki/{slug}"
    slug_en = query.replace(" ", "_")
    en = request_json(WIKI_API_SUMMARY_EN + urllib.parse.quote(slug_en))
    if en and en.get("extract"):
        return en.get("extract"), "en", f"https://en.wikipedia.org/wiki/{slug_en}"
    return None, None, None



def fetch_wikipedia_bio_es_or_en(name: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    query = (name or "").strip()
    if not query:
        return None, None, None
    return _fetch_wikipedia_bio_cached(query)


# =========================
# Mapeo de géneros → genéricos
# =========================
def map_categories_to_es(categories: Optional[List[str]]) -> Tuple[List[str], List[str]]:
    """
    Colapsa subjects/categorías a un set chico y genérico.
    Devuelve (genericos_es, raw_dedup).
    """
    if not categories:
        return [], []

    raw = dedup([c for c in categories if isinstance(c, str) and c.strip()])

    ORDER = [
        "Fantasía",
        "Ciencia ficción",
        "Terror",
        "Policial",
        "Misterio/Suspenso",
        "Romance",
        "Histórica",
        "Aventura",
        "Juvenil",
        "No ficción",
    ]
    labels: set = set()

    def norm(s: str) -> str:
        try:
            import unicodedata
            s2 = unicodedata.normalize("NFD", s.lower())
            s2 = "".join(ch for ch in s2 if not unicodedata.combining(ch))
            return s2
        except Exception:
            return s.lower()

    def add(lbl: str):
        labels.add(lbl)

    def classify(token: str):
        t = norm(token)
        if not t or t.startswith("nyt:"):
            return

        if (
            "science fiction" in t or "sci fi" in t or "sci-fi" in t or "scifi" in t or
            "ciencia ficcion" in t or "space opera" in t or "dystop" in t or
            "postapoc" in t or "post-apoc" in t or "cyberpunk" in t or
            "time travel" in t or "viaje en el tiempo" in t or
            "alien" in t or "robot" in t or "mecha" in t
        ):
            add("Ciencia ficción"); return

        if (
            "fantasy" in t or "fantasia" in t or "epic fantasy" in t or "high fantasy" in t or
            "urban fantasy" in t or "sword and sorcery" in t or
            "magic" in t or "magia" in t or "dragon" in t or "dragons" in t or
            "witch" in t or "bruja" in t or "brujeria" in t or "elf" in t
        ):
            add("Fantasía"); return

        if (
            "horror" in t or "terror" in t or "ghost" in t or "haunt" in t or
            "vampir" in t or "werewolf" in t or "licantrop" in t or
            "lovecraft" in t or "slasher" in t
        ):
            add("Terror"); return

        if (
            "policial" in t or "detective" in t or "crime" in t or "noir" in t or
            "procedural" in t or "police" in t or "hardboiled" in t
        ):
            add("Policial"); return

        if (
            "mystery" in t or "whodunit" in t or "thriller" in t or "suspense" in t or
            "suspenso" in t or "conspiracy" in t or "psychological thriller" in t
        ):
            add("Misterio/Suspenso"); return

        if (
            "romance" in t or "romantica" in t or "love" in t or "chick lit" in t or
            "erotic fiction" in t or "novela rosa" in t
        ):
            add("Romance"); return

        if "historical fiction" in t or "ficcion histor" in t or "novela histor" in t or "historiques" in t:
            add("Histórica"); return

        if "adventure" in t or "aventura" in t or "swashbuckler" in t or "pirat" in t or "explor" in t:
            add("Aventura"); return

        if (
            "young adult" in t or "ya " in t or t.startswith("ya") or
            "juvenile" in t or "juvenil" in t or "teen" in t or
            "middle grade" in t or "children" in t or "infantil" in t
        ):
            add("Juvenil"); return

        if (
            "nonfiction" in t or "non-fiction" in t or "no ficcion" in t or
            "biography" in t or "biografia" in t or "memoir" in t or "ensayo" in t or
            ("history" in t and "historical fiction" not in t) or
            "philosophy" in t or "filosofia" in t or "religion" in t or
            "politics" in t or "politica" in t or "business" in t or "econom" in t or
            ("science" in t and "science fiction" not in t)
        ):
            add("No ficción"); return

    for c in raw:
        parts = re.split(r"[\/;–—\-]+", c)
        flat = []
        for p in parts:
            flat += [x.strip() for x in p.split(",")]
        for tok in flat:
            classify(tok)

    mapped = [g for g in ORDER if g in labels]
    return dedup(mapped), raw


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
# Helpers de subjects (enriquecidos)
# =========================
def openlibrary_search_works_docs_for_title(title: str, limit: int = 10) -> List[dict]:
    params = {"q": title, "fields": "key,subject,edition_count,first_publish_year", "limit": limit}
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


# =========================
# Cover (Longitood) → JPG (best-effort)
# =========================
def _image_bytes_to_jpeg(src_bytes: bytes) -> Optional[bytes]:
    """Convierte cualquier imagen a JPEG. Requiere Pillow; si falla, devuelve None."""
    try:
        from PIL import Image  # type: ignore
        img = Image.open(io.BytesIO(src_bytes))
        if img.mode in ("RGBA", "LA"):
            bg = Image.new("RGB", img.size, (255, 255, 255))
            bg.paste(img, mask=img.split()[-1])
            img = bg
        else:
            img = img.convert("RGB")
        out = io.BytesIO()
        img.save(out, format="JPEG", quality=88, optimize=True)
        return out.getvalue()
    except Exception:
        return None


def fetch_cover_from_longitood_as_jpg(isbn: str) -> bytes:
    """
    1) Llama a longitood y obtiene JSON {"url": "<imagen real>"}
    2) Descarga la imagen real
    3) Convierte a JPEG y retorna bytes JPEG (si no puede, retorna los bytes originales)
    """
    api_url = f"{LONGITOOD_API}{isbn}"
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
    return jpg_bytes if jpg_bytes is not None else src_bytes  # fallback: sube como "jpg" igual


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
        "genres": [],              # se completan con OL
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
        "characters": [],
        "places": [],
    }
    return out


def merge_with_openlibrary(out_json: Dict[str, Any], work_key: Optional[str],
                           translate: bool = True, target_lang: str = "es",
                           ol_editions_scan: int = 60, title_hint: Optional[str] = None) -> Dict[str, Any]:
    subjects_raw, ol_desc = gather_ol_subjects_enriched(
        primary_work_key=work_key,
        title=title_hint,
        editions_scan_total=ol_editions_scan,
    )

    if (not out_json.get("synopsis")) and ol_desc:
        cleaned = strip_html(ol_desc)
        syn = cleaned
        prov = None
        if translate:
            syn, prov = translate_text(cleaned, source_lang="auto", target_lang=target_lang)
        out_json["synopsis"] = syn
        out_json["synopsis_source_lang"] = out_json.get("synopsis_source_lang") or "unknown"
        out_json["synopsis_translation"] = out_json.get("synopsis_translation") or prov

    if subjects_raw:
        mapped_es, _ = map_categories_to_es(subjects_raw)
        out_json["genres"] = mapped_es                 # SOLO genéricos
        out_json["genres_raw"] = subjects_raw          # crudos a parte
    else:
        out_json["genres"] = out_json.get("genres_google") or []
        out_json["genres_raw"] = out_json.get("genres_google_raw") or []

    return out_json


# =========================
# LibraryThing helpers
# =========================
def fetch_lt_best_effort(isbn: Optional[str], apikey: Optional[str]) -> Tuple[List[str], List[str], str]:
    if not isbn or not is_isbn13(isbn):
        return [], [], "lt_skipped_no_isbn"
    key = apikey or os.getenv("LIBRARYTHING_API_KEY")
    if not key:
        return [], [], "lt_skipped_no_key"
    try:
        chars, places, status = try_get_characters_and_places(isbn, key)
        return chars, places, status
    except Exception as e:
        name = type(e).__name__
        return [], [], f"lt_err_{name}"


# =========================
# Agregador (sin persistir)
# =========================
def aggregate_book_api(isbn: Optional[str], title: Optional[str], author: Optional[str], lang: str = "es",
                       translate_flag: bool = True, ol_editions_scan: int = 60,
                       lt: bool = True, lt_api_key: Optional[str] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
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

    # Fallback OL-only si Google no responde
    if not items:
        work_key = None
        isbn_list: List[str] = []
        if isbn:
            s = re.sub(r"[^0-9]", "", isbn)
            if is_isbn13(s):
                work_key = openlibrary_work_from_isbn(s)
                isbn_list = [s]
        elif title:
            wk, candidates = openlibrary_search_title(title, prefer_lang=lang, limit=5)
            work_key = wk
            isbn_list = candidates

        if not work_key:
            raise HTTPException(status_code=404, detail="No se encontraron resultados (Google sin datos y OL sin work).")

        subjects_raw, ol_desc = gather_ol_subjects_enriched(work_key, title, editions_scan_total=ol_editions_scan)
        mapped_es, _ = map_categories_to_es(subjects_raw)

        out = {
            "id": work_key.replace("/works/", ""),
            "title": title,
            "authors": [],
            "preferred_isbn13": (isbn_list[0] if isbn_list else None),
            "all_isbn13": isbn_list,
            "genres": mapped_es,
            "genres_raw": subjects_raw,
            "synopsis": strip_html(ol_desc) if ol_desc else "",
            "synopsis_source_lang": None,
            "synopsis_translation": None,
            "google_volume_id": None,
            "language": None,
            "canonical_volume_link": None,
            "publishedDate": None,
            "characters": [],
            "places": [],
        }

        lt_status = "lt_skipped_no_isbn"
        if lt:
            lt_chars, lt_places, lt_status = fetch_lt_best_effort(out.get("preferred_isbn13"), lt_api_key)
            out["characters"] = lt_chars
            out["places"] = lt_places

        info = {"work_id": out["id"], "edition_count": None, "first_publish_year": None, "lt_status": lt_status}
        return out, info

    # Normal (con Google)
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
    for author_name in (vi.get("authors") or []):
        before = _fetch_wikipedia_bio_cached.cache_info()
        bio, bio_lang, wiki_url = fetch_wikipedia_bio_es_or_en(author_name)
        after = _fetch_wikipedia_bio_cached.cache_info()
        author_bios[author_name] = {"bio": bio, "lang": bio_lang, "url": wiki_url}
        if after.misses > before.misses:
            time.sleep(0.2)

    out = build_output_json(
        primary_item=primary,
        fallback_item=fallback_any,
        work_key=work_key,
        author_bios=author_bios,
        preferred_isbn13=prefer_isbn,
        translate=translate_flag,
        target_lang=lang or "es",
    )

    out = merge_with_openlibrary(
        out,
        work_key,
        translate=translate_flag,
        target_lang=lang or "es",
        ol_editions_scan=ol_editions_scan,
        title_hint=title,
    )

    lt_status = "lt_skipped_no_isbn"
    if lt:
        isbn_to_use = out.get("preferred_isbn13") or (out.get("all_isbn13") or [None])[0]
        lt_chars, lt_places, lt_status = fetch_lt_best_effort(isbn_to_use, lt_api_key)
        out["characters"] = lt_chars
        out["places"] = lt_places

    info = {}
    if work_key:
        ed_count, fpy = openlibrary_work_stats(work_key)
        info = {
            "work_id": work_key.replace("/works/", ""),
            "edition_count": ed_count,
            "first_publish_year": fpy,
            "lt_status": lt_status,
        }

    out.pop("genres_google", None)
    out.pop("genres_google_raw", None)

    return out, info


# =========================
# Persistencia
# =========================
def persist_book_record(
    data: Dict[str, Any],
    top_genres_n: int = 3,
    top_places_n: int = 5,
    cover_obs_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Guarda:
      - books (isbn, title, author_id por nombre, synopsis, cover_obs_key)
      - authors.description (bio)
      - book_genres (top N)
      - characters (todos) y places (top N con orden)
    """
    isbn = data.get("preferred_isbn13") or (data.get("all_isbn13") or [None])[0]
    if not isbn or not is_isbn13(isbn):
        raise HTTPException(status_code=422, detail="No hay ISBN-13 para persistir este libro.")

    title_to_use = data.get("title")
    synopsis_to_use = data.get("synopsis") or None

    author_name = None
    author_bio = None
    if data.get("authors"):
        author_name = (data["authors"][0] or {}).get("name")
        author_bio = (data["authors"][0] or {}).get("bio")

    # upsert libro + autor (incluye cover_obs_key si lo tenemos)
    upsert_book(
        isbn=isbn,
        title=title_to_use,
        author_name=author_name,
        synopsis=synopsis_to_use,
        cover_obs_key=cover_obs_key,
    )

    if author_name and author_bio:
        update_author_description(author_name, author_bio)

    genres = data.get("genres") or []
    genres_attached = attach_genres(isbn, genres, top_n=top_genres_n)

    chars = data.get("characters") or []
    places = data.get("places") or []
    chars_attached = attach_characters(isbn, chars)
    places_attached = attach_places(isbn, places, top_n=top_places_n)

    return {
        "isbn": isbn,
        "title": title_to_use,
        "author_name": author_name,
        "synopsis_len": len(synopsis_to_use or ""),
        "genres_attached": genres_attached,
        "characters_attached": chars_attached,
        "places_attached": places_attached,
        "author_description_updated": bool(author_name and author_bio),
        "cover_obs_key_saved": bool(cover_obs_key),
    }

def _clean_isbn(s: str) -> str:
    return re.sub(r"[^0-9Xx]", "", s or "")

def isbn10_to_isbn13(isbn10: str) -> Optional[str]:
    """
    Convierte ISBN-10 a ISBN-13 (prefijo 978 + nuevo dígito verificador).
    """
    s = _clean_isbn(isbn10)
    if len(s) != 10:
        return None
    core9 = s[:9]
    if not core9.isdigit():
        return None
    base12 = "978" + core9
    total = 0
    for i, ch in enumerate(base12):
        d = int(ch)
        total += d if (i % 2 == 0) else 3 * d
    check = (10 - (total % 10)) % 10
    return base12 + str(check)

def openlibrary_collect_isbns_from_editions(work_key: str, scan_limit: int = 80) -> List[str]:
    """
    Recorre ediciones de un work y reúne ISBN-13; si encuentra ISBN-10, lo convierte a 13.
    work_key ejemplo: '/works/OL16813053W'
    """
    if not work_key:
        return []
    acc: List[str] = []
    remaining = max(0, int(scan_limit))
    offset = 0
    step = 50 if remaining > 50 else (remaining or 50)

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
            # ISBN-13 directos
            for i13 in (e.get("isbn_13") or []):
                s13 = _clean_isbn(i13)
                if len(s13) == 13 and s13.isdigit():
                    acc.append(s13)
            # Convertir ISBN-10
            for i10 in (e.get("isbn_10") or []):
                s10 = _clean_isbn(i10)
                if len(s10) == 10:
                    conv = isbn10_to_isbn13(s10)
                    if conv:
                        acc.append(conv)

        got = len(entries)
        if got < lim:
            break
        remaining -= got
        offset += got

    # dedup manteniendo orden
    seen, out = set(), []
    for x in acc:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out



# =========================
# FastAPI
# =========================
app = FastAPI(
    title="Book Metadata API",
    version="1.3.0",
    description="Google Books + Open Library + Wikipedia + LibraryThing (+traducción) con OBS Huawei y persistencia PostgreSQL"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ajusta en prod
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
    author: Optional[str] = Query(None, description="Autor (opcional)"),
    lang: str = Query("es", description="Idioma preferido"),
    translate: bool = Query(True, description="Traducir sinopsis"),
    ol_editions_scan: int = Query(60, ge=0, le=300, description="Ediciones OL a escanear"),
    lt: bool = Query(True, description="Intentar personajes/lugares desde LibraryThing"),
    lt_api_key: Optional[str] = Query(None, description="API key de LibraryThing (si no, var de entorno)"),
    persist: bool = Query(False, description="Persistir en BD"),
    top_genres_n: int = Query(3, ge=1, le=20, description="Géneros a guardar si persist=true"),
    top_places_n: int = Query(5, ge=1, le=50, description="Lugares a guardar si persist=true"),
):
    if not isbn and not title:
        raise HTTPException(status_code=400, detail="Debes enviar 'isbn' o 'title'.")

    data, info = aggregate_book_api(
        isbn=isbn, title=title, author=author, lang=lang,
        translate_flag=translate, ol_editions_scan=ol_editions_scan,
        lt=lt, lt_api_key=lt_api_key,
    )

    db_result = None
    if persist:
        db_result = persist_book_record(data, top_genres_n=top_genres_n, top_places_n=top_places_n)

    return {"info": info, "data": data, "db": db_result}


@app.post("/book/save")
def save_book(
    payload: Dict[str, Any] = Body(
        ...,
        example={
            "isbn": "9788466657549",
            "title": None,
            "author": None,
            "lang": "es",
            "translate": True,
            "ol_editions_scan": 120,
            "lt": True,
            "lt_api_key": None,
            "top_genres_n": 3,
            "top_places_n": 5
        },
        description="Enviá isbn o title (author opcional)."
    )
):
    isbn = payload.get("isbn")
    title = payload.get("title")
    author = payload.get("author")
    lang = payload.get("lang", "es")
    translate = bool(payload.get("translate", True))
    ol_editions_scan = int(payload.get("ol_editions_scan", 60))
    lt = bool(payload.get("lt", True))
    lt_api_key = payload.get("lt_api_key")
    top_genres_n = int(payload.get("top_genres_n", 3))
    top_places_n = int(payload.get("top_places_n", 5))

    if not isbn and not title:
        raise HTTPException(status_code=400, detail="Debes enviar 'isbn' o 'title' en el body.")

    data, info = aggregate_book_api(
        isbn=isbn, title=title, author=author, lang=lang,
        translate_flag=translate, ol_editions_scan=ol_editions_scan,
        lt=lt, lt_api_key=lt_api_key,
    )

    db_result = persist_book_record(data, top_genres_n=top_genres_n, top_places_n=top_places_n)
    return {"info": info, "data": data, "db": db_result}


# =========================
# Nuevo: /book/ingest (multipart) -> PDF a OBS + cover a OBS + persist
# =========================
@app.post("/book/ingest")
async def ingest_book(
    # metadata (Form)
    isbn: Optional[str] = Form(None, description="ISBN-13 del libro"),
    title: Optional[str] = Form(None, description="Título del libro"),
    author: Optional[str] = Form(None, description="Autor (opcional)"),
    lang: str = Form("es"),
    translate: bool = Form(True),
    ol_editions_scan: int = Form(60),
    lt: bool = Form(True),
    lt_api_key: Optional[str] = Form(None),
    top_genres_n: int = Form(3),
    top_places_n: int = Form(5),
    fetch_cover: bool = Form(True, description="Intentar obtener y subir cover de Longitood"),
    # archivo
    pdf: UploadFile = File(None, description="PDF del libro (application/pdf)"),
):
    if not isbn and not title:
        raise HTTPException(status_code=400, detail="Debes enviar 'isbn' o 'title'.")

    # 1) Metadata agregada (Google+OL+Wiki+LT)
    data, info = aggregate_book_api(
        isbn=isbn, title=title, author=author, lang=lang,
        translate_flag=translate, ol_editions_scan=ol_editions_scan,
        lt=lt, lt_api_key=lt_api_key,
    )

    # Determinar ISBN final para claves OBS
    final_isbn = data.get("preferred_isbn13") or (data.get("all_isbn13") or [None])[0] or (isbn if isbn and is_isbn13(isbn) else None)
    if not final_isbn or not is_isbn13(final_isbn):
        raise HTTPException(status_code=422, detail="No se pudo determinar un ISBN-13 válido para OBS/persistencia.")

    obs = {"pdf_key": None, "cover_key": None}

    # 2) PDF -> OBS
    if pdf is not None:
        if pdf.content_type != "application/pdf":
            raise HTTPException(status_code=400, detail="El archivo debe ser PDF (content-type application/pdf).")
        pdf_bytes = await pdf.read()
        pdf_key = f"books/{final_isbn}/original.pdf"
        try:
            upload_bytes(pdf_key, pdf_bytes, content_type="application/pdf")
            obs["pdf_key"] = pdf_key
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Error subiendo PDF a OBS: {e}")

    # 3) Cover (Longitood) -> OBS
    cover_key = None
    if fetch_cover:
        try:
            cover_bytes = fetch_cover_from_longitood_as_jpg(final_isbn)
            cover_key = f"covers/{final_isbn}.jpg"  # clave fija .jpg
            upload_bytes(cover_key, cover_bytes, content_type="image/jpeg")
            obs["cover_key"] = cover_key
        except Exception as e:
            # No romper ingestión completa por fallo de cover
            obs["cover_key"] = None
            info["cover_error"] = f"{type(e).__name__}: {e}"

    # 4) Persistencia (incluye cover_obs_key si lo tenemos)
    db_result = persist_book_record(
        data,
        top_genres_n=top_genres_n,
        top_places_n=top_places_n,
        cover_obs_key=cover_key,
    )

    return {"info": info, "data": data, "db": db_result, "obs": obs}


@app.post("/book/ensure")
async def ensure_book(
    # metadata (Form)
    isbn: Optional[str] = Form(None, description="ISBN-13 si lo tenés"),
    title: Optional[str] = Form(None, description="Título del libro"),
    author: Optional[str] = Form(None, description="Autor (opcional para mejorar el match)"),
    lang: str = Form("es"),
    translate: bool = Form(True),
    ol_editions_scan: int = Form(60),
    lt: bool = Form(True),
    lt_api_key: Optional[str] = Form(None),
    top_genres_n: int = Form(3),
    top_places_n: int = Form(5),
    fetch_cover: bool = Form(True, description="Intentar obtener y subir cover de Longitood"),
    # archivo PDF opcional
    pdf: UploadFile = File(None, description="PDF del libro (application/pdf)"),
):
    """
    1) Si existe en BD -> devolver desde BD (rápido, sin APIs externas).
    2) Si no existe -> hacer ingesta completa (igual que /book/ingest), persistir y devolver.
    Salida: {title, author, isbn, genres, synopsis, characters, places, cover_key}
    """
    # --- Validación mínima ---
    if not isbn and not title:
        raise HTTPException(status_code=400, detail="Debes enviar 'isbn' o 'title'.")

    # --- 1) Chequear en BD primero ---
    if isbn:
        db_row = get_book_info(isbn)
        if db_row:
            return {
                "title": db_row.get("title"),
                "author": (db_row.get("author") or {}).get("name"),
                "isbn": db_row.get("isbn"),
                "genres": db_row.get("genres") or [],
                "synopsis": db_row.get("synopsis") or "",
                "characters": db_row.get("characters") or [],
                "places": db_row.get("places") or [],
                "cover_key": db_row.get("cover_obs_key"),
                "source": "db",
            }

    if (not isbn) and title:
        matches = db_search_books_by_title(title, limit=1)
        if matches:
            only = matches[0]
            db_row = get_book_info(only["isbn"])
            if db_row:
                return {
                    "title": db_row.get("title"),
                    "author": (db_row.get("author") or {}).get("name"),
                    "isbn": db_row.get("isbn"),
                    "genres": db_row.get("genres") or [],
                    "synopsis": db_row.get("synopsis") or "",
                    "characters": db_row.get("characters") or [],
                    "places": db_row.get("places") or [],
                    "cover_key": db_row.get("cover_obs_key"),
                    "source": "db",
                }

    # --- 2) No está en BD: realizar ingesta completa ---
    # Agregar metadatos (Google/OL/Wiki/LT)
    data, info = aggregate_book_api(
        isbn=isbn, title=title, author=author, lang=lang,
        translate_flag=translate, ol_editions_scan=ol_editions_scan,
        lt=lt, lt_api_key=lt_api_key,
    )

    # Resolver ISBN final (incluye fallback a ediciones OL si falta)
    final_isbn = data.get("preferred_isbn13") or (data.get("all_isbn13") or [None])[0] or (isbn if isbn and is_isbn13(isbn) else None)
    if (not final_isbn) and data.get("id"):
        wk = "/works/" + str(data["id"])
        candidates = openlibrary_collect_isbns_from_editions(wk, scan_limit=max(ol_editions_scan, 80))
        if candidates:
            final_isbn = candidates[0]
            data["preferred_isbn13"] = final_isbn
            data["all_isbn13"] = candidates

    if not final_isbn or not is_isbn13(final_isbn):
        raise HTTPException(status_code=422, detail="No se pudo determinar un ISBN-13 válido para ingestar/persistir.")

    # 2.a) Subir PDF (si vino)
    if pdf is not None:
        if pdf.content_type != "application/pdf":
            raise HTTPException(status_code=400, detail="El archivo debe ser PDF (content-type application/pdf).")
        pdf_bytes = await pdf.read()
        pdf_key = f"books/{final_isbn}/original.pdf"
        try:
            upload_bytes(pdf_key, pdf_bytes, content_type="application/pdf")
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Error subiendo PDF a OBS: {e}")

    # 2.b) Cover -> OBS (opcional)
    cover_key = None
    if fetch_cover:
        try:
            cover_bytes = fetch_cover_from_longitood_as_jpg(final_isbn)
            cover_key = f"covers/{final_isbn}.jpg"
            upload_bytes(cover_key, cover_bytes, content_type="image/jpeg")
        except Exception:
            cover_key = None  # no romper si falla

    # 2.c) Persistir todo (incluye cover_obs_key)
    _db = persist_book_record(
        data,
        top_genres_n=top_genres_n,
        top_places_n=top_places_n,
        cover_obs_key=cover_key,
    )

    # 2.d) Armar respuesta reducida
    out_author = None
    if data.get("authors"):
        out_author = (data["authors"][0] or {}).get("name")
    return {
        "title": data.get("title"),
        "author": out_author,
        "isbn": final_isbn,
        "genres": data.get("genres") or [],
        "synopsis": data.get("synopsis") or "",
        "characters": data.get("characters") or [],
        "places": data.get("places") or [],
        "cover_key": cover_key,
        "source": "ingest",
        "info": info,  # por si querés monitorear qué pasó (LT status, etc.)
    }

