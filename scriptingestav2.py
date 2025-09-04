#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
book_fetch.py — versión robusta con:
- Búsqueda por título SIN autor (autor opcional), o por ISBN
- Selección de edición con mejores metadatos (prioriza ES con géneros/sinopsis)
- Fallback a Open Library (subjects/description del work)
- Traducción de sinopsis al español (opcional; LibreTranslate)

Uso rápido:
  pip install requests

  # Por título solo (sin autor)
  python book_fetch.py --title "Palabras radiantes" --lang es

  # Título + autor (opcional, mejora precisión)
  python book_fetch.py --title "Words of Radiance" --author "Brandon Sanderson" --lang es

  # Por ISBN
  python book_fetch.py --isbn 9788466657549

  # Traducción activada (default) usando LibreTranslate
  # (setear antes la URL del servicio; ej: https://libretranslate.com/translate)
  export LIBRETRANSLATE_URL="https://libretranslate.com/translate"
  python book_fetch.py --title "Words of Radiance" --lang es --translate

  # Desactivar traducción
  python book_fetch.py --title "Words of Radiance" --lang es --no-translate
"""
from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import requests


# --- Constantes de endpoints ---
GOOGLE_API_BASE = "https://www.googleapis.com/books/v1/volumes"
WIKI_API_SEARCH_ES = "https://es.wikipedia.org/w/api.php"
WIKI_API_SUMMARY_ES = "https://es.wikipedia.org/api/rest_v1/page/summary/"
WIKI_API_SUMMARY_EN = "https://en.wikipedia.org/api/rest_v1/page/summary/"
OPENLIB_ISBN = "https://openlibrary.org/isbn/{}.json"
OPENLIB_WORK = "https://openlibrary.org{}.json"
OPENLIB_WORK_EDITIONS = "https://openlibrary.org{}/editions.json?limit=1"  # 'size' = edition_count
OPENLIB_SEARCH = "https://openlibrary.org/search.json"

# --- Utilidades HTTP ---
def request_json(url: str, params: Optional[dict] = None, timeout: int = 15, method: str = "GET", data: Optional[dict] = None, headers: Optional[dict] = None) -> Optional[dict]:
    try:
        base_headers = {"User-Agent": "book-fetch/1.2"}
        if headers:
            base_headers.update(headers)
        if method.upper() == "GET":
            r = requests.get(url, params=params, timeout=timeout, headers=base_headers)
        else:
            r = requests.post(url, params=params, data=data, timeout=timeout, headers=base_headers)
        if r.status_code == 200:
            return r.json()
        else:
            print(f"[WARN] {method} {url} -> HTTP {r.status_code} | body={r.text[:200]}", file=sys.stderr)
    except requests.RequestException as e:
        print(f"[ERROR] request_json({url}) -> {e}", file=sys.stderr)
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

# --- Google Books ---
def google_search(q: str, lang: Optional[str], max_results: int = 20) -> Optional[dict]:
    params = {
        "q": q,
        "printType": "books",
        "projection": "full",
        "maxResults": max_results,
        "fields": "items(id,volumeInfo(title,authors,description,categories,industryIdentifiers,language,canonicalVolumeLink,publishedDate,subtitle))",
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
    """Devuelve lista combinada de items probando variantes (con y sin lang)."""
    items: List[dict] = []
    tried = []

    # 1) Título entrecomillado + lang
    tried.append((f'intitle:"{title}"', lang))
    data = google_search(f'intitle:"{title}"', lang)
    if data and data.get("items"): items += data["items"]

    # 2) Título sin comillas + lang
    tried.append((f"intitle:{title}", lang))
    data = google_search(f"intitle:{title}", lang)
    if data and data.get("items"): items += data["items"]

    # 3) Igual sin restricción de idioma
    tried.append((f"intitle:{title}", None))
    data = google_search(f"intitle:{title}", None)
    if data and data.get("items"): items += data["items"]

    # Eliminar duplicados por id
    by_id: Dict[str, dict] = {}
    for it in items:
        by_id[it.get("id")] = it
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
    has_desc = bool(vi.get("description"))
    has_cats = bool(vi.get("categories"))
    return has_desc, has_cats

def choose_best_item(items: List[dict], prefer_lang: str = "es", title_query: Optional[str] = None) -> Tuple[Optional[dict], Optional[dict]]:
    """
    Elige:
      - primary: mejor item priorizando ES *y* que tenga sinopsis o categorías.
      - fallback_any: mejor item (cualquier idioma) con sinopsis o categorías (por si primary carece).
    """
    if not items:
        return None, None

    def score(it: dict, hard_require_lang: Optional[str]) -> int:
        vi = it.get("volumeInfo", {}) or {}
        lang = vi.get("language")
        title = (vi.get("title") or "").lower()
        has_desc, has_cats = _has_rich_metadata(it)
        isbn_count = len(extract_isbn13s(it))
        cats_len = len(vi.get("categories") or [])
        score = 0
        # Calidad de metadatos
        if has_desc: score += 6
        if has_cats: score += 5 + min(cats_len, 3)
        if isbn_count: score += min(isbn_count, 2)
        if vi.get("publishedDate"): score += 1
        # Afinidad por idioma
        if lang == prefer_lang: score += 4
        # Afinidad por título
        if title_query:
            tq = title_query.lower()
            if tq == title: score += 3
            elif tq in title: score += 2
        return score

    # primary: prioriza ES pero exige metadatos ricos (desc o cats)
    rich_es = [it for it in items if (it.get("volumeInfo", {}) or {}).get("language") == prefer_lang and any(_has_rich_metadata(it))]
    primary = None
    if rich_es:
        primary = sorted(rich_es, key=lambda it: score(it, prefer_lang), reverse=True)[0]
    else:
        # si no hay ES rico, tomar el mejor *cualquier idioma* que sí tenga data
        rich_any = [it for it in items if any(_has_rich_metadata(it))]
        if rich_any:
            primary = sorted(rich_any, key=lambda it: score(it, None), reverse=True)[0]
        else:
            primary = items[0]

    # fallback_any: por si el primary (si fue ES) tiene huecos
    rich_any = [it for it in items if any(_has_rich_metadata(it))]
    fallback_any = sorted(rich_any, key=lambda it: score(it, None), reverse=True)[0] if rich_any else None

    return primary, fallback_any

# --- Open Library ---
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
    work_key = best.get("key")  # e.g., "/works/OL16813053W"

    # Extraer algunos ISBN-13 candidatos
    isbn13s: List[str] = []
    for d in docs_sorted[:limit]:
        for isbn in d.get("isbn", []) or []:
            s = re.sub(r"[^0-9]", "", str(isbn))
            if is_isbn13(s):
                isbn13s.append(s)
    return work_key, dedup(isbn13s)

def openlibrary_work_from_isbn(isbn13: str) -> Optional[str]:
    url = OPENLIB_ISBN.format(isbn13)
    data = request_json(url)
    if not data:
        return None
    works = data.get("works") or []
    if works:
        key = works[0].get("key")  # e.g., "/works/OL16813053W"
        return key
    return None

def openlibrary_work_stats(work_key: str) -> Tuple[Optional[int], Optional[int]]:
    edition_count = None
    first_publish_year = None

    eds = request_json(OPENLIB_WORK_EDITIONS.format(work_key))
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
    """
    Devuelve (subjects_raw, description_text) del work.
    'description' puede ser str o {value: "..."}.
    """
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

# --- Wikipedia bio ---
def fetch_wikipedia_bio_es_or_en(name: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not name:
        return None, None, None
    # ES search
    params = {"action": "query", "list": "search", "srsearch": name, "format": "json"}
    data = request_json(WIKI_API_SEARCH_ES, params=params)
    if data and data.get("query", {}).get("search"):
        title = data["query"]["search"][0]["title"]
        slug = title.replace(" ", "_")
        es = request_json(WIKI_API_SUMMARY_ES + urllib.parse.quote(slug))
        if es and es.get("extract"):
            return es.get("extract"), "es", f"https://es.wikipedia.org/wiki/{slug}"
    # EN direct
    slug_en = name.replace(" ", "_")
    en = request_json(WIKI_API_SUMMARY_EN + urllib.parse.quote(slug_en))
    if en and en.get("extract"):
        return en.get("extract"), "en", f"https://en.wikipedia.org/wiki/{slug_en}"
    return None, None, None

# --- Mapeo de categorías a ES ---
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
        # etiquetas especiales tipo nyt:... se preservan
    }
    raw = []
    tokens = []
    for c in categories:
        if not c:
            continue
        raw.append(c)
        parts = re.split(r"[\/,;–—-]+", c)
        for p in parts:
            token = p.strip()
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

# --- Traducción (LibreTranslate opcional) ---
def translate_text(text: str, source_lang: Optional[str], target_lang: str = "es") -> Tuple[str, str]:
    """
    Intenta traducir usando LibreTranslate si LIBRETRANSLATE_URL está definido.
    Devuelve (texto_traducido, proveedor_usado|reason).
    Si no puede traducir, devuelve el texto original.
    """
    text = (text or "").strip()
    if not text:
        return text, "empty"

    url = os.getenv("LIBRETRANSLATE_URL")
    api_key = os.getenv("LIBRETRANSLATE_API_KEY")
    if not url:
        return text, "no-provider"

    payload = {
        "q": text,
        "source": (source_lang or "auto"),
        "target": target_lang,
        "format": "text",
    }
    if api_key:
        payload["api_key"] = api_key

    try:
        resp = requests.post(url, data=payload, timeout=20, headers={"Accept": "application/json"})
        if resp.status_code == 200:
            data = resp.json()
            translated = data.get("translatedText") or data.get("translated_text") or text
            return translated, "libretranslate"
        else:
            return text, f"provider-error:{resp.status_code}"
    except Exception as e:
        return text, f"provider-exc:{e}"

# --- Construcción de salida ---
def build_output_json(
    primary_item: dict,
    fallback_item: Optional[dict],
    work_key: Optional[str],
    author_bios: Dict[str, Dict[str, Optional[str]]],
    preferred_isbn13: Optional[str],
    translate: bool = True,
    target_lang: str = "es",
) -> Dict[str, Any]:
    # Tomamos datos del primary
    vi_p = primary_item.get("volumeInfo", {}) or {}
    title = vi_p.get("title")
    authors = vi_p.get("authors") or []
    description = vi_p.get("description") or ""
    lang_desc = vi_p.get("language")
    categories = vi_p.get("categories") or []

    # Si faltan datos, probar con fallback (otra edición)
    if not description and fallback_item:
        vi_f = fallback_item.get("volumeInfo", {}) or {}
        if vi_f.get("description"):
            description = vi_f.get("description")
            lang_desc = vi_f.get("language") or lang_desc
    if not categories and fallback_item:
        vi_f = fallback_item.get("volumeInfo", {}) or {}
        if vi_f.get("categories"):
            categories = vi_f.get("categories")

    # ISBNs
    all_isbn13 = extract_isbn13s(primary_item)
    if fallback_item:
        all_isbn13 = dedup(all_isbn13 + extract_isbn13s(fallback_item))
    preferred = preferred_isbn13 if (preferred_isbn13 and preferred_isbn13 in all_isbn13) else (all_isbn13[0] if all_isbn13 else preferred_isbn13)

    # Bio autores
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

    # ID final
    final_id = work_key.replace("/works/", "") if work_key else f"GB:{primary_item.get('id')}"

    # Sinopsis: limpiar HTML y traducir si corresponde
    synopsis_raw = strip_html(description)
    synopsis_es = synopsis_raw
    synopsis_src_lang = (lang_desc or "").lower() if lang_desc else None
    synopsis_translation_provider = None
    if translate and synopsis_raw and (synopsis_src_lang and synopsis_src_lang != target_lang):
        synopsis_es, synopsis_translation_provider = translate_text(synopsis_raw, source_lang=synopsis_src_lang, target_lang=target_lang)

    # Géneros (map desde categorías Google)
    genres_es, genres_raw = map_categories_to_es(categories)

    out = {
        "id": final_id,
        "title": title,
        "authors": out_authors,
        "preferred_isbn13": preferred,
        "all_isbn13": all_isbn13,
        "genres": genres_es,
        "genres_raw": genres_raw,
        "synopsis": synopsis_es or "",
        "synopsis_source_lang": synopsis_src_lang,
        "synopsis_translation": synopsis_translation_provider,  # 'libretranslate' | 'no-provider' | 'empty' | 'provider-error:...'
        "google_volume_id": primary_item.get("id"),
        "language": vi_p.get("language"),
        "canonical_volume_link": vi_p.get("canonicalVolumeLink"),
        "publishedDate": vi_p.get("publishedDate"),
    }
    return out

def merge_with_openlibrary(out_json: Dict[str, Any], work_key: Optional[str], translate: bool = True, target_lang: str = "es") -> Dict[str, Any]:
    """
    Si faltan géneros/sinopsis, intenta completarlos desde Open Library (work subjects/description).
    """
    if not work_key:
        return out_json

    subjects_raw, ol_desc = openlibrary_work_subjects_and_description(work_key)

    # Completar géneros
    if not out_json.get("genres"):
        genres_es_from_ol, _raw_tmp = map_categories_to_es(subjects_raw)
        out_json["genres"] = dedup(out_json.get("genres", []) + genres_es_from_ol)
    # Guardar subjects crudos también
    if subjects_raw:
        out_json["genres_raw"] = dedup((out_json.get("genres_raw") or []) + subjects_raw)

    # Completar sinopsis
    if (not out_json.get("synopsis")) and ol_desc:
        cleaned = strip_html(ol_desc)
        syn = cleaned
        prov = None
        if translate:
            syn, prov = translate_text(cleaned, source_lang="auto", target_lang=target_lang)
        out_json["synopsis"] = syn
        out_json["synopsis_source_lang"] = out_json.get("synopsis_source_lang") or "unknown"
        out_json["synopsis_translation"] = out_json.get("synopsis_translation") or prov

    return out_json

# --- Flujo principal ---
def run(isbn: Optional[str], title: Optional[str], author: Optional[str], lang: str = "es", translate_flag: bool = True) -> None:
    data_items: List[dict] = []
    prefer_isbn = None
    work_key_hint = None

    if isbn:
        prefer_isbn = re.sub(r"[^0-9]", "", isbn)
        data = google_search_by_isbn(prefer_isbn, lang=lang)
        if data and data.get("items"):
            data_items += data["items"]
        else:
            # Intento sin restricción de idioma por si la edición ES no figura
            data = google_search_by_isbn(prefer_isbn, lang=None)
            if data and data.get("items"):
                data_items += data["items"]
    elif title:
        # 1) Google: varias variantes SOLO con título (autor opcional)
        data = google_search_title_author(title, author, lang=lang)
        if data and data.get("items"):
            data_items += data["items"]
        # variantes
        data_items += google_search_title_variants(title, lang=lang)

        # 2) Si sigue pobre, intentamos Open Library por título para recuperar work/ISBNs
        if not data_items:
            wk, isbn_candidates = openlibrary_search_title(title, prefer_lang=lang, limit=5)
            if wk:
                work_key_hint = wk
            for cand in isbn_candidates:
                d = google_search_by_isbn(cand, lang=lang)
                if d and d.get("items"):
                    data_items += d["items"]
                    prefer_isbn = cand
                    break

    if not data_items:
        print("[ERROR] No se encontraron resultados en Google Books.", file=sys.stderr)
        sys.exit(2)

    # Elegir mejor item y un fallback para completar huecos
    primary, fallback_any = choose_best_item(data_items, prefer_lang=lang, title_query=title)

    if not primary:
        print("[ERROR] No se pudo elegir un item adecuado.", file=sys.stderr)
        sys.exit(2)

    # Calcular work desde ISBNs, o usar hint del search por título
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

    # Construir salida con Google (+fallback edición)
    out = build_output_json(
        primary_item=primary,
        fallback_item=fallback_any,
        work_key=work_key,
        author_bios=author_bios,
        preferred_isbn13=prefer_isbn,
        translate=translate_flag,
        target_lang=lang or "es",
    )

    # Completar con Open Library si faltan géneros/sinopsis
    out = merge_with_openlibrary(out, work_key, translate=translate_flag, target_lang=lang or "es")

    # [INFO] línea de encabezado si hay stats del work
    if work_key:
        ed_count, fpy = openlibrary_work_stats(work_key)
        work_id = work_key.replace("/works/", "")
        info_bits = []
        if ed_count is not None:
            info_bits.append(f"edition_count={ed_count}")
        if fpy is not None:
            info_bits.append(f"first_publish_year={fpy}")
        info_str = "  (" + ", ".join(info_bits) + ")" if info_bits else ""
        print(f"[INFO] Work raíz: {work_id}{info_str}")

    print(json.dumps(out, ensure_ascii=False, indent=2))

def main():
    parser = argparse.ArgumentParser(description="Obtiene metadatos de libros (ES) con Google Books + Open Library + Wikipedia (+traducción opcional).")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--isbn", help="ISBN-13 del libro (recomendado)")
    g.add_argument("--title", help="Título del libro (autor opcional)")
    parser.add_argument("--author", help="Autor (opcional)")
    parser.add_argument("--lang", default="es", help="Idioma preferido (default: es)")
    parser.add_argument("--translate", dest="translate", action="store_true", default=True, help="Traducir sinopsis al idioma destino si no está en ese idioma (default: ON)")
    parser.add_argument("--no-translate", dest="translate", action="store_false", help="Desactivar traducción")
    args = parser.parse_args()

    run(isbn=args.isbn, title=args.title, author=args.author, lang=args.lang, translate_flag=args.translate)

if __name__ == "__main__":
    main()
