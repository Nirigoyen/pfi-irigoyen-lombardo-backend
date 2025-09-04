#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
book_fetch.py
-------------
Script para obtener metadatos de un libro (en lo posible en español) desde:
- Google Books (edición, ISBNs, categorías, sinopsis)
- Open Library (Work ID como identificador "global" de obra si hay ISBN)
- Wikipedia (bio resumida del autor en ES; si no hay, intenta EN)

Uso rápido (Windows / Linux / macOS):
    pip install requests
    # Opcional (mejor cuota): setear GOOGLE_BOOKS_API_KEY en tu entorno
    # PowerShell:
    #   $env:GOOGLE_BOOKS_API_KEY="tu_api_key"
    # Linux/macOS:
    #   export GOOGLE_BOOKS_API_KEY="tu_api_key"

    # Búsqueda por ISBN-13
    python book_fetch.py --isbn 9788466657549

    # Búsqueda por título + autor
    python book_fetch.py --title "Words of Radiance" --author "Brandon Sanderson" --lang es
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


GOOGLE_API_BASE = "https://www.googleapis.com/books/v1/volumes"
WIKI_API_SEARCH_ES = "https://es.wikipedia.org/w/api.php"
WIKI_API_SUMMARY_ES = "https://es.wikipedia.org/api/rest_v1/page/summary/"
WIKI_API_SUMMARY_EN = "https://en.wikipedia.org/api/rest_v1/page/summary/"
OPENLIB_ISBN = "https://openlibrary.org/isbn/{}.json"
OPENLIB_WORK = "https://openlibrary.org{}.json"
OPENLIB_WORK_EDITIONS = "https://openlibrary.org{}/editions.json?limit=1"  # size = edition_count


def request_json(url: str, params: Optional[dict] = None, timeout: int = 15) -> Optional[dict]:
    try:
        r = requests.get(url, params=params, timeout=timeout, headers={"User-Agent": "book-fetch/1.0"})
        if r.status_code == 200:
            return r.json()
        else:
            print(f"[WARN] {url} -> HTTP {r.status_code}", file=sys.stderr)
    except requests.RequestException as e:
        print(f"[ERROR] request_json({url}) -> {e}", file=sys.stderr)
    return None


def strip_html(text: str) -> str:
    # Quita etiquetas HTML simples y decodifica entidades
    text = html.unescape(text)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)  # remove tags
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_isbn13(s: str) -> bool:
    s = re.sub(r"[^0-9Xx]", "", s)
    return len(s) == 13 and s.isdigit()


def dedup(seq: List[Any]) -> List[Any]:
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def google_search_by_isbn(isbn: str, lang: str = "es") -> Optional[dict]:
    params = {
        "q": f"isbn:{isbn}",
        "langRestrict": lang,
        "printType": "books",
        "projection": "full",
        "maxResults": 10,
        "fields": "items(id,volumeInfo(title,authors,description,categories,industryIdentifiers,language,canonicalVolumeLink,publishedDate))",
    }
    api_key = os.getenv("GOOGLE_BOOKS_API_KEY")
    if api_key:
        params["key"] = api_key
    return request_json(GOOGLE_API_BASE, params=params)


def google_search_by_title_author(title: str, author: Optional[str], lang: str = "es") -> Optional[dict]:
    q = f'intitle:"{title}"'
    if author:
        q += f' inauthor:"{author}"'
    params = {
        "q": q,
        "langRestrict": lang,
        "printType": "books",
        "projection": "full",
        "maxResults": 20,
        "fields": "items(id,volumeInfo(title,authors,description,categories,industryIdentifiers,language,canonicalVolumeLink,publishedDate))",
    }
    api_key = os.getenv("GOOGLE_BOOKS_API_KEY")
    if api_key:
        params["key"] = api_key
    return request_json(GOOGLE_API_BASE, params=params)


def choose_best_item(items: List[dict], prefer_lang: str = "es") -> Optional[dict]:
    if not items:
        return None
    # Preferimos el primer item con language == prefer_lang
    for it in items:
        vi = it.get("volumeInfo", {})
        if vi.get("language") == prefer_lang:
            return it
    # Si no hay en ES, devolvemos el primero
    return items[0]


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


def map_categories_to_es(categories: Optional[List[str]]) -> Tuple[List[str], List[str]]:
    """
    Toma las categorías crudas (posible inglés) y devuelve (genres_es, genres_raw).
    Aplica un mapeo simple token a token y normaliza.
    """
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
        # Mantener etiquetas especiales sin traducir (nyt:...)
    }

    raw = []
    tokens = []

    for c in categories:
        if not c:
            continue
        raw.append(c)
        # separadores frecuentes
        parts = re.split(r"[\/,;–—-]+", c)
        for p in parts:
            token = p.strip()
            if not token:
                continue
            low = token.lower()
            if low.startswith("nyt:"):
                tokens.append(token)  # mantener
                continue
            # mapeo directo por frase
            if low in mapping:
                tokens.append(mapping[low])
                continue
            # mapeo por palabras (e.g., "Fiction / Fantasy / Epic")
            words = [w.strip() for w in low.split() if w.strip()]
            if "science" in words and "fiction" in words:
                tokens.append(mapping["science fiction"])
                continue
            # por palabra individual
            mapped_any = False
            for key, val in mapping.items():
                if key in low:
                    tokens.append(val)
                    mapped_any = True
                    break
            if not mapped_any:
                # si no pudimos mapear, dejamos el token original (capitalizado)
                tokens.append(token[:1].upper() + token[1:])

    genres_es = dedup(tokens)
    genres_raw = dedup(raw)
    return genres_es, genres_raw


def fetch_wikipedia_bio_es_or_en(name: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Devuelve (bio_text, lang, wiki_url) intentando primero ES y luego EN.
    """
    if not name:
        return None, None, None

    # 1) Buscar en ES
    params = {
        "action": "query",
        "list": "search",
        "srsearch": name,
        "format": "json",
    }
    data = request_json(WIKI_API_SEARCH_ES, params=params)
    if data and data.get("query", {}).get("search"):
        title = data["query"]["search"][0]["title"]
        slug = title.replace(" ", "_")
        es = request_json(WIKI_API_SUMMARY_ES + urllib.parse.quote(slug))
        if es and es.get("extract"):
            return es.get("extract"), "es", f"https://es.wikipedia.org/wiki/{slug}"

    # 2) Intentar con EN directamente (sin buscar)
    slug_en = name.replace(" ", "_")
    en = request_json(WIKI_API_SUMMARY_EN + urllib.parse.quote(slug_en))
    if en and en.get("extract"):
        return en.get("extract"), "en", f"https://en.wikipedia.org/wiki/{slug_en}"

    return None, None, None


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
    """Devuelve (edition_count, first_publish_year) si se pueden estimar rápidamente."""
    edition_count = None
    first_publish_year = None

    # editions.json size = total ediciones
    eds = request_json(OPENLIB_WORK_EDITIONS.format(work_key))
    if eds and isinstance(eds.get("size"), int):
        edition_count = eds["size"]

    # first_publish_year a veces está en el work, o se puede inferir de 'first_publish_date'
    wk = request_json(OPENLIB_WORK.format(work_key))
    if wk:
        # Algunos works tienen 'first_publish_date': "2012"
        fpd = wk.get("first_publish_date")
        if fpd and re.search(r"\d{4}", fpd):
            try:
                first_publish_year = int(re.search(r"(\d{4})", fpd).group(1))
            except Exception:
                pass

    return edition_count, first_publish_year


def build_output_json(
    google_item: dict,
    work_key: Optional[str],
    author_bios: Dict[str, Dict[str, Optional[str]]],
    preferred_isbn13: Optional[str],
) -> Dict[str, Any]:
    vi = google_item.get("volumeInfo", {}) or {}
    title = vi.get("title")
    authors = vi.get("authors") or []
    description = vi.get("description") or ""
    description = strip_html(description)
    categories = vi.get("categories") or []

    genres_es, genres_raw = map_categories_to_es(categories)
    all_isbn13 = extract_isbn13s(google_item)

    # elegir ISBN preferido
    preferred = preferred_isbn13 if (preferred_isbn13 and preferred_isbn13 in all_isbn13) else (all_isbn13[0] if all_isbn13 else preferred_isbn13)

    # armar autores con bio
    out_authors = []
    for a in authors:
        bio = author_bios.get(a, {})
        out_authors.append({
            "id": None,  # podrías mapear a Open Library author o usar un slug de Wikipedia
            "name": a,
            "bio": bio.get("bio"),
            "bio_lang": bio.get("lang"),
            "wikipedia_url": bio.get("url"),
        })

    # id final: si tenemos work, usamos ese como ID global; si no, Google Volume ID
    final_id = work_key.replace("/works/", "") if work_key else f"GB:{google_item.get('id')}"

    out = {
        "id": final_id,
        "title": title,
        "authors": out_authors,
        "preferred_isbn13": preferred,
        "all_isbn13": all_isbn13,
        "genres": genres_es,
        "genres_raw": genres_raw,
        "synopsis": description,
        "google_volume_id": google_item.get("id"),
        "language": vi.get("language"),
        "canonical_volume_link": vi.get("canonicalVolumeLink"),
        "publishedDate": vi.get("publishedDate"),
    }
    return out


def run(isbn: Optional[str], title: Optional[str], author: Optional[str], lang: str = "es") -> None:
    # 1) Buscar en Google Books
    data = None
    prefer_isbn = None

    if isbn:
        data = google_search_by_isbn(isbn, lang=lang)
        prefer_isbn = re.sub(r"[^0-9]", "", isbn)
    elif title:
        data = google_search_by_title_author(title, author, lang=lang)

    if not data or not data.get("items"):
        print("[ERROR] No se encontraron resultados en Google Books.", file=sys.stderr)
        sys.exit(2)

    item = choose_best_item(data["items"], prefer_lang=lang)
    if not item:
        print("[ERROR] No se pudo elegir un item adecuado.", file=sys.stderr)
        sys.exit(2)

    vi = item.get("volumeInfo", {}) or {}

    # 2) Obtener Work de Open Library (si hay algún ISBN-13)
    isbns13 = extract_isbn13s(item)
    work_key = None
    if isbns13:
        # Si el usuario pasó un ISBN, probar ese primero; si no, probar todos hasta encontrar uno
        candidates = [prefer_isbn] + isbns13 if prefer_isbn else isbns13
        for candidate in candidates:
            if candidate and is_isbn13(candidate):
                wk = openlibrary_work_from_isbn(candidate)
                if wk:
                    work_key = wk
                    break

    # 3) Bio de autores (Wikipedia)
    author_bios: Dict[str, Dict[str, Optional[str]]] = {}
    for a in (vi.get("authors") or []):
        bio, bio_lang, wiki_url = fetch_wikipedia_bio_es_or_en(a)
        author_bios[a] = {"bio": bio, "lang": bio_lang, "url": wiki_url}
        time.sleep(0.2)  # ser amable con la API

    # 4) Construir salida
    out = build_output_json(item, work_key, author_bios, prefer_isbn)

    # 5) Imprimir línea [INFO] si tenemos stats del work
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

    # 6) JSON final
    print(json.dumps(out, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description="Obtiene metadatos de libros (ES) usando Google Books + Open Library + Wikipedia.")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--isbn", help="ISBN-13 del libro (recomendado)")
    g.add_argument("--title", help="Título del libro (si no tienes ISBN)")
    parser.add_argument("--author", help="Autor (mejora la precisión si buscas por título)")
    parser.add_argument("--lang", default="es", help="Código de idioma preferido (por defecto: es)")
    args = parser.parse_args()

    run(isbn=args.isbn, title=args.title, author=args.author, lang=args.lang)


if __name__ == "__main__":
    main()
