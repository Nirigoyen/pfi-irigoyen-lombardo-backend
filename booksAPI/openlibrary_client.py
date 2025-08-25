# app/providers/openlibrary_client.py
from __future__ import annotations
import os, re, requests
from typing import Optional, Dict, Any, List
from olclient.openlibrary import OpenLibrary  # cliente oficial

OL_BASE = "https://openlibrary.org"
COVERS = "https://covers.openlibrary.org"  # no se usa aquí (mantenés longitood)
UA = {"User-Agent": os.getenv("OPENLIBRARY_USER_AGENT", "Livrario/1.0 (+contact)")}
TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SECONDS", "15"))

# ----- helpers ISBN -----
def _clean_isbn(isbn: str) -> str:
    return re.sub(r"[^0-9Xx]", "", isbn).upper()

def _isbn10_to_13(isbn10: str) -> Optional[str]:
    s = _clean_isbn(isbn10)
    if len(s) != 10:
        return None
    core = "978" + s[:-1]
    total = sum((int(d) if i % 2 == 0 else int(d) * 3) for i, d in enumerate(core))
    check = (10 - (total % 10)) % 10
    return core + str(check)

def _isbn13_to_10(isbn13: str) -> Optional[str]:
    s = _clean_isbn(isbn13)
    if len(s) != 13 or not s.startswith("978"):
        return None
    core = s[3:-1]
    total = 0
    for i, ch in enumerate(core, start=1):
        total += int(ch) * (11 - i)
    check = (11 - (total % 11)) % 11
    check_char = "X" if check == 10 else str(check)
    return core + check_char

# ----- llamadas HTTP básicas (Search / ISBN → edición) -----
def _search_by_isbn(isbn: str) -> Optional[dict]:
    r = requests.get(
        f"{OL_BASE}/search.json",
        params={
            "q": f"isbn:{isbn}",
            "fields": "key,title,author_key,author_name,editions,subject_facet",
            "limit": 1,
        },
        headers=UA,
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    docs = (r.json().get("docs") or [])
    return docs[0] if docs else None

def _edition_by_isbn(isbn: str) -> Optional[dict]:
    r = requests.get(f"{OL_BASE}/isbn/{isbn}.json", headers=UA, timeout=TIMEOUT)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()

# ----- fallbacks JSON crudos (robustos) -----
def _get_work_json_safe(work_key: str) -> dict | None:
    """GET /works/{OLID}.json → dict o None si 404/err."""
    wk = work_key if work_key.startswith("/works/") else f"/works/{work_key}"
    try:
        r = requests.get(f"{OL_BASE}{wk}.json", headers=UA, timeout=TIMEOUT)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None

def _get_author_json_safe(author_key: str) -> dict | None:
    """GET /authors/{OLID}.json → dict o None si 404/err."""
    ak = author_key if author_key.startswith("/authors/") else f"/authors/{author_key}"
    try:
        r = requests.get(f"{OL_BASE}{ak}.json", headers=UA, timeout=TIMEOUT)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None

# ----- normalización -----
GENRES_MAP = {
    "Fantasía": ["fantasy", "magic", "sword", "witch", "dragon"],
    "Ciencia ficción": ["science fiction", "sci-fi", "space", "dystop", "cyberpunk", "robots"],
    "Policial/Thriller": ["mystery", "detective", "crime", "thriller", "noir", "suspense"],
    "Romance": ["romance", "love"],
    "Histórica": ["historical", "history"],
    "Terror": ["horror", "supernatural", "ghost", "vampire", "werewolf"],
    "Aventuras": ["adventure", "journey", "quest", "action"],
    "Clásico": ["classic"],
    "Juvenil/YA": ["young adult", "ya", "teen", "juvenile fiction"],
    "Infantil": ["children", "picture book", "juvenile literature"],
    "No ficción": ["nonfiction", "non-fiction", "essay", "memoir", "reportage"],
    "Biografía": ["biography", "autobiography"],
}

def _flat_desc(v) -> Optional[str]:
    if not v:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s or None
    if isinstance(v, dict):
        val = v.get("value")
        if isinstance(val, str):
            val = val.strip()
            return val or None
    return None

def _subjects_to_genres(subjects: List[str], limit: int = 3) -> List[str]:
    scores = {g: 0 for g in GENRES_MAP}
    for s in subjects or []:
        s_l = str(s).lower()
        for g, keys in GENRES_MAP.items():
            if any(k in s_l for k in keys):
                scores[g] += 1
    ranked = sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))
    return [g for g, sc in ranked if sc > 0][:limit]

# ----- función principal (con fallbacks) -----
def fetch_with_olclient(isbn_in: str) -> Dict[str, Any]:
    """
    Retorna:
    {
      "title", "author", "author_description", "synopsis", "genres" (top-3),
      "raw": { "work_olid", "edition_olid", "author_olid" }
    }
    """
    isbn = _clean_isbn(isbn_in)
    isbn10 = _isbn13_to_10(isbn) if len(isbn) == 13 else isbn
    isbn13 = _isbn10_to_13(isbn) if len(isbn) == 10 else isbn

    # 1) Search (rápido)
    work_key = None
    edition_olid = None
    doc = _search_by_isbn(isbn13 or isbn10 or isbn)
    if doc:
        wk = doc.get("key")
        work_key = wk if (wk and wk.startswith("/works/")) else (f"/works/{wk}" if wk else None)
        eds = (((doc or {}).get("editions") or {}).get("docs") or [])
        if eds:
            ed_key = eds[0].get("key")
            if ed_key:
                edition_olid = ed_key.split("/")[-1]

    # 2) Fallback: ISBN → edición → works[0]
    if not work_key:
        ed = _edition_by_isbn(isbn13 or isbn10 or isbn)
        if ed:
            edition_olid = (ed.get("key") or "").split("/")[-1] if ed.get("key") else None
            works = ed.get("works") or []
            if works and isinstance(works[0], dict):
                work_key = works[0].get("key")

    if not work_key:
        raise ValueError("No se encontró Work para el ISBN en Open Library.")

    ol = OpenLibrary()
    work_olid = work_key.split("/")[-1]

    # 3) Intento A: cliente oficial; si falla, Intento B: JSON crudo
    title = synopsis = None
    subjects: List[str] = []
    author_olid = None

    try:
        work_obj = ol.Work.get(work_olid)  # <- puede fallar con algunos registros
        title = getattr(work_obj, "title", None)
        synopsis = _flat_desc(getattr(work_obj, "description", None))
        subjects = list(getattr(work_obj, "subjects", []) or [])
        authors_meta = getattr(work_obj, "authors", None)
        if isinstance(authors_meta, list) and authors_meta:
            maybe = authors_meta[0]
            if isinstance(maybe, dict):
                a = (maybe.get("author") or {})
                if a.get("key"):
                    author_olid = a["key"].split("/")[-1]
    except Exception:
        # Fallback robusto al JSON crudo del work
        wj = _get_work_json_safe(work_key)
        if not wj:
            raise  # si ni el JSON existe, propagamos el error
        title = title or wj.get("title")
        synopsis = synopsis or _flat_desc(wj.get("description"))
        subjects = subjects or list(wj.get("subjects") or [])
        if not author_olid:
            authors = wj.get("authors") or []
            if authors and isinstance(authors[0], dict):
                akey = (authors[0].get("author") or {}).get("key")
                if akey:
                    author_olid = akey.split("/")[-1]

    # 4) Autor: cliente oficial → JSON crudo → Search doc
    author_name = None
    author_bio = None
    if author_olid:
        try:
            author_obj = ol.Author.get(author_olid)
            author_name = getattr(author_obj, "name", None)
            author_bio = _flat_desc(getattr(author_obj, "bio", None))
        except Exception:
            aj = _get_author_json_safe(author_olid)
            if aj:
                author_name = author_name or aj.get("name")
                author_bio = author_bio or _flat_desc(aj.get("bio"))
    if not author_name and doc and doc.get("author_name"):
        author_name = (doc["author_name"][0] if isinstance(doc["author_name"], list) else doc["author_name"])

    # 5) Géneros (top-3) desde subjects del WORK
    genres = _subjects_to_genres([str(s) for s in (subjects or [])], limit=3)

    return {
        "title": title,
        "author": author_name,
        "author_description": author_bio,
        "synopsis": synopsis,
        "genres": genres,
        "raw": {
            "work_olid": work_olid,
            "edition_olid": edition_olid,
            "author_olid": author_olid,
        },
    }
