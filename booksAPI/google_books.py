import os, requests
from typing import Tuple, List, Optional, Dict, Any

GB_BASE = "https://www.googleapis.com/books/v1/volumes"
UA = {"User-Agent": os.getenv("OPENLIBRARY_USER_AGENT", "Livrario/1.0 (you@domain)")}
TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SECONDS", "15"))
GB_KEY = os.getenv("GOOGLE_BOOKS_API_KEY")  # opcional

def _gb_call(params: dict) -> Optional[Dict[str, Any]]:
    r = requests.get(GB_BASE, params=params, headers=UA, timeout=TIMEOUT)
    r.raise_for_status()
    items = (r.json() or {}).get("items") or []
    return items[0] if items else None

def gb_by_isbn_es(isbn: str) -> Optional[Dict[str, Any]]:
    base = {
        "q": f"isbn:{isbn}",
        "langRestrict": "es",
        "printType": "books",
        "maxResults": 5,
    }
    # 1) con key si existe
    if GB_KEY:
        try:
            return _gb_call({**base, "key": GB_KEY})
        except requests.HTTPError as e:
            # reintento sin key si es 403 (restricción/referrer)
            if getattr(e.response, "status_code", None) == 403:
                pass
            else:
                raise
    # 2) sin key (cuota más baja, pero sirve para pruebas)
    return _gb_call(base)

def gb_pick_fields(item: Dict[str, Any]) -> Tuple[Optional[str], List[str], Optional[str]]:
    vi = (item or {}).get("volumeInfo") or {}
    desc = vi.get("description")
    cats = vi.get("categories") or []
    title_es = vi.get("title")
    return desc, cats, title_es
