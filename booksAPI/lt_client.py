# booksAPI/lt_client.py
from __future__ import annotations
import os
from typing import Tuple, List
import cloudscraper
from parsers import parse_librarything_xml

LT_URL = "https://www.librarything.com/services/rest/1.1/"

# Creamos un scraper global para reusar sesión (mantiene cookies/headers)
# Podés ajustar el "browser" si hiciera falta; tu ejemplo usa chrome/windows.
SCRAPER = cloudscraper.create_scraper(
    browser={
        "browser": os.getenv("LT_BROWSER_NAME", "chrome"),
        "platform": os.getenv("LT_BROWSER_PLATFORM", "windows"),
        "mobile": False,
    }
)

def fetch_ck_work_xml(isbn: str, apikey: str, timeout: int = 20) -> bytes:
    """
    Llama a LibraryThing CK con 'apikey' (tal cual te funcionó).
    Ejemplo equivalente:
    https://www.librarything.com/services/rest/1.1/?method=librarything.ck.getwork&isbn=...&apikey=...
    """
    params = {
        "method": "librarything.ck.getwork",
        "isbn": isbn,
        "apikey": apikey,   # <- clave exacta
    }
    r = SCRAPER.get(LT_URL, params=params, timeout=timeout)
    r.raise_for_status()
    # Devuelve XML (bytes)
    return r.content

def get_characters_and_places(isbn: str, apikey: str) -> Tuple[List[str], List[str]]:
    """
    Devuelve (characters, places) parseados desde el XML de LibraryThing.
    Lanza excepción si hay error HTTP/parseo.
    """
    xml_bytes = fetch_ck_work_xml(isbn, apikey)
    characters, places = parse_librarything_xml(xml_bytes)

    # Deduplicar manteniendo orden (por si vinieran repetidos)
    seen = set()
    characters = [c for c in characters if not (c in seen or seen.add(c))]
    seen.clear()
    places = [p for p in places if not (p in seen or seen.add(p))]
    return characters, places

def try_get_characters_and_places(isbn: str, apikey: str) -> Tuple[List[str], List[str], str]:
    """
    Variante 'best-effort': nunca levanta excepción.
    Retorna (characters, places, status_str).
    """
    try:
        chars, places = get_characters_and_places(isbn, apikey)
        return chars, places, "lt_ok"
    except Exception as e:
        # devolvemos estado legible para logs/respuesta
        name = type(e).__name__
        status = getattr(getattr(e, "response", None), "status_code", None)
        return [], [], f"lt_err_{name}{'' if status is None else f'_{status}'}"
