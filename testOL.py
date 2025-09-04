# import requests

# # Endpoint de LibraryThing
# url = "https://www.librarything.com/services/rest/1.1/?method=librarything.ck.getwork&isbn=9788491050759&apikey=b4a490e8886f4dafa809d65749b13bbe"
# params = {
#     "method": "librarything.ck.getwork",
#     "isbn": "9788491050759",
#     "apikey": "b4a490e8886f4dafa809d65749b13bbe"
# }

# headers = {
#     "Cookie": "LTAnonSessionID=2496523620",
#     "Cache-Control": "no-cache",
#     "User-Agent": "PostmanRuntime/7.45.0",
#     "Accept": "*/*",
#     "Accept-Encoding": "gzip, deflate, br",
#     "Connection": "keep-alive"
# }
# try:
#     response = requests.get(url)
#     response.raise_for_status()  # Lanza excepción si el status code no es 200
#     print("Respuesta del servidor:\n")
#     print(response.text)  # Muestra la respuesta cruda (probablemente XML)
# except requests.exceptions.RequestException as e:
#     print(f"Error en la request: {e}")

# =================================================================================================
# import requests

# # OJO: Usá URL base + params (o usa la URL completa y NO pases params)
# url = "https://www.librarything.com/services/rest/1.1/"
# params = {
#     "method": "librarything.ck.getwork",
#     "isbn": "9788491050759",
#     "apikey": "b4a490e8886f4dafa809d65749b13bbe",
# }

# # Copiá los headers de Postman; evitamos 'br' para no requerir la librería brotli
# headers = {
#     "Cookie": "LTAnonSessionID=2496523620",
#     "Cache-Control": "no-cache",
#     "User-Agent": "PostmanRuntime/7.45.0",
#     "Accept": "*/*",
#     "Accept-Encoding": "gzip, deflate",  # quitamos 'br' por compatibilidad
#     "Connection": "keep-alive",
# }

# # Si tenés proxy corporativo, descomentá y ajustá:
# # proxies = {"http": "http://usuario:pass@proxy:8080", "https": "http://usuario:pass@proxy:8080"}

# try:
#     with requests.Session() as s:
#         s.headers.update(headers)
#         # r = s.get(url, params=params, proxies=proxies, timeout=20)  # si usás proxy
#         r = s.get(url, params=params, timeout=20, allow_redirects=True)
#         r.raise_for_status()

#         # Log para entender qué se envió finalmente
#         print("URL final:", r.url)
#         print("Status:", r.status_code)
#         print("Request UA:", r.request.headers.get("User-Agent"))
#         print("Server Content-Encoding:", r.headers.get("Content-Encoding"))

#         # Mostrar respuesta (XML). Si hay caracteres raros, usá .content
#         print(r.text)

# except requests.exceptions.HTTPError as e:
#     print("HTTPError:", e.response.status_code, e.response.text[:500])
# except requests.exceptions.RequestException as e:
#     print("Error de red:", e)

# =================================================================================================

# import requests
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry

# url = "https://www.librarything.com/services/rest/1.1/"
# params = {
#     "method": "librarything.ck.getwork",
#     "isbn": "9788491050759",
#     "apikey": "b4a490e8886f4dafa809d65749b13bbe",
# }

# headers = {
#     # Evitá 'python-requests' y 'br'
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#                   "AppleWebKit/537.36 (KHTML, like Gecko) "
#                   "Chrome/123.0.0.0 Safari/537.36",
#     "Accept": "text/xml,application/xml;q=0.9,*/*;q=0.8",
#     "Accept-Encoding": "gzip, deflate",  # sin 'br'
#     "Connection": "keep-alive",
# }

# session = requests.Session()
# session.headers.update(headers)

# retry = Retry(
#     total=3, backoff_factor=0.5,
#     status_forcelist=(429, 500, 502, 503, 504),
#     allowed_methods=frozenset(["GET"])
# )
# session.mount("https://", HTTPAdapter(max_retries=retry))

# r = session.get(url, params=params, timeout=15)
# print("Status:", r.status_code)
# if r.status_code == 403 and b"Just a moment" in r.content[:200]:
#     print("Bloqueado por Cloudflare (challenge). Pedí whitelist o usa un cliente tipo navegador.")
# else:
#     print(r.text)

# =================================================================================================

# pip install cloudscraper
# import cloudscraper

# scraper = cloudscraper.create_scraper(browser={"browser":"chrome","platform":"windows","mobile":False})
# url = "https://www.librarything.com/services/rest/1.1/"
# params = {
#     "method": "librarything.ck.getwork",
#     "isbn": "9788491050759",
#     "apikey": "b4a490e8886f4dafa809d65749b13bbe",
# }
# r = scraper.get(url, params=params, timeout=20)
# print("Status:", r.status_code)
# print(r.text)

# =================================================================================================

import json
import re
import time
import requests
from urllib.parse import quote

BASE = "https://openlibrary.org"
HEADERS = {"User-Agent": "BookAggregator/1.3 (mailto:you@example.com)"}

# ---------------------------
# Utilidades HTTP / parsing
# ---------------------------

def get_json(url, retries=3, timeout=20):
    last_exc = None
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            time.sleep(1 + i)  # backoff simple
        except Exception as e:
            last_exc = e
            time.sleep(1 + i)
    if last_exc:
        raise last_exc
    raise RuntimeError(f"GET falló: {url}")

def dict_or_str(val):
    if isinstance(val, dict):
        return val.get("value")
    return val

# ---------------------------
# Heurísticas de selección
# ---------------------------

def parse_year_from_any(e):
    """Obtiene el año 'más reciente' que parezca válido para ordenar ediciones."""
    py = e.get("publish_year")
    if isinstance(py, list) and py:
        try:
            return max(int(y) for y in py if isinstance(y, int))
        except Exception:
            pass
    pd = e.get("publish_date") or e.get("publish_date_s")
    years = []
    if isinstance(pd, list):
        for s in pd:
            if isinstance(s, str):
                years += [int(x) for x in re.findall(r"(19|20)\d{2}", s)]
    elif isinstance(pd, str):
        years += [int(x) for x in re.findall(r"(19|20)\d{2}", pd)]
    return max(years) if years else 0

def choose_preferred_edition(editions, prefer_lang="/languages/spa"):
    """Devuelve (best_es, best_any). Elegí best_es si existe; si no, best_any."""
    def is_lang(e, lang_key):
        return any((l.get("key") == lang_key) for l in e.get("languages", []))
    def has_isbn13(e):
        return bool(e.get("isbn_13"))
    ordered = sorted(editions, key=lambda e: (has_isbn13(e), parse_year_from_any(e)), reverse=True)
    es = [e for e in ordered if is_lang(e, prefer_lang)]
    best_es = es[0] if es else None
    best_any = ordered[0] if ordered else None
    return best_es, best_any

def edition_score(e):
    return (1 if e.get("isbn_13") else 0, parse_year_from_any(e))

# ---------------------------
# Endpoints OpenLibrary
# ---------------------------

def fetch_work(work_olid):
    return get_json(f"{BASE}/works/{work_olid}.json")

def fetch_editions(work_olid, limit=200):
    j = get_json(f"{BASE}/works/{work_olid}/editions.json?limit={limit}")
    return j.get("entries", []) or j.get("docs", []) or []

def fetch_author(aid):
    return get_json(f"{BASE}/authors/{aid}.json")

# ---------------------------
# Unión / normalización
# ---------------------------

def union_subjects(*lists):
    s = set()
    for l in lists:
        if not l:
            continue
        for x in l:
            if isinstance(x, str):
                s.add(x.strip())
    return sorted(s)

# Mapeo opcional a español (amplialo a tu taxonomía)
SUBJ_MAP_ES = {
    "fantasy fiction": "Fantasía",
    "epic fantasy": "Fantasía épica",
    "high fantasy": "Alta fantasía",
    "magic": "Magia",
    "life on other planets": "Vida en otros planetas",
    "imaginary wars and battles": "Guerras y batallas imaginarias",
    "fiction": "Ficción",
    "new york times bestseller": "Best seller NYT",
}
def map_subjects_to_es(subjects):
    out = []
    for s in subjects:
        k = s.lower()
        out.append(SUBJ_MAP_ES.get(k, s))
    # dedup manteniendo orden
    seen, res = set(), []
    for x in out:
        if x not in seen:
            seen.add(x)
            res.append(x)
    return res

# ---------------------------
# Selección del "work raíz"
# ---------------------------

def pick_best_work_by_metadata(docs):
    """
    Elige el work 'canónico' entre los docs del search:
    - más subjects (del work.json; si no hay, del doc.subject)
    - mayor edition_count
    - más antiguo (first_publish_year)
    Devuelve (doc_elegido, work_json_elegido)
    """
    best = None
    for d in docs:
        wid = d["key"].split("/")[-1]
        try:
            w = fetch_work(wid)
        except Exception:
            w = {}
        subs = w.get("subjects", []) or []
        doc_subs = d.get("subject", []) or []
        subs_for_score = subs if subs else doc_subs
        score = (
            len(subs_for_score),
            d.get("edition_count", 0),
            -(d.get("first_publish_year") or 10**9),
        )
        if (best is None) or (score > best[0]):
            best = (score, d, w)
    _, doc, work = best
    return doc, work

# ---------------------------
# Título localizado
# ---------------------------

def build_title_from_edition(e):
    """
    Construye un título legible desde la edición:
    usa title [+ ': ' + subtitle] o, si faltan, other_titles/full_title.
    """
    if not e:
        return None
    candidates = []
    t = e.get("title")
    st = e.get("subtitle")
    ft = e.get("full_title")
    if t and st:
        candidates.append(f"{t}: {st}")
    if t:
        candidates.append(t)
    if ft:
        candidates.append(ft)
    # algunas ediciones traen 'other_titles' (lista)
    for ot in (e.get("other_titles") or []):
        if isinstance(ot, str) and ot.strip():
            candidates.append(ot.strip())
    # devuelve el primero no vacío
    for c in candidates:
        if c and c.strip():
            return c.strip()
    return None

def choose_display_title(work, final_pref, prefer_lang="/languages/spa"):
    """
    Si la edición elegida es en español, usamos su título; si no, el del work.
    Devuelve (display_title, original_title)
    """
    title_work = work.get("title")
    is_pref_es = False
    if final_pref:
        langs = final_pref.get("languages", [])
        is_pref_es = any((l.get("key") == prefer_lang) for l in langs)
    if is_pref_es:
        t_edit = build_title_from_edition(final_pref)
        if t_edit:
            return t_edit, title_work
    return title_work, None

# ---------------------------
# Agregador principal
# ---------------------------

def aggregate_book(query_title, author=None, prefer_lang="/languages/spa", verbose=False):
    # 1) search con 'q' para capturar works ES/EN
    q = f"{query_title} {author}" if author else query_title
    search_url = (
        f"{BASE}/search.json?"
        f"q={quote(q)}"
        "&fields=key,title,author_key,author_name,edition_count,first_publish_year,language,subject"
        "&limit=10"
    )
    s = get_json(search_url)
    docs = [d for d in s.get("docs", []) if str(d.get("key", "")).startswith("/works/")]
    if not docs:
        return {"error": "No se encontraron works para la búsqueda."}

    # 2) elegir el work con mejor metadata
    root_doc, work = pick_best_work_by_metadata(docs)
    root_wid = root_doc["key"].split("/")[-1]
    if verbose:
        print(f"[INFO] Work raíz: {root_wid}  (edition_count={root_doc.get('edition_count')}, "
              f"first_publish_year={root_doc.get('first_publish_year')})")

    synopsis = dict_or_str(work.get("description"))
    subjects_root = work.get("subjects", []) or root_doc.get("subject", []) or []

    # 3) Unir subjects de TODOS los works candidatos
    subjects_union = union_subjects(subjects_root)
    for d in docs:
        wid = d["key"].split("/")[-1]
        if wid == root_wid:
            continue
        try:
            w_alt = fetch_work(wid)
            subs_alt = w_alt.get("subjects", []) or d.get("subject", []) or []
            subjects_union = union_subjects(subjects_union, subs_alt)
        except Exception:
            pass

    # 4) Recolectar ediciones de TODOS los works (para ISBN y géneros si faltan)
    all_isbn13 = set()
    all_editions = []
    for d in docs:
        wid = d["key"].split("/")[-1]
        try:
            eds = fetch_editions(wid)
        except Exception:
            eds = []
        all_editions.extend(eds)
        for e in eds:
            for i in (e.get("isbn_13") or []):
                all_isbn13.add(i)
        # sumar subjects de ediciones si todavía no hay géneros
        if not subjects_union:
            subj_from_ed = union_subjects(*[e.get("subjects", []) for e in eds])
            subjects_union = union_subjects(subjects_union, subj_from_ed)

    all_isbn13 = sorted(all_isbn13)

    # 5) Elegir edición preferida: primero español, luego mejor global
    best_es, best_any = choose_preferred_edition(all_editions, prefer_lang=prefer_lang)
    final_pref = best_es or best_any

    # 6) Completar sinopsis, géneros e ISBN a partir de la edición elegida
    preferred_isbn13 = None
    if final_pref:
        if not synopsis:
            synopsis = dict_or_str(final_pref.get("description")) or dict_or_str(final_pref.get("notes"))
        subjects_union = union_subjects(subjects_union, final_pref.get("subjects", []))
        if final_pref.get("isbn_13"):
            preferred_isbn13 = final_pref["isbn_13"][0]
        elif final_pref.get("isbn_10"):
            preferred_isbn13 = final_pref["isbn_10"][0]

    # 7) Autores (del work raíz)
    author_ids = [a["author"]["key"].split("/")[-1] for a in (work.get("authors") or [])]
    authors = []
    for aid in author_ids:
        try:
            a = fetch_author(aid)
            authors.append({
                "id": aid,
                "name": a.get("name"),
                "bio": dict_or_str(a.get("bio"))
            })
        except Exception:
            authors.append({"id": aid, "name": None, "bio": None})

    # 8) Título localizado
    display_title, original_title = choose_display_title(work, final_pref, prefer_lang=prefer_lang)

    # 9) Salida normalizada
    genres_raw = subjects_union
    genres_es = map_subjects_to_es(genres_raw)

    return {
        "id": root_wid,                          # Work OLID canónico
        "title": display_title,                  # preferentemente en español si hay edición ES
        "original_title": original_title,        # título original del work (si difiere)
        "authors": authors,                      # [{id, name, bio}]
        "preferred_isbn13": preferred_isbn13,    # si existe en alguna edición
        "all_isbn13": all_isbn13,                # todos los ISBN-13 encontrados
        "genres": genres_es,                     # mapeados a español
        "genres_raw": genres_raw,                # subjects originales
        "synopsis": synopsis
    }

# ---------------------------
# Prueba rápida
# ---------------------------

if __name__ == "__main__":
    # Prueba con "Palabras Radiantes" de Brandon Sanderson
    data = aggregate_book("Palabras Radiantes", author="", verbose=True)
    print(json.dumps(data, ensure_ascii=False, indent=2))