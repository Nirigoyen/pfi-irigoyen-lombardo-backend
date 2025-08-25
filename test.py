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
import cloudscraper

scraper = cloudscraper.create_scraper(browser={"browser":"chrome","platform":"windows","mobile":False})
url = "https://www.librarything.com/services/rest/1.1/"
params = {
    "method": "librarything.ck.getwork",
    "isbn": "9788491050759",
    "apikey": "b4a490e8886f4dafa809d65749b13bbe",
}
r = scraper.get(url, params=params, timeout=20)
print("Status:", r.status_code)
print(r.text)

