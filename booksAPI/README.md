# Books Ingest Service (microservicio de ingesta)

Servicio **FastAPI** que recibe un **PDF** de libro junto con su **ISBN**, consulta fuentes externas para obtener **personajes** y **lugares** (LibraryThing), descarga la **portada** y la **sube a Huawei Cloud OBS**, y persiste todo en una base **SQL**. Deja espacios **TODO** para completar más adelante título, autor, sinopsis y géneros desde otra API bibliográfica.

> Este servicio implementa la primera etapa del pipeline de Livrario: **ingesta + metadatos**. Prepara datos para los siguientes módulos (embeddings, RAG y generación de imágenes/skybox).

---

## Índice

* [Arquitectura breve](#arquitectura-breve)
* [Endpoints](#endpoints)
* [Esquema de base de datos](#esquema-de-base-de-datos)
* [Variables de entorno](#variables-de-entorno)
* [Instalación y ejecución](#instalación-y-ejecución)
* [Pruebas rápidas](#pruebas-rápidas)
* [Detalles de integración](#detalles-de-integración)
* [Errores comunes](#errores-comunes)
* [TODO / Próximos pasos](#todo--próximos-pasos)

---

## Arquitectura breve

* **FastAPI** expone endpoints REST.
* **Requests** consulta:

  * **LibraryThing** (`librarything.ck.getwork`) para **personajes** (`characternames`) y **lugares** (`placesmentioned`).
  * **(TODO)** API bibliográfica externa para **título, autor, sinopsis, géneros**.
* **OBS (Huawei Cloud)**: se sube la **portada** (`covers/{ISBN}.jpg`) y el **PDF** (`books/{ISBN}/original.pdf`).
* **SQL (Postgres)**: persistencia normalizada de libro, autor, personajes, lugares y géneros.

```
PDF + ISBN ──► /ingest ──► LibraryThing (XML) ──► parseo personajes/lugares
                      └─► cover.longitood.com ──► descarga cover ─► OBS
                      └─► SQL upsert ─► books / authors / characters / places / genres
                      └─► TODO: completar título/autor/sinopsis/géneros desde otra API
```

---

## Endpoints

### 1) `POST /ingest`

**Multipart form-data**

* `isbn` *(string, requerido)*
* `libr_api_key` *(string, requerido)* — API key de **LibraryThing**
* `pdf` *(archivo, requerido)* — PDF del libro (`content-type: application/pdf`)
* `force_title` *(string, opcional)* — solo para pruebas hasta integrar API bibliográfica
* `force_author` *(string, opcional)* — idem

**Qué hace**

1. Sube el **PDF** a OBS → `books/{isbn}/original.pdf`.
2. Llama a **LibraryThing** y parsea `characternames` (personajes) y `placesmentioned` (lugares).
3. Descarga la **portada** desde `https://bookcover.longitood.com/bookcover/{isbn}` y la sube a OBS → `covers/{isbn}.jpg`.
4. Inserta/actualiza el **libro** en SQL y relaciona **personajes** y **lugares** (top 5).
5. Deja **TODO** para título/autor/sinopsis/géneros (o toma `force_*`).

**200 OK (JSON)**

```json
{
  "isbn": "9788491050219",
  "pdf_obs_key": "books/9788491050219/original.pdf",
  "cover_obs_key": "covers/9788491050219.jpg",
  "characters_inserted": 8,
  "places_inserted": 5,
  "genres_inserted": 0,
  "todos": {
    "title": "TODO desde otra API (o usar force_title)",
    "author": "TODO desde otra API (o usar force_author)",
    "author_description": "TODO desde otra API",
    "synopsis": "TODO desde otra API",
    "genres": "TODO desde otra API (top 3)"
  }
}
```

**4xx/5xx**: ver [Errores comunes](#errores-comunes).

---

### 2) `GET /books/{isbn}/author`

Devuelve **autor** y su **descripción** (si existe) asociados al ISBN.

**200 OK (JSON)**

```json
{
  "isbn": "9788491050219",
  "author": {
    "id": 12,
    "name": "Nombre del Autor",
    "description": "Bio del autor (si está disponible)"
  }
}
```

**404** si no hay autor asociado aún.

---

### 3) `GET /books/{isbn}/cover`

Devuelve la **portada** desde OBS como **bytes** (`image/jpeg`).

**200 OK (image/jpeg)**: cuerpo binario de la imagen.

**404** si no hay `cover_obs_key` o si falla la descarga desde OBS.

---

### 4) `GET /books/{isbn}`

Devuelve la **ficha completa** del libro.

**200 OK (JSON)**

```json
{
  "isbn": "9788491050219",
  "title": "(puede venir de force_title o TODO API)",
  "synopsis": null,
  "cover_obs_key": "covers/9788491050219.jpg",
  "author": { "id": 12, "name": "Autor", "description": null },
  "genres": [],
  "places": ["Lugar 1", "Lugar 2", "..."],
  "characters": ["Personaje 1", "Personaje 2", "..."]
}
```

**404** si el libro no existe.

---

## Esquema de base de datos

Tablas principales (resumen):

* `authors(id PK, name UNIQUE, description)`
* `books(isbn PK, title, author_id FK→authors, synopsis, cover_obs_key, created_at, updated_at)`
* `characters(id PK, name UNIQUE)`
* `book_characters(isbn FK→books, character_id FK→characters, PK compuesta)`
* `places(id PK, name UNIQUE)`
* `book_places(isbn FK→books, place_id FK→places, ord, PK compuesta)`
* `book_genres(isbn FK→books, genre, ord, PK compuesta)`

> Dimensiones con `UNIQUE (name)` para no duplicar entidades entre libros.

---

## Variables de entorno

| Variable       | Descripción                                                        |
| -------------- | ------------------------------------------------------------------ |
| `DATABASE_URL` | Cadena de conexión SQL (ej. `postgresql://user:pass@host:5432/db`) |
| `OBS_AK`       | Access Key de Huawei **OBS**                                       |
| `OBS_SK`       | Secret Key de Huawei **OBS**                                       |
| `OBS_SERVER`   | Endpoint regional OBS (ej. `obs.la-south-2.myhuaweicloud.com`)     |
| `OBS_BUCKET`   | Nombre del bucket OBS (debe existir)                               |

> La **API key de LibraryThing** se envía como campo `libr_api_key` en el `POST /ingest`.

---

## Instalación y ejecución

### Opción A — Local (sin Docker)

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt

export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/booksdb
export OBS_AK=...
export OBS_SK=...
export OBS_SERVER=obs.la-south-2.myhuaweicloud.com
export OBS_BUCKET=books

uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

### Opción B — Docker

Incluye `Dockerfile`. Si usás Compose, recordá inyectar las variables de entorno anteriores y levantar una base Postgres.

---

## Pruebas rápidas

### Ingesta

```bash
curl -X POST http://localhost:8080/ingest \
  -F "isbn=9788491050219" \
  -F "libr_api_key=TU_API_KEY_LT" \
  -F "pdf=@/ruta/al/libro.pdf;type=application/pdf" \
  -F "force_title=Titulo provisional" \
  -F "force_author=Autor provisional"
```

### Autor

```bash
curl http://localhost:8080/books/9788491050219/author
```

### Cover (descarga imagen)

```bash
curl -o cover.jpg http://localhost:8080/books/9788491050219/cover
```

### Ficha completa del libro

```bash
curl http://localhost:8080/books/9788491050219
```

---

## Detalles de integración

* **LibraryThing**: se consulta `method=librarything.ck.getwork&isbn={ISBN}&apikey={KEY}` y se parsean los **XML** con `ElementTree`, extrayendo las secciones:

  * `<field name="characternames"> ... <factList><fact>Nombre</fact> ...` → **personajes**
  * `<field name="placesmentioned"> ... <factList><fact>Lugar</fact> ...` → **lugares** (se guardan **hasta 5** en orden)
* **Cover**: se obtiene de `https://bookcover.longitood.com/bookcover/{ISBN}`, se guarda en OBS como `covers/{ISBN}.jpg`.
* **OBS SDK** (`esdk-obs-python`): se usa `putContent` y `getObject(loadStreamInMemory=True)`.
* **TODO API bibliográfica**:

  * completar **título**, **autor**, **bio de autor**, **sinopsis** y **géneros** (hasta 3) y actualizar tablas.

---

## Errores comunes

* **400**: el archivo no es PDF, o faltan campos requeridos.
* **404**: ISBN inexistente en base, `cover_obs_key` ausente o fallo al recuperar desde OBS.
* **502**: error HTTP al consultar servicios externos (LibraryThing o cover).
* **500**: error interno (DB/OBS/parseo). Revisar logs de FastAPI.

---

## TODO / Próximos pasos

* Integrar **Open Library / Google Books / ISBNdb** para completar *título/autor/sinopsis/géneros*.
* Normalizar **géneros** y limitar a **top 3** coherentes.
* Validación de **ISBN** (ISBN-10/ISBN-13) y manejo de duplicados.
* Tests unitarios e integración (mocks para LibraryThing/OBS).
* Endpoint `POST /books/{isbn}/refresh-metadata` para re-hidratar datos.
