# Pruebas unitarias para `main-estable.py`

Este directorio contiene pruebas unitarias centradas en el módulo `ingest/main-estable.py`, diseñadas para correr sin dependencias externas (BD/HTTP/FastAPI reales).

## Qué cubren

- Helpers puros: `strip_html`, `is_isbn13`, `dedup`, `extract_isbn13s`, `choose_best_item`, `map_categories_to_es`, `isbn10_to_isbn13`.
- Construcción de salida: `build_output_json` (sin traducción).
- LibraryThing (best‑effort): rutas sin ISBN y sin API key.

Para poder importar el módulo sin instalar librerías externas, los tests crean stubs en tiempo de ejecución para:
- `db` (upserts/attach no‑ops),
- `lt_client` (retorna personajes/lugares vacíos),
- `obs_client` (upload no‑op),
- `fastapi` (stub mínimo para declarar rutas).

## Requisitos

- Python 3.8+ (no se requiere instalar paquetes extra).
- Ejecutar desde la raíz del repo.

## Ejecución con salida “bonita” (ticks/cruces)

El archivo de test incluye un runner con salida enriquecida cuando se ejecuta directamente.

- Windows (PowerShell):
  - `python ingest\pruebas\test_main_estable_unit.py`
- macOS/Linux:
  - `python ingest/pruebas/test_main_estable_unit.py`

Verás líneas como:
- `▶ Running: Clase.metodo`
- `✓ Passed:  Clase.metodo`
- `✗ Failed/Error: Clase.metodo`

## Descubrimiento con `unittest` (sin ticks por defecto)

Si prefieres usar discover estándar:
- `python -m unittest discover -s ingest/pruebas -p "test_main_estable_unit.py"`

Nota: el runner de `unittest` no usa el formato con ticks. Para obtener el formato con ticks usando un comando único, puedes ejecutar el archivo como script usando `runpy`:

- Windows (PowerShell):
  - `python -c "import runpy; runpy.run_path('ingest/pruebas/test_main_estable_unit.py', run_name='__main__')"`
- macOS/Linux:
  - `python -c 'import runpy; runpy.run_path("ingest/pruebas/test_main_estable_unit.py", run_name="__main__")'`

## Notas y limitaciones

- Las pruebas no realizan llamadas de red ni a la base de datos; toda I/O externa está stubbeada.
- Si deseas ejecutar otros tests del repo bajo `tests/`, instala las dependencias correspondientes (por ejemplo, `fastapi`). Estas pruebas de `ingest/pruebas` no lo requieren.
- Si cambias el nombre o ubicación de `main-estable.py`, actualiza la ruta en `spec_from_file_location` dentro del test.

## Archivos

- `test_main_estable_unit.py`: suite de pruebas + runner con ticks/cruces.

