# ---- Runtime base
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Necesario para instalar openlibrary-client desde GitHub (si lo mantenés)
RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

# Carpeta de trabajo base
WORKDIR /app

# Instala dependencias primero (mejor cache)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# Copia tu código (carpeta booksAPI)
COPY booksAPI/ ./booksAPI

# Cambiamos el WD al paquete de la API
WORKDIR /app/booksAPI

# Usuario no-root
RUN adduser --disabled-password --gecos "" appuser \
 && chown -R appuser:appuser /app
USER appuser

EXPOSE 8080

# Arranque del API (ahora testfinal.py vive dentro de booksAPI/)
# Si tu archivo se llama testfinal.py y define `app = FastAPI(...)`, es:
CMD ["uvicorn", "testfinal:app", "--host", "0.0.0.0", "--port", "8080"]
