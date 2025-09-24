# ===== 1) Build stage =====
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Herramientas básicas por si alguna wheel lo requiere
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copiamos solo requirements primero para aprovechar la cache
COPY requirements.txt /app/requirements.txt

# Construimos wheels de todas las dependencias
RUN pip install --upgrade pip && \
    pip wheel --no-cache-dir --no-deps -r requirements.txt -w /wheels


# ===== 2) Runtime stage =====
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UVICORN_HOST=0.0.0.0 \
    UVICORN_PORT=8080 \
    UVICORN_WORKERS=2 \
    TZ=Etc/UTC

# Paquetes mínimos de runtime (TZ y curl para healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata curl \
  && rm -rf /var/lib/apt/lists/*

# Usuario no-root
RUN useradd -ms /bin/bash appuser

WORKDIR /app

# Instalamos las wheels ya construidas
COPY --from=builder /wheels /wheels
RUN pip install --no-cache /wheels/*

# Copiamos el código de la app
COPY main.py /app/main.py

# Permisos
RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 8080

# Healthcheck simple contra /healthz
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8080/healthz || exit 1

# Comando de inicio
CMD ["bash", "-lc", "uvicorn main:app --host ${UVICORN_HOST} --port ${UVICORN_PORT} --workers ${UVICORN_WORKERS}"]


