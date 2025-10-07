FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UVICORN_HOST=0.0.0.0 \
    UVICORN_PORT=8080 \
    PYTHONUTF8=1

# Paquetes del sistema + CA + netbase + tzdata
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential gcc git libjpeg-dev zlib1g-dev libpq-dev \
      ca-certificates netbase tzdata curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# (opcional) renombrado si aplica
RUN if [ -f /app/main-estable.py ]; then mv /app/main-estable.py /app/main.py || true; fi

RUN adduser --disabled-password --gecos "" appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8080
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
