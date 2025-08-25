# LIVRARIO — Plataforma de lectura inmersiva con IA

Este repositorio contiene los servicios y componentes de **Livrario**, una app orientada a mejorar la comprensión, el disfrute y la persistencia en la lectura de novelas para hispanohablantes, con foco en personas con afantasía y/o TDAH, mediante entornos inmersivos y asistencia con IA. :contentReference[oaicite:0]{index=0} :contentReference[oaicite:1]{index=1}

## Visión y objetivos

- **Objetivo general:** promover comprensión, disfrute y persistencia en la lectura a través de una app de RV que represente escenarios y personajes. :contentReference[oaicite:2]{index=2}
- **Objetivos clave:**
  - Hub interactivo que cambia según el **género** (fantasía, ciencia ficción, policial). :contentReference[oaicite:3]{index=3}
  - **Imágenes y videos** de personajes + **escenas 360°** generadas por IA. :contentReference[oaicite:4]{index=4}
  - **Seguimiento del progreso** (porcentaje leído). :contentReference[oaicite:5]{index=5}
  - **Chat inteligente** (RAG) que responde sin spoilers, limitado a lo ya leído. :contentReference[oaicite:6]{index=6}

> En el estado del arte, no hay soluciones que integren **todo**: ambientaciones por género, generación de imágenes contextuales, chat sin spoilers y tracking de progreso en una sola plataforma. :contentReference[oaicite:7]{index=7} :contentReference[oaicite:8]{index=8}

## Alcance (MVP)

- Hub de lectura en RV con ambientación por género (fantasía, ciencia ficción, policial).  
- Generación de imágenes de personajes y escenas 360°.
- Entorno que minimiza distracciones y seguimiento del progreso.  
- Chat inteligente para resúmenes/explicaciones **hasta el punto leído** (sin spoilers).

**Fuera de alcance** (posibles releases futuras): integración con plataformas externas, multijugador, idiomas adicionales, visualizaciones en tiempo real del texto completo, módulos de evaluación formal, etc.


## Stack

- **Backend**: Python + FastAPI
- **Persistencia**: Postgres
- **Object Storage**: Huawei Cloud **OBS**
- **Infra**: Docker / Docker Compose
- **IA**: Ollama (local) para LLM; pipelines de imágenes/skybox



