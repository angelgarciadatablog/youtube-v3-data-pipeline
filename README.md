# YouTube V3 Data Pipeline

Pipeline de datos automatizado que extrae información de un canal de YouTube mediante la **YouTube Data API v3**, la transforma y la carga en **Google BigQuery**. Diseñado para correr en **Google Cloud Functions** con actualizaciones diarias y semanales.

---

## Descripción general

El pipeline recopila métricas e información de un canal de YouTube de forma programada. Los datos se almacenan en BigQuery para su análisis histórico, permitiendo hacer seguimiento del crecimiento del canal, el desempeño de cada video y la evolución de las playlists a lo largo del tiempo.

---

## Arquitectura

```
YouTube Data API v3
        │
        ▼
 YouTubeClient          ← Extracción (scripts/youtube_client.py)
        │
        ▼
  pipeline.py           ← Transformación (scripts/pipeline.py)
        │
        ▼
BigQueryRepository      ← Carga (scripts/bigquery_repository.py)
        │
        ▼
  Google BigQuery
```

El punto de entrada `main.py` expone dos funciones HTTP para Google Cloud Functions:
- `daily()` — se ejecuta una vez al día
- `weekly()` — se ejecuta una vez a la semana

---

## Tablas en BigQuery

### Actualizaciones diarias

| Tabla | Descripción | Estrategia |
|---|---|---|
| `channels_snapshot` | Registro diario de suscriptores, vistas totales y cantidad de videos del canal | Append (idempotente) |
| `latest_videos_current` | Los 5 videos más recientes con sus métricas actuales (vistas, likes, comentarios) | Truncate |

### Actualizaciones semanales

| Tabla | Descripción | Estrategia |
|---|---|---|
| `channels_static` | Metadata del canal: título, descripción, país, thumbnail, URL | Truncate |
| `videos_static` | Todos los videos del canal con título, descripción, duración, categoría, thumbnail | Truncate |
| `playlists_manual_static` | Playlists creadas manualmente en el canal (excluye la playlist de uploads automática) | Truncate |
| `playlist_items_manual_static` | Relación actual entre playlists y videos (qué video pertenece a qué playlist) | Truncate |
| `playlist_items_snapshot` | Foto semanal de la relación playlists ↔ videos (permite ver cambios históricos) | Append (idempotente) |
| `videos_snapshot` | Métricas semanales de **todos** los videos del canal (vistas, likes, comentarios) | Append (idempotente) |

> Las tablas de tipo **Snapshot** usan un patrón delete-before-insert para garantizar idempotencia: si el pipeline se ejecuta más de una vez en la misma semana, no genera duplicados.

---

## Stack tecnológico

| Componente | Tecnología |
|---|---|
| Lenguaje | Python 3.11 |
| Fuente de datos | YouTube Data API v3 |
| Base de datos | Google BigQuery |
| Infraestructura | Google Cloud Functions |
| Procesamiento | pandas, pyarrow |
| Autenticación GCP | google-auth |

---

## Estructura del proyecto

```
youtube-v3-data-pipeline/
├── main.py                      # Entry points para Cloud Functions (daily / weekly)
├── scripts/
│   ├── pipeline.py              # Funciones ETL (8 en total)
│   ├── youtube_client.py        # Wrapper de la YouTube Data API v3
│   └── bigquery_repository.py   # Operaciones de lectura/escritura en BigQuery
├── notebooks/                   # Notebooks de exploración y prototipado (18)
├── requirements.txt             # Dependencias para desarrollo local
├── requirements-prod.txt        # Dependencias mínimas para Cloud Functions
└── .env                         # Variables de entorno (no incluido en el repo)
```

---

## Configuración local

### 1. Clonar el repositorio

```bash
git clone https://github.com/tu-usuario/youtube-v3-data-pipeline.git
cd youtube-v3-data-pipeline
```

### 2. Crear y activar el entorno virtual

```bash
python -m venv venv
source venv/bin/activate        # macOS / Linux
venv\Scripts\activate           # Windows
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Configurar variables de entorno

Crea un archivo `.env` en la raíz del proyecto con el siguiente contenido:

```env
YOUTUBE_API_KEY=tu_api_key_de_youtube
YOUTUBE_CHANNEL_ID=tu_channel_id
GCP_PROJECT=tu_proyecto_de_gcp
```

| Variable | Cómo obtenerla |
|---|---|
| `YOUTUBE_API_KEY` | [Google Cloud Console](https://console.cloud.google.com/) → APIs & Services → Credenciales |
| `YOUTUBE_CHANNEL_ID` | URL de tu canal de YouTube (el string que empieza con `UC`) |
| `GCP_PROJECT` | ID del proyecto en Google Cloud |

### 5. Autenticación con Google Cloud

```bash
gcloud auth application-default login
```

### 6. Ejecutar el pipeline localmente

```bash
python test_pipeline.py
```

O directamente desde Python:

```python
from main import daily, weekly

daily()    # Ejecuta actualizaciones diarias
weekly()   # Ejecuta actualizaciones semanales
```

---

## Despliegue en Google Cloud Functions

### Requisitos previos
- Google Cloud SDK instalado y configurado
- Dataset `angelgarciadatablog` creado en BigQuery

### Desplegar la función `daily`

```bash
gcloud functions deploy daily \
  --runtime python311 \
  --trigger-http \
  --entry-point daily \
  --region us-central1 \
  --set-env-vars YOUTUBE_API_KEY=...,YOUTUBE_CHANNEL_ID=...,GCP_PROJECT=...
```

### Desplegar la función `weekly`

```bash
gcloud functions deploy weekly \
  --runtime python311 \
  --trigger-http \
  --entry-point weekly \
  --region us-central1 \
  --set-env-vars YOUTUBE_API_KEY=...,YOUTUBE_CHANNEL_ID=...,GCP_PROJECT=...
```

### Programar ejecución automática con Cloud Scheduler

```bash
# Ejecución diaria a las 2:00 AM UTC
gcloud scheduler jobs create http daily-pipeline \
  --schedule="0 2 * * *" \
  --uri="https://REGION-PROJECT.cloudfunctions.net/daily" \
  --http-method=GET

# Ejecución semanal los lunes a las 1:30 AM UTC
gcloud scheduler jobs create http weekly-pipeline \
  --schedule="30 1 * * 1" \
  --uri="https://REGION-PROJECT.cloudfunctions.net/weekly" \
  --http-method=GET
```

---

## Preguntas de negocio que este pipeline permite responder

- ¿Cómo ha crecido el número de suscriptores mes a mes?
- ¿Cuáles son los videos con mayor crecimiento de vistas semana a semana?
- ¿Qué videos entraron o salieron de una playlist en las últimas semanas?
- ¿Cuál es la tendencia de engagement (likes / vistas) por video a lo largo del tiempo?
- ¿Qué tan rápido crece un video nuevo en sus primeras semanas?

---

## Autor

**Angel Garcia** — [LinkedIn](https://www.linkedin.com/in/tu-perfil) · [angelgarciadatablog.com](https://angelgarciadatablog.com)
