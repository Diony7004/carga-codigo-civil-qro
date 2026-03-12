# carga-codigo-civil-qro API

## URL interna (desde n8n)
`http://carga-codigo-civil-qro:8000`

## Autenticación
Header: `x-api-key: {API_KEY configurada en Easypanel}`

## Endpoints

### GET /health
Sin autenticación.
Retorna: `{"status": "ok", "service": "carga-codigo-civil-qro"}`

### POST /cargar
Requiere `x-api-key`.
Inicia el pipeline completo en background:
1. Descarga PDF del Código Civil QRO
2. Extrae texto
3. Chunking inteligente por artículo
4. Crea tabla en PostgreSQL (`d26_codigo_civil_qro`)
5. Inserta chunks en PostgreSQL (con idempotencia)
6. Crea colección Qdrant (`codigo_civil_queretaro`)
7. Genera embeddings (text-embedding-3-small)
8. Upsert en Qdrant

**Request body (opcional):**
```json
{
  "pdf_url": "https://site.legislaturaqueretaro.gob.mx/CloudPLQ/InvEst/Codigos/COD001_60.pdf"
}
```
Si no se envía `pdf_url`, usa la URL por defecto configurada.

**Response:**
```json
{
  "message": "Pipeline de carga iniciado en background",
  "job_id": "a1b2c3d4",
  "pdf_url": "https://...",
  "status_endpoint": "/status"
}
```

### GET /status
Requiere `x-api-key`.
Retorna el estado actual del proceso de carga.
```json
{
  "running": true,
  "phase": "generando_embeddings_y_qdrant",
  "total_chunks": 2543,
  "chunks_processed": 1500,
  "error": null,
  "job_id": "a1b2c3d4"
}
```

Fases posibles:
- `idle` → `descargando_pdf` → `extrayendo_texto` → `chunking` → `creando_tabla_pg` 
- → `insertando_postgres` → `creando_coleccion_qdrant` → `generando_embeddings_y_qdrant` → `completado`
- `error` (con detalle en campo `error`)

### GET /stats
Requiere `x-api-key`.
Estadísticas de PostgreSQL y Qdrant.

## Configuración n8n — Nodo HTTP Request

### Iniciar carga
- **Method**: POST
- **URL**: `http://carga-codigo-civil-qro:8000/cargar`
- **Authentication**: Header Auth
  - **Name**: `x-api-key`
  - **Value**: `{API_KEY}`
- **Body Type**: JSON
- **Body**: `{}` (vacío para usar URL por defecto)

### Consultar progreso
- **Method**: GET
- **URL**: `http://carga-codigo-civil-qro:8000/status`
- **Authentication**: Header Auth como arriba

## Ejemplo con curl (desde red Docker)
```bash
# Iniciar carga
curl -X POST \
  -H "x-api-key: {API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{}' \
  http://carga-codigo-civil-qro:8000/cargar

# Ver progreso
curl -H "x-api-key: {API_KEY}" \
  http://carga-codigo-civil-qro:8000/status

# Estadísticas
curl -H "x-api-key: {API_KEY}" \
  http://carga-codigo-civil-qro:8000/stats
```

## Variables de entorno requeridas
| Variable | Descripción |
|---|---|
| `API_KEY` | Token de autenticación del servicio |
| `PORT` | Puerto (default: 8000) |
| `DATABASE_URL` | Connection string PostgreSQL |
| `QDRANT_URL` | URL del servicio Qdrant |
| `QDRANT_API_KEY` | API key de Qdrant (opcional) |
| `OPENAI_API_KEY` | Key de OpenAI para embeddings |
| `PDF_URL` | URL del PDF (tiene default) |
