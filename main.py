"""
carga-codigo-civil-qro — Microservicio FastAPI para extraer y cargar
el Código Civil del Estado de Querétaro en PostgreSQL + Qdrant.

Replica el patrón del flujo n8n "D26 Cargar CNPCF" como servicio independiente.
"""

import os
import re
import hashlib
import logging
import time
import uuid
from collections import deque
from datetime import datetime
from typing import Optional

import httpx
import pymupdf  # PyMuPDF (fitz)
import psycopg2
from psycopg2.extras import execute_values
from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from pydantic import BaseModel
from qdrant_client import QdrantClient
from qdrant_client.models import (
    VectorParams, Distance, PointStruct,
    models as qdrant_models
)
from openai import OpenAI

# ─── Logging con buffer en memoria ──────────────────────────────────
log_buffer = deque(maxlen=500)  # últimos 500 mensajes

class BufferHandler(logging.Handler):
    def emit(self, record):
        log_buffer.append(self.format(record))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
buffer_handler = BufferHandler()
buffer_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(buffer_handler)

# ─── Config ─────────────────────────────────────────────────────────
API_KEY = os.environ.get("API_KEY", "")
PORT = int(os.environ.get("PORT", "8000"))

PDF_URL = os.environ.get(
    "PDF_URL",
    "https://site.legislaturaqueretaro.gob.mx/CloudPLQ/InvEst/Codigos/COD001_60.pdf"
)

POSTGRES_URL = os.environ.get("DATABASE_URL", "")
QDRANT_URL = os.environ.get("QDRANT_URL", "")
QDRANT_API_KEY = os.environ.get("QDRANT_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "")  # For OpenRouter proxy

QDRANT_COLLECTION = "codigo_civil_queretaro"
PG_TABLE = "d26_codigo_civil_qro"
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMS = 1536
BATCH_SIZE = 50

ORDENAMIENTO = "Código Civil del Estado de Querétaro"
FUENTE = "Legislatura de Querétaro"

# ─── App ────────────────────────────────────────────────────────────
app = FastAPI(
    title="Carga Código Civil QRO",
    description="Extrae el Código Civil de Querétaro (PDF), lo chunka por artículo y lo carga en Postgres + Qdrant.",
    version="1.0.0",
)

# ─── Estado global del proceso ──────────────────────────────────────
process_status = {
    "running": False,
    "last_run": None,
    "total_chunks": 0,
    "chunks_processed": 0,
    "phase": "idle",
    "error": None,
    "job_id": None,
}


# ─── Auth ───────────────────────────────────────────────────────────
def verify_api_key(x_api_key: str = Header(default="")):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")


# ─── Models ─────────────────────────────────────────────────────────
class CargarRequest(BaseModel):
    pdf_url: Optional[str] = None  # override de la URL si se desea


class ChunkResult(BaseModel):
    libro: Optional[str]
    titulo: Optional[str]
    capitulo: Optional[str]
    seccion: Optional[str]
    articulo: str
    fraccion: Optional[str]
    tipo: str
    texto: str
    ordenamiento: str
    fuente: str
    orden: int
    hash_contenido: str


# ═══════════════════════════════════════════════════════════════════
#  PASO 1: Descargar PDF
# ═══════════════════════════════════════════════════════════════════
def descargar_pdf(url: str) -> bytes:
    logger.info(f"Descargando PDF desde: {url}")
    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()
    logger.info(f"PDF descargado: {len(resp.content)} bytes")
    return resp.content


# ═══════════════════════════════════════════════════════════════════
#  PASO 2: Extraer texto del PDF
# ═══════════════════════════════════════════════════════════════════
def extraer_texto(pdf_bytes: bytes) -> str:
    logger.info("Extrayendo texto del PDF...")
    doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    text_parts = []
    for page_num, page in enumerate(doc, 1):
        page_text = page.get_text()
        text_parts.append(f"\n<<<PAGE_{page_num}>>>\n")
        text_parts.append(page_text)
    doc.close()
    full_text = "".join(text_parts)
    logger.info(f"Texto extraído: {len(full_text)} caracteres, {page_num} páginas")
    return full_text


# ═══════════════════════════════════════════════════════════════════
#  PASO 3: Chunking inteligente por artículo
#  Replica la lógica del nodo "Chunking Inteligente" del flujo CNPCF
# ═══════════════════════════════════════════════════════════════════
def limpiar_texto(text: str) -> str:
    """Limpia headers/footers repetitivos del PDF."""
    # Remover encabezados/pies de página comunes del PDF de Querétaro
    text = re.sub(r'C[ÓO]DIGO\s+CIVIL\s+DEL?\s+ESTADO\s+DE\s+QUER[ÉE]TARO\n?', '', text, flags=re.IGNORECASE)
    text = re.sub(r'Poder\s+Legislativo\s+(del?\s+Estado\s+de\s+Quer[ée]taro)?\n?', '', text, flags=re.IGNORECASE)
    text = re.sub(r'Legislatura\s+del?\s+Estado\s+de\s+Quer[ée]taro\n?', '', text, flags=re.IGNORECASE)
    text = re.sub(r'P[áa]gina\s+\d+\s+de\s+\d+\n?', '', text, flags=re.IGNORECASE)
    # Remover números de página sueltos (una línea que es solo un número)
    text = re.sub(r'^\d{1,4}\s*$', '', text, flags=re.MULTILINE)
    # Limpiar caracteres no imprimibles (preservando español)
    text = re.sub(r'[^\x20-\x7E\náéíóúüñÁÉÍÓÚÜÑ¿¡°«»—–\n\r\t]', '', text)
    # Normalizar saltos de línea excesivos
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text


def chunking_inteligente(raw_text: str) -> list[dict]:
    """
    Parsea el texto del Código Civil de Querétaro y genera chunks
    por artículo completo con contexto jerárquico.
    """
    text = limpiar_texto(raw_text)

    # Patrones de detección
    patron_libro = re.compile(r'^Libro\s+(\w[\s\w]*)', re.IGNORECASE)
    patron_titulo = re.compile(r'^T[ií]tulo\s+(\w[\s\w]*)', re.IGNORECASE)
    patron_capitulo = re.compile(r'^Cap[ií]tulo\s+(\w[\s\w]*)', re.IGNORECASE)
    patron_seccion = re.compile(r'^Secci[oó]n\s+(\w[\s\w]*)', re.IGNORECASE)
    patron_articulo = re.compile(r'^Art[ií]culo\s+(\d+)\s*[\.\-]+\s*(.*)', re.IGNORECASE)
    patron_fraccion_roman = re.compile(r'^([IVXLCDM]+)\.\s+(.*)')
    patron_fraccion_arab = re.compile(r'^(\d{1,3})\.\s+(.*)')
    patron_page = re.compile(r'^<<<PAGE_(\d+)>>>$')

    # Estado del parser
    jerarquia = {"libro": None, "titulo": None, "capitulo": None, "seccion": None}
    articulo_actual = None
    jerarquia_articulo = None
    lineas_articulo = []
    fraccion_count = 0
    current_page = 1
    chunks = []

    def build_context_prefix(jer: dict) -> str:
        parts = [ORDENAMIENTO]
        if jer.get("libro"):
            parts.append(f"Libro {jer['libro']}")
        if jer.get("titulo"):
            parts.append(f"Título {jer['titulo']}")
        if jer.get("capitulo"):
            parts.append(f"Capítulo {jer['capitulo']}")
        if jer.get("seccion"):
            parts.append(f"Sección {jer['seccion']}")
        return " — ".join(parts) + ".\n\n"

    def guardar_articulo():
        nonlocal articulo_actual, lineas_articulo, fraccion_count
        if articulo_actual is None or not lineas_articulo:
            return
        prefix = build_context_prefix(jerarquia_articulo)
        texto_completo = prefix + "\n".join(lineas_articulo).strip()

        # Hash determinista para idempotencia
        hash_input = f"{articulo_actual}|{ORDENAMIENTO}|{texto_completo[:200]}"
        hash_val = hashlib.md5(hash_input.encode()).hexdigest()[:12]
        hash_contenido = f"{hash_val}_art{articulo_actual}_completo"

        chunks.append({
            **jerarquia_articulo,
            "articulo": articulo_actual,
            "fraccion": None,
            "tipo": "articulo",
            "texto": texto_completo,
            "ordenamiento": ORDENAMIENTO,
            "fuente": FUENTE,
            "orden": len(chunks) + 1,
            "hash_contenido": hash_contenido,
            "total_fracciones": fraccion_count,
        })

    # Parsear línea por línea
    lineas = text.split("\n")
    for linea_raw in lineas:
        linea = linea_raw.strip()

        # Track page markers
        page_match = patron_page.match(linea)
        if page_match:
            current_page = int(page_match.group(1))
            continue

        if not linea:
            continue

        # Libro
        match = patron_libro.match(linea)
        if match:
            guardar_articulo()
            articulo_actual = None
            lineas_articulo = []
            fraccion_count = 0
            jerarquia = {
                "libro": match.group(1).strip(),
                "titulo": None,
                "capitulo": None,
                "seccion": None,
            }
            continue

        # Título
        match = patron_titulo.match(linea)
        if match:
            guardar_articulo()
            articulo_actual = None
            lineas_articulo = []
            fraccion_count = 0
            jerarquia["titulo"] = match.group(1).strip()
            jerarquia["capitulo"] = None
            jerarquia["seccion"] = None
            continue

        # Capítulo
        match = patron_capitulo.match(linea)
        if match:
            guardar_articulo()
            articulo_actual = None
            lineas_articulo = []
            fraccion_count = 0
            jerarquia["capitulo"] = match.group(1).strip()
            jerarquia["seccion"] = None
            continue

        # Sección
        match = patron_seccion.match(linea)
        if match:
            guardar_articulo()
            articulo_actual = None
            lineas_articulo = []
            fraccion_count = 0
            jerarquia["seccion"] = match.group(1).strip()
            continue

        # Artículo
        match = patron_articulo.match(linea)
        if match:
            guardar_articulo()
            articulo_actual = match.group(1)
            jerarquia_articulo = dict(jerarquia)
            lineas_articulo = [linea_raw]
            fraccion_count = 0
            continue

        # Dentro de un artículo: acumular
        if articulo_actual is not None:
            lineas_articulo.append(linea_raw)
            if patron_fraccion_roman.match(linea) or patron_fraccion_arab.match(linea):
                fraccion_count += 1

    # Guardar último artículo pendiente
    guardar_articulo()

    logger.info(f"Chunking completado: {len(chunks)} chunks generados")
    return chunks


# ═══════════════════════════════════════════════════════════════════
#  PASO 4: Crear tabla en PostgreSQL
# ═══════════════════════════════════════════════════════════════════
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {PG_TABLE} (
    id              SERIAL PRIMARY KEY,
    libro           TEXT,
    titulo          TEXT,
    capitulo        TEXT,
    seccion         TEXT,
    articulo        TEXT,
    fraccion        TEXT,
    tipo            TEXT,
    texto           TEXT,
    ordenamiento    TEXT DEFAULT '{ORDENAMIENTO}',
    fuente          TEXT DEFAULT '{FUENTE}',
    orden           INTEGER,
    hash_contenido  TEXT UNIQUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ccqro_articulo ON {PG_TABLE} (articulo);
CREATE INDEX IF NOT EXISTS idx_ccqro_tipo ON {PG_TABLE} (tipo);
CREATE INDEX IF NOT EXISTS idx_ccqro_libro ON {PG_TABLE} (libro);
"""

INSERT_SQL = f"""
INSERT INTO {PG_TABLE}
    (libro, titulo, capitulo, seccion, articulo, fraccion, tipo, texto, ordenamiento, fuente, orden, hash_contenido)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (hash_contenido)
DO UPDATE SET texto = EXCLUDED.texto, orden = EXCLUDED.orden;
"""


def get_pg_connection():
    return psycopg2.connect(POSTGRES_URL)


def crear_tabla_pg():
    logger.info("Creando tabla en PostgreSQL si no existe...")
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        logger.info(f"Tabla {PG_TABLE} lista")
    finally:
        conn.close()


def insertar_chunks_pg(chunks: list[dict]):
    logger.info(f"Insertando {len(chunks)} chunks en PostgreSQL...")
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            for chunk in chunks:
                cur.execute(INSERT_SQL, (
                    chunk.get("libro"),
                    chunk.get("titulo"),
                    chunk.get("capitulo"),
                    chunk.get("seccion"),
                    chunk.get("articulo"),
                    chunk.get("fraccion"),
                    chunk.get("tipo"),
                    chunk.get("texto"),
                    chunk.get("ordenamiento"),
                    chunk.get("fuente"),
                    chunk.get("orden"),
                    chunk.get("hash_contenido"),
                ))
        conn.commit()
        logger.info("Insert en PostgreSQL completado")
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
#  PASO 5: Generar embeddings con OpenAI
# ═══════════════════════════════════════════════════════════════════
def generar_embeddings(textos: list[str]) -> list[list[float]]:
    client_kwargs = {"api_key": OPENAI_API_KEY}
    if OPENAI_BASE_URL:
        client_kwargs["base_url"] = OPENAI_BASE_URL
    client = OpenAI(**client_kwargs)
    embeddings = []
    # Procesar en sub-batches de 100 (límite de API)
    for i in range(0, len(textos), 100):
        batch = textos[i:i + 100]
        response = client.embeddings.create(
            input=batch,
            model=EMBEDDING_MODEL,
        )
        for item in response.data:
            embeddings.append(item.embedding)
        logger.info(f"  Embeddings: {len(embeddings)}/{len(textos)}")
    return embeddings


# ═══════════════════════════════════════════════════════════════════
#  PASO 6: Cargar en Qdrant
# ═══════════════════════════════════════════════════════════════════
def get_qdrant_client() -> QdrantClient:
    if QDRANT_API_KEY:
        return QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
    return QdrantClient(url=QDRANT_URL)


def crear_coleccion_qdrant():
    client = get_qdrant_client()
    collections = [c.name for c in client.get_collections().collections]
    if QDRANT_COLLECTION not in collections:
        logger.info(f"Creando colección Qdrant: {QDRANT_COLLECTION}")
        client.create_collection(
            collection_name=QDRANT_COLLECTION,
            vectors_config=VectorParams(
                size=EMBEDDING_DIMS,
                distance=Distance.COSINE,
            ),
        )
    else:
        logger.info(f"Colección {QDRANT_COLLECTION} ya existe")


def cargar_qdrant(chunks: list[dict], embeddings: list[list[float]]):
    client = get_qdrant_client()
    points = []
    for chunk, embedding in zip(chunks, embeddings):
        # ID determinista para upsert
        point_id = f"CCQRO-{chunk['articulo']}-{chunk.get('fraccion') or 'base'}-{chunk['tipo']}"
        # Usar UUID determinista basado en el ID string
        deterministic_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, point_id))

        payload = {
            "articulo": chunk.get("articulo"),
            "texto": chunk.get("texto"),
            "libro": chunk.get("libro"),
            "titulo": chunk.get("titulo"),
            "capitulo": chunk.get("capitulo"),
            "seccion": chunk.get("seccion"),
            "ordenamiento": chunk.get("ordenamiento"),
            "fuente": chunk.get("fuente"),
            "tipo": chunk.get("tipo"),
            "orden": chunk.get("orden"),
            "hash_contenido": chunk.get("hash_contenido"),
        }

        points.append(PointStruct(
            id=deterministic_uuid,
            vector=embedding,
            payload=payload,
        ))

    # Upsert en batches
    for i in range(0, len(points), BATCH_SIZE):
        batch = points[i:i + BATCH_SIZE]
        client.upsert(collection_name=QDRANT_COLLECTION, points=batch)
        logger.info(f"  Qdrant upsert: {min(i + BATCH_SIZE, len(points))}/{len(points)}")


# ═══════════════════════════════════════════════════════════════════
#  Pipeline completo (ejecutado en background)
# ═══════════════════════════════════════════════════════════════════
def ejecutar_pipeline(pdf_url: str, job_id: str):
    global process_status
    process_status["running"] = True
    process_status["error"] = None
    process_status["job_id"] = job_id

    try:
        # Paso 1: Descargar PDF
        process_status["phase"] = "descargando_pdf"
        pdf_bytes = descargar_pdf(pdf_url)

        # Paso 2: Extraer texto
        process_status["phase"] = "extrayendo_texto"
        raw_text = extraer_texto(pdf_bytes)

        # Paso 3: Chunking
        process_status["phase"] = "chunking"
        chunks = chunking_inteligente(raw_text)
        process_status["total_chunks"] = len(chunks)

        if not chunks:
            raise ValueError("No se generaron chunks. Verifica el PDF.")

        # Paso 4: Crear tabla PostgreSQL
        process_status["phase"] = "creando_tabla_pg"
        crear_tabla_pg()

        # Paso 5: Insertar en PostgreSQL
        process_status["phase"] = "insertando_postgres"
        insertar_chunks_pg(chunks)
        logger.info(f"{len(chunks)} chunks insertados en PostgreSQL")

        # Paso 6: Crear colección Qdrant
        process_status["phase"] = "creando_coleccion_qdrant"
        crear_coleccion_qdrant()

        # Paso 7: Generar embeddings + cargar en Qdrant (en batches)
        process_status["phase"] = "generando_embeddings_y_qdrant"
        total = len(chunks)
        for i in range(0, total, BATCH_SIZE):
            batch_chunks = chunks[i:i + BATCH_SIZE]
            textos = [c["texto"] for c in batch_chunks]

            embeddings = generar_embeddings(textos)
            cargar_qdrant(batch_chunks, embeddings)

            process_status["chunks_processed"] = min(i + BATCH_SIZE, total)
            logger.info(f"Progreso: {process_status['chunks_processed']}/{total}")

            # Pausa para no saturar APIs
            time.sleep(0.5)

        process_status["phase"] = "completado"
        process_status["last_run"] = datetime.now().isoformat()
        logger.info("═══ Pipeline completado exitosamente ═══")

    except Exception as e:
        process_status["phase"] = "error"
        process_status["error"] = str(e)
        logger.error(f"Error en pipeline: {e}", exc_info=True)

    finally:
        process_status["running"] = False


# ═══════════════════════════════════════════════════════════════════
#  ENDPOINTS
# ═══════════════════════════════════════════════════════════════════

@app.get("/health")
def health():
    """Health check — sin autenticación."""
    return {
        "status": "ok",
        "service": "carga-codigo-civil-qro",
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.post("/cargar", dependencies=[])
def cargar(req: CargarRequest, background_tasks: BackgroundTasks, x_api_key: str = Header(default="")):
    """Inicia el pipeline completo de carga del Código Civil QRO."""
    verify_api_key(x_api_key)

    if process_status["running"]:
        raise HTTPException(status_code=409, detail="Ya hay un proceso de carga en ejecución")

    url = req.pdf_url or PDF_URL
    job_id = str(uuid.uuid4())[:8]

    # Reset status
    process_status["chunks_processed"] = 0
    process_status["total_chunks"] = 0

    background_tasks.add_task(ejecutar_pipeline, url, job_id)

    return {
        "message": "Pipeline de carga iniciado en background",
        "job_id": job_id,
        "pdf_url": url,
        "status_endpoint": "/status",
    }


@app.get("/status")
def status(x_api_key: str = Header(default="")):
    """Estado actual del proceso de carga."""
    verify_api_key(x_api_key)
    return process_status


@app.get("/stats")
def stats(x_api_key: str = Header(default="")):
    """Estadísticas de la tabla y colección."""
    verify_api_key(x_api_key)

    result = {"postgres": {}, "qdrant": {}}

    # Postgres stats
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {PG_TABLE}")
            result["postgres"]["total_rows"] = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(DISTINCT articulo) FROM {PG_TABLE}")
            result["postgres"]["unique_articles"] = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(DISTINCT libro) FROM {PG_TABLE}")
            result["postgres"]["unique_books"] = cur.fetchone()[0]
        conn.close()
    except Exception as e:
        result["postgres"]["error"] = str(e)

    # Qdrant stats
    try:
        client = get_qdrant_client()
        info = client.get_collection(QDRANT_COLLECTION)
        result["qdrant"]["points_count"] = info.points_count
        result["qdrant"]["vectors_count"] = info.vectors_count
    except Exception as e:
        result["qdrant"]["error"] = str(e)

    return result


@app.get("/diag")
def diagnostics(x_api_key: str = Header(default="")):
    """Diagnóstico de conexiones individuales."""
    verify_api_key(x_api_key)
    results = {}

    # Test PostgreSQL
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        results["postgres"] = {"status": "ok"}
    except Exception as e:
        results["postgres"] = {"status": "error", "detail": str(e)}

    # Test Qdrant
    try:
        client = get_qdrant_client()
        cols = client.get_collections()
        results["qdrant"] = {"status": "ok", "collections": [c.name for c in cols.collections]}
    except Exception as e:
        results["qdrant"] = {"status": "error", "detail": str(e)}

    # Test OpenAI/OpenRouter
    try:
        client_kwargs = {"api_key": OPENAI_API_KEY}
        if OPENAI_BASE_URL:
            client_kwargs["base_url"] = OPENAI_BASE_URL
        client = OpenAI(**client_kwargs)
        resp = client.embeddings.create(input=["test"], model=EMBEDDING_MODEL)
        results["openai_embeddings"] = {"status": "ok", "dims": len(resp.data[0].embedding)}
    except Exception as e:
        results["openai_embeddings"] = {"status": "error", "detail": str(e)}

    # Test PDF URL
    try:
        with httpx.Client(timeout=15.0, follow_redirects=True) as client:
            resp = client.head(PDF_URL)
        results["pdf_url"] = {"status": "ok", "http_code": resp.status_code}
    except Exception as e:
        results["pdf_url"] = {"status": "error", "detail": str(e)}

    return results


@app.get("/logs")
def get_logs(x_api_key: str = Header(default=""), lines: int = 100):
    """Retorna los últimos N mensajes de log."""
    verify_api_key(x_api_key)
    return {"logs": list(log_buffer)[-lines:]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
