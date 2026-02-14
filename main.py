"""
main.py - Ponto de entrada da aplicacao FastAPI.

Inicializa o banco de dados, configura CORS, registra routers e
agenda limpeza automatica de disco.
"""

import asyncio

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config.database import init_db
from app.config.settings import logger
from app.features.extractor.ext_routes import router as extractor_router
from app.utils.cleanup import cleanup_old_files

# ---------------------------------------------------------------------------
# Inicializacao
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Sistema de Extracao Inteligente de PDFs",
    description=(
        "API para extracao, normalizacao e conversao de relatorios musicais "
        "em PDF para planilhas Excel padronizadas."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    root_path="/apidois",
)

# ---------------------------------------------------------------------------
# CORS - Permite qualquer origem (ajustar em producao)
# ---------------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(extractor_router)

# ---------------------------------------------------------------------------
# Eventos de ciclo de vida
# ---------------------------------------------------------------------------


async def _periodic_cleanup(interval_seconds: int = 3600) -> None:
    """Executa limpeza de disco periodicamente."""
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            cleanup_old_files()
        except Exception:
            logger.exception("Erro durante limpeza periodica de disco.")


@app.on_event("startup")
async def startup_event() -> None:
    """Executado ao iniciar a aplicacao."""
    logger.info("Inicializando banco de dados...")
    init_db()
    logger.info("Banco de dados inicializado com sucesso.")

    # Executa limpeza inicial
    cleanup_old_files()

    # Agenda limpeza periodica (a cada 1 hora)
    asyncio.create_task(_periodic_cleanup())

    logger.info("Aplicacao iniciada - API disponivel em /docs")


@app.get("/", tags=["Health"])
async def health_check() -> dict:
    """Endpoint de verificacao de saude da API."""
    return {
        "status": "online",
        "service": "Sistema de Extracao Inteligente de PDFs",
        "version": "1.0.0",
    }
