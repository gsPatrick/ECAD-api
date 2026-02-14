"""
Configuracoes globais do backend.
Define paths de armazenamento, configuracoes de banco de dados e logging.
"""

import logging
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Diretorios
# ---------------------------------------------------------------------------

BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent
UPLOAD_DIR: Path = BASE_DIR / "uploads"
EXPORT_DIR: Path = BASE_DIR / "exports"

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Banco de dados (SQLite para simplicidade)
# ---------------------------------------------------------------------------

DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    f"sqlite:///{BASE_DIR / 'db.sqlite3'}",
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT: str = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

logger: logging.Logger = logging.getLogger("backend")
