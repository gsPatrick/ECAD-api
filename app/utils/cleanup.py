"""
Limpeza automatica de disco.
Remove arquivos das pastas /uploads e /exports com mais de 24 horas.
"""

from __future__ import annotations

import time
from pathlib import Path

from app.config.settings import EXPORT_DIR, UPLOAD_DIR, logger

# Idade maxima dos arquivos em segundos (24 horas)
MAX_FILE_AGE_SECONDS: int = 24 * 60 * 60


def cleanup_old_files(
    max_age_seconds: int = MAX_FILE_AGE_SECONDS,
) -> dict[str, int]:
    """
    Remove arquivos mais antigos que max_age_seconds das pastas
    de upload e export.

    Returns:
        Dicionario com contagem de arquivos removidos por diretorio.
    """
    now = time.time()
    result = {"uploads_removed": 0, "exports_removed": 0}

    for label, directory in [("uploads", UPLOAD_DIR), ("exports", EXPORT_DIR)]:
        if not directory.exists():
            continue
        for file_path in directory.iterdir():
            if not file_path.is_file():
                continue
            file_age = now - file_path.stat().st_mtime
            if file_age > max_age_seconds:
                try:
                    file_path.unlink()
                    result[f"{label}_removed"] += 1
                    logger.info(
                        "Arquivo removido (%.1fh): %s",
                        file_age / 3600,
                        file_path.name,
                    )
                except OSError as e:
                    logger.warning(
                        "Falha ao remover arquivo %s: %s",
                        file_path.name,
                        e,
                    )

    total = result["uploads_removed"] + result["exports_removed"]
    if total > 0:
        logger.info(
            "Limpeza concluida: %d uploads e %d exports removidos.",
            result["uploads_removed"],
            result["exports_removed"],
        )
    else:
        logger.debug("Limpeza concluida: nenhum arquivo antigo encontrado.")

    return result
