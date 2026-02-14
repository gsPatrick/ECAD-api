"""
ext_controller.py - Logica de processamento de arquivos e resposta.

Coordena o fluxo entre:
- Recebimento de arquivos
- Armazenamento no disco
- Criacao de jobs no banco
- Disparo da extracao em background
- Consolidacao multi-arquivo por batch
- Consulta de status e resultados
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

from app.config.settings import EXPORT_DIR, UPLOAD_DIR, logger
from app.features.extractor.ext_service import (
    export_single_job_to_excel,
    export_to_excel,
    extract_pdf,
    identify_layout,
)
from app.models.job import JobStatus


def create_job(
    db: Session,
    original_filename: str,
    saved_filename: str,
    batch_id: str,
) -> JobStatus:
    """
    Cria um novo registro de job no banco de dados.
    """
    job = JobStatus(
        id=str(uuid.uuid4()),
        batch_id=batch_id,
        filename=saved_filename,
        original_filename=original_filename,
        status="pendente",
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    logger.info(
        "Job criado: id=%s, batch=%s, arquivo=%s", job.id, batch_id, original_filename
    )
    return job


def get_job(db: Session, job_id: str) -> Optional[JobStatus]:
    """Busca um job pelo ID."""
    return db.query(JobStatus).filter(JobStatus.id == job_id).first()


def get_jobs_by_batch(db: Session, batch_id: str) -> list[JobStatus]:
    """Busca todos os jobs de um batch."""
    return (
        db.query(JobStatus)
        .filter(JobStatus.batch_id == batch_id)
        .order_by(JobStatus.created_at.asc())
        .all()
    )


def get_all_jobs(db: Session) -> list[JobStatus]:
    """Retorna todos os jobs ordenados por data de criacao."""
    return (
        db.query(JobStatus)
        .order_by(JobStatus.created_at.desc())
        .all()
    )


def save_uploaded_file(file_content: bytes, original_filename: str) -> str:
    """
    Salva o arquivo enviado no disco.
    Retorna o nome do arquivo salvo (com UUID para evitar colisoes).
    """
    ext = Path(original_filename).suffix or ".pdf"
    safe_name = f"{uuid.uuid4()}{ext}"
    file_path = UPLOAD_DIR / safe_name
    file_path.write_bytes(file_content)
    logger.info("Arquivo salvo: %s -> %s", original_filename, file_path)
    return safe_name


def process_extraction(db: Session, job_id: str) -> None:
    """
    Funcao de processamento executada em background.
    1. Atualiza status para 'processando'.
    2. Identifica layout do PDF.
    3. Extrai dados.
    4. Salva resultado como JSON e Excel.
    5. Atualiza status para 'concluido' ou 'erro'.
    6. Verifica se o batch inteiro esta concluido para gerar consolidado.
    """
    job = get_job(db, job_id)
    if not job:
        logger.error("Job nao encontrado para processamento: %s", job_id)
        return

    pdf_path = str(UPLOAD_DIR / job.filename)

    try:
        # Atualiza para processando
        job.status = "processando"
        db.commit()
        logger.info("Iniciando processamento do job %s: %s", job_id, job.original_filename)

        # Extrai dados
        layout_name, df = extract_pdf(pdf_path, job.original_filename)

        job.layout_identified = layout_name
        job.total_pages = _count_pages(pdf_path)
        job.pages_processed = job.total_pages
        job.progress = 100.0
        db.commit()

        # Salva resultado JSON
        result_json_path = EXPORT_DIR / f"resultado_{job_id}.json"
        df.to_json(str(result_json_path), orient="records", force_ascii=False, indent=2)

        # Salva resultado Excel individual
        excel_path = export_single_job_to_excel(df, job_id)

        job.result_path = excel_path
        job.status = "concluido"
        db.commit()

        logger.info(
            "Job %s concluido com sucesso: %d linhas extraidas, layout=%s",
            job_id,
            len(df),
            layout_name,
        )

        # Tenta gerar consolidado se todos do batch ja concluiram
        _try_generate_consolidated(db, job.batch_id)

    except ValueError as e:
        job.status = "erro"
        job.error_message = str(e)
        db.commit()
        logger.error("Erro de validacao no job %s: %s", job_id, e)

    except Exception as e:
        job.status = "erro"
        job.error_message = f"Erro inesperado durante o processamento: {str(e)}"
        db.commit()
        logger.exception("Erro inesperado no job %s", job_id)


def _try_generate_consolidated(db: Session, batch_id: str) -> None:
    """
    Verifica se todos os jobs do batch estao concluidos (sucesso ou erro).
    Se sim, gera o arquivo consolidado_mestre.xlsx com os jobs que deram certo.
    """
    jobs = get_jobs_by_batch(db, batch_id)
    if not jobs:
        return

    # "all_done" se todos estao em estado terminal
    all_terminal = all(j.status in ("concluido", "erro") for j in jobs)
    if not all_terminal:
        logger.info(
            "Batch %s: aguardando terminacao de todos os jobs (%d/%d concluidos/erros).",
            batch_id,
            sum(1 for j in jobs if j.status in ("concluido", "erro")),
            len(jobs),
        )
        return

    # Coleta DataFrames apenas dos jobs concluidos com sucesso
    dataframes: list[pd.DataFrame] = []
    success_count = 0
    for job in jobs:
        if job.status != "concluido":
            continue
        json_path = EXPORT_DIR / f"resultado_{job.id}.json"
        if json_path.exists():
            try:
                df = pd.read_json(str(json_path))
                if not df.empty:
                    dataframes.append(df)
                    success_count += 1
            except Exception:
                logger.error("Falha ao ler cache JSON para job %s", job.id)

    if not dataframes:
        logger.warning("Batch %s: nenhum dado de sucesso disponivel para consolidacao.", batch_id)
        return

    try:
        consolidated_path = export_to_excel(
            dataframes,
            f"consolidado_mestre_{batch_id}",
            sheet_name="Dados Consolidados",
        )
        logger.info(
            "Arquivo consolidado gerado (sucesso parcial: %d/%d): %s",
            success_count,
            len(jobs),
            consolidated_path,
        )
    except Exception as e:
        logger.exception("Erro ao gerar consolidado para batch %s: %s", batch_id, e)


def get_job_data(db: Session, job_id: str) -> Optional[list[dict]]:
    """
    Retorna os dados extraidos de um job em formato de lista de dicionarios.
    """
    job = get_job(db, job_id)
    if not job or job.status != "concluido":
        return None

    result_json_path = EXPORT_DIR / f"resultado_{job_id}.json"
    if not result_json_path.exists():
        logger.error("Arquivo de resultado nao encontrado: %s", result_json_path)
        return None

    with open(result_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data


def get_batch_data(db: Session, batch_id: str) -> Optional[list[dict]]:
    """Retorna dados consolidados de todos os jobs de um batch."""
    jobs = get_jobs_by_batch(db, batch_id)
    all_data: list[dict] = []
    for job in jobs:
        if job.status != "concluido":
            continue
        data = get_job_data(db, job.id)
        if data:
            all_data.extend(data)
    return all_data if all_data else None


def get_export_path(db: Session, job_id: str) -> Optional[str]:
    """
    Retorna o caminho do arquivo Excel exportado.
    """
    job = get_job(db, job_id)
    if not job or job.status != "concluido":
        return None

    if job.result_path and Path(job.result_path).exists():
        return job.result_path

    return None


def get_consolidated_export_path(batch_id: str) -> Optional[str]:
    """Retorna o caminho do excel consolidado de um batch."""
    path = EXPORT_DIR / f"consolidado_mestre_{batch_id}.xlsx"
    if path.exists():
        return str(path)
    return None


def _count_pages(pdf_path: str) -> int:
    """Conta o numero de paginas de um PDF."""
    import pdfplumber

    with pdfplumber.open(pdf_path) as pdf:
        return len(pdf.pages)
