"""
ext_routes.py - Endpoints da API de extracao.

Endpoints:
  POST   /api/extractor/upload               - Upload de multiplos PDFs (batch)
  GET    /api/extractor/status/{job_id}       - Status de um job individual
  GET    /api/extractor/batch/{batch_id}      - Status de todos os jobs de um batch
  GET    /api/extractor/data/{job_id}         - Dados extraidos de um job em JSON
  GET    /api/extractor/batch-data/{batch_id} - Dados consolidados de um batch
  GET    /api/extractor/export/{job_id}       - Download do .xlsx individual
  GET    /api/extractor/export-consolidated/{batch_id} - Download do consolidado
  GET    /api/extractor/jobs                  - Lista todos os jobs
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import List, Optional

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    File,
    HTTPException,
    UploadFile,
)
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.config.database import get_db
from app.config.settings import logger
from app.features.extractor.ext_controller import (
    create_job,
    get_all_jobs,
    get_batch_data,
    get_consolidated_export_path,
    get_export_path,
    get_job,
    get_job_data,
    get_jobs_by_batch,
    process_extraction,
    save_uploaded_file,
)

router = APIRouter(prefix="/api/extractor", tags=["Extractor"])


@router.post("/upload")
async def upload_files(
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...),
    db: Session = Depends(get_db),
) -> dict:
    """
    Recebe um ou mais arquivos PDF para extracao.
    Todos os arquivos de um upload compartilham o mesmo batch_id.
    O processamento ocorre em background.
    """
    if not files:
        raise HTTPException(
            status_code=400,
            detail="Nenhum arquivo enviado. Envie pelo menos um PDF.",
        )

    batch_id = str(uuid.uuid4())
    jobs_created: list[dict] = []

    for file in files:
        if not file.filename:
            continue

        # Valida extensao
        if not file.filename.lower().endswith(".pdf"):
            logger.warning("Arquivo ignorado (nao e' PDF): %s", file.filename)
            jobs_created.append(
                {
                    "filename": file.filename,
                    "status": "rejeitado",
                    "detail": "Apenas arquivos PDF sao aceitos.",
                }
            )
            continue

        # Le o conteudo
        content = await file.read()
        if not content:
            jobs_created.append(
                {
                    "filename": file.filename,
                    "status": "rejeitado",
                    "detail": "Arquivo vazio.",
                }
            )
            continue

        # Salva e cria job
        saved_name = save_uploaded_file(content, file.filename)
        job = create_job(db, file.filename, saved_name, batch_id)

        # Agenda processamento em background
        background_tasks.add_task(process_extraction, db, job.id)

        jobs_created.append(
            {
                "job_id": job.id,
                "filename": file.filename,
                "status": "pendente",
            }
        )

    logger.info(
        "Upload processado: %d arquivo(s) recebido(s), batch=%s",
        len(jobs_created),
        batch_id,
    )

    return {
        "message": f"{len(jobs_created)} arquivo(s) recebido(s) para processamento.",
        "batch_id": batch_id,
        "jobs": jobs_created,
    }


@router.get("/status/{job_id}")
async def get_status(
    job_id: str,
    db: Session = Depends(get_db),
) -> dict:
    """Retorna o status atual de um job de extracao."""
    job = get_job(db, job_id)
    if not job:
        raise HTTPException(
            status_code=404,
            detail=f"Job nao encontrado: {job_id}",
        )
    return job.to_dict()


@router.get("/batch/{batch_id}")
async def get_batch_status(
    batch_id: str,
    db: Session = Depends(get_db),
) -> dict:
    """Retorna o status de todos os jobs de um batch."""
    jobs = get_jobs_by_batch(db, batch_id)
    if not jobs:
        raise HTTPException(
            status_code=404,
            detail=f"Batch nao encontrado: {batch_id}",
        )

    total = len(jobs)
    concluidos = sum(1 for j in jobs if j.status == "concluido")
    erros = sum(1 for j in jobs if j.status == "erro")
    processando = sum(1 for j in jobs if j.status == "processando")
    pendentes = sum(1 for j in jobs if j.status == "pendente")

    consolidated_ready = get_consolidated_export_path(batch_id) is not None

    return {
        "batch_id": batch_id,
        "total": total,
        "concluidos": concluidos,
        "erros": erros,
        "processando": processando,
        "pendentes": pendentes,
        "all_done": (concluidos + erros) == total,
        "consolidated_ready": consolidated_ready,
        "jobs": [j.to_dict() for j in jobs],
    }


@router.get("/data/{job_id}")
async def get_data(
    job_id: str,
    db: Session = Depends(get_db),
) -> dict:
    """
    Retorna os dados extraidos em formato JSON.
    So disponivel quando o job esta concluido.
    """
    job = get_job(db, job_id)
    if not job:
        raise HTTPException(
            status_code=404,
            detail=f"Job nao encontrado: {job_id}",
        )

    if job.status == "processando":
        raise HTTPException(
            status_code=202,
            detail="O processamento ainda esta em andamento. Tente novamente em instantes.",
        )

    if job.status == "erro":
        raise HTTPException(
            status_code=500,
            detail=f"Erro durante o processamento: {job.error_message}",
        )

    if job.status == "pendente":
        raise HTTPException(
            status_code=202,
            detail="O job ainda esta pendente de processamento.",
        )

    data = get_job_data(db, job_id)
    if data is None:
        raise HTTPException(
            status_code=500,
            detail="Dados de resultado nao encontrados.",
        )

    return {
        "job_id": job_id,
        "layout": job.layout_identified,
        "total_rows": len(data),
        "data": data,
    }


@router.get("/batch-data/{batch_id}")
async def get_batch_data_endpoint(
    batch_id: str,
    db: Session = Depends(get_db),
) -> dict:
    """Retorna os dados consolidados de todo o batch."""
    jobs = get_jobs_by_batch(db, batch_id)
    if not jobs:
        raise HTTPException(status_code=404, detail=f"Batch nao encontrado: {batch_id}")

    all_done = all(j.status in ("concluido", "erro") for j in jobs)
    if not all_done:
        raise HTTPException(
            status_code=202,
            detail="Alguns jobs ainda estao em processamento.",
        )

    data = get_batch_data(db, batch_id)
    if data is None:
        raise HTTPException(status_code=500, detail="Dados nao encontrados.")

    return {
        "batch_id": batch_id,
        "total_rows": len(data),
        "data": data,
    }


@router.get("/export/{job_id}")
async def export_file(
    job_id: str,
    db: Session = Depends(get_db),
) -> FileResponse:
    """
    Retorna o arquivo .xlsx gerado para download.
    So disponivel quando o job esta concluido.
    """
    job = get_job(db, job_id)
    if not job:
        raise HTTPException(
            status_code=404,
            detail=f"Job nao encontrado: {job_id}",
        )

    if job.status != "concluido":
        raise HTTPException(
            status_code=400,
            detail=f"O job ainda nao foi concluido. Status atual: {job.status}",
        )

    export_path = get_export_path(db, job_id)
    if not export_path or not Path(export_path).exists():
        raise HTTPException(
            status_code=500,
            detail="Arquivo de exportacao nao encontrado.",
        )

    filename = f"extracao_{job.original_filename.replace('.pdf', '')}.xlsx"

    return FileResponse(
        path=export_path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@router.get("/export-consolidated/{batch_id}")
async def export_consolidated(
    batch_id: str,
    titulars: Optional[List[str]] = Depends(lambda titulars: titulars.split(',') if titulars else None),
    db: Session = Depends(get_db),
) -> FileResponse:
    """Download do arquivo consolidado_mestre.xlsx de um batch."""
    if titulars:
        # Geração dinâmica para seleção específica
        path = get_consolidated_export_path(batch_id, titulars=titulars, db=db)
        filename = f"consolidado_selecao_{batch_id[:8]}.xlsx"
    else:
        # Retorno do arquivo já gerado no processamento
        path = get_consolidated_export_path(batch_id)
        filename = "consolidado_mestre.xlsx"

    if not path or not Path(path).exists():
        raise HTTPException(
            status_code=404,
            detail="Arquivo consolidado nao encontrado ou selecao vazia. Verifique se todos os jobs do batch foram concluidos.",
        )

    return FileResponse(
        path=path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@router.get("/jobs")
async def list_jobs(
    db: Session = Depends(get_db),
) -> dict:
    """Lista todos os jobs de extracao."""
    jobs = get_all_jobs(db)
    return {
        "total": len(jobs),
        "jobs": [j.to_dict() for j in jobs],
    }
