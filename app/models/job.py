"""
Modelo SQLAlchemy para o controle de status dos jobs de extracao.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Float, Integer, String, Text

from app.config.database import Base


class JobStatus(Base):
    """Representa um job de extracao de PDF."""

    __tablename__ = "job_status"

    id: str = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    batch_id: str = Column(String(36), nullable=False, index=True)
    filename: str = Column(String(512), nullable=False)
    original_filename: str = Column(String(512), nullable=False)
    status: str = Column(String(32), nullable=False, default="pendente")
    layout_identified: str = Column(String(256), nullable=True)
    total_pages: int = Column(Integer, nullable=True)
    pages_processed: int = Column(Integer, default=0)
    progress: float = Column(Float, default=0.0)
    error_message: str = Column(Text, nullable=True)
    result_path: str = Column(String(1024), nullable=True)
    created_at: datetime = Column(
        DateTime, default=lambda: datetime.now(timezone.utc)
    )
    updated_at: datetime = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    def to_dict(self) -> dict:
        """Serializa o job para um dicionario."""
        return {
            "id": self.id,
            "batch_id": self.batch_id,
            "filename": self.original_filename,
            "status": self.status,
            "layout_identified": self.layout_identified,
            "total_pages": self.total_pages,
            "pages_processed": self.pages_processed,
            "progress": self.progress,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
