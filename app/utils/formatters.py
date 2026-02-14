"""
Funcoes de formatacao e normalizacao de dados extraidos.
"""

from __future__ import annotations

import re
from typing import Optional

import pandas as pd

from app.config.settings import logger


# ---------------------------------------------------------------------------
# Schema Mestre - colunas padrao de saida
# ---------------------------------------------------------------------------

MASTER_SCHEMA_COLUMNS: list[str] = [
    "titular",
    "documento_origem",
    "arquivo_origem",
    "obra_referencia",
    "rubrica",
    "periodo",
    "rendimento",
    "percentual_rateio",
    "valor_rateio",
    "correcao",
    "execucoes",
    "categoria",
    "caracteristica",
    "isrc_iswc",
    "data_pagamento",
    "valor_bruto",
    "valor_liquido",
    "tipo_extracao",
]


def normalize_to_master_schema(
    df: pd.DataFrame,
    tipo_extracao: str,
    titular: Optional[str] = None,
    documento_origem: Optional[str] = None,
) -> pd.DataFrame:
    """
    Normaliza um DataFrame extraido para o Schema Mestre.
    Colunas faltantes sao preenchidas com None.
    Colunas extras sao mantidas no final.
    """
    if df.empty:
        logger.warning("DataFrame vazio recebido para normalizacao.")
        return pd.DataFrame(columns=MASTER_SCHEMA_COLUMNS)

    # Adiciona colunas de contexto se nao existirem
    if "tipo_extracao" not in df.columns:
        df["tipo_extracao"] = tipo_extracao
    if "titular" not in df.columns and titular:
        df["titular"] = titular
    if "documento_origem" not in df.columns and documento_origem:
        df["documento_origem"] = documento_origem
    if "arquivo_origem" not in df.columns:
        df["arquivo_origem"] = None

    # Garante que todas as colunas do schema existem
    for col in MASTER_SCHEMA_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Reordena: colunas do schema primeiro, extras depois
    extra_cols = [c for c in df.columns if c not in MASTER_SCHEMA_COLUMNS]
    ordered_cols = MASTER_SCHEMA_COLUMNS + extra_cols
    df = df[[c for c in ordered_cols if c in df.columns]]

    logger.info(
        "DataFrame normalizado: %d linhas, %d colunas, tipo=%s",
        len(df),
        len(df.columns),
        tipo_extracao,
    )

    return df


def format_period(period_str: Optional[str]) -> Optional[str]:
    """Normaliza formato de periodo. Ex: '06/2024 A 08/2024' -> '06/2024 - 08/2024'."""
    if not period_str:
        return None
    return re.sub(r"\s+A\s+", " - ", period_str.strip())


def safe_strip(value: object) -> Optional[str]:
    """Retorna a string limpa ou None."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    return s if s and s not in ("None", "nan", "") else None
