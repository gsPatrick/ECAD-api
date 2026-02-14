"""
ext_service.py - O "Cerebro" do sistema de extracao.

Responsavel por:
1. Identificar o layout/modelo do PDF com base em assinaturas digitais.
2. Extrair dados tabulares usando pdfplumber.
3. Implementar Forward Fill para cabecalhos de secao (obras/artistas em destaque).
4. Normalizar os dados extraidos para o Schema Mestre.
5. Exportar os dados consolidados para Excel (.xlsx).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd
import pdfplumber
from openpyxl.styles import Font

from app.config.settings import EXPORT_DIR, logger
from app.utils.formatters import (
    MASTER_SCHEMA_COLUMNS,
    format_period,
    normalize_to_master_schema,
    safe_strip,
)
from app.utils.pdf_parser import (
    clean_monetary_value,
    clean_text,
    extract_first_page_text,
    extract_header_info,
    extract_isrc_from_line,
    extract_isrc_from_window,
    extract_page_lines,
)


# ==========================================================================
# 1. ASSINATURAS DIGITAIS - Identificacao de Layout
# ==========================================================================

@dataclass
class LayoutSignature:
    """Define a assinatura digital de um modelo de PDF."""

    name: str
    keywords: list[str]
    priority: int = 0  # Maior prioridade = verificado primeiro


# A ordem importa: assinaturas mais especificas primeiro para evitar falsos positivos.
# "DEMONSTRATIVO DO TITULAR" e' substring de varios outros, entao fica por ultimo.
LAYOUT_SIGNATURES: list[LayoutSignature] = [
    LayoutSignature(
        name="RELATORIO_ANALITICO_AUTORAL",
        keywords=[
            "RELATÓRIO ANALÍTICO DE TITULAR AUTORAL",
            "SUAS OBRAS",
        ],
        priority=100,
    ),
    LayoutSignature(
        name="RELATORIO_ANALITICO_CONEXO",
        keywords=[
            "RELATÓRIO ANALÍTICO DE TITULAR CONEXO",
            "SUAS GRAVAÇÕES",
        ],
        priority=100,
    ),
    LayoutSignature(
        name="DEMONSTRATIVO_SERVICOS_DIGITAIS",
        keywords=[
            "DEMONSTRATIVO DO TITULAR DE SERVIÇOS",
            "DIGITAIS",
        ],
        priority=90,
    ),
    LayoutSignature(
        name="DEMONSTRATIVO_ANTECIPACAO_PRESCRITOS",
        keywords=[
            "DEMONSTRATIVO DO TITULAR",
            "ANTECIPAÇÃO DE PRESCRITOS",
        ],
        priority=80,
    ),
    LayoutSignature(
        name="DISTRIBUICAO_PRESCRITIVEIS",
        keywords=[
            "DEMONSTRATIVO DO TITULAR",
            "DISTRIBUIÇÃO DE PRESCRITÍVEIS",
        ],
        priority=70,
    ),
    LayoutSignature(
        name="DEMONSTRATIVO_TITULAR",
        keywords=[
            "DEMONSTRATIVO DO TITULAR",
            "EXECUÇÃO PÚBLICA",
        ],
        priority=10,
    ),
]


def identify_layout(pdf_path: str) -> Optional[str]:
    """
    Identifica o layout do PDF com base nas assinaturas digitais.
    Retorna o nome do layout ou None se nao identificado.
    """
    first_page_text = extract_first_page_text(pdf_path)
    if not first_page_text:
        logger.error("Nao foi possivel extrair texto da primeira pagina: %s", pdf_path)
        return None

    text_upper = first_page_text.upper()

    # Ordena por prioridade decrescente
    sorted_signatures = sorted(
        LAYOUT_SIGNATURES, key=lambda s: s.priority, reverse=True
    )

    for sig in sorted_signatures:
        all_found = all(kw.upper() in text_upper for kw in sig.keywords)
        if all_found:
            logger.info(
                "Layout identificado: '%s' para arquivo: %s", sig.name, pdf_path
            )
            return sig.name

    logger.warning("Layout nao identificado para o arquivo: %s", pdf_path)
    return None


# ==========================================================================
# 2. EXTRATORES ESPECIFICOS POR LAYOUT
# ==========================================================================

def _extract_demonstrativo_titular(pdf_path: str, original_filename: str) -> pd.DataFrame:
    """Extrai dados do layout 'DEMONSTRATIVO DO TITULAR'."""
    logger.info("Extraindo DEMONSTRATIVO_TITULAR: %s", pdf_path)
    return _extract_demonstrativo_padrao(pdf_path, "DEMONSTRATIVO_TITULAR", original_filename)


def _extract_distribuicao_prescritiveis(pdf_path: str, original_filename: str) -> pd.DataFrame:
    """Extrai dados do layout 'DISTRIBUICAO DE PRESCRITIVEIS'."""
    logger.info("Extraindo DISTRIBUICAO_PRESCRITIVEIS: %s", pdf_path)
    return _extract_demonstrativo_padrao(pdf_path, "DISTRIBUICAO_PRESCRITIVEIS", original_filename)


def _extract_antecipacao_prescritos(pdf_path: str, original_filename: str) -> pd.DataFrame:
    """Extrai dados do layout 'DEMONSTRATIVO ANTECIPACAO DE PRESCRITOS'."""
    logger.info("Extraindo DEMONSTRATIVO_ANTECIPACAO_PRESCRITOS: %s", pdf_path)
    return _extract_demonstrativo_padrao(pdf_path, "DEMONSTRATIVO_ANTECIPACAO_PRESCRITOS", original_filename)


def _extract_servicos_digitais(pdf_path: str, original_filename: str) -> pd.DataFrame:
    """Extrai dados do layout 'DEMONSTRATIVO DE SERVICOS DIGITAIS'."""
    logger.info("Extraindo DEMONSTRATIVO_SERVICOS_DIGITAIS: %s", pdf_path)
    return _extract_demonstrativo_padrao(pdf_path, "DEMONSTRATIVO_SERVICOS_DIGITAIS", original_filename)


def _extract_demonstrativo_padrao(pdf_path: str, tipo: str, original_filename: str) -> pd.DataFrame:
    """
    Extrator generico para layouts tipo 'demonstrativo'.
    Todos os 4 demonstrativos possuem estrutura similar:
    - Cabecalho com titular, CPF, numero, etc.
    - Linha com codigo da obra (numerico) + titulo + valor total parcial
    - Linhas de dados com artista/rubrica/periodo/valores
    - Linhas ISRC opcionais
    
    Forward Fill: quando uma linha comeca com numero (codigo da obra),
    esse valor vira o 'obra_referencia' de todas as linhas subsequentes
    ate o proximo codigo de obra.
    """
    rows: list[dict] = []
    header_info = extract_header_info(extract_first_page_text(pdf_path))
    titular = header_info.get("titular")
    periodo_dist = header_info.get("periodo_distribuicao")
    valor_total = header_info.get("valor_total")

    current_obra_codigo: Optional[str] = None
    current_obra_titulo: Optional[str] = None
    current_isrc: Optional[str] = None

    # Regex para detectar linha de cabecalho de obra (codigo numerico no inicio)
    re_obra_header = re.compile(r"^(\d{5,})\s+(.+?)(?:\s+[\d.,]+\s*---|\s*$)")
    # Regex para detectar linha de detalhe de dados (Geral)
    re_data_line = re.compile(
        r"^(.+?)\s+"               # artista/gravacao (grupo 1)
        r"([A-Z]{2,}(?:\s+[A-Z0-9\-/]+)*?)\s+"  # rubrica (grupo 2 - ex: SDI, AP-SHOW, etc)
        r"(\d{2}/\d{4}(?:\s+A\s+\d{2}/\d{4})?)\s+"  # periodo (grupo 3)
        r"([\d.,\-]+)\s+"            # rendimento (grupo 4)
        r"([\d.,\-]+)\s+"            # % rateio (grupo 5)
        r"([\d.,\-]+)\s+"            # valor rateio (grupo 6)
        r"(---)\s+"                # correcao (grupo 7)
        r"(.+?)\s+"                # execucoes (grupo 8)
        r"([A-Z])\s+"              # categoria (grupo 9)
        r"([A-Z]{2})\s*"           # caracteristica (grupo 10)
        r"([^\s]+)?\s*"             # OR.E/PESO (grupo 11) - Opcional
        r"([A-Z]{2})?$"             # TP. EXEC (grupo 12) - Opcional
    )
    # Regex para rubrica de servicos digitais (mais abrangente para plataformas e decimais longos)
    re_data_line_digital = re.compile(
        r"^(.+?)\s+"               # artista/compositor (grupo 1)
        r"(SPOTIFY|TIKTOK|YOUTUBE|DEEZER|APPLE MUSIC|AMAZON|META|KWAI|CLARO MUSICA|TIM MUSIC|"
        r"NAPSTER|TIDAL|FACEBOOK|INSTAGRAM|GOOGLE|RESSO|PALCO MP3|GLOBO).*?\s+"  # rubrica (grupo 2)
        r"(\d{2}/\d{4}(?:\s+A\s+\d{2}/\d{4})?)\s+"  # periodo (grupo 3)
        r"([\d.,\-]+)\s+"            # rendimento (grupo 4)
        r"([\d.,\-]+)\s+"            # % rateio (grupo 5)
        r"([\d.,\-]+)\s+"            # valor rateio (grupo 6)
        r"(---)\s+"                # correcao (grupo 7)
        r"([\d\s.,\-]+)\s+"          # execucoes (grupo 8)
        r"([A-Z])\s+"              # categoria (grupo 9)
        r"([A-Z]{2})"              # caracteristica (grupo 10)
    )
    # ISRC - agora usa helpers robustos de pdf_parser
    # Linha de capitulo (TV)
    re_capitulo = re.compile(r"CAP[IÍ]TULO:\s*(.+)")

    # Linha de obra com formato simplificado
    re_obra_header_simple = re.compile(r"^(\d{5,})\s+(.+)")

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        for page_idx, page in enumerate(pdf.pages):
            lines = extract_page_lines(page)
            skip_header = True

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # Ignora linhas de cabecalho do documento
                if skip_header:
                    if "OBRA" in line and ("RUBRICA" in line or "RENDIMENTO" in line or "PERÍODO" in line):
                        skip_header = False
                        logger.debug("Fim do cabecalho detectado na linha: %s", line)
                        continue
                    if any(
                        kw in line
                        for kw in [
                            "DISTRIBUIÇÃO DE DIREITOS",
                            "EXECUÇÃO PÚBLICA",
                            "DEMONSTRATIVO DO TITULAR",
                            "DISTRIBUIÇÃO DE PRESCRITÍVEIS",
                            "ANTECIPAÇÃO DE PRESCRITOS",
                            "CNPJ/CPF:",
                            "CAE/IPI:",
                            "COD ECAD:",
                            "A B R A M U S",
                            "SBACEM",
                            "ABRAMUS",
                            "UBC",
                            "OR.E",
                            "TP.",
                        ]
                    ):
                        continue
                    continue

                # Redetecta header na proxima pagina
                if "DISTRIBUIÇÃO DE DIREITOS" in line:
                    skip_header = True
                    continue
                if any(
                    kw in line
                    for kw in [
                        "EXEC. - NÚM.",
                        "AS INFORMAÇÕES REFERENTES",
                        "ECAD.Tec",
                        "DEMONSTRATIVO DO TITULAR",
                        "DISTRIBUIÇÃO DE PRESCRITÍVEIS",
                        "ANTECIPAÇÃO DE PRESCRITOS",
                        "CNPJ/CPF:",
                        "CAE/IPI:",
                        "COD ECAD:",
                        "OR.E",
                        "TP.",
                    ]
                ):
                    continue

                # Verifica se e' uma linha de ISRC (deteccao robusta)
                isrc_detected = extract_isrc_from_line(line)
                if isrc_detected:
                    current_isrc = isrc_detected
                    # Se a linha CONTEM apenas ISRC, pula
                    cleaned = re.sub(r"ISRC\s+[A-Z]{2}[\w-]+", "", line).strip()
                    if not cleaned or len(cleaned) < 5:
                        continue

                # Verifica se e' uma linha de cabecalho de obra (Forward Fill)
                obra_match = re_obra_header.match(line)
                if obra_match:
                    current_obra_codigo = obra_match.group(1)
                    titulo_raw = obra_match.group(2).strip()
                    # Limpa o titulo removendo valores numericos no final
                    titulo_clean = re.sub(r"\s+[\d.,]+\s*%?\s*[\d.,]*\s*---.*$", "", titulo_raw)
                    current_obra_titulo = clean_text(titulo_clean)
                    current_isrc = None
                    logger.debug(
                        "Forward Fill - Nova obra: [%s] %s",
                        current_obra_codigo,
                        current_obra_titulo,
                    )
                    continue

                # Tenta match simplificado de obra (apenas codigo + titulo)
                if not obra_match:
                    simple_match = re_obra_header_simple.match(line)
                    if simple_match:
                        candidate_code = simple_match.group(1)
                        candidate_rest = simple_match.group(2).strip()
                        # Se o restante nao possui padroes de dados, e' um cabecalho de obra
                        if not re.search(r"AP-|SPOTIFY|TIKTOK|YOUTUBE|DEEZER", candidate_rest):
                            # Verifica se parece ser: CODIGO TITULO VALOR_TOTAL --- EXEC
                            title_match = re.match(
                                r"^(.+?)\s+([\d.,]+)\s*---\s*(.*)", candidate_rest
                            )
                            if title_match:
                                current_obra_codigo = candidate_code
                                current_obra_titulo = clean_text(title_match.group(1))
                                current_isrc = None
                                continue
                            # Pode ser: CODIGO PERCENTUAL % VALOR --- ...
                            perc_match = re.match(
                                r"^(\d+)\s+%\s+([\d.,]+)\s*---", candidate_rest
                            )
                            if perc_match:
                                current_obra_codigo = candidate_code
                                current_obra_titulo = None
                                current_isrc = None
                                continue

                # Tenta extrair linha de dados padrao (AP-...)
                data_match = re_data_line.match(line)
                if not data_match:
                    # Tenta match digital
                    data_match = re_data_line_digital.match(line)

                if data_match:
                    # Se nao temos ISRC, busca em janela de linhas vizinhas
                    line_idx = lines.index(line.strip()) if line.strip() in lines else -1
                    if not current_isrc and line_idx >= 0:
                        current_isrc = extract_isrc_from_window(lines, line_idx, window_size=2)

                    row = {
                        "titular": titular,
                        "documento_origem": tipo,
                        "arquivo_origem": original_filename,
                        "obra_referencia": current_obra_titulo or current_obra_codigo,
                        "obra_codigo": current_obra_codigo,
                        "rubrica": clean_text(data_match.group(2)),
                        "periodo": format_period(data_match.group(3)),
                        "rendimento": clean_monetary_value(data_match.group(4)),
                        "percentual_rateio": clean_monetary_value(data_match.group(5)),
                        "valor_rateio": clean_monetary_value(data_match.group(6)),
                        "correcao": data_match.group(7).strip(),
                        "execucoes": clean_text(data_match.group(8)),
                        "categoria": data_match.group(9).strip(),
                        "caracteristica": data_match.group(10).strip(),
                        "isrc_iswc": current_isrc,
                        "data_pagamento": periodo_dist,
                        "valor_bruto": clean_monetary_value(data_match.group(4)),
                        "valor_liquido": clean_monetary_value(data_match.group(6)),
                        "tipo_extracao": tipo,
                        "artista_gravacao": clean_text(data_match.group(1)),
                        "or_e_peso": clean_text(data_match.group(11)) if data_match.lastindex >= 11 else None,
                    }
                    rows.append(row)
                    # ISRC consumido
                    current_isrc = None
                    continue

                # Tenta match alternativo para linhas sem o padrao 'AP-'
                # Possiveis linhas de continuacao (capitulo, etc.)
                cap_match = re_capitulo.match(line)
                if cap_match:
                    # Linhas de capitulo sao complementares
                    # Tentamos parsear o restante da linha como dados parciais
                    rest = cap_match.group(1)
                    val_match = re.search(
                        r"([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s+(---)\s+(\d+)\s+([A-Z])\s+([A-Z]{2})",
                        rest,
                    )
                    if val_match:
                        row = {
                            "titular": titular,
                            "documento_origem": tipo,
                            "obra_referencia": current_obra_titulo or current_obra_codigo,
                            "obra_codigo": current_obra_codigo,
                            "rubrica": "CAPITULO:" + cap_match.group(1).split()[0] if cap_match.group(1).split() else None,
                            "periodo": None,
                            "rendimento": clean_monetary_value(val_match.group(1)),
                            "percentual_rateio": clean_monetary_value(val_match.group(2)),
                            "valor_rateio": clean_monetary_value(val_match.group(3)),
                            "correcao": val_match.group(4),
                            "execucoes": clean_text(val_match.group(5)),
                            "categoria": val_match.group(6),
                            "caracteristica": val_match.group(7),
                            "isrc_iswc": current_isrc,
                            "data_pagamento": periodo_dist,
                            "valor_bruto": clean_monetary_value(val_match.group(1)),
                            "valor_liquido": clean_monetary_value(val_match.group(3)),
                            "tipo_extracao": tipo,
                            "artista_gravacao": None,
                        }
                        rows.append(row)
                        current_isrc = None

    logger.info(
        "Extracao concluida para %s: %d linhas extraidas de %s",
        tipo,
        len(rows),
        pdf_path,
    )

    if not rows:
        return pd.DataFrame(columns=MASTER_SCHEMA_COLUMNS)

    df = pd.DataFrame(rows)
    return normalize_to_master_schema(df, tipo, titular=titular, documento_origem=tipo)


def _extract_relatorio_analitico_autoral(pdf_path: str, original_filename: str) -> pd.DataFrame:
    """Extrai dados do layout 'RELATORIO ANALITICO DE TITULAR AUTORAL E SUAS OBRAS'."""
    logger.info("Extraindo RELATORIO_ANALITICO_AUTORAL: %s", pdf_path)
    rows: list[dict] = []
    header_info = extract_header_info(extract_first_page_text(pdf_path))
    titular = header_info.get("titular")
    associacao = header_info.get("associacao")

    current_obra_codigo: Optional[str] = None
    current_obra_titulo: Optional[str] = None
    current_iswc: Optional[str] = None
    current_situacao: Optional[str] = None
    current_tipo_obra: Optional[str] = None
    current_nacional: Optional[str] = None

    # Regex para linha de obra
    # Formato: COD_OBRA ISWC/vazio TITULO ASS SITUACAO TIPO_OBRA NAC INCLUSAO
    re_obra = re.compile(
        r"^(\d{5,})\s+"       # codigo (grupo 1)
        r"(T-[\d.\-]+)?\s*"   # ISWC opcional (grupo 2)
        r"(.+?)\s+"            # titulo (grupo 3)
        r"(ABRAMUS|UBC|SBACEM|SOCINPRO|AMAR|SADEMBRA|ASSIM)\s+"  # associacao resp. (grupo 4)
        r"(\w+(?:/\w+)?)\s+"   # situacao (grupo 5)
        r"(ORIGINAL|VERSAO|ARRANJO|COMPILACAO|TRILHA|PARODIA|MEDLEY)?\s*"  # tipo obra (grupo 6)
        r"(SIM|NÃO|NAO)?\s*"   # nacional (grupo 7)
        r"(\d{2}/\d{2}/\d{4})?"  # data inclusao (grupo 8)
    )
    # Formato simplificado de obra (codigo + ISWC + titulo + resto)
    re_obra_simple = re.compile(
        r"^(\d{5,})\s+(T-[\d.\-]+)\s+(.+?)(?:\s+(ABRAMUS|UBC|SBACEM|SOCINPRO|AMAR|SADEMBRA|ASSIM))?(?:\s+(\w+/?\w*))?(?:\s+(ORIGINAL|VERSAO))?\s*(SIM|NÃO|NAO)?\s*(\d{2}/\d{2}/\d{4})?\s*$"
    )
    # Formato obra sem ISWC
    re_obra_no_iswc = re.compile(
        r"^(\d{5,})\s+(.+?)(?:\s+(ABRAMUS|UBC|SBACEM|SOCINPRO|AMAR|SADEMBRA|ASSIM))?\s+(LB|BL|BL/DU|DU|LB/DU)\s+(ORIGINAL|VERSAO|ARRANJO|COMPILACAO|TRILHA|PARODIA|MEDLEY)?\s*(SIM|NÃO|NAO)?\s*(\d{2}/\d{2}/\d{4})?\s*$"
    )

    # Regex para linha de titular da obra (participante)
    re_participante = re.compile(
        r"^(\d{4,})\s+"       # codigo ecad (grupo 1)
        r"(.+?)\s+"            # nome titular (grupo 2)
        r"([A-Z][A-Z\s.]+?)\s+"  # pseudonimo (grupo 3)
        r"(\d{5}\.\d{2}\.\d{2}\.\d{2})\s+"  # CAE (grupo 4)
        r"(ABRAMUS|UBC|SBACEM|SOCINPRO|AMAR|SADEMBRA|ASSIM)\s+"  # associacao (grupo 5)
        r"([A-Z]{1,2})\s+"    # categoria (grupo 6)
        r"([\d.,]+)"           # percentual (grupo 7)
    )

    with pdfplumber.open(pdf_path) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            lines = extract_page_lines(page)
            in_data = False

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # Ignora cabecalhos repetidos
                if any(
                    kw in line
                    for kw in [
                        "RELATÓRIO ANALÍTICO",
                        "ASSOCIAÇÃO:",
                        "TITULAR:",
                        "CÓD. OBRA",
                        "CONTRATO",
                        "CÓD. ECAD",
                        "CÓDIGO",
                        "NOME DO TITULAR",
                        "* SITUAÇÃO",
                        "CLASSIFICAÇÃO DA OBRA",
                        "Pag ",
                        "ECAD.Tec",
                    ]
                ):
                    if "CÓD. OBRA" in line or "CÓDIGO" in line:
                        in_data = True
                    continue

                in_data = True

                # Tenta match de obra com ISWC
                obra_match = re_obra.match(line)
                if not obra_match:
                    obra_match = re_obra_simple.match(line)
                if not obra_match:
                    obra_match = re_obra_no_iswc.match(line)

                if obra_match:
                    current_obra_codigo = obra_match.group(1)
                    if obra_match.lastindex >= 3:
                        current_iswc = obra_match.group(2) if obra_match.group(2) else None
                        current_obra_titulo = clean_text(obra_match.group(3))
                    else:
                        current_obra_titulo = clean_text(obra_match.group(2))
                    logger.debug(
                        "Forward Fill Autoral - Obra: [%s] %s",
                        current_obra_codigo,
                        current_obra_titulo,
                    )
                    continue

                # Tenta match por fallback: linha que comeca com codigo numerico longo
                if not obra_match and re.match(r"^\d{5,}", line):
                    parts = line.split()
                    if len(parts) >= 2:
                        code = parts[0]
                        # Verifica se segundo campo pode ser ISWC
                        rest_text = " ".join(parts[1:])
                        iswc_m = re.match(r"(T-[\d.\-]+)\s+(.*)", rest_text)
                        if iswc_m:
                            current_obra_codigo = code
                            current_iswc = iswc_m.group(1)
                            current_obra_titulo = clean_text(iswc_m.group(2).split("ABRAMUS")[0].split("UBC")[0].split("SBACEM")[0].split("SOCINPRO")[0].strip())
                        else:
                            # Pode ser um participante ou obra sem ISWC
                            part_match = re_participante.match(line)
                            if part_match:
                                row = {
                                    "titular": titular,
                                    "documento_origem": "RELATORIO_ANALITICO_AUTORAL",
                                    "obra_referencia": current_obra_titulo,
                                    "obra_codigo": current_obra_codigo,
                                    "isrc_iswc": current_iswc,
                                    "cod_ecad_participante": part_match.group(1),
                                    "nome_participante": clean_text(part_match.group(2)),
                                    "pseudonimo_participante": clean_text(part_match.group(3)),
                                    "cae_participante": part_match.group(4),
                                    "associacao_participante": part_match.group(5),
                                    "categoria": part_match.group(6),
                                    "percentual_rateio": clean_monetary_value(part_match.group(7)),
                                    "tipo_extracao": "RELATORIO_ANALITICO_AUTORAL",
                                }
                                rows.append(row)
                                continue
                            else:
                                # Assume que e' uma obra sem ISWC
                                current_obra_codigo = code
                                current_iswc = None
                                titulo_parts = rest_text.split()
                                # Pega titulo ate encontrar associacao ou keyword
                                titulo_tokens: list[str] = []
                                for t in titulo_parts:
                                    if t in ("ABRAMUS", "UBC", "SBACEM", "SOCINPRO", "LB", "BL", "DU", "ORIGINAL", "VERSAO", "SIM", "NÃO", "NAO"):
                                        break
                                    titulo_tokens.append(t)
                                current_obra_titulo = clean_text(" ".join(titulo_tokens)) if titulo_tokens else None
                        continue

                # Tenta match de participante
                part_match = re_participante.match(line)
                if part_match:
                    row = {
                        "titular": titular,
                        "documento_origem": "RELATORIO_ANALITICO_AUTORAL",
                        "arquivo_origem": original_filename,
                        "obra_referencia": current_obra_titulo,
                        "obra_codigo": current_obra_codigo,
                        "isrc_iswc": current_iswc,
                        "cod_ecad_participante": part_match.group(1),
                        "nome_participante": clean_text(part_match.group(2)),
                        "pseudonimo_participante": clean_text(part_match.group(3)),
                        "cae_participante": part_match.group(4),
                        "associacao_participante": part_match.group(5),
                        "categoria": part_match.group(6),
                        "percentual_rateio": clean_monetary_value(part_match.group(7)),
                        "tipo_extracao": "RELATORIO_ANALITICO_AUTORAL",
                    }
                    rows.append(row)

    logger.info(
        "Extracao concluida para RELATORIO_ANALITICO_AUTORAL: %d linhas de %s",
        len(rows),
        pdf_path,
    )

    if not rows:
        return pd.DataFrame(columns=MASTER_SCHEMA_COLUMNS)

    df = pd.DataFrame(rows)
    return normalize_to_master_schema(
        df, "RELATORIO_ANALITICO_AUTORAL", titular=titular
    )


def _extract_relatorio_analitico_conexo(pdf_path: str, original_filename: str) -> pd.DataFrame:
    """Extrai dados do layout 'RELATORIO ANALITICO DE TITULAR CONEXO E SUAS GRAVACOES'."""
    logger.info("Extraindo RELATORIO_ANALITICO_CONEXO: %s", pdf_path)
    rows: list[dict] = []
    header_info = extract_header_info(extract_first_page_text(pdf_path))
    titular = header_info.get("titular")
    associacao = header_info.get("associacao")

    current_gravacao_codigo: Optional[str] = None
    current_gravacao_titulo: Optional[str] = None
    current_isrc: Optional[str] = None
    current_situacao: Optional[str] = None
    current_complemento: Optional[str] = None

    # Regex para linha de gravacao
    # COD.ECAD  ISRC  SITUACAO  TITULO  ROTULO  NAC
    re_gravacao = re.compile(
        r"^(\d{5,})\s+"               # codigo ecad (grupo 1)
        r"([A-Z]{2}-[\w-]+)\s+"        # ISRC (grupo 2)
        r"(LIBERADO|BLOQUEADO|PENDENTE)\s+"  # situacao (grupo 3)
        r"(.+?)\s+"                    # titulo (grupo 4)
        r"(SIM|NÃO|NAO)\s*"           # nacional (grupo 5)
        r"([X\s]*)$"                   # classificacao (grupo 6)
    )

    # Regex para linha de participante do conexo
    re_participante_conexo = re.compile(
        r"^(\d{4,})\s+"               # codigo ecad (grupo 1)
        r"(.+?)\s{2,}"                # nome (grupo 2)
        r"([A-Z][A-Z\s.]*?)\s+"       # pseudonimo (grupo 3)
        r"([A-Z]{1,2})\s+"            # categoria (grupo 4)
        r"([A-Z]{1,2})\s+"            # subcategoria (grupo 5)
        r"(ABRAMUS|UBC|SBACEM|SOCINPRO|AMAR|SADEMBRA|ASSIM)\s+"  # associacao (grupo 6)
        r"([\d.,]+)"                   # participacao % (grupo 7)
    )

    with pdfplumber.open(pdf_path) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            lines = extract_page_lines(page)

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # Ignora cabecalhos
                if any(
                    kw in line
                    for kw in [
                        "RELATÓRIO ANALÍTICO",
                        "ASSOCIAÇÃO:",
                        "TITULAR:",
                        "CÓD. ECAD",
                        "CLASSIFICAÇÃO",
                        "PSEUDÔNIMO",
                        "CAT.",
                        "SUBCAT.",
                        "PART. (%)",
                        "Pag ",
                        "ECAD.Tec",
                        "SUBCATEGORIA:",
                        "ISRC/GRA",
                        "SITUACAO",
                        "RÓTULO",
                    ]
                ):
                    continue

                # Tenta match de gravacao
                grav_match = re_gravacao.match(line)
                if grav_match:
                    current_gravacao_codigo = grav_match.group(1)
                    current_isrc = grav_match.group(2)
                    current_situacao = grav_match.group(3)
                    current_gravacao_titulo = clean_text(grav_match.group(4))
                    current_complemento = None
                    logger.debug(
                        "Forward Fill Conexo - Gravacao: [%s] %s (%s)",
                        current_gravacao_codigo,
                        current_gravacao_titulo,
                        current_isrc,
                    )
                    continue

                # Fallback: linha comecando com codigo numerico + ISRC
                if not grav_match and re.match(r"^\d{5,}", line):
                    parts = line.split()
                    if len(parts) >= 2:
                        code = parts[0]
                        isrc_candidate = parts[1] if len(parts) > 1 else ""
                        if re.match(r"^[A-Z]{2}-[\w-]+$", isrc_candidate):
                            current_gravacao_codigo = code
                            current_isrc = isrc_candidate
                            # Tenta extrair situacao e titulo
                            rest = " ".join(parts[2:])
                            sit_match = re.match(
                                r"(LIBERADO|BLOQUEADO|PENDENTE)\s+(.+?)(?:\s+(SIM|NÃO|NAO))?\s*([X\s]*)$",
                                rest,
                            )
                            if sit_match:
                                current_situacao = sit_match.group(1)
                                current_gravacao_titulo = clean_text(sit_match.group(2))
                            else:
                                current_gravacao_titulo = clean_text(rest)
                            current_complemento = None
                            continue
                        # Se nao e' ISRC, pode ser um participante
                        part_match = re_participante_conexo.match(line)
                        if part_match:
                            row = {
                                "titular": titular,
                                "documento_origem": "RELATORIO_ANALITICO_CONEXO",
                                "obra_referencia": current_gravacao_titulo,
                                "obra_codigo": current_gravacao_codigo,
                                "isrc_iswc": current_isrc,
                                "situacao": current_situacao,
                                "complemento_titulo": current_complemento,
                                "cod_ecad_participante": part_match.group(1),
                                "nome_participante": clean_text(part_match.group(2)),
                                "pseudonimo_participante": clean_text(part_match.group(3)),
                                "categoria": part_match.group(4),
                                "subcategoria": part_match.group(5),
                                "associacao_participante": part_match.group(6),
                                "percentual_rateio": clean_monetary_value(part_match.group(7)),
                                "tipo_extracao": "RELATORIO_ANALITICO_CONEXO",
                            }
                            rows.append(row)
                            continue

                # Linha complementar (ex: "LIVE (AO VIVO)")
                if (
                    current_gravacao_codigo
                    and not re.match(r"^\d", line)
                    and len(line.split()) <= 5
                    and not re_participante_conexo.match(line)
                ):
                    current_complemento = clean_text(line)
                    continue

                # Tenta match de participante
                part_match = re_participante_conexo.match(line)
                if part_match:
                    row = {
                        "titular": titular,
                        "documento_origem": "RELATORIO_ANALITICO_CONEXO",
                        "arquivo_origem": original_filename,
                        "obra_referencia": current_gravacao_titulo,
                        "obra_codigo": current_gravacao_codigo,
                        "isrc_iswc": current_isrc,
                        "situacao": current_situacao,
                        "complemento_titulo": current_complemento,
                        "cod_ecad_participante": part_match.group(1),
                        "nome_participante": clean_text(part_match.group(2)),
                        "pseudonimo_participante": clean_text(part_match.group(3)),
                        "categoria": part_match.group(4),
                        "subcategoria": part_match.group(5),
                        "associacao_participante": part_match.group(6),
                        "percentual_rateio": clean_monetary_value(part_match.group(7)),
                        "tipo_extracao": "RELATORIO_ANALITICO_CONEXO",
                    }
                    rows.append(row)

    logger.info(
        "Extracao concluida para RELATORIO_ANALITICO_CONEXO: %d linhas de %s",
        len(rows),
        pdf_path,
    )

    if not rows:
        return pd.DataFrame(columns=MASTER_SCHEMA_COLUMNS)

    df = pd.DataFrame(rows)
    return normalize_to_master_schema(
        df, "RELATORIO_ANALITICO_CONEXO", titular=titular
    )


# ==========================================================================
# 3. DISPATCHER - Mapeia layout para extrator
# ==========================================================================

EXTRACTOR_MAP: dict[str, callable] = {
    "DEMONSTRATIVO_TITULAR": _extract_demonstrativo_titular,
    "DEMONSTRATIVO_SERVICOS_DIGITAIS": _extract_servicos_digitais,
    "DEMONSTRATIVO_ANTECIPACAO_PRESCRITOS": _extract_antecipacao_prescritos,
    "DISTRIBUICAO_PRESCRITIVEIS": _extract_distribuicao_prescritiveis,
    "RELATORIO_ANALITICO_AUTORAL": _extract_relatorio_analitico_autoral,
    "RELATORIO_ANALITICO_CONEXO": _extract_relatorio_analitico_conexo,
}


def extract_pdf(pdf_path: str, original_filename: str) -> tuple[str, pd.DataFrame]:
    """
    Funcao principal de extracao.
    1. Identifica o layout.
    2. Despacha para o extrator correto.
    3. Retorna (layout_name, dataframe).
    """
    layout = identify_layout(pdf_path)
    if not layout:
        raise ValueError(
            f"Layout nao identificado para o arquivo '{Path(pdf_path).name}'. "
            "O arquivo pode estar corrompido ou em um formato nao suportado."
        )

    extractor = EXTRACTOR_MAP.get(layout)
    if not extractor:
        raise ValueError(
            f"Extrator nao implementado para o layout '{layout}'."
        )

    df = extractor(pdf_path, original_filename)
    return layout, df


# ==========================================================================
# 4. EXPORTADOR EXCEL
# ==========================================================================

def export_to_excel(
    dataframes: list[pd.DataFrame],
    output_filename: str,
    sheet_name: str = "Dados Consolidados",
) -> str:
    """
    Consolida uma lista de DataFrames e exporta para um arquivo .xlsx
    com formatacao basica (cabecalho em negrito).
    
    Args:
        dataframes: Lista de DataFrames a consolidar.
        output_filename: Nome do arquivo de saida (sem extensao).
        sheet_name: Nome da aba no Excel.
    
    Returns:
        Caminho absoluto do arquivo gerado.
    """
    if not dataframes:
        raise ValueError("Nenhum DataFrame fornecido para exportacao.")

    # Consolida todos os DataFrames
    consolidated = pd.concat(dataframes, ignore_index=True)

    # Garante que o nome tem extensao
    if not output_filename.endswith(".xlsx"):
        output_filename += ".xlsx"

    output_path = EXPORT_DIR / output_filename

    # Exporta com openpyxl para permitir formatacao
    with pd.ExcelWriter(str(output_path), engine="openpyxl") as writer:
        consolidated.to_excel(writer, index=False, sheet_name=sheet_name)

        # Aplica formatacao no cabecalho
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]

        header_font = Font(bold=True, size=11)
        for cell in worksheet[1]:
            cell.font = header_font

        # Ajusta largura das colunas
        for col_idx, col in enumerate(consolidated.columns, start=1):
            max_len = max(
                len(str(col)),
                consolidated[col].astype(str).str.len().max() if len(consolidated) > 0 else 0,
            )
            adjusted_width = min(max_len + 2, 50)
            worksheet.column_dimensions[
                worksheet.cell(row=1, column=col_idx).column_letter
            ].width = adjusted_width

    logger.info("Arquivo Excel exportado: %s (%d linhas)", output_path, len(consolidated))
    return str(output_path)


def export_single_job_to_excel(df: pd.DataFrame, job_id: str) -> str:
    """
    Exporta os dados de um unico job para Excel.
    
    Args:
        df: DataFrame com os dados extraidos.
        job_id: ID do job para gerar o nome do arquivo.
    
    Returns:
        Caminho absoluto do arquivo gerado.
    """
    return export_to_excel([df], f"extracao_{job_id}")
