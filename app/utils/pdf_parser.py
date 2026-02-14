"""
Helpers para parsing de PDF com pdfplumber.
Funcoes de baixo nivel para extracao de texto e tabelas.
"""

from __future__ import annotations

import re
from typing import Optional

import pdfplumber
from pdfplumber.page import Page

from app.config.settings import logger


def extract_full_text(pdf_path: str) -> str:
    """Extrai todo o texto de um PDF concatenando todas as paginas."""
    pages_text: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages_text.append(text)
    return "\n".join(pages_text)


def extract_first_page_text(pdf_path: str) -> str:
    """Extrai o texto da primeira pagina para identificacao do layout."""
    with pdfplumber.open(pdf_path) as pdf:
        if pdf.pages:
            return pdf.pages[0].extract_text() or ""
    return ""


def extract_page_lines(page: Page) -> list[str]:
    """Extrai o texto de uma pagina e retorna como lista de linhas."""
    text = page.extract_text()
    if not text:
        return []
    return text.split("\n")


def extract_header_info(first_page_text: str) -> dict[str, Optional[str]]:
    """
    Extrai informacoes do cabecalho do demonstrativo:
    - titular (nome da pessoa)
    - cpf/cnpj
    - demonstrativo numero
    - cae/ipi
    - pseudonimo / cod ecad
    - periodo de distribuicao
    - valor total
    """
    info: dict[str, Optional[str]] = {
        "titular": None,
        "cnpj_cpf": None,
        "demonstrativo_numero": None,
        "cae_ipi": None,
        "pseudonimo": None,
        "cod_ecad": None,
        "periodo_distribuicao": None,
        "valor_total": None,
        "associacao": None,
    }

    lines = first_page_text.split("\n")

    for line in lines:
        # Periodo de distribuicao (ex: "NOVEMBRO/2024" ou "DEZEMBRO/2025")
        if not info["periodo_distribuicao"]:
            match_periodo = re.search(
                r"(?:DIREITOS AUTORAIS|TOTAL[:\s]*)\s*([A-ZÇ]+/\d{4})", 
                line, 
                re.IGNORECASE
            )
            if match_periodo:
                info["periodo_distribuicao"] = match_periodo.group(1).strip()
            # Fallback para linha contendo apenas MES/ANO
            elif re.match(r"^[A-ZÇ]+/\d{4}$", line.strip(), re.IGNORECASE):
                info["periodo_distribuicao"] = line.strip()

        # Valor total
        if not info["valor_total"]:
            match_total = re.search(r"TOTAL[:\s]*([\d.,]+)", line, re.IGNORECASE)
            if match_total:
                info["valor_total"] = match_total.group(1).strip()

        # Nome do titular e CPF
        # Formato: NOME ... CNPJ/CPF: 000...
        match_titular = re.search(
            r"^([A-Z\s]+?)\s+(?:CNPJ/CPF:|CAE/IPI:)\s*([\d.\-/]+)", line
        )
        if match_titular:
            info["titular"] = match_titular.group(1).strip()
            if "CNPJ/CPF:" in line:
                info["cnpj_cpf"] = match_titular.group(2).strip()

        # Demonstrativo numero
        match_demo = re.search(
            r"DEMONSTRATIVO\s+N[ºo]?:\s*([\d\s/]+)", line, re.IGNORECASE
        )
        if match_demo:
            info["demonstrativo_numero"] = match_demo.group(1).strip()

        # CAE/IPI
        match_cae = re.search(r"CAE/IPI:\s*(\d+)", line)
        if match_cae:
            info["cae_ipi"] = match_cae.group(1).strip()

        # Pseudonimo e COD ECAD
        match_pseudo = re.search(
            r"^([A-Z\s]+?)\s+COD ECAD:\s*(\d+)", line
        )
        if match_pseudo:
            if not info["titular"]:
                info["pseudonimo"] = match_pseudo.group(1).strip()
            info["cod_ecad"] = match_pseudo.group(2).strip()

        # Associacao (para relatorios analiticos)
        match_assoc = re.search(
            r"ASSOCIA[CÇ][AÃ]O\s*:\s*(.+)", line, re.IGNORECASE
        )
        if match_assoc:
            info["associacao"] = match_assoc.group(1).strip()

        # Titular (formato relatorios analiticos)
        match_tit_analitico = re.search(
            r"TITULAR:\s*(\d+)\s+(.+?)(?:\s+CATEGORIA:|\s+PSEUD[OÔ]NIMO:|$)",
            line,
            re.IGNORECASE,
        )
        if match_tit_analitico and not info["titular"]:
            info["titular"] = match_tit_analitico.group(2).strip()

    return info


# ---------------------------------------------------------------------------
# ISRC / ISWC - Deteccao Robusta
# ---------------------------------------------------------------------------

# Patterns para ISRC (ex: BR-WNV-17-00176, BX-P5O-18-00014)
_RE_ISRC_FULL = re.compile(r"\b([A-Z]{2}-?[\w]{3}-?\d{2}-?\d{5})\b")
_RE_ISRC_LABEL = re.compile(r"ISRC\s+([A-Z]{2}[\w-]+)")

# Patterns para ISWC (ex: T-323.947.319-5, T-032.901.290-4)
_RE_ISWC = re.compile(r"\b(T-?\d{3,}\.?\d{3,}\.?\d{3,}-?\d)\b")


def extract_isrc_from_line(line: str) -> Optional[str]:
    """
    Tenta extrair um codigo ISRC de uma linha.
    Suporta:
      - 'ISRC BR-WNV-17-00176'
      - ISRC inline junto com dados
      - Codigos soltos no formato XX-XXX-NN-NNNNN
    """
    if not line:
        return None

    # Prioridade 1: label explicita "ISRC ..."
    m = _RE_ISRC_LABEL.search(line)
    if m:
        return m.group(1).strip()

    # Prioridade 2: pattern ISRC completo
    m = _RE_ISRC_FULL.search(line)
    if m:
        return m.group(1).strip()

    return None


def extract_iswc_from_line(line: str) -> Optional[str]:
    """Tenta extrair um codigo ISWC (T-NNN.NNN.NNN-N) de uma linha."""
    if not line:
        return None
    m = _RE_ISWC.search(line)
    if m:
        return m.group(1).strip()
    return None


def extract_isrc_from_window(
    lines: list[str],
    current_index: int,
    window_size: int = 2,
) -> Optional[str]:
    """
    Busca ISRC em uma janela de linhas ao redor do indice atual.
    Util quando o ISRC esta na linha acima ou abaixo dos dados.
    """
    start = max(0, current_index - window_size)
    end = min(len(lines), current_index + window_size + 1)

    for i in range(start, end):
        if i == current_index:
            continue
        isrc = extract_isrc_from_line(lines[i])
        if isrc:
            return isrc
    return None


def clean_monetary_value(value: str) -> Optional[float]:
    """
    Limpa e converte um valor monetario brasileiro para float.
    - '1.234,56' -> 1234.56
    - '0,00' -> 0.0
    - '---' -> None (Null no Excel)
    """
    if not value or not isinstance(value, str):
        return None
    
    val = value.strip()
    
    # Tratamento explicito pedido pelo usuario
    if val in ("---", "-", "", "None"):
        return None
        
    # Remove caracteres nao numericos exceto ponto, virgula e sinal negativo
    numeric_val = re.sub(r"[^\d.,\-]", "", val)
    if not numeric_val:
        return None
        
    # Formato brasileiro: 1.234,56 -> 1234.56
    if "," in numeric_val:
        # Se tem ponto e virgula (1.234,56)
        if "." in numeric_val:
            numeric_val = numeric_val.replace(".", "").replace(",", ".")
        else:
            # So tem virgula (0,00)
            numeric_val = numeric_val.replace(",", ".")
            
    try:
        return float(numeric_val)
    except ValueError:
        return None


def clean_text(text: Optional[str]) -> Optional[str]:
    """Remove espaços extras e normaliza o texto."""
    if not text or not isinstance(text, str):
        return None
    text = re.sub(r"\s+", " ", text).strip()
    return text if text else None
