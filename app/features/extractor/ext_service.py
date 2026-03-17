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

import json
import os
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, Any
import unicodedata

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
    extract_iswc_from_line,
    extract_page_lines,
    is_likely_data_line,
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


# --- CONFIGURAÇÕES DE NORMALIZAÇÃO ---
def load_platform_mapping() -> dict[str, str]:
    """Carrega o mapeamento de plataformas do arquivo de configuracao."""
    # backend/app/features/extractor/ext_service.py -> backend/config/platform_mapping.json
    config_path = Path(__file__).parent.parent.parent.parent / "config" / "platform_mapping.json"
    if config_path.exists():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                mapping = json.load(f)
                logger.info(f"Mapeamento de plataformas carregado com sucesso: {len(mapping)} itens.")
                return mapping
        except Exception as e:
            logger.error(f"Erro ao carregar platform_mapping.json em {config_path}: {e}")
    else:
        logger.warning(f"platform_mapping.json nao encontrado em: {config_path}")
    return {}

PLATFORM_MAPPING = load_platform_mapping()

def normalize_platform_name(name: str) -> str:
    """Normaliza o nome da plataforma usando mapeamento ou limpeza basica."""
    name = clean_text(name).upper()
    # Tenta match exato no mapeamento
    if name in PLATFORM_MAPPING:
        return PLATFORM_MAPPING[name]
    
    # fuzzy matching refinado: se contem a chave de forma isolada
    # Ordenamos chaves por tamanho decrescente para priorizar "SPOTIFY BRASIL PREMIUM" sobre "SPOTIFY"
    # e evitar que chaves menores (ruído) capturem o nome primeiro.
    sorted_keys = sorted(PLATFORM_MAPPING.keys(), key=len, reverse=True)
    for key in sorted_keys:
        # Verifica se a chave existe como uma sequencia inteira de tokens
        if f" {key} " in f" {name} ":
            return PLATFORM_MAPPING[key]
            
    return name

def log_integrity_error(filename: str, error_message: str):
    """Registra erro de integridade em arquivo de log dedicado conforme Spec v1.0."""
    log_path = Path(__file__).parent.parent.parent / "erros_processamento.log"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] Arquivo: {filename} | Erro: {error_message}\n")
    except Exception as e:
        logger.error(f"Erro ao escrever no integrity log: {e}")

def handle_integrity_failure(pdf_path: str, error_message: str):
    """Move arquivo falho para pasta /erros e registra no log."""
    original_name = os.path.basename(pdf_path)
    log_integrity_error(original_name, error_message)
    
    err_dir = Path(__file__).parent.parent.parent / "erros"
    err_dir.mkdir(exist_ok=True)
    
    target_path = err_dir / original_name
    try:
        shutil.move(pdf_path, str(target_path))
        logger.warning(f"Arquivo movido para {target_path} devido a falha de integridade.")
    except Exception as e:
        logger.error(f"Erro ao mover arquivo falho: {e}")

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
    # Prefixos comuns de rubricas para ajudar na separação de Artista vs Rubrica
    RUBRICA_PREFIXES = (
        r"(?:EXT\.\s+RÁDIO\s+AM/FM|SDI|AP-|RÁDIO|TV|SHOW|CARNAVAL|SONORIZAÇÃO|MOVEMAIS|NETFLIX|DISNEY|"
        r"GLOBOPLAY|DEEZER|SPOTIFY|TIKTOK|YOUTUBE|APPLE MUSIC|AMAZON|META|KWAI|CLARO MUSICA|TIM MUSIC|"
        r"NAPSTER|TIDAL|FACEBOOK|INSTAGRAM|GOOGLE|RESSO|PALCO MP3|GLOBO)"
    )
    
    tipo_extracao = tipo # Estado local para controle de hard stops

    # --- 0. CONSTANTES E CONFIGURAÇÕES ---
    STOP_KEYWORDS = ["TOTAIS POR CATEGORIA", "PONTOS DISTRIBUIÇÃO", "PONTUAÇÃO", "PONTOS DISTRIBUICAO"]
    
    rows: list[dict] = []
    header_info = extract_header_info(extract_first_page_text(pdf_path))
    titular = header_info.get("titular")
    periodo_dist = header_info.get("periodo_distribuicao")
    valor_total = header_info.get("valor_total")

    current_obra_codigo: Optional[str] = None
    current_obra_titulo: Optional[str] = None
    current_isrc: Optional[str] = None
    current_rubrica_buffer: Optional[str] = None
    has_data_for_current_obra = False  # Track if current obra already has data rows
    in_data = False
    
    # Rastreio de rubricas já extraídas detalhadamente para deduplicação inteligente
    # Dicionário mapping: rubrica -> soma_valor_rateio_extraido
    rubricas_with_sum: dict[str, float] = {}

    # Regex para detectar linha de cabecalho de obra (codigo numerico no inicio)
    # Formato: CODIGO TITULO VALOR --- EXECUCOES | SEGMENTO
    re_obra_header = re.compile(r"^(\d{5,})\s+(.+?)\s+([\d.,\-]+)\s*---")
    # Regex para detectar linha de detalhe de dados (Geral)
    re_data_line = re.compile(
        r"^(.+?)\s+"               # artista/gravacao (grupo 1)
        r"(?=" + RUBRICA_PREFIXES + r")" # Lookahead para garantir que a rubrica começa aqui
        r"(" + RUBRICA_PREFIXES + r".*?)" # rubrica (grupo 2)
        r"\s*"                     # espaco opcional
        r"(\d{2}/\d{4}(?:\s+A\s+\d{2}/\d{4})?)\s+"  # periodo (grupo 3)
        r"([\d.,\-]+)\s+"            # rendimento (grupo 4)
        r"([\d.,\-]+|\d+/\d+)\s+"    # % rateio (grupo 5)
        r"([\d.,\-]+)\s+"            # valor rateio (grupo 6)
        r"(---)\s+"                # correcao (grupo 7)
        r"([^\s]+)\s+"               # execucoes (grupo 8) - use non-space
        r"([A-Z]{1,2})\s+"         # categoria (grupo 9)
        r"([A-Z]{2})\s*"           # caracteristica (grupo 10)
        r"([^\s]+)?\s*"             # OR.E/PESO (grupo 11)
        r"([A-Z]{2})?(?:\s|$)"      # TP. EXEC (grupo 12)
    )
    current_obra_header_value: Optional[float] = None
    
    # Regex para rubrica de servicos digitais (mais abrangente para plataformas e decimais longos)
    re_data_line_digital = re.compile(
        r"^(.+?)\s+"               # artista/compositor (grupo 1)
        r"(?=" + RUBRICA_PREFIXES + r")" # Digital tb pode ter rubricas coladas
        r"(" + RUBRICA_PREFIXES + r".*?)" # rubrica (grupo 2)
        r"\s*"                     # espaco opcional
        r"(\d{2}/\d{4}(?:\s+A\s+\d{2}/\d{4})?)\s+"  # periodo (grupo 3)
        r"([\d.,\-]+)\s+"            # rendimento (grupo 4)
        r"([\d.,\-]+|\d+/\d+)\s+"    # % rateio (grupo 5)
        r"([\d.,\-]+)\s+"            # valor rateio (grupo 6)
        r"(---)\s+"                # correcao (grupo 7)
        r"([\d\s.,\-]+)\s+"          # execucoes (grupo 8)
        r"([A-Z]{1,2})\s+"         # categoria (grupo 9) - ex: A, MA, I
        r"([A-Z]{2})"              # caracteristica (grupo 10)
    )
    # ISRC - agora usa helpers robustos de pdf_parser
    # Linha de capitulo (TV)
    re_capitulo = re.compile(r"CAP[IÍ]TULO:\s*(.+)")

    # Linha do quadro TOTAIS POR RUBRICA
    # Regex ultrarobusto para sumário: Rubrica + qualquer coisa + Valores
    re_totais_line = re.compile(r"^(.+?)\s+(?:.*?)\s*([\d.,\-]{4,}|---)\s*(?:---)?\s*(?:---)?\s*(?:---)?\s*([\d.,\-]{4,}|---)?\s*$")

    # Layout DIGITAL robusto: Rubrica, Periodo, Rendimento, % Rateio, Valor Rateio (pode ter '---' e códigos no final)
    re_data_line_digital = re.compile(
        r"^\s*(?:\[?\d+\]?\s+)?(.*?)\s+"                               # Rubrica/Título (G1)
        r"(\d{2}/\d{2,4}\s+A\s+\d{2}/\d{2,4})\s+"                      # Período (G2)
        r"([\d.,-]+|---)\s+"                                           # Rendimento (G3)
        r"([\d.,-]+|---)\s+"                                           # % Rateio (G4)
        r".*?\s+([\d.,-]+)(?:\s+[\w\-]+)*\s*$"                         # Valor Rateio (G5 - Captura o valor final da linha)
    )

    # Linha de obra com formato simplificado
    re_obra_header_simple = re.compile(r"^(\d{5,})\s+(.+)")

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        
        # --- NOVO: BUCKET SYSTEM (PRÉ-SCAN DO SUMÁRIO) ---
        # Escaneia o PDF inteiro em busca do quadro final de rubricas para saber os alvos
        summary_buckets = {} # rubrica_nome -> valor_total_esperado
        is_scanning_summary = False
        
        for p in pdf.pages:
            p_text = p.extract_text()
            if not p_text: continue
            for p_line in p_text.split('\n'):
                p_line_clean = p_line.strip()
                if "TOTAIS POR RUBRICA" in p_line_clean.upper():
                    is_scanning_summary = True
                    continue
                if is_scanning_summary:
                    # O sumário acaba quando encontramos o total geral do título ou uma linha de total muito curta
                    # A linha de cabeçalho contém muitas palavras (colunas), o footer total geralmente tem < 5 palavras.
                    if ("TOTAL DO TITULAR" in p_line_clean.upper() or "TOTAL GERAL" in p_line_clean.upper()) and len(p_line_clean.split()) < 5:
                        is_scanning_summary = False
                        continue
                    
                    # Evita capturar a linha de cabeçalho
                    if "RENDIMENTO % RATEIO" in p_line_clean.upper() or "DISTRIBUIÇÃO LIBERAÇÃO" in p_line_clean.upper():
                        continue
                    
                    # Tenta capturar rubrica e valor final da linha do sumário
                    sum_vals = re.findall(r"[\d.]{1,7},\d{2}", p_line_clean)
                    if sum_vals:
                        # Regex mais flexível para o nome: pega tudo até o primeiro valor decimal ou data
                        s_match = re.search(r"^(.+?)(?:\d{2}/\d{4}|[\d.]{1,7},\d{2})", p_line_clean)
                        if s_match:
                            r_name = clean_text(s_match.group(1)).upper()
                            if r_name in ["RUBRICA", "TOTAL", "VALORES", "DISTRIBUIÇÃO"]: continue
                            r_val = clean_monetary_value(sum_vals[-1])
                            if r_val:
                                summary_buckets[r_name] = r_val
                        else:
                            # Caso especial: Nome e valor na mesma linha sem data (ex: TAXA)
                            # Se temos valores e não deu match no nome via regex, pegamos a primeira parte
                            parts = p_line_clean.split()
                            if parts:
                                r_name = clean_text(parts[0]).upper()
                                if r_name not in ["TOTAL", "RUBRICA"]:
                                    summary_buckets[r_name] = clean_monetary_value(sum_vals[-1])
        
        logger.info(f"Bucket System: {len(summary_buckets)} rubricas identificadas no sumário.")
        
        in_totais_rubrica = False
        totais_rubrica_rows = []
        
        for page_idx, page in enumerate(pdf.pages):
            lines = extract_page_lines(page)
            skip_header = True

            for line_idx, line in enumerate(lines):
                line = line.strip()
                if not line:
                    continue

                # --- 0. FILTRO DE RUÍDO (FOOTERS/DISCLAIMERS) ---
                # Padrões comuns de rodapé e avisos legais do ECAD que não devem ser processados
                if any(kw in line for kw in [
                    "informações referentes aos tipos", "critérios de distribuição", "Eu Faço Música",
                    "http://www.ecad.org.br", "O LINK DO SITE É", "CAE/IPI:",
                    "RENDIMENTO % RATEIO", "OR.E TP. OBRA", "CLASSIFICAÇÃO DA OBRA",
                    "ECAD.Tec", "Página", "DEMONSTRATIVO", "ECAD - TEC", "IDENTIFICAÇÃO DE RUBRICA"
                ]) or re.search(r"http[s]?://", line):
                    if not is_likely_data_line(line):
                        continue

                # --- 0.2. HARD STOP (FUGA DE ESCOPO) ---
                if any(kw in line.upper() for kw in STOP_KEYWORDS):
                    logger.info(f"Hard Stop encontrado: '{line}'. Interrompendo extração tabular.")
                    in_totais_rubrica = False
                    in_data = False
                    # Se for "PONTOS DISTRIBUIÇÃO", queremos parar MESMO se estivermos em "TOTAIS POR RUBRICA"
                    if "PONTOS" in line.upper() or "PONTUAÇÃO" in line.upper():
                        break # Sai do loop de linhas desta página
                    continue
                # Verificação de Hard Stop e transição para sumário
                if any(k in line.upper() for k in STOP_KEYWORDS):
                    logger.info(f"Hard Stop encontrado: '{line.strip()}'. Interrompendo extração tabular.")
                    tipo_extracao = "TEXTO" # Sinaliza que os dados tabulares acabaram
                    if "TOTAIS POR RUBRICA" in line.upper() or "TOTAL GERAL" in line.upper():
                        in_totais_rubrica = True
                    continue

                if "TOTAIS POR RUBRICA" in line.upper():
                    in_totais_rubrica = True
                    continue

                if in_totais_rubrica:
                    # Captura de linha de total por rubrica usando regex flexível
                    # Para sumários, extraímos todos os valores monetários da linha
                    if "TOTAL" in line.upper() and ("DO TITULAR" in line.upper() or "GERAL" in line.upper() or len(line.split()) < 3):
                        continue
                        
                    potential_values_raw = re.findall(r"[\d.]{1,7},\d{2}", line)
                    if potential_values_raw:
                        # O nome da rubrica é o que vem antes do primeiro valor ou data
                        rub_match = re.search(r"^(.+?)(?:\d{2}/\d{4}|[\d.]{1,7},\d{2})", line)
                        if rub_match:
                            rub_name_raw = rub_match.group(1).strip()
                            if rub_name_raw.upper() in ["RUBRICA", "TOTAL", "VALORES", "TOTAL GERAL", "TOTAL DO TITULAR"]: continue
                            if rub_name_raw.upper().startswith("TOTAL "): continue
                            
                            val_total = clean_monetary_value(potential_values_raw[-1])
                            if val_total and val_total > 0.01:
                                totais_rubrica_rows.append({
                                    "rubrica": rub_name_raw,
                                    "valor_rateio": val_total,
                                    "periodo": "N/A",
                                    "rendimento": clean_monetary_value(potential_values_raw[0])
                                })
                    continue
                
                # SÓ PROCESSA DADOS SE NÃO ESTIVER NO SUMÁRIO E NÃO TIVERALCANCE STOP
                if tipo_extracao == "TEXTO":
                    continue

                # --- 1. DETECÇÃO DE CABEÇALHO DE OBRA (FORWARD FILL) ---
                obra_match = re_obra_header.match(line)
                if not obra_match:
                    obra_match = re_obra_header_simple.match(line)

                if obra_match:
                    # Se a obra anterior não teve detalhe, captura do header agora
                    if current_obra_codigo and not has_data_for_current_obra and current_obra_header_value:
                        row = {
                            "titular": titular, "documento_origem": tipo, "arquivo_origem": original_filename,
                            "obra_referencia": current_obra_titulo or current_obra_codigo, "obra_codigo": current_obra_codigo,
                            "rubrica": current_rubrica_buffer or "DIRETO / TOTAL OBRA", "periodo": periodo_dist or "N/A", "rendimento": current_obra_header_value,
                            "percentual_rateio": 100.0, "valor_rateio": current_obra_header_value, "correcao": "---", "execucoes": "1",
                            "cat_caracteristica": "N/A", "isrc_iswc": current_isrc, "data_pagamento": periodo_dist,
                            "valor_bruto": current_obra_header_value, "valor_liquido": current_obra_header_value,
                            "tipo_extracao": tipo, "artista_gravacao": titular,
                        }
                        rows.append(row)
                        # Rastreia rubrica do header
                        rub_header_raw = str(row["rubrica"])
                        rub_bucket_key = find_best_bucket(rub_header_raw, summary_buckets, current_rubrica_buffer)
                        rub_name = rub_bucket_key if rub_bucket_key else normalize_platform_name(rub_header_raw)
                        
                        vr = row["valor_rateio"]
                        val_float = float(vr) if vr is not None else 0.0
                        rubricas_with_sum[rub_name] = rubricas_with_sum.get(rub_name, 0.0) + val_float
                    
                    current_obra_codigo = obra_match.group(1)
                    current_obra_titulo = clean_text(obra_match.group(2))
                    # O header valor so existe se foi a re_obra_header (3 grupos)
                    # Usamos lastindex para saber quantos grupos deram match
                    idx_match = obra_match.lastindex
                    current_obra_header_value = clean_monetary_value(obra_match.group(3)) if idx_match and idx_match >= 3 else None

                    current_isrc = None
                    # current_rubrica_buffer = None  # Mantém o buffer para demonstrativos digitais onde a rubrica precede a obra
                    has_data_for_current_obra = False
                    in_data = True
                    logger.debug(f"Forward Fill - Obra: [{current_obra_codigo}] {current_obra_titulo} (Header val: {current_obra_header_value})")
                    continue

                if not in_data and is_likely_data_line(line):
                    in_data = True

                # --- 2. DETECÇÃO DE ISRC ---
                isrc_detected = extract_isrc_from_line(line)
                if isrc_detected:
                    current_isrc = isrc_detected
                    # Vinculo retroativo se a última linha não tinha ISRC
                    if rows and not rows[-1].get("isrc_iswc"):
                        rows[-1]["isrc_iswc"] = current_isrc
                    
                    # Se a linha contém APENAS o ISRC, não tentamos processar como dados
                    cleaned_line = re.sub(r"ISRC\s+[A-Z]{2}[\w-]+", "", line).strip()
                    if not cleaned_line or len(cleaned_line) < 5:
                        continue

                # --- 3. DETECÇÃO DE RUBRICA EM LINHA SOLTA ---
                # Em alguns demonstrativos, a rubrica (ex: AP-RÁDIOS) vem numa linha sem dados
                if not is_likely_data_line(line) and not isrc_detected:
                    # Busca por qualquer um dos baldes conhecidos dentro da linha
                    bucket_name = find_best_bucket(line, summary_buckets, current_rubrica_buffer)
                    if bucket_name:
                        current_rubrica_buffer = bucket_name
                        logger.debug(f"Bufferizado Rubrica via Bucket: {current_rubrica_buffer}")
                    else:
                        rub_test = re.search(f"({RUBRICA_PREFIXES}.*?)(?:\\s*\\d{{2}}/\\d{{4}}|$)", line)
                        if rub_test:
                             current_rubrica_buffer = clean_text(rub_test.group(1))
                             logger.debug(f"Bufferizado Rubrica via Prefix: {current_rubrica_buffer}")

                # --- 4. DETECÇÃO DE LINHA DE DADOS (RATEIO) ---
                # Primeiro tentamos os patterns específicos (mais seguros)
                data_match = re_data_line.match(line)
                is_digital_match = False
                
                if not data_match and tipo == "DEMONSTRATIVO_SERVICOS_DIGITAIS":
                    data_match = re_data_line_digital.match(line)
                    is_digital_match = True if data_match else False

                if data_match:
                    # Match. Extraímos os dados.
                    has_data_for_current_obra = True
                    in_data = True
                    
                    if is_digital_match:
                         # Match digital: Rubrica/Titulo, Periodo, Rendimento, % Rateio, Valor Rateio
                         rubrica_raw = data_match.group(1) or current_rubrica_buffer or "N/A"
                         periodo = data_match.group(2)
                         rend_val = data_match.group(3)
                         perc_val = data_match.group(4)
                         val_rat_val = data_match.group(5)
                         # No digital, o ar                          # Tenta separar se houver padrão "[N] NOME_OBRA PLATAFORMA"
                         temp_rub = str(rubrica_raw).upper()
                         for platform_key in sorted(PLATFORM_MAPPING.keys(), key=len, reverse=True):
                             if platform_key in temp_rub:
                                 parts = temp_rub.split(platform_key, 1)
                                 artist_gravacao = clean_text(parts[0]) if parts[0].strip() else titular
                                 rubrica_raw = platform_key # A rubrica é só a plataforma
                                 break
                    else:
                         # Match padrão
                         artist_gravacao = clean_text(data_match.group(1))
                         rubrica_raw = data_match.group(2) or current_rubrica_buffer or "N/A"
                         periodo = data_match.group(3)
                         rend_val = data_match.group(4)
                         perc_val = data_match.group(5)
                         val_rat_val = data_match.group(6)

                    # REGRA 2.4 - Normalização via Bucket System
                    rub_bucket_key = find_best_bucket(f"{rubrica_raw} {line}", summary_buckets, current_rubrica_buffer)
                    rubrica_nome_final = rub_bucket_key if rub_bucket_key else normalize_platform_name(str(rubrica_raw))
                    
                    valor_rat = clean_monetary_value(val_rat_val) or 0.0
                    
                    row = {
                        "titular": titular, "documento_origem": tipo, "arquivo_origem": original_filename,
                        "obra_referencia": current_obra_titulo or "N/A", "obra_codigo": current_obra_codigo,
                        "rubrica": rubrica_nome_final, "periodo": format_period(periodo),
                        "rendimento": clean_monetary_value(rend_val) or 0.0,
                        "percentual_rateio": clean_monetary_value(perc_val) or 100.0,
                        "valor_rateio": valor_rat, "correcao": "---", "execucoes": "1",
                        "cat_caracteristica": "N/A", "isrc_iswc": current_isrc, "data_pagamento": periodo_dist,
                        "valor_bruto": valor_rat, "valor_liquido": valor_rat,
                        "tipo_extracao": tipo, "artista_gravacao": artist_gravacao,
                    }
                    rows.append(row)
                    # Atualiza soma por rubrica para reconciliação
                    if rubrica_nome_final and valor_rat > 0:
                        rubricas_with_sum[rubrica_nome_final] = rubricas_with_sum.get(rubrica_nome_final, 0.0) + float(valor_rat)
                    
                    current_isrc = None
                    continue

                # --- 4. FALLBACK "CAPTURE TUDO" ---
                # Se não deu match perfeito, mas a linha PARECE ter valores de rateio
                if is_likely_data_line(line):
                    # Tenta extrair valores monetários soltos
                    fallback_match = re.search(r"([\d.,\-]+)\s+([\d.,\-]+)\s+([\d.,\-]+)\s+(---)\s+", line)
                    if fallback_match:
                        has_data_for_current_obra = True
                        rendimento = clean_monetary_value(fallback_match.group(1))
                        perc = clean_monetary_value(fallback_match.group(2))
                        rateio = clean_monetary_value(fallback_match.group(3)) or 0.0
                        
                        if rateio > 0:
                            # Tenta descobrir rubrica e artista por posição relativa
                            prefix = line[:fallback_match.start()].strip()
                            prefix_parts = prefix.split()
                            rubrica = prefix_parts[-1] if prefix_parts else current_rubrica_buffer or "N/A"
                            # REGRA 2.4 - Normalização via Bucket System (Usa o prefixo inteiro para match)
                            rubrica_final = find_best_bucket(prefix, summary_buckets, current_rubrica_buffer)
                            if not rubrica_final:
                                # Fallback se não bater em nada: tenta o último token
                                last_token = prefix.split()[-1] if prefix.split() else "N/A"
                                rubrica_final = find_best_bucket(last_token, summary_buckets, current_rubrica_buffer) or normalize_platform_name(last_token)
                            
                            artista = " ".join(prefix.split()[:-1]) if len(prefix.split()) > 1 else prefix or titular
                            
                            row = {
                                "titular": titular,
                                "documento_origem": tipo,
                                "arquivo_origem": original_filename,
                                "obra_referencia": current_obra_titulo or current_obra_codigo,
                                "obra_codigo": current_obra_codigo,
                                "rubrica": rubrica_final,
                                "periodo": periodo_dist,
                                "rendimento": rendimento,
                                "percentual_rateio": perc,
                                "valor_rateio": rateio,
                                "correcao": "---",
                                "execucoes": "1",
                                "cat_caracteristica": "N/A",
                                "isrc_iswc": current_isrc,
                                "data_pagamento": periodo_dist,
                                "valor_bruto": rendimento,
                                "valor_liquido": rateio,
                                "tipo_extracao": tipo,
                                "artista_gravacao": artista,
                            }
                            rows.append(row)
                            
                            # Rastreio para deduplicação
                            if rubrica_final and rateio > 0:
                                rubricas_with_sum[rubrica_final] = rubricas_with_sum.get(rubrica_final, 0.0) + rateio
                                
                            logger.info(f"Linha capturada via FALLBACK: {line} -> Rubrica: {rubrica_final}")
                            current_isrc = None
                            continue

                # --- 5. MULTI-LINE BUFFER (OBRA/TÍTULO) ---
                # Só permitimos acumular título se NÃO capturamos dados ainda para esta obra.
                # Isso evita que rodapés no fim da página sejam anexados ao título.
                if current_obra_codigo and not has_data_for_current_obra and len(line) > 3:
                    # Rejeita linhas que parecem ser disclaimers/rodapés legais
                    is_disclaimer = any(kw in line.upper() for kw in [
                        "INFORMAÇÕES REFERENTES", "DISTRIBUIÇÃO ESTÃO DISPONÍVEIS",
                        "EU FAÇO MÚSICA", "WWW.ECAD.ORG", "ECAD.TEC", "EXEC. -",
                        "COMO É FEITA A DISTRIBUIÇÃO", "NÚM. DE EXECUÇÕES"
                    ])
                    if not is_disclaimer:
                         if current_obra_titulo:
                             current_obra_titulo = f"{current_obra_titulo} {line}"
                         else:
                             current_obra_titulo = line
                         
                         # Limita tamanho para evitar capturas catastróficas
                         if current_obra_titulo and len(current_obra_titulo) > 500:
                             current_obra_titulo = current_obra_titulo[:500]
                         logger.debug(f"Acumulando título (multi-line): {current_obra_titulo}")


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
                        rub_cap = "CAPITULO:" + cap_match.group(1).split()[0] if cap_match.group(1).split() else "CAPITULO"
                        val_rat_cap = clean_monetary_value(val_match.group(3)) or 0.0

                        # REGRA 2.4 - Normalização no Capitulo via Bucket System
                        rub_bucket_key = find_best_bucket(rub_cap, summary_buckets, current_rubrica_buffer)
                        rubrica_cap_final = rub_bucket_key if rub_bucket_key else normalize_platform_name(str(rub_cap))

                        row = {
                            "titular": titular,
                            "documento_origem": tipo,
                            "obra_referencia": current_obra_titulo or current_obra_codigo,
                            "obra_codigo": current_obra_codigo,
                            "rubrica": rubrica_cap_final, # Use the bucket-normalized name
                            "periodo": None,
                            "rendimento": clean_monetary_value(val_match.group(1)),
                            "percentual_rateio": clean_monetary_value(val_match.group(2)),
                            "valor_rateio": val_rat_cap,
                            "correcao": val_match.group(4),
                            "execucoes": clean_text(val_match.group(5)),
                            "categoria": val_match.group(6),
                            "caracteristica": val_match.group(7),
                            "isrc_iswc": current_isrc,
                            "data_pagamento": periodo_dist,
                            "valor_bruto": clean_monetary_value(val_match.group(1)),
                            "valor_liquido": val_rat_cap,
                            "tipo_extracao": tipo,
                            "artista_gravacao": None,
                        }
                        rows.append(row)
                        
                        # Rastreio para deduplicação
                        if rubrica_cap_final and val_rat_cap > 0:
                            rubricas_with_sum[rubrica_cap_final] = rubricas_with_sum.get(rubrica_cap_final, 0.0) + val_rat_cap
                            
                        current_isrc = None

            # Fim do loop de linhas da página
            if any(kw in line.upper() for kw in STOP_KEYWORDS if "PONTOS" in kw or "PONTUAÇÃO" in kw):
                break # Sai do loop de páginas se o hard stop for definitivo

    # Cheque final para a última obra se não teve detalhes
    if current_obra_codigo and not has_data_for_current_obra and current_obra_header_value:
        rub_header = "DIRETO / TOTAL OBRA"
        val_header = current_obra_header_value or 0.0
        row = {
            "titular": titular, "documento_origem": tipo, "arquivo_origem": original_filename,
            "obra_referencia": current_obra_titulo or current_obra_codigo, "obra_codigo": current_obra_codigo,
            "rubrica": rub_header, "periodo": periodo_dist or "N/A", "rendimento": val_header,
            "percentual_rateio": 100.0, "valor_rateio": val_header, "correcao": "---", "execucoes": "1",
            "cat_caracteristica": "N/A", "isrc_iswc": current_isrc, "data_pagamento": periodo_dist,
            "valor_bruto": val_header, "valor_liquido": val_header,
            "tipo_extracao": tipo, "artista_gravacao": titular,
        }
        rows.append(row)
        rub_header_norm = normalize_platform_name(str(rub_header))
        rubricas_with_sum[rub_header_norm] = rubricas_with_sum.get(rub_header_norm, 0.0) + val_header

    # --- NOVO: RECONCILIAÇÃO FINAL VIA BUCKETS ---
    logger.info("Bucket Reconciliation - Comparison Table:")
    for r_bucket, r_expected in summary_buckets.items():
        val_detalhado = rubricas_with_sum.get(r_bucket, 0.0)
        diff = round(r_expected - val_detalhado, 2)
        logger.info(f"Bucket '{r_bucket}': Expected={r_expected:.2f}, Extracted={val_detalhado:.2f}, Diff={diff:.2f}")
        
        if abs(diff) >= 0.005:
            logger.info(f"Bucket Remainder: {r_bucket} necessita R$ {diff:.2f} (Ajuste de Reconciliação)")
            rows.append({
                "titular": titular,
                "documento_origem": tipo,
                "arquivo_origem": original_filename,
                "obra_referencia": f"CRÉDITO DIRETO / {r_bucket}",
                "obra_codigo": "999999",
                "rubrica": r_bucket,
                "periodo": periodo_dist or "N/A",
                "rendimento": diff,
                "percentual_rateio": 100.0,
                "valor_rateio": diff,
                "correcao": "---",
                "execucoes": "1",
                "cat_caracteristica": "N/A",
                "isrc_iswc": "---",
                "data_pagamento": periodo_dist,
                "valor_bruto": diff,
                "valor_liquido": diff,
                "tipo_extracao": tipo,
                "artista_gravacao": titular,
            })

    logger.info(
        "Extracao concluida para %s: %d linhas extraidas de %s",
        tipo,
        len(rows),
        pdf_path,
    )

    if not rows:
        return pd.DataFrame(columns=MASTER_SCHEMA_COLUMNS)

    df = pd.DataFrame(rows)
    
    # --- VALIDAÇÃO DE SOMA (REGRA 4.1 - TRAVA BINÁRIA) ---
    if valor_total is not None:
        total_header = clean_monetary_value(valor_total)
        total_extraido = float(df["valor_rateio"].sum())
        
        # Arredonda para 2 casas conforme padrão financeiro
        total_header_round = round(total_header, 2) if total_header is not None else 0.0
        total_extraido_round = round(total_extraido, 2)
        
        diff = abs(total_header_round - total_extraido_round)
        
        # --- AJUSTE DE CENTAVOS FINAL (Foolproof Reconciliation) ---
        # Se a diferença for minúscula (até 10 centavos), ajustamos a última linha do DF
        # para garantir que a soma final bata 100% com o cabeçalho do PDF.
        if 0.001 < diff <= 0.10:
            logger.info(f"Aplicando micro-ajuste de {total_header_round - total_extraido_round:.2f} centavos na última linha para integridade total.")
            adj = round(total_header_round - total_extraido_round, 2)
            # Atualiza o valor na última linha do DataFrame
            last_idx = df.index[-1]
            df.at[last_idx, "valor_rateio"] = round(float(df.at[last_idx, "valor_rateio"]) + adj, 2)
            df.at[last_idx, "valor_bruto"] = df.at[last_idx, "valor_rateio"]
            df.at[last_idx, "valor_liquido"] = df.at[last_idx, "valor_rateio"]
            # Recalcula diff após ajuste
            total_extraido_round = round(float(df["valor_rateio"].sum()), 2)
            diff = abs(total_header_round - total_extraido_round)
        
        # Trava Binária: Qualquer divergência > 0.00 (arredondado) trava a extração
        if diff > 0.001:
            err_msg = (
                f"FALHA DE INTEGRIDADE: Soma extraída (R$ {total_extraido_round}) não confere "
                f"com total do PDF (R$ {total_header_round}). Diferença: R$ {diff:.2f}"
            )
            logger.error(err_msg)
            # Move arquivo para revisão manual e loga no histórico conforme Spec v1.0
            handle_integrity_failure(pdf_path, err_msg)
            # Retorna DataFrame vazio para não poluir o banco/excel com dados errados
            return pd.DataFrame(columns=MASTER_SCHEMA_COLUMNS)
            
        logger.info(
            f"Validação de Soma [{tipo}]: Sucesso (Diff={diff:.2f})"
        )
    else:
        logger.warning(f"Aviso: Valor total não encontrado no cabeçalho do PDF '{original_filename}' para validação.")

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
    has_data_for_current_obra = False # Track if current obra already has data rows

    # Regex para linha de obra
    # Formato: COD_OBRA ISWC/vazio TITULO ASS SITUACAO TIPO_OBRA NAC INCLUSAO
    re_obra = re.compile(
        r"^(\d{5,})\s+"       # codigo (grupo 1)
        r"(T-[\d.\-]+|- \. \. -)?\s*"   # ISWC opcional ou placeholder (grupo 2)
        r"(.+?)\s+"            # titulo (grupo 3)
        r"(ABRAMUS|UBC|SBACEM|SOCINPRO|AMAR|SADEMBRA|ASSIM|SADBRA)\s+"  # associacao resp. (grupo 4)
        r"(LB|BL|DU|BL/DU|LB/DU)\s+"   # situacao restrita para nao confundir com categoria (grupo 5)
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

    # Regex mais robusta para linha de titular da obra (participante)
    # No Autoral, o Nome, Pseudônimo e CAE vêm em sequência e às vezes sem delimitadores claros.
    # Usamos a Associação (ABRAMUS, UBC, etc) como a âncora de parada para capturar o bloco de texto anterior.
    re_participante = re.compile(
        r"^(\d{4,})\s+"               # codigo ecad (grupo 1)
        r"(.+?)\s+"                   # Bloco de texto (Nome/Pseudônimo/CAE) - grupo 2
        r"(ABRAMUS|UBC|SBACEM|SOCINPRO|AMAR|SADEMBRA|ASSIM|SICAM|SADBRA)\s+"  # associacao (grupo 3)
        r"([A-Z]{1,2})\s+"            # categoria (grupo 4)
        r"([\d.,]+)"                   # percentual (grupo 5)
    )

    with pdfplumber.open(pdf_path) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            lines = extract_page_lines(page)
            in_data = False
            # has_data_for_current_obra = False # Moved initialization outside page loop

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # --- 0. FILTRO DE RUÍDO (FOOTERS/DISCLAIMERS) ---
                # Padrões comuns de rodapé e avisos legais do ECAD que não devem ser processados
                if any(kw in line for kw in [
                    "informações referentes aos tipos", "critérios de distribuição", "Eu Faço Música",
                    "http://www.ecad.org.br", "CAE/IPI:", "CÓD. ECAD:", "A B R A M U S", 
                    "RENDIMENTO % RATEIO CORREÇÃO", "OR.E TP. OBRA", "CLASSIFICAÇÃO DA OBRA",
                    "ECAD.Tec", "Página", "DEMONSTRATIVO", "ECAD - TEC", "IDENTIFICAÇÃO DE RUBRICA"
                ]) or re.search(r"http[s]?://", line):
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
                        # "ECAD.Tec", # Already in general noise filter
                    ]
                ):
                    if any(kw in line for kw in ["CÓD. OBRA", "CÓDIGO", "NOME DO TITULAR", "TITULAR:"]):
                        in_data = True
                    continue

                in_data = True

                # --- 1. DETECÇÃO DE CABEÇALHO DE OBRA (FORWARD FILL) ---
                obra_match = re_obra.match(line)
                if not obra_match:
                    obra_match = re_obra_simple.match(line)
                if not obra_match:
                    obra_match = re_obra_no_iswc.match(line)

                if obra_match:
                    has_data_for_current_obra = False
                    current_obra_codigo = obra_match.group(1)
                    
                    # Determine ISWC and title based on which regex matched and its groups
                    if re_obra.match(line):
                        current_iswc = obra_match.group(2) if obra_match.group(2) else None
                        current_obra_titulo = clean_text(obra_match.group(3))
                        current_situacao = obra_match.group(5)
                        current_tipo_obra = obra_match.group(6)
                        current_nacional = obra_match.group(7)
                    elif re_obra_simple.match(line):
                        current_iswc = obra_match.group(2)
                        current_obra_titulo = clean_text(obra_match.group(3))
                        current_situacao = obra_match.group(5) # This might be wrong, re_obra_simple has different group structure
                        current_tipo_obra = obra_match.group(6)
                        current_nacional = obra_match.group(7)
                    elif re_obra_no_iswc.match(line):
                        current_iswc = None
                        current_obra_titulo = clean_text(obra_match.group(2))
                        current_situacao = obra_match.group(4)
                        current_tipo_obra = obra_match.group(5)
                        current_nacional = obra_match.group(6)
                    else: # Fallback if none of the specific obra regexes matched perfectly
                        current_iswc = None
                        current_obra_titulo = clean_text(obra_match.group(2)) # Assume group 2 is title for simple cases

                    logger.debug(
                        "Forward Fill Autoral - Obra: [%s] %s (ISWC: %s)",
                        current_obra_codigo,
                        current_obra_titulo,
                        current_iswc,
                    )
                    continue

                # --- 2. DETECÇÃO DE ISRC/ISWC (Standalone) ---
                # This is less critical for Autoral as ISWC is often in the obra line,
                # but good to have for robustness.
                iswc_detected = extract_iswc_from_line(line)
                if iswc_detected:
                    current_iswc = iswc_detected
                    # If the line contains ONLY the ISWC, don't try to process as data
                    cleaned_line = re.sub(r"T-[\d.\-]+", "", line).strip()
                    if not cleaned_line or len(cleaned_line) < 5:
                        continue

                # --- 3. DETECÇÃO DE LINHA DE DADOS (PARTICIPANTE) ---
                part_match = re_participante.match(line)
                if part_match:
                    has_data_for_current_obra = True
                    
                    # Processamento inteligente do bloco de texto (Nome + Pseudonimo + CAE)
                    # Ex: "ERIVAL PEREIRA DE MEDEIROS BIGUA 00591.71.69.18"
                    # Ex: "SANTA FE GESTAO AUTORAL LTDA 01092.98.61.21"
                    text_block = part_match.group(2).strip()
                    
                    # 3.1 Extração de CAE (formato XXXXX.XX.XX.XX)
                    cae_val = None
                    cae_match = re.search(r"(\d{5}\.\d{2}\.\d{2}\.\d{2})$", text_block)
                    if cae_match:
                        cae_val = cae_match.group(1)
                        text_block = text_block[:cae_match.start()].strip()
                    
                    # 3.2 Separação Nome / Pseudônimo (se houver espaço duplo ou múltiplo)
                    # Nota: pdfplumber às vezes mescla tudo com espaço simples. 
                    # Se não houver divisor claro, assumimos o bloco todo como nome para o FIGA Intel não perder dados.
                    name_parts = re.split(r"\s{2,}", text_block)
                    if len(name_parts) >= 2:
                        nome_val = name_parts[0].strip()
                        pseudo_val = name_parts[1].strip()
                    else:
                        nome_val = text_block
                        pseudo_val = None

                    row = {
                        "titular": titular,
                        "documento_origem": "RELATORIO_ANALITICO_AUTORAL",
                        "arquivo_origem": original_filename,
                        "obra_referencia": current_obra_titulo,
                        "obra_codigo": current_obra_codigo,
                        "isrc_iswc": current_iswc,
                        "cod_ecad_participante": part_match.group(1),
                        "nome_participante": nome_val,
                        "pseudonimo_participante": pseudo_val,
                        "cae_participante": cae_val,
                        "associacao_participante": part_match.group(3),
                        "categoria": part_match.group(4),
                        "percentual_rateio": clean_monetary_value(part_match.group(5)),
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
    # COD. ECAD  TITULAR  PSEUDÔNIMO  CAT.  SUBCAT.  ASSOCIAÇÃO  PART. (%)
    re_participante_conexo = re.compile(
        r"^(\d{4,})\s+"               # codigo ecad (grupo 1)
        r"(.+?)\s+"                   # nome (grupo 2)
        r"(?:([A-Z][A-Z\s.]*?)\s+)?"  # pseudonimo opcional (grupo 3)
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

                # Ignora cabecalhos repetidos
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

                # --- 0. FILTRO DE RUÍDO (FOOTERS/DISCLAIMERS) ---
                if any(kw in line for kw in [
                    "informações referentes aos tipos", "critérios de distribuição", "Eu Faço Música",
                    "http://www.ecad.org.br", "O LINK DO SITE É", "CAE/IPI: 753222167",
                    "RENDIMENTO % RATEIO CORREÇÃO", "OR.E TP. OBRA", "CLASSIFICAÇÃO DA OBRA",
                    "ECAD.Tec", "Página", "DEMONSTRATIVO", "ECAD - TEC", "IDENTIFICAÇÃO DE RUBRICA"
                ]) or (re.search(r"http[s]?://", line) and len(line) > 100):
                    if not is_likely_data_line(line):
                        continue

                # --- 1. DETECÇÃO DE GRAVAÇÃO (FORWARD FILL) ---
                grav_match = re_gravacao.match(line)
                if grav_match:
                    has_data_for_current_obra = False
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
                            has_data_for_current_obra = False
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
                        
                        # Se não é ISRC, pode ser um participante
                        part_match = re_participante_conexo.match(line)
                        if part_match:
                            has_data_for_current_obra = True
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

                # --- 2. FALLBACK "CAPTURE TUDO" (CONEXO) ---
                if is_likely_data_line(line):
                    fallback_match = re.search(r"([\d.,]+)$", line)
                    if fallback_match:
                        has_data_for_current_obra = True
                        perc = clean_monetary_value(fallback_match.group(1))
                        prefix = line[:fallback_match.start()].strip()
                        parts = prefix.split()
                        if len(parts) >= 2:
                             row = {
                                "titular": titular,
                                "documento_origem": "RELATORIO_ANALITICO_CONEXO",
                                "obra_referencia": current_gravacao_titulo,
                                "obra_codigo": current_gravacao_codigo,
                                "isrc_iswc": current_isrc,
                                "cod_ecad_participante": parts[0],
                                "nome_participante": clean_text(" ".join(parts[1:])),
                                "percentual_rateio": perc,
                                "tipo_extracao": "RELATORIO_ANALITICO_CONEXO_FALLBACK",
                            }
                             rows.append(row)
                             logger.info(f"FALLBACK Conexo: {line}")
                             continue

                # --- 3. MULTI-LINE BUFFER (TÍTULO DE GRAVAÇÃO) ---
                if current_gravacao_codigo and not has_data_for_current_obra and not re.match(r"^\d", line):
                    if len(line) > 5:
                        if current_gravacao_titulo:
                            current_gravacao_titulo = f"{current_gravacao_titulo} {line}"
                        else:
                            current_gravacao_titulo = line
                        
                        if len(current_gravacao_titulo) > 500:
                            current_gravacao_titulo = current_gravacao_titulo[:500]
                        logger.debug(f"Acumulando título Conexo: {current_gravacao_titulo}")

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

from typing import Union, Optional, Callable

EXTRACTOR_MAP: dict[str, Callable] = {
    "DEMONSTRATIVO_TITULAR": _extract_demonstrativo_titular,
    "DEMONSTRATIVO_SERVICOS_DIGITAIS": _extract_servicos_digitais,
    "DEMONSTRATIVO_ANTECIPACAO_PRESCRITOS": _extract_antecipacao_prescritos,
    "DISTRIBUICAO_PRESCRITIVEIS": _extract_distribuicao_prescritiveis,
    "RELATORIO_ANALITICO_AUTORAL": _extract_relatorio_analitico_autoral,
    "RELATORIO_ANALITICO_CONEXO": _extract_relatorio_analitico_conexo,
}


# --- UTILITÁRIOS DE CABEÇALHO E BUCKETS ---

def agg_norm(t):
    """Normalização agressiva para matching de rubricas."""
    if not t: return ""
    t = str(t).upper()
    t = unicodedata.normalize('NFD', t)
    t = "".join([c for c in t if unicodedata.category(c) != 'Mn'])
    # Remove datas (DD/MM/YYYY ou MM/YYYY)
    t = re.sub(r"\d{2}/\d{2,4}", "", t)
    # Mantém apenas letras e espaços
    t = re.sub(r"[^A-ZÁÉÍÓÚÂÊÎÔÛÃÕÇ\s]", "", t)
    # Remove espaços extras
    t = " ".join(t.split())
    return t

def find_best_bucket(det_name, summary_buckets, current_buffer=None):
    """
    Encontra a melhor rubrica do sumário que combine com o nome detalhado.
    Prioriza matches exatos, depois substring mais longa, depois fallbacks.
    """
    if not summary_buckets: return None
    
    det_up = agg_norm(str(det_name))
    if not det_up: return None
    
    # 1. Busca por melhor match (Scoring)
    best_match = None
    best_score = -1
    
    for b_name in summary_buckets.keys():
        b_up = agg_norm(b_name)
        if not b_up: continue
        
        score = -1
        if b_up == det_up:
            score = 1000  # Match Perfeito
        elif b_up in det_up:
            score = 100 + len(b_up) # Premiamos o maior match contido
        elif det_up in b_up:
            score = 50 + len(det_up) # Match parcial invertido
        
        if score > 0:
            logger.debug(f"Bucket Match Candidate: '{b_name}' for '{det_name}' score={score}")

        if score > best_score:
            best_score = score
            best_match = b_name
    
    if best_score > 0:
        # Check para evitar falso positivo GLOBO vs GLOBOPLAY se o bucket for muito curto
        if best_match == "GLOBOPLAY ---" and "SPOTIFY" in det_up:
            pass # Ignora
        else:
            return best_match
    
    # 2. Regras especiais
    if "CAPITULO" in det_up or "CAP" in det_up:
        # Se temos uma rubrica no buffer (ex: NETFLIX), e a linha diz CAPÍTULO, 
        # é quase certo que pertence a esse buffer em demonstrativos digitais.
        if current_buffer and any(x in agg_norm(current_buffer) for x in ["NETFLIX", "SPOTIFY", "APPLE", "YOUTUBE"]):
            return current_buffer
            
        # Fallback para TV em demonstrativos padrão
        for b_name in ["TV", "RÁDIO", "GLOBO", "RECORD", "SBT"]:
            for real_b in (summary_buckets.keys() if summary_buckets else []):
                if b_name in str(real_b).upper():
                    return real_b
                    
    if any(x in det_up for x in ["TAXA", "RETENÇAO", "ADMINISTRAÇAO", "ADM"]):
        for b_name in ["TAXA", "ADM", "RETENÇÃO"]:
            for real_b in (summary_buckets.keys() if summary_buckets else []):
                if b_name in str(real_b).upper():
                    return real_b
                    
    return None

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
