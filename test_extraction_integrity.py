import sys
import os
import pandas as pd
from pathlib import Path

# Adiciona o diretório do backend ao path para importar as funções
backend_path = "/Volumes/Lexar/trabalho/PatrickDeveloper/backup/novprojetopytonocr/backend"
sys.path.append(backend_path)

from app.features.extractor.ext_service import extract_pdf
from app.config.settings import logger

def test_integrity():
    # Caminho do arquivo PDF de teste
    pdf_path = "/Volumes/Lexar/trabalho/PatrickDeveloper/backup/novprojetopytonocr/DEMONSTRATIVO DO TITULAR ANTECIPAÇÃO DE PRESCRITOS (1).pdf"
    
    if not os.path.exists(pdf_path):
        print(f"ERRO: Arquivo não encontrado em {pdf_path}")
        return

    print(f"Iniciando teste de integridade para: {pdf_path}")
    
    try:
        layout, df = extract_pdf(pdf_path, "Demonstrativo_Teste.pdf")
        
        print(f"\nLayout Identificado: {layout}")
        print(f"Total de Linhas Extraídas: {len(df)}")
        
        total_sum = df["valor_rateio"].sum()
        total_sum_round = round(total_sum, 2)
        
        target_sum = 10784.69
        diff = abs(total_sum_round - target_sum)
        
        print(f"Soma Total Extraída: R$ {total_sum_round}")
        print(f"Soma Alvo (Mandatória): R$ {target_sum}")
        print(f"Diferença: R$ {diff:.2f}")
        
        if diff <= 0.02:
            print("\n✅ SUCESSO ABSOLUTO: A extração atingiu a meta de integridade!")
            
            # Verificação de multi-line (opcional, para debug)
            # Mostra as primeiras 10 linhas para conferir títulos
            print("\n--- Amostra de Dados (Primeiras 10 linhas) ---")
            sample_cols = ["obra_referencia", "artista_gravacao", "rubrica", "valor_rateio"]
            print(df[sample_cols].head(10).to_string(index=False))
            
            # Verifica se há nomes cortados ou vazios
            empty_titles = df[df["obra_referencia"].isna() | (df["obra_referencia"] == "")]
            if not empty_titles.empty:
                print(f"⚠️ AVISO: {len(empty_titles)} linhas estão sem título de obra.")
            
        else:
            print("\n❌ FALHA: A soma total não confere!")
            sys.exit(1)

    except Exception as e:
        print(f"\n❌ ERRO DURANTE A EXTRAÇÃO: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    test_integrity()
