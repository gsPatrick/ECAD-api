import sys
import os
import pandas as pd
from pathlib import Path

# Adiciona o diretório do backend ao path
backend_path = "/Volumes/Lexar/trabalho/PatrickDeveloper/backup/novprojetopytonocr/backend"
sys.path.append(backend_path)

from app.features.extractor.ext_service import extract_pdf
from app.config.settings import logger

SAMPLES = [
    {
        "path": "/Volumes/Lexar/trabalho/PatrickDeveloper/backup/novprojetopytonocr/PDFS/demonstrativo-do-titular.pdf",
        "target": 901.54
    },
    {
        "path": "/Volumes/Lexar/trabalho/PatrickDeveloper/backup/novprojetopytonocr/PDFS/DEMONSTRATIVO DO TITULAR ANTECIPAÇÃO DE PRESCRITOS (2).pdf",
        "target": 10784.69
    },
    {
        "path": "/Volumes/Lexar/trabalho/PatrickDeveloper/backup/novprojetopytonocr/PDFS/DISTRIBUIÇÃO DE PRESCRITÍVEIS.pdf",
        "target": 5153.37
    },
    {
        "path": "/Volumes/Lexar/trabalho/PatrickDeveloper/backup/novprojetopytonocr/PDFS/DEMONSTRATIVO DO TITULAR DE SERVIÇOS DIGITAIS (1).pdf",
        "target": 12172.43
    }
]

def run_all_tests():
    print("="*80)
    print(f"{'RELATÓRIO DE AUDITORIA FINAL - MOTOR ECAD v1.0':^80}")
    print("="*80)
    print(f"{'Arquivo':<50} | {'Status':<10} | {'Extraído':<10} | {'Alvo':<10} | {'Diff':<10}")
    print("-"*80)
    
    overall_success = True
    
    for sample in SAMPLES:
        path = sample["path"]
        target = sample["target"]
        fname = os.path.basename(path)
        
        if not os.path.exists(path):
            print(f"{fname:<50} | {'MISSING':<10} | {'---':<10} | {target:<10} | {'---':<10}")
            overall_success = False
            continue
            
        try:
            layout, df = extract_pdf(path, fname)
            if df.empty:
                 print(f"{fname:<50} | {'FAILED':<10} | {'---':<10} | {target:<10} | {'---':<10}")
                 overall_success = False
                 continue
                 
            total = round(float(df["valor_rateio"].sum()), 2)
            diff = round(abs(total - target), 2)
            
            status = "SUCCESS" if diff <= 0.01 else "DIFF_ERR"
            if diff > 0.01: overall_success = False
            
            print(f"{fname[:48]:<50} | {status:<10} | {total:>10.2f} | {target:>10.2f} | {diff:>10.2f}")
            
        except Exception as e:
            print(f"{fname[:48]:<50} | {'ERROR':<10} | {'---':<10} | {target:<10} | {str(e)[:10]}")
            overall_success = False

    print("="*80)
    if overall_success:
        print(f"{'✅ CONCLUSÃO: TODOS OS TESTES PASSARAM COM DIFERENÇA 0.00':^80}")
    else:
        print(f"{'❌ CONCLUSÃO: FORAM ENCONTRADAS DIVERGÊNCIAS NO PROCESSO':^80}")
    print("="*80)

if __name__ == "__main__":
    run_all_tests()
