import pdfplumber
import re

pdf_path = "/Volumes/Lexar/trabalho/PatrickDeveloper/backup/novprojetopytonocr/backend/app/erros/DEMONSTRATIVO DO TITULAR DE SERVIÇOS DIGITAIS (1).pdf"

with pdfplumber.open(pdf_path) as pdf:
    for i, page in enumerate(pdf.pages):
        text = page.extract_text()
        if text and "GLOBOPLAY" in text.upper():
            print(f"--- Page {i+1} ---")
            for line in text.split('\n'):
                if "GLOBOPLAY" in line.upper():
                    print(f"|{line}|")
