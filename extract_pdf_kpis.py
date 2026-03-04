import PyPDF2
import re

pdf_path = "/tmp/output_test.pdf"
with open(pdf_path, 'rb') as f:
    reader = PyPDF2.PdfReader(f)
    text = ""
    for page in reader.pages:
        text += page.extract_text()
    
    # Look for KPIs
    net = re.search(r"Valor líquido total\s+(R\$\s*[\d.,\-]+)", text)
    plays = re.search(r"Total de execuções\s+([\d.,]+)", text)
    
    print(f"PDF Net: {net.group(1) if net else 'Not found'}")
    print(f"PDF Plays: {plays.group(1) if plays else 'Not found'}")
