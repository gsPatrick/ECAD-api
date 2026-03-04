import PyPDF2
import re

pdf_path = "/tmp/output_test.pdf"
with open(pdf_path, 'rb') as f:
    reader = PyPDF2.PdfReader(f)
    text = ""
    for page in reader.pages:
        text += f"\n--- Page {reader.pages.index(page)+1} ---\n"
        text += page.extract_text()
    
    print(text)
