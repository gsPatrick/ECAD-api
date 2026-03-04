import PyPDF2

pdf_path = "/tmp/output_test.pdf"
with open(pdf_path, 'rb') as f:
    reader = PyPDF2.PdfReader(f)
    for i, page in enumerate(reader.pages):
        print(f"\n--- Page {i+1} ---\n")
        print(page.extract_text())
