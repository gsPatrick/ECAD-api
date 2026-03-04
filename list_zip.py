import zipfile
import os

zip_path = "/tmp/source_test.zip"
if os.path.exists(zip_path):
    with zipfile.ZipFile(zip_path, 'r') as z:
        for info in z.infolist()[:50]:
            print(f"{info.filename} - {info.file_size} bytes")
else:
    print("Zip not found")
