import pandas as pd
import zipfile
import io

zip_path = "/tmp/source_test.zip"
results = []

with zipfile.ZipFile(zip_path, 'r') as z:
    for filename in z.namelist():
        if filename.endswith('.xlsx'):
            with z.open(filename) as f:
                df = pd.read_excel(io.BytesIO(f.read()))
                # Find amount columns
                net_col = [c for c in df.columns if 'Net' in c]
                play_col = [c for c in df.columns if 'Play' in c or 'Count' in c]
                
                res = {
                    "file": filename,
                    "rows": len(df),
                    "net_sum": df[net_col[0]].sum() if net_col else "N/A",
                    "play_sum": df[play_col[0]].sum() if play_col else "N/A"
                }
                results.append(res)

for r in results:
    print(r)
