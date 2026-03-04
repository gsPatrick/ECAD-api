import pandas as pd
import zipfile
import io

zip_path = "/tmp/source_test.zip"
with zipfile.ZipFile(zip_path, 'r') as z:
    with z.open('I-003274029-5--member_detail_report__per.xlsx') as f:
        df = pd.read_excel(io.BytesIO(f.read()))
        play_col = [c for c in df.columns if 'Play' in c or 'Count' in c][0]
        net_col = [c for c in df.columns if 'Net' in c][0]
        
        print(f"Excel Column: {play_col}")
        print(f"Excel Total Plays: {df[play_col].sum()}")
        print(f"Excel Total Net: {df[net_col].sum()}")
        
        # Breakdown by title top 5
        top = df.groupby('Title')[net_col].sum().sort_values(ascending=False).head(5)
        print("\nTop 5 Titles (Excel):")
        print(top)
