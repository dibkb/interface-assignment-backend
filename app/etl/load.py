import pandas as pd
import io
def read_file(file):
    content = file.file.read()
    if file.filename.endswith('.csv'):
        return pd.read_csv(io.StringIO(content.decode('utf-8')))
    elif file.filename.endswith(('.xls', '.xlsx')):
        return pd.read_excel(io.BytesIO(content))
    else:
        # Todo log this in json file
        raise ValueError(f"Unsupported file format: {file.filename}")

def preprocess_dataframe(df):
    df.columns = df.columns.str.title().str.replace(' ', '')
    return df.map(lambda x: x.strip() if isinstance(x, str) else x)