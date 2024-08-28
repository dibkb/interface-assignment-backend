import pandas as pd

def load_file(file, file_type):
    if file_type == 'csv':
        return pd.read_csv(file)
    elif file_type == 'excel':
        return pd.read_excel(file)
    else:
        raise ValueError("Unsupported file type")

def preprocess_dataframe(df):
    df.columns = df.columns.str.title().str.replace(' ', '')
    return df.map(lambda x: x.strip() if isinstance(x, str) else x)