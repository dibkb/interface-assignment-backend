import pandas as pd
from fastapi import File
from ..logs.logger import log_errors
from ..db.schema.error_log import LevelType
import io

def read_file(file:File):
    try:
        # start logging..
        log_errors(error="Starting reading file", context="reading and converting to pandas df", level=LevelType.INFO.value, 
            additional_info={"file_info": file.filename, "size": file.size})
        
        content = file.file.read()
        if file.filename.endswith('.csv'):
            return pd.read_csv(io.StringIO(content.decode('utf-8')))
        elif file.filename.endswith(('.xls', '.xlsx')):
            return pd.read_excel(io.BytesIO(content))
        
        else:
            log_errors(error="Unsupported file format",
                       context=f"Error while reading file: {file.filename}")
            raise ValueError(f"Unsupported file format: {file.filename}")
    except Exception as e:
        log_errors(e,context=f"Error while reading file: {file.filename}")   
        raise

def preprocess_dataframe(df:pd.DataFrame) -> pd.DataFrame:

    try:
        df.columns = df.columns.str.title().str.replace(' ', '')
        return df.map(lambda x: x.strip() if isinstance(x, str) else x)
    except Exception as e:
        log_errors(e,context=f"Cannot strip values: {df.columns}") 
        raise ValueError(f"Cannot strip values: {df.columns}")