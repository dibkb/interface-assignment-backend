import pandas as pd
from fastapi import File
from ..logs.logger import log_errors
from ..db.schema.error_log import LevelType
import io


def read_file(file: File):
    try:
        log_errors(
            error="Starting file read operation",
            context="Initializing file read process",
            level=LevelType.INFO.value,
            additional_info={
                "file_name": file.filename,
                "file_size": file.size})

        content = file.file.read()
        if file.filename.endswith('.csv'):
            df = pd.read_csv(io.StringIO(content.decode('utf-8')))
        elif file.filename.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(io.BytesIO(content))
        else:
            log_errors(
                error="Unsupported file format",
                context="File format validation",
                level=LevelType.ERROR.value,
                additional_info={"file_name": file.filename}
            )
            raise ValueError(f"Unsupported file format: {file.filename}")

        log_errors(
            error="File read successful",
            context="Completed file read process",
            level=LevelType.INFO.value,
            additional_info={
                "file_name": file.filename,
                "rows": len(df),
                "columns": len(
                    df.columns)})
        return df

    except Exception as e:
        log_errors(
            error=str(e),
            context="Error during file read operation",
            level=LevelType.ERROR.value,
            additional_info={"file_name": file.filename}
        )
        raise


def preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    try:
        log_errors(
            error="Starting dataframe preprocessing",
            context="Initializing dataframe preprocessing",
            level=LevelType.INFO.value,
            additional_info={"initial_columns": list(df.columns)}
        )

        df.columns = df.columns.str.title().str.replace(' ', '')
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

        log_errors(
            error="Dataframe preprocessing completed",
            context="Finished dataframe preprocessing",
            level=LevelType.INFO.value,
            additional_info={"processed_columns": list(df.columns)}
        )
        return df

    except Exception as e:
        log_errors(
            error=str(e),
            context="Error during dataframe preprocessing",
            level=LevelType.ERROR.value,
            additional_info={"problematic_columns": list(df.columns)}
        )
        raise ValueError(f"Error preprocessing dataframe: {str(e)}")
