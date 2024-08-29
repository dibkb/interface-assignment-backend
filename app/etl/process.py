import pandas as pd
from .load import read_file, preprocess_dataframe
from .transform.mtr import transform_mtr_df
from .transform.payment import transform_payment_df
from .transform.main import merge_dataframes, mark_df, apply_tolerance_check, empty_order_summary
from ..pydantic.model import FileInput
from ..logs.logger import log_errors
from ..db.schema.error_log import LevelType

def process_files(input_files: FileInput):
    try:
        # Start of the process
        log_errors(
            "Starting the file processing pipeline",
            context="process_files",
            level=LevelType.INFO.value,
            additional_info={"input_file_name":input_files.dict()}
        )
        
        # Load data frames and preprocess
        mtr_df = preprocess_dataframe(read_file(input_files.mtr_file))
        payment_df = preprocess_dataframe(read_file(input_files.payment_file))
        log_errors(
            "Loaded and preprocessed MTR and Payment dataframes",
            context="process_files",
            level=LevelType.INFO.value,
            additional_info={"mtr_df_shape": mtr_df.shape, "payment_df_shape": payment_df.shape}
        )

        # Transform pandas data frames
        mtr_df = transform_mtr_df(mtr_df)
        payment_df = transform_payment_df(payment_df)
        log_errors(
            "Transformed MTR and Payment dataframes",
            context="process_files",
            level=LevelType.INFO.value,
            additional_info={"mtr_df_columns": mtr_df.columns.to_list(), "payment_df_columns": payment_df.columns.to_list()}
        )

        # Merge data frames
        merged_df = merge_dataframes(mtr_df, payment_df)
        log_errors(
            "Merged MTR and Payment dataframes",
            context="process_files",
            level=LevelType.INFO.value,
            additional_info={"merged_df_shape": merged_df.shape}
        )
        
        # Mark dataframe
        merged_df = mark_df(merged_df)
        log_errors(
            "Marked the merged dataframe with appropriate labels",
            context="process_files",
            level=LevelType.INFO.value
        )

        # Apply tolerance level check
        merged_df = apply_tolerance_check(merged_df)
        log_errors(
            "Applied tolerance level check to the dataframe",
            context="process_files",
            level=LevelType.INFO.value
        )

        # Generate classification summary
        classification_summary = merged_df['mark'].value_counts().reset_index()
        classification_summary.columns = ['mark', 'count']
        log_errors(
            "Generated classification summary",
            context="process_files",
            level=LevelType.INFO.value,
            additional_info={"classification_summary": classification_summary.to_dict(orient='records')}
        )

        # Generate tolerance summary
        tolerance_summary = merged_df['ToleranceCheck'].value_counts().reset_index()
        tolerance_summary.columns = ['ToleranceCheck', 'Count']
        log_errors(
            "Generated tolerance summary",
            context="process_files",
            level=LevelType.INFO.value,
            additional_info={"tolerance_summary": tolerance_summary.to_dict(orient='records')}
        )

        # Generate empty order summary
        empty_order_sum = empty_order_summary(merged_df)
        log_errors(
            "Generated empty order summary",
            context="process_files",
            level=LevelType.INFO.value,
            additional_info={"empty_order_summary": empty_order_sum.to_dict(orient='records')}
        )

        # Output summaries
        print(classification_summary)
        print(tolerance_summary)
        print(empty_order_sum)

        # End of process
        log_errors(
            "Completed the file processing pipeline successfully",
            context="process_files",
            level=LevelType.INFO.value
        )

    except pd.errors.EmptyDataError as ede:
        log_errors(
            ede,
            context="EmptyDataError in process_files: Possibly an empty input file.",
            level=LevelType.ERROR.value
        )
        raise
    except ValueError as ve:
        log_errors(
            ve,
            context="ValueError in process_files: Likely due to unsupported file format or incorrect data processing.",
            level=LevelType.ERROR.value
        )
        raise
    except KeyError as ke:
        log_errors(
            ke,
            context="KeyError in process_files: Missing expected columns in one of the data frames.",
            level=LevelType.ERROR.value,
        )
        raise
    except Exception as e:
        log_errors(
            e,
            context="General error in process_files.",
            level=LevelType.ERROR.value
        )
        raise
