import pandas as pd
from .load import read_file,preprocess_dataframe
from .transform.mtr import transform_mtr_df
from .transform.payment import transform_payment_df
from .transform.main import merge_dataframes,mark_df,apply_tolerance_check,empty_order_summary
from ..pydantic.model import FileInput
from ..logs.logger import log_errors
def process_files(input_files: FileInput):
    try:
        # Load data frames and preprocess
        mtr_df = preprocess_dataframe(read_file(input_files.mtr_file))
        payment_df = preprocess_dataframe(read_file(input_files.payment_file))

        # Transform pandas data frames
        mtr_df = transform_mtr_df(mtr_df)
        payment_df = transform_payment_df(payment_df)

        # Merge data frames
        merged_df = merge_dataframes(mtr_df, payment_df)
        merged_df = mark_df(merged_df)

        # Apply tolerance level
        merged_df = apply_tolerance_check(merged_df)

        # Generate classification summary
        classification_summary = merged_df['mark'].value_counts().reset_index()
        classification_summary.columns = ['mark', 'count']
        
        tolerance_summary = merged_df['ToleranceCheck'].value_counts().reset_index()
        tolerance_summary.columns = ['ToleranceCheck', 'Count']
        
        # Generate empty_order_summary
        empty_order_sum = empty_order_summary(merged_df)

        # Output summaries
        print(classification_summary)
        print(tolerance_summary)
        print(empty_order_sum)

    except pd.errors.EmptyDataError as ede:
        log_errors(ede, context="EmptyDataError in process_files: Possibly an empty input file.")
        raise
    except ValueError as ve:
        log_errors(ve, context="ValueError in process_files: Likely due to unsupported file format or incorrect data processing.")
        raise
    except KeyError as ke:
        log_errors(ke, context="KeyError in process_files: Missing expected columns in one of the data frames.")
        raise
    except Exception as e:
        log_errors(e, context="General error in process_files.")
        raise