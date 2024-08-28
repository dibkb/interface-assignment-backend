from .load import load_file, preprocess_dataframe
from .transform import (
    transform_mtr_df, transform_payment_df, merge_dataframes,
    mark_df, apply_tolerance_check, empty_order_summary
)

def process_files(mtr_file, payment_file):
    # Load and preprocess
    mtr_df = preprocess_dataframe(load_file(mtr_file.file, mtr_file.filename.split('.')[-1]))
    payment_df = preprocess_dataframe(load_file(payment_file.file, payment_file.filename.split('.')[-1]))

    # Transform
    mtr_df = transform_mtr_df(mtr_df)
    payment_df = transform_payment_df(payment_df)

    # Merge and further processing
    merged_df = merge_dataframes(mtr_df, payment_df)
    merged_df = mark_df(merged_df)
    merged_df = apply_tolerance_check(merged_df)

    # Generate summaries
    classification_summary = merged_df['mark'].value_counts().reset_index()
    classification_summary.columns = ['mark', 'count']
    
    tolerance_summary = merged_df['ToleranceCheck'].value_counts().reset_index()
    tolerance_summary.columns = ['ToleranceCheck', 'Count']
    
    empty_order_sum = empty_order_summary(merged_df)

    return classification_summary, tolerance_summary, empty_order_sum