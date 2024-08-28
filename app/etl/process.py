from .load import read_file,preprocess_dataframe
from .transform.mtr import transform_mtr_df
from .transform.payment import transform_payment_df
from .transform.main import merge_dataframes,mark_df,apply_tolerance_check,empty_order_summary
from ..pydantic.model import FileInput
def process_files(input_files: FileInput):
    # load data frame and preprocess
    mtr_df = preprocess_dataframe(read_file(input_files.mtr_file))
    payment_df = preprocess_dataframe(read_file(input_files.payment_file))

    # transform pandas data frame
    mtr_df = transform_mtr_df(mtr_df)
    payment_df = transform_payment_df(payment_df)

    # merge data frame
    merged_df = merge_dataframes(mtr_df, payment_df)
    merged_df = mark_df(merged_df)

    # apply tolerance level
    merged_df = apply_tolerance_check(merged_df)

    # Generate classification_summary
    classification_summary = merged_df['mark'].value_counts().reset_index()
    classification_summary.columns = ['mark', 'count']
    
    tolerance_summary = merged_df['ToleranceCheck'].value_counts().reset_index()
    tolerance_summary.columns = ['ToleranceCheck', 'Count']
    
    # Generate empty_order_sum
    empty_order_sum = empty_order_summary(merged_df)
    print(classification_summary)
    print(tolerance_summary)
    print(empty_order_sum)