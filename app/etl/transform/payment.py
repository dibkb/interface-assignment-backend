import pandas as pd
from ...logs.logger import log_errors
from ...db.schema.error_log import LevelType


def transform_payment_df(df: pd.DataFrame):
    try:
        # Log the start of the transformation process
        log_errors(
            "Starting payment transformation",
            context="transform_payment_df",
            level=LevelType.INFO.value,
            additional_info={"payment_df_columns": df.columns.to_list()}
        )

        # Filter out 'Transfer' types and rename columns
        df = df[df['Type'] != 'Transfer']
        log_errors(
            "Filtered out 'Transfer' types",
            context="transform_payment_df",
            level=LevelType.INFO.value,
            additional_info={"remaining_rows": df.shape[0]}
        )

        df = df.rename(
            columns={
                'Type': 'PaymentType',
                'Date/Time': 'PaymentDate'})
        log_errors(
            "Renamed columns: 'Type' to 'PaymentType' and 'Date/Time' to 'PaymentDate'",
            context="transform_payment_df",
            level=LevelType.INFO.value)

        # Replace specific values in the 'PaymentType' column
        df['PaymentType'] = df['PaymentType'].replace({
            'Ajdustment': 'Order',
            'FBA Inventory Fee': 'Order',
            'Fulfilment Fee Refund': 'Order',
            'Service Fee': 'Order',
            'Refund': 'Return'
        })
        log_errors(
            "Replaced values in 'PaymentType' column",
            context="transform_payment_df",
            level=LevelType.INFO.value
        )

        # Add a new column 'TransactionType'
        df['TransactionType'] = 'Payment'
        log_errors(
            "Added 'TransactionType' column with value 'Payment'",
            context="transform_payment_df",
            level=LevelType.INFO.value,
            additional_info={"updated_columns": df.columns.to_list()}
        )

        # Log the successful completion of the transformation process
        log_errors(
            "Payment transformation completed successfully",
            context="transform_payment_df",
            level=LevelType.INFO.value,
            additional_info={"final_df_shape": df.shape}
        )

        return df

    except KeyError as k:
        # Log KeyError with detailed information
        log_errors(
            k,
            context="KeyError in transform_payment_df: Missing 'Type' or 'Date/Time' column",
            level=LevelType.ERROR.value,
            additional_info={
                "payment_df_columns": df.columns.to_list()})
        raise

    except Exception as e:
        # Log any other exceptions with detailed information
        log_errors(
            e,
            context="General Error in transform_payment_df",
            level=LevelType.ERROR.value
        )
        raise
