import pandas as pd
from ...logs.logger import log_errors
from ...db.schema.error_log import LevelType
def transform_payment_df(df:pd.DataFrame):
    try:
        # start logging
        log_errors("Starting payment transformation", context="transforming payment dataframe", level=LevelType.INFO.value, 
            additional_info={"payment_df_columns": df.columns.to_list()})
        
        df = df[df['Type'] != 'Transfer']
        df = df.rename(columns={'Type': 'PaymentType', 'Date/Time': 'PaymentDate'})

        # start logging
        log_errors("Renaming PaymentDate successfully", context="rename column", level=LevelType.INFO.value)
        
        df['PaymentType'] = df['PaymentType'].replace({
            'Ajdustment': 'Order',
            'FBA Inventory Fee': 'Order',
            'Fulfilment Fee Refund': 'Order',
            'Service Fee': 'Order',
            'Refund': 'Return'
        })

        # start logging
        log_errors("Renaming PaymentType values successfully", context="renaming cells of PaymentType", level=LevelType.INFO.value, 
            )
        
        df['TransactionType'] = 'Payment'

        # start logging
        log_errors("Renaming complete in payment_dataframe", context="rename some cells of           PaymentType", level=LevelType.INFO.value,additional_info={"payment_df_columns": df.columns.to_list()} 
        )

        return df
    except KeyError as k:
        # transform_payment_df error
        log_errors(k, context="KeyError in transform_payment_df: Missing 'Type' or 'Date/Time' column")
        raise
    except Exception as e:
        # other general errors
        log_errors(e, context="General Error in transform_payment_df")
        raise