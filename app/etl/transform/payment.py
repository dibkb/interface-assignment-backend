from ...logs.log import log_error
import pandas as pd
def transform_payment_df(df:pd.DataFrame):
    try:
        # start logging
        log_error("Starting payment transformation", context="transforming payment dataframe", level="INFO", 
            additional_info={"payment_df_columns": df.columns})
        
        df = df[df['Type'] != 'Transfer']
        df = df.rename(columns={'Type': 'PaymentType', 'Date/Time': 'PaymentDate'})

        # start logging
        log_error("Renaming PaymentDate successfully", context="rename column", level="INFO", 
            )
        
        df['PaymentType'] = df['PaymentType'].replace({
            'Ajdustment': 'Order',
            'FBA Inventory Fee': 'Order',
            'Fulfilment Fee Refund': 'Order',
            'Service Fee': 'Order',
            'Refund': 'Return'
        })

        # start logging
        log_error("Renaming PaymentType values successfully", context="rename some cells of PaymentType", level="INFO", 
            )
        
        df['TransactionType'] = 'Payment'

        # start logging
        log_error("Renaming complete in payment_dataframe", context="rename some cells of PaymentType", level="INFO",additional_info={"payment_df_columns": df.columns} 
        )

        return df
    except KeyError as k:
        # transform_payment_df error
        log_error(k, context="KeyError in transform_payment_df: Missing 'Type' or 'Date/Time' column")
        raise
    except Exception as e:
        # other general errors
        log_error(e, context="General Error in transform_payment_df")
        raise