from ...logs.log import log_error
def transform_payment_df(df):
    try:
        df = df[df['Type'] != 'Transfer']
        df = df.rename(columns={'Type': 'PaymentType', 'Date/Time': 'PaymentDate'})
        df['PaymentType'] = df['PaymentType'].replace({
            'Ajdustment': 'Order',
            'FBA Inventory Fee': 'Order',
            'Fulfilment Fee Refund': 'Order',
            'Service Fee': 'Order',
            'Refund': 'Return'
        })
        df['TransactionType'] = 'Payment'
        return df
    except KeyError as k:
        # transform_payment_df error
        log_error(k, context="KeyError in transform_payment_df: Missing 'Type' or 'Date/Time' column")
        raise
    except Exception as e:
        # other general errors
        log_error(e, context="General Error in transform_payment_df")
        raise