def transform_payment_df(df):
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