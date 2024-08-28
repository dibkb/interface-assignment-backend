import pandas as pd
import numpy as np
def merge_dataframes(mtr_df, payment_df):
    merged_df = pd.merge(mtr_df, payment_df, on='OrderId', how='outer', suffixes=('_mtr', '_payment'))
    merged_df['Total'] = merged_df['Total'].where(merged_df['Total'].notna(), np.nan)
    merged_df['Total'] = merged_df['Total'].str.replace(',', '').astype(float)
    merged_df['NetAmount'] = merged_df.apply(
        lambda row: float(row['InvoiceAmount']) - float(row['Total']) if pd.notna(row['InvoiceAmount']) and pd.notna(row['Total']) else np.nan,
        axis=1
    )
    return merged_df

def mark_df(df):
    df['mark'] = ''
    df.loc[df['OrderId'].str.len() == 10, 'mark'] = 'Removal Order IDs'
    df.loc[(df['TransactionType_mtr'] == 'Return') & (df['InvoiceAmount'].notna()), 'mark'] = 'Return'
    df.loc[(df['TransactionType_payment'] == 'Payment') & (df['NetAmount'] < 0), 'mark'] = 'Negative Payout'
    df.loc[(df['NetAmount'].notna()) & (df['InvoiceAmount'].notna()), 'mark'] = 'Order & Payment Received'
    df.loc[(df['NetAmount'].notna()) & (df['InvoiceAmount'].isna()), 'mark'] = 'Order Not Applicable but Payment Received'
    df.loc[(df['InvoiceAmount'].notna()) & (df['NetAmount'].isna()), 'mark'] = 'Payment Pending'
    return df