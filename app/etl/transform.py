import numpy as np
import pandas as pd

def transform_mtr_df(df):
    df = df[df['TransactionType'] != 'Cancel']
    df.loc[df["TransactionType"] == "Refund", "TransactionType"] = "Return"
    df.loc[df["TransactionType"] == "FreeReplacement", "TransactionType"] = "Return"
    return df

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

def check_tolerance(row):
    pna = row['NetAmount']
    sia = row['InvoiceAmount']
    if pd.isna(pna) or pd.isna(sia) or sia == 0:
        return np.nan
        
    percentage = (pna / sia) * 100
        
    if 0 < pna <= 300:
        return 'Within Tolerance' if percentage > 50 else 'Tolerance Breached'
    elif 300 < pna <= 500:
        return 'Within Tolerance' if percentage > 45 else 'Tolerance Breached'
    elif 500 < pna <= 900:
        return 'Within Tolerance' if percentage > 43 else 'Tolerance Breached'
    elif 900 < pna <= 1500:
        return 'Within Tolerance' if percentage > 38 else 'Tolerance Breached'
    elif pna > 1500:
        return 'Within Tolerance' if percentage > 30 else 'Tolerance Breached'
    else:
        return np.nan

def apply_tolerance_check(df):
    df['ToleranceCheck'] = df.apply(check_tolerance, axis=1)
    return df

def empty_order_summary(df):
    return df[df['NetAmount'] > 0 & df['OrderId'].notna() & (df['OrderId'] == '')].groupby('Description')['NetAmount'].sum().reset_index()