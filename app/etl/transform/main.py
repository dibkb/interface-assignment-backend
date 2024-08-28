import pandas as pd
import numpy as np
from  ...logs.log import log_error
def merge_dataframes(mtr_df, payment_df):
    try:
        merged_df = pd.merge(mtr_df, payment_df, on='OrderId', how='outer', suffixes=('_mtr', '_payment')) 
        merged_df['Total'] = merged_df['Total'].where(merged_df['Total'].notna(), np.nan)
        merged_df['Total'] = merged_df['Total'].str.replace(',', '').astype(float)
        merged_df['NetAmount'] = merged_df.apply(
            lambda row: float(row['InvoiceAmount']) - float(row['Total']) if pd.notna(row['InvoiceAmount']) and pd.notna(row['Total']) else np.nan,
            axis=1
        )
        return merged_df
    except KeyError as ke:
        log_error(ke, context="KeyError in merge_dataframes : Missing expected columns")
        raise   
    except Exception as e:
        log_error(e, context="General Error in merge_dataframes")
        raise

def mark_df(df):
    try:
        df['mark'] = ''
        df.loc[df['OrderId'].str.len() == 10, 'mark'] = 'Removal Order IDs'
        df.loc[(df['TransactionType_mtr'] == 'Return') & (df['InvoiceAmount'].notna()), 'mark'] = 'Return'
        df.loc[(df['TransactionType_payment'] == 'Payment') & (df['NetAmount'] < 0), 'mark'] = 'Negative Payout'
        df.loc[(df['NetAmount'].notna()) & (df['InvoiceAmount'].notna()), 'mark'] = 'Order & Payment Received'
        df.loc[(df['NetAmount'].notna()) & (df['InvoiceAmount'].isna()), 'mark'] = 'Order Not Applicable but Payment Received'
        df.loc[(df['InvoiceAmount'].notna()) & (df['NetAmount'].isna()), 'mark'] = 'Payment Pending'
        return df
    
    except KeyError as ke:
        log_error(ke, context="KeyError in mark_df: Missing expected columns")
        raise
    except Exception as e:
        log_error(e, context="General Error in mark_df")
        raise

def check_tolerance(row):
    try:
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
        
    except KeyError as ke:
        log_error(ke, context="KeyError in check_tolerance: Missing 'NetAmount' or 'InvoiceAmount'")
        raise
    except Exception as e:
        log_error(e, context="General Error in check_tolerance")
        raise        

def apply_tolerance_check(df):
    try:
        df['ToleranceCheck'] = df.apply(check_tolerance, axis=1)
        return df
    except Exception as e:
        log_error(e, context="Error in apply_tolerance_check")
        raise

def empty_order_summary(df):
    try:
        return df[df['NetAmount'] > 0 & df['OrderId'].notna() & (df['OrderId'] == '')].groupby('Description')['NetAmount'].sum().reset_index()
    except KeyError as ke:
        log_error(ke, context="KeyError in empty_order_summary: Missing expected columns")
        raise
    except Exception as e:
        log_error(e, context="General Error in empty_order_summary")
        raise