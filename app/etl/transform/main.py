import pandas as pd
import numpy as np
from ...db.schema.error_log import LevelType
from ...logs.logger import log_errors


def merge_dataframes(mtr_df, payment_df):
    try:
        # Logging the start of the dataframe merge process
        log_errors(
            error="Starting dataframe merge",
            context="merge_dataframes",
            level=LevelType.INFO.value,
            additional_info={
                "mtr_df_shape": mtr_df.shape,
                "payment_df_shape": payment_df.shape})

        # Merging the dataframes on 'OrderId' with an outer join
        merged_df = pd.merge(
            mtr_df,
            payment_df,
            on='OrderId',
            how='outer',
            suffixes=(
                '_mtr',
                '_payment'))

        # Logging the successful merge of the dataframes
        log_errors(
            error="Dataframes merged successfully",
            context="merge_dataframes",
            level=LevelType.INFO.value,
            additional_info={"merged_df_shape": merged_df.shape}
        )

        # Performing data transformations on the merged dataframe
        log_errors(
            error="Starting data transformations",
            context="merge_dataframes",
            level=LevelType.INFO.value
        )
        merged_df['Total'] = merged_df['Total'].where(
            merged_df['Total'].notna(), np.nan)
        merged_df['Total'] = merged_df['Total'].str.replace(
            ',', '').astype(float)

        # Calculating 'NetAmount' by subtracting 'Total' from 'InvoiceAmount'
        merged_df['NetAmount'] = merged_df.apply(
            lambda row: float(row['InvoiceAmount']) - float(row['Total'])
            if pd.notna(row['InvoiceAmount']) and pd.notna(row['Total'])
            else np.nan,
            axis=1
        )

        # Logging the completion of merging and transformation
        log_errors(
            error="Merging dataframe and transformation complete",
            context="merge_dataframes",
            level=LevelType.INFO.value,
            additional_info={
                "merged_df_columns": merged_df.columns.tolist(),
                "merged_df_shape": merged_df.shape
            }
        )
        return merged_df

    except KeyError as ke:
        # Logging any KeyError exceptions encountered
        log_errors(
            ke,
            context="KeyError in merge_dataframes: Missing expected columns",
            level=LevelType.ERROR.value,
            additional_info={"error_type": "Missing expected columns"}
        )
        raise
    except Exception as e:
        # Logging any general exceptions encountered
        log_errors(
            e,
            context="General Error in merge_dataframes",
            level=LevelType.ERROR.value
        )
        raise


def mark_df(df):
    try:
        # Logging the start of the mark column creation process
        log_errors(
            error="Starting mark column creation",
            context="mark_df",
            level=LevelType.INFO.value
        )

        df['mark'] = ''
        df.loc[df['OrderId'].str.len() == 10, 'mark'] = 'Removal Order IDs'
        df.loc[(df['TransactionType_mtr'] == 'Return') & (
            df['InvoiceAmount'].notna()), 'mark'] = 'Return'
        df.loc[(df['TransactionType_payment'] == 'Payment') & (
            df['NetAmount'] < 0), 'mark'] = 'Negative Payout'
        df.loc[(df['NetAmount'].notna()) & (df['InvoiceAmount'].notna()),
               'mark'] = 'Order & Payment Received'
        df.loc[(df['NetAmount'].notna()) & (df['InvoiceAmount'].isna()),
               'mark'] = 'Order Not Applicable but Payment Received'
        df.loc[(df['InvoiceAmount'].notna()) & (
            df['NetAmount'].isna()), 'mark'] = 'Payment Pending'

        # Logging the completion of mark column creation
        log_errors(
            error="Mark column creation complete",
            context="mark_df",
            level=LevelType.INFO.value,
            additional_info={
                "mark_value_counts": df['mark'].value_counts().to_dict()})
        return df

    except KeyError as ke:
        # Logging any KeyError exceptions encountered
        log_errors(
            ke,
            context="KeyError in mark_df: Missing expected columns",
            level=LevelType.ERROR.value
        )
        raise
    except Exception as e:
        # Logging any general exceptions encountered
        log_errors(
            e,
            context="General Error in mark_df",
            level=LevelType.ERROR.value
        )
        raise


def check_tolerance(row):
    try:
        pna = row['NetAmount']
        sia = row['InvoiceAmount']

        if pd.isna(pna) or pd.isna(sia) or sia == 0:
            return np.nan

        # Calculate the percentage difference
        percentage = (pna / sia) * 100

        # Check if within tolerance based on thresholds
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
        # Logging any KeyError exceptions encountered
        log_errors(
            ke,
            context="KeyError in check_tolerance: Missing 'NetAmount' or 'InvoiceAmount'",
            level=LevelType.ERROR.value)
        raise
    except Exception as e:
        # Logging any general exceptions encountered
        log_errors(
            e,
            context="General Error in check_tolerance",
            level=LevelType.ERROR.value
        )
        raise


def apply_tolerance_check(df):
    try:
        # Logging the start of tolerance check application
        log_errors(
            error="Starting tolerance check application",
            context="apply_tolerance_check",
            level=LevelType.INFO.value
        )

        # Apply tolerance check
        df['ToleranceCheck'] = df.apply(check_tolerance, axis=1)

        # Logging the completion of tolerance check application
        log_errors(
            error="Tolerance check application complete",
            context="apply_tolerance_check",
            level=LevelType.INFO.value,
            additional_info={
                "tolerance_check_value_counts": df['ToleranceCheck'].value_counts().to_dict()})
        return df
    except Exception as e:
        # Logging any general exceptions encountered
        log_errors(
            e,
            context="Error in apply_tolerance_check",
            level=LevelType.ERROR.value
        )
        raise


def empty_order_summary(df: pd.DataFrame):
    try:
        # Logging the start of empty order summary generation
        log_errors(
            error="Starting empty order summary",
            context="empty_order_summary",
            level=LevelType.INFO.value
        )

        # Generate summary for empty orders with positive 'NetAmount'
        summary = df[df['NetAmount'] > 0 & df['OrderId'].notna() & (
            df['OrderId'] == '')].groupby('Description')['NetAmount'].sum().reset_index()

        # Logging the completion of empty order summary generation
        log_errors(
            error="Empty order summary complete",
            context="empty_order_summary",
            level=LevelType.INFO.value,
            additional_info={"summary_shape": summary.shape}
        )
        return summary
    except KeyError as ke:
        # Logging any KeyError exceptions encountered
        log_errors(
            ke,
            context="KeyError in empty_order_summary: Missing expected columns",
            level=LevelType.ERROR.value)
        raise
    except Exception as e:
        # Logging any general exceptions encountered
        log_errors(
            e,
            context="General Error in empty_order_summary",
            level=LevelType.ERROR.value
        )
