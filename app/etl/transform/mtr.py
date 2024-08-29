import pandas as pd
from ...logs.logger import log_errors
from ...db.schema.error_log import LevelType

def transform_mtr_df(df: pd.DataFrame):
    try:
        # Start logging
        log_errors(
            error="Starting mtr transformation",
            context="transform_mtr_df",
            level=LevelType.INFO.value,
            additional_info={
                "initial_shape": df.shape,
                "initial_columns": df.columns.tolist(),
                "initial_transaction_types": df['TransactionType'].value_counts().to_dict()
            }
        )
    
        # Filtering out 'Cancel' transactions
        df = df[df['TransactionType'] != 'Cancel']
        log_errors(
            error="Filtered out 'Cancel' transactions",
            context="transform_mtr_df",
            level=LevelType.INFO.value,
            additional_info={"rows_after_cancel_filter": len(df)}
        )

        # Transforming 'Refund' and 'FreeReplacement' to 'Return'
        refund_count = sum(df["TransactionType"] == "Refund")
        free_replacement_count = sum(df["TransactionType"] == "FreeReplacement")
        
        df.loc[df["TransactionType"] == "Refund", "TransactionType"] = "Return"
        df.loc[df["TransactionType"] == "FreeReplacement", "TransactionType"] = "Return"

        log_errors(
            error="Transformed 'Refund' and 'FreeReplacement' to 'Return'",
            context="transform_mtr_df",
            level=LevelType.INFO.value,
            additional_info={
                "refunds_transformed": refund_count,
                "free_replacements_transformed": free_replacement_count
            }
        )

        # Finish logging
        log_errors(
            error="MTR transformation complete",
            context="transform_mtr_df",
            level=LevelType.INFO.value,
            additional_info={
                "final_shape": df.shape,
                "final_columns": df.columns.tolist(),
                "final_transaction_types": df['TransactionType'].value_counts().to_dict()
            }
        )
        
        return df

    except KeyError as k:
        log_errors(
            error=str(k),
            context="KeyError in transform_mtr_df: Missing 'TransactionType' column",
            level=LevelType.ERROR.value,
            additional_info={"available_columns": df.columns.tolist()}
        )
        raise

    except Exception as e:
        log_errors(
            error=str(e),
            context="General Error in transform_mtr_df",
            level=LevelType.ERROR.value
        )
        raise