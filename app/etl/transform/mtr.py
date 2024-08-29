import pandas as pd
from ...logs.logger import log_errors
from ...db.schema.error_log import LevelType
def transform_mtr_df(df:pd.DataFrame):
    try:
        # start logging
        # log_errors("Starting mtr transformation", context="transforming mtr", level=LevelType.INFO.value, 
        #     additional_info={"mtr_df_columns": df.columns})
    
        df = df[df['TransactionType'] != 'Cancel']
        df.loc[df["TransactionType"] == "Refund", "TransactionType"] = "Return"
        df.loc[df["TransactionType"] == "FreeReplacement", "TransactionType"] = "Return"

        # start logging
        # log_errors("mtr transformation complete", context="Transformation complete", level=LevelType.INFO.value, 
        #     additional_info={"mtr_df_columns": df.columns})
        
        return df
    except KeyError as k:
        # transform_mtr_df error
        # log_errors(k, context="KeyError in transform_mtr_df: Missing 'TransactionType' column")
        raise
    except Exception as e:
        # log any other exception
        # log_errors(e, context="General Error in transform_mtr_df")
        raise