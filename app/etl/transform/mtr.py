from ...logs.log import log_error
def transform_mtr_df(df):
    try:
        df = df[df['TransactionType'] != 'Cancel']
        df.loc[df["TransactionType"] == "Refund", "TransactionType"] = "Return"
        df.loc[df["TransactionType"] == "FreeReplacement", "TransactionType"] = "Return"
        return df
    except KeyError as k:
        # transform_mtr_df error
        log_error(k, context="KeyError in transform_mtr_df: Missing 'TransactionType' column")
        raise
    except Exception as e:
        # log any other exception
        log_error(e, context="General Error in transform_mtr_df")
        raise