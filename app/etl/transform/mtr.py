def transform_mtr_df(df):
    #Todo log errors
    df = df[df['TransactionType'] != 'Cancel']
    df.loc[df["TransactionType"] == "Refund", "TransactionType"] = "Return"
    df.loc[df["TransactionType"] == "FreeReplacement", "TransactionType"] = "Return"
    return df