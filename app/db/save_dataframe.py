from typing import  Optional
from .database import get_db_context, get_db_from_request
from ..db.schema.processed_dataframe import DataFrameRecord
from fastapi import Request
import pandas as pd
import numpy as np
import math
def save_dataframe_to_db(df:pd.DataFrame, filename:str,request: Optional[Request] = None):
    with get_db_context() as db:
        if request:
            db = get_db_from_request(request)
            
        records = []
        for _, row in df.iterrows():
            record = DataFrameRecord(filename=str(filename))
            
            if "InvoiceDate" in df.columns:
                record.InvoiceDate = convert_to_string(row['InvoiceDate'])
            
            if "TransactionType_mtr" in df.columns:
                record.TransactionType_mtr = convert_to_string(row['TransactionType_mtr'])

            if "OrderId" in df.columns:
                record.OrderId = convert_to_string(row['OrderId'])

            if "ShipmentDate" in df.columns:
                record.ShipmentDate = convert_to_string(row['ShipmentDate'])

            if "OrderDate" in df.columns:
                record.OrderDate = convert_to_string(row['OrderDate'])

            if "ShipmentItemId" in df.columns:
                record.ShipmentItemId = convert_to_string(row['ShipmentItemId'])

            if "ItemDescription" in df.columns:
                record.ItemDescription = convert_to_string(row['ItemDescription'])

            if "PaymentDate" in df.columns:
                record.PaymentDate = convert_to_string(row['PaymentDate'])
                
            if "PaymentType" in df.columns:
                record.PaymentType = convert_to_string(row['PaymentType'])

            if "Description" in df.columns:
                record.Description = convert_to_string(row['Description'])

            if "Total" in df.columns:
                record.Total = convert_to_string(row['Total'])

            if "TransactionType_payment" in df.columns:
                record.TransactionType_payment = convert_to_string(row['TransactionType_payment'])

            if "NetAmount" in df.columns:
                record.NetAmount = convert_to_string(row['NetAmount'])

            if "ToleranceCheck" in df.columns:
                record.ToleranceCheck = convert_to_string(row['ToleranceCheck'])

            records.append(record)

        db.add_all(records)

        db.commit()

def convert_to_string(value):
    
    if np.isnan(value):
        return ""
    elif isinstance(value, (int, float, np.number)):
        if np.isinf(value):
            return ""
        return str(value)
    else: 
        return str(value)
