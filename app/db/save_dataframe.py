from typing import  Optional
from .database import get_db_context, get_db_from_request
from ..db.schema.processed_dataframe import DataFrameRecord
from fastapi import Request
import pandas as pd

def save_dataframe_to_db(df:pd.DataFrame, filename:str,request: Optional[Request] = None):
    with get_db_context() as db:
        if request:
            db = get_db_from_request(request)
            records = []
            for _, row in df.iterrows():
                record = DataFrameRecord(filename=str(filename))
                
                if "InvoiceDate" in df.columns:
                    record.InvoiceDate = str(row['InvoiceDate'])
                
                if "TransactionType_mtr" in df.columns:
                    record.TransactionType_mtr = str(row['TransactionType_mtr'])

                if "OrderId" in df.columns:
                    record.OrderId = str(row['OrderId'])

                if "ShipmentDate" in df.columns:
                    record.ShipmentDate = str(row['ShipmentDate'])

                if "OrderDate" in df.columns:
                    record.OrderDate = str(row['OrderDate'])

                if "ShipmentItemId" in df.columns:
                    record.ShipmentItemId = row['ShipmentItemId']

                if "ItemDescription" in df.columns:
                    record.ItemDescription = str(row['ItemDescription'])

                if "PaymentDate" in df.columns:
                    record.PaymentDate = str(row['PaymentDate'])
                    
                if "PaymentType" in df.columns:
                    record.PaymentType = str(row['PaymentType'])

                if "Description" in df.columns:
                    record.Description = str(row['Description'])

                if "Total" in df.columns:
                    record.Total = row['Total']

                if "TransactionType_payment" in df.columns:
                    record.TransactionType_payment = str(row['TransactionType_payment'])

                if "NetAmount" in df.columns:
                    record.NetAmount = row['NetAmount']

                if "ToleranceCheck" in df.columns:
                    record.ToleranceCheck = str(row['ToleranceCheck'])
                    
                records.append(record)

            db.add_all(records)
            db.commit()

