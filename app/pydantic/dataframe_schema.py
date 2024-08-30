from pydantic import BaseModel
from typing import Optional, List

class DataFrameRecordSchema(BaseModel):
    id: int
    filename: str
    InvoiceDate: Optional[str] = None
    TransactionType_mtr: Optional[str] = None
    OrderId: Optional[str] = None
    ShipmentDate: Optional[str] = None
    OrderDate: Optional[str] = None
    ShipmentItemId: Optional[float] = None
    ItemDescription: Optional[str] = None
    PaymentDate: Optional[str] = None
    PaymentType: Optional[str] = None
    Description: Optional[str] = None
    Total: Optional[float] = None
    TransactionType_payment: Optional[str] = None
    NetAmount: Optional[float] = None
    mark: Optional[str] = None
    ToleranceCheck: Optional[str] = None

    class Config:
        orm_mode = True
        from_attributes = True

class DataFrameRecordResponse(BaseModel):
    reports: List[DataFrameRecordSchema]