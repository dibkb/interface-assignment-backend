from pydantic import BaseModel
from typing import List, Optional
from fastapi import UploadFile

class FileInput(BaseModel):
    mtr_file: UploadFile
    payment_file: UploadFile
    
class SummaryItem(BaseModel):
    category: str
    count: int

class EmptyOrderItem(BaseModel):
    Description: str
    NetAmount: float

class ProcessResult(BaseModel):
    classification_summary: List[SummaryItem]
    tolerance_summary: List[SummaryItem]
    empty_order_summary: Optional[List[EmptyOrderItem]]