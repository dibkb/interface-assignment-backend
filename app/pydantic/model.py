from pydantic import BaseModel
from typing import List, Optional
from fastapi import UploadFile


class FileInput(BaseModel):
    mtr_file: UploadFile
    payment_file: UploadFile

    def dict(self, **kwargs):

        return {
            "mtr_file": {
                "filename": self.mtr_file.filename,
                "content_type": self.mtr_file.content_type,
                "size": self.mtr_file.size,
            },
            "payment_file": {
                "filename": self.payment_file.filename,
                "content_type": self.payment_file.content_type,
                "size": self.payment_file.size,
            }
        }


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
