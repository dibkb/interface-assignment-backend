from sqlalchemy import Column, Integer, String, Float,DateTime
from ..database import Base
class DataFrameRecord(Base):
    __tablename__ = 'dataframe_records'
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String, index=True, nullable=False)

    InvoiceDate = Column(String, nullable=True)
    TransactionType_mtr = Column(String, nullable=True)
    OrderId = Column(String, nullable=True)
    ShipmentDate = Column(String, nullable=True)
    OrderDate = Column(String, nullable=True)
    ShipmentItemId = Column(Float, nullable=True)
    ItemDescription = Column(String, nullable=True)
    PaymentDate = Column(String, nullable=True)
    PaymentType = Column(String, nullable=True)
    Description = Column(String, nullable=True)
    Total = Column(Float, nullable=True)
    TransactionType_payment = Column(String, nullable=True)
    NetAmount = Column(Float, nullable=True)
    mark = Column(String, nullable=True)
    ToleranceCheck = Column(String, nullable=True)

