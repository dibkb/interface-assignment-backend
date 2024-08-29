from sqlalchemy import Column, Integer, String, DateTime, JSON
from ..database import Base
import datetime

class ErrorLog(Base):
    __tablename__ = "error_logs"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.datetime.now())
    level = Column(String, index=True)
    error_type = Column(String)
    message = Column(String)
    context = Column(String)
    additional_info = Column(JSON, default={})