from fastapi import FastAPI, File, UploadFile, HTTPException, Depends,Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String,DateTime,JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from typing import Union, Optional
from pydantic import BaseModel
from .pydantic.model import FileInput
from pydantic import ValidationError
from .etl.process import process_files
import os
import datetime
from contextlib import contextmanager
# Set up FastAPI app
app = FastAPI()

# Enable CORS for specified origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Allow requests from this origin
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

# Set up the database
SQLALCHEMY_DATABASE_URL = os.environ.get("DATABASE_URL")
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@contextmanager
def get_db_context():
    db = next(get_db())
    try:
        yield db
    finally:
        db.close()

def get_db_from_request(request: Request) -> Session:
    return request.state.db if hasattr(request.state, 'db') else next(get_db())

# SQLAlchemy model for the the Error modal
class ErrorLog(Base):
    __tablename__ = "error_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.datetime.now())
    level = Column(String, index=True)
    error_type = Column(String)
    message = Column(String)
    context = Column(String)
    additional_info = Column(JSON, default={})

# Create the table if it doesn't exist
Base.metadata.create_all(bind=engine)

@app.api_route("/logs", methods=["GET"])
async def test_db(db: Session = Depends(get_db)):
    # Query all logs entries
    demos = db.query(ErrorLog).all()

    return JSONResponse(content=jsonable_encoder(demos),status_code=200)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/process-files")
async def process_uploaded_files(db: Session = Depends(get_db),mtr_file: UploadFile = File(...), payment_file: UploadFile = File(...)):
    try:
        # logging info
        log_errors(error="Processing input files", 
           context="processing the mtr_file and payment_file from client",
           level="INFO",
           additional_info={
               "mtr_file": mtr_file.filename,
               "payment_file": payment_file.filename
        })

        input_files = FileInput(mtr_file=mtr_file, payment_file=payment_file)
        process_files(input_files)
        
        # finished processing
        log_errors(error="Processing input files complete", 
           context="finished processing the mtr_file and payment_file from client",
           level="INFO",
           additional_info={
               "mtr_file": mtr_file.filename,
               "payment_file": payment_file.filename
        })

    except ValidationError as v:
        error_details = log_errors(v, context="Validation Error")
        raise HTTPException(status_code=422, detail=f"Validation Error: {error_details['message']}")
    
    except Exception as e:
        error_details = log_errors(e, context="File Processing Error")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {error_details['message']}")


def log_errors(
    error: Union[str, Exception],
    context: str = "",
    level="ERROR",
    additional_info=None,
    request: Optional[Request] = None
):
    with get_db_context() as db:
        if request:
            db = get_db_from_request(request)
        
        error_log = ErrorLog(
            timestamp=datetime.datetime.now(),
            level=level,
            error_type=type(error).__name__ if isinstance(error, Exception) else "INFO",
            message=str(error),
            context=context,
            additional_info=additional_info or {}
        )
        db.add(error_log)
        db.commit()