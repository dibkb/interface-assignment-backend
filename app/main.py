# Import necessary modules from FastAPI, SQLAlchemy, and other utilities
from fastapi import FastAPI, File, UploadFile, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from pydantic import ValidationError
import pandas as pd

# Import custom modules and functions
from .pydantic.model import FileInput 
from .etl.process import process_files
from .database_init import init_db 
from .logs.logger import log_errors 
from .db.schema.error_log import ErrorLog,LevelType
from .db.save_dataframe import save_dataframe_to_db
from .db.schema.processed_dataframe import DataFrameRecord
from .db.database import get_db_context, get_db_from_request
from .pydantic.dataframe_schema import DataFrameRecordResponse,DataFrameRecordSchema
# Set up FastAPI app
app = FastAPI()

# Enable CORS for specified origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"], 
)

# init db...
init_db()

@app.api_route("/logs", methods=["GET"])
async def show_logs(request:Request,page: int = 1, limit: int = 20):
    # Query all logs entries
    page = max(1,page)
    offest = (page-1)*limit
    with get_db_context() as db:
            db = get_db_from_request(request)
            logs = db.query(ErrorLog).offset(offest).limit(limit).all()
            total_count = db.query(ErrorLog).count()
            total_pages = (total_count - 1) // limit + 1
            return JSONResponse(content=jsonable_encoder({
                 "results" : logs,
                 "current_page": page,
                 "total_page" : total_pages,
            }),status_code=200)

@app.get("/")
async def root():
    return {"message": LevelType.INFO.value}

@app.post("/process-files")
async def process_uploaded_files(request: Request, mtr_file: UploadFile = File(...), payment_file: UploadFile = File(...)):
    try:
        # Using context manager to handle DB session
        with get_db_context() as db:
            # Check if the merged_df table exists and has any rows
            existing_file = pd.read_sql("SELECT * FROM information_schema.tables WHERE table_name = 'merge_df'", con=db.connection())
            
            if not existing_file.empty:  # Check if the DataFrame is not empty
                return JSONResponse({"processed_file": merged_df.to_json()}, status_code=200)
            
            # Logging info
            log_errors(
                error="Processing input files",
                context="processing the mtr_file and payment_file from client",
                level=LevelType.INFO.value,
                additional_info={
                    "mtr_file": mtr_file.filename,
                    "payment_file": payment_file.filename
                }
            )

            # Processing the files
            input_files = FileInput(mtr_file=mtr_file, payment_file=payment_file)
            classification_summary, tolerance_summary, transaction_summary, merged_df = process_files(input_files)

            # Logging completion
            log_errors(
                error="Processing input files complete",
                context="finished processing the mtr_file and payment_file from client",
                level=LevelType.INFO.value,
                additional_info={
                    "mtr_file": mtr_file.filename,
                    "payment_file": payment_file.filename
                }
            )

            # Save the DataFrame to the database
            merged_df.to_sql('merge_df', con=db.connection(), if_exists='replace', index=False)

            return JSONResponse({"processed_file": merged_df.to_json()}, status_code=200)

    except ValidationError as v:
        error_details = log_errors(v, context="Validation Error")
        raise HTTPException(status_code=422, detail=f"Validation Error: {error_details['message']}")
    
    except Exception as e:
        error_details = log_errors(e, context="File Processing Error")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {error_details['message']}")

