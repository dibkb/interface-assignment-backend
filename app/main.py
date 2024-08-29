# Import necessary modules from FastAPI, SQLAlchemy, and other utilities
from fastapi import FastAPI, File, UploadFile, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from pydantic import ValidationError

# Import custom modules and functions
from .pydantic.model import FileInput 
from .etl.process import process_files
from .database_init import init_db 
from .logs.logger import log_errors 
from .db.schema.error_log import ErrorLog 
from .db.database import get_db_context, get_db_from_request

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
async def show_logs(request:Request):
    # Query all logs entries
    with get_db_context() as db:
            db = get_db_from_request(request)
            demos = db.query(ErrorLog).all()
            return JSONResponse(content=jsonable_encoder(demos),status_code=200)

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/process-files")
async def process_uploaded_files(mtr_file: UploadFile = File(...), payment_file: UploadFile = File(...)):
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

