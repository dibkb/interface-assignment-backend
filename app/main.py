from fastapi import FastAPI,File,UploadFile,HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine,Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from .etl.process import process_files
from .pydantic.model import FileInput
from .logs.log import log_error
import os
from pathlib import Path
import json
app = FastAPI()

SQLALCHEMY_DATABASE_URL = os.environ.get("DATABASE_URL")
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Define the User model
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    email = Column(String, unique=True, index=True)

Base.metadata.create_all(bind=engine)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"], 
)

@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/process-files")
async def process_uploaded_files(mtr_file: UploadFile = File(...), payment_file: UploadFile = File(...)):
    try:
        log_error("Starting processing files", context="mtr and payment file", additional_info={
            "mtr_file_info" : {
                mtr_file.filename,
            },
            "payment_file_info" : {
                payment_file.filename,
            },
        })
        input_files = FileInput(mtr_file=mtr_file, payment_file=payment_file)
        process_files(input_files)

    except ValidationError as v:
        error_details = log_error(v, context="Validation Error")
        raise HTTPException(status_code=422, detail=f"Validation Error: {error_details['message']}")
    
    except Exception as e:
        error_details = log_error(e, context="File Processing Error")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {error_details['message']}")
    
@app.get("/error-logs")
async def get_error_logs():
    log_dir = Path("logs")  # Make sure this matches the directory in your log_error function
    log_file = log_dir / "error_log.json"

    try:
        if log_file.exists():
            with log_file.open("r") as file:
                logs = json.load(file)
            return JSONResponse(content=logs)
        else:
            return JSONResponse(content=[], status_code=200)
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Error reading log file: Invalid JSON")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading log file: {str(e)}")
    
@app.get("/db-test")
async def db_test():
    try:
        db = SessionLocal()
        
        user = db.query(User).filter_by(email="johndoe@example.com").first()

        if user:
            return {
                "message": "Database connection successful",
                "user": {
                    "id": user.id,
                    "name": user.name,
                    "email": user.email,
                },
            }
        # Create a new user
        new_user = User(name="John Doe", email="johndoe@example.com")
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        user = db.query(User).filter_by(email="johndoe@example.com").first()
        if user:
            return {
                "message": "Database connection successful",
                "user": {
                    "id": user.id,
                    "name": user.name,
                    "email": user.email,
                },
            }
        else:
            return {"message": "User not found"}

    except Exception as e:
        return {"message": f"Database connection failed: {str(e)}"}

    finally:
        db.close()
