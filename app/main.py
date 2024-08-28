from fastapi import FastAPI,File,UploadFile
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine,Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from fastapi.responses import JSONResponse
from .etl.load import read_file
import io
import pandas as pd
import os

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
    allow_origins=["http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"], 
)

@app.get("/")
async def root():
    return {"message": "Hello World"}
@app.post("/process-files/")
async def process_uploaded_files(mtr_file: UploadFile = File(...), payment_file: UploadFile = File(...)):
    mtr_df = read_file(mtr_file)
    payment_df = read_file(payment_file)

@app.get("/db-test")
async def db_test():
    try:
        db = SessionLocal()

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
