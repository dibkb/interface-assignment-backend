from fastapi import FastAPI
from sqlalchemy import create_engine,Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
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

@app.get("/")
async def root():
    return {"message": "Hello World"}

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
