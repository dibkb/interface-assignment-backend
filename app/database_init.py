# database_init.py
from .db.database import Base, engine

def init_db():
    Base.metadata.create_all(bind=engine)