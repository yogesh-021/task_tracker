from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy.sql import func
from app.database import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key = True, index=True)
    username = Column(String, unique = True, nullable = False)
    email = Column(String, unique = True, nullable = False)
    hashed_password = Column(String, nullable = False)
    created_at = Column(DateTime(timezone=True), server_default = func.now())
    role = Column(String, default = "developer")
    is_active = Column(Boolean, default = True)


