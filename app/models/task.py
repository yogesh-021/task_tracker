from sqlalchemy import Column, Integer, String, DateTime, Date, ForeignKey, Text
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base

class Task(Base):
    __tablename__ = "tasks"

    id = Column(Integer, primary_key = True, index=True)
    title = Column(String, nullable = False)
    desc = Column(Text, nullable = True)
    priority = Column(String, default = "Medium")
    created_at = Column(DateTime(timezone=True), server_default = func.now())
    creator_id = Column(Integer,ForeignKey("users.id"), nullable = False)
    assignee_id = Column(Integer, ForeignKey("users.id"), nullable = True)
    status = Column(String, default = "To Do")
    due_date = Column(Date, nullable = True)
    file_path = Column(String, nullable = True)
    bronze_file_path = Column(String, nullable = True)
    silver_file_path = Column(String, nullable = True)
    gold_file_path = Column(String, nullable = True)

    creator = relationship("User", foreign_keys=[creator_id])
    assignee = relationship("User", foreign_keys=[assignee_id])



    
