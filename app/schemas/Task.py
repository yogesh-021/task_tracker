from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Optional


class TaskBase(BaseModel):
    title: str
    desc: Optional[str] = None
    priority: Optional[str] = "Medium"
    status: Optional[str] = "To Do"
    due_date: Optional[datetime]
    

class TaskCreate(TaskBase):
    pass
    

class TaskUpdate(TaskBase):
    title: Optional[str] = None


class TaskOut(TaskBase):
    id: int
    created_at: datetime 
    creator_id: int
    assignee_id: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)