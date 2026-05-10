from pathlib import Path
from pydantic import BaseModel, ConfigDict, field_validator
from datetime import date, datetime
from typing import Literal, Optional


Priority = Literal["Low", "Medium", "High", "Critical"]
Status = Literal["To Do", "In Progress", "In Review", "Done", "Blocked"]


class TaskCreate(BaseModel):
    title: str
    desc: Optional[str] = None
    priority: Priority = "Medium"
    due_date: Optional[date] = None


class TaskUpdate(BaseModel):
    title: Optional[str] = None
    desc: Optional[str] = None
    priority: Optional[Priority] = None
    due_date: Optional[date] = None


class StatusUpdate(BaseModel):
    status: Status


class TaskOut(BaseModel):
    id: int
    title: str
    desc: Optional[str] = None
    priority: str
    status: str
    due_date: Optional[date] = None
    created_at: datetime
    creator_id: int
    assignee_id: Optional[int] = None
    file_path: Optional[str] = None
    bronze_file_path: Optional[str] = None
    silver_file_path: Optional[str] = None
    gold_file_path: Optional[str] = None

    @field_validator("file_path", "bronze_file_path", "silver_file_path", "gold_file_path", mode="before")
    @classmethod
    def filename_only(cls, v: Optional[str]) -> Optional[str]:
       
        return Path(v).name if v else None

    model_config = ConfigDict(from_attributes=True)
