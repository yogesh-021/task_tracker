from pydantic import BaseModel, EmailStr, ConfigDict
from datetime import datetime
from typing import Literal


class UserCreate(BaseModel):
    username: str
    password: str
    email: EmailStr
    role: Literal["manager", "developer"] = "developer"


class UserOut(BaseModel):
    id: int
    username: str
    email: EmailStr
    created_at: datetime
    role: str
    is_active: bool

    model_config = ConfigDict(from_attributes=True)


class RoleUpdate(BaseModel):
    role: Literal["admin", "manager", "developer"]
