import os
import uuid
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

from app.core.medallion import VALID_LAYERS, transform
from app.database import get_db
from app.models.task import Task
from app.models.user import User
from app.router.auth import get_current_user, require_developer, require_manager
from app.schemas.Task import StatusUpdate, TaskOut, TaskUpdate

UPLOAD_DIR = Path(__file__).resolve().parent.parent.parent / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

VALID_PRIORITIES = {"Low", "Medium", "High", "Critical"}

_SORT_MAP = {
    "created_at": Task.created_at,
    "due_date": Task.due_date,
    "priority": Task.priority,
    "status": Task.status,
    "title": Task.title,
}

router = APIRouter(prefix="/tasks", tags=["Tasks"])


def _apply_filters_and_sort(query, status, priority, sort_by, sort_order):
    if status:
        query = query.filter(Task.status == status)
    if priority:
        query = query.filter(Task.priority == priority)
    col = _SORT_MAP.get(sort_by, Task.created_at)
    query = query.order_by(col.desc() if sort_order == "desc" else col.asc())
    return query


# ─────────────────────────── Manager endpoints ────────────────────────────

@router.get("/", response_model=list[TaskOut], summary="Manager dashboard — own tasks")
def manager_dashboard(
    status: Optional[str] = Query(None, description="Filter by status"),
    priority: Optional[str] = Query(None, description="Filter by priority"),
    sort_by: str = Query("created_at", description="Field to sort by: created_at | due_date | priority | status | title"),
    sort_order: str = Query("desc", description="asc or desc"),
    skip: int = Query(0),
    limit: int = Query(100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_manager),
):
    query = db.query(Task)
    if current_user.role != "admin":
        query = query.filter(Task.creator_id == current_user.id)
    query = _apply_filters_and_sort(query, status, priority, sort_by, sort_order)
    return query.offset(skip).limit(limit).all()


@router.post("/", response_model=TaskOut, summary="Create a task with optional CSV attachment (manager)")
async def create_task(
    title: str = Form(...),
    desc: Optional[str] = Form(None),
    priority: Optional[str] = Form("Medium"),
    due_date: Optional[str] = Form(None, description="Date in YYYY-MM-DD format, e.g. 2025-12-31"),
    file: Optional[UploadFile] = File(None, description="Optional CSV file"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_manager),
):
    if priority and priority not in VALID_PRIORITIES:
        raise HTTPException(status_code=400, detail=f"Invalid priority. Choose from: {', '.join(VALID_PRIORITIES)}")

    parsed_due_date = None
    if due_date:
        try:
            parsed_due_date = date.fromisoformat(due_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid due_date. Use YYYY-MM-DD, e.g. 2025-12-31")

    file_path = None
    if file:
        if not file.filename.lower().endswith(".csv"):
            raise HTTPException(status_code=400, detail="Only .csv files are accepted.")
        unique_name = f"{uuid.uuid4()}.csv"
        save_path = UPLOAD_DIR / unique_name
        save_path.write_bytes(await file.read())
        file_path = str(save_path)

    new_task = Task(
        title=title,
        desc=desc,
        priority=priority or "Medium",
        due_date=parsed_due_date,
        file_path=file_path,
        creator_id=current_user.id,
    )
    db.add(new_task)
    db.commit()
    db.refresh(new_task)
    return new_task


@router.put("/{task_id}", response_model=TaskOut, summary="Update task details (manager)")
def update_task(
    task_id: int,
    task: TaskUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_manager),
):
    db_task = db.query(Task).filter(Task.id == task_id).first()
    if not db_task:
        raise HTTPException(status_code=404, detail="Task not found.")
    if current_user.role != "admin" and db_task.creator_id != current_user.id:
        raise HTTPException(status_code=403, detail="Insufficient permissions.")

    for key, value in task.model_dump(exclude_unset=True).items():
        setattr(db_task, key, value)
    db.commit()
    db.refresh(db_task)
    return db_task


@router.delete("/{task_id}", summary="Delete a task (manager)")
def delete_task(
    task_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_manager),
):
    db_task = db.query(Task).filter(Task.id == task_id).first()
    if not db_task:
        raise HTTPException(status_code=404, detail="Task not found.")
    if current_user.role != "admin" and db_task.creator_id != current_user.id:
        raise HTTPException(status_code=403, detail="Insufficient permissions.")
    for path_col in [db_task.file_path, db_task.bronze_file_path, db_task.silver_file_path, db_task.gold_file_path]:
        if path_col and os.path.exists(path_col):
            os.remove(path_col)
    db.delete(db_task)
    db.commit()
    return {"message": "Task deleted successfully."}


@router.put("/{task_id}/assign", summary="Assign task to a developer (manager)")
def assign_task(
    task_id: int,
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_manager),
):
    db_task = db.query(Task).filter(Task.id == task_id).first()
    if not db_task:
        raise HTTPException(status_code=404, detail="Task not found.")
    if current_user.role == "manager" and db_task.creator_id != current_user.id:
        raise HTTPException(status_code=403, detail="Managers can only assign their own tasks.")

    db_user = db.query(User).filter(User.id == user_id, User.role == "developer").first()
    if not db_user:
        raise HTTPException(status_code=404, detail="Developer not found.")

    db_task.assignee_id = user_id
    db.commit()
    db.refresh(db_task)
    return {"message": f"Task assigned to '{db_user.username}' successfully."}


# ─────────────────────────── Shared endpoints ─────────────────────────────

@router.put("/{task_id}/status", response_model=TaskOut, summary="Update task status (manager or developer)")
def update_status(
    task_id: int,
    body: StatusUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    db_task = db.query(Task).filter(Task.id == task_id).first()
    if not db_task:
        raise HTTPException(status_code=404, detail="Task not found.")

    role = current_user.role
    if role == "manager" and db_task.creator_id != current_user.id:
        raise HTTPException(status_code=403, detail="Insufficient permissions.")
    if role == "developer" and db_task.assignee_id != current_user.id:
        raise HTTPException(status_code=403, detail="This task is not assigned to you.")

    db_task.status = body.status
    db.commit()
    db.refresh(db_task)
    return db_task


# ─────────────────────────── Developer endpoints ──────────────────────────

# NOTE: /assigned MUST be declared before /{task_id} to prevent FastAPI
# treating "assigned" as an integer path parameter.
@router.get("/assigned", response_model=list[TaskOut], summary="Developer dashboard — assigned tasks")
def developer_dashboard(
    status: Optional[str] = Query(None, description="Filter by status"),
    priority: Optional[str] = Query(None, description="Filter by priority"),
    sort_by: str = Query("created_at", description="Field to sort by: created_at | due_date | priority | status | title"),
    sort_order: str = Query("desc", description="asc or desc"),
    skip: int = Query(0),
    limit: int = Query(100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_developer),
):
    query = db.query(Task)
    if current_user.role != "admin":
        query = query.filter(Task.assignee_id == current_user.id)
    query = _apply_filters_and_sort(query, status, priority, sort_by, sort_order)
    return query.offset(skip).limit(limit).all()


@router.post("/{task_id}/transform/{layer}", summary="Apply medallion transformation on the task's CSV (developer)")
def apply_transformation(
    task_id: int,
    layer: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_developer),
):
    if layer not in VALID_LAYERS:
        raise HTTPException(status_code=400, detail=f"Invalid layer. Choose from: {', '.join(sorted(VALID_LAYERS))}")

    db_task = db.query(Task).filter(Task.id == task_id).first()
    if not db_task:
        raise HTTPException(status_code=404, detail="Task not found.")
    if current_user.role == "developer" and db_task.assignee_id != current_user.id:
        raise HTTPException(status_code=403, detail="This task is not assigned to you.")
    if not db_task.file_path:
        raise HTTPException(status_code=400, detail="No CSV file is attached to this task.")
    if not os.path.exists(db_task.file_path):
        raise HTTPException(status_code=404, detail="File not found on server.")

    try:
        result = transform(db_task.file_path, layer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Transformation failed: {e}")

    # Save transformed data as a new CSV on disk
    original_stem = Path(db_task.file_path).stem
    output_filename = f"{original_stem}_{layer}.csv"
    output_path = UPLOAD_DIR / output_filename
    pd.DataFrame(result["data"]).to_csv(output_path, index=False)

    # Persist the output path in the task record
    setattr(db_task, f"{layer}_file_path", str(output_path))
    db.commit()

    result["saved_as"] = output_filename
    return result
