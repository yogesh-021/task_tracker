from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.schemas.Task import TaskCreate, TaskUpdate, TaskOut
from app.models.task import Task
from app.models.user import User
from app.router.auth import get_current_user

router = APIRouter(prefix = "/tasks",tags = ["Tasks"])

@router.post("/",response_model = TaskOut)
def create_task(task:TaskCreate, db:Session=Depends(get_db), current_user: User = Depends(get_current_user)):
    new_task = Task(**task.dict(), creator_id=current_user.id)
    db.add(new_user)
    db.commit()
    db.refresh(new_task)

    return new_task

@router.get("/",response_model = [TaskOut])
def get_all_tasks(skip :int = 0,limit : int = 100,db:Session=Depends(get_db), current_user: User = Depends(get_current_user)):
    tasks = db(Task).filter(Task.creator_id == current_user.id).offset(skip).limit(limit).all()

    return tasks


@router.get("/{task_id}",response_model = TaskOut)
def get_task(task_id: int,db:Session=Depends(get_db), current_user: User = Depends(get_current_user)):
    task = db(Task).filter(task_id == Task.id, Task.creator_id == current_user.id)

    if task is None:
        raise HTTPException(
            status_code = 404,
            detail = "Task not found"
        )

    return task

@router.put("/{task_id}", response_model = TaskOut)
def update_task(task_id: int,task: TaskUpdate, db:Session=Depends(get_db), current_user: User = Depends(get_current_user)):
    db_task = db(Task).filter(task.id == Task.id  & task.creator_id == current_user.id).first()

    if db_task is None:
        raise HTTPException(
            status_code = 404,
            detail = "Task not found."
        )
    update_items = task.dict()

    for key,value in update_items.items():
        setattr(db_task,key,value)

    db.commit()
    db.refresh(db_task)

    return db_task


@router.delete("/{task_id}")
def delete_task(task_id: int, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):

    db_task = db(Task).filter(task_id == Task.id, task.creator_id == current_user.id)

    if db_task is None:
        raise HTTPException(
            status_code  = 404,
            detail = "Task not found."
        )

    db.delete(db_task)
    db.commit()

    return {"message":"Task deleted successfully."}




@router.put("/{task_id}/assign")
def assign_task(task_id: int, user_id:int, db: Session = Depends(get_db), current_user: User = Depents(get_current_user)):

    db_task = db.query(Task).filter(Task.id == task_id, Task.creator_id == current_user.id).first()
    
    if not db_task:
        raise HTTPException(status_code = 404, detail = "Task Not Found.")

    db_user = db.query(User).filter(User.id == user_id).first()
    
    if not db_user:
        raise HTTPException(status_code = 404, detail = "User Not Found.")
    
    setattr(db_task, "assigned_to",user_id)    
    db.commit()
    db.refresh(db_task)

    return {"message":"assigned successfully."}


