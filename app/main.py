from fastapi import FastAPI
from app.database import Base, engine
from app.models import user, task
from app.router.auth import router as auth_router
from app.router.task import router as task_router


app = FastAPI(title= "Task Tracker", version="1.0")

app.include_router(auth_router)
app.include_router(task_router)

Base.metadata.create_all(bind=engine)

@app.get("/")
def home():
    return {"Message":"Task Tracker"}


