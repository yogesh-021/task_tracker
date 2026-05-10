from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from app.schemas.User import UserCreate, UserLogin, UserOut
from app.schemas.Token import Token
from app.core.auth import create_access_token, decode_token
from app.core.security import hash_password, verify_password
from app.database import get_db
from app.models.user import User

router = APIRouter(prefix="/auth", tags = ["Authentication"])

@router.post("/register", response_model = UserOut)
def register(user:UserCreate, db:Session = Depends(get_db)):
    already_registered = db.query(User).filter((User.username == user.username) | (User.email == user.email)).first()
    if already_registered:
        raise HTTPException(
            status_code=400,
            detail="Username or email already registered."
        )

    new_user = User(
        username = user.username,
        email = user.email,
        hashed_pwd = hash_password(user.password)
    )

    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    return new_user

@router.post("/login", response_model = Token)
def login(user:UserLogin, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.username == user.username).first()

    if not db_user or not verify_password(user.password,db_user.hashed_pwd):
        raise HTTPException(
            status_code = 401,
            detail = "Invalid username or password"
        )

    access_token = create_access_token(data={"sub": db_user.username})
    return {"access_token": access_token, "token_type": "bearer"}



oauth2 = OAuth2PasswordBearer(tokenUrl = "auth/login")

def get_current_user(token: str = Depends(oauth2),db: Session = Depends(get_db)) -> User:
    username = decode_token(token)

    if not username:
        raise HTTPException(status_code = 401, detail = "Invalid credentials.")

    user = db.query(User).filter(User.username == username).first()
    if not user:
        raise HTTPException(status_code = 401, detail = "Invalid credentials.")

    return user
