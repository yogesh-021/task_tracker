from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from app.schemas.User import UserCreate, UserOut
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
        hashed_password = hash_password(user.password),
        role = user.role,
    )

    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    return new_user

@router.post("/login", response_model = Token)
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.username == form_data.username).first()

    if not db_user or not verify_password(form_data.password, db_user.hashed_password):
        raise HTTPException(
            status_code = 401,
            detail = "Invalid username or password"
        )

    access_token = create_access_token(data={"sub": db_user.username})
    return {"access_token": access_token, "token_type": "bearer"}



oauth2 = OAuth2PasswordBearer(tokenUrl = "auth/login")

def get_current_user(token: str = Depends(oauth2), db: Session = Depends(get_db)) -> User:
    username = decode_token(token)

    if not username:
        raise HTTPException(status_code=401, detail="Invalid credentials.")

    user = db.query(User).filter(User.username == username).first()
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials.")
    if not user.is_active:
        raise HTTPException(status_code=403, detail="Account is deactivated.")

    return user


def require_role(*roles: str):
    """Return a dependency that enforces the current user has one of the given roles."""
    def checker(current_user: User = Depends(get_current_user)) -> User:
        if current_user.role not in roles:
            raise HTTPException(status_code=403, detail="Insufficient permissions.")
        return current_user
    return checker


require_admin = require_role("admin")
require_manager = require_role("admin", "manager")
require_developer = require_role("admin", "developer")
