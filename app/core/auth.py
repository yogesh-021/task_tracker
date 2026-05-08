# For JWT Token 

from app.core.config import settings
from datetime import datetime, timedelta, timezone
from jose import JWTError, jwt


def create_access_token(data:dict)-> str:
    data_to_encode=data.copy()
    expiry = datetime.now(timezone.utc) + timedelta(minutes = settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    data_to_encode.update({"exp":expiry})
    return jwt.encode(data_to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)

def decode_token(token:str)-> str | None:
    try:
        payload=jwt.decode(token,settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        username: str = payload.get("sub")
        return username
    except JWTError:
        return None
