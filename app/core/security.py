# For password hashing

from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def hash_password(original_password:str) -> str:
    return pwd_context.hash(original_password)

def verify_password(original_password:str, hashed_password:str) -> bool:
    return pwd_context.verify(original_password,hashed_password)

