from sqlalchemy.orm import Session
from app.models.user import User
from app.schemas.user import UserCreate
from app.core.security import get_password_hash


def get_user_by_email(db: Session, email: str) -> User:
    return db.query(User).filter(User.email == email).first()


def get_user_by_username(db: Session, username: str) -> User:
    return db.query(User).filter(User.username == username).first()


def create_user(db: Session, user_create: UserCreate) -> User:
    db_user = User(
        email=user_create.email,
        username=user_create.username,
        hashed_password=get_password_hash(user_create.password),
        full_name=user_create.full_name,
        is_active=user_create.is_active,
        is_superuser=user_create.is_superuser,
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user