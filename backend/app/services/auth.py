from sqlalchemy.orm import Session
from app.models.user import User
from app.core.security import verify_password


def authenticate_user(db: Session, username: str, password: str) -> User:
    user = db.query(User).filter(
        (User.username == username) | (User.email == username)
    ).first()
    if not user:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return user