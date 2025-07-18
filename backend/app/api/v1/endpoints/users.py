from typing import Any, List
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.user import User
from app.schemas.user import UserCreate, UserResponse
from app.services.user import create_user, get_user_by_email
from app.api.deps import get_current_user

router = APIRouter()


@router.post("/", response_model=UserResponse)
def create_new_user(
    user_in: UserCreate,
    db: Session = Depends(get_db),
) -> Any:
    user = get_user_by_email(db, email=user_in.email)
    if user:
        raise HTTPException(
            status_code=400,
            detail="A user with this email already exists.",
        )
    user = create_user(db=db, user_create=user_in)
    return user


@router.get("/me", response_model=UserResponse)
def read_user_me(
    current_user: User = Depends(get_current_user),
) -> Any:
    return current_user