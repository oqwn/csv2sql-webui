import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from app.db.session import SessionLocal
from app.services.user import create_user
from app.schemas.user import UserCreate


def create_admin_user():
    db = SessionLocal()
    try:
        admin_user = UserCreate(
            email="admin@example.com",
            username="admin",
            password="adminpassword",
            full_name="Admin User",
            is_superuser=True,
            is_active=True
        )
        
        user = create_user(db, admin_user)
        print(f"Admin user created successfully: {user.email}")
    except Exception as e:
        print(f"Error creating admin user: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    create_admin_user()