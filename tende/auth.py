from pydantic import BaseModel
from uuid import UUID
from fastapi import Depends

class User(BaseModel):
    id: UUID
    email: str
    is_active: bool = True

async def get_current_user() -> User:
    """Get the current authenticated user.
    For now, this is a mock implementation that returns a default user.
    In a real application, this would validate a JWT token or session cookie.
    """
    return User(
        id=UUID("00000000-0000-0000-0000-000000000000"),
        email="default@example.com"
    ) 