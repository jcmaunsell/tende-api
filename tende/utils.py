import psycopg
from fastapi import HTTPException, status

def jsonapi_response(formula: dict, included: list[dict]) -> dict:
    return {
        "data": formula,
        "included": included
    }

def handle_database_error(e: Exception) -> None:
    """Handle database errors and raise appropriate HTTP exceptions."""
    if isinstance(e, psycopg.OperationalError):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection error"
        )
    elif isinstance(e, psycopg.DataError):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid data format"
        )
    elif isinstance(e, psycopg.IntegrityError):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Database constraint violation"
        )
    elif isinstance(e, psycopg.ProgrammingError):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database programming error"
        )
    else:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected database error"
        )
