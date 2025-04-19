import psycopg
from typing import List, Optional, Dict
from uuid import UUID
from dataclasses import dataclass
import json
import logging

logger = logging.getLogger(__name__)

@dataclass
class Formula:
    id: UUID
    name: str
    description: str
    ingredients: Dict[str, float]  # ingredient_id -> percentage
    mass: float

class FormulaRepository:
    def __init__(self, db):
        self.db = db

    async def create(self, formula: Formula) -> Formula:
        """Create a new formula in the database."""
        logger.info("Creating new formula", extra={
            "operation": "create_formula",
            "formula_name": formula.name,
            "ingredient_count": len(formula.ingredients)
        })
        
        try:
            async with self.db.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO formulas (id, name, description, ingredients, mass) 
                    VALUES (%s, %s, %s, %s::jsonb, %s)
                    """,
                    (
                        str(formula.id),
                        formula.name,
                        formula.description,
                        json.dumps(formula.ingredients),
                        formula.mass
                    )
                )
            logger.info("Successfully created formula", extra={
                "operation": "create_formula",
                "formula_id": str(formula.id),
                "formula_name": formula.name,
                "status": "success"
            })
            return formula
        except (psycopg.OperationalError, psycopg.DataError, 
                psycopg.IntegrityError, psycopg.ProgrammingError) as e:
            logger.exception("Failed to create formula", extra={
                "operation": "create_formula",
                "formula_name": formula.name,
                "error_type": type(e).__name__
            })
            raise

    async def get_by_id(self, formula_id: UUID) -> Optional[Formula]:
        """Get a formula by its ID."""
        logger.info("Fetching formula", extra={
            "operation": "get_formula",
            "formula_id": str(formula_id)
        })
        
        try:
            async with self.db.cursor() as cur:
                await cur.execute(
                    """
                    SELECT id, name, description, ingredients, mass 
                    FROM formulas 
                    WHERE id = %s
                    """,
                    (str(formula_id),)
                )
                row = await cur.fetchone()
                
                if not row:
                    logger.warning("Formula not found", extra={
                        "operation": "get_formula",
                        "formula_id": str(formula_id),
                        "status": "not_found"
                    })
                    return None

            logger.info("Successfully fetched formula", extra={
                "operation": "get_formula",
                "formula_id": str(formula_id),
                "formula_name": row[1],
                "status": "success"
            })
            return Formula(
                id=row[0],
                name=row[1],
                description=row[2],
                ingredients=row[3],
                mass=row[4]
            )
        except (psycopg.OperationalError, psycopg.DataError, 
                psycopg.IntegrityError, psycopg.ProgrammingError) as e:
            logger.exception("Failed to fetch formula", extra={
                "operation": "get_formula",
                "formula_id": str(formula_id),
                "error_type": type(e).__name__
            })
            raise

    async def list_all(self, page: int = 1, size: int = 10) -> tuple[List[Formula], int]:
        """Get a list of formulas with pagination."""
        logger.info("Fetching formulas", extra={
            "operation": "list_formulas",
            "page": page,
            "page_size": size
        })
        
        try:
            async with self.db.cursor() as cur:
                # Build query with pagination
                offset = (page - 1) * size
                query = f"""
                    SELECT id, name, description, ingredients, mass 
                    FROM formulas 
                    LIMIT {size} OFFSET {offset}
                """
                await cur.execute(query)
                rows = await cur.fetchall()

                # Get total count
                await cur.execute("SELECT COUNT(*) FROM formulas")
                total_count = (await cur.fetchone())[0]

            logger.info("Successfully fetched formulas", extra={
                "operation": "list_formulas",
                "count": len(rows),
                "total_count": total_count,
                "page": page,
                "page_size": size,
                "status": "success"
            })
            
            return [
                Formula(
                    id=row[0],
                    name=row[1],
                    description=row[2],
                    ingredients=row[3],
                    mass=row[4]
                ) for row in rows
            ], total_count
        except (psycopg.OperationalError, psycopg.DataError, 
                psycopg.IntegrityError, psycopg.ProgrammingError) as e:
            logger.exception("Failed to fetch formulas", extra={
                "operation": "list_formulas",
                "page": page,
                "page_size": size,
                "error_type": type(e).__name__
            })
            raise

    async def update(self, formula: Formula) -> Formula:
        """Update an existing formula."""
        logger.info("Updating formula", extra={
            "operation": "update_formula",
            "formula_id": str(formula.id),
            "formula_name": formula.name
        })
        
        try:
            async with self.db.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE formulas 
                    SET name = %s, description = %s, ingredients = %s::jsonb, mass = %s
                    WHERE id = %s
                    """,
                    (
                        formula.name,
                        formula.description,
                        json.dumps(formula.ingredients),
                        formula.mass,
                        str(formula.id)
                    )
                )
            logger.info("Successfully updated formula", extra={
                "operation": "update_formula",
                "formula_id": str(formula.id),
                "formula_name": formula.name,
                "status": "success"
            })
            return formula
        except (psycopg.OperationalError, psycopg.DataError, 
                psycopg.IntegrityError, psycopg.ProgrammingError) as e:
            logger.exception("Failed to update formula", extra={
                "operation": "update_formula",
                "formula_id": str(formula.id),
                "error_type": type(e).__name__
            })
            raise

    async def delete(self, formula_id: UUID) -> None:
        """Delete a formula."""
        logger.info("Deleting formula", extra={
            "operation": "delete_formula",
            "formula_id": str(formula_id)
        })
        
        try:
            async with self.db.cursor() as cur:
                await cur.execute(
                    "DELETE FROM formulas WHERE id = %s",
                    (str(formula_id),)
                )
            logger.info("Successfully deleted formula", extra={
                "operation": "delete_formula",
                "formula_id": str(formula_id),
                "status": "success"
            })
        except (psycopg.OperationalError, psycopg.DataError, 
                psycopg.IntegrityError, psycopg.ProgrammingError) as e:
            logger.exception("Failed to delete formula", extra={
                "operation": "delete_formula",
                "formula_id": str(formula_id),
                "error_type": type(e).__name__
            })
            raise

    async def search(self, query: str, page: int = 1, size: int = 10) -> tuple[List[Formula], int]:
        """Search for formulas using full-text search."""
        logger.info("Searching formulas", extra={
            "operation": "search_formulas",
            "search_term": query,
            "page": page,
            "page_size": size
        })
        
        try:
            async with self.db.cursor() as cur:
                # Build search query
                offset = (page - 1) * size
                search_query = f"""
                    SELECT id, name, description, ingredients, mass,
                           ts_rank_cd(search_vector, plainto_tsquery('english', %s)) as rank
                    FROM formulas
                    WHERE search_vector @@ plainto_tsquery('english', %s)
                    ORDER BY rank DESC
                    LIMIT {size} OFFSET {offset}
                """
                await cur.execute(search_query, (query, query))
                rows = await cur.fetchall()

                # Get total count
                count_query = """
                    SELECT COUNT(*)
                    FROM formulas
                    WHERE search_vector @@ plainto_tsquery('english', %s)
                """
                await cur.execute(count_query, (query,))
                total_count = (await cur.fetchone())[0]

            logger.info("Successfully searched formulas", extra={
                "operation": "search_formulas",
                "search_term": query,
                "count": len(rows),
                "total_count": total_count,
                "page": page,
                "page_size": size,
                "status": "success"
            })
            
            return [
                Formula(
                    id=row[0],
                    name=row[1],
                    description=row[2],
                    ingredients=row[3],
                    mass=row[4]
                ) for row in rows
            ], total_count
        except (psycopg.OperationalError, psycopg.DataError, 
                psycopg.IntegrityError, psycopg.ProgrammingError) as e:
            logger.exception("Failed to search formulas", extra={
                "operation": "search_formulas",
                "search_term": query,
                "error_type": type(e).__name__
            })
            raise 