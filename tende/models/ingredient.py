import psycopg
from typing import List, Optional
from uuid import UUID
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class Ingredient:
    id: UUID
    name: str
    unit: str
    cost_per_unit: float
    density: Optional[float] = None

class IngredientRepository:
    def __init__(self, db):
        self.db = db

    async def create(self, ingredient: Ingredient) -> Ingredient:
        """Create a new ingredient in the database."""
        logger.info("Creating new ingredient", extra={
            "operation": "create_ingredient",
            "ingredient_name": ingredient.name,
            "ingredient_unit": ingredient.unit
        })
        
        try:
            async with self.db.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO ingredients (id, name, unit, cost_per_unit, density) 
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        str(ingredient.id),
                        ingredient.name,
                        ingredient.unit,
                        ingredient.cost_per_unit,
                        ingredient.density
                    )
                )
            logger.info("Successfully created ingredient", extra={
                "operation": "create_ingredient",
                "ingredient_id": str(ingredient.id),
                "ingredient_name": ingredient.name,
                "status": "success"
            })
            return ingredient
        except (psycopg.OperationalError, psycopg.DataError, 
                psycopg.IntegrityError, psycopg.ProgrammingError) as e:
            logger.exception("Failed to create ingredient", extra={
                "operation": "create_ingredient",
                "ingredient_name": ingredient.name,
                "error_type": type(e).__name__
            })
            raise

    async def get_by_id(self, ingredient_id: UUID) -> Optional[Ingredient]:
        """Get an ingredient by its ID."""
        logger.info("Fetching ingredient", extra={
            "operation": "get_ingredient",
            "ingredient_id": str(ingredient_id)
        })
        
        try:
            async with self.db.cursor() as cur:
                await cur.execute(
                    """
                    SELECT id, name, unit, cost_per_unit, density 
                    FROM ingredients 
                    WHERE id = %s
                    """,
                    (str(ingredient_id),)
                )
                row = await cur.fetchone()
                
                if not row:
                    logger.warning("Ingredient not found", extra={
                        "operation": "get_ingredient",
                        "ingredient_id": str(ingredient_id),
                        "status": "not_found"
                    })
                    return None

            logger.info("Successfully fetched ingredient", extra={
                "operation": "get_ingredient",
                "ingredient_id": str(ingredient_id),
                "ingredient_name": row[1],
                "status": "success"
            })
            return Ingredient(
                id=row[0],
                name=row[1],
                unit=row[2],
                cost_per_unit=float(row[3]),
                density=row[4]
            )
        except (psycopg.OperationalError, psycopg.DataError, 
                psycopg.IntegrityError, psycopg.ProgrammingError) as e:
            logger.exception("Failed to fetch ingredient", extra={
                "operation": "get_ingredient",
                "ingredient_id": str(ingredient_id),
                "error_type": type(e).__name__
            })
            raise

    async def list_all(self, page: int = 1, size: int = 10, name_filter: Optional[str] = None) -> tuple[List[Ingredient], int]:
        """Get a list of ingredients with optional filtering and pagination."""
        logger.info("Fetching ingredients", extra={
            "operation": "list_ingredients",
            "page": page,
            "page_size": size,
            "filter": name_filter
        })
        
        try:
            async with self.db.cursor() as cur:
                # Build query with filters
                query = "SELECT id, name, unit, cost_per_unit, density FROM ingredients"
                params = []
                
                if name_filter:
                    query += " WHERE name ILIKE %s"
                    params.append(f"%{name_filter}%")
                
                # Add pagination
                offset = (page - 1) * size
                query += f" LIMIT {size} OFFSET {offset}"
                
                await cur.execute(query, params)
                rows = await cur.fetchall()

                # Get total count for pagination
                count_query = "SELECT COUNT(*) FROM ingredients"
                if name_filter:
                    count_query += " WHERE name ILIKE %s"
                await cur.execute(count_query, params)
                total_count = (await cur.fetchone())[0]

            logger.info("Successfully fetched ingredients", extra={
                "operation": "list_ingredients",
                "count": len(rows),
                "total_count": total_count,
                "page": page,
                "page_size": size,
                "status": "success"
            })
            
            return [
                Ingredient(
                    id=row[0],
                    name=row[1],
                    unit=row[2],
                    cost_per_unit=float(row[3]),
                    density=row[4]
                ) for row in rows
            ], total_count
        except (psycopg.OperationalError, psycopg.DataError, 
                psycopg.IntegrityError, psycopg.ProgrammingError) as e:
            logger.exception("Failed to fetch ingredients", extra={
                "operation": "list_ingredients",
                "page": page,
                "page_size": size,
                "error_type": type(e).__name__
            })
            raise

    async def update(self, ingredient: Ingredient) -> Ingredient:
        """Update an existing ingredient."""
        logger.info("Updating ingredient", extra={
            "operation": "update_ingredient",
            "ingredient_id": str(ingredient.id),
            "ingredient_name": ingredient.name
        })
        
        try:
            async with self.db.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE ingredients 
                    SET name = %s, unit = %s, cost_per_unit = %s, density = %s
                    WHERE id = %s
                    """,
                    (
                        ingredient.name,
                        ingredient.unit,
                        ingredient.cost_per_unit,
                        ingredient.density,
                        str(ingredient.id)
                    )
                )
            logger.info("Successfully updated ingredient", extra={
                "operation": "update_ingredient",
                "ingredient_id": str(ingredient.id),
                "ingredient_name": ingredient.name,
                "status": "success"
            })
            return ingredient
        except (psycopg.OperationalError, psycopg.DataError, 
                psycopg.IntegrityError, psycopg.ProgrammingError) as e:
            logger.exception("Failed to update ingredient", extra={
                "operation": "update_ingredient",
                "ingredient_id": str(ingredient.id),
                "error_type": type(e).__name__
            })
            raise

    async def delete(self, ingredient_id: UUID) -> None:
        """Delete an ingredient."""
        logger.info("Deleting ingredient", extra={
            "operation": "delete_ingredient",
            "ingredient_id": str(ingredient_id)
        })
        
        try:
            async with self.db.cursor() as cur:
                await cur.execute(
                    "DELETE FROM ingredients WHERE id = %s",
                    (str(ingredient_id),)
                )
            logger.info("Successfully deleted ingredient", extra={
                "operation": "delete_ingredient",
                "ingredient_id": str(ingredient_id),
                "status": "success"
            })
        except (psycopg.OperationalError, psycopg.DataError, 
                psycopg.IntegrityError, psycopg.ProgrammingError) as e:
            logger.exception("Failed to delete ingredient", extra={
                "operation": "delete_ingredient",
                "ingredient_id": str(ingredient_id),
                "error_type": type(e).__name__
            })
            raise 