import psycopg
from typing import List, Optional
from uuid import UUID
from dataclasses import dataclass
from datetime import datetime
import logging
import os
import json
import shutil

logger = logging.getLogger(__name__)

@dataclass
class Invoice:
    id: UUID
    date: datetime
    supplier: str
    pdf_path: str
    ingredients: List[dict]  # List of ingredient data with additional metadata

class InvoiceRepository:
    def __init__(self, db, upload_dir: str):
        self.db = db
        self.upload_dir = upload_dir

    async def create(self, invoice: Invoice, file: bytes) -> Invoice:
        """Create a new invoice in the database and save the PDF file."""
        logger.info("Creating new invoice", extra={
            "operation": "create_invoice",
            "supplier": invoice.supplier,
            "invoice_id": str(invoice.id),
            "file_name": os.path.basename(invoice.pdf_path)
        })
        
        try:
            # Save the PDF file
            file_path = os.path.join(self.upload_dir, invoice.pdf_path)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "wb") as f:
                f.write(file)
            
            # Convert ingredients list to JSON string
            ingredients_json = json.dumps(invoice.ingredients)
            
            # Insert into database
            async with self.db.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO invoices (id, date, supplier, pdf_path, ingredients) 
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        str(invoice.id),
                        invoice.date,
                        invoice.supplier,
                        invoice.pdf_path,
                        ingredients_json
                    )
                )
            logger.info("Successfully created invoice", extra={
                "operation": "create_invoice",
                "invoice_id": str(invoice.id),
                "supplier": invoice.supplier,
                "status": "success"
            })
            return invoice
        except Exception as e:
            # Clean up the file if database operation failed
            if 'file_path' in locals() and os.path.exists(file_path):
                os.remove(file_path)
            raise

    async def get_by_id(self, invoice_id: UUID) -> Optional[Invoice]:
        """Get an invoice by its ID."""
        logger.info("Fetching invoice", extra={
            "operation": "get_invoice",
            "invoice_id": str(invoice_id)
        })
        
        try:
            async with self.db.cursor() as cur:
                await cur.execute(
                    """
                    SELECT id, date, supplier, pdf_path, ingredients 
                    FROM invoices 
                    WHERE id = %s
                    """,
                    (str(invoice_id),)
                )
                row = await cur.fetchone()
                
                if not row:
                    logger.warning("Invoice not found", extra={
                        "operation": "get_invoice",
                        "invoice_id": str(invoice_id),
                        "status": "not_found"
                    })
                    return None

            logger.info("Successfully fetched invoice", extra={
                "operation": "get_invoice",
                "invoice_id": str(invoice_id),
                "supplier": row[2],
                "status": "success"
            })
            return Invoice(
                id=row[0],
                date=row[1],
                supplier=row[2],
                pdf_path=row[3],
                ingredients=row[4]
            )
        except Exception as e:
            logger.exception("Failed to fetch invoice", extra={
                "operation": "get_invoice",
                "invoice_id": str(invoice_id),
                "error_type": type(e).__name__
            })
            raise

    async def list_all(self, page: int = 1, size: int = 10) -> tuple[List[Invoice], int]:
        """Get a list of invoices with pagination."""
        logger.info("Fetching invoices", extra={
            "operation": "list_invoices",
            "page": page,
            "page_size": size
        })
        
        try:
            async with self.db.cursor() as cur:
                # Build query with pagination
                query = """
                    SELECT id, date, supplier, pdf_path, ingredients
                    FROM invoices
                    ORDER BY date DESC
                    LIMIT %s OFFSET %s
                """
                
                offset = (page - 1) * size
                await cur.execute(query, (size, offset))
                rows = await cur.fetchall()

                # Get total count
                count_query = "SELECT COUNT(*) FROM invoices"
                await cur.execute(count_query)
                total_count = (await cur.fetchone())[0]

            logger.info("Successfully fetched invoices", extra={
                "operation": "list_invoices",
                "count": len(rows),
                "total_count": total_count,
                "page": page,
                "page_size": size,
                "status": "success"
            })
            
            return [
                Invoice(
                    id=row[0],
                    date=row[1],
                    supplier=row[2],
                    pdf_path=row[3],
                    ingredients=row[4] if row[4] else []
                ) for row in rows
            ], total_count
        except Exception as e:
            logger.exception("Failed to fetch invoices", extra={
                "operation": "list_invoices",
                "page": page,
                "page_size": size,
                "error_type": type(e).__name__
            })
            raise

    async def update(self, invoice: Invoice) -> Invoice:
        """Update an existing invoice."""
        logger.info("Updating invoice", extra={
            "operation": "update_invoice",
            "invoice_id": str(invoice.id),
            "supplier": invoice.supplier
        })
        
        try:
            async with self.db.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE invoices 
                    SET date = %s, supplier = %s, ingredients = %s::jsonb
                    WHERE id = %s
                    """,
                    (
                        invoice.date,
                        invoice.supplier,
                        invoice.ingredients,
                        str(invoice.id)
                    )
                )
            logger.info("Successfully updated invoice", extra={
                "operation": "update_invoice",
                "invoice_id": str(invoice.id),
                "supplier": invoice.supplier,
                "status": "success"
            })
            return invoice
        except (psycopg.OperationalError, psycopg.DataError, 
                psycopg.IntegrityError, psycopg.ProgrammingError) as e:
            logger.exception("Failed to update invoice", extra={
                "operation": "update_invoice",
                "invoice_id": str(invoice.id),
                "error_type": type(e).__name__
            })
            raise

    async def delete(self, invoice_id: UUID) -> None:
        """Delete an invoice and its associated PDF file."""
        logger.info("Deleting invoice", extra={
            "operation": "delete_invoice",
            "invoice_id": str(invoice_id)
        })
        
        try:
            # First get the PDF path
            async with self.db.cursor() as cur:
                await cur.execute(
                    "SELECT pdf_path FROM invoices WHERE id = %s",
                    (str(invoice_id),)
                )
                row = await cur.fetchone()
                if not row:
                    logger.warning("Invoice not found", extra={
                        "operation": "delete_invoice",
                        "invoice_id": str(invoice_id),
                        "status": "not_found"
                    })
                    return

                pdf_path = row[0]
                file_path = os.path.join(self.upload_dir, pdf_path)

                # Delete from database
                await cur.execute(
                    "DELETE FROM invoices WHERE id = %s",
                    (str(invoice_id),)
                )

                # Delete the file
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info("Successfully deleted invoice file", extra={
                        "operation": "delete_invoice",
                        "invoice_id": str(invoice_id),
                        "file_path": file_path
                    })

            logger.info("Successfully deleted invoice", extra={
                "operation": "delete_invoice",
                "invoice_id": str(invoice_id),
                "status": "success"
            })
        except (psycopg.OperationalError, psycopg.DataError, 
                psycopg.IntegrityError, psycopg.ProgrammingError) as e:
            logger.exception("Failed to delete invoice", extra={
                "operation": "delete_invoice",
                "invoice_id": str(invoice_id),
                "error_type": type(e).__name__
            })
            raise
        except (FileNotFoundError, PermissionError, OSError) as e:
            logger.exception("Failed to delete invoice file", extra={
                "operation": "delete_invoice",
                "invoice_id": str(invoice_id),
                "error_type": type(e).__name__
            })
            # Continue even if file deletion fails
            pass 