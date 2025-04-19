import os
import sys
import pytest
import psycopg
from datetime import datetime, UTC
from uuid import uuid4
from models import IngredientRepository, FormulaRepository, InvoiceRepository

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tende.main import app, init_repositories

# Test database configuration
TEST_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/tende_test"

@pytest.fixture(scope="session")
async def test_db():
    """Create a test database connection."""
    conn = await psycopg.AsyncConnection.connect(TEST_DATABASE_URL, autocommit=True)
    yield conn
    await conn.close()

@pytest.fixture(scope="session")
async def ingredient_repo(test_db):
    """Initialize the ingredient repository."""
    return IngredientRepository(test_db)

@pytest.fixture(scope="session")
async def formula_repo(test_db):
    """Initialize the formula repository."""
    return FormulaRepository(test_db)

@pytest.fixture(scope="session")
async def invoice_repo(test_db):
    """Initialize the invoice repository."""
    return InvoiceRepository(test_db, "uploads/invoices")

@pytest.fixture(scope="session")
async def test_client():
    """Create a test client for the FastAPI application."""
    from fastapi.testclient import TestClient
    return TestClient(app)

@pytest.fixture(autouse=True)
async def setup_teardown(test_db):
    """Setup and teardown for each test."""
    # Setup: Clear and initialize test data
    async with test_db.cursor() as cur:
        await cur.execute("TRUNCATE TABLE ingredients CASCADE")
        await cur.execute("TRUNCATE TABLE formulas CASCADE")
        await cur.execute("TRUNCATE TABLE invoices CASCADE")
    
    yield
    
    # Teardown: Clean up test data
    async with test_db.cursor() as cur:
        await cur.execute("TRUNCATE TABLE ingredients CASCADE")
        await cur.execute("TRUNCATE TABLE formulas CASCADE")
        await cur.execute("TRUNCATE TABLE invoices CASCADE")

@pytest.fixture
def sample_ingredient():
    """Sample ingredient data for testing."""
    return {
        "name": "Test Ingredient",
        "unit": "ml",
        "cost_per_unit": 10.50,
        "density": 1.0
    }

@pytest.fixture
def sample_formula():
    """Sample formula data for testing."""
    return {
        "name": "Test Formula",
        "description": "Test Description",
        "ingredients": {
            str(uuid4()): 50.0,
            str(uuid4()): 50.0
        },
        "mass": 100.0
    }

@pytest.fixture
def sample_invoice():
    """Sample invoice data for testing."""
    return {
        "date": datetime.now(UTC).isoformat(),
        "supplier": "Test Supplier",
        "pdf_path": "test.pdf",
        "ingredients": {
            str(uuid4()): {
                "quantity": 100,
                "unit_price": 10.50
            }
        }
    } 