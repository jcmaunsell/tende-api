import json
import logging
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import List, Optional
from uuid import UUID, uuid4

# Configure Datadog APM
import ddtrace
import psycopg
from datadog import initialize, statsd
from ddtrace import config
from ddtrace.contrib.asgi import TraceMiddleware
from fastapi import (
    Depends,
    FastAPI,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    UploadFile,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp, Receive, Scope, Send

from tende.auth import User, get_current_user
from tende.datadog_logger import dd_logger
from tende.models import (
    FormulaRepository,
    IngredientRepository,
    Invoice,
    InvoiceRepository,
)
from tende.schemas import (
    BulkDeleteIngredientData,
    BulkDeleteIngredientIn,
    BulkDeleteIngredientOut,
    BulkFormulaIn,
    BulkFormulaOut,
    BulkIngredientIn,
    BulkIngredientOut,
    BulkUpdateFormulaIn,
    BulkUpdateIngredientIn,
    ErrorDetail,
    ErrorResponse,
    FilterParams,
    FormulaIn,
    FormulaOut,
    IncludeParams,
    IngredientIn,
    IngredientOut,
    InvoiceIn,
    InvoiceOut,
    PaginationParams,
    SearchParams,
    SearchResult,
)
from tende.utils import handle_database_error

# Configure the tracer
ddtrace.config.service = "tende-api"
ddtrace.config.env = os.getenv("ENVIRONMENT", "development")
ddtrace.config.version = "1.0.0"

# Configure JSON logging
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "service": "tende-api",
            "dd.trace_id": record.__dict__.get("dd.trace_id", ""),
            "dd.span_id": record.__dict__.get("dd.span_id", ""),
        }
        
        # Add extra fields if they exist
        if hasattr(record, "extra"):
            log_data.update(record.extra)
            
        # Add exception info if it exists
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "stack_trace": self.formatException(record.exc_info)
            }
            
        return json.dumps(log_data)

# Configure root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Remove any existing handlers
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Add stdout handler for JSON logging
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(JSONFormatter())
logger.addHandler(stdout_handler)

# Configure file handler for /var/log/tende-api.log
log_file = Path("/var/log/tende-api.log")
try:
    # Create log file if it doesn't exist
    log_file.touch(mode=0o644, exist_ok=True)
    
    # Add file handler
    file_handler = logging.FileHandler(str(log_file))
    file_handler.setFormatter(JSONFormatter())
    logger.addHandler(file_handler)
    
    logger.info("Logging to /var/log/tende-api.log configured successfully", extra={
        "operation": "logging",
        "component": "file_handler",
        "log_file": "/var/log/tende-api.log",
        "status": "success"
    })
except Exception as e:
    logger.error("Failed to configure logging to /var/log/tende-api.log", extra={
        "operation": "logging",
        "component": "file_handler",
        "log_file": "/var/log/tende-api.log",
        "status": "error",
        "error": str(e)
    })
    # Fallback to local file if /var/log is not accessible
    file_handler = logging.FileHandler('api.log')
    file_handler.setFormatter(JSONFormatter())
    logger.addHandler(file_handler)
    logger.warning("Falling back to local api.log file", extra={
        "operation": "logging",
        "component": "file_handler",
        "log_file": "api.log",
        "status": "fallback"
    })

# Load environment variables
DATADOG_API_KEY = os.getenv("DD_API_KEY")
DATADOG_SITE = os.getenv("DATADOG_SITE", "datadoghq.com")
SERVICE_NAME = os.getenv("SERVICE_NAME", "tende-api")
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

# Initialize Datadog
if DATADOG_API_KEY:
    initialize(
        api_key=DATADOG_API_KEY,
        host_name=SERVICE_NAME,
        site=DATADOG_SITE,
        statsd_host="dd-agent",
    )

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://j.maunsell@localhost:5432/tende")

# Create uploads directory if it doesn't exist
UPLOAD_DIR = Path("uploads/invoices")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Initialize FastAPI app with Datadog APM
app = FastAPI(
    title="Tende API",
    description="API for managing formulas and ingredients",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

# Add Datadog APM middleware
TraceMiddleware(app)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:5174"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Custom logging middleware
class StructuredLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        
        # Log the request with structured data
        dd_logger.info("HTTP request", extra={
            "operation": "http_request",
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "process_time": process_time,
            "client_ip": request.client.host if request.client else None,
            "user_agent": request.headers.get("user-agent"),
            "referer": request.headers.get("referer")
        })
        
        return response

# Add structured logging middleware
app.add_middleware(StructuredLoggingMiddleware)

# Mount static files
app.mount("/static", StaticFiles(directory="uploads"), name="static")

# Custom OpenAPI schema
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Tende API",
        version="1.0.0",
        description="API for managing formulas and ingredients",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# Add metrics middleware
@app.middleware("http")
async def add_metrics(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    
    # Record metrics
    statsd.increment("api.request.count", tags=[
        f"method:{request.method}",
        f"path:{request.url.path}",
        f"status:{response.status_code}"
    ])
    statsd.histogram("api.request.duration", process_time, tags=[
        f"method:{request.method}",
        f"path:{request.url.path}"
    ])
    
    return response

# Add test endpoint for metrics
@app.get("/api/v1/test/metrics")
async def test_metrics():
    """Test endpoint to verify Datadog metrics"""
    try:
        # Test different metric types
        statsd.increment("test.counter")
        statsd.gauge("test.gauge", 42)
        statsd.histogram("test.histogram", 1.23)
        
        return {"status": "success", "message": "Metrics sent to Datadog"}
    except Exception as e:
        dd_logger.error("Failed to test metrics", extra={
            "operation": "metrics",
            "component": "datadog",
            "status": "error",
            "error": str(e)
        })
        raise HTTPException(status_code=500, detail="Failed to test metrics")

@app.on_event("startup")
async def startup():
    app.state.db = await psycopg.AsyncConnection.connect(DATABASE_URL, autocommit=True)
    # Initialize repositories
    init_repositories(app.state.db, str(UPLOAD_DIR))
    dd_logger.info("Application startup complete", extra={
        "operation": "startup",
        "component": "application",
        "status": "success"
    })

@app.on_event("shutdown")
async def shutdown():
    await app.state.db.close()
    dd_logger.info("Application shutdown initiated", extra={
        "operation": "shutdown",
        "component": "application",
        "status": "in_progress"
    })

def generate_error_id() -> str:
    """Generate a unique error ID."""
    return str(uuid4())

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    error_id = generate_error_id()
    
    # Handle both string and dict detail types
    match exc.detail:
        case dict() as detail_dict:
            detail = detail_dict.get("message", "An error occurred")
            # Use provided error_id if it exists, otherwise use generated one
            error_id = detail_dict.get("error_id", error_id)
        case _:
            detail = str(exc.detail)

    dd_logger.error("HTTP Exception", extra={
        "operation": "error_handling",
        "error_type": "http_exception",
        "status_code": exc.status_code,
        "error": detail,
        "error_id": error_id,
        "path": request.url.path
    })
    
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            errors=[ErrorDetail(
                status=str(exc.status_code),
                title="Error",
                detail=detail,
                error_id=error_id
            )]
        ).dict()
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    error_id = generate_error_id()
    dd_logger.exception("Unexpected error", extra={
        "operation": "error_handling",
        "error_type": "unexpected_error",
        "error_id": error_id,
        "path": request.url.path
    })
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            errors=[ErrorDetail(
                status="500",
                title="Internal Server Error",
                detail="An unexpected error occurred",
                error_id=error_id
            )]
        ).dict()
    )

def handle_database_error(operation: str, context: dict, e: Exception) -> None:
    """Handle database errors consistently across the application."""
    error_id = generate_error_id()
    error_context = {
        "operation": operation,
        "error_type": "unexpected_error",
        "error_id": error_id
    }
    error_context.update(context)
    
    match e:
        case psycopg.OperationalError():
            error_context["error_type"] = "database_connection"
            dd_logger.exception("Database connection error", extra=error_context)
            raise HTTPException(status_code=503, detail="Database connection error")
            
        case psycopg.DataError():
            error_context["error_type"] = "data_format"
            dd_logger.exception("Invalid data format", extra=error_context)
            raise HTTPException(status_code=400, detail="Invalid data format")
            
        case psycopg.IntegrityError():
            error_context["error_type"] = "constraint_violation"
            dd_logger.exception("Database constraint violation", extra=error_context)
            raise HTTPException(status_code=409, detail="Database constraint violation")
            
        case psycopg.ProgrammingError():
            error_context["error_type"] = "programming_error"
            dd_logger.exception("Database programming error", extra=error_context)
            raise HTTPException(status_code=500, detail="Database programming error")
            
        case FileNotFoundError():
            error_context["error_type"] = "file_not_found"
            dd_logger.exception("File not found", extra=error_context)
            raise HTTPException(status_code=404, detail="File not found")
            
        case PermissionError():
            error_context["error_type"] = "permission_denied"
            dd_logger.exception("Permission denied", extra=error_context)
            raise HTTPException(status_code=403, detail="Permission denied")
            
        case OSError():
            error_context["error_type"] = "file_system_error"
            dd_logger.exception("File system error", extra=error_context)
            raise HTTPException(status_code=500, detail="File system error")
            
        case json.JSONDecodeError():
            error_context["error_type"] = "json_decode_error"
            dd_logger.exception("Invalid JSON format", extra=error_context)
            raise HTTPException(status_code=400, detail="Invalid JSON format")
            
        case _:
            # Only log unexpected errors, don't catch them
            dd_logger.exception("Unexpected error", extra=error_context)
            raise e  # Re-raise the original exception

# Initialize repositories
ingredient_repo = None
formula_repo = None
invoice_repo = None

def init_repositories(db, upload_dir: str):
    """Initialize repository instances with database connection."""
    global ingredient_repo, formula_repo, invoice_repo
    ingredient_repo = IngredientRepository(db)
    formula_repo = FormulaRepository(db)
    invoice_repo = InvoiceRepository(db, upload_dir)

@app.post("/api/v1/ingredients", response_model=IngredientOut)
@app.post("/api/v1/ingredients/", response_model=IngredientOut)
async def create_ingredient(ingredient: IngredientIn):
    """Create a new ingredient."""
    try:
        dd_logger.info("Creating new ingredient", extra={
            "ingredient_name": ingredient.data.attributes.name,
            "unit": ingredient.data.attributes.unit
        })
        
        # Create ingredient using repository
        created_ingredient = await ingredient_repo.create(Ingredient(
            id=uuid4(),
            name=ingredient.data.attributes.name,
            unit=ingredient.data.attributes.unit,
            cost_per_unit=ingredient.data.attributes.cost_per_unit,
            density=ingredient.data.attributes.density
        ))
        
        dd_logger.info("Successfully created ingredient", extra={
            "ingredient_id": str(created_ingredient.id),
            "ingredient_name": created_ingredient.name,
            "unit": created_ingredient.unit
        })
        
        # Format response according to IngredientOut model
        return {
            "data": {
                "id": str(created_ingredient.id),
                "type": "ingredient",
                "attributes": {
                    "name": created_ingredient.name,
                    "unit": created_ingredient.unit,
                    "cost_per_unit": created_ingredient.cost_per_unit,
                    "density": created_ingredient.density
                }
            }
        }
        
    except psycopg.OperationalError as e:
        dd_logger.exception("Database connection error", extra={
            "operation": "create_ingredient",
            "ingredient_name": ingredient.data.attributes.name,
            "error_type": "database_connection"
        })
        raise HTTPException(status_code=503, detail="Database connection error")
    except psycopg.DataError as e:
        dd_logger.exception("Invalid data format", extra={
            "operation": "create_ingredient",
            "ingredient_name": ingredient.data.attributes.name,
            "error_type": "data_format"
        })
        raise HTTPException(status_code=400, detail="Invalid data format")
    except psycopg.IntegrityError as e:
        dd_logger.exception("Database constraint violation", extra={
            "operation": "create_ingredient",
            "ingredient_name": ingredient.data.attributes.name,
            "error_type": "constraint_violation"
        })
        raise HTTPException(status_code=409, detail="Database constraint violation")
    except psycopg.ProgrammingError as e:
        dd_logger.exception("Database programming error", extra={
            "operation": "create_ingredient",
            "ingredient_name": ingredient.data.attributes.name,
            "error_type": "programming_error"
        })
        raise HTTPException(status_code=500, detail="Database programming error")

@app.get("/api/v1/ingredients", response_model=List[IngredientOut])
@app.get("/api/v1/ingredients/", response_model=List[IngredientOut])
async def get_ingredients(page: int = 1, per_page: int = 10):
    """Get a list of ingredients with pagination."""
    try:
        dd_logger.info("Getting ingredients", extra={
            "operation": "get_ingredients",
            "page": page,
            "per_page": per_page
        })
        
        # Get ingredients using repository
        ingredients, total_count = await ingredient_repo.list_all(page, per_page)
        
        dd_logger.info("Successfully retrieved ingredients", extra={
            "operation": "get_ingredients",
            "page": page,
            "per_page": per_page,
            "count": len(ingredients),
            "total_count": total_count
        })
        
        # Format response to match IngredientOut model
        return [{
            "data": {
                "id": str(ingredient.id),
                "type": "ingredient",
                "attributes": {
                    "name": ingredient.name,
                    "unit": ingredient.unit,
                    "cost_per_unit": ingredient.cost_per_unit,
                    "density": ingredient.density
                }
            }
        } for ingredient in ingredients]
        
    except psycopg.OperationalError as e:
        dd_logger.exception("Database connection error", extra={
            "operation": "get_ingredients",
            "page": page,
            "per_page": per_page,
            "error_type": "database_connection"
        })
        raise HTTPException(status_code=503, detail="Database connection error")
    except psycopg.DataError as e:
        dd_logger.exception("Invalid data format", extra={
            "operation": "get_ingredients",
            "page": page,
            "per_page": per_page,
            "error_type": "data_format"
        })
        raise HTTPException(status_code=400, detail="Invalid data format")
    except psycopg.ProgrammingError as e:
        dd_logger.exception("Database programming error", extra={
            "operation": "get_ingredients",
            "page": page,
            "per_page": per_page,
            "error_type": "programming_error"
        })
        raise HTTPException(status_code=500, detail="Database programming error")

@app.get("/api/v1/ingredients/{ingredient_id}", response_model=IngredientOut)
async def get_ingredient(ingredient_id: str):
    dd_logger.info("Fetching ingredient", extra={
        "operation": "get_ingredient",
        "ingredient_id": ingredient_id
    })
    try:
        async with app.state.db.cursor() as cur:
            await cur.execute(
                """
                SELECT id, name, unit, cost_per_unit, density 
                FROM ingredients 
                WHERE id = %s
                """,
                (ingredient_id,)
            )
            row = await cur.fetchone()
            
            if not row:
                dd_logger.warning("Ingredient not found", extra={
                    "operation": "get_ingredient",
                    "ingredient_id": ingredient_id,
                    "status": "not_found"
                })
                raise HTTPException(status_code=404, detail="Ingredient not found")

        dd_logger.info("Successfully fetched ingredient", extra={
            "operation": "get_ingredient",
            "ingredient_id": ingredient_id,
            "ingredient_name": row[1],
            "status": "success"
        })
        return {
            "data": {
                "id": str(row[0]),
                "type": "ingredient",
                "attributes": {
                    "name": row[1],
                    "unit": row[2],
                    "cost_per_unit": float(row[3]),
                    "density": row[4]
                }
            }
        }
    except psycopg.OperationalError as e:
        dd_logger.exception("Database connection error", extra={
            "operation": "get_ingredient",
            "ingredient_id": ingredient_id,
            "error_type": "database_connection"
        })
        raise HTTPException(status_code=503, detail="Database connection error")
    except psycopg.DataError as e:
        dd_logger.exception("Invalid data format", extra={
            "operation": "get_ingredient",
            "ingredient_id": ingredient_id,
            "error_type": "data_format"
        })
        raise HTTPException(status_code=400, detail="Invalid data format")
    except psycopg.ProgrammingError as e:
        dd_logger.exception("Database programming error", extra={
            "operation": "get_ingredient",
            "ingredient_id": ingredient_id,
            "error_type": "programming_error"
        })
        raise HTTPException(status_code=500, detail="Database programming error")

@app.patch("/api/v1/ingredients/{ingredient_id}", response_model=IngredientOut)
async def update_ingredient(ingredient_id: str, payload: IngredientIn):
    dd_logger.info("Updating ingredient", extra={
        "operation": "update_ingredient",
        "ingredient_id": ingredient_id,
        "ingredient_name": payload.data.attributes.name
    })
    try:
        async with app.state.db.cursor() as cur:
            # First check if the ingredient exists
            await cur.execute(
                "SELECT id FROM ingredients WHERE id = %s",
                (ingredient_id,)
            )
            if not await cur.fetchone():
                dd_logger.warning("Ingredient not found", extra={
                    "operation": "update_ingredient",
                    "ingredient_id": ingredient_id,
                    "status": "not_found"
                })
                raise HTTPException(status_code=404, detail="Ingredient not found")

            # Update the ingredient
            await cur.execute(
                """
                UPDATE ingredients 
                SET name = %s, unit = %s, cost_per_unit = %s, density = %s
                WHERE id = %s
                RETURNING id, name, unit, cost_per_unit, density
                """,
                (
                    payload.data.attributes.name,
                    payload.data.attributes.unit,
                    payload.data.attributes.cost_per_unit,
                    payload.data.attributes.density,
                    ingredient_id
                )
            )
            row = await cur.fetchone()

        dd_logger.info("Successfully updated ingredient", extra={
            "operation": "update_ingredient",
            "ingredient_id": ingredient_id,
            "ingredient_name": row[1],
            "status": "success"
        })
        return {
            "data": {
                "id": str(row[0]),
                "type": "ingredient",
                "attributes": {
                    "name": row[1],
                    "unit": row[2],
                    "cost_per_unit": float(row[3]),
                    "density": row[4]
                }
            }
        }
    except Exception as e:
        dd_logger.exception("Failed to update ingredient", extra={
            "operation": "update_ingredient",
            "ingredient_id": ingredient_id,
            "error_type": type(e).__name__
        })
        raise HTTPException(status_code=500, detail="Failed to update ingredient")

@app.delete("/api/v1/ingredients/{ingredient_id}", status_code=204)
async def delete_ingredient(ingredient_id: str):
    dd_logger.info("Deleting ingredient", extra={
        "operation": "delete_ingredient",
        "ingredient_id": ingredient_id
    })
    try:
        async with app.state.db.cursor() as cur:
            # First check if the ingredient exists
            await cur.execute(
                "SELECT id FROM ingredients WHERE id = %s",
                (ingredient_id,)
            )
            if not await cur.fetchone():
                dd_logger.warning("Ingredient not found", extra={
                    "operation": "delete_ingredient",
                    "ingredient_id": ingredient_id,
                    "status": "not_found"
                })
                raise HTTPException(status_code=404, detail="Ingredient not found")

            # Check if the ingredient is used in any formulas
            await cur.execute(
                """
                SELECT COUNT(*) 
                FROM formulas 
                WHERE ingredients ? %s
                """,
                (ingredient_id,)
            )
            count = (await cur.fetchone())[0]
            if count > 0:
                dd_logger.warning("Cannot delete ingredient: used in formulas", extra={
                    "operation": "delete_ingredient",
                    "ingredient_id": ingredient_id,
                    "formula_count": count,
                    "status": "conflict"
                })
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot delete ingredient: it is used in {count} formula(s)"
                )

            # Delete the ingredient
            await cur.execute(
                "DELETE FROM ingredients WHERE id = %s",
                (ingredient_id,)
            )

        dd_logger.info("Successfully deleted ingredient", extra={
            "operation": "delete_ingredient",
            "ingredient_id": ingredient_id,
            "status": "success"
        })
    except Exception as e:
        dd_logger.exception("Failed to delete ingredient", extra={
            "operation": "delete_ingredient",
            "ingredient_id": ingredient_id,
            "error_type": type(e).__name__
        })
        raise HTTPException(status_code=500, detail="Failed to delete ingredient")

@app.post("/api/v1/formulas", response_model=FormulaOut)
@app.post("/api/v1/formulas/", response_model=FormulaOut)
async def create_formula(payload: FormulaIn):
    dd_logger.info("Creating new formula", extra={
        "operation": "create_formula",
        "formula_name": payload.data.attributes.name,
        "formula_id": str(uuid4()),
        "ingredient_count": len(payload.data.relationships["ingredients"]["data"])
    })
    formula_id = str(uuid4())
    try:
        match payload.data.relationships:
            case {"ingredients": {"data": ingredients_data}}:
                ingredients_json = {
                    ingredient["id"]: ingredient["meta"]["percentage"]
                    for ingredient in ingredients_data
                }
            case _:
                dd_logger.error("Invalid formula data structure", extra={
                    "operation": "create_formula",
                    "formula_name": payload.data.attributes.name,
                    "error": "Missing or invalid ingredients data"
                })
                raise HTTPException(status_code=400, detail="Invalid formula data")
        
        dd_logger.info("Processed ingredients for formula", extra={
            "operation": "create_formula",
            "formula_id": formula_id,
            "ingredients": ingredients_json
        })
        
        async with app.state.db.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO formulas (id, name, description, ingredients, mass) 
                VALUES (%s, %s, %s, %s::jsonb, %s)
                """,
                (
                    formula_id,
                    payload.data.attributes.name,
                    payload.data.attributes.description,
                    json.dumps(ingredients_json),
                    payload.data.attributes.mass
                )
            )
            dd_logger.info("Successfully created formula", extra={
                "operation": "create_formula",
                "formula_id": formula_id,
                "formula_name": payload.data.attributes.name,
                "status": "success"
            })
            
            return {
                "data": {
                    "id": formula_id,
                    "type": "formula",
                    "attributes": payload.data.attributes,
                    "relationships": payload.data.relationships,
                }
            }
    except psycopg.OperationalError as e:
        dd_logger.exception("Database connection error", extra={
            "operation": "create_formula",
            "formula_name": payload.data.attributes.name,
            "error_type": "database_connection"
        })
        raise HTTPException(status_code=503, detail="Database connection error")
    except psycopg.DataError as e:
        dd_logger.exception("Invalid data format", extra={
            "operation": "create_formula",
            "formula_name": payload.data.attributes.name,
            "error_type": "data_format"
        })
        raise HTTPException(status_code=400, detail="Invalid data format")
    except psycopg.IntegrityError as e:
        dd_logger.exception("Database constraint violation", extra={
            "operation": "create_formula",
            "formula_name": payload.data.attributes.name,
            "error_type": "constraint_violation"
        })
        raise HTTPException(status_code=409, detail="Database constraint violation")
    except psycopg.ProgrammingError as e:
        dd_logger.exception("Database programming error", extra={
            "operation": "create_formula",
            "formula_name": payload.data.attributes.name,
            "error_type": "programming_error"
        })
        raise HTTPException(status_code=500, detail="Database programming error")
    except Exception as e:
        dd_logger.exception("Unexpected error", extra={
            "operation": "create_formula",
            "formula_name": payload.data.attributes.name,
            "error_type": "unexpected_error"
        })
        raise HTTPException(status_code=500, detail="An unexpected error occurred")

@app.get("/api/v1/formulas", response_model=List[FormulaOut])
@app.get("/api/v1/formulas/", response_model=List[FormulaOut])
async def get_formulas():
    dd_logger.info("Fetching all formulas", extra={
        "operation": "list_formulas"
    })
    try:
        async with app.state.db.cursor() as cur:
            await cur.execute(
                "SELECT id, name, description, ingredients, mass FROM formulas"
            )
            rows = await cur.fetchall()

        formulas = []
        for row in rows:
            # Convert ingredients JSON to relationships format
            relationships = {
                "ingredients": {
                    "data": []
                }
            }

            # Fetch related ingredients
            async with app.state.db.cursor() as cur:
                for ingredient_id, percentage in row[3].items():
                    await cur.execute(
                        "SELECT id, name, unit, cost_per_unit, density FROM ingredients WHERE id = %s",
                        (ingredient_id,)
                    )
                    ingredient_row = await cur.fetchone()
                    if ingredient_row:
                        relationships["ingredients"]["data"].append({
                            "type": "ingredient",
                            "id": str(ingredient_row[0]),
                            "meta": {"percentage": percentage}
                        })

            formulas.append({
                "data": {
                    "id": str(row[0]),
                    "type": "formula",
                    "attributes": {
                        "name": row[1],
                        "description": row[2],
                        "mass": row[4]
                    },
                    "relationships": relationships
                }
            })

        dd_logger.info("Successfully fetched formulas", extra={
            "operation": "list_formulas",
            "count": len(formulas),
            "status": "success"
        })
        return formulas
    except psycopg.OperationalError as e:
        dd_logger.exception("Database connection error", extra={
            "operation": "list_formulas",
            "error_type": "database_connection"
        })
        raise HTTPException(status_code=503, detail="Database connection error")
    except psycopg.DataError as e:
        dd_logger.exception("Invalid data format", extra={
            "operation": "list_formulas",
            "error_type": "data_format"
        })
        raise HTTPException(status_code=400, detail="Invalid data format")
    except psycopg.ProgrammingError as e:
        dd_logger.exception("Database programming error", extra={
            "operation": "list_formulas",
            "error_type": "programming_error"
        })
        raise HTTPException(status_code=500, detail="Database programming error")

@app.patch("/api/v1/formulas/{formula_id}", response_model=FormulaOut)
async def update_formula(formula_id: str, payload: FormulaIn):
    dd_logger.info("Updating formula", extra={
        "operation": "update_formula",
        "formula_id": formula_id,
        "formula_name": payload.data.attributes.name
    })
    try:
        async with app.state.db.cursor() as cur:
            # First check if the formula exists
            await cur.execute(
                "SELECT id FROM formulas WHERE id = %s",
                (formula_id,)
            )
            if not await cur.fetchone():
                dd_logger.warning("Formula not found", extra={
                    "operation": "update_formula",
                    "formula_id": formula_id,
                    "status": "not_found"
                })
                raise HTTPException(status_code=404, detail="Formula not found")

            # Convert the ingredients from relationships to a JSONB-compatible format
            ingredients_json = {
                ingredient["id"]: ingredient["meta"]["percentage"]
                for ingredient in payload.data.relationships["ingredients"]["data"]
            }
            
            # Update the formula
            await cur.execute(
                """
                UPDATE formulas 
                SET name = %s, description = %s, ingredients = %s::jsonb, mass = %s
                WHERE id = %s
                RETURNING id, name, description, ingredients, mass
                """,
                (
                    payload.data.attributes.name,
                    payload.data.attributes.description,
                    json.dumps(ingredients_json),
                    payload.data.attributes.mass,
                    formula_id
                )
            )
            row = await cur.fetchone()

        dd_logger.info("Successfully updated formula", extra={
            "operation": "update_formula",
            "formula_id": formula_id,
            "formula_name": row[1],
            "status": "success"
        })
        return {
            "data": {
                "id": str(row[0]),
                "type": "formula",
                "attributes": {
                    "name": row[1],
                    "description": row[2],
                    "mass": row[4]
                },
                "relationships": {
                    "ingredients": {
                        "data": [
                            {
                                "type": "ingredient",
                                "id": ingredient_id,
                                "meta": {"percentage": percentage}
                            }
                            for ingredient_id, percentage in row[3].items()]
                    }
                }
            }
        }
    except Exception as e:
        dd_logger.exception("Failed to update formula", extra={
            "operation": "update_formula",
            "formula_id": formula_id,
            "error_type": type(e).__name__
        })
        raise HTTPException(status_code=500, detail="Failed to update formula")

@app.delete("/api/v1/formulas/{formula_id}", status_code=204)
async def delete_formula(formula_id: str):
    dd_logger.info("Deleting formula", extra={
        "operation": "delete_formula",
        "formula_id": formula_id
    })
    try:
        async with app.state.db.cursor() as cur:
            # First check if the formula exists
            await cur.execute(
                "SELECT id FROM formulas WHERE id = %s",
                (formula_id,)
            )
            if not await cur.fetchone():
                dd_logger.warning("Formula not found", extra={
                    "operation": "delete_formula",
                    "formula_id": formula_id,
                    "status": "not_found"
                })
                raise HTTPException(status_code=404, detail="Formula not found")

            # Delete the formula
            await cur.execute(
                "DELETE FROM formulas WHERE id = %s",
                (formula_id,)
            )

        dd_logger.info("Successfully deleted formula", extra={
            "operation": "delete_formula",
            "formula_id": formula_id,
            "status": "success"
        })
    except Exception as e:
        dd_logger.exception("Failed to delete formula", extra={
            "operation": "delete_formula",
            "formula_id": formula_id,
            "error_type": type(e).__name__
        })
        raise HTTPException(status_code=500, detail="Failed to delete formula")

@app.get("/api/v1/formulas/by-ingredient/{ingredient_id}", response_model=list[FormulaOut])
async def get_formulas_by_ingredient(ingredient_id: str):
    async with app.state.db.cursor() as cur:
        # First check if the ingredient exists
        await cur.execute(
            "SELECT id FROM ingredients WHERE id = %s",
            (ingredient_id,)
        )
        if not await cur.fetchone():
            raise HTTPException(status_code=404, detail="Ingredient not found")

        # Find formulas containing the ingredient
        await cur.execute(
            """
            SELECT id, name, description, ingredients 
            FROM formulas 
            WHERE ingredients ? %s
            """,
            (ingredient_id,)
        )
        rows = await cur.fetchall()

    formulas = []
    for row in rows:
        # Convert ingredients JSON to relationships format
        relationships = {
            "ingredients": {
                "data": []
            }
        }

        # Fetch related ingredients
        async with app.state.db.cursor() as cur:
            for ingredient_id, percentage in row[3].items():
                await cur.execute(
                    "SELECT id, name, unit, cost_per_unit FROM ingredients WHERE id = %s",
                    (ingredient_id,)
                )
                ingredient_row = await cur.fetchone()
                if ingredient_row:
                    relationships["ingredients"]["data"].append({
                        "type": "ingredient",
                        "id": str(ingredient_row[0]),
                        "meta": {"percentage": percentage}
                    })

        formulas.append({
            "id": str(row[0]),
            "type": "formula",
            "attributes": {
                "name": row[1],
                "description": row[2]
            },
            "relationships": relationships
        })

    return formulas

@app.post("/api/v1/bulk/ingredients", response_model=BulkIngredientOut, status_code=201)
async def bulk_create_ingredients(payload: BulkIngredientIn):
    dd_logger.info("Creating bulk ingredients", extra={
        "operation": "bulk_create_ingredients",
        "count": len(payload.data)
    })
    try:
        async with app.state.db.cursor() as cur:
            # Prepare the bulk insert query
            values = []
            params = []
            for item in payload.data:
                ingredient_id = str(uuid4())
                values.append("(%s, %s, %s, %s, %s)")
                params.extend([
                    ingredient_id,
                    item.attributes.name,
                    item.attributes.unit,
                    item.attributes.cost_per_unit,
                    item.attributes.density
                ])

            query = """
                INSERT INTO ingredients (id, name, unit, cost_per_unit, density) 
                VALUES {}
                RETURNING id, name, unit, cost_per_unit, density
            """.format(", ".join(values))

            await cur.execute(query, params)
            rows = await cur.fetchall()

        dd_logger.info("Successfully created bulk ingredients", extra={
            "operation": "bulk_create_ingredients",
            "count": len(rows),
            "status": "success"
        })
        return {
            "data": [{
                "id": str(row[0]),
                "type": "ingredient",
                "attributes": {
                    "name": row[1],
                    "unit": row[2],
                    "cost_per_unit": float(row[3]),
                    "density": row[4]
                }
            } for row in rows],
            "meta": {
                "total_count": len(rows)
            }
        }
    except Exception as e:
        dd_logger.exception("Failed to create bulk ingredients", extra={
            "operation": "bulk_create_ingredients",
            "count": len(payload.data),
            "error_type": type(e).__name__
        })
        raise HTTPException(status_code=500, detail="Failed to create bulk ingredients")

@app.patch("/api/v1/bulk/ingredients", response_model=BulkIngredientOut)
async def bulk_update_ingredients(payload: BulkUpdateIngredientIn):
    dd_logger.info("Updating bulk ingredients", extra={
        "operation": "bulk_update_ingredients",
        "count": len(payload.data)
    })
    try:
        async with app.state.db.cursor() as cur:
            # First verify all ingredients exist
            ingredient_ids = [item.id for item in payload.data]
            await cur.execute(
                "SELECT id FROM ingredients WHERE id = ANY(%s)",
                (ingredient_ids,)
            )
            existing_ids = {row[0] for row in await cur.fetchall()}
            missing_ids = set(ingredient_ids) - existing_ids
            
            if missing_ids:
                dd_logger.warning("Some ingredients not found", extra={
                    "operation": "bulk_update_ingredients",
                    "missing_ids": list(missing_ids),
                    "status": "not_found"
                })
                raise HTTPException(
                    status_code=404,
                    detail=f"Ingredients not found: {', '.join(missing_ids)}"
                )

            # Update ingredients in bulk
            updated_ingredients = []
            for item in payload.data:
                await cur.execute(
                    """
                    UPDATE ingredients 
                    SET name = %s, unit = %s, cost_per_unit = %s, density = %s
                    WHERE id = %s
                    RETURNING id, name, unit, cost_per_unit, density
                    """,
                    (
                        item.attributes.name,
                        item.attributes.unit,
                        item.attributes.cost_per_unit,
                        item.attributes.density,
                        item.id
                    )
                )
                row = await cur.fetchone()
                updated_ingredients.append({
                    "id": str(row[0]),
                    "type": "ingredient",
                    "attributes": {
                        "name": row[1],
                        "unit": row[2],
                        "cost_per_unit": float(row[3]),
                        "density": row[4]
                    }
                })

        dd_logger.info("Successfully updated bulk ingredients", extra={
            "operation": "bulk_update_ingredients",
            "count": len(updated_ingredients),
            "status": "success"
        })
        return {
            "data": updated_ingredients,
            "meta": {
                "total_count": len(updated_ingredients)
            }
        }
    except Exception as e:
        dd_logger.exception("Failed to update bulk ingredients", extra={
            "operation": "bulk_update_ingredients",
            "count": len(payload.data),
            "error_type": type(e).__name__
        })
        raise HTTPException(status_code=500, detail="Failed to update bulk ingredients")

@app.post("/api/v1/bulk/formulas", response_model=BulkFormulaOut, status_code=201)
async def bulk_create_formulas(payload: BulkFormulaIn):
    dd_logger.info("Creating bulk formulas", extra={
        "operation": "bulk_create_formulas",
        "count": len(payload.data)
    })
    try:
        async with app.state.db.cursor() as cur:
            # Prepare the bulk insert query
            values = []
            params = []
            for item in payload.data:
                formula_id = str(uuid4())
                ingredients_json = {
                    ingredient["id"]: ingredient["meta"]["percentage"]
                    for ingredient in item.relationships["ingredients"]["data"]
                }
                values.append("(%s, %s, %s, %s::jsonb, %s)")
                params.extend([
                    formula_id,
                    item.attributes.name,
                    item.attributes.description,
                    json.dumps(ingredients_json),
                    item.attributes.mass
                ])

            query = """
                INSERT INTO formulas (id, name, description, ingredients, mass) 
                VALUES {}
                RETURNING id, name, description, ingredients, mass
            """.format(", ".join(values))

            await cur.execute(query, params)
            rows = await cur.fetchall()

        dd_logger.info("Successfully created bulk formulas", extra={
            "operation": "bulk_create_formulas",
            "count": len(rows),
            "status": "success"
        })
        return {
            "data": [{
                "id": str(row[0]),
                "type": "formula",
                "attributes": {
                    "name": row[1],
                    "description": row[2],
                    "mass": row[4]
                },
                "relationships": {
                    "ingredients": {
                        "data": [{
                            "type": "ingredient",
                            "id": ingredient_id,
                            "meta": {"percentage": percentage}
                        } for ingredient_id, percentage in row[3].items()]
                    }
                }
            } for row in rows],
            "meta": {
                "total_count": len(rows)
            }
        }
    except psycopg.OperationalError as e:
        dd_logger.exception("Database connection error", extra={
            "operation": "bulk_create_formulas",
            "count": len(payload.data),
            "error_type": "database_connection"
        })
        raise HTTPException(status_code=503, detail="Database connection error")
    except psycopg.DataError as e:
        dd_logger.exception("Invalid data format", extra={
            "operation": "bulk_create_formulas",
            "count": len(payload.data),
            "error_type": "data_format"
        })
        raise HTTPException(status_code=400, detail="Invalid data format")
    except psycopg.IntegrityError as e:
        dd_logger.exception("Database constraint violation", extra={
            "operation": "bulk_create_formulas",
            "count": len(payload.data),
            "error_type": "constraint_violation"
        })
        raise HTTPException(status_code=409, detail="Database constraint violation")
    except psycopg.ProgrammingError as e:
        dd_logger.exception("Database programming error", extra={
            "operation": "bulk_create_formulas",
            "count": len(payload.data),
            "error_type": "programming_error"
        })
        raise HTTPException(status_code=500, detail="Database programming error")
    except Exception as e:
        dd_logger.exception("Unexpected error", extra={
            "operation": "bulk_create_formulas",
            "count": len(payload.data),
            "error_type": "unexpected_error"
        })
        raise HTTPException(status_code=500, detail="An unexpected error occurred")

@app.patch("/api/v1/bulk/formulas", response_model=BulkFormulaOut)
async def bulk_update_formulas(payload: BulkUpdateFormulaIn):
    dd_logger.info("Updating bulk formulas", extra={
        "operation": "bulk_update_formulas",
        "count": len(payload.data)
    })
    try:
        async with app.state.db.cursor() as cur:
            # First verify all formulas exist
            formula_ids = [item.id for item in payload.data]
            await cur.execute(
                "SELECT id FROM formulas WHERE id = ANY(%s)",
                (formula_ids,)
            )
            existing_ids = {row[0] for row in await cur.fetchall()}
            missing_ids = set(formula_ids) - existing_ids
            
            if missing_ids:
                dd_logger.warning("Some formulas not found", extra={
                    "operation": "bulk_update_formulas",
                    "missing_ids": list(missing_ids),
                    "status": "not_found"
                })
                raise HTTPException(
                    status_code=404,
                    detail=f"Formulas not found: {', '.join(missing_ids)}"
                )

            # Update formulas in bulk
            updated_formulas = []
            for item in payload.data:
                ingredients_json = {
                    ingredient["id"]: ingredient["meta"]["percentage"]
                    for ingredient in item.relationships["ingredients"]["data"]
                }
                await cur.execute(
                    """
                    UPDATE formulas 
                    SET name = %s, description = %s, ingredients = %s::jsonb, mass = %s
                    WHERE id = %s
                    RETURNING id, name, description, ingredients, mass
                    """,
                    (
                        item.attributes.name,
                        item.attributes.description,
                        json.dumps(ingredients_json),
                        item.attributes.mass,
                        item.id
                    )
                )
                row = await cur.fetchone()
                updated_formulas.append({
                    "id": str(row[0]),
                    "type": "formula",
                    "attributes": {
                        "name": row[1],
                        "description": row[2],
                        "mass": row[4]
                    },
                    "relationships": {
                        "ingredients": {
                            "data": [{
                                "type": "ingredient",
                                "id": ingredient_id,
                                "meta": {"percentage": percentage}
                            } for ingredient_id, percentage in row[3].items()]
                        }
                    }
                })

        dd_logger.info("Successfully updated bulk formulas", extra={
            "operation": "bulk_update_formulas",
            "count": len(updated_formulas),
            "status": "success"
        })
        return {
            "data": updated_formulas,
            "meta": {
                "total_count": len(updated_formulas)
            }
        }
    except Exception as e:
        dd_logger.exception("Failed to update bulk formulas", extra={
            "operation": "bulk_update_formulas",
            "count": len(payload.data),
            "error_type": type(e).__name__
        })
        raise HTTPException(status_code=500, detail="Failed to update bulk formulas")

@app.get("/api/v1/search/ingredients", response_model=SearchResult)
async def search_ingredients(
    q: Optional[str] = None,
    search: Optional[str] = None,
    page: int = 1,
    size: int = 10,
    include: Optional[str] = None
):
    dd_logger.info("Searching ingredients", extra={
        "operation": "search_ingredients",
        "search_term": q or search,
        "page": page,
        "page_size": size,
        "include": include
    })
    try:
        # Use either q or search parameter, with q taking precedence
        search_term = q or search
        if not search_term:
            dd_logger.warning("No search term provided", extra={
                "operation": "search_ingredients",
                "status": "invalid_request"
            })
            raise HTTPException(status_code=400, detail="Search term is required. Use 'q' or 'search' parameter.")

        async with app.state.db.cursor() as cur:
            # Build the search query
            query = """
                SELECT id, name, unit, cost_per_unit, density
                FROM ingredients
                WHERE name ILIKE %s OR unit ILIKE %s
                ORDER BY name
                LIMIT %s OFFSET %s
            """
            
            search_pattern = f"%{search_term}%"
            offset = (page - 1) * size
            await cur.execute(query, (search_pattern, search_pattern, size, offset))
            rows = await cur.fetchall()

            # Get total count
            count_query = """
                SELECT COUNT(*)
                FROM ingredients
                WHERE name ILIKE %s OR unit ILIKE %s
            """
            await cur.execute(count_query, (search_pattern, search_pattern))
            total_count = (await cur.fetchone())[0]

        dd_logger.info("Successfully searched ingredients", extra={
            "operation": "search_ingredients",
            "search_term": search_term,
            "count": len(rows),
            "total_count": total_count,
            "page": page,
            "page_size": size,
            "status": "success"
        })
        return {
            "data": [{
                "id": str(row[0]),
                "type": "ingredient",
                "attributes": {
                    "name": row[1],
                    "unit": row[2],
                    "cost_per_unit": float(row[3]),
                    "density": row[4]
                }
            } for row in rows],
            "meta": {
                "total_count": total_count,
                "page_count": (total_count + size - 1) // size,
                "page_size": size,
                "current_page": page,
                "search_term": search_term
            }
        }
    except psycopg.OperationalError as e:
        dd_logger.exception("Database connection error", extra={
            "operation": "search_ingredients",
            "search_term": q or search,
            "error_type": "database_connection"
        })
        raise HTTPException(status_code=503, detail="Database connection error")
    except psycopg.DataError as e:
        dd_logger.exception("Invalid data format", extra={
            "operation": "search_ingredients",
            "search_term": q or search,
            "error_type": "data_format"
        })
        raise HTTPException(status_code=400, detail="Invalid data format")
    except psycopg.ProgrammingError as e:
        dd_logger.exception("Database programming error", extra={
            "operation": "search_ingredients",
            "search_term": q or search,
            "error_type": "programming_error"
        })
        raise HTTPException(status_code=500, detail="Database programming error")
    except Exception as e:
        dd_logger.exception("Unexpected error", extra={
            "operation": "search_ingredients",
            "search_term": q or search,
            "error_type": "unexpected_error"
        })
        raise HTTPException(status_code=500, detail="An unexpected error occurred")

def build_search_query(params: SearchParams, is_fuzzy: bool = False) -> tuple[str, list]:
    """Build the SQL query for searching formulas.
    
    Args:
        params: Search parameters
        is_fuzzy: Whether to use fuzzy search
        
    Returns:
        Tuple of (query string, parameters)
    """
    if is_fuzzy:
        query = """
            SELECT id, name, description, ingredients, mass,
                   GREATEST(
                       ts_rank_cd(search_vector, plainto_tsquery('english', %s)),
                       similarity(name, %s),
                       similarity(description, %s)
                   ) as rank
            FROM formulas
            WHERE 
                search_vector @@ plainto_tsquery('english', %s)
                OR similarity(name, %s) > %s
                OR similarity(description, %s) > %s
                OR levenshtein(lower(name), lower(%s)) <= %s
                OR levenshtein(lower(description), lower(%s)) <= %s
            ORDER BY rank DESC
            LIMIT %s OFFSET %s
        """
        query_params = [
            params.q, params.q, params.q,  # For ts_rank_cd
            params.q, params.q,  # For tsquery
            params.q, params.fuzzy.similarity_threshold,  # For name similarity
            params.q, params.fuzzy.similarity_threshold,  # For description similarity
            params.q, params.fuzzy.max_distance,  # For name levenshtein
            params.q, params.fuzzy.max_distance,  # For description levenshtein
            params.size, (params.page - 1) * params.size
        ]
    else:
        query = """
            SELECT id, name, description, ingredients, mass,
                   ts_rank_cd(search_vector, plainto_tsquery('english', %s)) as rank
            FROM formulas
            WHERE search_vector @@ plainto_tsquery('english', %s)
            ORDER BY rank DESC
            LIMIT %s OFFSET %s
        """
        query_params = [
            params.q, params.q,
            params.size, (params.page - 1) * params.size
        ]
    
    return query, query_params

def build_count_query(params: SearchParams, is_fuzzy: bool = False) -> tuple[str, list]:
    """Build the SQL query for counting search results.
    
    Args:
        params: Search parameters
        is_fuzzy: Whether to use fuzzy search
        
    Returns:
        Tuple of (query string, parameters)
    """
    if is_fuzzy:
        query = """
            SELECT COUNT(*)
            FROM formulas
            WHERE 
                search_vector @@ plainto_tsquery('english', %s)
                OR similarity(name, %s) > %s
                OR similarity(description, %s) > %s
                OR levenshtein(lower(name), lower(%s)) <= %s
                OR levenshtein(lower(description), lower(%s)) <= %s
        """
        params = [
            params.q, params.q,  # For tsquery
            params.q, params.fuzzy.similarity_threshold,  # For name similarity
            params.q, params.fuzzy.similarity_threshold,  # For description similarity
            params.q, params.fuzzy.max_distance,  # For name levenshtein
            params.q, params.fuzzy.max_distance   # For description levenshtein
        ]
    else:
        query = """
            SELECT COUNT(*)
            FROM formulas
            WHERE search_vector @@ plainto_tsquery('english', %s)
        """
        params = [params.q]
    
    return query, params

async def fetch_included_ingredients(cur, ingredient_ids: set) -> list:
    """Fetch included ingredients for search results.
    
    Args:
        cur: Database cursor
        ingredient_ids: Set of ingredient IDs to fetch
        
    Returns:
        List of included ingredients
    """
    if not ingredient_ids:
        return []
        
    await cur.execute(
        """
        SELECT id, name, unit, cost_per_unit, density
        FROM ingredients
        WHERE id = ANY(%s)
        """,
        (list(ingredient_ids),)
    )
    ingredient_rows = await cur.fetchall()
    
    return [{
        "id": str(row[0]),
        "type": "ingredient",
        "attributes": {
            "name": row[1],
            "unit": row[2],
            "cost_per_unit": float(row[3]),
            "density": row[4]
        }
    } for row in ingredient_rows]

@app.get("/api/v1/search/formulas", response_model=SearchResult)
async def search_formulas(params: SearchParams = Depends()):
    """Search for formulas with optional fuzzy matching.
    
    Args:
        params: Search parameters including query and pagination
        
    Returns:
        Search results with optional included ingredients
    """
    dd_logger.info("Searching formulas", extra={
        "operation": "search_formulas",
        "search_term": params.q,
        "page": params.page,
        "page_size": params.size,
        "fuzzy_search": params.fuzzy is not None
    })
    
    try:
        async with app.state.db.cursor() as cur:
            # Build and execute search query
            search_query, search_params = build_search_query(params, params.fuzzy is not None)
            await cur.execute(search_query, search_params)
            rows = await cur.fetchall()

            # Get total count
            count_query, count_params = build_count_query(params, params.fuzzy is not None)
            await cur.execute(count_query, count_params)
            total_count = (await cur.fetchone())[0]

            # Fetch included ingredients if requested
            included = []
            if params.include == "ingredients":
                ingredient_ids = set()
                for row in rows:
                    ingredient_ids.update(row[3].keys())
                included = await fetch_included_ingredients(cur, ingredient_ids)

        dd_logger.info("Successfully searched formulas", extra={
            "operation": "search_formulas",
            "search_term": params.q,
            "count": len(rows),
            "total_count": total_count,
            "page": params.page,
            "page_size": params.size,
            "status": "success"
        })
        return {
            "data": {
                "id": "list",
                "type": "invoice",
                "attributes": {
                    "items": [{
                        "id": str(row[0]),
                        "type": "invoice",
                        "attributes": {
                            "date": row[1],
                            "supplier": row[2],
                            "pdf_path": row[3]
                        },
                        "relationships": {
                            "ingredients": {
                                "data": row[5]
                            }
                        }
                    } for row in rows]
                }
            },
            "meta": {
                "total_count": total_count,
                "page_count": (total_count + params.size - 1) // params.size,
                "page_size": params.size,
                "current_page": params.page
            }
        }
    except Exception as e:
        dd_logger.exception("Failed to search formulas", extra={
            "operation": "search_formulas",
            "search_term": params.q,
            "error_type": type(e).__name__
        })
        raise HTTPException(status_code=500, detail="Failed to search formulas")

@app.get("/api/v1/test/logging")
async def test_logging():
    """Test endpoint to verify Datadog logging setup"""
    try:
        # Test different log levels
        dd_logger.debug("Test debug message", extra={
            "operation": "testing",
            "component": "logging",
            "message_type": "debug"
        })
        dd_logger.info("Test info message", extra={
            "operation": "test_logging",
            "test_type": "info"
        })
        dd_logger.warning("Test warning message", extra={
            "operation": "test_logging",
            "test_type": "warning"
        })
        dd_logger.error("Test error message", extra={
            "operation": "test_logging",
            "test_type": "error"
        })
        
        # Test with exception
        try:
            raise ValueError("Test exception")
        except Exception as e:
            dd_logger.error("Test error with exception", extra={
                "operation": "test_logging",
                "test_type": "exception",
                "error": str(e),
                "error_type": type(e).__name__
            }, exc_info=True)
        
        return {"status": "success", "message": "Logs sent to Datadog"}
    except psycopg.OperationalError as e:
        dd_logger.exception("Database connection error", extra={
            "operation": "test_logging",
            "error_type": "database_connection"
        })
        raise HTTPException(status_code=503, detail="Database connection error")
    except psycopg.ProgrammingError as e:
        dd_logger.exception("Database programming error", extra={
            "operation": "test_logging",
            "error_type": "programming_error"
        })
        raise HTTPException(status_code=500, detail="Database programming error")
    except Exception as e:
        dd_logger.exception("Unexpected error", extra={
            "operation": "test_logging",
            "error_type": "unexpected_error"
        })
        raise HTTPException(status_code=500, detail="An unexpected error occurred")

@app.get("/api/v1/health")
async def health_check():
    dd_logger.info("Performing health check", extra={
        "operation": "health_check",
        "check_type": "full"
    })
    try:
        # Check database connection
        async with app.state.db.cursor() as cur:
            await cur.execute("SELECT 1")
            await cur.fetchone()
            dd_logger.info("Database connection check successful", extra={
                "operation": "health_check",
                "component": "database",
                "status": "healthy"
            })
        
        # Check Datadog connection if configured
        if DATADOG_API_KEY:
            statsd.increment("health.check")
            dd_logger.info("Datadog connection check successful", extra={
                "operation": "health_check",
                "component": "datadog",
                "status": "healthy"
            })
        
        return {
            "status": "healthy",
            "database": "connected",
            "datadog": "configured" if DATADOG_API_KEY else "not_configured"
        }
    except psycopg.OperationalError as e:
        dd_logger.exception("Database connection error", extra={
            "operation": "health_check",
            "error_type": "database_connection",
            "status": "unhealthy"
        })
        raise HTTPException(status_code=503, detail="Database connection error")
    except psycopg.ProgrammingError as e:
        dd_logger.exception("Database programming error", extra={
            "operation": "health_check",
            "error_type": "programming_error",
            "status": "unhealthy"
        })
        raise HTTPException(status_code=500, detail="Database programming error")
    except Exception as e:
        dd_logger.exception("Unexpected error", extra={
            "operation": "health_check",
            "error_type": "unexpected_error",
            "status": "unhealthy"
        })
        raise HTTPException(status_code=503, detail="Service unhealthy")

@app.get("/api/v1/invoices", response_model=List[InvoiceOut])
async def get_invoices(
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
    current_user: User = Depends(get_current_user)
):
    """Get a list of invoices with pagination."""
    try:
        dd_logger.info("Getting invoices", extra={
            "operation": "get_invoices",
            "page": page,
            "page_size": size
        })
        
        # Get invoices using repository
        invoices, total_count = await invoice_repo.list_all(page, size)
        
        dd_logger.info("Successfully retrieved invoices", extra={
            "operation": "get_invoices",
            "page": page,
            "page_size": size,
            "count": len(invoices),
            "total_count": total_count
        })
        
        # Format response to match InvoiceOut model
        return [{
            "data": {
                "id": str(invoice.id),
                "type": "invoice",
                "attributes": {
                    "date": invoice.date.isoformat(),
                    "supplier": invoice.supplier,
                    "pdf_path": invoice.pdf_path
                },
                "relationships": {
                    "ingredients": {
                        "data": invoice.ingredients
                    }
                }
            }
        } for invoice in invoices]
        
    except psycopg.OperationalError as e:
        dd_logger.exception("Database connection error", extra={
            "operation": "get_invoices",
            "page": page,
            "page_size": size,
            "error_type": "database_connection"
        })
        raise HTTPException(status_code=503, detail="Database connection error")
    except psycopg.DataError as e:
        dd_logger.exception("Invalid data format", extra={
            "operation": "get_invoices",
            "page": page,
            "page_size": size,
            "error_type": "data_format"
        })
        raise HTTPException(status_code=400, detail="Invalid data format")
    except psycopg.ProgrammingError as e:
        dd_logger.exception("Database programming error", extra={
            "operation": "get_invoices",
            "page": page,
            "page_size": size,
            "error_type": "programming_error"
        })
        raise HTTPException(status_code=500, detail="Database programming error")

@app.post("/api/v1/invoices", response_model=InvoiceOut)
async def create_invoice(
    date: str = Form(...),
    supplier: str = Form(...),
    ingredients: str = Form(...),
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user)
):
    try:
        dd_logger.info("Creating new invoice", extra={
            "operation": "create_invoice",
            "supplier": supplier,
            "user_id": str(current_user.id)
        })
        
        # Parse ingredients JSON string
        try:
            ingredients_data = json.loads(ingredients)
        except json.JSONDecodeError:
            error_id = generate_error_id()
            dd_logger.exception("Invalid JSON format for ingredients", extra={
                "operation": "create_invoice",
                "supplier": supplier,
                "error_type": "json_decode_error",
                "error_id": error_id
            })
            raise HTTPException(
                status_code=400,
                detail={"message": "Invalid JSON format for ingredients", "error_id": error_id}
            )
        
        # Generate a unique filename for the PDF
        pdf_filename = f"{uuid4()}.pdf"
        pdf_path = f"invoices/{pdf_filename}"
        
        try:
            # Create Invoice object
            invoice = Invoice(
                id=uuid4(),
                date=datetime.fromisoformat(date),
                supplier=supplier,
                pdf_path=pdf_path,
                ingredients=ingredients_data
            )
        except ValueError:
            error_id = generate_error_id()
            dd_logger.exception("Invalid date format", extra={
                "operation": "create_invoice",
                "supplier": supplier,
                "error_type": "date_format_error",
                "error_id": error_id
            })
            raise HTTPException(
                status_code=400,
                detail={"message": "Invalid date format", "error_id": error_id}
            )
        
        # Read file contents
        file_contents = await file.read()
        
        # Create invoice in database
        created_invoice = await invoice_repo.create(invoice, file_contents)
        
        dd_logger.info("Successfully created invoice", extra={
            "operation": "create_invoice",
            "invoice_id": str(created_invoice.id),
            "supplier": created_invoice.supplier,
            "status": "success"
        })
        
        return created_invoice
        
    except Exception as e:
        error_id = generate_error_id()
        dd_logger.exception("Unexpected error while creating invoice", extra={
            "operation": "create_invoice",
            "supplier": supplier,
            "error_type": "unexpected_error",
            "error_id": error_id
        })
        raise HTTPException(
            status_code=500,
            detail={"message": "Unexpected error while creating invoice", "error_id": error_id}
        )

@app.delete("/api/v1/bulk/ingredients", response_model=BulkDeleteIngredientOut)
async def bulk_delete_ingredients(payload: BulkDeleteIngredientIn):
    print(f"Starting bulk delete operation for {len(payload.data)} ingredients")
    try:
        async with app.state.db.cursor() as cur:
            # Debug: Show all ingredients in the database
            await cur.execute("SELECT id::text, name FROM ingredients")
            all_ingredients = await cur.fetchall()
            print(f"All ingredients in database: {all_ingredients}")

            # First verify all ingredients exist
            ingredient_ids = [item.id for item in payload.data]
            placeholders = ','.join(['%s' for _ in ingredient_ids])
            
            # Debug: Log the exact query and parameters
            query = f"SELECT id::text, name FROM ingredients WHERE id::text IN ({placeholders})"
            print(f"Query: {query}")
            print(f"Parameters: {ingredient_ids}")
            
            await cur.execute(query, ingredient_ids)
            found_ingredients = await cur.fetchall()
            print(f"Found ingredients: {found_ingredients}")

            existing_ids = {row[0] for row in found_ingredients}
            missing_ids = set(ingredient_ids) - existing_ids

            if missing_ids:
                print(f"Missing IDs: {missing_ids}")
                print(f"Existing IDs: {existing_ids}")
                raise HTTPException(
                    status_code=404,
                    detail=f"Ingredients not found: {', '.join(missing_ids)}"
                )

            # If we get here, all ingredients exist, so delete them
            delete_query = f"DELETE FROM ingredients WHERE id::text IN ({placeholders})"
            await cur.execute(delete_query, ingredient_ids)

        return {
            "meta": {
                "deleted_count": len(ingredient_ids)
            }
        }
    except Exception as e:
        print(f"Error during bulk delete: {str(e)}")
        raise

