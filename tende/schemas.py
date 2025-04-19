from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Union
from fastapi import HTTPException
from datetime import date, datetime
from pydantic import validator
import logging

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add a file handler
file_handler = logging.FileHandler('/var/log/tende-api/api.log')
file_handler.setLevel(logging.INFO)

# Add a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

class ErrorDetail(BaseModel):
    status: str
    title: str
    detail: str
    error_id: str
    code: Optional[str] = None
    source: Optional[Dict[str, Any]] = None

    @validator('status')
    def validate_status(cls, v):
        if not v.isdigit():
            raise ValueError("Status must be a numeric string")
        return v

    @validator('title', 'detail')
    def validate_strings(cls, v):
        if not isinstance(v, str):
            return str(v)
        return v


class ErrorResponse(BaseModel):
    errors: List[ErrorDetail]


class Links(BaseModel):
    self: str
    next: Optional[str] = None
    prev: Optional[str] = None
    first: Optional[str] = None
    last: Optional[str] = None


class Meta(BaseModel):
    total_count: Optional[int] = None
    page_count: Optional[int] = None
    page_size: Optional[int] = None
    current_page: Optional[int] = None


class IngredientAttributes(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    unit: str = Field(..., min_length=1, max_length=50)
    cost_per_unit: float = Field(..., ge=0)
    density: Optional[float] = Field(None, ge=0)

    @validator('name')
    def validate_name(cls, v):
        logger.info("Validating ingredient name", extra={
            "operation": "validation",
            "validation_type": "ingredient_name",
            "ingredient_name": v
        })
        if not v.strip():
            logger.error("Ingredient name cannot be empty", extra={
                "operation": "validation",
                "validation_type": "ingredient_name",
                "error": "empty_name",
                "ingredient_name": v
            })
            raise ValueError("Ingredient name cannot be empty")
        return v.strip()

    @validator('unit')
    def validate_unit(cls, v):
        logger.info("Validating ingredient unit", extra={
            "operation": "validation",
            "validation_type": "ingredient_unit",
            "unit": v
        })
        if not v.strip():
            logger.error("Unit cannot be empty", extra={
                "operation": "validation",
                "validation_type": "ingredient_unit",
                "error": "empty_unit",
                "unit": v
            })
            raise ValueError("Unit cannot be empty")
        return v.strip()


class IngredientData(BaseModel):
    type: str = Field("ingredient", const=True)
    attributes: IngredientAttributes


class IngredientIn(BaseModel):
    data: IngredientData


class IngredientOut(BaseModel):
    data: Dict[str, Any]
    included: Optional[List[Dict[str, Any]]] = None
    links: Optional[Links] = None
    meta: Optional[Meta] = None


class FormulaAttributes(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    mass: Optional[float] = Field(None, ge=0)

    @validator('name')
    def validate_name(cls, v):
        logger.info("Validating formula name", extra={
            "operation": "validation",
            "validation_type": "formula_name",
            "formula_name": v
        })
        if not v.strip():
            logger.error("Formula name cannot be empty", extra={
                "operation": "validation",
                "validation_type": "formula_name",
                "error": "empty_name",
                "formula_name": v
            })
            raise ValueError("Formula name cannot be empty")
        return v.strip()

    @validator('mass')
    def validate_mass(cls, v):
        logger.info("Validating formula mass", extra={
            "operation": "validation",
            "validation_type": "formula_mass",
            "mass": v
        })
        if v <= 0:
            logger.error("Mass must be greater than 0", extra={
                "operation": "validation",
                "validation_type": "formula_mass",
                "error": "invalid_mass",
                "mass": v
            })
            raise ValueError("Mass must be greater than 0")
        return v


class FormulaData(BaseModel):
    type: str = Field("formula", const=True)
    attributes: FormulaAttributes
    relationships: Dict[str, Any]


class FormulaIn(BaseModel):
    data: FormulaData


class FormulaOut(BaseModel):
    data: Dict[str, Any]
    included: Optional[List[Dict[str, Any]]] = None
    links: Optional[Links] = None
    meta: Optional[Meta] = None


class InvoiceAttributes(BaseModel):
    date: date
    supplier: str = Field(..., min_length=1, max_length=255)
    pdf_path: str = Field(..., min_length=1)

    @validator('supplier')
    def validate_supplier(cls, v):
        logger.info("Validating invoice supplier", extra={
            "operation": "validation",
            "validation_type": "invoice_supplier",
            "supplier": v
        })
        if not v.strip():
            logger.error("Supplier cannot be empty", extra={
                "operation": "validation",
                "validation_type": "invoice_supplier",
                "error": "empty_supplier",
                "supplier": v
            })
            raise ValueError("Supplier cannot be empty")
        return v.strip()

    @validator('pdf_path')
    def validate_pdf_path(cls, v):
        logger.info("Validating invoice PDF path", extra={
            "operation": "validation",
            "validation_type": "invoice_pdf_path",
            "pdf_path": v
        })
        if not v.strip():
            logger.error("PDF path cannot be empty", extra={
                "operation": "validation",
                "validation_type": "invoice_pdf_path",
                "error": "empty_pdf_path",
                "pdf_path": v
            })
            raise ValueError("PDF path cannot be empty")
        return v.strip()


class InvoiceData(BaseModel):
    type: str = Field("invoice", const=True)
    attributes: InvoiceAttributes
    relationships: Dict[str, Any]


class InvoiceIn(BaseModel):
    data: InvoiceData


class InvoiceOut(BaseModel):
    data: Dict[str, Any]
    included: Optional[List[Dict[str, Any]]] = None
    links: Optional[Links] = None
    meta: Optional[Meta] = None


class InvoiceListOut(BaseModel):
    data: Dict[str, Any]
    included: Optional[List[Dict[str, Any]]] = None
    links: Optional[Links] = None
    meta: Optional[Meta] = None


class UpdateInvoiceAttributes(BaseModel):
    date: Optional[str] = None
    supplier: Optional[str] = Field(None, min_length=1, max_length=255)
    relationships: Optional[Dict[str, Any]] = None

    @validator('date')
    def validate_date(cls, v):
        if v is None:
            return None
        try:
            date.fromisoformat(v)
            return v
        except ValueError:
            raise ValueError("Invalid date format. Use YYYY-MM-DD")

    @validator('supplier')
    def validate_supplier(cls, v):
        if v is None:
            return None
        logger.info("Validating invoice supplier", extra={
            "operation": "validation",
            "validation_type": "invoice_supplier",
            "supplier": v
        })
        return v.strip()


class UpdateInvoiceData(BaseModel):
    type: str = Field("invoice", const=True)
    attributes: UpdateInvoiceAttributes
    relationships: Optional[Dict[str, Any]] = None


class UpdateInvoiceIn(BaseModel):
    data: UpdateInvoiceData


class PaginationParams(BaseModel):
    page: int = Field(1, ge=1)
    size: int = Field(10, ge=1, le=100)


class IncludeParams(BaseModel):
    include: Optional[str] = None


class FilterParams(BaseModel):
    name: Optional[str] = None
    supplier: Optional[str] = None


class SearchParams(BaseModel):
    q: str
    page: int = Field(1, ge=1)
    size: int = Field(10, ge=1, le=100)
    include: Optional[str] = None


class SearchResult(BaseModel):
    data: List[Dict[str, Any]]
    meta: Dict[str, Any]


class BulkIngredientData(BaseModel):
    type: str = Field("ingredient", const=True)
    attributes: IngredientAttributes


class BulkIngredientIn(BaseModel):
    data: List[BulkIngredientData]


class BulkIngredientOut(BaseModel):
    data: List[Dict[str, Any]]
    meta: Dict[str, Any]


class BulkUpdateIngredientData(BaseModel):
    type: str = Field("ingredient", const=True)
    id: str
    attributes: IngredientAttributes


class BulkUpdateIngredientIn(BaseModel):
    data: List[BulkUpdateIngredientData]


class BulkFormulaData(BaseModel):
    type: str = Field("formula", const=True)
    attributes: FormulaAttributes
    relationships: Dict[str, Any]


class BulkFormulaIn(BaseModel):
    data: List[BulkFormulaData]


class BulkFormulaOut(BaseModel):
    data: List[Dict[str, Any]]
    meta: Dict[str, Any]


class BulkUpdateFormulaData(BaseModel):
    type: str = Field("formula", const=True)
    id: str
    attributes: FormulaAttributes
    relationships: Dict[str, Any]


class BulkUpdateFormulaIn(BaseModel):
    data: List[BulkUpdateFormulaData]


class SuggestionParams(BaseModel):
    q: str
    limit: int = Field(10, ge=1, le=100)
    min_similarity: float = Field(0.3, ge=0, le=1)


class SuggestionsResponse(BaseModel):
    data: List[Dict[str, Any]]
    meta: Dict[str, Any]


class IngredientBase(BaseModel):
    name: str
    unit: str
    supplier: Optional[str] = None
    cost_per_unit: Optional[float] = None
    notes: Optional[str] = None


class IngredientCreate(IngredientBase):
    pass


class IngredientUpdate(IngredientBase):
    name: Optional[str] = None
    unit: Optional[str] = None


class Ingredient(IngredientBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class FormulaBase(BaseModel):
    name: str
    mass: float
    ingredients: List[Dict[str, Any]]

    @validator('name')
    def validate_name(cls, v):
        logger.info("Validating formula name", extra={
            "operation": "validation",
            "validation_type": "formula_name",
            "formula_name": v
        })
        if not v.strip():
            logger.error("Formula name cannot be empty", extra={
                "operation": "validation",
                "validation_type": "formula_name",
                "error": "empty_name",
                "formula_name": v
            })
            raise ValueError("Formula name cannot be empty")
        return v.strip()

    @validator('mass')
    def validate_mass(cls, v):
        logger.info("Validating formula mass", extra={
            "operation": "validation",
            "validation_type": "formula_mass",
            "mass": v
        })
        if v <= 0:
            logger.error("Mass must be greater than 0", extra={
                "operation": "validation",
                "validation_type": "formula_mass",
                "error": "invalid_mass",
                "mass": v
            })
            raise ValueError("Mass must be greater than 0")
        return v

    @validator('ingredients')
    def validate_ingredients(cls, v):
        logger.info("Validating formula ingredients", extra={
            "operation": "validation",
            "validation_type": "formula_ingredients",
            "ingredient_count": len(v)
        })
        if not v:
            logger.error("Formula must have at least one ingredient", extra={
                "operation": "validation",
                "validation_type": "formula_ingredients",
                "error": "no_ingredients",
                "ingredient_count": 0
            })
            raise ValueError("Formula must have at least one ingredient")
        
        total_percentage = sum(ingredient.get('meta', {}).get('percentage', 0) for ingredient in v)
        if abs(total_percentage - 100) > 0.01:  # Allow for small floating point differences
            logger.error("Total percentage must equal 100", extra={
                "operation": "validation",
                "validation_type": "formula_ingredients",
                "error": "invalid_percentage_total",
                "total_percentage": total_percentage,
                "ingredient_count": len(v)
            })
            raise ValueError("Total percentage must equal 100")
        return v


class FormulaCreate(FormulaBase):
    pass


class FormulaUpdate(FormulaBase):
    name: Optional[str] = None
    mass: Optional[float] = None
    ingredients: Optional[List[Dict[str, Any]]] = None


class Formula(FormulaBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class InvoiceBase(BaseModel):
    supplier: str
    invoice_number: str
    date: datetime
    total_amount: float
    file_name: str

    @validator('supplier')
    def validate_supplier(cls, v):
        logger.info("Validating invoice supplier", extra={
            "operation": "validation",
            "validation_type": "invoice_supplier",
            "supplier": v
        })
        if not v.strip():
            logger.error("Supplier cannot be empty", extra={
                "operation": "validation",
                "validation_type": "invoice_supplier",
                "error": "empty_supplier",
                "supplier": v
            })
            raise ValueError("Supplier cannot be empty")
        return v.strip()

    @validator('invoice_number')
    def validate_invoice_number(cls, v):
        logger.info("Validating invoice number", extra={
            "operation": "validation",
            "validation_type": "invoice_number",
            "invoice_number": v
        })
        if not v.strip():
            logger.error("Invoice number cannot be empty", extra={
                "operation": "validation",
                "validation_type": "invoice_number",
                "error": "empty_invoice_number",
                "invoice_number": v
            })
            raise ValueError("Invoice number cannot be empty")
        return v.strip()

    @validator('total_amount')
    def validate_total_amount(cls, v):
        logger.info("Validating invoice total amount", extra={
            "operation": "validation",
            "validation_type": "invoice_total_amount",
            "total_amount": v
        })
        if v <= 0:
            logger.error("Total amount must be greater than 0", extra={
                "operation": "validation",
                "validation_type": "invoice_total_amount",
                "error": "invalid_amount",
                "total_amount": v
            })
            raise ValueError("Total amount must be greater than 0")
        return v

    @validator('file_name')
    def validate_file_name(cls, v):
        logger.info("Validating invoice file name", extra={
            "operation": "validation",
            "validation_type": "invoice_file_name",
            "file_name": v
        })
        if not v.strip():
            logger.error("File name cannot be empty", extra={
                "operation": "validation",
                "validation_type": "invoice_file_name",
                "error": "empty_file_name",
                "file_name": v
            })
            raise ValueError("File name cannot be empty")
        return v.strip()


class InvoiceCreate(InvoiceBase):
    pass


class InvoiceUpdate(InvoiceBase):
    supplier: Optional[str] = None
    invoice_number: Optional[str] = None
    date: Optional[datetime] = None
    total_amount: Optional[float] = None
    file_name: Optional[str] = None


class Invoice(InvoiceBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class BulkDeleteIngredientData(BaseModel):
    type: str = Field("ingredient", const=True)
    id: str


class BulkDeleteIngredientIn(BaseModel):
    data: List[BulkDeleteIngredientData]


class BulkDeleteIngredientOut(BaseModel):
    meta: Dict[str, Any]
