from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime
import uuid

class ColumnMeta(BaseModel):
    name: str
    dtype: str
    missing_count: int
    unique_count: Optional[int] = None
    example_values: Optional[List[Any]] = None


class Issue(BaseModel):
    column: str
    issue_type: str  # e.g. "missing_values", "outlier", "invalid_format"
    description: str
    severity: str  # e.g. "low", "medium", "high"


class Transformation(BaseModel):
    name: str  # e.g. "fillna", "drop_duplicates"
    parameters: Dict[str, Any]
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ProvenanceEvent(BaseModel):
    actor: str  # which agent or user performed it
    action: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    details: Optional[Dict[str, Any]] = None


class DatasetMetadata(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    filename: str
    cloudinary_url: str
    uploaded_at: datetime = Field(default_factory=datetime.utcnow)
    size: int

    # --- Extended Data Metadata ---
    preview: List[Dict[str, Any]]  # first few rows from CSV as list of dicts
    schema: Dict[str, ColumnMeta]  # inferred column types and stats
    issues: List[Issue] = []
    transformations: List[Transformation] = []
    provenance: List[ProvenanceEvent] = []

    # --- Common metadata ---
    row_count: int
    status: str = "raw"  # raw / cleaned / validated
    notes: Optional[str] = None
