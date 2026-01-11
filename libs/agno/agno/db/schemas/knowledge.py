from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, model_validator


class KnowledgeRow(BaseModel):
    """Knowledge Row that is stored in the database"""

    # id for this knowledge, auto-generated if not provided
    id: Optional[str] = None
    name: str
    description: str
    metadata: Optional[Dict[str, Any]] = None
    type: Optional[str] = None
    size: Optional[int] = None
    linked_to: Optional[str] = None
    access_count: Optional[int] = None
    status: Optional[str] = None
    status_message: Optional[str] = None
    created_at: Optional[int] = None
    updated_at: Optional[int] = None
    external_id: Optional[str] = None
    # Multi-ingestion path fields (all nullable for backwards compatibility)
    original_path: Optional[str] = None      # Absolute path - link mode only (INTERNAL)
    storage_path: Optional[str] = None       # Absolute path - copy mode only (INTERNAL)
    relative_path: Optional[str] = None      # Path from root (USER-VISIBLE)
    root_id: Optional[str] = None            # FK to workspace_roots
    root_path: Optional[str] = None          # Denormalized root path (INTERNAL)
    root_label: Optional[str] = None         # Human-readable label (USER-VISIBLE)
    source_type: Optional[str] = None        # local_file|local_folder|zip_extract|drag_drop|legacy
    link_status: Optional[str] = None        # ok|broken (USER-VISIBLE)
    link_checked_at: Optional[int] = None    # Unix timestamp
    upload_batch_id: Optional[str] = None    # Batch tracking

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def generate_id(self) -> "KnowledgeRow":
        if self.id is None:
            from uuid import uuid4

            self.id = str(uuid4())
        return self

    def to_dict(self) -> Dict[str, Any]:
        _dict = self.model_dump(exclude={"updated_at"})

        _dict["updated_at"] = datetime.fromtimestamp(self.updated_at).isoformat() if self.updated_at else None

        return _dict
