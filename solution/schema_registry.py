"""Dynamic schema and disposition registry.

Loads workflow schemas and disposition catalogues from the supplied files.
Does not hardcode field names or workflow definitions.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Dict, List, Optional, Any

# Type aliases for clarity
FieldSchema = Dict[str, Any]
WorkflowSchema = Dict[str, Any]
Disposition = Dict[str, Any]


class SchemaRegistry:
    """Registry for workflow schemas and disposition catalogues."""

    def __init__(self, schema_dir: Path, dispositions_path: Path):
        self._schema_dir = schema_dir
        self._dispositions_path = dispositions_path
        self._schemas: Dict[str, WorkflowSchema] = {}
        self._dispositions: Dict[str, List[Disposition]] = {}
        self._load_all()

    def _load_all(self) -> None:
        """Load all schemas and dispositions."""
        self._load_schemas()
        self._load_dispositions()

    def _load_schemas(self) -> None:
        """Load all JSON schema files from the schema directory."""
        if not self._schema_dir.exists():
            raise SchemaError(f"Schema directory not found: {self._schema_dir}")

        for schema_file in sorted(self._schema_dir.glob("*.json")):
            try:
                schema = self._load_schema_file(schema_file)
                workflow_id = schema.get("workflow_id")
                if not workflow_id:
                    raise SchemaError(f"Schema {schema_file.name} missing workflow_id")
                self._schemas[workflow_id] = schema
            except Exception as e:
                raise SchemaError(f"Failed to load schema {schema_file.name}: {e}") from e

    def _load_schema_file(self, path: Path) -> WorkflowSchema:
        """Load a single schema JSON file."""
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _load_dispositions(self) -> None:
        """Load disposition catalogue from CSV."""
        if not self._dispositions_path.exists():
            raise SchemaError(f"Dispositions file not found: {self._dispositions_path}")

        dispositions_by_workflow: Dict[str, List[Disposition]] = {}

        with open(self._dispositions_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                workflow = row.get("workflow", "")
                if not workflow:
                    continue

                disposition: Disposition = {
                    "code": row.get("code", "").strip(),
                    "workflow": workflow,
                    "terminal": row.get("terminal", "").strip().lower() == "true",
                    "success": row.get("success", "").strip().lower() == "true",
                    "display_name": row.get("display_name", "").strip(),
                }

                if disposition["code"] not in dispositions_by_workflow.setdefault(workflow, []):
                    dispositions_by_workflow[workflow].append(disposition)

        self._dispositions = dispositions_by_workflow

    def get_schema(self, workflow_id: str) -> WorkflowSchema:
        """Get schema for a workflow, raising if not found."""
        if workflow_id not in self._schemas:
            raise SchemaError(f"Unknown workflow: {workflow_id}")
        return self._schemas[workflow_id]

    def get_fields(self, workflow_id: str) -> List[FieldSchema]:
        """Get field definitions for a workflow in schema order."""
        schema = self.get_schema(workflow_id)
        return schema.get("fields", [])

    def get_field(self, workflow_id: str, field_name: str) -> Optional[FieldSchema]:
        """Get a specific field definition by name."""
        for field in self.get_fields(workflow_id):
            if field.get("name") == field_name:
                return field
        return None

    def get_dispositions(self, workflow_id: str) -> List[Disposition]:
        """Get dispositions for a workflow."""
        return self._dispositions.get(workflow_id, [])

    def get_disposition(self, workflow_id: str, code: str) -> Optional[Disposition]:
        """Get a specific disposition by code for a workflow."""
        for disp in self.get_dispositions(workflow_id):
            if disp["code"] == code:
                return disp
        return None

    def is_terminal(self, workflow_id: str, code: str) -> bool:
        """Check if a disposition code is terminal for a workflow."""
        disp = self.get_disposition(workflow_id, code)
        return disp["terminal"] if disp else False

    def is_success(self, workflow_id: str, code: str) -> bool:
        """Check if a disposition code is success for a workflow."""
        disp = self.get_disposition(workflow_id, code)
        return disp["success"] if disp else False

    def is_known_disposition(self, workflow_id: str, code: str) -> bool:
        """Check if a disposition code is known for a workflow."""
        return self.get_disposition(workflow_id, code) is not None

    def list_workflows(self) -> List[str]:
        """List all known workflow IDs."""
        return list(self._schemas.keys())


class SchemaError(Exception):
    """Raised when schema loading or validation fails."""
    pass
