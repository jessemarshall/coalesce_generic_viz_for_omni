"""
Table column lookup from Omni models for COUNT(*) dependencies.
"""

import json
import logging
import re
from typing import List, Optional
import os

logger = logging.getLogger(__name__)


class TableColumnLookup:
    """Lookup table column definitions from Omni models."""

    def __init__(self, models_file_path: str = None):
        """Initialize with the path to models.json."""
        if models_file_path is None:
            # Default path relative to the script location
            models_file_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                'local_run_data', 'extracted_data', 'models.json'
            )

        self.models_file_path = models_file_path
        self._models_cache = None
        self._table_columns_cache = {}

    def _load_models(self):
        """Load models from JSON file."""
        if self._models_cache is not None:
            return self._models_cache

        try:
            with open(self.models_file_path, 'r') as f:
                self._models_cache = json.load(f)
            return self._models_cache
        except Exception as e:
            logger.warning(f"Failed to load models from {self.models_file_path}: {e}")
            return []

    def get_table_columns(self, table_name: str) -> List[str]:
        """
        Get all column names for a table.

        Args:
            table_name: Full table name (e.g., 'analytics_db.public.orders')

        Returns:
            List of fully qualified column names
        """
        # Check cache first
        if table_name in self._table_columns_cache:
            return self._table_columns_cache[table_name]

        columns = []

        # Load models
        models = self._load_models()

        # Extract just the table name from the full path
        table_base = table_name.split('.')[-1].upper() if '.' in table_name else table_name.upper()

        # Try to find a view that matches this table name
        view_name = None
        for model in models:
            yaml_def = model.get('yaml_definition', {})
            if isinstance(yaml_def, dict) and 'files' in yaml_def:
                files = yaml_def.get('files', {})

                # Check each view file
                for file_name, content in files.items():
                    if '.view' in file_name and isinstance(content, str):
                        # Check if this view matches our table
                        # Look for table_name: TABLE_NAME pattern
                        if f'table_name: {table_base}' in content or f'table_name: "{table_base}"' in content:
                            view_name = file_name
                            logger.debug(f"Found view '{view_name}' for table '{table_name}'")
                            break

                if view_name:
                    break

        # Search for the view definition in all models
        for model in models:
            if 'yaml_definition' in model and 'files' in model['yaml_definition']:
                files = model['yaml_definition']['files']

                if view_name in files:
                    view_def = files[view_name]
                    columns = self._extract_columns_from_view(view_def, table_name)
                    if columns:
                        # Cache the result
                        self._table_columns_cache[table_name] = columns
                        return columns

        # If not found, return empty list
        logger.warning(f"No column definitions found for table: {table_name}")
        return []

    def _extract_columns_from_view(self, view_def: str, table_name: str) -> List[str]:
        """Extract column names from a view definition."""
        columns = []

        # Parse the YAML-like view definition
        lines = view_def.split('\n')
        in_dimensions = False
        current_dimension = None

        for line in lines:
            line = line.strip()

            if line == 'dimensions:':
                in_dimensions = True
                continue
            elif line == 'measures:':
                in_dimensions = False
                continue

            if in_dimensions:
                # Look for dimension definitions
                if line and not line.startswith('#') and ':' in line:
                    if line.strip().startswith('sql:'):
                        # This is the SQL definition - extract the actual column name
                        # Format is like: sql: '"COLUMN_NAME"' or sql: '"_meta/ctime"'
                        sql_part = line.split(':', 1)[1].strip()
                        # Remove outer quotes (single quotes)
                        if sql_part.startswith("'") and sql_part.endswith("'"):
                            sql_part = sql_part[1:-1]
                        # Now extract the column name from within double quotes
                        if '"' in sql_part:
                            # Extract content between double quotes
                            match = re.search(r'"([^"]+)"', sql_part)
                            if match:
                                col_name = match.group(1)
                                if col_name and current_dimension:
                                    columns.append(f"{table_name}.{col_name}")
                                    current_dimension = None
                    elif not line.strip().startswith('sql:'):
                        # This is a dimension name - store it for when we see the sql line
                        current_dimension = line.split(':')[0].strip()

        return columns