"""
Omni to BI Importer Transformer Module

Converts Omni extraction data to Coalesce BI Importer CSV format.
Creates CSV files that can be uploaded to Coalesce Catalog via the BI Importer.

File outputs (per Coalesce documentation):
- dashboards.csv (required)
- dashboard_queries.csv (optional)
- dashboard_fields.csv (optional, only for VIZ_MODEL types)
"""

import csv
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class OmniToBIImporter:
    """Convert Omni data to Coalesce BI Importer format"""

    def __init__(self, extracted_data_dir: str, output_dir: str, base_url: str = ""):
        """
        Initialize converter

        Args:
            extracted_data_dir: Directory containing extracted Omni data
            output_dir: Directory for output CSV files
            base_url: Omni instance base URL (e.g., https://your-company.omniapp.co)
        """
        self.extracted_data_dir = Path(extracted_data_dir)
        self.output_dir = Path(output_dir)
        self.base_url = base_url
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # REQUIRED: Always use Unix timestamp for file naming per BI Importer docs
        self.timestamp = int(datetime.now().timestamp())
        self.file_prefix = f"{self.timestamp}_"

        # Store view SQL definitions and topics for query expansion
        self.view_definitions = {}  # Global view definitions (fallback when model-specific not available)
        self.view_definitions_by_model = {}  # Store view definitions per model ID (preferred)
        self.view_full_content = {}  # Global full view content (fallback)
        self.view_full_content_by_model = {}  # Store full view content per model ID (preferred)
        self.field_metadata = {}  # Store field types and formats from Omni models
        self.topics = {}
        self.relationships = {}
        self.connections = []

    def extract_view_definitions(self, models: List[Dict]) -> Dict[str, str]:
        """
        Extract view SQL definitions from model YAML files

        Args:
            models: List of model dictionaries

        Returns:
            Dictionary mapping view names to their SQL definitions
        """
        view_defs = {}

        for model in models:
            model_id = model.get('id', '')
            yaml_def = model.get('yaml_definition', {})

            # Initialize storage for this model if needed
            if model_id:
                if model_id not in self.view_definitions_by_model:
                    self.view_definitions_by_model[model_id] = {}
                if model_id not in self.view_full_content_by_model:
                    self.view_full_content_by_model[model_id] = {}

            # Process views from both 'files' and 'viewNames' sections
            sources_to_check = []

            # Check 'files' section
            if isinstance(yaml_def, dict) and 'files' in yaml_def:
                files = yaml_def.get('files', {})
                sources_to_check.extend([(file_name, content) for file_name, content in files.items()])

            # Note: viewNames contains view name mappings, not actual content
            # The actual content is in the 'files' section

            for file_name, content in sources_to_check:
                if '.view' in file_name and isinstance(content, str):
                    # Parse view name from filename
                    original_name = file_name
                    view_name = file_name.replace('.view', '').replace('.query', '')
                    view_name = view_name.split('/')[-1]  # Get last part if there's a path

                    # Store the full content for measure/dimension lookup (both global and per-model)
                    self.view_full_content[view_name] = content
                    if model_id:
                        self.view_full_content_by_model[model_id][view_name] = content

                    # Extract field metadata (dimensions and measures with their types/formats)
                    self._extract_field_metadata(view_name, content)

                    # Extract SQL from content
                    if 'sql:' in content:
                        # Find the SQL block
                        sql_match = re.search(r'sql:\s*\|-?\s*(.*?)(?:^\w|\Z)', content,
                                              re.DOTALL | re.MULTILINE)
                        if sql_match:
                            sql = sql_match.group(1).strip()
                            view_defs[view_name] = sql
                            if model_id:
                                self.view_definitions_by_model[model_id][view_name] = sql
                            logger.debug(f"Extracted SQL for view: {view_name} in model: {model_id[:8]}...")

        logger.info(f"Extracted SQL definitions for {len(view_defs)} views globally")
        logger.info(f"Extracted views for {len(self.view_definitions_by_model)} models")
        logger.info(f"Extracted view content for {len(self.view_full_content)} views globally")
        return view_defs

    def _extract_field_metadata(self, view_name: str, content: str):
        """
        Extract field metadata from view definition including types and formats.

        Args:
            view_name: Name of the view
            content: YAML-like content of the view definition
        """
        lines = content.split('\n')
        current_field = None
        current_section = None
        indent_level = None

        for line in lines:
            stripped = line.strip()

            # Skip empty lines and comments
            if not stripped or stripped.startswith('#'):
                continue

            # Calculate indentation level
            current_indent = len(line) - len(line.lstrip())

            # Track which section we're in
            if stripped == 'dimensions:':
                current_section = 'dimension'
                indent_level = current_indent
                current_field = None
                continue
            elif stripped == 'measures:':
                current_section = 'measure'
                indent_level = current_indent
                current_field = None
                continue

            # Check if this is a new field definition
            # It should be indented more than the section header but not be a property
            if current_section and ':' in stripped:
                # If we have an indent_level, check if this is a field definition
                if indent_level is not None:
                    # Field definitions are typically 2 spaces more indented than section header
                    # Properties are typically 4+ spaces more indented
                    if current_indent == indent_level + 2 and not any(stripped.startswith(prop + ':')
                                                                        for prop in ['sql', 'format', 'type', 'aggregate_type', 'description', 'primary_key', 'hidden']):
                        # New field definition
                        current_field = stripped.split(':')[0].strip()
                        if current_field:
                            # Initialize field metadata
                            field_key = f"{view_name}.{current_field}"
                            if field_key not in self.field_metadata:
                                self.field_metadata[field_key] = {
                                    'type': current_section,  # dimension or measure
                                    # Don't set data_type here - only set it when we have explicit type info
                                }

            # Handle field properties (more indented than field definition)
            if current_field and current_section:
                field_key = f"{view_name}.{current_field}"

                if stripped.startswith('format:'):
                    # Store format information but don't infer data type from it
                    format_value = stripped.split(':', 1)[1].strip()
                    if field_key in self.field_metadata:
                        self.field_metadata[field_key]['format'] = format_value

                elif stripped.startswith('type:'):
                    # Store type information but don't use it for data type inference
                    # Only dashboard.json omniType should be used
                    type_value = stripped.split(':', 1)[1].strip()
                    if field_key in self.field_metadata:
                        self.field_metadata[field_key]['model_type'] = type_value

                elif stripped.startswith('aggregate_type:'):
                    # Store aggregate_type for reference but don't infer data type from it
                    agg_value = stripped.split(':', 1)[1].strip()
                    if field_key in self.field_metadata:
                        self.field_metadata[field_key]['aggregate_type'] = agg_value

    def extract_topics_and_relationships(self, models: List[Dict]) -> tuple[Dict[str, Dict], Dict[str, List]]:
        """
        Extract topics and relationships from model YAML files

        Args:
            models: List of model dictionaries

        Returns:
            Tuple of (topics dict, relationships dict)
        """
        topics = {}
        relationships = {}

        for model in models:
            yaml_def = model.get('yaml_definition', {})
            if isinstance(yaml_def, dict) and 'files' in yaml_def:
                files = yaml_def.get('files', {})

                # Extract topics
                for file_name, content in files.items():
                    if '.topic' in file_name and isinstance(content, str):
                        topic_name = file_name.replace('.topic', '').split('/')[-1]

                        # Parse topic content
                        topic_data = {}
                        if 'base_view:' in content:
                            match = re.search(r'base_view:\s*(\S+)', content)
                            if match:
                                topic_data['base_view'] = match.group(1)

                        if 'label:' in content:
                            match = re.search(r'label:\s*(.+)', content)
                            if match:
                                topic_data['label'] = match.group(1).strip()

                        if 'joins:' in content:
                            # Extract joins
                            joins_match = re.search(r'joins:\s*\{([^}]*)\}', content, re.DOTALL)
                            if joins_match:
                                joins_text = joins_match.group(1)
                                joins = re.findall(r'(\w+):\s*\{[^}]*\}', joins_text)
                                topic_data['joins'] = joins

                        topics[topic_name] = topic_data
                        logger.debug(f"Extracted topic: {topic_name}")

                # Extract relationships
                if 'relationships' in files:
                    rel_content = files['relationships']
                    if isinstance(rel_content, str):
                        # Parse relationships
                        rel_list = []

                        # Find all relationship definitions
                        rel_pattern = r'- join_from_view:\s*(\S+)\s+join_to_view:\s*(\S+)\s+join_type:\s*(\S+)\s+on_sql:\s*(.+?)(?:relationship_type|$)'
                        matches = re.findall(rel_pattern, rel_content, re.DOTALL)

                        for match in matches:
                            rel_list.append({
                                'from': match[0],
                                'to': match[1],
                                'type': match[2],
                                'sql': match[3].strip()
                            })

                        if rel_list:
                            model_name = model.get('name', model.get('id'))
                            relationships[model_name] = rel_list

        logger.info(f"Extracted {len(topics)} topics and relationships for {len(relationships)} models")
        return topics, relationships

    def load_extracted_data(self) -> Dict[str, List[Dict]]:
        """Load extracted Omni data from JSON files"""
        data = {}

        # Load dashboards
        dashboards_file = self.extracted_data_dir / "dashboards.json"
        if dashboards_file.exists():
            with open(dashboards_file) as f:
                data['dashboards'] = json.load(f)
                logger.info(f"Loaded {len(data['dashboards'])} dashboards")
        else:
            data['dashboards'] = []

        # Load models
        models_file = self.extracted_data_dir / "models.json"
        if models_file.exists():
            with open(models_file) as f:
                data['models'] = json.load(f)
                logger.info(f"Loaded {len(data['models'])} models")
        else:
            data['models'] = []

        # Load connections
        connections_file = self.extracted_data_dir / "connections.json"
        if connections_file.exists():
            with open(connections_file) as f:
                data['connections'] = json.load(f)
                logger.info(f"Loaded {len(data['connections'])} connections")

                # Create connection mapping for easy lookup
                self.connection_map = {}
                for conn in data['connections']:
                    # Use connection name as fallback if no database specified
                    default_db = conn.get('name', 'default_database')
                    self.connection_map[conn['id']] = {
                        'database': conn.get('database', default_db),
                        'dialect': conn.get('dialect', 'snowflake'),
                        'schema': conn.get('defaultSchema'),
                        'name': conn.get('name', 'Unknown')
                    }
        else:
            data['connections'] = []
            self.connection_map = {}

        return data

    def format_array_field(self, items: List) -> str:
        """
        Format a list as a string array for CSV
        Example: ['item1', 'item2'] -> "['item1','item2']"
        """
        if not items:
            return ""

        # Ensure items are strings and properly quoted
        formatted_items = [f"'{str(item)}'" for item in items]
        return f"[{','.join(formatted_items)}]"

    def extract_parent_tables(self, dashboard: Dict) -> List[str]:
        """
        Extract parent tables from dashboard queries, properly handling CTEs
        Returns list of unique table references in database.schema.table format
        """
        parent_tables = set()
        cte_names = set()  # Track CTE names to exclude them from parent tables

        # Get connection info for database prefix
        connection_id = dashboard.get('connectionId', '')
        database_name = ''
        if connection_id and hasattr(self, 'connection_map') and connection_id in self.connection_map:
            database_name = self.connection_map[connection_id].get('database', '')

        # Extract tables from queries
        for query in dashboard.get('queries', []):
            if isinstance(query, dict) and 'query' in query:
                query_data = query['query']

                # Check for userEditedSQL which might contain CTEs
                user_sql = query_data.get('userEditedSQL', '')
                if user_sql and 'WITH' in user_sql.upper():
                    # Parse CTE definitions to find actual source tables
                    try:
                        # Extract CTE names first - including nested CTEs
                        # More comprehensive pattern to catch all CTE declarations
                        # This catches WITH x AS, , x AS, and nested CTEs at line starts
                        cte_pattern = r'(?:^|\s|,)\s*(\w+)\s+(?:AS|as)\s*\('
                        cte_matches = re.findall(cte_pattern, user_sql, re.IGNORECASE | re.MULTILINE)
                        cte_names.update(cte_matches)

                        # Also catch quoted CTE names
                        quoted_cte_pattern = r'(?:^|\s|,)\s*["\']([^"\']+)["\']\s+(?:AS|as)\s*\('
                        quoted_matches = re.findall(quoted_cte_pattern, user_sql, re.IGNORECASE | re.MULTILINE)
                        cte_names.update(quoted_matches)

                        # Now extract actual tables from the CTE definitions and main query
                        # Pattern to match table references (FROM and JOIN clauses)
                        table_pattern = r'(?:FROM|JOIN)\s+([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*){0,3})'
                        table_matches = re.findall(table_pattern, user_sql, re.IGNORECASE)

                        for table_ref in table_matches:
                            # Skip if it's a CTE name
                            table_base = table_ref.split('.')[-1] if '.' in table_ref else table_ref
                            if table_base.lower() not in [cte.lower() for cte in cte_names]:
                                # Only add if it has proper database.schema.table format
                                # or can be formatted to have it
                                if '.' in table_ref and table_ref.count('.') >= 2:
                                    # Already has database.schema.table format
                                    parent_tables.add(table_ref)
                                # Skip tables without proper format - they're likely CTEs we missed

                    except Exception as e:
                        logger.warning(f"Failed to parse CTEs in SQL: {e}")

                # Get direct table reference (but skip if it's a CTE)
                table_ref = query_data.get('table', '')
                if table_ref and table_ref.lower() not in [cte.lower() for cte in cte_names]:
                    # Check if this is a view that we know maps to real tables
                    if table_ref in self.view_definitions:
                        # Parse the view definition to get source tables
                        view_sql = self.view_definitions[table_ref]
                        # More specific pattern to catch actual table references
                        table_pattern = r'(?:FROM|JOIN)\s+([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*){0,3})'
                        table_matches = re.findall(table_pattern, view_sql, re.IGNORECASE)

                        # Look for CTEs in the view definition too
                        view_cte_pattern = r'(?:^|\s|,)\s*(\w+)\s+(?:AS|as)\s*\('
                        view_cte_matches = re.findall(view_cte_pattern, view_sql, re.IGNORECASE | re.MULTILINE)
                        view_cte_names = set(view_cte_matches)

                        for match in table_matches:
                            table_base = match.split('.')[-1] if '.' in match else match
                            # Skip if it's a CTE in the view definition
                            if table_base.lower() not in [cte.lower() for cte in view_cte_names]:
                                if '.' in match and match.count('.') >= 2:
                                    # Has database.schema.table format
                                    parent_tables.add(match)
                    # Don't add table references without schema - they're likely CTEs

                # Check fields for table references (format: table.column)
                # But skip this since field references usually refer to CTEs or aliases,
                # not actual database tables

        # Remove empty strings and CTE names, return sorted list
        # Only keep tables with proper database.schema.table format
        filtered_tables = []
        for table in parent_tables:
            if table:
                # Final check: skip if it's a CTE name
                table_base = table.split('.')[-1] if '.' in table else table
                if table_base.lower() not in [cte.lower() for cte in cte_names]:
                    # Only add if it has at least database.schema.table format (2 dots)
                    # This helps filter out any remaining CTEs or incomplete references
                    if '.' in table and table.count('.') >= 2:
                        filtered_tables.append(table)

        return sorted(filtered_tables)

    def extract_parent_columns_from_sql(self, sql_text: str, connection_id: str = None, field_name: str = None) -> List[str]:
        """
        Extract parent columns from SQL, handling CTEs properly.
        Returns list of columns in DATABASE.SCHEMA.TABLE.COLUMN format.

        Args:
            sql_text: SQL query text
            connection_id: Connection ID for database context
            field_name: Optional field name for field-specific lineage

        Returns:
            List of parent columns in proper format
        """
        if not sql_text:
            return []

        # Check if SQL contains CTEs (case-insensitive check)
        sql_upper = sql_text.upper()
        if 'WITH' in sql_upper and ' AS (' in sql_upper:
            try:
                # Determine dialect from connection if available
                dialect = 'snowflake'  # default
                if connection_id and hasattr(self, 'connection_map') and connection_id in self.connection_map:
                    # Try to infer dialect from connection type if available
                    connection_info = self.connection_map[connection_id]
                    # For now, assume snowflake as default
                    dialect = connection_info.get('dialect', 'snowflake')

                # Use field-specific parser if field_name is provided
                if field_name:
                    try:
                        from omni_to_catalog.field_lineage_parser import extract_field_lineage_simple
                        logger.info(f"Field lineage parser: Processing field '{field_name}'")
                        logger.debug(f"  SQL has WITH: {'WITH' in sql_text.upper() if sql_text else 'No SQL'}")
                        logger.debug(f"  SQL length: {len(sql_text) if sql_text else 0}")
                        # Pass the models file path for table column lookup
                        models_file_path = os.path.join(self.extracted_data_dir, 'models.json')
                        columns = extract_field_lineage_simple(sql_text, field_name, dialect=dialect, models_file_path=models_file_path)
                        logger.info(f"Field lineage parser result: {len(columns)} columns for field '{field_name}'")
                        if columns:
                            logger.debug(f"  First 3 columns: {columns[:3]}")
                        # For field-level lineage, we need actual columns not tables
                        # If the parser returns empty, we can't fall back to table parser
                        if not columns:
                            logger.warning(f"Field lineage parser returned empty for field {field_name}, SQL length: {len(sql_text)}")
                            columns = []  # Keep empty rather than using wrong parser
                    except ImportError as e:
                        logger.error(f"Failed to import field lineage parser: {e}")
                        columns = []
                    except Exception as e:
                        logger.error(f"Field lineage parser failed for {field_name}: {e}")
                        import traceback
                        logger.error(f"Traceback: {traceback.format_exc()}")
                        columns = []  # Keep empty rather than using wrong parser
                else:
                    # Use general SQL parser for CTE queries (table-level lineage)
                    from omni_to_catalog.table_lineage_parser import TableLineageParser
                    parser = TableLineageParser(dialect=dialect)
                    tables = parser.parse_cte_lineage(sql_text)

                    # For table-level lineage without field_name, return table references
                    columns = list(tables)

                # Format columns properly - no need to add prefixes, just ensure consistency
                formatted_columns = []
                for col in columns:
                    parts = col.split('.')
                    # If column already has 4 parts (database.schema.table.column), keep as is
                    if len(parts) == 4:
                        formatted_columns.append(col)
                    # If 3 parts, could be:
                    # - schema.table.column (missing database)
                    # - database.table.column (missing schema)
                    # Without more context, keep as-is since we don't know which is missing
                    elif len(parts) == 3:
                        formatted_columns.append(col)
                    # If 2 parts (table.column), we can't reliably add database/schema without context
                    elif len(parts) == 2:
                        # Keep as-is - the lineage parser should have provided full paths
                        formatted_columns.append(col)
                    else:
                        # Single part or other format, include as is
                        formatted_columns.append(col)

                return formatted_columns

            except Exception as e:
                logger.warning(f"Failed to parse SQL with CTEs, falling back to regex: {e}")
                import traceback
                logger.warning(f"Traceback: {traceback.format_exc()}")
                # Fall through to regex approach

        # Use existing regex for simple queries or as fallback
        # Look for both simple table.column and fully qualified patterns
        simple_pattern = r'\b([a-zA-Z_][\w]*\.[a-zA-Z_][\w]*)\b'
        full_pattern = r'\b([a-zA-Z_][\w]*\.[a-zA-Z_][\w]*\.[a-zA-Z_][\w]*\.[a-zA-Z_][\w]*)\b'

        # Try to find fully qualified references first
        full_columns = re.findall(full_pattern, sql_text)
        if full_columns:
            return list(set(full_columns))  # Return the fully qualified columns

        # Fall back to simple pattern
        columns = re.findall(simple_pattern, sql_text)

        # Return as-is - we can't reliably add database/schema without more context
        return list(set(columns))  # Remove duplicates

    def extract_database_name(self, query_obj: Dict, sql_text: str = None, connection_id: str = None) -> str:
        """
        Extract database name from connection info or table references
        Returns the database name if found, otherwise a default
        """
        # First, try to get database from connection mapping
        if connection_id and hasattr(self, 'connection_map') and connection_id in self.connection_map:
            connection_db = self.connection_map[connection_id]['database']
            if connection_db:
                return connection_db

        # Try to find database.schema.table patterns
        databases_found = set()

        # Check direct table reference
        if query_obj.get('table'):
            table_ref = query_obj['table']
            parts = table_ref.split('.')
            if len(parts) == 3:  # database.schema.table
                return parts[0]
            elif len(parts) == 2:  # Might be database.table or schema.table
                # For 2-part names, we can't reliably determine if it's database.table or schema.table
                # Add to potential databases for later evaluation
                databases_found.add(parts[0])

        # Check fields for database references
        for field in query_obj.get('fields', []):
            if '.' in field:
                parts = field.split('.')
                if len(parts) >= 3:  # database.schema.table.column or database.table.column
                    databases_found.add(parts[0])

        # Check SQL text for database references
        if sql_text or query_obj.get('userEditedSQL'):
            sql_to_check = sql_text or query_obj.get('userEditedSQL', '')
            # Look for database.schema.table patterns in FROM/JOIN clauses
            # Match 3-part qualified table names (database.schema.table)
            pattern = r'(?:FROM|JOIN)\s+([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)'
            matches = re.findall(pattern, sql_to_check, re.IGNORECASE)
            for match in matches:
                databases_found.add(match[0])  # First part is database

            # Also check for database.table patterns (2-part names)
            # But be careful not to double-match 3-part names
            if not matches:  # Only if we didn't find 3-part names
                pattern2 = r'(?:FROM|JOIN)\s+([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)'
                matches2 = re.findall(pattern2, sql_to_check, re.IGNORECASE)
                for match in matches2:
                    # Assume first part of 2-part name could be a database
                    # (though it might be schema.table instead)
                    databases_found.add(match[0])

        # If we found any database references, use one
        if databases_found:
            # If only one database found, use it
            if len(databases_found) == 1:
                return list(databases_found)[0]
            # If multiple found, return the first one alphabetically for consistency
            return sorted(databases_found)[0]

        # Try to extract from connection info if available
        if self.connections:
            for conn in self.connections:
                if 'database' in conn:
                    return conn['database']
                # Check if connection name looks like a database name
                if 'name' in conn and '.' not in conn['name']:
                    return conn['name']

        # Default to generic name if nothing found
        return 'default_database'

    def create_dashboards_csv(self, dashboards: List[Dict]) -> str:
        """
        Create dashboards.csv file per BI Importer specification
        Creates a hierarchy: VIZ_MODEL → TILE → DASHBOARD

        Required fields:
        - id: Dashboard/Model/Tile identifier
        - dashboard_type: DASHBOARD, VIZ_MODEL, or TILE
        - name: Dashboard/Model/Tile name
        - folder_path: Folder path (without dashboard name)
        - url: Dashboard/Tile URL

        Optional fields:
        - description: Description
        - user_name: Owner email
        - view_count: Popularity metric
        - parent_tables: Array of source tables
        - parent_dashboards: Parent dashboard/model IDs
        - created_at: Creation timestamp
        - updated_at: Update timestamp
        """
        csv_file = self.output_dir / f"{self.file_prefix}dashboards.csv"

        all_rows = []  # Collect all rows to write at once

        # Process dashboards
        for dashboard in dashboards:
            # Extract common metadata
            owner = dashboard.get('owner', {})
            if isinstance(owner, dict):
                user_name = owner.get('email', '')
            else:
                user_name = ''

            dashboard_id = dashboard.get('identifier') or dashboard.get('id', '')
            dashboard_name = dashboard.get('name', 'Unnamed Dashboard')
            dashboard_url = f"{self.base_url}/dashboards/{dashboard_id}" if dashboard_id and self.base_url else ""

            # Extract folder path
            folder = dashboard.get('folder')
            if isinstance(folder, dict):
                folder_path = folder.get('path', '/')
                if not folder_path.startswith('/'):
                    folder_path = '/' + folder_path
            elif folder == 'None' or not folder:
                folder_path = '/'
            else:
                folder_path = str(folder)

            # Extract parent tables
            parent_tables = self.extract_parent_tables(dashboard)

            # Timestamps
            created_at = dashboard.get('createdAt', dashboard.get('updatedAt', ''))
            updated_at = dashboard.get('updatedAt', '')

            # 1. Extract workbook model ID first
            # Look for modelId in various places
            workbook_model_id = None

            # Try from export_data first
            export_data = dashboard.get('export_data', {})
            if export_data and 'dashboard' in export_data:
                workbook_model_id = export_data['dashboard'].get('modelId') or export_data['dashboard'].get('workbookId')

            # Fallback to direct modelId
            if not workbook_model_id:
                workbook_model_id = dashboard.get('modelId')

            # Try from queries
            if not workbook_model_id and dashboard.get('queries'):
                first_query = dashboard['queries'][0] if dashboard['queries'] else {}
                if isinstance(first_query, dict) and 'query' in first_query:
                    workbook_model_id = first_query['query'].get('modelId')

            # If no model ID found, generate one based on dashboard ID
            if not workbook_model_id:
                workbook_model_id = f"{dashboard_id}_model"

            # 2. Create VIZ_MODEL entry (top-level)
            # Construct the model URL
            model_base_url = dashboard_url.split('/dashboards/')[0] if '/dashboards/' in dashboard_url else self.base_url
            model_url = f"{model_base_url}/models/{workbook_model_id}/ide?mode=combined" if model_base_url else ""

            viz_model_row = {
                'id': workbook_model_id,
                'dashboard_type': 'VIZ_MODEL',
                'name': f"{dashboard_name} Model",
                'folder_path': folder_path,
                'url': model_url,
                'description': 'Workbook model for dashboard',
                'user_name': user_name,
                'view_count': '',
                'parent_tables': self.format_array_field(parent_tables),
                'parent_dashboards': '',  # VIZ_MODEL is top-level, no parents
                'created_at': created_at,
                'updated_at': updated_at
            }
            all_rows.append(viz_model_row)

            # 3. Create TILE entries first (so we can link dashboard to them)
            tile_ids = []  # Track tile IDs for this dashboard
            tiles_created = False

            # Try to extract tiles from export_data
            if export_data and 'dashboard' in export_data:
                dash_data = export_data['dashboard']
                if 'queryPresentationCollection' in dash_data:
                    qpc = dash_data['queryPresentationCollection']
                    if 'queryPresentationCollectionMemberships' in qpc:
                        memberships = qpc['queryPresentationCollectionMemberships']

                        for idx, membership in enumerate(memberships):
                            if 'queryPresentation' in membership:
                                qp = membership['queryPresentation']
                                tile_id = qp.get('id', f"{dashboard_id}_tile_{idx}")
                                tile_name = qp.get('name', f"Tile {idx + 1}")

                                # TILE entries don't have parent_tables
                                # Only VIZ_MODEL has parent_tables since it's the semantic layer

                                tile_row = {
                                    'id': tile_id,
                                    'dashboard_type': 'TILE',
                                    'name': tile_name,
                                    'folder_path': folder_path,
                                    'url': dashboard_url,
                                    'description': qp.get('description', ''),
                                    'user_name': user_name,
                                    'view_count': '',
                                    'parent_tables': '',  # TILE doesn't have parent_tables
                                    'parent_dashboards': self.format_array_field([workbook_model_id]),  # TILE points to VIZ_MODEL
                                    'created_at': qp.get('createdAt', created_at),
                                    'updated_at': qp.get('updatedAt', updated_at)
                                }
                                all_rows.append(tile_row)
                                tile_ids.append(tile_id)  # Track this tile ID
                                tiles_created = True

            # Fallback: If no tiles created from export_data, create single tile from queries
            if not tiles_created and dashboard.get('queries'):
                # Create a single combined tile representing all queries
                tile_id = f"{dashboard_id}_tile"
                tile_name = f"{dashboard_name} - Queries"

                tile_row = {
                    'id': tile_id,
                    'dashboard_type': 'TILE',
                    'name': tile_name,
                    'folder_path': folder_path,
                    'url': dashboard_url,
                    'description': 'Combined queries for dashboard',
                    'user_name': user_name,
                    'view_count': '',
                    'parent_tables': '',  # TILE doesn't have parent_tables
                    'parent_dashboards': self.format_array_field([workbook_model_id]),  # TILE points to VIZ_MODEL
                    'created_at': created_at,
                    'updated_at': updated_at
                }
                all_rows.append(tile_row)
                tile_ids.append(tile_id)  # Track this tile ID

            # 4. Create DASHBOARD entry (child of TILE)
            # Dashboard points to its tile(s) to create linear flow:
            # Table -> VIZ_MODEL -> TILE -> DASHBOARD
            dashboard_row = {
                'id': dashboard_id,
                'dashboard_type': 'DASHBOARD',
                'name': dashboard_name,
                'folder_path': folder_path,
                'url': dashboard_url,
                'description': dashboard.get('description', ''),
                'user_name': user_name,
                'view_count': str(dashboard.get('view_count', 0)),
                'parent_tables': '',  # DASHBOARD doesn't have parent_tables, only VIZ_MODEL does
                'parent_dashboards': self.format_array_field(tile_ids) if tile_ids else self.format_array_field([workbook_model_id]),  # Points to TILEs for linear flow
                'created_at': created_at,
                'updated_at': updated_at
            }
            all_rows.append(dashboard_row)

        # Write all rows to CSV
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'id', 'dashboard_type', 'name', 'folder_path', 'url',
                'description', 'user_name', 'view_count', 'parent_tables',
                'parent_dashboards', 'created_at', 'updated_at'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)

        logger.info(f"Created {csv_file} with {len(all_rows)} entries ({len(dashboards)} dashboards with hierarchy)")
        return str(csv_file)

    def build_sql_from_query(self, query_obj: Dict, query_data: Dict, dashboard_id: str, query_idx: int) -> str:
        """
        Build a basic SQL statement from Omni's structured query data.
        This is a fallback when compiled_sql or userEditedSQL are not available.

        Note: This creates simplified SQL and does not attempt to guess complex
        structures like GROUP BY clauses. For accurate SQL, use Omni's compiled_sql.

        Args:
            query_obj: The query object containing table, fields, filters, etc.
            query_data: The parent query data containing name and ID
            dashboard_id: The dashboard ID
            query_idx: The query index for uniqueness

        Returns:
            Basic SQL statement string
        """
        table = query_obj.get('table', '')
        fields = query_obj.get('fields', [])

        if not table or not fields:
            return ''

        # Build field list
        field_list = ', '.join(fields)

        # Start building the SQL
        sql_parts = []
        # Note: Query deduplication is handled at a higher level in generate_dashboard_queries_csv()
        # to prevent duplicate SQL from being generated for the same dashboard

        # Check if we have a view definition for this table
        cte_sql = None
        if table in self.view_definitions:
            # We have the SQL definition for this view
            cte_sql = f'WITH "{table}" AS (\n  SELECT * FROM (\n{self.view_definitions[table]}\n  )\n)\n'

        # Build field list
        field_list = ', '.join(fields)

        if cte_sql:
            sql_parts.append(cte_sql)

        sql_parts.append(f"SELECT {field_list}")

        # FROM clause
        from_clause = f'FROM "{table}"' if table in self.view_definitions else f"FROM {table}"

        # Handle joins if present
        join_paths = query_obj.get('join_paths', [])
        join_from_topic = query_obj.get('join_paths_from_topic_name', '')
        if join_from_topic and join_from_topic != table:
            # There's a join from a different topic
            from_clause = f"FROM {join_from_topic}"
            # Could add JOIN clauses here if we had the join details

        sql_parts.append(from_clause)

        # WHERE clause from filters
        filters = query_obj.get('filters', {})
        where_conditions = []

        for field_name, filter_spec in filters.items():
            if isinstance(filter_spec, dict):
                filter_kind = filter_spec.get('kind', '')
                filter_type = filter_spec.get('type', '')

                if filter_kind == 'TIME_FOR_INTERVAL_DURATION':
                    # Time-based filter
                    left_side = filter_spec.get('left_side', '')
                    right_side = filter_spec.get('right_side', '')
                    if left_side and right_side:
                        where_conditions.append(f"{field_name} >= DATEADD(DAY, -{left_side.split()[0]}, CURRENT_DATE())")

                elif filter_kind == 'EXACT':
                    # Exact match filter
                    value = filter_spec.get('value', '')
                    if value:
                        # Handle string vs numeric values
                        if isinstance(value, str):
                            where_conditions.append(f"{field_name} = '{value}'")
                        else:
                            where_conditions.append(f"{field_name} = {value}")

                elif filter_kind == 'RANGE':
                    # Range filter
                    min_val = filter_spec.get('min')
                    max_val = filter_spec.get('max')
                    if min_val is not None:
                        where_conditions.append(f"{field_name} >= {min_val}")
                    if max_val is not None:
                        where_conditions.append(f"{field_name} <= {max_val}")

        if where_conditions:
            sql_parts.append("WHERE " + " AND ".join(where_conditions))

        # Note: We don't try to guess GROUP BY clauses here
        # Omni provides properly formed SQL with GROUP BY when needed
        # String matching on field names is unreliable (e.g., "summary" contains "sum")

        # ORDER BY clause from sorts
        sorts = query_obj.get('sorts', [])
        if sorts:
            order_by_parts = []
            for sort in sorts:
                column = sort.get('column_name', '')
                desc = sort.get('sort_descending', False)
                if column:
                    order_by_parts.append(f"{column} {'DESC' if desc else 'ASC'}")

            if order_by_parts:
                sql_parts.append(f"ORDER BY {', '.join(order_by_parts)}")

        # LIMIT clause
        limit = query_obj.get('limit')
        if limit:
            sql_parts.append(f"LIMIT {limit}")

        return '\n'.join(sql_parts)

    def create_dashboard_queries_csv(self, dashboards: List[Dict]) -> Optional[str]:
        """
        Create dashboard_queries.csv file using queries from Omni API
        Links queries to TILE entries and also duplicates them for parent DASHBOARD entries

        Required fields:
        - dashboard_id: Links to TILE or DASHBOARD ID
        - dashboard_type: TILE or DASHBOARD
        - text: SQL query text
        - database_name: Database name (extracted from table references)
        """
        queries = []
        # Track unique SQL per entity to prevent duplicates
        seen_sql_per_tile = {}

        # Build tile_id -> dashboard_id mapping so we can duplicate queries onto dashboards
        tile_to_dashboard = {}

        # Extract queries from dashboards
        for dashboard in dashboards:
            dashboard_id = dashboard.get('identifier') or dashboard.get('id', '')
            connection_id = dashboard.get('connectionId', '')

            # Check if we have export_data with queryPresentations (tiles)
            export_data = dashboard.get('export_data', {})
            tiles_with_queries = []

            if export_data and 'dashboard' in export_data:
                dashboard_data = export_data['dashboard']
                qp_collection = dashboard_data.get('queryPresentationCollection', {})
                memberships = qp_collection.get('queryPresentationCollectionMemberships', [])

                # Map each queryPresentation (tile) to its query
                for idx, membership in enumerate(memberships):
                    if 'queryPresentation' in membership:
                        qp = membership['queryPresentation']
                        tile_id = qp.get('id', f"{dashboard_id}_tile_{idx}")
                        # Get the query ID from the queryPresentation
                        query_id = qp.get('queryId')
                        tiles_with_queries.append({'tile_id': tile_id, 'query_id': query_id, 'tile_idx': idx})
                        tile_to_dashboard[tile_id] = dashboard_id
            else:
                # Fallback single tile
                tile_to_dashboard[f"{dashboard_id}_tile"] = dashboard_id

            # Get queries from the API endpoint data
            dashboard_queries = dashboard.get('queries', [])

            # If we have tiles mapped to queries, process accordingly
            if tiles_with_queries:
                # Map queries to their tiles
                for query_idx, query_data in enumerate(dashboard_queries):
                    # Find matching tile for this query
                    tile_id = None

                    if isinstance(query_data, dict):
                        query_obj = query_data.get('query', {})
                        query_id = query_obj.get('id')

                        # Find the tile for this query
                        for tile_info in tiles_with_queries:
                            if tile_info['query_id'] == query_id:
                                tile_id = tile_info['tile_id']
                                break

                        # If no match by ID, use index matching as fallback
                        if not tile_id and query_idx < len(tiles_with_queries):
                            tile_id = tiles_with_queries[query_idx]['tile_id']

                        # If still no tile, use a generic tile ID
                        if not tile_id:
                            tile_id = f"{dashboard_id}_tile_{query_idx}"

                        # Process the query
                        sql_text = ''
                        if query_obj.get('userEditedSQL'):
                            sql_text = query_obj['userEditedSQL']
                        else:
                            sql_text = self.build_sql_from_query(query_obj, query_data, dashboard_id, query_idx)

                        if sql_text:
                            # Initialize set for this tile if not exists
                            if tile_id not in seen_sql_per_tile:
                                seen_sql_per_tile[tile_id] = set()

                            # Check if we've already seen this SQL for this tile
                            if sql_text not in seen_sql_per_tile[tile_id]:
                                database_name = self.extract_database_name(query_obj, sql_text, connection_id)
                                seen_sql_per_tile[tile_id].add(sql_text)
                                queries.append({
                                    'dashboard_id': tile_id,  # Link to TILE
                                    'dashboard_type': 'TILE',
                                    'text': sql_text,
                                    'database_name': database_name
                                })
            else:
                # No tiles from export_data, use fallback single tile for all queries
                tile_id = f"{dashboard_id}_tile"

                # Initialize set for this tile if not exists
                if tile_id not in seen_sql_per_tile:
                    seen_sql_per_tile[tile_id] = set()

                for query_idx, query_data in enumerate(dashboard_queries):
                    sql_text = ''

                    if isinstance(query_data, dict):
                        query_obj = query_data.get('query', {})
                        if query_obj.get('userEditedSQL'):
                            sql_text = query_obj['userEditedSQL']
                        else:
                            sql_text = self.build_sql_from_query(query_obj, query_data, dashboard_id, query_idx)

                        if sql_text:
                            # Check if we've already seen this SQL for this tile
                            if sql_text not in seen_sql_per_tile[tile_id]:
                                database_name = self.extract_database_name(query_obj, sql_text, connection_id)
                                seen_sql_per_tile[tile_id].add(sql_text)
                                queries.append({
                                    'dashboard_id': tile_id,  # Link to TILE
                                    'dashboard_type': 'TILE',
                                    'text': sql_text,
                                    'database_name': database_name
                                })

        if not queries:
            logger.info("No SQL queries found (Omni uses structured queries)")
            return None

        # Duplicate TILE queries onto their parent DASHBOARD entities
        # Deduplicate using normalized SQL to match Coalesce's query cleaning
        # (strips comments, normalizes whitespace) and avoid ComparatorDuplicateDetected
        seen_dashboard_queries = set()
        dashboard_queries_to_add = []
        for query in queries:
            tile_id = query['dashboard_id']
            parent_dashboard_id = tile_to_dashboard.get(tile_id)
            if parent_dashboard_id:
                # Normalize SQL for dedup: strip comments and collapse whitespace
                # to match what Coalesce's query cleaner does
                normalized = re.sub(r'--[^\n]*', '', query['text'])
                normalized = re.sub(r'/\*.*?\*/', '', normalized, flags=re.DOTALL)
                normalized = ' '.join(normalized.split())
                dedup_key = (parent_dashboard_id, normalized)
                if dedup_key not in seen_dashboard_queries:
                    seen_dashboard_queries.add(dedup_key)
                    dashboard_queries_to_add.append({
                        'dashboard_id': parent_dashboard_id,
                        'dashboard_type': 'DASHBOARD',
                        'text': query['text'],
                        'database_name': query['database_name']
                    })
        queries.extend(dashboard_queries_to_add)

        csv_file = self.output_dir / f"{self.file_prefix}dashboard_queries.csv"

        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['dashboard_id', 'dashboard_type', 'text', 'database_name']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(queries)

        logger.info(f"Created {csv_file} with {len(queries)} queries "
                    f"({len(dashboard_queries_to_add)} duplicated to DASHBOARD level)")
        return str(csv_file)

    def create_dashboard_fields_csv(self, models: List[Dict], dashboards: List[Dict] = None) -> Optional[str]:
        """
        Create dashboard_fields.csv file for VIZ_MODEL entries only
        Per Coalesce docs: "Dashboard fields are only supported for Dashboards of dashboard_type = VIZ_MODEL"

        Required fields:
        - dashboard_id: Links to VIZ_MODEL ID
        - external_id: Field identifier
        - name: Field name
        - role: MEASURE or DIMENSION

        Optional fields:
        - data_type: Data type
        - description: Field description
        - is_primary_key: Boolean
        - label: Display label
        - view_name: View/table name
        - view_label: View display label
        - child_dashboards: Array of dependent dashboard IDs
        - parent_columns: Array of source columns
        """
        fields = []
        seen_fields = set()  # Track unique fields to avoid duplicates

        # First pass: Build mapping of field -> dashboards that use it
        field_to_dashboards = {}  # field_key -> set of dashboard_ids (actual dashboards, not viz models)
        dashboard_to_model = {}  # dashboard_id -> viz_model_id mapping

        # Scan all dashboards to build field usage mapping
        for dashboard in dashboards:
            dashboard_id = dashboard.get('identifier') or dashboard.get('id', '')

            # Get the VIZ_MODEL ID for this dashboard (we still need this for validation)
            viz_model_id = None

            # Try from export_data first
            export_data = dashboard.get('export_data', {})
            if export_data and 'dashboard' in export_data:
                viz_model_id = export_data['dashboard'].get('modelId') or export_data['dashboard'].get('workbookId')

            # Fallback to direct modelId
            if not viz_model_id:
                viz_model_id = dashboard.get('modelId')

            # Try from queries
            if not viz_model_id and dashboard.get('queries'):
                first_query = dashboard['queries'][0] if dashboard['queries'] else {}
                if isinstance(first_query, dict) and 'query' in first_query:
                    viz_model_id = first_query['query'].get('modelId')

            # Skip if no viz model found
            if not viz_model_id:
                logger.warning(f"No model ID found for dashboard {dashboard_id}, skipping fields")
                continue

            # Store the dashboard to model mapping
            dashboard_to_model[dashboard_id] = viz_model_id

            # Extract field types from export_data if available
            export_data = dashboard.get('export_data', {})
            if export_data and 'dashboard' in export_data:
                dash_data = export_data['dashboard']

                # Check queryPresentationCollection structure
                if 'queryPresentationCollection' in dash_data:
                    qpc = dash_data['queryPresentationCollection']
                    memberships = qpc.get('queryPresentationCollectionMemberships', [])

                    for membership in memberships:
                        if isinstance(membership, dict) and 'queryPresentation' in membership:
                            query_pres = membership['queryPresentation']
                            if isinstance(query_pres, dict) and 'visConfig' in query_pres:
                                vis_config = query_pres['visConfig']
                                if isinstance(vis_config, dict) and 'spec' in vis_config:
                                    spec = vis_config['spec']

                                    # Extract from all possible field locations in spec
                                    field_locations = []

                                    # Check x axis
                                    if 'x' in spec and isinstance(spec['x'], dict) and 'field' in spec['x']:
                                        field_locations.append(spec['x']['field'])

                                    # Check y axis
                                    if 'y' in spec and isinstance(spec['y'], dict) and 'field' in spec['y']:
                                        field_locations.append(spec['y']['field'])

                                    # Check series
                                    if 'series' in spec and isinstance(spec['series'], list):
                                        for series_item in spec['series']:
                                            if isinstance(series_item, dict) and 'field' in series_item:
                                                field_locations.append(series_item['field'])

                                    # Check tooltip
                                    if 'tooltip' in spec and isinstance(spec['tooltip'], list):
                                        for tooltip_item in spec['tooltip']:
                                            if isinstance(tooltip_item, dict) and 'field' in tooltip_item:
                                                field_locations.append(tooltip_item['field'])

                                    # Process all found fields
                                    for field_obj in field_locations:
                                        if isinstance(field_obj, dict):
                                            omni_type = field_obj.get('omniType')
                                            view_name = field_obj.get('viewName')
                                            field_name = field_obj.get('fieldName')
                                            is_measure = field_obj.get('isMeasure', False)

                                            if view_name and field_name:
                                                # Store with original field name (including time grain)
                                                metadata_key = f"{view_name}.{field_name}"

                                                if metadata_key not in self.field_metadata:
                                                    self.field_metadata[metadata_key] = {}

                                                # Store omniType if available
                                                if omni_type:
                                                    # Use omniType directly without conversion as requested
                                                    self.field_metadata[metadata_key]['data_type'] = omni_type
                                                    self.field_metadata[metadata_key]['omni_type'] = omni_type
                                                    logger.debug(f"Found omniType '{omni_type}' for field '{metadata_key}' from export_data")

                                                # Store role based on isMeasure
                                                self.field_metadata[metadata_key]['type'] = 'measure' if is_measure else 'dimension'
                                                logger.debug(f"Field '{metadata_key}' isMeasure={is_measure} -> type={'measure' if is_measure else 'dimension'}")

                                                # Also store without time grain for lookup
                                                if '[' in field_name:
                                                    field_without_grain = field_name.split('[')[0]
                                                    metadata_key_no_grain = f"{view_name}.{field_without_grain}"
                                                    if metadata_key_no_grain not in self.field_metadata:
                                                        self.field_metadata[metadata_key_no_grain] = {}
                                                    if omni_type:
                                                        self.field_metadata[metadata_key_no_grain]['data_type'] = omni_type
                                                        self.field_metadata[metadata_key_no_grain]['omni_type'] = omni_type
                                                    self.field_metadata[metadata_key_no_grain]['type'] = 'measure' if is_measure else 'dimension'
                                                    logger.debug(f"Also stored for field '{metadata_key_no_grain}' (without time grain)")

            for query in dashboard.get('queries', []):
                if isinstance(query, dict) and 'query' in query:
                    query_data = query['query']

                    # Track fields used in this DASHBOARD (not viz model)
                    query_fields = query_data.get('fields', [])
                    for field_name in query_fields:
                        parts = field_name.split('.')
                        if len(parts) >= 2:
                            view_name = parts[0]
                            field_actual = '.'.join(parts[1:])

                            # Remove time grain notation
                            if '[' in field_actual:
                                field_actual = field_actual.split('[')[0]

                            field_key = f"{view_name}_{field_actual}"
                            if field_key not in field_to_dashboards:
                                field_to_dashboards[field_key] = set()
                            field_to_dashboards[field_key].add(dashboard_id)  # Add DASHBOARD ID, not viz model ID

        # NOTE: Model field extraction is intentionally skipped
        # We only extract fields that are actually used in dashboard queries
        # This ensures the dashboard_fields.csv contains only relevant fields (~83)
        # rather than all fields defined in models (potentially thousands)
        # Extract fields from dashboard queries
        for dashboard in dashboards:
            dashboard_id = dashboard.get('identifier') or dashboard.get('id', '')
            connection_id = dashboard.get('connectionId', '')

            # Get the VIZ_MODEL ID for this dashboard
            viz_model_id = None

            # Try from export_data first
            export_data = dashboard.get('export_data', {})
            if export_data and 'dashboard' in export_data:
                viz_model_id = export_data['dashboard'].get('modelId') or export_data['dashboard'].get('workbookId')

            # Fallback to direct modelId
            if not viz_model_id:
                viz_model_id = dashboard.get('modelId')

            # Try from queries
            if not viz_model_id and dashboard.get('queries'):
                first_query = dashboard['queries'][0] if dashboard['queries'] else {}
                if isinstance(first_query, dict) and 'query' in first_query:
                    viz_model_id = first_query['query'].get('modelId')

            # Skip if no viz model found
            if not viz_model_id:
                logger.warning(f"No model ID found for dashboard {dashboard_id}, skipping query fields")
                continue

            # First, collect all CTE definitions from this dashboard
            # This includes both explicit CTEs in userEditedSQL and view definitions
            dashboard_cte_sql = {}

            # Add view definitions that might be used as CTEs
            for query in dashboard.get('queries', []):
                if isinstance(query, dict) and 'query' in query:
                    q = query['query']
                    table = q.get('table', '')

                    # Check if this table is a view definition
                    # Use model-specific view if available
                    if table:
                        view_sql = None
                        if viz_model_id and viz_model_id in self.view_definitions_by_model:
                            model_views = self.view_definitions_by_model[viz_model_id]
                            if table in model_views:
                                view_sql = model_views[table]
                                logger.debug(f"Added model-specific view '{table}' to dashboard_cte_sql from model {viz_model_id[:8]}... (has CTEs: {'WITH' in view_sql.upper()})")
                        elif table in self.view_definitions:
                            view_sql = self.view_definitions[table]
                            logger.debug(f"Added global view '{table}' to dashboard_cte_sql (has CTEs: {'WITH' in view_sql.upper()})")

                        if view_sql:
                            dashboard_cte_sql[table.lower()] = view_sql

                    # Also check for explicit CTEs in userEditedSQL
                    user_sql = q.get('userEditedSQL', '')
                    if user_sql and 'WITH' in user_sql.upper() and ' AS (' in user_sql.upper():
                        # If there's userEditedSQL with CTEs, use it for the table name too
                        if table:
                            dashboard_cte_sql[table.lower()] = user_sql

                        # Extract CTE names from this SQL
                        # Pattern to catch both quoted and unquoted CTE names
                        cte_patterns = [
                            r'WITH\s+"([^"]+)"\s+AS\s*\(',           # WITH "name" AS (
                            r"WITH\s+'([^']+)'\s+AS\s*\(",           # WITH 'name' AS (
                            r'(?:WITH|with)\s+(\w+)\s+(?:AS|as)\s*\(',  # WITH name AS (
                            r',\s*(\w+)\s+(?:AS|as)\s*\('            # , name AS ( (comma-separated)
                        ]
                        cte_names = []
                        for pattern in cte_patterns:
                            matches = re.findall(pattern, user_sql, re.IGNORECASE)
                            cte_names.extend(matches)
                        for cte_name in cte_names:
                            dashboard_cte_sql[cte_name.lower()] = user_sql

            # Process queries to extract fields
            for query in dashboard.get('queries', []):
                if isinstance(query, dict) and 'query' in query:
                    query_data = query['query']

                    # Extract database name from connection or query
                    database_name = self.extract_database_name(query_data, query_data.get('userEditedSQL'), connection_id)

                    # Extract fields from the query
                    query_fields = query_data.get('fields', [])
                    for field_name in query_fields:
                        # Parse field name to extract view and column
                        parts = field_name.split('.')
                        if len(parts) >= 2:
                            view_name = parts[0]
                            field_actual = '.'.join(parts[1:])

                            # Extract any time grain notation like [date] to infer type
                            time_grain = None
                            if '[' in field_actual:
                                time_grain = field_actual.split('[')[1].rstrip(']')
                                field_actual = field_actual.split('[')[0]

                            field_key = f"{viz_model_id}_{view_name}_{field_actual}"
                            if field_key not in seen_fields:
                                seen_fields.add(field_key)

                                # First try to get metadata from Omni model definitions
                                metadata_key = f"{view_name}.{field_actual}"
                                metadata = self.field_metadata.get(metadata_key, {})

                                # Determine if it's a measure or dimension
                                if metadata.get('type'):
                                    # Use type from Omni model (dimension or measure)
                                    role = 'MEASURE' if metadata['type'] == 'measure' else 'DIMENSION'
                                else:
                                    # Default to DIMENSION when not explicitly specified
                                    role = 'DIMENSION'

                                # Determine data type - only from explicit Omni metadata
                                data_type = ''  # Empty by default - no assumptions

                                if metadata.get('data_type'):
                                    data_type = metadata['data_type']  # Only use Omni metadata when explicitly available
                                    logger.debug(f"Field '{field_actual}' using Omni metadata type: {data_type}")
                                # else: leave data_type empty - we don't infer from time grains or patterns

                                # Get child dashboards for this field
                                # Only include dashboards that belong to this VIZ_MODEL
                                field_lookup_key = f"{view_name}_{field_actual}"
                                all_dashboards_using_field = field_to_dashboards.get(field_lookup_key, set())
                                child_dashboards = [
                                    d_id for d_id in all_dashboards_using_field
                                    if dashboard_to_model.get(d_id) == viz_model_id
                                ]

                                # Infer parent columns from the field structure with database prefix
                                # Format: database.table.column for full lineage
                                parent_columns = []

                                # Check if this field references a CTE (either in this query or another query in the dashboard)
                                user_sql = query_data.get('userEditedSQL', '')
                                cte_sql = None

                                # First check if the view itself has SQL with CTEs
                                # Use model-specific view definitions if available
                                if not cte_sql and viz_model_id and viz_model_id in self.view_definitions_by_model:
                                    model_views = self.view_definitions_by_model[viz_model_id]
                                    if view_name in model_views:
                                        view_sql = model_views[view_name]
                                        if 'WITH' in view_sql.upper() and ' AS (' in view_sql.upper():
                                            cte_sql = view_sql
                                            logger.debug(f"Using model-specific view definition SQL for '{view_name}' from model {viz_model_id[:8]}... (has CTEs)")
                                # Fallback to global view definitions
                                elif not cte_sql and view_name in self.view_definitions:
                                    view_sql = self.view_definitions[view_name]
                                    if 'WITH' in view_sql.upper() and ' AS (' in view_sql.upper():
                                        cte_sql = view_sql
                                        logger.debug(f"Using global view definition SQL for '{view_name}' (has CTEs)")

                                # Check if the view_name or table matches a known CTE
                                if not cte_sql and view_name.lower() in dashboard_cte_sql:
                                    cte_sql = dashboard_cte_sql[view_name.lower()]
                                    logger.debug(f"Found CTE SQL for view '{view_name}' in dashboard_cte_sql")
                                elif not cte_sql and query_data.get('table', '').lower() in dashboard_cte_sql:
                                    cte_sql = dashboard_cte_sql[query_data['table'].lower()]
                                    logger.debug(f"Found CTE SQL for table '{query_data.get('table')}' in dashboard_cte_sql")
                                elif not cte_sql and user_sql and 'WITH' in user_sql.upper() and ' AS (' in user_sql.upper():
                                    cte_sql = user_sql
                                    logger.debug(f"Using userEditedSQL as CTE SQL")

                                if cte_sql:
                                    logger.info(f"Found CTE SQL for field '{field_actual}', attempting field lineage extraction")

                                    # Check if field_actual is a measure that references a dimension
                                    field_for_lineage = field_actual

                                    # Use model-specific view content if available
                                    view_content = None

                                    logger.debug(f"Looking for view '{view_name}' for field '{field_actual}'")

                                    if viz_model_id and viz_model_id in self.view_full_content_by_model:
                                        model_view_content = self.view_full_content_by_model[viz_model_id]
                                        if view_name in model_view_content:
                                            view_content = model_view_content[view_name]
                                            logger.debug(f"Found model-specific view content for '{view_name}'")

                                    # Fallback to global view content if not found in model-specific
                                    if not view_content and view_name in self.view_full_content:
                                        view_content = self.view_full_content[view_name]
                                        logger.debug(f"Using global view content for '{view_name}'")
                                    if view_content:
                                        # Parse measures to check if field_actual is a measure referencing a dimension
                                        if 'measures:' in view_content and field_actual + ':' in view_content:
                                            # Extract the measure definition
                                            measure_start = view_content.find(f"{field_actual}:")
                                            if measure_start != -1:
                                                measure_section = view_content[measure_start:]
                                                # Look for sql: ${view.dimension} pattern
                                                sql_match = re.search(r'sql:\s*\$\{[^.]+\.([^}]+)\}', measure_section[:200])
                                                if sql_match:
                                                    dimension_name = sql_match.group(1)
                                                    logger.info(f"Field '{field_actual}' is a measure referencing dimension '{dimension_name}'")
                                                    field_for_lineage = dimension_name

                                    # Query or dashboard has CTEs, use field-specific extraction
                                    # Try to extract parent columns using field-specific CTE-aware method
                                    parent_columns = self.extract_parent_columns_from_sql(
                                        cte_sql,
                                        connection_id,
                                        field_name=field_for_lineage
                                    )

                                    # If field-specific extraction didn't find anything, DO NOT fall back to table-level extraction
                                    # Table-level extraction returns table names, not column names, which is incorrect for field lineage
                                    if not parent_columns:
                                        logger.warning(f"Field lineage extraction failed for field '{field_actual}' in CTE query")
                                        # Keep empty rather than using incorrect table names
                                        parent_columns = []
                                    else:
                                        logger.debug(f"Field lineage extraction succeeded: {parent_columns[:5]}...")

                                # If no CTEs or no extraction, use simple approach
                                if not parent_columns:
                                    # Check if this is a view with CTEs - if so, don't use simple fallback
                                    table_ref = query_data.get('table', '')
                                    view_sql = None

                                    # Try model-specific view first
                                    if table_ref and viz_model_id and viz_model_id in self.view_definitions_by_model:
                                        model_views = self.view_definitions_by_model[viz_model_id]
                                        if table_ref in model_views:
                                            view_sql = model_views[table_ref]
                                    # Fallback to global view definitions
                                    elif table_ref and table_ref in self.view_definitions:
                                        view_sql = self.view_definitions[table_ref]

                                    if view_sql:
                                        if 'WITH' in view_sql.upper() and ' AS (' in view_sql.upper():
                                            logger.warning(f"Field '{field_actual}' in view '{table_ref}' has CTEs but field lineage extraction failed. Leaving parent_columns empty.")
                                            parent_columns = []  # Keep empty rather than using wrong simple approach
                                        else:
                                            logger.debug(f"No parent columns found for field '{field_actual}' in non-CTE view, using simple approach")
                                            table_ref = query_data['table']
                                            # Check how many parts table_ref has
                                            parts = table_ref.split('.')
                                            if len(parts) == 3:  # database.schema.table
                                                parent_columns = [f"{table_ref}.{field_actual}"]
                                            elif len(parts) == 2:  # Could be database.table or schema.table
                                                # If we have database_name and it's not in the table_ref, add it
                                                if database_name and database_name not in table_ref:
                                                    parent_columns = [f"{database_name}.{table_ref}.{field_actual}"]
                                                else:
                                                    parent_columns = [f"{table_ref}.{field_actual}"]
                                            elif len(parts) == 1:  # Just table name
                                                # Add database if available, but don't guess schema
                                                if database_name:
                                                    parent_columns = [f"{database_name}.{table_ref}.{field_actual}"]
                                                else:
                                                    parent_columns = [f"{table_ref}.{field_actual}"]
                                    elif query_data.get('table'):
                                        # Not a view, use simple approach
                                        logger.debug(f"No parent columns found for field '{field_actual}', using simple approach")
                                        table_ref = query_data['table']
                                        # Check how many parts table_ref has
                                        parts = table_ref.split('.')
                                        if len(parts) == 3:  # database.schema.table
                                            parent_columns = [f"{table_ref}.{field_actual}"]
                                        elif len(parts) == 2:  # Could be database.table or schema.table
                                            # If we have database_name and it's not in the table_ref, add it
                                            if database_name and database_name not in table_ref:
                                                parent_columns = [f"{database_name}.{table_ref}.{field_actual}"]
                                            else:
                                                parent_columns = [f"{table_ref}.{field_actual}"]
                                        elif len(parts) == 1:  # Just table name
                                            # Add database if available, but don't guess schema
                                            if database_name:
                                                parent_columns = [f"{database_name}.{table_ref}.{field_actual}"]
                                            else:
                                                parent_columns = [f"{table_ref}.{field_actual}"]
                                    else:
                                        # Use view_name with database prefix if available
                                        if database_name:
                                            parent_columns = [f"{database_name}.{view_name}.{field_actual}"]
                                        else:
                                            parent_columns = [f"{view_name}.{field_actual}"]

                                fields.append({
                                    'dashboard_id': viz_model_id,  # Link to VIZ_MODEL, not dashboard
                                    'external_id': f"{viz_model_id}_{view_name}_{field_actual}",
                                    'name': field_actual,
                                    'role': role,
                                    'data_type': data_type,
                                    'description': '',
                                    'is_primary_key': 'False',  # Uppercase F to match the sample
                                    'label': field_actual,  # Keep original name to avoid transformation issues
                                    'view_name': view_name,
                                    'view_label': view_name,  # Keep original name to avoid transformation issues
                                    'child_dashboards': self.format_array_field(child_dashboards),
                                    'parent_columns': self.format_array_field(parent_columns)
                                })

                    # Also extract from calculations if present
                    for calc in query_data.get('calculations', []):
                        calc_name = calc.get('name', '')
                        if calc_name:
                            field_key = f"{viz_model_id}_calc_{calc_name}"
                            if field_key not in seen_fields:
                                seen_fields.add(field_key)
                                # Extract parent columns from calculation expression
                                parent_columns = []
                                expression = calc.get('expression', '')
                                if expression:
                                    # Use new extraction method that handles CTEs with field-specific lineage
                                    parent_columns = self.extract_parent_columns_from_sql(
                                        expression,
                                        connection_id,
                                        field_name=calc_name  # Pass field name for specific lineage
                                    )
                                    # The method already adds database prefix, so no need to add it again

                                fields.append({
                                    'dashboard_id': viz_model_id,  # Link to VIZ_MODEL, not dashboard
                                    'external_id': f"{viz_model_id}_calc_{calc_name}",
                                    'name': calc_name,
                                    'role': 'MEASURE',  # Calculations are typically measures
                                    'data_type': '',  # Don't assume type without metadata
                                    'description': f"Calculated field: {calc.get('expression', '')}",
                                    'is_primary_key': 'False',
                                    'label': calc_name,  # Keep original name to avoid transformation issues
                                    'view_name': 'calculations',
                                    'view_label': 'Calculations',
                                    'child_dashboards': self.format_array_field([viz_model_id]),  # This calc belongs to this viz model
                                    'parent_columns': self.format_array_field(parent_columns)
                                })

        if not fields:
            logger.info("No fields found in model data")
            return None, 0

        csv_file = self.output_dir / f"{self.file_prefix}dashboard_fields.csv"

        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'dashboard_id', 'data_type', 'description', 'external_id',
                'is_primary_key', 'label', 'name', 'role',
                'view_name', 'view_label', 'child_dashboards', 'parent_columns'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(fields)

        logger.info(f"Created {csv_file} with {len(fields)} fields")
        return str(csv_file), len(fields)

    def convert(self) -> Dict[str, Any]:
        """
        Convert Omni data to BI Importer CSV format

        Returns:
            Dictionary with file paths and statistics
        """
        logger.info("Starting Omni to BI Importer conversion...")

        # Load extracted data
        data = self.load_extracted_data()

        # Extract view SQL definitions from models for query expansion
        self.view_definitions = self.extract_view_definitions(data['models'])

        # Extract topics and relationships for better SQL generation
        self.topics, self.relationships = self.extract_topics_and_relationships(data['models'])

        # Save topics and relationships for debugging
        if self.topics:
            topics_file = self.extracted_data_dir / "topics.json"
            with open(topics_file, 'w') as f:
                json.dump(self.topics, f, indent=2)
            logger.info(f"Saved {len(self.topics)} topics to {topics_file}")

        if self.relationships:
            relationships_file = self.extracted_data_dir / "relationships.json"
            with open(relationships_file, 'w') as f:
                json.dump(self.relationships, f, indent=2)
            logger.info(f"Saved relationships for {len(self.relationships)} models to {relationships_file}")

        # Count queries from dashboards
        total_queries = 0
        for dashboard in data['dashboards']:
            queries_list = dashboard.get('queries', [])
            if isinstance(queries_list, list):
                total_queries += len(queries_list)

        result = {
            'files_created': [],
            'statistics': {
                'dashboards': len(data['dashboards']),
                'models': len(data['models']),
                'queries': total_queries,
                'fields': 0
            }
        }

        # Create dashboards CSV (required)
        dashboards_csv = self.create_dashboards_csv(data['dashboards'])
        result['files_created'].append(dashboards_csv)

        # Create dashboard queries CSV (optional)
        queries_csv = self.create_dashboard_queries_csv(data['dashboards'])
        if queries_csv:
            result['files_created'].append(queries_csv)

        # Create dashboard fields CSV (for both models and dashboards)
        fields_csv, field_count = self.create_dashboard_fields_csv(data['models'], data['dashboards'])
        if fields_csv:
            result['files_created'].append(fields_csv)
        result['statistics']['fields'] = field_count

        logger.info(f"Conversion complete. Created {len(result['files_created'])} CSV files")
        logger.info(f"Files ready for upload to Coalesce Catalog BI Importer")
        return result


