"""
Table Lineage Parser for extracting table dependencies from SQL queries.

This module handles CTE parsing to identify actual source tables,
excluding intermediate CTEs from the lineage. It focuses on table-level
lineage extraction, complementing field_lineage_parser which handles
field-level lineage.
"""

import re
from typing import List, Set, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class TableLineageParser:
    """Parse SQL queries to extract table lineage, handling CTEs properly."""

    def __init__(self, dialect: str = 'snowflake'):
        """Initialize the SQL parser.

        Args:
            dialect: SQL dialect (snowflake, bigquery, etc.) - for compatibility
        """
        self.dialect = dialect
        # Patterns for SQL parsing
        self.cte_pattern = re.compile(
            r'WITH\s+(\w+)\s+AS\s*\(',
            re.IGNORECASE | re.MULTILINE
        )

        self.table_pattern = re.compile(
            r'FROM\s+(["`]?[\w\.]+["`]?(?:\s+AS\s+\w+)?)',
            re.IGNORECASE
        )

        self.join_pattern = re.compile(
            r'JOIN\s+(["`]?[\w\.]+["`]?(?:\s+AS\s+\w+)?)',
            re.IGNORECASE
        )

    def parse_cte_lineage(self, sql: str) -> Set[str]:
        """
        Parse SQL to extract actual table dependencies, excluding CTEs.

        Args:
            sql: SQL query string

        Returns:
            Set of fully-qualified table names (database.schema.table)
        """
        if not sql:
            return set()

        # First, identify all CTE names
        cte_names = self._extract_cte_names(sql)
        logger.debug(f"Found CTEs: {cte_names}")

        # Extract all table references
        all_tables = self._extract_all_table_references(sql)
        logger.debug(f"Found all tables: {all_tables}")

        # Filter out CTEs to get only real tables
        real_tables = {
            table for table in all_tables
            if not self._is_cte(table, cte_names)
        }

        logger.debug(f"Real tables after filtering CTEs: {real_tables}")
        return real_tables

    def _extract_cte_names(self, sql: str) -> Set[str]:
        """Extract all CTE names from the WITH clause."""
        cte_names = set()

        # More robust CTE extraction - find all CTEs before the main query
        # Look for pattern: WITH ... cte_name AS ( ... ) ... SELECT/INSERT/UPDATE/DELETE

        # First check if SQL starts with WITH (ignoring whitespace/comments)
        sql_upper = sql.upper().strip()
        if not sql_upper.startswith('WITH'):
            return cte_names

        # Find all CTE definitions - pattern: word AS (
        # This will capture all CTEs regardless of where the final SELECT is
        cte_pattern = re.compile(
            r'\b(\w+)\s+AS\s*\(',
            re.IGNORECASE
        )

        # Find the position of the final SELECT/INSERT/UPDATE/DELETE that's not inside parentheses
        # We need to parse up to this point to get all CTEs
        main_query_start = self._find_main_query_start(sql)

        if main_query_start > 0:
            # Extract only from the WITH clause part
            with_clause = sql[:main_query_start]
        else:
            # Fallback - use the whole SQL
            with_clause = sql

        # Extract CTE names from the WITH clause
        for match in cte_pattern.finditer(with_clause):
            cte_name = match.group(1)
            # Skip if it's after WITH keyword itself
            if cte_name.upper() not in ('WITH', 'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE'):
                cte_names.add(cte_name.lower())

        return cte_names

    def _find_main_query_start(self, sql: str) -> int:
        """Find the position where the main query (after CTEs) starts."""
        # Track parentheses depth to ignore SELECT inside CTEs
        depth = 0
        i = 0
        sql_len = len(sql)

        # Skip past WITH keyword
        with_pos = sql.upper().find('WITH')
        if with_pos >= 0:
            i = with_pos + 4

        while i < sql_len:
            if sql[i] == '(':
                depth += 1
            elif sql[i] == ')':
                depth -= 1
            elif depth == 0:
                # We're not inside parentheses, check for main query keywords
                remaining = sql[i:].upper()
                if (remaining.startswith('SELECT') or
                    remaining.startswith('INSERT') or
                    remaining.startswith('UPDATE') or
                    remaining.startswith('DELETE')):
                    # Make sure it's not part of a CTE definition
                    # Check if there's an AS before this (which would make it a CTE)
                    before = sql[:i].strip()
                    if not before.endswith('AS'):
                        return i
            i += 1

        return -1

    def _extract_all_table_references(self, sql: str) -> Set[str]:
        """Extract all table references from FROM and JOIN clauses."""
        tables = set()

        # Clean SQL for easier parsing
        sql_clean = self._clean_sql(sql)

        # Extract FROM tables
        from_matches = self.table_pattern.findall(sql_clean)
        for match in from_matches:
            table = self._clean_table_name(match)
            if table:
                tables.add(table)

        # Extract JOIN tables
        join_matches = self.join_pattern.findall(sql_clean)
        for match in join_matches:
            table = self._clean_table_name(match)
            if table:
                tables.add(table)

        # Also look for tables in subqueries
        subquery_tables = self._extract_subquery_tables(sql_clean)
        tables.update(subquery_tables)

        return tables

    def _extract_subquery_tables(self, sql: str) -> Set[str]:
        """Extract tables from subqueries."""
        tables = set()

        # Find subqueries (simplified approach)
        subquery_pattern = re.compile(
            r'\(\s*SELECT.*?FROM\s+(["`]?[\w\.]+["`]?).*?\)',
            re.IGNORECASE | re.DOTALL
        )

        for match in subquery_pattern.finditer(sql):
            table = self._clean_table_name(match.group(1))
            if table:
                tables.add(table)

        return tables

    def _clean_sql(self, sql: str) -> str:
        """Clean SQL for easier parsing."""
        # Remove comments
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)

        # Normalize whitespace
        sql = re.sub(r'\s+', ' ', sql)

        return sql.strip()

    def _clean_table_name(self, table_ref: str) -> Optional[str]:
        """Clean and normalize table name."""
        if not table_ref:
            return None

        # Remove alias (AS something)
        table = re.sub(r'\s+AS\s+\w+', '', table_ref, flags=re.IGNORECASE)

        # Remove quotes and backticks only if they're matching at start and end
        if (table.startswith('"') and table.endswith('"')) or \
           (table.startswith('`') and table.endswith('`')):
            table = table[1:-1]

        # Remove any remaining whitespace
        table = table.strip()

        # Skip if it's a function or value
        if '(' in table or not table or table.upper() in ('DUAL', 'VALUES'):
            return None

        return table

    def _is_cte(self, table_name: str, cte_names: Set[str]) -> bool:
        """Check if a table name is actually a CTE."""
        if not table_name:
            return False

        # Get the base table name (last part if qualified)
        parts = table_name.split('.')
        base_name = parts[-1].lower()

        return base_name in cte_names

    def extract_tables_from_sql(self, sql: str) -> List[str]:
        """
        Extract real table dependencies from SQL, excluding CTEs.

        Args:
            sql: SQL query string

        Returns:
            List of fully-qualified table names
        """
        tables = self.parse_cte_lineage(sql)
        return sorted(list(tables))