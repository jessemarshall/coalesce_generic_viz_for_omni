"""
Simplified field-level lineage parser for SQL queries with CTEs.
Focuses on accurately tracing specific fields through CTE transformations.
"""

import logging
from typing import List, Dict, Set
import sqlglot
from sqlglot import exp
from .table_column_lookup import TableColumnLookup

logger = logging.getLogger(__name__)


def extract_field_lineage_simple(sql: str, field_name: str, dialect: str = 'snowflake', models_file_path: str = None) -> List[str]:
    """
    Extract lineage for a specific field in a simpler, more robust way.

    Args:
        sql: SQL query string
        field_name: Name of the field to trace
        dialect: SQL dialect
        models_file_path: Optional path to models.json for table column lookup

    Returns:
        List of source columns that contribute to this field
    """
    logger.debug(f"extract_field_lineage_simple called for field '{field_name}'")
    logger.debug(f"  SQL length: {len(sql)}, has WITH: {'WITH' in sql.upper()}")
    try:
        # Initialize table column lookup
        table_lookup = TableColumnLookup(models_file_path)

        # Parse the SQL
        parsed = sqlglot.parse_one(sql, dialect=dialect)

        # Build CTE definitions map
        cte_defs = {}
        for cte in parsed.find_all(exp.CTE):
            cte_name = str(cte.alias) if cte.alias else None
            if cte_name:
                cte_defs[cte_name] = cte

        # Find the final SELECT (not inside a CTE)
        final_select = None
        for select in parsed.find_all(exp.Select):
            parent = select.parent
            is_in_cte = False
            while parent:
                if isinstance(parent, exp.CTE):
                    is_in_cte = True
                    break
                parent = parent.parent if hasattr(parent, 'parent') else None
            if not is_in_cte:
                final_select = select
                break

        if not final_select:
            return []

        # Find the field in the final SELECT
        field_expr = None

        # Check for SELECT * case
        if any(isinstance(expr, exp.Star) for expr in final_select.expressions):
            logger.debug(f"  Final SELECT has SELECT *")
            # SELECT * - need to trace the field from the FROM clause
            from_tables = get_from_references(final_select)
            logger.debug(f"  FROM tables: {from_tables}")
            if from_tables:
                # Assume field comes from the first table/CTE
                table_ref = from_tables[0]
                if table_ref in cte_defs:
                    logger.debug(f"  Tracing field '{field_name}' through CTE '{table_ref}'")
                    # It's a CTE, trace the field through it
                    result = sorted(list(trace_cte_field(field_name, cte_defs[table_ref], cte_defs, table_lookup)))
                    logger.debug(f"  Result: {result}")
                    return result
                else:
                    # Direct table reference
                    logger.debug(f"  Direct table reference: {table_ref}")
                    return [f"{table_ref}.{field_name}"]

        # Look for specific field
        for expr in final_select.expressions:
            if isinstance(expr, exp.Alias):
                alias_str = str(expr.alias)
                # Check both exact match and suffix match for prefixed field names
                if alias_str == field_name or alias_str.endswith(f".{field_name}"):
                    field_expr = expr.this
                    break
            elif isinstance(expr, exp.Column):
                col_name = str(expr.name)
                if col_name == field_name or col_name.endswith(f".{field_name}"):
                    field_expr = expr
                    break

        if not field_expr:
            return []

        # Trace the field back to source tables
        source_columns = trace_expression(field_expr, final_select, cte_defs, table_lookup)

        return sorted(list(source_columns))

    except Exception as e:
        logger.warning(f"Failed to extract field lineage for {field_name}: {e}")
        return []


def trace_expression(expr: exp.Expression, context: exp.Select, cte_defs: Dict, table_lookup: TableColumnLookup = None) -> Set[str]:
    """
    Trace an expression back to its source columns.

    Args:
        expr: Expression to trace
        context: SELECT context
        cte_defs: Map of CTE definitions
        table_lookup: Optional table column lookup for COUNT(*) handling

    Returns:
        Set of source columns (only actual tables, not CTEs)
    """
    sources = set()

    # Handle different expression types
    if isinstance(expr, exp.Column):
        col_name = str(expr.name) if expr.name else str(expr)
        # Remove quotes if present (but preserve special characters like /)
        col_name = col_name.strip('"').strip("'")
        table_ref = str(expr.table) if expr.table else None

        if table_ref:
            # Check if it's a CTE reference
            if table_ref in cte_defs:
                # Trace through the CTE - don't add the CTE name itself
                cte_sources = trace_cte_field(col_name, cte_defs[table_ref], cte_defs, table_lookup)
                sources.update(cte_sources)
            else:
                # Direct table reference - only add if it's not a CTE
                sources.add(f"{table_ref}.{col_name}")
        else:
            # No explicit table, check FROM clause
            from_tables = get_from_references(context)
            for table in from_tables:
                if table in cte_defs:
                    # It's a CTE - trace through it, don't add the CTE name
                    cte_sources = trace_cte_field(col_name, cte_defs[table], cte_defs, table_lookup)
                    sources.update(cte_sources)
                else:
                    # Regular table
                    sources.add(f"{table}.{col_name}")

    elif isinstance(expr, (exp.DateTrunc, exp.TimestampTrunc)):
        # Date truncation - trace the date column
        for col in expr.find_all(exp.Column):
            sources.update(trace_expression(col, context, cte_defs, table_lookup))

    elif isinstance(expr, exp.Coalesce):
        # COALESCE - all arguments contribute
        # First argument is in expr.this, rest in expr.expressions
        if expr.this:
            sources.update(trace_expression(expr.this, context, cte_defs, table_lookup))
        for arg in expr.expressions:
            sources.update(trace_expression(arg, context, cte_defs, table_lookup))

    elif isinstance(expr, (exp.Min, exp.Max, exp.Sum, exp.Avg)):
        # Aggregate functions (except COUNT) - trace the aggregated expression
        if expr.this:
            sources.update(trace_expression(expr.this, context, cte_defs, table_lookup))

    elif isinstance(expr, exp.Count):
        # COUNT - if COUNT(*), depends on all columns that determine row existence
        if expr.this and isinstance(expr.this, exp.Star):
            # COUNT(*) - find all columns referenced in the query context
            # This includes GROUP BY, WHERE, and SELECT columns

            # Get GROUP BY columns if present
            group_by = context.find(exp.Group)
            if group_by:
                for col in group_by.find_all(exp.Column):
                    sources.update(trace_expression(col, context, cte_defs, table_lookup))

            # Get WHERE clause columns
            where_clause = context.find(exp.Where)
            if where_clause:
                for col in where_clause.find_all(exp.Column):
                    sources.update(trace_expression(col, context, cte_defs, table_lookup))

            # Get columns from FROM clause tables/CTEs
            from_tables = get_from_references(context)
            for table in from_tables:
                if table in cte_defs:
                    # Recursively get all columns this CTE depends on
                    cte = cte_defs[table]
                    select = cte.find(exp.Select)
                    if select:
                        # Get all columns this CTE exposes/uses
                        for sel_expr in select.expressions:
                            if isinstance(sel_expr, exp.Star):
                                # SELECT * in CTE - trace its source tables
                                inner_from = get_from_references(select)
                                for inner_table in inner_from:
                                    if inner_table not in cte_defs:
                                        # Base table - try to look up all columns
                                        if table_lookup:
                                            columns = table_lookup.get_table_columns(inner_table)
                                            if columns:
                                                sources.update(columns)
                                            else:
                                                # Fallback to wildcard if lookup fails
                                                sources.add(f"{inner_table}.*")
                                        else:
                                            sources.add(f"{inner_table}.*")
                            elif isinstance(sel_expr, (exp.Alias, exp.Column)):
                                # Specific columns - trace them
                                if isinstance(sel_expr, exp.Alias):
                                    sources.update(trace_expression(sel_expr.this, select, cte_defs, table_lookup))
                                else:
                                    sources.update(trace_expression(sel_expr, select, cte_defs, table_lookup))
                else:
                    # Direct table - COUNT(*) depends on all its columns
                    if table_lookup:
                        # Try to look up all columns for the table
                        columns = table_lookup.get_table_columns(table)
                        if columns:
                            sources.update(columns)
                        else:
                            # Fallback to wildcard if lookup fails
                            sources.add(f"{table}.*")
                    else:
                        sources.add(f"{table}.*")
        elif expr.this:
            # COUNT(specific_column) - trace that column
            sources.update(trace_expression(expr.this, context, cte_defs, table_lookup))

    else:
        # For other expressions (including window functions), find all column references
        for col in expr.find_all(exp.Column):
            sources.update(trace_expression(col, context, cte_defs, table_lookup))

    return sources


def trace_cte_field(field_name: str, cte: exp.CTE, cte_defs: Dict, table_lookup: TableColumnLookup = None) -> Set[str]:
    """
    Trace a field through a CTE definition.

    Args:
        field_name: Field to trace
        cte: CTE definition
        cte_defs: Map of all CTE definitions
        table_lookup: Optional table column lookup for COUNT(*) handling

    Returns:
        Set of source columns
    """
    sources = set()

    # Find the SELECT in the CTE
    select = cte.find(exp.Select)
    if not select:
        return sources

    # Check for SELECT * case in the CTE
    if any(isinstance(expr, exp.Star) for expr in select.expressions):
        # CTE has SELECT *, need to trace from its FROM clause
        from_tables = get_from_references(select)
        if from_tables:
            table_ref = from_tables[0]
            if table_ref in cte_defs:
                # Recursive CTE reference
                sources.update(trace_cte_field(field_name, cte_defs[table_ref], cte_defs, table_lookup))
            else:
                # Direct table reference
                sources.add(f"{table_ref}.{field_name}")
        return sources

    # Find the field in the CTE's SELECT
    for expr in select.expressions:
        expr_field = None
        expr_value = None

        if isinstance(expr, exp.Alias):
            expr_field = str(expr.alias)
            expr_value = expr.this
        elif isinstance(expr, exp.Column):
            expr_field = str(expr.name) if expr.name else str(expr)
            expr_value = expr

        if expr_field == field_name:
            # Found the field, trace its expression
            # Only the direct dependencies, not the GROUP BY context
            sources.update(trace_expression(expr_value, select, cte_defs, table_lookup))
            break

    # Don't automatically include GROUP BY columns
    # Only trace the direct dependencies of the field expression
    # GROUP BY columns are not direct data dependencies for lineage purposes

    return sources


def get_from_references(select: exp.Select) -> List[str]:
    """
    Get table/CTE references from the FROM clause.

    Args:
        select: SELECT statement

    Returns:
        List of table/CTE names
    """
    tables = []

    # Look for tables in the FROM clause
    from_clause = select.find(exp.From)
    if from_clause:
        for table in from_clause.find_all(exp.Table):
            # Get the full table name
            parts = []
            if hasattr(table, 'catalog') and table.catalog:
                parts.append(str(table.catalog))
            if hasattr(table, 'db') and table.db:
                parts.append(str(table.db))
            if hasattr(table, 'name') and table.name:
                parts.append(str(table.name))

            table_name = '.'.join(parts) if parts else ''
            if table_name:
                tables.append(table_name)

    return tables