from typing import Any, Optional, List, Dict
from fastapi import APIRouter, HTTPException, Form
from pydantic import BaseModel

from app.services.local_storage import local_storage
from app.services.sql_executor import DataSourceSQLExecutor

router = APIRouter()


class TableDataRequest(BaseModel):
    table_name: str
    page: int = 0
    page_size: int = 25
    search_column: Optional[str] = None
    search_value: Optional[str] = None
    order_by: Optional[str] = None
    order_direction: str = "ASC"
    data_source_id: int


class RecordCreateRequest(BaseModel):
    table_name: str
    data: Dict[str, Any]
    data_source_id: int


class RecordUpdateRequest(BaseModel):
    table_name: str
    primary_key_column: str
    primary_key_value: Any
    data: Dict[str, Any]
    data_source_id: int


class RecordDeleteRequest(BaseModel):
    table_name: str
    primary_key_column: str
    primary_key_value: Any
    data_source_id: int


async def get_primary_key(executor: DataSourceSQLExecutor, table_name: str) -> Optional[str]:
    """Get the primary key column name for a table"""
    try:
        # Try different database-specific queries for primary key detection
        queries = [
            # PostgreSQL
            f"""
            SELECT column_name 
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = '{table_name}' AND tc.constraint_type = 'PRIMARY KEY'
            """,
            # MySQL
            f"""
            SELECT column_name 
            FROM information_schema.key_column_usage 
            WHERE table_name = '{table_name}' AND constraint_name = 'PRIMARY'
            """,
            # Generic fallback - first column
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position LIMIT 1"
        ]
        
        for query in queries:
            result = await executor.execute_query(query)
            if not result['error'] and result['data']:
                return result['data'][0]['column_name'] if result['data'] else None
        
        return None
    except Exception:
        return None


@router.post("/data")
async def get_table_data(request: TableDataRequest) -> Any:
    """Get paginated table data with optional search"""
    # Get data source
    data_source = local_storage.get_data_source(request.data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Create executor
    executor = DataSourceSQLExecutor(
        data_source['type'],
        data_source['connection_config']
    )
    
    try:
        # Build base query
        query = f'SELECT * FROM "{request.table_name}"'
        count_query = f'SELECT COUNT(*) as total FROM "{request.table_name}"'
        
        # Add search filter if provided
        where_clause = ""
        if request.search_column and request.search_value:
            # Use LIKE for broader database compatibility
            where_clause = f' WHERE "{request.search_column}" LIKE \'%{request.search_value}%\''
            query += where_clause
            count_query += where_clause
        
        # Add ordering
        if request.order_by:
            query += f' ORDER BY "{request.order_by}" {request.order_direction}'
        
        # Add pagination
        query += f' LIMIT {request.page_size} OFFSET {request.page * request.page_size}'
        
        # Execute count query
        count_result = await executor.execute_query(count_query)
        if count_result['error']:
            raise HTTPException(status_code=400, detail=f"Count query failed: {count_result['error']}")
        
        total_count = count_result['rows'][0][0] if count_result['rows'] else 0
        
        # Execute data query
        data_result = await executor.execute_query(query)
        if data_result['error']:
            raise HTTPException(status_code=400, detail=f"Data query failed: {data_result['error']}")
        
        raw_rows = data_result['rows'] or []
        columns = data_result['columns'] or []
        
        # Convert rows from arrays to objects
        rows = []
        for raw_row in raw_rows:
            row_dict = {}
            for i, value in enumerate(raw_row):
                if i < len(columns):
                    row_dict[columns[i]] = value
            rows.append(row_dict)
        
        # Get primary key
        primary_key = await get_primary_key(executor, request.table_name)
        
        return {
            "columns": columns,
            "rows": rows,
            "total_count": total_count,
            "page": request.page,
            "page_size": request.page_size,
            "primary_key": primary_key
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/record")
async def create_record(request: RecordCreateRequest) -> Any:
    """Create a new record in the specified table"""
    # Get data source
    data_source = local_storage.get_data_source(request.data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Create executor
    executor = DataSourceSQLExecutor(
        data_source['type'],
        data_source['connection_config']
    )
    
    try:
        # Build INSERT query
        columns = list(request.data.keys())
        escaped_columns = [f'"{col}"' for col in columns]
        
        # Build values with proper escaping
        values = []
        for value in request.data.values():
            if value is None:
                values.append("NULL")
            elif isinstance(value, str):
                escaped_value = value.replace("'", "''")
                values.append(f"'{escaped_value}'")
            else:
                values.append(str(value))
        
        query = f"""
        INSERT INTO "{request.table_name}" ({', '.join(escaped_columns)})
        VALUES ({', '.join(values)})
        """
        
        # Execute query
        result = await executor.execute_query(query)
        if result['error']:
            raise HTTPException(status_code=400, detail=f"Insert failed: {result['error']}")
        
        return {
            "message": "Record created successfully",
            "table_name": request.table_name
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/record")
async def update_record(request: RecordUpdateRequest) -> Any:
    """Update an existing record in the specified table"""
    # Get data source
    data_source = local_storage.get_data_source(request.data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Create executor
    executor = DataSourceSQLExecutor(
        data_source['type'],
        data_source['connection_config']
    )
    
    try:
        # Build UPDATE query - exclude primary key from data
        set_clauses = []
        
        for col, value in request.data.items():
            if col != request.primary_key_column:
                if value is None:
                    set_clauses.append(f'"{col}" = NULL')
                elif isinstance(value, str):
                    escaped_value = value.replace("'", "''")
                    set_clauses.append(f'"{col}" = \'{escaped_value}\'')
                else:
                    set_clauses.append(f'"{col}" = {value}')
        
        if not set_clauses:
            raise HTTPException(status_code=400, detail="No fields to update")
        
        # Escape primary key value
        if isinstance(request.primary_key_value, str):
            escaped_pk_value = request.primary_key_value.replace("'", "''")
            pk_value_str = f"'{escaped_pk_value}'"
        else:
            pk_value_str = str(request.primary_key_value)
        
        query = f"""
        UPDATE "{request.table_name}"
        SET {', '.join(set_clauses)}
        WHERE "{request.primary_key_column}" = {pk_value_str}
        """
        
        # Execute query
        result = await executor.execute_query(query)
        if result['error']:
            raise HTTPException(status_code=400, detail=f"Update failed: {result['error']}")
        
        return {
            "message": "Record updated successfully",
            "table_name": request.table_name
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/record/delete")
async def delete_record(request: RecordDeleteRequest) -> Any:
    """Delete a record from the specified table"""
    # Get data source
    data_source = local_storage.get_data_source(request.data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Create executor
    executor = DataSourceSQLExecutor(
        data_source['type'],
        data_source['connection_config']
    )
    
    try:
        # Escape primary key value
        if isinstance(request.primary_key_value, str):
            escaped_pk_value = request.primary_key_value.replace("'", "''")
            pk_value_str = f"'{escaped_pk_value}'"
        else:
            pk_value_str = str(request.primary_key_value)
        
        # Build DELETE query
        query = f"""
        DELETE FROM "{request.table_name}"
        WHERE "{request.primary_key_column}" = {pk_value_str}
        """
        
        # Execute query
        result = await executor.execute_query(query)
        if result['error']:
            raise HTTPException(status_code=400, detail=f"Delete failed: {result['error']}")
        
        return {
            "message": "Record deleted successfully",
            "table_name": request.table_name
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/table/{table_name}/info")
async def get_table_info(
    table_name: str,
    data_source_id: int = Form(...)
) -> Any:
    """Get detailed information about a table including columns and constraints"""
    # Get data source
    data_source = local_storage.get_data_source(data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Create executor
    executor = DataSourceSQLExecutor(
        data_source['type'],
        data_source['connection_config']
    )
    
    try:
        # Get column information using information_schema
        columns_query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        
        columns_result = await executor.execute_query(columns_query)
        if columns_result['error']:
            raise HTTPException(status_code=400, detail=f"Failed to get table info: {columns_result['error']}")
        
        columns = columns_result['data'] or []
        
        # Get primary key
        primary_key = await get_primary_key(executor, table_name)
        
        # Format column information
        column_info = []
        for col in columns:
            col_data = {
                "name": col['column_name'],
                "type": col['data_type'],
                "nullable": col['is_nullable'].lower() == 'yes',
                "default": col['column_default'],
                "is_primary": col['column_name'] == primary_key,
                "is_unique": False,  # Would need additional query for this
                "foreign_key": None  # Would need additional query for this
            }
            column_info.append(col_data)
        
        return {
            "table_name": table_name,
            "columns": column_info,
            "primary_key": primary_key,
            "foreign_keys": [],  # Would need additional implementation
            "unique_constraints": []  # Would need additional implementation
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/table/{table_name}")
async def delete_table(
    table_name: str,
    data_source_id: int = Form(...)
) -> Any:
    """Delete a table from the database"""
    # Get data source
    data_source = local_storage.get_data_source(data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Create executor
    executor = DataSourceSQLExecutor(
        data_source['type'],
        data_source['connection_config']
    )
    
    try:
        # Drop the table
        query = f'DROP TABLE "{table_name}" CASCADE'
        result = await executor.execute_query(query)
        
        if result['error']:
            raise HTTPException(status_code=400, detail=f"Failed to delete table: {result['error']}")
        
        return {
            "message": f"Table '{table_name}' deleted successfully",
            "table_name": table_name
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))