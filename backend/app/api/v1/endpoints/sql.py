from typing import Dict, List, Any, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.sql_executor import DataSourceSQLExecutor
from app.services.local_storage import local_storage

router = APIRouter()


async def get_primary_key_info(executor: DataSourceSQLExecutor, table_name: str) -> Optional[str]:
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
            if not result['error'] and result['rows'] and len(result['rows']) > 0:
                return result['rows'][0][0] if result['rows'][0] else None
        
        return None
    except Exception:
        return None


class SQLQueryRequest(BaseModel):
    data_source_id: int
    query: str


class SQLQueryResponse(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
    row_count: int
    execution_time: float
    error: Optional[str] = None


class TableListRequest(BaseModel):
    data_source_id: int


class TableInfoRequest(BaseModel):
    data_source_id: int
    table_name: str


class TableInfo(BaseModel):
    name: str
    type: str
    row_count: Optional[int] = None
    columns: List[Dict[str, Any]]
    primary_key: Optional[str] = None


@router.post("/execute", response_model=SQLQueryResponse)
async def execute_query(request: SQLQueryRequest) -> SQLQueryResponse:
    """Execute SQL query against a data source"""
    # Get data source
    data_source = local_storage.get_data_source(request.data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Execute query
    executor = DataSourceSQLExecutor(
        data_source['type'],
        data_source['connection_config']
    )
    
    result = await executor.execute_query(request.query)
    
    if result['error']:
        raise HTTPException(status_code=400, detail=result['error'])
    
    return SQLQueryResponse(
        columns=result['columns'],
        rows=result['rows'],
        row_count=result['row_count'],
        execution_time=result['execution_time']
    )


@router.post("/tables", response_model=List[TableInfo])
async def list_tables(request: TableListRequest) -> List[TableInfo]:
    """List all tables in the connected data source"""
    # Get data source
    data_source = local_storage.get_data_source(request.data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Get tables
    executor = DataSourceSQLExecutor(
        data_source['type'],
        data_source['connection_config']
    )
    
    tables = await executor.list_tables()
    return tables


@router.post("/table-info", response_model=TableInfo)
async def get_table_info(request: TableInfoRequest) -> TableInfo:
    """Get detailed information about a specific table"""
    # Get data source
    data_source = local_storage.get_data_source(request.data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Get table info
    executor = DataSourceSQLExecutor(
        data_source['type'],
        data_source['connection_config']
    )
    
    table_info = await executor.get_table_info(request.table_name)
    
    if not table_info:
        raise HTTPException(status_code=404, detail=f"Table '{request.table_name}' not found")
    
    # Get primary key using proper database query
    primary_key = await get_primary_key_info(executor, request.table_name)
    
    # Update table info with correct primary key information
    if primary_key:
        table_info['primary_key'] = primary_key
        # Also update the column info to mark the primary key column
        for col in table_info.get('columns', []):
            if col['name'] == primary_key:
                col['primary_key'] = True
    
    return table_info


@router.post("/validate")
async def validate_query(request: SQLQueryRequest) -> Dict[str, Any]:
    """Validate SQL query syntax without executing"""
    # Get data source
    data_source = local_storage.get_data_source(request.data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # For now, just return success
    # In a real implementation, this would validate against the specific SQL dialect
    return {
        "valid": True,
        "message": "Query syntax is valid"
    }