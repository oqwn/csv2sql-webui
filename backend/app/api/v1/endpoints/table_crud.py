from typing import Any, Optional, List, Dict
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text, inspect
from pydantic import BaseModel

from app.db.session import get_db

router = APIRouter()


class TableDataRequest(BaseModel):
    table_name: str
    page: int = 0
    page_size: int = 25
    search_column: Optional[str] = None
    search_value: Optional[str] = None
    order_by: Optional[str] = None
    order_direction: str = "ASC"


class RecordCreateRequest(BaseModel):
    table_name: str
    data: Dict[str, Any]


class RecordUpdateRequest(BaseModel):
    table_name: str
    primary_key_column: str
    primary_key_value: Any
    data: Dict[str, Any]


class RecordDeleteRequest(BaseModel):
    table_name: str
    primary_key_column: str
    primary_key_value: Any


def get_primary_key(db: Session, table_name: str) -> Optional[str]:
    """Get the primary key column name for a table"""
    try:
        inspector = inspect(db.get_bind())
        pk_constraint = inspector.get_pk_constraint(table_name)
        if pk_constraint and pk_constraint['constrained_columns']:
            return pk_constraint['constrained_columns'][0]
        
        # Fallback: assume first column is primary key
        columns = inspector.get_columns(table_name)
        return columns[0]['name'] if columns else None
    except Exception:
        return None


@router.post("/data")
async def get_table_data(
    request: TableDataRequest,
    db: Session = Depends(get_db),
) -> Any:
    """
    Get paginated table data with optional search
    """
    try:
        # Build base query
        query = f'SELECT * FROM "{request.table_name}"'
        count_query = f'SELECT COUNT(*) as total FROM "{request.table_name}"'
        
        # Add search filter if provided
        where_clause = ""
        if request.search_column and request.search_value:
            where_clause = f' WHERE "{request.search_column}" ILIKE :search_value'
            query += where_clause
            count_query += where_clause
        
        # Add ordering
        if request.order_by:
            query += f' ORDER BY "{request.order_by}" {request.order_direction}'
        
        # Add pagination
        query += f' LIMIT :limit OFFSET :offset'
        
        # Execute count query
        count_params = {}
        if request.search_value:
            count_params['search_value'] = f'%{request.search_value}%'
        
        count_result = db.execute(text(count_query), count_params).fetchone()
        total_count = count_result[0] if count_result else 0
        
        # Execute data query
        data_params = {
            'limit': request.page_size,
            'offset': request.page * request.page_size
        }
        if request.search_value:
            data_params['search_value'] = f'%{request.search_value}%'
        
        result = db.execute(text(query), data_params)
        
        # Fetch all rows first
        rows_data = result.fetchall()
        
        # Get column names
        columns = list(result.keys()) if rows_data else []
        
        # Convert rows to dictionaries
        rows = []
        for row in rows_data:
            rows.append(dict(row._mapping))
        
        # Get primary key
        primary_key = get_primary_key(db, request.table_name)
        
        return {
            "columns": columns,
            "rows": rows,
            "total_count": total_count,
            "page": request.page,
            "page_size": request.page_size,
            "primary_key": primary_key
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/record")
async def create_record(
    request: RecordCreateRequest,
    db: Session = Depends(get_db),
) -> Any:
    """
    Create a new record in the specified table
    """
    try:
        # Build INSERT query
        columns = list(request.data.keys())
        placeholders = [f':{col}' for col in columns]
        
        query = f"""
        INSERT INTO "{request.table_name}" ({', '.join([f'"{col}"' for col in columns])})
        VALUES ({', '.join(placeholders)})
        RETURNING *
        """
        
        # Execute query
        result = db.execute(text(query), request.data)
        db.commit()
        
        # Return the created record
        created_record = dict(result.fetchone()._mapping)
        
        return {
            "message": "Record created successfully",
            "record": created_record
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/record")
async def update_record(
    request: RecordUpdateRequest,
    db: Session = Depends(get_db),
) -> Any:
    """
    Update an existing record in the specified table
    """
    try:
        # Build UPDATE query - exclude primary key from data to prevent overwriting
        set_clauses = []
        params = {}
        
        for col, value in request.data.items():
            # Skip primary key column to avoid setting it in the update
            if col != request.primary_key_column:
                set_clauses.append(f'"{col}" = :{col}')
                params[col] = value
        
        # Add primary key to params for WHERE clause
        params['pk_value'] = request.primary_key_value
        
        # Check if we have anything to update
        if not set_clauses:
            raise HTTPException(status_code=400, detail="No fields to update")
        
        query = f"""
        UPDATE "{request.table_name}"
        SET {', '.join(set_clauses)}
        WHERE "{request.primary_key_column}" = :pk_value
        RETURNING *
        """
        
        # Execute query
        result = db.execute(text(query), params)
        db.commit()
        
        # Check if record was updated
        updated_record = result.fetchone()
        if not updated_record:
            raise HTTPException(status_code=404, detail="Record not found")
        
        return {
            "message": "Record updated successfully",
            "record": dict(updated_record._mapping)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/record/delete")
async def delete_record(
    request: RecordDeleteRequest,
    db: Session = Depends(get_db),
) -> Any:
    """
    Delete a record from the specified table
    """
    try:
        # Build DELETE query
        query = f"""
        DELETE FROM "{request.table_name}"
        WHERE "{request.primary_key_column}" = :pk_value
        RETURNING *
        """
        
        params = {'pk_value': request.primary_key_value}
        
        # Execute query
        result = db.execute(text(query), params)
        db.commit()
        
        # Check if record was deleted
        deleted_record = result.fetchone()
        if not deleted_record:
            raise HTTPException(status_code=404, detail="Record not found")
        
        return {
            "message": "Record deleted successfully",
            "record": dict(deleted_record._mapping)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/table/{table_name}/info")
async def get_table_info(
    table_name: str,
    db: Session = Depends(get_db),
) -> Any:
    """
    Get detailed information about a table including columns and constraints
    """
    try:
        inspector = inspect(db.get_bind())
        
        # Get columns
        columns = inspector.get_columns(table_name)
        
        # Get primary key
        pk_constraint = inspector.get_pk_constraint(table_name)
        primary_keys = pk_constraint['constrained_columns'] if pk_constraint else []
        
        # Get foreign keys
        foreign_keys = inspector.get_foreign_keys(table_name)
        
        # Get unique constraints
        unique_constraints = inspector.get_unique_constraints(table_name)
        
        # Format column information
        column_info = []
        for col in columns:
            col_data = {
                "name": col['name'],
                "type": str(col['type']),
                "nullable": col['nullable'],
                "default": col['default'],
                "is_primary": col['name'] in primary_keys,
                "is_unique": any(col['name'] in uc['column_names'] for uc in unique_constraints),
                "foreign_key": None
            }
            
            # Check if column is a foreign key
            for fk in foreign_keys:
                if col['name'] in fk['constrained_columns']:
                    col_data['foreign_key'] = {
                        "table": fk['referred_table'],
                        "column": fk['referred_columns'][0] if fk['referred_columns'] else None
                    }
                    break
            
            column_info.append(col_data)
        
        return {
            "table_name": table_name,
            "columns": column_info,
            "primary_key": primary_keys[0] if primary_keys else None,
            "foreign_keys": foreign_keys,
            "unique_constraints": unique_constraints
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))