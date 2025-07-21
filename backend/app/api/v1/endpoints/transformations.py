from typing import Any, List, Dict, Optional, Union
from fastapi import APIRouter, HTTPException, Form, Body
from pydantic import BaseModel, Field
from enum import Enum
import json
import pandas as pd
from datetime import datetime

from app.services.local_storage import local_storage
from app.services.sql_executor import DataSourceSQLExecutor
from app.services.transformation_engine import TransformationEngine
from app.services.cross_datasource_engine import CrossDataSourceEngine

router = APIRouter()


class TransformationType(str, Enum):
    FILTER = "filter"
    CLEAN = "clean"
    AGGREGATE = "aggregate"
    JOIN = "join"
    SPLIT_COLUMN = "split_column"
    MERGE_COLUMN = "merge_column"
    CUSTOM_SQL = "custom_sql"
    CUSTOM_PYTHON = "custom_python"
    TYPE_CONVERSION = "type_conversion"
    RENAME = "rename"
    DROP = "drop"
    FILL_NULL = "fill_null"


class FilterOperator(str, Enum):
    EQUALS = "="
    NOT_EQUALS = "!="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_EQUAL = ">="
    LESS_EQUAL = "<="
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    IS_NULL = "is_null"
    NOT_NULL = "not_null"


class AggregateFunction(str, Enum):
    SUM = "sum"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    MEDIAN = "median"
    STD = "std"
    VAR = "var"


class JoinType(str, Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"


class CleaningRule(BaseModel):
    column: str
    rule_type: str = Field(..., description="trim, remove_special, lowercase, uppercase, remove_numbers, etc.")
    parameters: Optional[Dict[str, Any]] = None


class FilterRule(BaseModel):
    column: str
    operator: FilterOperator
    value: Optional[Union[str, int, float, List[Any]]] = None
    case_sensitive: bool = False


class AggregationConfig(BaseModel):
    group_by: List[str]
    aggregations: List[Dict[str, str]]  # {"column": "amount", "function": "sum", "alias": "total_amount"}
    having: Optional[List[FilterRule]] = None


class JoinConfig(BaseModel):
    left_source: Dict[str, Any]  # {"datasource_id": 1, "table": "users"}
    right_source: Dict[str, Any]  # {"datasource_id": 2, "table": "orders"}
    join_type: JoinType
    join_conditions: List[Dict[str, str]]  # [{"left": "id", "right": "user_id"}]


class ColumnSplitConfig(BaseModel):
    column: str
    delimiter: Optional[str] = None
    pattern: Optional[str] = None  # regex pattern
    new_columns: List[str]
    keep_original: bool = False


class ColumnMergeConfig(BaseModel):
    columns: List[str]
    separator: str = " "
    new_column: str
    drop_original: bool = True


class TypeConversionConfig(BaseModel):
    column: str
    target_type: str  # "integer", "float", "string", "date", "datetime", "boolean"
    format: Optional[str] = None  # for date/datetime conversions
    default_value: Optional[Any] = None  # for failed conversions


class CustomScriptConfig(BaseModel):
    script_type: str = Field(..., description="python or sql")
    script: str
    parameters: Optional[Dict[str, Any]] = None


class TransformationStep(BaseModel):
    id: Optional[str] = None
    name: str
    type: TransformationType
    config: Dict[str, Any]
    description: Optional[str] = None


class TransformationPipeline(BaseModel):
    id: Optional[str] = None
    name: str
    description: Optional[str] = None
    source_config: Dict[str, Any]  # datasource_id, table_name or query
    steps: List[TransformationStep]
    output_config: Optional[Dict[str, Any]] = None  # save to table, export, etc.
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class TransformationPreviewRequest(BaseModel):
    source_config: Dict[str, Any]
    steps: List[TransformationStep]
    preview_rows: int = 100


class TransformationExecuteRequest(BaseModel):
    pipeline_id: Optional[str] = None
    source_config: Optional[Dict[str, Any]] = None
    steps: Optional[List[TransformationStep]] = None
    output_config: Dict[str, Any]
    execute_async: bool = False


@router.post("/pipelines")
async def create_pipeline(pipeline: TransformationPipeline) -> Any:
    """Create a new transformation pipeline"""
    try:
        # Generate ID and timestamps
        pipeline.id = f"pipeline_{datetime.now().timestamp()}"
        pipeline.created_at = datetime.now()
        pipeline.updated_at = datetime.now()
        
        # Save to storage
        local_storage.save_transformation_pipeline(pipeline.dict())
        
        return {
            "status": "success",
            "pipeline": pipeline,
            "message": "Transformation pipeline created successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/pipelines")
async def list_pipelines() -> Any:
    """List all transformation pipelines"""
    try:
        pipelines = local_storage.get_transformation_pipelines()
        return {
            "pipelines": pipelines,
            "total": len(pipelines)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/pipelines/{pipeline_id}")
async def get_pipeline(pipeline_id: str) -> Any:
    """Get a specific transformation pipeline"""
    try:
        pipeline = local_storage.get_transformation_pipeline(pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        return pipeline
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/pipelines/{pipeline_id}")
async def update_pipeline(pipeline_id: str, pipeline: TransformationPipeline) -> Any:
    """Update an existing transformation pipeline"""
    try:
        existing = local_storage.get_transformation_pipeline(pipeline_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        pipeline.id = pipeline_id
        pipeline.created_at = existing.get("created_at")
        pipeline.updated_at = datetime.now()
        
        local_storage.save_transformation_pipeline(pipeline.dict())
        
        return {
            "status": "success",
            "pipeline": pipeline,
            "message": "Pipeline updated successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/pipelines/{pipeline_id}")
async def delete_pipeline(pipeline_id: str) -> Any:
    """Delete a transformation pipeline"""
    try:
        if not local_storage.get_transformation_pipeline(pipeline_id):
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        local_storage.delete_transformation_pipeline(pipeline_id)
        
        return {
            "status": "success",
            "message": "Pipeline deleted successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/preview")
async def preview_transformation(request: TransformationPreviewRequest) -> Any:
    """Preview transformation results without saving"""
    try:
        # Initialize transformation engine
        engine = TransformationEngine()
        
        # Load source data
        source_config = request.source_config
        data_source = local_storage.get_data_source(source_config['datasource_id'])
        if not data_source:
            raise HTTPException(status_code=404, detail="Data source not found")
        
        # Create executor
        executor = DataSourceSQLExecutor(
            data_source['type'],
            data_source['connection_config']
        )
        
        # Load data
        if 'query' in source_config:
            query = source_config['query']
        else:
            table_name = source_config['table_name']
            query = f'SELECT * FROM "{table_name}" LIMIT {request.preview_rows}'
        
        result = await executor.execute_query(query)
        if result['error']:
            raise HTTPException(status_code=400, detail=f"Failed to load source data: {result['error']}")
        
        # Convert to DataFrame
        df = pd.DataFrame(result['rows'], columns=result['columns'])
        
        # Apply transformations
        transformed_df = await engine.apply_transformations(df, request.steps)
        
        # Prepare preview response
        preview_data = transformed_df.head(request.preview_rows).to_dict('records')
        
        return {
            "status": "success",
            "original_shape": {"rows": len(df), "columns": len(df.columns)},
            "transformed_shape": {"rows": len(transformed_df), "columns": len(transformed_df.columns)},
            "columns": list(transformed_df.columns),
            "data_types": transformed_df.dtypes.astype(str).to_dict(),
            "preview": preview_data,
            "transformations_applied": len(request.steps)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/execute")
async def execute_transformation(request: TransformationExecuteRequest) -> Any:
    """Execute transformation pipeline and save results"""
    try:
        # Get pipeline configuration
        if request.pipeline_id:
            pipeline = local_storage.get_transformation_pipeline(request.pipeline_id)
            if not pipeline:
                raise HTTPException(status_code=404, detail="Pipeline not found")
            source_config = pipeline['source_config']
            steps = [TransformationStep(**step) for step in pipeline['steps']]
        else:
            if not request.source_config or not request.steps:
                raise HTTPException(status_code=400, detail="Either pipeline_id or source_config and steps must be provided")
            source_config = request.source_config
            steps = request.steps
        
        # Initialize engines
        engine = TransformationEngine()
        
        # Check if this is a cross-datasource operation
        has_cross_datasource = any(
            step.type == TransformationType.JOIN and 
            step.config.get('left_source', {}).get('datasource_id') != step.config.get('right_source', {}).get('datasource_id')
            for step in steps
        )
        
        if has_cross_datasource:
            cross_engine = CrossDataSourceEngine()
            result_df = await cross_engine.execute_pipeline(source_config, steps)
        else:
            # Load source data
            data_source = local_storage.get_data_source(source_config['datasource_id'])
            if not data_source:
                raise HTTPException(status_code=404, detail="Data source not found")
            
            executor = DataSourceSQLExecutor(
                data_source['type'],
                data_source['connection_config']
            )
            
            # Load data
            if 'query' in source_config:
                query = source_config['query']
            else:
                table_name = source_config['table_name']
                query = f'SELECT * FROM "{table_name}"'
            
            result = await executor.execute_query(query)
            if result['error']:
                raise HTTPException(status_code=400, detail=f"Failed to load source data: {result['error']}")
            
            # Convert to DataFrame
            df = pd.DataFrame(result['rows'], columns=result['columns'])
            
            # Apply transformations
            result_df = await engine.apply_transformations(df, steps)
        
        # Save results based on output config
        output_config = request.output_config
        output_type = output_config.get('type', 'table')
        
        if output_type == 'table':
            # Save to database table
            target_datasource_id = output_config.get('datasource_id', source_config['datasource_id'])
            target_table = output_config['table_name']
            
            target_datasource = local_storage.get_data_source(target_datasource_id)
            if not target_datasource:
                raise HTTPException(status_code=404, detail="Target data source not found")
            
            target_executor = DataSourceSQLExecutor(
                target_datasource['type'],
                target_datasource['connection_config']
            )
            
            # Create table and insert data
            await engine.save_to_table(result_df, target_table, target_executor, output_config.get('if_exists', 'replace'))
            
            return {
                "status": "success",
                "message": f"Transformation completed. Data saved to table '{target_table}'",
                "rows_processed": len(result_df),
                "columns": list(result_df.columns),
                "output_location": {
                    "type": "table",
                    "datasource_id": target_datasource_id,
                    "table_name": target_table
                }
            }
        
        elif output_type == 'export':
            # Export to file (CSV/Excel)
            export_format = output_config.get('format', 'csv')
            file_path = await engine.export_data(result_df, export_format, output_config.get('filename'))
            
            return {
                "status": "success",
                "message": f"Transformation completed. Data exported to {export_format.upper()} file",
                "rows_processed": len(result_df),
                "columns": list(result_df.columns),
                "output_location": {
                    "type": "file",
                    "format": export_format,
                    "path": file_path
                }
            }
        
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported output type: {output_type}")
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/validate-script")
async def validate_custom_script(config: CustomScriptConfig) -> Any:
    """Validate a custom transformation script"""
    try:
        engine = TransformationEngine()
        
        if config.script_type == "python":
            is_valid, error = await engine.validate_python_script(config.script)
        elif config.script_type == "sql":
            is_valid, error = await engine.validate_sql_script(config.script)
        else:
            raise HTTPException(status_code=400, detail="Invalid script type. Must be 'python' or 'sql'")
        
        return {
            "status": "success" if is_valid else "error",
            "valid": is_valid,
            "error": error,
            "script_type": config.script_type
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/functions")
async def get_available_functions() -> Any:
    """Get list of available transformation functions and their parameters"""
    return {
        "filter_operators": [op.value for op in FilterOperator],
        "aggregate_functions": [func.value for func in AggregateFunction],
        "join_types": [jt.value for jt in JoinType],
        "transformation_types": [tt.value for tt in TransformationType],
        "cleaning_rules": [
            "trim", "remove_special", "lowercase", "uppercase",
            "remove_numbers", "remove_spaces", "normalize_whitespace",
            "remove_punctuation", "remove_html", "remove_urls"
        ],
        "type_conversions": [
            "integer", "float", "string", "date", "datetime", 
            "boolean", "json", "array"
        ],
        "date_formats": [
            "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d",
            "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"
        ]
    }