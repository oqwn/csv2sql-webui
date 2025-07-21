from typing import Any, List, Dict, Optional, Union
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, field_validator
import json
from datetime import datetime
from enum import Enum

from app.services.data_extraction.extraction_manager import DataExtractionManager
from app.services.data_extraction.realtime_sync_manager import RealTimeSyncManager
from app.services.local_storage import local_storage

router = APIRouter()
extraction_manager = DataExtractionManager()
sync_manager = RealTimeSyncManager()


class DataSourceType(str, Enum):
    # Relational Databases
    MYSQL = "mysql"
    POSTGRESQL = "postgresql" 
    SQLITE = "sqlite"
    MSSQL = "mssql"
    ORACLE = "oracle"
    
    # NoSQL Databases
    MONGODB = "mongodb"
    REDIS = "redis"
    ELASTICSEARCH = "elasticsearch"
    HBASE = "hbase"
    
    # Message Queues
    KAFKA = "kafka"
    RABBITMQ = "rabbitmq"
    
    # APIs
    REST_API = "rest_api"
    GRAPHQL = "graphql"
    
    # Files
    CSV = "csv"
    EXCEL = "excel"
    JSON_FILE = "json_file"
    PARQUET = "parquet"


class ExtractionMode(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    REAL_TIME = "real_time"
    CHUNKED = "chunked"


class DataSourceCreate(BaseModel):
    name: str
    type: DataSourceType
    connection_config: Dict[str, Any]
    extraction_config: Optional[Dict[str, Any]] = None
    description: Optional[str] = None


class DataSourceUpdate(BaseModel):
    name: Optional[str] = None
    connection_config: Optional[Dict[str, Any]] = None
    extraction_config: Optional[Dict[str, Any]] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None


class ExtractionJobCreate(BaseModel):
    data_source_id: int
    job_name: str
    extraction_mode: ExtractionMode
    source_query: Optional[str] = None
    target_table: str
    config: Optional[Dict[str, Any]] = None


class ConnectionTestRequest(BaseModel):
    type: DataSourceType
    connection_config: Dict[str, Any]


class DataPreviewRequest(BaseModel):
    type: DataSourceType
    connection_config: Dict[str, Any]
    source_name: str
    limit: int = 100


class RealTimeSyncRequest(BaseModel):
    data_source_id: int
    source_name: str
    target_table: str
    sync_config: Optional[Dict[str, Any]] = None


class RealTimeSyncStopRequest(BaseModel):
    sync_id: str


class DataSourceResponse(BaseModel):
    id: int
    name: str
    type: str
    connection_config: Dict[str, Any]
    extraction_config: Optional[Dict[str, Any]] = None
    description: Optional[str] = None
    is_active: bool
    created_at: Union[str, datetime]
    updated_at: Union[str, datetime]
    last_sync_at: Optional[Union[str, datetime]] = None

    @field_validator('created_at', 'updated_at', 'last_sync_at', mode='before')
    def convert_datetime_to_string(cls, v):
        if isinstance(v, datetime):
            return v.isoformat()
        return v

    class Config:
        from_attributes = True


class ExtractionJobResponse(BaseModel):
    id: int
    data_source_id: int
    job_name: str
    extraction_mode: str
    source_query: Optional[str] = None
    target_table: str
    status: str
    records_processed: int
    error_message: Optional[str] = None
    started_at: Optional[Union[str, datetime]] = None
    completed_at: Optional[Union[str, datetime]] = None
    created_at: Union[str, datetime]
    config: Optional[Dict[str, Any]] = None

    @field_validator('started_at', 'completed_at', 'created_at', mode='before')
    def convert_datetime_to_string(cls, v):
        if isinstance(v, datetime):
            return v.isoformat()
        return v

    class Config:
        from_attributes = True


@router.get("/supported")
async def get_supported_data_sources() -> List[Dict[str, Any]]:
    """Get list of supported data source types"""
    return extraction_manager.get_supported_data_sources()


@router.post("/test-connection")
async def test_data_source_connection(request: ConnectionTestRequest) -> Dict[str, Any]:
    """Test connection to a data source"""
    try:
        result = await extraction_manager.test_connection(
            request.type.value,
            request.connection_config
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/schema")
async def get_data_source_schema(request: ConnectionTestRequest) -> List[Dict[str, Any]]:
    """Get schema information from data source"""
    try:
        schema_info = await extraction_manager.get_schema_info(
            request.type.value,
            request.connection_config
        )
        return schema_info
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/preview")
async def preview_data_source(request: DataPreviewRequest) -> Dict[str, Any]:
    """Get preview of data from source"""
    try:
        preview_data = await extraction_manager.get_data_preview(
            request.type.value,
            request.connection_config,
            request.source_name,
            request.limit
        )
        return preview_data
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/incremental-info")
async def get_incremental_extraction_info(
    request: DataPreviewRequest
) -> Dict[str, Any]:
    """Get information for setting up incremental extraction"""
    try:
        info = await extraction_manager.get_incremental_extraction_info(
            request.type.value,
            request.connection_config,
            request.source_name
        )
        return info
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/")
async def get_data_sources(
    skip: int = 0,
    limit: int = 100
) -> List[DataSourceResponse]:
    """Get all data sources"""
    data_sources = local_storage.get_data_sources(skip, limit)
    return [DataSourceResponse(**ds) for ds in data_sources]


@router.post("/")
async def create_data_source(
    data_source: DataSourceCreate
) -> DataSourceResponse:
    """Create a new data source"""
    try:
        # Test connection before saving
        test_result = await extraction_manager.test_connection(
            data_source.type.value,
            data_source.connection_config
        )
        
        if test_result.get("status") != "success":
            raise HTTPException(
                status_code=400,
                detail=f"Connection test failed: {test_result.get('error', 'Unknown error')}"
            )
        
        # Create data source
        ds_data = {
            "name": data_source.name,
            "type": data_source.type.value,
            "connection_config": data_source.connection_config,
            "extraction_config": data_source.extraction_config,
            "description": data_source.description,
            "is_active": True
        }
        
        created_ds = local_storage.create_data_source(ds_data)
        return DataSourceResponse(**created_ds)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{data_source_id}")
async def get_data_source(
    data_source_id: int
) -> DataSourceResponse:
    """Get a specific data source"""
    data_source = local_storage.get_data_source(data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    return DataSourceResponse(**data_source)


@router.put("/{data_source_id}")
async def update_data_source(
    data_source_id: int,
    data_source_update: DataSourceUpdate
) -> DataSourceResponse:
    """Update a data source"""
    db_data_source = local_storage.get_data_source(data_source_id)
    if not db_data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Test connection if config was updated
    if data_source_update.connection_config:
        test_result = await extraction_manager.test_connection(
            db_data_source['type'],
            data_source_update.connection_config
        )
        
        if test_result.get("status") != "success":
            raise HTTPException(
                status_code=400,
                detail=f"Connection test failed: {test_result.get('error', 'Unknown error')}"
            )
    
    # Update data source
    updates = data_source_update.dict(exclude_unset=True)
    updated_ds = local_storage.update_data_source(data_source_id, updates)
    
    if not updated_ds:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    return DataSourceResponse(**updated_ds)


@router.delete("/{data_source_id}")
async def delete_data_source(
    data_source_id: int
) -> Dict[str, str]:
    """Delete a data source"""
    success = local_storage.delete_data_source(data_source_id)
    if not success:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    return {"message": "Data source deleted successfully"}


@router.post("/{data_source_id}/extract")
async def extract_data(
    data_source_id: int,
    job_request: ExtractionJobCreate,
    background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    """Start data extraction job"""
    # Get data source
    data_source = local_storage.get_data_source(data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Create extraction job
    job_data = {
        "data_source_id": data_source_id,
        "job_name": job_request.job_name,
        "extraction_mode": job_request.extraction_mode.value,
        "source_query": job_request.source_query,
        "target_table": job_request.target_table,
        "config": job_request.config
    }
    
    extraction_job = local_storage.create_extraction_job(job_data)
    
    # Start extraction in background
    background_tasks.add_task(
        run_extraction_job,
        extraction_job['id'],
        data_source,
        job_request
    )
    
    return {
        "message": "Extraction job started",
        "job_id": extraction_job['id'],
        "status": "running"
    }


@router.get("/{data_source_id}/jobs")
async def get_extraction_jobs(
    data_source_id: int
) -> List[ExtractionJobResponse]:
    """Get extraction jobs for a data source"""
    jobs = local_storage.get_extraction_jobs(data_source_id)
    return [ExtractionJobResponse(**job) for job in jobs]


@router.get("/jobs/{job_id}")
async def get_extraction_job(
    job_id: int
) -> ExtractionJobResponse:
    """Get a specific extraction job"""
    job = local_storage.get_extraction_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Extraction job not found")
    return ExtractionJobResponse(**job)


async def run_extraction_job(
    job_id: int,
    data_source: Dict[str, Any],
    job_request: ExtractionJobCreate
):
    """Background task to run data extraction"""
    try:
        # Update job status
        local_storage.update_extraction_job(job_id, {
            "status": "running",
            "started_at": datetime.utcnow().isoformat()
        })
        
        # Run extraction
        extraction_config = job_request.config or {}
        extraction_config.update({
            "mode": job_request.extraction_mode.value,
            "chunk_size": extraction_config.get("chunk_size", 10000)
        })
        
        source_name = job_request.source_query or extraction_config.get("source_name", "")
        
        # Note: This would need to be modified to extract to the connected data source
        # instead of a local database. For now, just mark as completed.
        result = {
            "status": "success",
            "records_processed": 0,
            "message": "Extraction completed (simulated - no local database)"
        }
        
        # Update job with results
        if result.get("status") == "success":
            local_storage.update_extraction_job(job_id, {
                "status": "completed",
                "records_processed": result.get("records_processed", 0),
                "completed_at": datetime.utcnow().isoformat()
            })
        else:
            local_storage.update_extraction_job(job_id, {
                "status": "failed",
                "error_message": result.get("error", "Unknown error"),
                "completed_at": datetime.utcnow().isoformat()
            })
        
    except Exception as e:
        # Update job with error
        local_storage.update_extraction_job(job_id, {
            "status": "failed",
            "error_message": str(e),
            "completed_at": datetime.utcnow().isoformat()
        })


# Real-time sync endpoints
@router.post("/{data_source_id}/sync/start")
async def start_real_time_sync(
    data_source_id: int,
    sync_request: RealTimeSyncRequest
) -> Dict[str, Any]:
    """Start real-time synchronization for a data source"""
    # Get data source
    data_source = local_storage.get_data_source(data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Generate unique sync ID
    import uuid
    sync_id = f"{data_source['type']}_{data_source_id}_{uuid.uuid4().hex[:8]}"
    
    try:
        # Note: Real-time sync would need to sync between data sources
        # For now, just return a simulated response
        return {
            "sync_id": sync_id,
            "status": "started",
            "message": "Real-time sync started (simulated - no local database)"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/sync/stop")
async def stop_real_time_sync(
    stop_request: RealTimeSyncStopRequest
) -> Dict[str, Any]:
    """Stop real-time synchronization"""
    try:
        result = await sync_manager.stop_sync(stop_request.sync_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/sync/status")
async def get_sync_status(sync_id: Optional[str] = None) -> Dict[str, Any]:
    """Get status of real-time synchronization(s)"""
    return sync_manager.get_sync_status(sync_id)


@router.post("/{data_source_id}/validate-realtime")
async def validate_real_time_config(
    data_source_id: int
) -> Dict[str, Any]:
    """Validate if data source supports real-time sync and check configuration"""
    data_source = local_storage.get_data_source(data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    try:
        if data_source['type'] == 'mysql':
            from app.services.data_extraction.mysql_binlog_connector import MySQLBinlogConnector
            connector = MySQLBinlogConnector(data_source['connection_config'])
            result = await connector.validate_binlog_configuration()
            
            if result['valid']:
                current_pos = await connector.get_current_binlog_position()
                result['current_position'] = current_pos
            
            return result
            
        elif data_source['type'] == 'mongodb':
            # MongoDB change streams require replica set
            return {
                'valid': True,
                'requirements': [
                    'MongoDB must be running as a replica set',
                    'User must have changeStream privileges'
                ],
                'supports_resume': True
            }
            
        elif data_source['type'] in ['kafka', 'rabbitmq']:
            return {
                'valid': True,
                'note': 'Message queues have inherent real-time capabilities',
                'streaming': True
            }
            
        else:
            return {
                'valid': False,
                'error': f'Real-time sync not supported for {data_source["type"]}'
            }
            
    except Exception as e:
        return {
            'valid': False,
            'error': str(e)
        }