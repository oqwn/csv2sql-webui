from typing import Any, List, Dict, Optional, Union
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, field_validator
import json
from datetime import datetime

from app.db.session import get_db
from app.services.data_extraction.extraction_manager import DataExtractionManager
from app.services.data_extraction.realtime_sync_manager import RealTimeSyncManager
from app.models.data_source import DataSource, ExtractionJob, DataSourceType, ExtractionMode

router = APIRouter()
extraction_manager = DataExtractionManager()
sync_manager = RealTimeSyncManager()


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
    limit: int = 100,
    db: Session = Depends(get_db)
) -> List[DataSourceResponse]:
    """Get all data sources"""
    data_sources = db.query(DataSource).offset(skip).limit(limit).all()
    return [DataSourceResponse.model_validate(ds) for ds in data_sources]


@router.post("/")
async def create_data_source(
    data_source: DataSourceCreate,
    db: Session = Depends(get_db)
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
        db_data_source = DataSource(
            name=data_source.name,
            type=data_source.type.value,
            connection_config=data_source.connection_config,
            extraction_config=data_source.extraction_config,
            description=data_source.description
        )
        
        db.add(db_data_source)
        db.commit()
        db.refresh(db_data_source)
        
        return DataSourceResponse.model_validate(db_data_source)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{data_source_id}")
async def get_data_source(
    data_source_id: int,
    db: Session = Depends(get_db)
) -> DataSourceResponse:
    """Get a specific data source"""
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    return DataSourceResponse.model_validate(data_source)


@router.put("/{data_source_id}")
async def update_data_source(
    data_source_id: int,
    data_source_update: DataSourceUpdate,
    db: Session = Depends(get_db)
) -> DataSourceResponse:
    """Update a data source"""
    db_data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not db_data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Update fields
    update_data = data_source_update.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_data_source, field, value)
    
    # Test connection if config was updated
    if data_source_update.connection_config:
        test_result = await extraction_manager.test_connection(
            db_data_source.type,
            db_data_source.connection_config
        )
        
        if test_result.get("status") != "success":
            raise HTTPException(
                status_code=400,
                detail=f"Connection test failed: {test_result.get('error', 'Unknown error')}"
            )
    
    db.commit()
    db.refresh(db_data_source)
    return DataSourceResponse.model_validate(db_data_source)


@router.delete("/{data_source_id}")
async def delete_data_source(
    data_source_id: int,
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Delete a data source"""
    db_data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not db_data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    db.delete(db_data_source)
    db.commit()
    return {"message": "Data source deleted successfully"}


@router.post("/{data_source_id}/extract")
async def extract_data(
    data_source_id: int,
    job_request: ExtractionJobCreate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Start data extraction job"""
    # Get data source
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Create extraction job
    extraction_job = ExtractionJob(
        data_source_id=data_source_id,
        job_name=job_request.job_name,
        extraction_mode=job_request.extraction_mode.value,
        source_query=job_request.source_query,
        target_table=job_request.target_table,
        config=job_request.config
    )
    
    db.add(extraction_job)
    db.commit()
    db.refresh(extraction_job)
    
    # Start extraction in background
    background_tasks.add_task(
        run_extraction_job,
        extraction_job.id,
        data_source,
        job_request
    )
    
    return {
        "message": "Extraction job started",
        "job_id": extraction_job.id,
        "status": "running"
    }


@router.get("/{data_source_id}/jobs")
async def get_extraction_jobs(
    data_source_id: int,
    db: Session = Depends(get_db)
) -> List[ExtractionJobResponse]:
    """Get extraction jobs for a data source"""
    jobs = db.query(ExtractionJob).filter(
        ExtractionJob.data_source_id == data_source_id
    ).order_by(ExtractionJob.created_at.desc()).all()
    return [ExtractionJobResponse.model_validate(job) for job in jobs]


@router.get("/jobs/{job_id}")
async def get_extraction_job(
    job_id: int,
    db: Session = Depends(get_db)
) -> ExtractionJobResponse:
    """Get a specific extraction job"""
    job = db.query(ExtractionJob).filter(ExtractionJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Extraction job not found")
    return ExtractionJobResponse.model_validate(job)


async def run_extraction_job(
    job_id: int,
    data_source: DataSource,
    job_request: ExtractionJobCreate
):
    """Background task to run data extraction"""
    from app.db.session import SessionLocal
    
    db = SessionLocal()
    try:
        # Update job status
        job = db.query(ExtractionJob).filter(ExtractionJob.id == job_id).first()
        job.status = "running"
        job.started_at = datetime.utcnow()
        db.commit()
        
        # Run extraction
        extraction_config = job_request.config or {}
        extraction_config.update({
            "mode": job_request.extraction_mode.value,
            "chunk_size": extraction_config.get("chunk_size", 10000)
        })
        
        source_name = job_request.source_query or extraction_config.get("source_name", "")
        
        result = await extraction_manager.extract_and_load_data(
            db=db,
            data_source_type=data_source.type,
            connection_config=data_source.connection_config,
            source_name=source_name,
            target_table=job_request.target_table,
            extraction_config=extraction_config,
            create_table=True
        )
        
        # Update job with results
        if result.get("status") == "success":
            job.status = "completed"
            job.records_processed = result.get("records_processed", 0)
        else:
            job.status = "failed"
            job.error_message = result.get("error", "Unknown error")
        
        job.completed_at = datetime.utcnow()
        db.commit()
        
    except Exception as e:
        # Update job with error
        job = db.query(ExtractionJob).filter(ExtractionJob.id == job_id).first()
        job.status = "failed"
        job.error_message = str(e)
        job.completed_at = datetime.utcnow()
        db.commit()
        
    finally:
        db.close()


# Real-time sync endpoints
@router.post("/{data_source_id}/sync/start")
async def start_real_time_sync(
    data_source_id: int,
    sync_request: RealTimeSyncRequest,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Start real-time synchronization for a data source"""
    # Get data source
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Generate unique sync ID
    import uuid
    sync_id = f"{data_source.type}_{data_source_id}_{uuid.uuid4().hex[:8]}"
    
    try:
        result = await sync_manager.start_sync(
            sync_id=sync_id,
            data_source_type=data_source.type,
            connection_config=data_source.connection_config,
            source_name=sync_request.source_name,
            target_table=sync_request.target_table,
            target_db=db,
            sync_config=sync_request.sync_config
        )
        
        return result
        
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
    data_source_id: int,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Validate if data source supports real-time sync and check configuration"""
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    try:
        if data_source.type == 'mysql':
            from app.services.data_extraction.mysql_binlog_connector import MySQLBinlogConnector
            connector = MySQLBinlogConnector(data_source.connection_config)
            result = await connector.validate_binlog_configuration()
            
            if result['valid']:
                current_pos = await connector.get_current_binlog_position()
                result['current_position'] = current_pos
            
            return result
            
        elif data_source.type == 'mongodb':
            # MongoDB change streams require replica set
            return {
                'valid': True,
                'requirements': [
                    'MongoDB must be running as a replica set',
                    'User must have changeStream privileges'
                ],
                'supports_resume': True
            }
            
        elif data_source.type in ['kafka', 'rabbitmq']:
            return {
                'valid': True,
                'note': 'Message queues have inherent real-time capabilities',
                'streaming': True
            }
            
        else:
            return {
                'valid': False,
                'error': f'Real-time sync not supported for {data_source.type}'
            }
            
    except Exception as e:
        return {
            'valid': False,
            'error': str(e)
        }