"""
Job Scheduling API endpoints
"""

from typing import Dict, List, Any, Optional
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel
from datetime import datetime

from app.services.job_scheduler import job_scheduler, JobType, TriggerType, JobStatus
from app.api.deps import get_current_user

router = APIRouter()


class JobCreateRequest(BaseModel):
    """Request model for creating a scheduled job"""
    name: str
    description: str
    job_type: str
    configuration: Dict[str, Any]
    trigger_type: str = "cron"
    cron_expression: Optional[str] = None
    interval_seconds: Optional[int] = None
    scheduled_time: Optional[str] = None  # ISO format
    max_retries: int = 3
    timeout_seconds: Optional[int] = None
    dependencies: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None


class JobUpdateRequest(BaseModel):
    """Request model for updating a job"""
    name: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None
    cron_expression: Optional[str] = None
    interval_seconds: Optional[int] = None
    max_retries: Optional[int] = None
    timeout_seconds: Optional[int] = None
    configuration: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None


class JobResponse(BaseModel):
    """Response model for job information"""
    job_id: str
    name: str
    description: str
    job_type: str
    trigger_type: str
    cron_expression: Optional[str]
    interval_seconds: Optional[int]
    is_active: bool
    created_at: str
    updated_at: str
    created_by: str
    last_execution: Optional[str]
    next_execution: Optional[str]
    execution_count: int
    success_count: int
    failure_count: int
    tags: List[str]


@router.post("/create", response_model=Dict[str, str])
async def create_job(
    request: JobCreateRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    """Create a new scheduled job"""
    try:
        # Parse datetime if provided
        scheduled_time = None
        if request.scheduled_time:
            scheduled_time = datetime.fromisoformat(request.scheduled_time)
        
        # Convert string enums
        job_type = JobType(request.job_type)
        trigger_type = TriggerType(request.trigger_type)
        
        job_id = job_scheduler.create_job(
            name=request.name,
            description=request.description,
            job_type=job_type,
            configuration=request.configuration,
            trigger_type=trigger_type,
            cron_expression=request.cron_expression,
            interval_seconds=request.interval_seconds,
            scheduled_time=scheduled_time,
            created_by=current_user.get("username", "unknown"),
            max_retries=request.max_retries,
            timeout_seconds=request.timeout_seconds,
            dependencies=request.dependencies,
            tags=request.tags,
            metadata=request.metadata
        )
        
        # Start scheduler if not already running
        if not job_scheduler.is_running:
            background_tasks.add_task(job_scheduler.start_scheduler)
        
        return {
            "job_id": job_id,
            "message": "Job created successfully",
            "name": request.name
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create job: {str(e)}")


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get job details"""
    job = job_scheduler.get_job(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return JobResponse(
        job_id=job.job_id,
        name=job.name,
        description=job.description,
        job_type=job.job_type.value,
        trigger_type=job.trigger_type.value,
        cron_expression=job.cron_expression,
        interval_seconds=job.interval_seconds,
        is_active=job.is_active,
        created_at=job.created_at.isoformat(),
        updated_at=job.updated_at.isoformat(),
        created_by=job.created_by,
        last_execution=job.last_execution.isoformat() if job.last_execution else None,
        next_execution=job.next_execution.isoformat() if job.next_execution else None,
        execution_count=job.execution_count,
        success_count=job.success_count,
        failure_count=job.failure_count,
        tags=job.tags
    )


@router.get("/")
async def list_jobs(
    active_only: bool = False,
    job_type: Optional[str] = None,
    tags: Optional[str] = None,  # Comma-separated tags
    current_user: dict = Depends(get_current_user)
):
    """List all jobs with optional filtering"""
    try:
        # Parse filters
        job_type_filter = JobType(job_type) if job_type else None
        tags_filter = tags.split(",") if tags else None
        
        jobs = job_scheduler.list_jobs(
            active_only=active_only,
            job_type=job_type_filter,
            tags=tags_filter
        )
        
        job_list = []
        for job in jobs:
            job_list.append({
                "job_id": job.job_id,
                "name": job.name,
                "description": job.description,
                "job_type": job.job_type.value,
                "trigger_type": job.trigger_type.value,
                "is_active": job.is_active,
                "created_at": job.created_at.isoformat(),
                "last_execution": job.last_execution.isoformat() if job.last_execution else None,
                "next_execution": job.next_execution.isoformat() if job.next_execution else None,
                "execution_count": job.execution_count,
                "success_count": job.success_count,
                "failure_count": job.failure_count,
                "success_rate": job.success_count / max(job.execution_count, 1) * 100,
                "tags": job.tags
            })
        
        return {
            "jobs": job_list,
            "count": len(job_list),
            "filters": {
                "active_only": active_only,
                "job_type": job_type,
                "tags": tags_filter
            }
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list jobs: {str(e)}")


@router.put("/{job_id}")
async def update_job(
    job_id: str,
    request: JobUpdateRequest,
    current_user: dict = Depends(get_current_user)
):
    """Update an existing job"""
    try:
        # Prepare updates
        updates = {}
        for field, value in request.dict(exclude_unset=True).items():
            if value is not None:
                updates[field] = value
        
        success = job_scheduler.update_job(job_id, **updates)
        
        if not success:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return {
            "message": "Job updated successfully",
            "job_id": job_id,
            "updated_fields": list(updates.keys())
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update job: {str(e)}")


@router.delete("/{job_id}")
async def delete_job(
    job_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Delete a scheduled job"""
    try:
        success = job_scheduler.delete_job(job_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return {
            "message": "Job deleted successfully",
            "job_id": job_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete job: {str(e)}")


@router.post("/{job_id}/pause")
async def pause_job(
    job_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Pause a job"""
    try:
        success = job_scheduler.pause_job(job_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return {
            "message": "Job paused successfully",
            "job_id": job_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to pause job: {str(e)}")


@router.post("/{job_id}/resume")
async def resume_job(
    job_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Resume a paused job"""
    try:
        success = job_scheduler.resume_job(job_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return {
            "message": "Job resumed successfully",
            "job_id": job_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to resume job: {str(e)}")


@router.post("/{job_id}/trigger")
async def trigger_job(
    job_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Manually trigger a job execution"""
    try:
        execution_id = job_scheduler.trigger_job_now(job_id)
        
        if not execution_id:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return {
            "message": "Job triggered successfully",
            "job_id": job_id,
            "execution_id": execution_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger job: {str(e)}")


@router.get("/{job_id}/statistics")
async def get_job_statistics(
    job_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get job execution statistics"""
    try:
        stats = job_scheduler.get_job_statistics(job_id)
        
        if not stats:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return stats
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get job statistics: {str(e)}")


@router.get("/{job_id}/executions")
async def get_job_executions(
    job_id: str,
    limit: int = 100,
    current_user: dict = Depends(get_current_user)
):
    """Get job execution history"""
    try:
        executions = job_scheduler.get_job_executions(job_id, limit)
        
        execution_list = []
        for execution in executions:
            execution_list.append({
                "execution_id": execution.execution_id,
                "job_id": execution.job_id,
                "status": execution.status.value,
                "start_time": execution.start_time.isoformat(),
                "end_time": execution.end_time.isoformat() if execution.end_time else None,
                "duration_seconds": (execution.end_time - execution.start_time).total_seconds() if execution.end_time else None,
                "error_message": execution.error_message,
                "result": execution.result,
                "logs": execution.logs
            })
        
        return {
            "job_id": job_id,
            "executions": execution_list,
            "count": len(execution_list),
            "limit": limit
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get job executions: {str(e)}")


@router.get("/execution/{execution_id}")
async def get_execution_details(
    execution_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get detailed execution information"""
    try:
        execution = job_scheduler.get_execution(execution_id)
        
        if not execution:
            raise HTTPException(status_code=404, detail="Execution not found")
        
        return {
            "execution_id": execution.execution_id,
            "job_id": execution.job_id,
            "status": execution.status.value,
            "start_time": execution.start_time.isoformat(),
            "end_time": execution.end_time.isoformat() if execution.end_time else None,
            "duration_seconds": (execution.end_time - execution.start_time).total_seconds() if execution.end_time else None,
            "result": execution.result,
            "error_message": execution.error_message,
            "logs": execution.logs,
            "metadata": execution.metadata
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get execution details: {str(e)}")


@router.post("/scheduler/start")
async def start_scheduler(
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    """Start the job scheduler"""
    try:
        if job_scheduler.is_running:
            return {"message": "Scheduler is already running"}
        
        background_tasks.add_task(job_scheduler.start_scheduler)
        
        return {"message": "Scheduler started successfully"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start scheduler: {str(e)}")


@router.post("/scheduler/stop")
async def stop_scheduler(
    current_user: dict = Depends(get_current_user)
):
    """Stop the job scheduler"""
    try:
        if not job_scheduler.is_running:
            return {"message": "Scheduler is not running"}
        
        await job_scheduler.stop_scheduler()
        
        return {"message": "Scheduler stopped successfully"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop scheduler: {str(e)}")


@router.get("/scheduler/status")
async def get_scheduler_status(
    current_user: dict = Depends(get_current_user)
):
    """Get scheduler status"""
    try:
        return {
            "is_running": job_scheduler.is_running,
            "total_jobs": len(job_scheduler.jobs),
            "active_jobs": len([job for job in job_scheduler.jobs.values() if job.is_active]),
            "total_executions": len(job_scheduler.executions)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get scheduler status: {str(e)}")


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "scheduler_running": job_scheduler.is_running,
        "jobs_count": len(job_scheduler.jobs),
        "executions_count": len(job_scheduler.executions)
    }