"""
Job Scheduling System with Cron Expression Support
Handles ETL job scheduling, execution, and monitoring
"""

import asyncio
import uuid
from typing import Dict, List, Any, Optional, Callable, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import logging
from croniter import croniter
import json

from app.services.local_storage import LocalStorage
from app.services.transaction_manager import transaction_manager
from app.services.transformation_engine import TransformationEngine
from app.services.checkpoint_manager import checkpoint_manager

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    """Job execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class JobType(Enum):
    """Types of scheduled jobs"""
    TRANSFORMATION_PIPELINE = "transformation_pipeline"
    DATA_EXTRACTION = "data_extraction"
    DATA_LOADING = "data_loading"
    DATA_EXPORT = "data_export"
    MAINTENANCE = "maintenance"
    CUSTOM_SCRIPT = "custom_script"


class TriggerType(Enum):
    """Types of job triggers"""
    CRON = "cron"
    INTERVAL = "interval"
    ONE_TIME = "one_time"
    EVENT_BASED = "event_based"


@dataclass
class JobExecution:
    """Represents a single job execution"""
    execution_id: str
    job_id: str
    status: JobStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    logs: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScheduledJob:
    """Represents a scheduled job"""
    job_id: str
    name: str
    description: str
    job_type: JobType
    trigger_type: TriggerType
    cron_expression: Optional[str]
    interval_seconds: Optional[int]
    scheduled_time: Optional[datetime]
    is_active: bool
    created_at: datetime
    updated_at: datetime
    created_by: str
    
    # Job configuration
    configuration: Dict[str, Any] = field(default_factory=dict)
    
    # Execution tracking
    last_execution: Optional[datetime] = None
    next_execution: Optional[datetime] = None
    execution_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    
    # Retry configuration
    max_retries: int = 3
    retry_delay: int = 60  # seconds
    
    # Timeout configuration
    timeout_seconds: Optional[int] = None
    
    # Dependencies
    dependencies: List[str] = field(default_factory=list)
    
    # Metadata
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class JobScheduler:
    """Advanced job scheduler with cron expression support"""
    
    def __init__(self):
        self.jobs: Dict[str, ScheduledJob] = {}
        self.executions: Dict[str, JobExecution] = {}
        self.storage = LocalStorage()
        self.is_running = False
        self.scheduler_task: Optional[asyncio.Task] = None
        self.transformation_engine = TransformationEngine(transaction_manager)
        
        # Load existing jobs
        self._load_jobs()
    
    def create_job(self,
                  name: str,
                  description: str,
                  job_type: JobType,
                  configuration: Dict[str, Any],
                  trigger_type: TriggerType = TriggerType.CRON,
                  cron_expression: Optional[str] = None,
                  interval_seconds: Optional[int] = None,
                  scheduled_time: Optional[datetime] = None,
                  created_by: str = "system",
                  max_retries: int = 3,
                  timeout_seconds: Optional[int] = None,
                  dependencies: Optional[List[str]] = None,
                  tags: Optional[List[str]] = None,
                  metadata: Optional[Dict[str, Any]] = None) -> str:
        """Create a new scheduled job"""
        
        job_id = str(uuid.uuid4())
        
        # Validate trigger configuration
        if trigger_type == TriggerType.CRON and not cron_expression:
            raise ValueError("Cron expression required for cron trigger")
        if trigger_type == TriggerType.INTERVAL and not interval_seconds:
            raise ValueError("Interval seconds required for interval trigger")
        if trigger_type == TriggerType.ONE_TIME and not scheduled_time:
            raise ValueError("Scheduled time required for one-time trigger")
        
        # Validate cron expression if provided
        if cron_expression:
            try:
                croniter(cron_expression)
            except Exception as e:
                raise ValueError(f"Invalid cron expression: {str(e)}")
        
        # Calculate next execution time
        next_execution = self._calculate_next_execution(
            trigger_type, cron_expression, interval_seconds, scheduled_time
        )
        
        job = ScheduledJob(
            job_id=job_id,
            name=name,
            description=description,
            job_type=job_type,
            trigger_type=trigger_type,
            cron_expression=cron_expression,
            interval_seconds=interval_seconds,
            scheduled_time=scheduled_time,
            is_active=True,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            created_by=created_by,
            configuration=configuration,
            next_execution=next_execution,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
            dependencies=dependencies or [],
            tags=tags or [],
            metadata=metadata or {}
        )
        
        self.jobs[job_id] = job
        self._save_job(job)
        
        logger.info(f"Created scheduled job {job_id}: {name}")
        return job_id
    
    def update_job(self,
                  job_id: str,
                  **updates) -> bool:
        """Update an existing job"""
        
        if job_id not in self.jobs:
            return False
        
        job = self.jobs[job_id]
        
        # Update fields
        for key, value in updates.items():
            if hasattr(job, key):
                setattr(job, key, value)
        
        job.updated_at = datetime.now()
        
        # Recalculate next execution if schedule changed
        if any(key in updates for key in ['cron_expression', 'interval_seconds', 'scheduled_time']):
            job.next_execution = self._calculate_next_execution(
                job.trigger_type, job.cron_expression, job.interval_seconds, job.scheduled_time
            )
        
        self._save_job(job)
        logger.info(f"Updated job {job_id}: {job.name}")
        return True
    
    def delete_job(self, job_id: str) -> bool:
        """Delete a scheduled job"""
        if job_id not in self.jobs:
            return False
        
        job = self.jobs[job_id]
        del self.jobs[job_id]
        
        # Remove from storage
        try:
            jobs_data = self.storage.read_file("scheduled_jobs.json", {})
            if job_id in jobs_data:
                del jobs_data[job_id]
                self.storage.write_file("scheduled_jobs.json", jobs_data)
        except Exception as e:
            logger.error(f"Error removing job from storage: {e}")
        
        logger.info(f"Deleted job {job_id}: {job.name}")
        return True
    
    def get_job(self, job_id: str) -> Optional[ScheduledJob]:
        """Get a job by ID"""
        return self.jobs.get(job_id)
    
    def list_jobs(self, 
                 active_only: bool = False,
                 job_type: Optional[JobType] = None,
                 tags: Optional[List[str]] = None) -> List[ScheduledJob]:
        """List jobs with optional filtering"""
        jobs = list(self.jobs.values())
        
        if active_only:
            jobs = [job for job in jobs if job.is_active]
        
        if job_type:
            jobs = [job for job in jobs if job.job_type == job_type]
        
        if tags:
            jobs = [job for job in jobs if any(tag in job.tags for tag in tags)]
        
        return jobs
    
    def pause_job(self, job_id: str) -> bool:
        """Pause a job"""
        if job_id not in self.jobs:
            return False
        
        self.jobs[job_id].is_active = False
        self.jobs[job_id].updated_at = datetime.now()
        self._save_job(self.jobs[job_id])
        
        logger.info(f"Paused job {job_id}")
        return True
    
    def resume_job(self, job_id: str) -> bool:
        """Resume a paused job"""
        if job_id not in self.jobs:
            return False
        
        job = self.jobs[job_id]
        job.is_active = True
        job.updated_at = datetime.now()
        
        # Recalculate next execution
        job.next_execution = self._calculate_next_execution(
            job.trigger_type, job.cron_expression, job.interval_seconds, job.scheduled_time
        )
        
        self._save_job(job)
        logger.info(f"Resumed job {job_id}")
        return True
    
    def trigger_job_now(self, job_id: str) -> Optional[str]:
        """Manually trigger a job execution"""
        if job_id not in self.jobs:
            return None
        
        job = self.jobs[job_id]
        execution_id = str(uuid.uuid4())
        
        # Create execution record
        execution = JobExecution(
            execution_id=execution_id,
            job_id=job_id,
            status=JobStatus.PENDING,
            start_time=datetime.now()
        )
        
        self.executions[execution_id] = execution
        
        # Execute the job
        asyncio.create_task(self._execute_job(job, execution))
        
        return execution_id
    
    async def start_scheduler(self):
        """Start the job scheduler"""
        if self.is_running:
            return
        
        self.is_running = True
        self.scheduler_task = asyncio.create_task(self._scheduler_loop())
        logger.info("Job scheduler started")
    
    async def stop_scheduler(self):
        """Stop the job scheduler"""
        self.is_running = False
        
        if self.scheduler_task:
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Job scheduler stopped")
    
    def get_execution(self, execution_id: str) -> Optional[JobExecution]:
        """Get job execution details"""
        return self.executions.get(execution_id)
    
    def get_job_executions(self, job_id: str, limit: int = 100) -> List[JobExecution]:
        """Get executions for a specific job"""
        executions = [exec for exec in self.executions.values() if exec.job_id == job_id]
        executions.sort(key=lambda x: x.start_time, reverse=True)
        return executions[:limit]
    
    def get_job_statistics(self, job_id: str) -> Dict[str, Any]:
        """Get statistics for a job"""
        if job_id not in self.jobs:
            return {}
        
        job = self.jobs[job_id]
        executions = self.get_job_executions(job_id)
        
        avg_duration = 0
        if executions:
            durations = []
            for exec in executions:
                if exec.end_time:
                    duration = (exec.end_time - exec.start_time).total_seconds()
                    durations.append(duration)
            
            if durations:
                avg_duration = sum(durations) / len(durations)
        
        return {
            "job_id": job_id,
            "job_name": job.name,
            "total_executions": job.execution_count,
            "successful_executions": job.success_count,
            "failed_executions": job.failure_count,
            "success_rate": job.success_count / max(job.execution_count, 1) * 100,
            "last_execution": job.last_execution.isoformat() if job.last_execution else None,
            "next_execution": job.next_execution.isoformat() if job.next_execution else None,
            "average_duration_seconds": avg_duration,
            "is_active": job.is_active
        }
    
    async def _scheduler_loop(self):
        """Main scheduler loop"""
        while self.is_running:
            try:
                current_time = datetime.now()
                
                # Check for jobs that need to be executed
                for job in self.jobs.values():
                    if not job.is_active:
                        continue
                    
                    if job.next_execution and job.next_execution <= current_time:
                        # Check dependencies
                        if self._check_dependencies(job):
                            # Create execution
                            execution_id = str(uuid.uuid4())
                            execution = JobExecution(
                                execution_id=execution_id,
                                job_id=job.job_id,
                                status=JobStatus.PENDING,
                                start_time=current_time
                            )
                            
                            self.executions[execution_id] = execution
                            
                            # Execute job asynchronously
                            asyncio.create_task(self._execute_job(job, execution))
                            
                            # Update next execution time
                            job.next_execution = self._calculate_next_execution(
                                job.trigger_type, job.cron_expression, 
                                job.interval_seconds, job.scheduled_time
                            )
                            self._save_job(job)
                
                # Sleep for a short interval
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                await asyncio.sleep(60)  # Wait longer on error
    
    async def _execute_job(self, job: ScheduledJob, execution: JobExecution):
        """Execute a single job"""
        try:
            execution.status = JobStatus.RUNNING
            execution.logs.append(f"Started execution at {datetime.now().isoformat()}")
            
            logger.info(f"Executing job {job.job_id}: {job.name}")
            
            # Execute based on job type
            if job.job_type == JobType.TRANSFORMATION_PIPELINE:
                result = await self._execute_transformation_pipeline(job, execution)
            elif job.job_type == JobType.DATA_EXTRACTION:
                result = await self._execute_data_extraction(job, execution)
            elif job.job_type == JobType.DATA_LOADING:
                result = await self._execute_data_loading(job, execution)
            elif job.job_type == JobType.DATA_EXPORT:
                result = await self._execute_data_export(job, execution)
            elif job.job_type == JobType.MAINTENANCE:
                result = await self._execute_maintenance(job, execution)
            elif job.job_type == JobType.CUSTOM_SCRIPT:
                result = await self._execute_custom_script(job, execution)
            else:
                raise ValueError(f"Unknown job type: {job.job_type}")
            
            # Mark as completed
            execution.status = JobStatus.COMPLETED
            execution.end_time = datetime.now()
            execution.result = result
            
            # Update job statistics
            job.execution_count += 1
            job.success_count += 1
            job.last_execution = execution.start_time
            
            execution.logs.append(f"Completed successfully at {execution.end_time.isoformat()}")
            
        except Exception as e:
            # Mark as failed
            execution.status = JobStatus.FAILED
            execution.end_time = datetime.now()
            execution.error_message = str(e)
            
            # Update job statistics
            job.execution_count += 1
            job.failure_count += 1
            job.last_execution = execution.start_time
            
            execution.logs.append(f"Failed at {execution.end_time.isoformat()}: {str(e)}")
            
            logger.error(f"Job {job.job_id} failed: {str(e)}")
        
        finally:
            self._save_job(job)
    
    async def _execute_transformation_pipeline(self, job: ScheduledJob, execution: JobExecution) -> Dict[str, Any]:
        """Execute a transformation pipeline job"""
        config = job.configuration
        
        # Create transaction if enabled
        transaction_context = None
        if config.get('use_transactions', False):
            transaction_id = transaction_manager.create_transaction(config.get('data_source_id', 1))
            transaction_context = transaction_manager._active_transactions[transaction_id]
        
        try:
            # Execute transformations
            # This would integrate with the actual transformation pipeline execution
            result = {
                "pipeline_executed": True,
                "steps_executed": config.get('steps', []),
                "transaction_id": transaction_context.transaction_id if transaction_context else None
            }
            
            if transaction_context:
                await transaction_manager._commit_transaction(transaction_context)
            
            return result
            
        except Exception as e:
            if transaction_context:
                await transaction_manager._rollback_transaction(transaction_context, str(e))
            raise
    
    async def _execute_data_extraction(self, job: ScheduledJob, execution: JobExecution) -> Dict[str, Any]:
        """Execute a data extraction job"""
        config = job.configuration
        
        return {
            "extraction_completed": True,
            "source": config.get('source', 'unknown'),
            "records_extracted": config.get('expected_records', 0)
        }
    
    async def _execute_data_loading(self, job: ScheduledJob, execution: JobExecution) -> Dict[str, Any]:
        """Execute a data loading job"""
        config = job.configuration
        
        return {
            "loading_completed": True,
            "target": config.get('target', 'unknown'),
            "records_loaded": config.get('records', 0)
        }
    
    async def _execute_data_export(self, job: ScheduledJob, execution: JobExecution) -> Dict[str, Any]:
        """Execute a data export job"""
        config = job.configuration
        
        return {
            "export_completed": True,
            "format": config.get('format', 'csv'),
            "file_path": config.get('output_path', 'export.csv')
        }
    
    async def _execute_maintenance(self, job: ScheduledJob, execution: JobExecution) -> Dict[str, Any]:
        """Execute a maintenance job"""
        config = job.configuration
        
        # Example maintenance tasks
        if config.get('cleanup_checkpoints'):
            checkpoint_manager.cleanup_checkpoints('*', config.get('keep_checkpoints', 5))
        
        return {
            "maintenance_completed": True,
            "tasks_executed": list(config.keys())
        }
    
    async def _execute_custom_script(self, job: ScheduledJob, execution: JobExecution) -> Dict[str, Any]:
        """Execute a custom script job"""
        config = job.configuration
        script = config.get('script', '')
        
        if not script:
            raise ValueError("No script provided for custom script job")
        
        # Execute the custom script
        # This is a placeholder - in reality, you'd want proper sandboxing
        return {
            "script_executed": True,
            "script_length": len(script)
        }
    
    def _check_dependencies(self, job: ScheduledJob) -> bool:
        """Check if job dependencies are satisfied"""
        if not job.dependencies:
            return True
        
        for dep_job_id in job.dependencies:
            if dep_job_id not in self.jobs:
                continue
            
            # Check if dependency job completed successfully recently
            dep_executions = self.get_job_executions(dep_job_id, 1)
            if not dep_executions or dep_executions[0].status != JobStatus.COMPLETED:
                return False
        
        return True
    
    def _calculate_next_execution(self,
                                 trigger_type: TriggerType,
                                 cron_expression: Optional[str],
                                 interval_seconds: Optional[int],
                                 scheduled_time: Optional[datetime]) -> Optional[datetime]:
        """Calculate next execution time"""
        
        current_time = datetime.now()
        
        if trigger_type == TriggerType.CRON and cron_expression:
            cron = croniter(cron_expression, current_time)
            return cron.get_next(datetime)
        
        elif trigger_type == TriggerType.INTERVAL and interval_seconds:
            return current_time + timedelta(seconds=interval_seconds)
        
        elif trigger_type == TriggerType.ONE_TIME and scheduled_time:
            return scheduled_time if scheduled_time > current_time else None
        
        return None
    
    def _save_job(self, job: ScheduledJob):
        """Save job to storage"""
        try:
            jobs_data = self.storage.read_file("scheduled_jobs.json", {})
            
            job_data = {
                "job_id": job.job_id,
                "name": job.name,
                "description": job.description,
                "job_type": job.job_type.value,
                "trigger_type": job.trigger_type.value,
                "cron_expression": job.cron_expression,
                "interval_seconds": job.interval_seconds,
                "scheduled_time": job.scheduled_time.isoformat() if job.scheduled_time else None,
                "is_active": job.is_active,
                "created_at": job.created_at.isoformat(),
                "updated_at": job.updated_at.isoformat(),
                "created_by": job.created_by,
                "configuration": job.configuration,
                "last_execution": job.last_execution.isoformat() if job.last_execution else None,
                "next_execution": job.next_execution.isoformat() if job.next_execution else None,
                "execution_count": job.execution_count,
                "success_count": job.success_count,
                "failure_count": job.failure_count,
                "max_retries": job.max_retries,
                "retry_delay": job.retry_delay,
                "timeout_seconds": job.timeout_seconds,
                "dependencies": job.dependencies,
                "tags": job.tags,
                "metadata": job.metadata
            }
            
            jobs_data[job.job_id] = job_data
            self.storage.write_file("scheduled_jobs.json", jobs_data)
            
        except Exception as e:
            logger.error(f"Error saving job to storage: {e}")
    
    def _load_jobs(self):
        """Load jobs from storage"""
        try:
            jobs_data = self.storage.read_file("scheduled_jobs.json", {})
            
            for job_id, job_data in jobs_data.items():
                job = ScheduledJob(
                    job_id=job_data["job_id"],
                    name=job_data["name"],
                    description=job_data["description"],
                    job_type=JobType(job_data["job_type"]),
                    trigger_type=TriggerType(job_data["trigger_type"]),
                    cron_expression=job_data.get("cron_expression"),
                    interval_seconds=job_data.get("interval_seconds"),
                    scheduled_time=datetime.fromisoformat(job_data["scheduled_time"]) if job_data.get("scheduled_time") else None,
                    is_active=job_data["is_active"],
                    created_at=datetime.fromisoformat(job_data["created_at"]),
                    updated_at=datetime.fromisoformat(job_data["updated_at"]),
                    created_by=job_data["created_by"],
                    configuration=job_data.get("configuration", {}),
                    last_execution=datetime.fromisoformat(job_data["last_execution"]) if job_data.get("last_execution") else None,
                    next_execution=datetime.fromisoformat(job_data["next_execution"]) if job_data.get("next_execution") else None,
                    execution_count=job_data.get("execution_count", 0),
                    success_count=job_data.get("success_count", 0),
                    failure_count=job_data.get("failure_count", 0),
                    max_retries=job_data.get("max_retries", 3),
                    retry_delay=job_data.get("retry_delay", 60),
                    timeout_seconds=job_data.get("timeout_seconds"),
                    dependencies=job_data.get("dependencies", []),
                    tags=job_data.get("tags", []),
                    metadata=job_data.get("metadata", {})
                )
                
                self.jobs[job_id] = job
            
            logger.info(f"Loaded {len(self.jobs)} scheduled jobs")
            
        except Exception as e:
            logger.error(f"Error loading jobs from storage: {e}")


# Global job scheduler instance
job_scheduler = JobScheduler()