import json
import os
from typing import Dict, List, Any, Optional
from datetime import datetime
import uuid
from pathlib import Path
import threading
from app.core.config import settings

class LocalStorage:
    """Local file-based storage for data sources, extraction jobs, and transformation pipelines"""
    
    def __init__(self, storage_dir: str = None):
        self.storage_dir = Path(storage_dir or settings.DATA_DIR)
        self.data_sources_file = self.storage_dir / "data_sources.json"
        self.extraction_jobs_file = self.storage_dir / "extraction_jobs.json"
        self.transformation_pipelines_file = self.storage_dir / "transformation_pipelines.json"
        self._lock = threading.Lock()
        
        # Create storage directory if it doesn't exist
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize files if they don't exist
        if not self.data_sources_file.exists():
            self._write_json(self.data_sources_file, [])
        if not self.extraction_jobs_file.exists():
            self._write_json(self.extraction_jobs_file, [])
        if not self.transformation_pipelines_file.exists():
            self._write_json(self.transformation_pipelines_file, [])
    
    def _read_json(self, file_path: Path) -> List[Dict[str, Any]]:
        """Read JSON data from file"""
        with self._lock:
            try:
                with open(file_path, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                return []
    
    def _write_json(self, file_path: Path, data: List[Dict[str, Any]]) -> None:
        """Write JSON data to file"""
        with self._lock:
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
    
    # Data Source methods
    def get_data_sources(self, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all data sources with pagination"""
        data_sources = self._read_json(self.data_sources_file)
        return data_sources[skip:skip + limit]
    
    def get_data_source(self, data_source_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific data source by ID"""
        data_sources = self._read_json(self.data_sources_file)
        for ds in data_sources:
            if ds.get('id') == data_source_id:
                return ds
        return None
    
    def create_data_source(self, data_source: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new data source"""
        data_sources = self._read_json(self.data_sources_file)
        
        # Generate new ID
        max_id = max([ds.get('id', 0) for ds in data_sources], default=0)
        data_source['id'] = max_id + 1
        
        # Add timestamps
        now = datetime.utcnow().isoformat()
        data_source['created_at'] = now
        data_source['updated_at'] = now
        
        # Add to list and save
        data_sources.append(data_source)
        self._write_json(self.data_sources_file, data_sources)
        
        return data_source
    
    def update_data_source(self, data_source_id: int, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update an existing data source"""
        data_sources = self._read_json(self.data_sources_file)
        
        for i, ds in enumerate(data_sources):
            if ds.get('id') == data_source_id:
                # Update fields
                for key, value in updates.items():
                    if value is not None:
                        ds[key] = value
                
                # Update timestamp
                ds['updated_at'] = datetime.utcnow().isoformat()
                
                # Save
                self._write_json(self.data_sources_file, data_sources)
                return ds
        
        return None
    
    def delete_data_source(self, data_source_id: int) -> bool:
        """Delete a data source"""
        data_sources = self._read_json(self.data_sources_file)
        
        for i, ds in enumerate(data_sources):
            if ds.get('id') == data_source_id:
                data_sources.pop(i)
                self._write_json(self.data_sources_file, data_sources)
                return True
        
        return False
    
    # Extraction Job methods
    def get_extraction_jobs(self, data_source_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get extraction jobs, optionally filtered by data source"""
        jobs = self._read_json(self.extraction_jobs_file)
        
        if data_source_id is not None:
            jobs = [job for job in jobs if job.get('data_source_id') == data_source_id]
        
        # Sort by created_at descending
        jobs.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        return jobs
    
    def get_extraction_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific extraction job by ID"""
        jobs = self._read_json(self.extraction_jobs_file)
        for job in jobs:
            if job.get('id') == job_id:
                return job
        return None
    
    def create_extraction_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new extraction job"""
        jobs = self._read_json(self.extraction_jobs_file)
        
        # Generate new ID
        max_id = max([j.get('id', 0) for j in jobs], default=0)
        job['id'] = max_id + 1
        
        # Add timestamps
        job['created_at'] = datetime.utcnow().isoformat()
        job['status'] = job.get('status', 'pending')
        job['records_processed'] = job.get('records_processed', 0)
        
        # Add to list and save
        jobs.append(job)
        self._write_json(self.extraction_jobs_file, jobs)
        
        return job
    
    def update_extraction_job(self, job_id: int, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update an existing extraction job"""
        jobs = self._read_json(self.extraction_jobs_file)
        
        for i, job in enumerate(jobs):
            if job.get('id') == job_id:
                # Update fields
                for key, value in updates.items():
                    if value is not None:
                        job[key] = value
                
                # Save
                self._write_json(self.extraction_jobs_file, jobs)
                return job
        
        return None
    
    def clear_all(self) -> None:
        """Clear all data (for testing purposes)"""
        self._write_json(self.data_sources_file, [])
        self._write_json(self.extraction_jobs_file, [])
        self._write_json(self.transformation_pipelines_file, [])
    
    # Transformation Pipeline methods
    def get_transformation_pipelines(self) -> List[Dict[str, Any]]:
        """Get all transformation pipelines"""
        pipelines = self._read_json(self.transformation_pipelines_file)
        # Sort by updated_at descending
        pipelines.sort(key=lambda x: x.get('updated_at', ''), reverse=True)
        return pipelines
    
    def get_transformation_pipeline(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific transformation pipeline by ID"""
        pipelines = self._read_json(self.transformation_pipelines_file)
        for pipeline in pipelines:
            if pipeline.get('id') == pipeline_id:
                return pipeline
        return None
    
    def save_transformation_pipeline(self, pipeline: Dict[str, Any]) -> Dict[str, Any]:
        """Save or update a transformation pipeline"""
        pipelines = self._read_json(self.transformation_pipelines_file)
        
        # Check if pipeline exists
        existing_index = None
        for i, p in enumerate(pipelines):
            if p.get('id') == pipeline.get('id'):
                existing_index = i
                break
        
        if existing_index is not None:
            # Update existing pipeline
            pipelines[existing_index] = pipeline
        else:
            # Add new pipeline
            pipelines.append(pipeline)
        
        self._write_json(self.transformation_pipelines_file, pipelines)
        return pipeline
    
    def delete_transformation_pipeline(self, pipeline_id: str) -> bool:
        """Delete a transformation pipeline"""
        pipelines = self._read_json(self.transformation_pipelines_file)
        
        for i, pipeline in enumerate(pipelines):
            if pipeline.get('id') == pipeline_id:
                pipelines.pop(i)
                self._write_json(self.transformation_pipelines_file, pipelines)
                return True
        
        return False


# Global instance
local_storage = LocalStorage()