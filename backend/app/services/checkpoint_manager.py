"""
Checkpoint and Resume Management System
Provides capability to save pipeline state and resume from checkpoints
"""

import uuid
import pickle
import json
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import logging
import os
from pathlib import Path

from app.services.local_storage import LocalStorage

logger = logging.getLogger(__name__)


class CheckpointType(Enum):
    """Types of checkpoints"""
    PIPELINE_START = "pipeline_start"
    STEP_COMPLETE = "step_complete"
    DATA_SNAPSHOT = "data_snapshot"
    ERROR_RECOVERY = "error_recovery"
    MANUAL = "manual"


class CheckpointStatus(Enum):
    """Checkpoint status"""
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"


@dataclass
class CheckpointMetadata:
    """Metadata for a checkpoint"""
    checkpoint_id: str
    checkpoint_type: CheckpointType
    status: CheckpointStatus
    pipeline_id: str
    step_index: int
    step_name: str
    timestamp: datetime
    data_hash: Optional[str] = None
    data_rows: Optional[int] = None
    data_columns: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    

@dataclass
class PipelineState:
    """Complete state of a pipeline execution"""
    pipeline_id: str
    execution_id: str
    current_step: int
    total_steps: int
    data_snapshot: Optional[pd.DataFrame]
    intermediate_results: Dict[str, Any]
    configuration: Dict[str, Any]
    start_time: datetime
    last_update: datetime
    status: str
    error_message: Optional[str] = None


class CheckpointManager:
    """Manager for pipeline checkpoints and resume capability"""
    
    def __init__(self, storage_path: Optional[str] = None):
        self.storage = LocalStorage()
        self.storage_path = storage_path or os.path.join(os.getcwd(), "checkpoints")
        self.checkpoints: Dict[str, CheckpointMetadata] = {}
        self.pipeline_states: Dict[str, PipelineState] = {}
        
        # Ensure storage directory exists
        Path(self.storage_path).mkdir(parents=True, exist_ok=True)
        
        # Load existing checkpoints
        self._load_checkpoints()
    
    def create_checkpoint(self, 
                         pipeline_id: str, 
                         step_index: int,
                         step_name: str,
                         data: Optional[pd.DataFrame] = None,
                         checkpoint_type: CheckpointType = CheckpointType.STEP_COMPLETE,
                         metadata: Optional[Dict[str, Any]] = None) -> str:
        """Create a new checkpoint"""
        
        checkpoint_id = str(uuid.uuid4())
        
        # Calculate data hash if data provided
        data_hash = None
        data_rows = None
        data_columns = None
        
        if data is not None:
            data_hash = self._calculate_data_hash(data)
            data_rows = len(data)
            data_columns = len(data.columns)
            
            # Save data snapshot
            self._save_data_snapshot(checkpoint_id, data)
        
        # Create checkpoint metadata
        checkpoint_meta = CheckpointMetadata(
            checkpoint_id=checkpoint_id,
            checkpoint_type=checkpoint_type,
            status=CheckpointStatus.ACTIVE,
            pipeline_id=pipeline_id,
            step_index=step_index,
            step_name=step_name,
            timestamp=datetime.now(),
            data_hash=data_hash,
            data_rows=data_rows,
            data_columns=data_columns,
            metadata=metadata or {}
        )
        
        # Store checkpoint
        self.checkpoints[checkpoint_id] = checkpoint_meta
        self._save_checkpoint_metadata(checkpoint_meta)
        
        logger.info(f"Created checkpoint {checkpoint_id} for pipeline {pipeline_id}, step {step_index}: {step_name}")
        
        return checkpoint_id
    
    def save_pipeline_state(self, 
                           pipeline_id: str,
                           execution_id: str,
                           current_step: int,
                           total_steps: int,
                           data_snapshot: Optional[pd.DataFrame] = None,
                           intermediate_results: Optional[Dict[str, Any]] = None,
                           configuration: Optional[Dict[str, Any]] = None,
                           status: str = "running") -> str:
        """Save complete pipeline state"""
        
        state_id = f"{pipeline_id}_{execution_id}"
        
        pipeline_state = PipelineState(
            pipeline_id=pipeline_id,
            execution_id=execution_id,
            current_step=current_step,
            total_steps=total_steps,
            data_snapshot=data_snapshot,
            intermediate_results=intermediate_results or {},
            configuration=configuration or {},
            start_time=datetime.now() if state_id not in self.pipeline_states else self.pipeline_states[state_id].start_time,
            last_update=datetime.now(),
            status=status
        )
        
        self.pipeline_states[state_id] = pipeline_state
        self._save_pipeline_state(pipeline_state)
        
        return state_id
    
    def load_pipeline_state(self, pipeline_id: str, execution_id: str) -> Optional[PipelineState]:
        """Load pipeline state for resuming"""
        state_id = f"{pipeline_id}_{execution_id}"
        
        if state_id in self.pipeline_states:
            return self.pipeline_states[state_id]
        
        # Try to load from storage
        return self._load_pipeline_state(state_id)
    
    def get_checkpoint(self, checkpoint_id: str) -> Optional[CheckpointMetadata]:
        """Get checkpoint metadata"""
        return self.checkpoints.get(checkpoint_id)
    
    def load_checkpoint_data(self, checkpoint_id: str) -> Optional[pd.DataFrame]:
        """Load data snapshot from checkpoint"""
        return self._load_data_snapshot(checkpoint_id)
    
    def list_checkpoints(self, pipeline_id: Optional[str] = None) -> List[CheckpointMetadata]:
        """List checkpoints, optionally filtered by pipeline"""
        checkpoints = list(self.checkpoints.values())
        
        if pipeline_id:
            checkpoints = [cp for cp in checkpoints if cp.pipeline_id == pipeline_id]
        
        # Sort by timestamp, most recent first
        checkpoints.sort(key=lambda x: x.timestamp, reverse=True)
        
        return checkpoints
    
    def can_resume_from_checkpoint(self, checkpoint_id: str) -> bool:
        """Check if pipeline can be resumed from a specific checkpoint"""
        checkpoint = self.checkpoints.get(checkpoint_id)
        
        if not checkpoint:
            return False
        
        if checkpoint.status != CheckpointStatus.ACTIVE:
            return False
        
        # Check if data snapshot exists and is valid
        if checkpoint.checkpoint_type == CheckpointType.DATA_SNAPSHOT:
            data_file = self._get_data_snapshot_path(checkpoint_id)
            if not os.path.exists(data_file):
                return False
        
        return True
    
    def resume_from_checkpoint(self, checkpoint_id: str) -> Tuple[CheckpointMetadata, Optional[pd.DataFrame]]:
        """Resume pipeline execution from a checkpoint"""
        checkpoint = self.checkpoints.get(checkpoint_id)
        
        if not checkpoint:
            raise ValueError(f"Checkpoint {checkpoint_id} not found")
        
        if not self.can_resume_from_checkpoint(checkpoint_id):
            raise ValueError(f"Cannot resume from checkpoint {checkpoint_id}")
        
        # Load data if available
        data = self.load_checkpoint_data(checkpoint_id)
        
        logger.info(f"Resuming from checkpoint {checkpoint_id} at step {checkpoint.step_index}: {checkpoint.step_name}")
        
        return checkpoint, data
    
    def cleanup_checkpoints(self, pipeline_id: str, keep_last: int = 5):
        """Clean up old checkpoints, keeping only the most recent ones"""
        pipeline_checkpoints = [cp for cp in self.checkpoints.values() if cp.pipeline_id == pipeline_id]
        pipeline_checkpoints.sort(key=lambda x: x.timestamp, reverse=True)
        
        # Keep only the most recent checkpoints
        to_remove = pipeline_checkpoints[keep_last:]
        
        for checkpoint in to_remove:
            self.delete_checkpoint(checkpoint.checkpoint_id)
    
    def delete_checkpoint(self, checkpoint_id: str):
        """Delete a checkpoint and its associated data"""
        if checkpoint_id not in self.checkpoints:
            return
        
        # Remove data file
        data_file = self._get_data_snapshot_path(checkpoint_id)
        if os.path.exists(data_file):
            os.remove(data_file)
        
        # Remove metadata file
        metadata_file = self._get_checkpoint_metadata_path(checkpoint_id)
        if os.path.exists(metadata_file):
            os.remove(metadata_file)
        
        # Remove from memory
        del self.checkpoints[checkpoint_id]
        
        logger.info(f"Deleted checkpoint {checkpoint_id}")
    
    def get_resume_options(self, pipeline_id: str) -> List[Dict[str, Any]]:
        """Get available resume options for a pipeline"""
        options = []
        
        pipeline_checkpoints = [cp for cp in self.checkpoints.values() 
                               if cp.pipeline_id == pipeline_id and cp.status == CheckpointStatus.ACTIVE]
        pipeline_checkpoints.sort(key=lambda x: x.timestamp, reverse=True)
        
        for checkpoint in pipeline_checkpoints:
            can_resume = self.can_resume_from_checkpoint(checkpoint.checkpoint_id)
            
            options.append({
                "checkpoint_id": checkpoint.checkpoint_id,
                "step_index": checkpoint.step_index,
                "step_name": checkpoint.step_name,
                "timestamp": checkpoint.timestamp.isoformat(),
                "checkpoint_type": checkpoint.checkpoint_type.value,
                "data_rows": checkpoint.data_rows,
                "data_columns": checkpoint.data_columns,
                "can_resume": can_resume,
                "metadata": checkpoint.metadata
            })
        
        return options
    
    def _calculate_data_hash(self, data: pd.DataFrame) -> str:
        """Calculate hash of DataFrame for integrity checking"""
        import hashlib
        # Use a sample of the data to create a hash
        sample_size = min(1000, len(data))
        sample_data = data.head(sample_size)
        data_string = sample_data.to_string()
        return hashlib.md5(data_string.encode()).hexdigest()
    
    def _save_data_snapshot(self, checkpoint_id: str, data: pd.DataFrame):
        """Save data snapshot to disk"""
        data_file = self._get_data_snapshot_path(checkpoint_id)
        
        # Use pickle for efficient storage of DataFrame
        with open(data_file, 'wb') as f:
            pickle.dump(data, f)
    
    def _load_data_snapshot(self, checkpoint_id: str) -> Optional[pd.DataFrame]:
        """Load data snapshot from disk"""
        data_file = self._get_data_snapshot_path(checkpoint_id)
        
        if not os.path.exists(data_file):
            return None
        
        try:
            with open(data_file, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            logger.error(f"Failed to load data snapshot for checkpoint {checkpoint_id}: {e}")
            return None
    
    def _save_checkpoint_metadata(self, checkpoint: CheckpointMetadata):
        """Save checkpoint metadata to disk"""
        metadata_file = self._get_checkpoint_metadata_path(checkpoint.checkpoint_id)
        
        metadata_dict = {
            "checkpoint_id": checkpoint.checkpoint_id,
            "checkpoint_type": checkpoint.checkpoint_type.value,
            "status": checkpoint.status.value,
            "pipeline_id": checkpoint.pipeline_id,
            "step_index": checkpoint.step_index,
            "step_name": checkpoint.step_name,
            "timestamp": checkpoint.timestamp.isoformat(),
            "data_hash": checkpoint.data_hash,
            "data_rows": checkpoint.data_rows,
            "data_columns": checkpoint.data_columns,
            "metadata": checkpoint.metadata
        }
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata_dict, f, indent=2)
    
    def _save_pipeline_state(self, state: PipelineState):
        """Save pipeline state to disk"""
        state_file = self._get_pipeline_state_path(f"{state.pipeline_id}_{state.execution_id}")
        
        state_dict = {
            "pipeline_id": state.pipeline_id,
            "execution_id": state.execution_id,
            "current_step": state.current_step,
            "total_steps": state.total_steps,
            "intermediate_results": state.intermediate_results,
            "configuration": state.configuration,
            "start_time": state.start_time.isoformat(),
            "last_update": state.last_update.isoformat(),
            "status": state.status,
            "error_message": state.error_message
        }
        
        with open(state_file, 'w') as f:
            json.dump(state_dict, f, indent=2)
        
        # Save data snapshot separately if it exists
        if state.data_snapshot is not None:
            data_file = self._get_pipeline_data_path(f"{state.pipeline_id}_{state.execution_id}")
            with open(data_file, 'wb') as f:
                pickle.dump(state.data_snapshot, f)
    
    def _load_pipeline_state(self, state_id: str) -> Optional[PipelineState]:
        """Load pipeline state from disk"""
        state_file = self._get_pipeline_state_path(state_id)
        
        if not os.path.exists(state_file):
            return None
        
        try:
            with open(state_file, 'r') as f:
                state_dict = json.load(f)
            
            # Load data snapshot if it exists
            data_snapshot = None
            data_file = self._get_pipeline_data_path(state_id)
            if os.path.exists(data_file):
                with open(data_file, 'rb') as f:
                    data_snapshot = pickle.load(f)
            
            state = PipelineState(
                pipeline_id=state_dict["pipeline_id"],
                execution_id=state_dict["execution_id"],
                current_step=state_dict["current_step"],
                total_steps=state_dict["total_steps"],
                data_snapshot=data_snapshot,
                intermediate_results=state_dict["intermediate_results"],
                configuration=state_dict["configuration"],
                start_time=datetime.fromisoformat(state_dict["start_time"]),
                last_update=datetime.fromisoformat(state_dict["last_update"]),
                status=state_dict["status"],
                error_message=state_dict.get("error_message")
            )
            
            self.pipeline_states[state_id] = state
            return state
            
        except Exception as e:
            logger.error(f"Failed to load pipeline state {state_id}: {e}")
            return None
    
    def _load_checkpoints(self):
        """Load all checkpoints from disk"""
        checkpoints_dir = os.path.join(self.storage_path, "metadata")
        if not os.path.exists(checkpoints_dir):
            return
        
        for filename in os.listdir(checkpoints_dir):
            if filename.endswith('.json'):
                checkpoint_id = filename[:-5]  # Remove .json extension
                try:
                    with open(os.path.join(checkpoints_dir, filename), 'r') as f:
                        metadata_dict = json.load(f)
                    
                    checkpoint = CheckpointMetadata(
                        checkpoint_id=metadata_dict["checkpoint_id"],
                        checkpoint_type=CheckpointType(metadata_dict["checkpoint_type"]),
                        status=CheckpointStatus(metadata_dict["status"]),
                        pipeline_id=metadata_dict["pipeline_id"],
                        step_index=metadata_dict["step_index"],
                        step_name=metadata_dict["step_name"],
                        timestamp=datetime.fromisoformat(metadata_dict["timestamp"]),
                        data_hash=metadata_dict.get("data_hash"),
                        data_rows=metadata_dict.get("data_rows"),
                        data_columns=metadata_dict.get("data_columns"),
                        metadata=metadata_dict.get("metadata", {})
                    )
                    
                    self.checkpoints[checkpoint_id] = checkpoint
                    
                except Exception as e:
                    logger.error(f"Failed to load checkpoint metadata {filename}: {e}")
    
    def _get_data_snapshot_path(self, checkpoint_id: str) -> str:
        """Get path for data snapshot file"""
        data_dir = os.path.join(self.storage_path, "data")
        os.makedirs(data_dir, exist_ok=True)
        return os.path.join(data_dir, f"{checkpoint_id}.pkl")
    
    def _get_checkpoint_metadata_path(self, checkpoint_id: str) -> str:
        """Get path for checkpoint metadata file"""
        metadata_dir = os.path.join(self.storage_path, "metadata")
        os.makedirs(metadata_dir, exist_ok=True)
        return os.path.join(metadata_dir, f"{checkpoint_id}.json")
    
    def _get_pipeline_state_path(self, state_id: str) -> str:
        """Get path for pipeline state file"""
        states_dir = os.path.join(self.storage_path, "states")
        os.makedirs(states_dir, exist_ok=True)
        return os.path.join(states_dir, f"{state_id}.json")
    
    def _get_pipeline_data_path(self, state_id: str) -> str:
        """Get path for pipeline data file"""
        states_dir = os.path.join(self.storage_path, "states")
        os.makedirs(states_dir, exist_ok=True)
        return os.path.join(states_dir, f"{state_id}_data.pkl")


# Global checkpoint manager instance
checkpoint_manager = CheckpointManager()