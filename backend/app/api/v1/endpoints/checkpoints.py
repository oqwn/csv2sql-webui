"""
Checkpoint Management API endpoints
"""

from typing import Dict, List, Any, Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from app.services.checkpoint_manager import checkpoint_manager, CheckpointType
from app.api.deps import get_current_user

router = APIRouter()


class CheckpointCreateRequest(BaseModel):
    """Request model for creating a checkpoint"""
    pipeline_id: str
    step_index: int
    step_name: str
    checkpoint_type: str = "step_complete"
    metadata: Optional[Dict[str, Any]] = None


class PipelineStateRequest(BaseModel):
    """Request model for saving pipeline state"""
    pipeline_id: str
    execution_id: str
    current_step: int
    total_steps: int
    intermediate_results: Optional[Dict[str, Any]] = None
    configuration: Optional[Dict[str, Any]] = None
    status: str = "running"


class ResumeRequest(BaseModel):
    """Request model for resuming from checkpoint"""
    checkpoint_id: str


@router.post("/create")
async def create_checkpoint(
    request: CheckpointCreateRequest,
    current_user: dict = Depends(get_current_user)
):
    """Create a new checkpoint"""
    try:
        checkpoint_type = CheckpointType(request.checkpoint_type)
        
        checkpoint_id = checkpoint_manager.create_checkpoint(
            pipeline_id=request.pipeline_id,
            step_index=request.step_index,
            step_name=request.step_name,
            checkpoint_type=checkpoint_type,
            metadata=request.metadata
        )
        
        return {
            "checkpoint_id": checkpoint_id,
            "message": "Checkpoint created successfully",
            "pipeline_id": request.pipeline_id,
            "step_index": request.step_index,
            "step_name": request.step_name
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create checkpoint: {str(e)}")


@router.get("/{checkpoint_id}")
async def get_checkpoint(
    checkpoint_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get checkpoint details"""
    checkpoint = checkpoint_manager.get_checkpoint(checkpoint_id)
    
    if not checkpoint:
        raise HTTPException(status_code=404, detail="Checkpoint not found")
    
    return {
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
        "metadata": checkpoint.metadata,
        "can_resume": checkpoint_manager.can_resume_from_checkpoint(checkpoint_id)
    }


@router.get("/pipeline/{pipeline_id}")
async def list_pipeline_checkpoints(
    pipeline_id: str,
    current_user: dict = Depends(get_current_user)
):
    """List all checkpoints for a specific pipeline"""
    checkpoints = checkpoint_manager.list_checkpoints(pipeline_id)
    
    checkpoint_list = []
    for checkpoint in checkpoints:
        checkpoint_list.append({
            "checkpoint_id": checkpoint.checkpoint_id,
            "checkpoint_type": checkpoint.checkpoint_type.value,
            "status": checkpoint.status.value,
            "step_index": checkpoint.step_index,
            "step_name": checkpoint.step_name,
            "timestamp": checkpoint.timestamp.isoformat(),
            "data_rows": checkpoint.data_rows,
            "data_columns": checkpoint.data_columns,
            "can_resume": checkpoint_manager.can_resume_from_checkpoint(checkpoint.checkpoint_id)
        })
    
    return {
        "pipeline_id": pipeline_id,
        "checkpoints": checkpoint_list,
        "count": len(checkpoint_list)
    }


@router.get("/")
async def list_all_checkpoints(
    current_user: dict = Depends(get_current_user)
):
    """List all checkpoints"""
    checkpoints = checkpoint_manager.list_checkpoints()
    
    checkpoint_list = []
    for checkpoint in checkpoints:
        checkpoint_list.append({
            "checkpoint_id": checkpoint.checkpoint_id,
            "checkpoint_type": checkpoint.checkpoint_type.value,
            "status": checkpoint.status.value,
            "pipeline_id": checkpoint.pipeline_id,
            "step_index": checkpoint.step_index,
            "step_name": checkpoint.step_name,
            "timestamp": checkpoint.timestamp.isoformat(),
            "data_rows": checkpoint.data_rows,
            "data_columns": checkpoint.data_columns,
            "can_resume": checkpoint_manager.can_resume_from_checkpoint(checkpoint.checkpoint_id)
        })
    
    return {
        "checkpoints": checkpoint_list,
        "count": len(checkpoint_list)
    }


@router.post("/resume")
async def resume_from_checkpoint(
    request: ResumeRequest,
    current_user: dict = Depends(get_current_user)
):
    """Resume pipeline execution from a checkpoint"""
    try:
        checkpoint, data = checkpoint_manager.resume_from_checkpoint(request.checkpoint_id)
        
        return {
            "message": "Resume data retrieved successfully",
            "checkpoint_id": request.checkpoint_id,
            "pipeline_id": checkpoint.pipeline_id,
            "step_index": checkpoint.step_index,
            "step_name": checkpoint.step_name,
            "has_data": data is not None,
            "data_shape": [len(data), len(data.columns)] if data is not None else None,
            "resume_from_step": checkpoint.step_index + 1  # Next step to execute
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to resume from checkpoint: {str(e)}")


@router.get("/pipeline/{pipeline_id}/resume-options")
async def get_resume_options(
    pipeline_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get available resume options for a pipeline"""
    try:
        options = checkpoint_manager.get_resume_options(pipeline_id)
        
        return {
            "pipeline_id": pipeline_id,
            "resume_options": options,
            "count": len(options)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get resume options: {str(e)}")


@router.post("/pipeline-state")
async def save_pipeline_state(
    request: PipelineStateRequest,
    current_user: dict = Depends(get_current_user)
):
    """Save complete pipeline state"""
    try:
        state_id = checkpoint_manager.save_pipeline_state(
            pipeline_id=request.pipeline_id,
            execution_id=request.execution_id,
            current_step=request.current_step,
            total_steps=request.total_steps,
            intermediate_results=request.intermediate_results,
            configuration=request.configuration,
            status=request.status
        )
        
        return {
            "state_id": state_id,
            "message": "Pipeline state saved successfully",
            "pipeline_id": request.pipeline_id,
            "execution_id": request.execution_id,
            "current_step": request.current_step,
            "total_steps": request.total_steps
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save pipeline state: {str(e)}")


@router.get("/pipeline-state/{pipeline_id}/{execution_id}")
async def load_pipeline_state(
    pipeline_id: str,
    execution_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Load pipeline state for resuming"""
    try:
        state = checkpoint_manager.load_pipeline_state(pipeline_id, execution_id)
        
        if not state:
            raise HTTPException(status_code=404, detail="Pipeline state not found")
        
        return {
            "pipeline_id": state.pipeline_id,
            "execution_id": state.execution_id,
            "current_step": state.current_step,
            "total_steps": state.total_steps,
            "intermediate_results": state.intermediate_results,
            "configuration": state.configuration,
            "start_time": state.start_time.isoformat(),
            "last_update": state.last_update.isoformat(),
            "status": state.status,
            "error_message": state.error_message,
            "has_data_snapshot": state.data_snapshot is not None,
            "data_shape": [len(state.data_snapshot), len(state.data_snapshot.columns)] if state.data_snapshot is not None else None
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load pipeline state: {str(e)}")


@router.delete("/{checkpoint_id}")
async def delete_checkpoint(
    checkpoint_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Delete a checkpoint"""
    try:
        checkpoint = checkpoint_manager.get_checkpoint(checkpoint_id)
        if not checkpoint:
            raise HTTPException(status_code=404, detail="Checkpoint not found")
        
        checkpoint_manager.delete_checkpoint(checkpoint_id)
        
        return {
            "message": "Checkpoint deleted successfully",
            "checkpoint_id": checkpoint_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete checkpoint: {str(e)}")


@router.post("/pipeline/{pipeline_id}/cleanup")
async def cleanup_checkpoints(
    pipeline_id: str,
    keep_last: int = 5,
    current_user: dict = Depends(get_current_user)
):
    """Clean up old checkpoints for a pipeline"""
    try:
        initial_count = len(checkpoint_manager.list_checkpoints(pipeline_id))
        
        checkpoint_manager.cleanup_checkpoints(pipeline_id, keep_last)
        
        final_count = len(checkpoint_manager.list_checkpoints(pipeline_id))
        removed_count = initial_count - final_count
        
        return {
            "message": f"Cleaned up {removed_count} old checkpoints",
            "pipeline_id": pipeline_id,
            "checkpoints_removed": removed_count,
            "checkpoints_remaining": final_count,
            "kept_last": keep_last
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to cleanup checkpoints: {str(e)}")


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    total_checkpoints = len(checkpoint_manager.checkpoints)
    total_pipeline_states = len(checkpoint_manager.pipeline_states)
    
    return {
        "status": "healthy",
        "total_checkpoints": total_checkpoints,
        "total_pipeline_states": total_pipeline_states,
        "storage_path": checkpoint_manager.storage_path
    }