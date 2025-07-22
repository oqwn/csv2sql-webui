"""
Transaction Management API endpoints
"""

from typing import Dict, List, Any, Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from app.services.transaction_manager import transaction_manager
from app.api.deps import get_current_user

router = APIRouter()


class TransactionCreateRequest(BaseModel):
    """Request model for creating a transaction"""
    data_source_id: int


class TransactionResponse(BaseModel):
    """Response model for transaction information"""
    transaction_id: str
    status: str
    start_time: str
    data_source_id: int
    checkpoints_count: int
    dirty_records_count: int
    logs_count: int


class CheckpointCreateRequest(BaseModel):
    """Request model for creating a checkpoint"""
    transaction_id: str
    step_name: str


class CheckpointResponse(BaseModel):
    """Response model for checkpoint information"""
    checkpoint_id: str
    step_name: str
    timestamp: str


class RollbackRequest(BaseModel):
    """Request model for rolling back to a checkpoint"""
    transaction_id: str
    checkpoint_id: str


class DirtyDataRequest(BaseModel):
    """Request model for isolating dirty data"""
    transaction_id: str
    dirty_records: List[Dict[str, Any]]
    reason: str


@router.post("/create", response_model=Dict[str, str])
async def create_transaction(
    request: TransactionCreateRequest,
    current_user: dict = Depends(get_current_user)
):
    """Create a new transaction"""
    try:
        transaction_id = transaction_manager.create_transaction(request.data_source_id)
        return {"transaction_id": transaction_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create transaction: {str(e)}")


@router.get("/{transaction_id}/status", response_model=TransactionResponse)
async def get_transaction_status(
    transaction_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get transaction status and details"""
    status = transaction_manager.get_transaction_status(transaction_id)
    if not status:
        raise HTTPException(status_code=404, detail="Transaction not found")
    
    return TransactionResponse(**status)


@router.post("/{transaction_id}/checkpoint", response_model=CheckpointResponse)
async def create_checkpoint(
    transaction_id: str,
    request: CheckpointCreateRequest,
    current_user: dict = Depends(get_current_user)
):
    """Create a checkpoint within the transaction"""
    try:
        checkpoint_id = transaction_manager.create_checkpoint(
            transaction_id, 
            request.step_name
        )
        
        # Get checkpoint details for response
        context = transaction_manager._active_transactions.get(transaction_id)
        if context:
            checkpoint = next(
                (cp for cp in context.checkpoints if cp.checkpoint_id == checkpoint_id), 
                None
            )
            if checkpoint:
                return CheckpointResponse(
                    checkpoint_id=checkpoint_id,
                    step_name=checkpoint.step_name,
                    timestamp=checkpoint.timestamp.isoformat()
                )
        
        raise HTTPException(status_code=500, detail="Failed to retrieve checkpoint details")
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create checkpoint: {str(e)}")


@router.post("/{transaction_id}/rollback")
async def rollback_to_checkpoint(
    transaction_id: str,
    request: RollbackRequest,
    current_user: dict = Depends(get_current_user)
):
    """Rollback transaction to a specific checkpoint"""
    try:
        data_snapshot = transaction_manager.rollback_to_checkpoint(
            transaction_id, 
            request.checkpoint_id
        )
        
        return {
            "message": "Successfully rolled back to checkpoint",
            "checkpoint_id": request.checkpoint_id,
            "has_data_snapshot": data_snapshot is not None,
            "snapshot_rows": len(data_snapshot) if data_snapshot is not None else 0
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to rollback: {str(e)}")


@router.post("/{transaction_id}/isolate-dirty-data")
async def isolate_dirty_data(
    transaction_id: str,
    request: DirtyDataRequest,
    current_user: dict = Depends(get_current_user)
):
    """Isolate dirty/invalid data for later analysis"""
    try:
        transaction_manager.isolate_dirty_data(
            transaction_id,
            request.dirty_records,
            request.reason
        )
        
        return {
            "message": "Dirty data isolated successfully",
            "isolated_records": len(request.dirty_records),
            "reason": request.reason
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to isolate dirty data: {str(e)}")


@router.get("/history")
async def get_transaction_history(
    limit: int = 100,
    current_user: dict = Depends(get_current_user)
):
    """Get transaction execution history"""
    try:
        history = transaction_manager.get_transaction_history(limit)
        return {
            "transactions": history,
            "count": len(history)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get transaction history: {str(e)}")


@router.get("/quarantine")
async def get_dirty_data_quarantine(
    transaction_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """Get quarantined dirty data"""
    try:
        dirty_data = transaction_manager.get_dirty_data_quarantine(transaction_id)
        return {
            "dirty_data": dirty_data,
            "count": len(dirty_data),
            "transaction_id": transaction_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get dirty data: {str(e)}")


@router.get("/active")
async def get_active_transactions(
    current_user: dict = Depends(get_current_user)
):
    """Get all currently active transactions"""
    active_transactions = []
    for transaction_id, context in transaction_manager._active_transactions.items():
        status_info = transaction_manager.get_transaction_status(transaction_id)
        if status_info:
            active_transactions.append(status_info)
    
    return {
        "active_transactions": active_transactions,
        "count": len(active_transactions)
    }