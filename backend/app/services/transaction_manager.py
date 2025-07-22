"""
Transaction Management System for ETL Operations
Provides transaction control, rollback, and error isolation capabilities.
"""

import uuid
import traceback
from datetime import datetime
from typing import Optional, Dict, Any, List, Callable, AsyncContextManager
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import logging

from app.services.sql_executor import DataSourceSQLExecutor
from app.services.local_storage import LocalStorage

logger = logging.getLogger(__name__)


class TransactionStatus(Enum):
    PENDING = "pending"
    ACTIVE = "active"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"


@dataclass
class TransactionCheckpoint:
    """Represents a savepoint within a transaction"""
    checkpoint_id: str
    timestamp: datetime
    step_name: str
    data_snapshot: Optional[pd.DataFrame] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass 
class TransactionLog:
    """Log entry for transaction operations"""
    transaction_id: str
    timestamp: datetime
    operation: str
    status: str
    message: str
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TransactionContext:
    """Context for managing transaction state"""
    transaction_id: str
    status: TransactionStatus
    start_time: datetime
    data_source_id: int
    checkpoints: List[TransactionCheckpoint] = field(default_factory=list)
    logs: List[TransactionLog] = field(default_factory=list)
    dirty_data_records: List[Dict[str, Any]] = field(default_factory=list)
    connection: Optional[Any] = None
    rollback_operations: List[Callable] = field(default_factory=list)


class TransactionManager:
    """Manages database transactions with rollback and checkpoint capabilities"""
    
    def __init__(self):
        self._active_transactions: Dict[str, TransactionContext] = {}
        self._storage = LocalStorage()
        
    def create_transaction(self, data_source_id: int) -> str:
        """Create a new transaction context"""
        transaction_id = str(uuid.uuid4())
        
        context = TransactionContext(
            transaction_id=transaction_id,
            status=TransactionStatus.PENDING,
            start_time=datetime.now(),
            data_source_id=data_source_id
        )
        
        self._active_transactions[transaction_id] = context
        
        # Log transaction creation
        self._log_operation(
            transaction_id,
            "CREATE_TRANSACTION",
            TransactionStatus.PENDING.value,
            "Transaction created"
        )
        
        return transaction_id
    
    @asynccontextmanager
    async def transaction(self, data_source_id: int) -> AsyncContextManager[TransactionContext]:
        """Context manager for automatic transaction handling"""
        transaction_id = self.create_transaction(data_source_id)
        context = self._active_transactions[transaction_id]
        
        try:
            # Start transaction
            await self._begin_transaction(context)
            yield context
            
            # Auto-commit if successful
            await self._commit_transaction(context)
            
        except Exception as e:
            # Auto-rollback on error
            logger.error(f"Transaction {transaction_id} failed: {str(e)}")
            await self._rollback_transaction(context, str(e))
            raise
            
        finally:
            # Cleanup
            self._cleanup_transaction(transaction_id)
    
    async def _begin_transaction(self, context: TransactionContext):
        """Begin a database transaction"""
        try:
            # We need to get connection config from data source ID
            # For now, using placeholder - this should be fetched from data_sources table
            executor = DataSourceSQLExecutor(
                "postgresql",  # This should be fetched from data source
                {},  # This should be actual connection config
                context
            )
            
            # Begin transaction
            connection_info = await executor.begin_transaction()
            context.connection = connection_info
            
            context.status = TransactionStatus.ACTIVE
            
            self._log_operation(
                context.transaction_id,
                "BEGIN_TRANSACTION", 
                TransactionStatus.ACTIVE.value,
                "Transaction started"
            )
            
        except Exception as e:
            context.status = TransactionStatus.FAILED
            self._log_operation(
                context.transaction_id,
                "BEGIN_TRANSACTION",
                TransactionStatus.FAILED.value,
                f"Failed to start transaction: {str(e)}",
                str(e)
            )
            raise
    
    async def _commit_transaction(self, context: TransactionContext):
        """Commit the transaction"""
        try:
            # Commit database transaction
            if context.connection:
                executor = DataSourceSQLExecutor("postgresql", {}, context)
                await executor.commit_transaction(context.connection)
            
            context.status = TransactionStatus.COMMITTED
            
            self._log_operation(
                context.transaction_id,
                "COMMIT_TRANSACTION",
                TransactionStatus.COMMITTED.value,
                "Transaction committed successfully"
            )
            
            # Persist transaction logs
            await self._persist_transaction_logs(context)
            
        except Exception as e:
            context.status = TransactionStatus.FAILED
            self._log_operation(
                context.transaction_id,
                "COMMIT_TRANSACTION",
                TransactionStatus.FAILED.value,
                f"Failed to commit transaction: {str(e)}",
                str(e)
            )
            raise
    
    async def _rollback_transaction(self, context: TransactionContext, error_message: str):
        """Rollback the transaction and execute cleanup operations"""
        try:
            # Rollback database transaction
            if context.connection:
                executor = DataSourceSQLExecutor("postgresql", {}, context)
                await executor.rollback_transaction(context.connection)
            
            # Execute rollback operations in reverse order
            for rollback_op in reversed(context.rollback_operations):
                try:
                    rollback_op()
                except Exception as rollback_error:
                    logger.error(f"Rollback operation failed: {rollback_error}")
            
            context.status = TransactionStatus.ROLLED_BACK
            
            self._log_operation(
                context.transaction_id,
                "ROLLBACK_TRANSACTION",
                TransactionStatus.ROLLED_BACK.value,
                f"Transaction rolled back due to error: {error_message}",
                error_message
            )
            
            # Persist transaction logs
            await self._persist_transaction_logs(context)
            
        except Exception as e:
            context.status = TransactionStatus.FAILED
            logger.error(f"Rollback failed for transaction {context.transaction_id}: {e}")
    
    def create_checkpoint(self, transaction_id: str, step_name: str, 
                         data_snapshot: Optional[pd.DataFrame] = None) -> str:
        """Create a checkpoint within the transaction"""
        if transaction_id not in self._active_transactions:
            raise ValueError(f"Transaction {transaction_id} not found")
        
        context = self._active_transactions[transaction_id]
        checkpoint_id = str(uuid.uuid4())
        
        checkpoint = TransactionCheckpoint(
            checkpoint_id=checkpoint_id,
            timestamp=datetime.now(),
            step_name=step_name,
            data_snapshot=data_snapshot.copy() if data_snapshot is not None else None
        )
        
        context.checkpoints.append(checkpoint)
        
        self._log_operation(
            transaction_id,
            "CREATE_CHECKPOINT",
            "success",
            f"Checkpoint created for step: {step_name}",
            metadata={"checkpoint_id": checkpoint_id, "step_name": step_name}
        )
        
        return checkpoint_id
    
    def rollback_to_checkpoint(self, transaction_id: str, checkpoint_id: str) -> Optional[pd.DataFrame]:
        """Rollback to a specific checkpoint and return the data snapshot"""
        if transaction_id not in self._active_transactions:
            raise ValueError(f"Transaction {transaction_id} not found")
        
        context = self._active_transactions[transaction_id]
        
        # Find the checkpoint
        checkpoint = None
        checkpoint_index = -1
        for i, cp in enumerate(context.checkpoints):
            if cp.checkpoint_id == checkpoint_id:
                checkpoint = cp
                checkpoint_index = i
                break
        
        if not checkpoint:
            raise ValueError(f"Checkpoint {checkpoint_id} not found")
        
        # Remove checkpoints after this one
        context.checkpoints = context.checkpoints[:checkpoint_index + 1]
        
        self._log_operation(
            transaction_id,
            "ROLLBACK_TO_CHECKPOINT",
            "success",
            f"Rolled back to checkpoint: {checkpoint.step_name}",
            metadata={"checkpoint_id": checkpoint_id, "step_name": checkpoint.step_name}
        )
        
        return checkpoint.data_snapshot
    
    def isolate_dirty_data(self, transaction_id: str, dirty_records: List[Dict[str, Any]], 
                          reason: str):
        """Isolate dirty/invalid data for later analysis"""
        if transaction_id not in self._active_transactions:
            raise ValueError(f"Transaction {transaction_id} not found")
        
        context = self._active_transactions[transaction_id]
        
        for record in dirty_records:
            dirty_entry = {
                "timestamp": datetime.now().isoformat(),
                "reason": reason,
                "data": record
            }
            context.dirty_data_records.append(dirty_entry)
        
        self._log_operation(
            transaction_id,
            "ISOLATE_DIRTY_DATA",
            "success",
            f"Isolated {len(dirty_records)} dirty records: {reason}",
            metadata={"dirty_count": len(dirty_records), "reason": reason}
        )
    
    def add_rollback_operation(self, transaction_id: str, rollback_func: Callable):
        """Add a rollback operation to be executed if transaction fails"""
        if transaction_id not in self._active_transactions:
            raise ValueError(f"Transaction {transaction_id} not found")
        
        context = self._active_transactions[transaction_id]
        context.rollback_operations.append(rollback_func)
    
    def get_transaction_status(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """Get the status and details of a transaction"""
        if transaction_id not in self._active_transactions:
            return None
        
        context = self._active_transactions[transaction_id]
        
        return {
            "transaction_id": transaction_id,
            "status": context.status.value,
            "start_time": context.start_time.isoformat(),
            "data_source_id": context.data_source_id,
            "checkpoints_count": len(context.checkpoints),
            "dirty_records_count": len(context.dirty_data_records),
            "logs_count": len(context.logs)
        }
    
    def _log_operation(self, transaction_id: str, operation: str, status: str, 
                      message: str, error: Optional[str] = None, 
                      metadata: Optional[Dict[str, Any]] = None):
        """Log a transaction operation"""
        if transaction_id not in self._active_transactions:
            return
        
        context = self._active_transactions[transaction_id]
        
        log_entry = TransactionLog(
            transaction_id=transaction_id,
            timestamp=datetime.now(),
            operation=operation,
            status=status,
            message=message,
            error=error,
            metadata=metadata or {}
        )
        
        context.logs.append(log_entry)
        logger.info(f"Transaction {transaction_id}: {operation} - {message}")
        
        if error:
            logger.error(f"Transaction {transaction_id} error: {error}")
    
    async def _persist_transaction_logs(self, context: TransactionContext):
        """Persist transaction logs to storage"""
        try:
            # Convert logs to serializable format
            logs_data = []
            for log in context.logs:
                logs_data.append({
                    "transaction_id": log.transaction_id,
                    "timestamp": log.timestamp.isoformat(),
                    "operation": log.operation,
                    "status": log.status,
                    "message": log.message,
                    "error": log.error,
                    "metadata": log.metadata
                })
            
            # Save to storage
            transaction_data = {
                "transaction_id": context.transaction_id,
                "status": context.status.value,
                "start_time": context.start_time.isoformat(),
                "data_source_id": context.data_source_id,
                "logs": logs_data,
                "checkpoints_count": len(context.checkpoints),
                "dirty_records_count": len(context.dirty_data_records)
            }
            
            # Store in transaction history
            existing_history = self._storage.read_file("transaction_history.json", [])
            existing_history.append(transaction_data)
            
            # Keep only last 1000 transactions
            if len(existing_history) > 1000:
                existing_history = existing_history[-1000:]
            
            self._storage.write_file("transaction_history.json", existing_history)
            
        except Exception as e:
            logger.error(f"Failed to persist transaction logs: {e}")
    
    def _cleanup_transaction(self, transaction_id: str):
        """Clean up transaction context"""
        if transaction_id in self._active_transactions:
            context = self._active_transactions[transaction_id]
            
            # Close connection if exists
            # if context.connection:
            #     try:
            #         context.connection.close()
            #     except Exception as e:
            #         logger.error(f"Error closing connection: {e}")
            
            # Remove from active transactions
            del self._active_transactions[transaction_id]
    
    def get_transaction_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get transaction execution history"""
        try:
            history = self._storage.read_file("transaction_history.json", [])
            return history[-limit:]
        except Exception as e:
            logger.error(f"Failed to get transaction history: {e}")
            return []
    
    def get_dirty_data_quarantine(self, transaction_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get quarantined dirty data"""
        if transaction_id and transaction_id in self._active_transactions:
            context = self._active_transactions[transaction_id]
            return context.dirty_data_records
        
        # Return all dirty data from completed transactions
        try:
            history = self._storage.read_file("transaction_history.json", [])
            all_dirty_data = []
            
            for transaction in history:
                if transaction.get("dirty_records_count", 0) > 0:
                    # We would need to store dirty data separately for completed transactions
                    pass
            
            return all_dirty_data
        except Exception as e:
            logger.error(f"Failed to get dirty data quarantine: {e}")
            return []


# Global transaction manager instance
transaction_manager = TransactionManager()