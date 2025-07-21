"""Real-time synchronization manager for handling real-time data sync from various sources."""

import asyncio
import logging
from typing import Dict, Any, Optional, Callable
from datetime import datetime
from sqlalchemy.orm import Session
import pandas as pd

from .mongodb_connector import MongoDBConnector
from .mysql_binlog_connector import MySQLBinlogConnector

logger = logging.getLogger(__name__)


class RealTimeSyncManager:
    """Manager for real-time data synchronization across different data sources."""
    
    def __init__(self):
        self.active_syncs = {}
        self.sync_tasks = {}
    
    async def start_sync(
        self,
        sync_id: str,
        data_source_type: str,
        connection_config: Dict[str, Any],
        source_name: str,
        target_table: str,
        target_db: Session,
        sync_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Start real-time synchronization for a data source."""
        try:
            if sync_id in self.active_syncs:
                return {"status": "error", "error": "Sync already running with this ID"}
            
            # Create callback function for processing changes
            callback = self._create_change_callback(target_table, target_db, sync_config)
            
            # Start sync based on data source type
            if data_source_type == 'mongodb':
                result = await self._start_mongodb_sync(
                    sync_id, connection_config, source_name, callback, sync_config
                )
            elif data_source_type == 'mysql':
                result = await self._start_mysql_sync(
                    sync_id, connection_config, source_name, callback, sync_config
                )
            elif data_source_type == 'kafka':
                result = await self._start_kafka_sync(
                    sync_id, connection_config, source_name, callback, sync_config
                )
            elif data_source_type == 'rabbitmq':
                result = await self._start_rabbitmq_sync(
                    sync_id, connection_config, source_name, callback, sync_config
                )
            else:
                return {
                    "status": "error",
                    "error": f"Real-time sync not supported for {data_source_type}"
                }
            
            # Store sync information
            self.active_syncs[sync_id] = {
                'data_source_type': data_source_type,
                'source_name': source_name,
                'target_table': target_table,
                'started_at': datetime.utcnow(),
                'status': 'running',
                'records_synced': 0,
                'last_sync_at': None,
                'sync_config': sync_config,
                'result': result
            }
            
            logger.info(f"Started real-time sync {sync_id} for {data_source_type}:{source_name}")
            
            return {
                "status": "success",
                "sync_id": sync_id,
                "message": f"Real-time sync started for {source_name}",
                "details": result
            }
            
        except Exception as e:
            logger.error(f"Failed to start real-time sync: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def _start_mongodb_sync(
        self,
        sync_id: str,
        connection_config: Dict[str, Any],
        collection_name: str,
        callback: Callable,
        sync_config: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Start MongoDB change stream sync."""
        connector = MongoDBConnector(connection_config)
        await connector.connect()
        
        resume_token = sync_config.get('resume_token') if sync_config else None
        
        # Start sync in background task
        task = asyncio.create_task(
            connector.start_real_time_sync(collection_name, callback, resume_token)
        )
        self.sync_tasks[sync_id] = task
        
        return {
            "type": "mongodb_changestream",
            "collection": collection_name,
            "resume_token": resume_token
        }
    
    async def _start_mysql_sync(
        self,
        sync_id: str,
        connection_config: Dict[str, Any],
        table_name: str,
        callback: Callable,
        sync_config: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Start MySQL binlog sync."""
        connector = MySQLBinlogConnector(connection_config)
        
        # Validate binlog configuration
        validation = await connector.validate_binlog_configuration()
        if not validation['valid']:
            raise Exception(f"MySQL binlog configuration issues: {', '.join(validation['issues'])}")
        
        # Get current position if not provided
        log_file = sync_config.get('log_file') if sync_config else None
        log_pos = sync_config.get('log_pos') if sync_config else None
        
        if not log_file or not log_pos:
            current_pos = await connector.get_current_binlog_position()
            log_file = current_pos['log_file']
            log_pos = current_pos['log_pos']
        
        # Start sync in background task
        tables = [table_name] if isinstance(table_name, str) else table_name
        task = asyncio.create_task(
            connector.start_real_time_sync(
                tables, callback, log_file=log_file, log_pos=log_pos
            )
        )
        self.sync_tasks[sync_id] = task
        
        return {
            "type": "mysql_binlog",
            "tables": tables,
            "log_file": log_file,
            "log_pos": log_pos
        }
    
    async def _start_kafka_sync(
        self,
        sync_id: str,
        connection_config: Dict[str, Any],
        topic_name: str,
        callback: Callable,
        sync_config: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Start Kafka real-time sync."""
        from .kafka_connector import KafkaConnector
        
        connector = KafkaConnector(connection_config)
        
        # Kafka streams are inherently real-time
        task = asyncio.create_task(
            self._kafka_streaming_task(connector, topic_name, callback)
        )
        self.sync_tasks[sync_id] = task
        
        return {
            "type": "kafka_stream",
            "topic": topic_name,
            "consumer_group": connection_config.get('consumer_group', 'csv2sql_consumer')
        }
    
    async def _kafka_streaming_task(
        self,
        connector,
        topic_name: str,
        callback: Callable
    ):
        """Task for continuous Kafka message consumption."""
        try:
            await connector.connect()
            
            async for chunk_df in connector.extract_data(topic_name, chunk_size=1):
                # Process each message as a real-time event
                for _, row in chunk_df.iterrows():
                    single_row_df = pd.DataFrame([row])
                    await callback(single_row_df, 'kafka_message')
                    
        except Exception as e:
            logger.error(f"Kafka streaming task failed: {str(e)}")
            raise
    
    async def _start_rabbitmq_sync(
        self,
        sync_id: str,
        connection_config: Dict[str, Any],
        queue_name: str,
        callback: Callable,
        sync_config: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Start RabbitMQ real-time sync."""
        from .rabbitmq_connector import RabbitMQConnector
        
        connector = RabbitMQConnector(connection_config)
        
        # RabbitMQ message consumption is inherently real-time
        task = asyncio.create_task(
            self._rabbitmq_streaming_task(connector, queue_name, callback)
        )
        self.sync_tasks[sync_id] = task
        
        return {
            "type": "rabbitmq_stream",
            "queue": queue_name
        }
    
    async def _rabbitmq_streaming_task(
        self,
        connector,
        queue_name: str,
        callback: Callable
    ):
        """Task for continuous RabbitMQ message consumption."""
        try:
            await connector.connect()
            
            async for chunk_df in connector.extract_data(queue_name, chunk_size=1):
                # Process each message as a real-time event
                for _, row in chunk_df.iterrows():
                    single_row_df = pd.DataFrame([row])
                    await callback(single_row_df, 'rabbitmq_message')
                    
        except Exception as e:
            logger.error(f"RabbitMQ streaming task failed: {str(e)}")
            raise
    
    def _create_change_callback(
        self,
        target_table: str,
        target_db: Session,
        sync_config: Optional[Dict[str, Any]]
    ) -> Callable:
        """Create a callback function for processing real-time changes."""
        
        async def process_change(df: pd.DataFrame, operation_type: str):
            try:
                # Add metadata columns
                df['_sync_timestamp'] = datetime.utcnow()
                df['_sync_operation'] = operation_type
                
                # Apply transformations if configured
                if sync_config and 'transformations' in sync_config:
                    df = self._apply_transformations(df, sync_config['transformations'])
                
                # Handle different operation types
                if operation_type == 'delete':
                    # For deletes, mark as soft delete or handle specially
                    if sync_config and sync_config.get('soft_delete', False):
                        df['_deleted'] = True
                        df.to_sql(
                            target_table,
                            con=target_db.get_bind(),
                            if_exists='append',
                            index=False,
                            method='multi'
                        )
                    else:
                        # For hard deletes, would need to identify and remove existing records
                        logger.warning(f"Hard delete operation not implemented for {target_table}")
                
                elif operation_type in ['insert', 'update', 'kafka_message', 'rabbitmq_message']:
                    # Insert new records or updates
                    df.to_sql(
                        target_table,
                        con=target_db.get_bind(),
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                
                # Update sync statistics
                sync_id = None
                for sid, sync_info in self.active_syncs.items():
                    if sync_info['target_table'] == target_table:
                        sync_id = sid
                        break
                
                if sync_id:
                    self.active_syncs[sync_id]['records_synced'] += len(df)
                    self.active_syncs[sync_id]['last_sync_at'] = datetime.utcnow()
                
                logger.debug(f"Processed {len(df)} records for {target_table} ({operation_type})")
                
            except Exception as e:
                logger.error(f"Error processing change for {target_table}: {str(e)}")
        
        return process_change
    
    def _apply_transformations(self, df: pd.DataFrame, transformations: Dict[str, Any]) -> pd.DataFrame:
        """Apply transformations to the DataFrame."""
        try:
            # Column mapping
            if 'column_mapping' in transformations:
                df = df.rename(columns=transformations['column_mapping'])
            
            # Column filtering
            if 'include_columns' in transformations:
                df = df[transformations['include_columns']]
            
            # Value transformations
            if 'value_transforms' in transformations:
                for column, transform in transformations['value_transforms'].items():
                    if column in df.columns:
                        if transform['type'] == 'uppercase':
                            df[column] = df[column].str.upper()
                        elif transform['type'] == 'lowercase':
                            df[column] = df[column].str.lower()
                        elif transform['type'] == 'replace':
                            df[column] = df[column].str.replace(
                                transform['from'], transform['to']
                            )
            
            return df
            
        except Exception as e:
            logger.error(f"Error applying transformations: {str(e)}")
            return df
    
    async def stop_sync(self, sync_id: str) -> Dict[str, Any]:
        """Stop a real-time synchronization."""
        try:
            if sync_id not in self.active_syncs:
                return {"status": "error", "error": "Sync not found"}
            
            # Cancel the background task
            if sync_id in self.sync_tasks:
                task = self.sync_tasks[sync_id]
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                del self.sync_tasks[sync_id]
            
            # Update sync status
            sync_info = self.active_syncs[sync_id]
            sync_info['status'] = 'stopped'
            sync_info['stopped_at'] = datetime.utcnow()
            
            logger.info(f"Stopped real-time sync {sync_id}")
            
            return {
                "status": "success",
                "message": f"Sync {sync_id} stopped",
                "records_synced": sync_info['records_synced']
            }
            
        except Exception as e:
            logger.error(f"Failed to stop sync {sync_id}: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    def get_sync_status(self, sync_id: Optional[str] = None) -> Dict[str, Any]:
        """Get status of synchronization(s)."""
        if sync_id:
            if sync_id in self.active_syncs:
                return {"status": "success", "sync": self.active_syncs[sync_id]}
            else:
                return {"status": "error", "error": "Sync not found"}
        else:
            return {"status": "success", "syncs": self.active_syncs}
    
    async def cleanup_completed_syncs(self):
        """Clean up completed or failed sync tasks."""
        completed_syncs = []
        
        for sync_id, task in self.sync_tasks.items():
            if task.done():
                completed_syncs.append(sync_id)
                
                # Update sync status based on task result
                if sync_id in self.active_syncs:
                    try:
                        await task  # This will raise exception if task failed
                        self.active_syncs[sync_id]['status'] = 'completed'
                    except Exception as e:
                        self.active_syncs[sync_id]['status'] = 'failed'
                        self.active_syncs[sync_id]['error'] = str(e)
                        logger.error(f"Sync {sync_id} failed: {str(e)}")
        
        # Remove completed tasks
        for sync_id in completed_syncs:
            del self.sync_tasks[sync_id]
        
        return len(completed_syncs)