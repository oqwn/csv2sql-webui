from typing import Dict, Any, List, Optional, Type
from .base_connector import DataSourceConnector
from .relational_connector import RelationalDatabaseConnector
from .mongodb_connector import MongoDBConnector
from .redis_connector import RedisConnector
from .api_connector import APIConnector
from .kafka_connector import KafkaConnector
from .rabbitmq_connector import RabbitMQConnector
from .elasticsearch_connector import ElasticsearchConnector
from .cassandra_connector import CassandraConnector
from ..type_detection import detect_column_type
from ..import_service import import_file_with_sql
from sqlalchemy.orm import Session
from sqlalchemy import text
import pandas as pd
import logging
import asyncio
from datetime import datetime

logger = logging.getLogger(__name__)


class DataExtractionManager:
    """Manager class for handling data extraction from various sources"""
    
    def __init__(self):
        self.connectors = {
            # Relational databases
            'mysql': RelationalDatabaseConnector,
            'postgresql': RelationalDatabaseConnector,
            'sqlite': RelationalDatabaseConnector,
            'mssql': RelationalDatabaseConnector,
            'oracle': RelationalDatabaseConnector,
            
            # NoSQL databases
            'mongodb': MongoDBConnector,
            'redis': RedisConnector,
            'elasticsearch': ElasticsearchConnector,
            'cassandra': CassandraConnector,
            
            # Message queues
            'kafka': KafkaConnector,
            'rabbitmq': RabbitMQConnector,
            
            # APIs
            'rest_api': APIConnector,
        }
    
    def get_connector(self, data_source_type: str, connection_config: Dict[str, Any]) -> DataSourceConnector:
        """Get appropriate connector for data source type"""
        if data_source_type not in self.connectors:
            raise ValueError(f"Unsupported data source type: {data_source_type}")
        
        connector_class = self.connectors[data_source_type]
        return connector_class(connection_config)
    
    async def test_connection(self, data_source_type: str, connection_config: Dict[str, Any]) -> Dict[str, Any]:
        """Test connection to a data source"""
        try:
            connector = self.get_connector(data_source_type, connection_config)
            
            # Validate configuration first
            validation_errors = await connector.validate_config()
            if validation_errors:
                return {
                    "status": "error",
                    "error": "Configuration validation failed",
                    "details": validation_errors
                }
            
            # Test connection
            result = await connector.test_connection()
            await connector.disconnect()
            
            return result
            
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_schema_info(self, data_source_type: str, connection_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get schema information from data source"""
        try:
            connector = self.get_connector(data_source_type, connection_config)
            await connector.connect()
            
            schema_info = await connector.get_schema_info()
            await connector.disconnect()
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to get schema info: {str(e)}")
            return []
    
    async def extract_and_load_data(
        self,
        db: Session,
        data_source_type: str,
        connection_config: Dict[str, Any],
        source_name: str,
        target_table: str,
        extraction_config: Dict[str, Any],
        create_table: bool = True
    ) -> Dict[str, Any]:
        """Extract data from source and load into target database"""
        try:
            connector = self.get_connector(data_source_type, connection_config)
            await connector.connect()
            
            total_records = 0
            chunk_count = 0
            
            # Get first chunk to analyze schema
            first_chunk = None
            async for chunk_df in connector.extract_data(source_name, extraction_config):
                first_chunk = chunk_df
                break
            
            if first_chunk is None or first_chunk.empty:
                return {
                    "status": "error",
                    "error": "No data found in source"
                }
            
            # Create table if needed
            if create_table:
                await self._create_target_table(db, first_chunk, target_table)
            
            # Process first chunk
            await self._load_chunk_to_database(db, first_chunk, target_table)
            total_records += len(first_chunk)
            chunk_count += 1
            
            # Process remaining chunks
            async for chunk_df in connector.extract_data(source_name, extraction_config):
                await self._load_chunk_to_database(db, chunk_df, target_table)
                total_records += len(chunk_df)
                chunk_count += 1
                
                # Log progress every 10 chunks
                if chunk_count % 10 == 0:
                    logger.info(f"Processed {chunk_count} chunks, {total_records} records")
            
            await connector.disconnect()
            
            return {
                "status": "success",
                "message": f"Successfully extracted and loaded {total_records} records",
                "records_processed": total_records,
                "chunks_processed": chunk_count,
                "target_table": target_table
            }
            
        except Exception as e:
            logger.error(f"Data extraction and loading failed: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_incremental_extraction_info(
        self,
        data_source_type: str,
        connection_config: Dict[str, Any],
        source_name: str
    ) -> Dict[str, Any]:
        """Get information for setting up incremental extraction"""
        try:
            connector = self.get_connector(data_source_type, connection_config)
            await connector.connect()
            
            supports_incremental = await connector.supports_incremental_extraction()
            incremental_columns = []
            
            if supports_incremental:
                incremental_columns = await connector.get_incremental_key_columns(source_name)
            
            await connector.disconnect()
            
            return {
                "supports_incremental": supports_incremental,
                "incremental_columns": incremental_columns,
                "supports_real_time": await connector.supports_real_time_sync()
            }
            
        except Exception as e:
            logger.error(f"Failed to get incremental extraction info: {str(e)}")
            return {
                "supports_incremental": False,
                "incremental_columns": [],
                "supports_real_time": False,
                "error": str(e)
            }
    
    async def get_data_preview(
        self,
        data_source_type: str,
        connection_config: Dict[str, Any],
        source_name: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Get a preview of data from the source"""
        try:
            connector = self.get_connector(data_source_type, connection_config)
            await connector.connect()
            
            # Create a preview extraction config
            preview_config = {
                "mode": "full",
                "chunk_size": limit,
                "filters": {},
                "columns": None,
                "order_by": None
            }
            
            # Get first chunk as preview
            preview_data = None
            async for chunk_df in connector.extract_data(source_name, preview_config, chunk_size=limit):
                preview_data = chunk_df.head(limit)
                break
            
            await connector.disconnect()
            
            if preview_data is not None and not preview_data.empty:
                # Convert to JSON-serializable format
                sample_data = preview_data.to_dict(orient='records')
                
                # Get column information
                columns_info = []
                for col in preview_data.columns:
                    sql_type, _ = detect_column_type(preview_data[col])
                    columns_info.append({
                        "name": col,
                        "type": str(preview_data[col].dtype),
                        "sql_type": sql_type,
                        "null_count": int(preview_data[col].isnull().sum()),
                        "unique_count": int(preview_data[col].nunique())
                    })
                
                return {
                    "status": "success",
                    "columns": columns_info,
                    "sample_data": sample_data,
                    "row_count": len(preview_data)
                }
            else:
                return {
                    "status": "error",
                    "error": "No data available for preview"
                }
                
        except Exception as e:
            logger.error(f"Failed to get data preview: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _create_target_table(self, db: Session, sample_df: pd.DataFrame, table_name: str):
        """Create target table based on sample data"""
        try:
            # Detect column types
            column_definitions = []
            
            for col in sample_df.columns:
                sql_type, _ = detect_column_type(sample_df[col])
                # Sanitize column name
                safe_col = col.replace(' ', '_').replace('-', '_').lower()
                safe_col = ''.join(c for c in safe_col if c.isalnum() or c == '_')
                
                column_definitions.append(f'"{safe_col}" {sql_type}')
            
            # Create table SQL
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                id BIGSERIAL PRIMARY KEY,
                {', '.join(column_definitions)},
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            db.execute(text(create_sql))
            db.commit()
            
            logger.info(f"Created table {table_name} with {len(column_definitions)} columns")
            
        except Exception as e:
            logger.error(f"Failed to create target table: {str(e)}")
            raise
    
    async def _load_chunk_to_database(self, db: Session, chunk_df: pd.DataFrame, table_name: str):
        """Load a chunk of data into the database"""
        try:
            # Sanitize column names
            sanitized_columns = {}
            for col in chunk_df.columns:
                safe_col = col.replace(' ', '_').replace('-', '_').lower()
                safe_col = ''.join(c for c in safe_col if c.isalnum() or c == '_')
                sanitized_columns[col] = safe_col
            
            # Rename columns
            chunk_df = chunk_df.rename(columns=sanitized_columns)
            
            # Load to database
            chunk_df.to_sql(
                table_name,
                con=db.get_bind(),
                if_exists='append',
                index=False,
                method='multi'
            )
            
        except Exception as e:
            logger.error(f"Failed to load chunk to database: {str(e)}")
            raise
    
    def get_supported_data_sources(self) -> List[Dict[str, Any]]:
        """Get list of supported data sources"""
        return [
            {
                "type": "mysql",
                "name": "MySQL",
                "category": "relational",
                "description": "MySQL relational database",
                "supports_incremental": True,
                "supports_real_time": False
            },
            {
                "type": "postgresql",
                "name": "PostgreSQL",
                "category": "relational", 
                "description": "PostgreSQL relational database",
                "supports_incremental": True,
                "supports_real_time": False
            },
            {
                "type": "mongodb",
                "name": "MongoDB",
                "category": "nosql",
                "description": "MongoDB document database",
                "supports_incremental": True,
                "supports_real_time": True
            },
            {
                "type": "redis",
                "name": "Redis",
                "category": "nosql",
                "description": "Redis key-value store",
                "supports_incremental": False,
                "supports_real_time": False
            },
            {
                "type": "kafka",
                "name": "Apache Kafka",
                "category": "message_queue",
                "description": "Apache Kafka message streaming platform",
                "supports_incremental": False,
                "supports_real_time": True
            },
            {
                "type": "rabbitmq",
                "name": "RabbitMQ",
                "category": "message_queue",
                "description": "RabbitMQ message broker",
                "supports_incremental": False,
                "supports_real_time": True
            },
            {
                "type": "elasticsearch",
                "name": "Elasticsearch",
                "category": "nosql",
                "description": "Elasticsearch search engine",
                "supports_incremental": True,
                "supports_real_time": False
            },
            {
                "type": "cassandra",
                "name": "Apache Cassandra",
                "category": "nosql",
                "description": "Cassandra distributed database",
                "supports_incremental": True,
                "supports_real_time": False
            },
            {
                "type": "rest_api",
                "name": "REST API",
                "category": "api",
                "description": "REST API data source",
                "supports_incremental": True,
                "supports_real_time": False
            }
        ]