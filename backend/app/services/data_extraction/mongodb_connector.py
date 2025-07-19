import pandas as pd
from typing import Dict, Any, List, Optional, Iterator
from .base_connector import DataSourceConnector, ExtractionConfig
import logging
from datetime import datetime
import json
import asyncio

logger = logging.getLogger(__name__)

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, OperationFailure
    from bson import ObjectId
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False


class MongoDBConnector(DataSourceConnector):
    """Connector for MongoDB databases"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self.client = None
        self.database = None
        
        if not PYMONGO_AVAILABLE:
            raise ImportError("pymongo is required for MongoDB connections. Install with: pip install pymongo")
    
    async def connect(self) -> bool:
        """Establish connection to MongoDB"""
        try:
            connection_string = self._build_connection_string()
            self.client = MongoClient(
                connection_string,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000
            )
            
            # Test the connection
            self.client.admin.command('ping')
            
            # Get database
            db_name = self.connection_config['database']
            self.database = self.client[db_name]
            
            logger.info(f"Successfully connected to MongoDB database: {db_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            return False
    
    async def disconnect(self) -> bool:
        """Close MongoDB connection"""
        try:
            if self.client:
                self.client.close()
                self.client = None
                self.database = None
            return True
        except Exception as e:
            logger.error(f"Failed to disconnect from MongoDB: {str(e)}")
            return False
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test MongoDB connection and return metadata"""
        try:
            if not self.client:
                await self.connect()
                
            # Get server info
            server_info = self.client.server_info()
            
            # Get database stats
            db_stats = self.database.command("dbStats")
            
            # Get collection names
            collections = self.database.list_collection_names()
            
            return {
                "status": "success",
                "database_type": "mongodb",
                "version": server_info.get("version", "Unknown"),
                "collection_count": len(collections),
                "database_size": db_stats.get("dataSize", 0),
                "connection_info": self.get_connection_info()
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_schema_info(self) -> List[Dict[str, Any]]:
        """Get MongoDB collections and their schema information"""
        try:
            if not self.database:
                await self.connect()
                
            schema_info = []
            
            for collection_name in self.database.list_collection_names():
                collection = self.database[collection_name]
                
                # Get collection stats
                stats = self.database.command("collStats", collection_name)
                
                # Sample documents to infer schema
                sample_doc = collection.find_one()
                schema = self._infer_schema(sample_doc) if sample_doc else []
                
                schema_info.append({
                    "name": collection_name,
                    "type": "collection",
                    "fields": schema,
                    "document_count": stats.get("count", 0),
                    "size": stats.get("size", 0)
                })
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to get MongoDB schema info: {str(e)}")
            return []
    
    async def extract_data(
        self,
        source: str,
        extraction_config: Dict[str, Any],
        chunk_size: Optional[int] = None
    ) -> Iterator[pd.DataFrame]:
        """Extract data from MongoDB collection"""
        try:
            if not self.database:
                await self.connect()
                
            config = ExtractionConfig.from_dict(extraction_config)
            chunk_size = chunk_size or config.chunk_size
            
            collection = self.database[source]
            
            # Build query filter
            query_filter = self._build_query_filter(config)
            
            # Build projection (fields to include)
            projection = None
            if config.columns:
                projection = {field: 1 for field in config.columns}
            
            # Get cursor
            cursor = collection.find(query_filter, projection)
            
            # Apply sorting for incremental extraction
            if config.incremental_column:
                cursor = cursor.sort(config.incremental_column, 1)
            
            # Process in chunks
            documents = []
            for doc in cursor:
                # Convert ObjectId to string
                doc = self._convert_objectids(doc)
                documents.append(doc)
                
                if len(documents) >= chunk_size:
                    yield pd.DataFrame(documents)
                    documents = []
            
            # Yield remaining documents
            if documents:
                yield pd.DataFrame(documents)
                
        except Exception as e:
            logger.error(f"Failed to extract data from MongoDB collection {source}: {str(e)}")
            raise
    
    async def get_record_count(self, source: str, filters: Optional[Dict] = None) -> int:
        """Get document count for a collection"""
        try:
            if not self.database:
                await self.connect()
                
            collection = self.database[source]
            
            if filters:
                query_filter = self._build_mongo_filter(filters)
                return collection.count_documents(query_filter)
            else:
                return collection.estimated_document_count()
                
        except Exception as e:
            logger.error(f"Failed to get document count: {str(e)}")
            return 0
    
    def get_required_config_fields(self) -> List[str]:
        """Return required configuration fields for MongoDB"""
        return ['host', 'database']
    
    async def supports_incremental_extraction(self) -> bool:
        """MongoDB supports incremental extraction"""
        return True
    
    async def supports_real_time_sync(self) -> bool:
        """MongoDB supports real-time sync via Change Streams"""
        return True
    
    async def get_incremental_key_columns(self, source: str) -> List[str]:
        """Get fields suitable for incremental extraction"""
        try:
            if not self.database:
                await self.connect()
                
            collection = self.database[source]
            sample_doc = collection.find_one()
            
            if not sample_doc:
                return []
            
            incremental_fields = []
            
            # _id is always available for incremental extraction
            incremental_fields.append("_id")
            
            # Look for timestamp fields
            for field, value in sample_doc.items():
                if isinstance(value, datetime):
                    incremental_fields.append(field)
                elif field.lower() in ['created_at', 'updated_at', 'timestamp', 'created', 'modified']:
                    incremental_fields.append(field)
                    
            return incremental_fields
            
        except Exception as e:
            logger.error(f"Failed to get incremental fields: {str(e)}")
            return ["_id"]  # Fallback to _id
    
    def _build_connection_string(self) -> str:
        """Build MongoDB connection string"""
        config = self.connection_config
        
        # Basic connection string
        if config.get('username') and config.get('password'):
            auth_string = f"{config['username']}:{config['password']}@"
        else:
            auth_string = ""
            
        host = config['host']
        port = config.get('port', 27017)
        
        connection_string = f"mongodb://{auth_string}{host}:{port}"
        
        # Add connection options
        options = []
        if config.get('auth_source'):
            options.append(f"authSource={config['auth_source']}")
        if config.get('replica_set'):
            options.append(f"replicaSet={config['replica_set']}")
        if config.get('ssl', False):
            options.append("ssl=true")
            
        if options:
            connection_string += "?" + "&".join(options)
            
        return connection_string
    
    def _build_query_filter(self, config: ExtractionConfig) -> Dict[str, Any]:
        """Build MongoDB query filter"""
        query_filter = {}
        
        # Add custom filters
        if config.filters:
            mongo_filter = self._build_mongo_filter(config.filters)
            query_filter.update(mongo_filter)
        
        # Add incremental filter
        if config.mode == "incremental" and config.incremental_column and config.last_value:
            if config.incremental_column == "_id":
                # Handle ObjectId comparison
                if isinstance(config.last_value, str):
                    query_filter["_id"] = {"$gt": ObjectId(config.last_value)}
                else:
                    query_filter["_id"] = {"$gt": config.last_value}
            else:
                query_filter[config.incremental_column] = {"$gt": config.last_value}
        
        return query_filter
    
    def _build_mongo_filter(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Convert generic filters to MongoDB query format"""
        mongo_filter = {}
        
        for field, value in filters.items():
            if isinstance(value, dict):
                # Handle operators
                mongo_ops = {}
                for op, val in value.items():
                    if op == "gt":
                        mongo_ops["$gt"] = val
                    elif op == "lt":
                        mongo_ops["$lt"] = val
                    elif op == "gte":
                        mongo_ops["$gte"] = val
                    elif op == "lte":
                        mongo_ops["$lte"] = val
                    elif op == "in":
                        mongo_ops["$in"] = val
                    elif op == "like":
                        mongo_ops["$regex"] = val
                        mongo_ops["$options"] = "i"  # Case insensitive
                        
                mongo_filter[field] = mongo_ops
            else:
                mongo_filter[field] = value
                
        return mongo_filter
    
    def _infer_schema(self, document: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Infer schema from a sample document"""
        schema = []
        
        for field, value in document.items():
            field_type = type(value).__name__
            
            # Map Python types to more descriptive names
            if isinstance(value, ObjectId):
                field_type = "ObjectId"
            elif isinstance(value, datetime):
                field_type = "datetime"
            elif isinstance(value, dict):
                field_type = "object"
            elif isinstance(value, list):
                field_type = "array"
                
            schema.append({
                "name": field,
                "type": field_type,
                "nullable": True  # MongoDB fields are generally nullable
            })
            
        return schema
    
    def _convert_objectids(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Convert ObjectId instances to strings for DataFrame compatibility"""
        converted = {}
        
        for key, value in doc.items():
            if isinstance(value, ObjectId):
                converted[key] = str(value)
            elif isinstance(value, dict):
                converted[key] = self._convert_objectids(value)
            elif isinstance(value, list):
                converted[key] = [
                    self._convert_objectids(item) if isinstance(item, dict)
                    else str(item) if isinstance(item, ObjectId)
                    else item
                    for item in value
                ]
            else:
                converted[key] = value
                
        return converted
    
    async def start_real_time_sync(
        self,
        source: str,
        callback: callable,
        resume_token: Optional[str] = None
    ) -> str:
        """Start real-time sync using MongoDB Change Streams"""
        try:
            if not self.database:
                await self.connect()
            
            collection = self.database[source]
            
            # Set up change stream options
            pipeline = []
            options = {}
            
            if resume_token:
                options['resume_after'] = {'_data': resume_token}
            else:
                options['start_at_operation_time'] = datetime.utcnow()
            
            # Start change stream
            change_stream = collection.watch(pipeline, **options)
            
            logger.info(f"Started real-time sync for collection {source}")
            
            # Process changes
            for change in change_stream:
                try:
                    # Convert change document to DataFrame-compatible format
                    change_data = self._process_change_event(change)
                    
                    if change_data:
                        df = pd.DataFrame([change_data])
                        
                        # Call the callback with the change data
                        await callback(df, change['operationType'])
                        
                except Exception as e:
                    logger.error(f"Error processing change event: {str(e)}")
                    continue
            
            return change_stream.resume_token['_data'] if change_stream.resume_token else ""
            
        except Exception as e:
            logger.error(f"Failed to start real-time sync: {str(e)}")
            raise
    
    def _process_change_event(self, change: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a change stream event into DataFrame-compatible format"""
        try:
            operation_type = change['operationType']
            
            # Extract the document based on operation type
            if operation_type in ['insert', 'replace']:
                document = change['fullDocument']
            elif operation_type == 'update':
                # For updates, combine the document key with updated fields
                document = change['documentKey'].copy()
                if 'updateDescription' in change and 'updatedFields' in change['updateDescription']:
                    document.update(change['updateDescription']['updatedFields'])
            elif operation_type == 'delete':
                document = change['documentKey']
            else:
                return None
            
            # Convert ObjectIds and add metadata
            processed_doc = self._convert_objectids(document)
            processed_doc['_change_operation'] = operation_type
            processed_doc['_change_timestamp'] = change['clusterTime'].time if hasattr(change['clusterTime'], 'time') else datetime.utcnow()
            
            return processed_doc
            
        except Exception as e:
            logger.error(f"Error processing change event: {str(e)}")
            return None
    
    async def stop_real_time_sync(self):
        """Stop real-time synchronization"""
        # Change streams automatically close when the client disconnects
        await self.disconnect()