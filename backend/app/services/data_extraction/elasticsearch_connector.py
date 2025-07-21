import pandas as pd
from typing import Dict, Any, List, Optional, Iterator
from .base_connector import DataSourceConnector, ExtractionConfig
import logging

logger = logging.getLogger(__name__)

try:
    from elasticsearch import Elasticsearch
    from elasticsearch.exceptions import ConnectionError, RequestError
    ELASTICSEARCH_AVAILABLE = True
except ImportError:
    ELASTICSEARCH_AVAILABLE = False


class ElasticsearchConnector(DataSourceConnector):
    """Connector for Elasticsearch search engine"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self.client = None
        
        if not ELASTICSEARCH_AVAILABLE:
            raise ImportError("elasticsearch is required for Elasticsearch connections. Install with: pip install elasticsearch")
    
    async def connect(self) -> bool:
        """Test connection to Elasticsearch cluster."""
        try:
            # Build connection configuration
            hosts = [f"{self.connection_config.get('host', 'localhost')}:{self.connection_config.get('port', 9200)}"]
            
            es_config = {
                'hosts': hosts,
                'timeout': 30,
                'max_retries': 3,
                'retry_on_timeout': True
            }
            
            # Add authentication if provided
            if self.connection_config.get('username') and self.connection_config.get('password'):
                es_config['http_auth'] = (self.connection_config['username'], self.connection_config['password'])
            
            # Add SSL configuration
            if self.connection_config.get('use_ssl', False):
                es_config['use_ssl'] = True
                es_config['verify_certs'] = self.connection_config.get('verify_certs', True)
                if self.connection_config.get('ca_certs'):
                    es_config['ca_certs'] = self.connection_config['ca_certs']
            
            # Add API key authentication
            if self.connection_config.get('api_key'):
                es_config['api_key'] = self.connection_config['api_key']
            
            self.client = Elasticsearch(**es_config)
            
            # Test connection
            cluster_info = self.client.info()
            
            logger.info(f"Successfully connected to Elasticsearch cluster: {cluster_info['cluster_name']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {str(e)}")
            return False
    
    async def disconnect(self) -> bool:
        """Close Elasticsearch connection"""
        try:
            if self.client:
                self.client = None
            return True
        except Exception as e:
            logger.error(f"Failed to disconnect from Elasticsearch: {str(e)}")
            return False
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test Elasticsearch connection and return metadata"""
        try:
            if not await self.connect():
                return {
                    "status": "error",
                    "error": "Failed to connect to Elasticsearch"
                }
            
            # Get cluster info
            cluster_info = self.client.info()
            
            return {
                "status": "success",
                "database_type": "elasticsearch",
                "version": cluster_info.get("version", {}).get("number", "Unknown"),
                "cluster_name": cluster_info.get("cluster_name", "Unknown"),
                "connection_info": self.get_connection_info()
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_schema_info(self) -> List[Dict[str, Any]]:
        """Get available Elasticsearch indices."""
        try:
            if not self.client:
                await self.connect()
            
            # Get all indices
            indices = self.client.indices.get_alias(index="*")
            schema_info = []
            
            for index_name in indices.keys():
                # Skip system indices
                if index_name.startswith('.'):
                    continue
                
                try:
                    # Get index stats
                    stats = self.client.indices.stats(index=index_name)
                    doc_count = stats['indices'][index_name]['total']['docs']['count']
                    
                    # Get mapping information
                    mapping = self.client.indices.get_mapping(index=index_name)
                    field_count = 0
                    
                    if index_name in mapping and 'mappings' in mapping[index_name]:
                        properties = mapping[index_name]['mappings'].get('properties', {})
                        field_count = len(properties)
                    
                    schema_info.append({
                        'name': index_name,
                        'type': 'index',
                        'document_count': doc_count,
                        'field_count': field_count,
                        'description': f'Elasticsearch index with {doc_count} documents'
                    })
                    
                except Exception as e:
                    logger.warning(f"Could not get info for index {index_name}: {str(e)}")
                    schema_info.append({
                        'name': index_name,
                        'type': 'index',
                        'document_count': 0,
                        'field_count': 0,
                        'description': 'Elasticsearch index (info unavailable)'
                    })
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to get Elasticsearch schema: {str(e)}")
            return []
    
    async def preview_data(self, source_name: str, limit: int = 100) -> Dict[str, Any]:
        """Preview documents from an Elasticsearch index."""
        try:
            if not self.client:
                await self.connect()
            
            # Search for sample documents
            search_body = {
                "size": limit,
                "query": {"match_all": {}}
            }
            
            response = self.client.search(index=source_name, body=search_body)
            
            if not response['hits']['hits']:
                return {
                    'status': 'success',
                    'columns': [],
                    'sample_data': [],
                    'row_count': 0,
                    'message': 'No documents found in index'
                }
            
            # Extract documents
            documents = []
            for hit in response['hits']['hits']:
                doc = hit['_source'].copy()
                doc['_id'] = hit['_id']
                doc['_score'] = hit.get('_score', 0)
                documents.append(doc)
            
            # Create DataFrame for consistent format
            df = pd.json_normalize(documents)
            
            columns = [
                {
                    'name': col,
                    'sql_type': self._infer_sql_type(df[col])
                }
                for col in df.columns
            ]
            
            sample_data = df.head(limit).to_dict('records')
            
            return {
                'status': 'success',
                'columns': columns,
                'sample_data': sample_data,
                'row_count': len(documents),
                'total_documents': response['hits']['total']['value']
            }
            
        except Exception as e:
            logger.error(f"Failed to preview Elasticsearch index {source_name}: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'columns': [],
                'sample_data': [],
                'row_count': 0
            }
    
    async def extract_data(
        self,
        source: str,
        extraction_config: Dict[str, Any],
        chunk_size: Optional[int] = None
    ) -> Iterator[pd.DataFrame]:
        """Extract data from Elasticsearch index"""
        try:
            if not self.client:
                await self.connect()
                
            config = ExtractionConfig.from_dict(extraction_config)
            chunk_size = chunk_size or config.chunk_size
            
            # Build search query
            search_body = {
                "size": chunk_size,
                "query": {"match_all": {}},
                "sort": ["_doc"]  # Use _doc for efficient pagination
            }
            
            # Add incremental filtering if specified
            if config.mode == "incremental" and config.incremental_column and config.last_value:
                search_body["query"] = {
                    "range": {
                        config.incremental_column: {
                            "gt": config.last_value
                        }
                    }
                }
                search_body["sort"] = [{config.incremental_column: {"order": "asc"}}]
            
            # Use scroll API for large datasets
            scroll_timeout = '5m'
            response = self.client.search(
                index=source,
                body=search_body,
                scroll=scroll_timeout
            )
            
            scroll_id = response['_scroll_id']
            
            while True:
                hits = response['hits']['hits']
                
                if not hits:
                    break
                
                # Extract documents
                documents = []
                for hit in hits:
                    doc = hit['_source'].copy()
                    doc['_id'] = hit['_id']
                    doc['_score'] = hit.get('_score', 0)
                    doc['_index'] = hit['_index']
                    documents.append(doc)
                
                # Convert to DataFrame
                if documents:
                    df = pd.json_normalize(documents)
                    yield df
                
                # Get next batch
                response = self.client.scroll(
                    scroll_id=scroll_id,
                    scroll=scroll_timeout
                )
                
                if not response['hits']['hits']:
                    break
            
            # Clear scroll context
            self.client.clear_scroll(scroll_id=scroll_id)
            
        except Exception as e:
            logger.error(f"Failed to extract data from Elasticsearch index {source}: {str(e)}")
            raise
    
    def _infer_sql_type(self, series: pd.Series) -> str:
        """Infer SQL type from pandas series."""
        if series.dtype == 'object':
            # Check if it's JSON-like
            non_null = series.dropna()
            if len(non_null) > 0:
                sample = non_null.iloc[0]
                if isinstance(sample, (dict, list)):
                    return 'JSONB'
                return 'TEXT'
            return 'TEXT'
        elif pd.api.types.is_integer_dtype(series):
            return 'BIGINT'
        elif pd.api.types.is_float_dtype(series):
            return 'DOUBLE PRECISION'
        elif pd.api.types.is_bool_dtype(series):
            return 'BOOLEAN'
        elif pd.api.types.is_datetime64_any_dtype(series):
            return 'TIMESTAMP'
        else:
            return 'TEXT'
    
    async def supports_incremental_extraction(self) -> bool:
        """Elasticsearch supports incremental extraction."""
        return True
    
    async def supports_real_time_sync(self) -> bool:
        """Elasticsearch doesn't natively support real-time sync."""
        return False
    
    async def get_incremental_key_columns(self, source: str) -> List[str]:
        """Get fields suitable for incremental extraction."""
        try:
            if not self.client:
                await self.connect()
            
            # Get mapping to find date/timestamp fields
            mapping = self.client.indices.get_mapping(index=source)
            incremental_fields = []
            
            if source in mapping and 'mappings' in mapping[source]:
                properties = mapping[source]['mappings'].get('properties', {})
                
                for field_name, field_config in properties.items():
                    field_type = field_config.get('type', '')
                    
                    # Date and timestamp fields are good for incremental extraction
                    if field_type in ['date', 'date_nanos']:
                        incremental_fields.append(field_name)
                    elif field_name.lower() in ['timestamp', '@timestamp', 'created_at', 'updated_at']:
                        incremental_fields.append(field_name)
            
            # Always include _id as fallback
            incremental_fields.append('_id')
            
            return incremental_fields
            
        except Exception as e:
            logger.error(f"Failed to get incremental fields: {str(e)}")
            return ['_id']
    
    async def get_record_count(self, source: str, filters: Optional[Dict] = None) -> int:
        """Get document count for an index"""
        try:
            if not self.client:
                await self.connect()
                
            count_body = {"query": {"match_all": {}}}
            
            if filters:
                # Convert filters to Elasticsearch query format
                count_body["query"] = self._build_es_filter(filters)
                
            response = self.client.count(index=source, body=count_body)
            return response.get("count", 0)
            
        except Exception as e:
            logger.error(f"Failed to get document count for index {source}: {str(e)}")
            return 0
    
    def get_required_config_fields(self) -> List[str]:
        """Return required configuration fields for Elasticsearch"""
        return ['host']
    
    def _build_es_filter(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Convert generic filters to Elasticsearch query format"""
        if not filters:
            return {"match_all": {}}
            
        must_clauses = []
        
        for field, value in filters.items():
            if isinstance(value, dict):
                # Handle operators
                for op, val in value.items():
                    if op == "gt":
                        must_clauses.append({"range": {field: {"gt": val}}})
                    elif op == "lt":
                        must_clauses.append({"range": {field: {"lt": val}}})
                    elif op == "gte":
                        must_clauses.append({"range": {field: {"gte": val}}})
                    elif op == "lte":
                        must_clauses.append({"range": {field: {"lte": val}}})
                    elif op == "in":
                        must_clauses.append({"terms": {field: val}})
                    elif op == "like":
                        must_clauses.append({"wildcard": {field: f"*{val}*"}})
            else:
                must_clauses.append({"term": {field: value}})
                
        return {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}