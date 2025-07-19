import pandas as pd
from typing import Dict, Any, List, Optional, Iterator
from .base_connector import DataSourceConnector, ExtractionConfig
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import DCAwareRoundRobinPolicy
    CASSANDRA_AVAILABLE = True
except ImportError:
    CASSANDRA_AVAILABLE = False


class CassandraConnector(DataSourceConnector):
    """Connector for Apache Cassandra distributed database"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self.cluster = None
        self.session = None
        
        if not CASSANDRA_AVAILABLE:
            raise ImportError("cassandra-driver is required for Cassandra connections. Install with: pip install cassandra-driver")
    
    async def connect(self) -> bool:
        """Test connection to Cassandra cluster."""
        try:
            # Build cluster configuration
            hosts = [self.connection_config.get('host', 'localhost')]
            port = self.connection_config.get('port', 9042)
            
            cluster_config = {
                'contact_points': hosts,
                'port': port,
                'protocol_version': self.connection_config.get('protocol_version', 4)
            }
            
            # Add authentication if provided
            if self.connection_config.get('username') and self.connection_config.get('password'):
                auth_provider = PlainTextAuthProvider(
                    username=self.connection_config['username'],
                    password=self.connection_config['password']
                )
                cluster_config['auth_provider'] = auth_provider
            
            # Add load balancing policy
            if self.connection_config.get('datacenter'):
                cluster_config['load_balancing_policy'] = DCAwareRoundRobinPolicy(
                    local_dc=self.connection_config['datacenter']
                )
            
            self.cluster = Cluster(**cluster_config)
            self.session = self.cluster.connect()
            
            # Test connection by getting cluster metadata
            cluster_name = self.cluster.metadata.cluster_name
            
            # Set keyspace if provided
            if self.connection_config.get('keyspace'):
                self.session.set_keyspace(self.connection_config['keyspace'])
            
            logger.info(f"Successfully connected to Cassandra cluster: {cluster_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {str(e)}")
            return False
    
    async def disconnect(self) -> bool:
        """Close Cassandra connections"""
        try:
            if self.session:
                self.session.shutdown()
                self.session = None
            if self.cluster:
                self.cluster.shutdown()
                self.cluster = None
            return True
        except Exception as e:
            logger.error(f"Failed to disconnect from Cassandra: {str(e)}")
            return False
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test Cassandra connection and return metadata"""
        try:
            if not await self.connect():
                return {
                    "status": "error",
                    "error": "Failed to connect to Cassandra"
                }
            
            # Get cluster info
            cluster_name = self.cluster.metadata.cluster_name
            keyspaces = list(self.cluster.metadata.keyspaces.keys())
            
            return {
                "status": "success",
                "database_type": "cassandra",
                "cluster_name": cluster_name,
                "keyspace_count": len(keyspaces),
                "connection_info": self.get_connection_info()
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_schema_info(self) -> List[Dict[str, Any]]:
        """Get available Cassandra keyspaces and tables."""
        try:
            if not self.session:
                await self.connect()
            
            schema_info = []
            
            # Get keyspaces
            keyspaces = self.cluster.metadata.keyspaces
            
            for keyspace_name, keyspace in keyspaces.items():
                # Skip system keyspaces
                if keyspace_name.startswith('system'):
                    continue
                
                # Get tables in keyspace
                for table_name, table in keyspace.tables.items():
                    try:
                        # Get approximate row count (this is expensive in Cassandra)
                        row_count = 0  # Cassandra doesn't provide easy row counts
                        
                        schema_info.append({
                            'name': f"{keyspace_name}.{table_name}",
                            'type': 'table',
                            'keyspace': keyspace_name,
                            'table': table_name,
                            'row_count': row_count,
                            'column_count': len(table.columns),
                            'description': f'Cassandra table with {len(table.columns)} columns'
                        })
                        
                    except Exception as e:
                        logger.warning(f"Could not get info for table {keyspace_name}.{table_name}: {str(e)}")
                        schema_info.append({
                            'name': f"{keyspace_name}.{table_name}",
                            'type': 'table',
                            'keyspace': keyspace_name,
                            'table': table_name,
                            'row_count': 0,
                            'column_count': 0,
                            'description': 'Cassandra table (info unavailable)'
                        })
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to get Cassandra schema: {str(e)}")
            return []
    
    async def preview_data(self, source_name: str, limit: int = 100) -> Dict[str, Any]:
        """Preview data from a Cassandra table."""
        try:
            if not self.session:
                await self.connect()
            
            # Parse table name (keyspace.table or just table)
            if '.' in source_name:
                keyspace, table = source_name.split('.', 1)
                full_table_name = f"{keyspace}.{table}"
            else:
                table = source_name
                full_table_name = table
            
            # Execute sample query
            query = f"SELECT * FROM {full_table_name} LIMIT {limit}"
            rows = self.session.execute(query)
            
            # Convert to list of dictionaries
            data = []
            columns_info = []
            
            if rows:
                # Get column names from first row
                first_row = None
                for row in rows:
                    if first_row is None:
                        first_row = row
                        # Get column metadata
                        for col_name in row._fields:
                            columns_info.append({
                                'name': col_name,
                                'sql_type': 'TEXT'  # Simplified type mapping
                            })
                    
                    # Convert row to dictionary
                    row_dict = {}
                    for i, field_name in enumerate(row._fields):
                        value = row[i]
                        # Convert special types to strings for JSON serialization
                        if hasattr(value, 'isoformat'):  # datetime-like objects
                            value = value.isoformat()
                        elif not isinstance(value, (str, int, float, bool, type(None))):
                            value = str(value)
                        row_dict[field_name] = value
                    
                    data.append(row_dict)
            
            return {
                'status': 'success',
                'columns': columns_info,
                'sample_data': data,
                'row_count': len(data)
            }
            
        except Exception as e:
            logger.error(f"Failed to preview Cassandra table {source_name}: {str(e)}")
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
        """Extract data from Cassandra table"""
        try:
            if not self.session:
                await self.connect()
                
            config = ExtractionConfig.from_dict(extraction_config)
            chunk_size = chunk_size or config.chunk_size
            
            # Parse table name
            if '.' in source:
                keyspace, table = source.split('.', 1)
                full_table_name = f"{keyspace}.{table}"
            else:
                table = source
                full_table_name = table
            
            # Build query
            base_query = f"SELECT * FROM {full_table_name}"
            
            # Add incremental filtering if specified
            if config.mode == "incremental" and config.incremental_column and config.last_value:
                base_query += f" WHERE {config.incremental_column} > '{config.last_value}'"
            
            # Execute query with paging
            query = base_query
            statement = self.session.prepare(query)
            statement.fetch_size = chunk_size
            
            rows = self.session.execute(statement)
            
            current_chunk = []
            for row in rows:
                # Convert row to dictionary
                row_dict = {}
                for i, field_name in enumerate(row._fields):
                    value = row[i]
                    # Convert special types
                    if hasattr(value, 'isoformat'):
                        value = value.isoformat()
                    elif not isinstance(value, (str, int, float, bool, type(None))):
                        value = str(value)
                    row_dict[field_name] = value
                
                current_chunk.append(row_dict)
                
                # Yield chunk when it reaches the specified size
                if len(current_chunk) >= chunk_size:
                    df = pd.DataFrame(current_chunk)
                    yield df
                    current_chunk = []
            
            # Yield remaining data
            if current_chunk:
                df = pd.DataFrame(current_chunk)
                yield df
            
        except Exception as e:
            logger.error(f"Failed to extract data from Cassandra table {source}: {str(e)}")
            raise
    
    async def supports_incremental_extraction(self) -> bool:
        """Cassandra supports limited incremental extraction."""
        return True
    
    async def supports_real_time_sync(self) -> bool:
        """Cassandra doesn't support real-time sync."""
        return False
    
    async def get_incremental_key_columns(self, source: str) -> List[str]:
        """Get fields suitable for incremental extraction."""
        try:
            if not self.session:
                await self.connect()
            
            # Parse table name
            if '.' in source:
                keyspace, table = source.split('.', 1)
            else:
                keyspace = self.connection_config.get('keyspace', 'system')
                table = source
            
            # Get table metadata
            table_metadata = self.cluster.metadata.keyspaces[keyspace].tables[table]
            
            incremental_fields = []
            
            # Look for timestamp columns
            for column_name, column in table_metadata.columns.items():
                column_type = str(column.cql_type).lower()
                
                if 'timestamp' in column_type or 'timeuuid' in column_type:
                    incremental_fields.append(column_name)
                elif column_name.lower() in ['created_at', 'updated_at', 'time']:
                    incremental_fields.append(column_name)
            
            # Add partition key as fallback
            for key in table_metadata.partition_key:
                if key.name not in incremental_fields:
                    incremental_fields.append(key.name)
            
            return incremental_fields
            
        except Exception as e:
            logger.error(f"Failed to get incremental fields: {str(e)}")
            return []
    
    async def get_record_count(self, source: str, filters: Optional[Dict] = None) -> int:
        """Get row count for a table (expensive operation in Cassandra)"""
        try:
            if not self.session:
                await self.connect()
                
            # Parse table name
            if '.' in source:
                keyspace, table = source.split('.', 1)
                full_table_name = f"{keyspace}.{table}"
            else:
                table = source
                full_table_name = table
                
            # Note: COUNT(*) is expensive in Cassandra and should be avoided for large tables
            query = f"SELECT COUNT(*) FROM {full_table_name}"
            if filters:
                # Basic filter support (limited in Cassandra)
                where_clauses = []
                for field, value in filters.items():
                    if isinstance(value, str):
                        where_clauses.append(f"{field} = '{value}'")
                    else:
                        where_clauses.append(f"{field} = {value}")
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)
                    
            result = self.session.execute(query)
            return result.one()[0] if result else 0
            
        except Exception as e:
            logger.error(f"Failed to get row count for table {source}: {str(e)}")
            return 0
    
    def get_required_config_fields(self) -> List[str]:
        """Return required configuration fields for Cassandra"""
        return ['host']