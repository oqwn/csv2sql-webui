import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text, inspect
from typing import Dict, Any, List, Optional, Iterator
from .base_connector import DataSourceConnector, ExtractionConfig
import logging
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)


class RelationalDatabaseConnector(DataSourceConnector):
    """Connector for relational databases (MySQL, PostgreSQL, SQLite, etc.)"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self.engine = None
        self.db_type = connection_config.get('type', 'postgresql')
        
    async def connect(self) -> bool:
        """Establish connection to the database"""
        try:
            connection_string = self._build_connection_string()
            self.engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            
            # Test the connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info(f"Successfully connected to {self.db_type} database")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            return False
    
    async def disconnect(self) -> bool:
        """Close database connection"""
        try:
            if self.engine:
                self.engine.dispose()
                self.engine = None
            return True
        except Exception as e:
            logger.error(f"Failed to disconnect: {str(e)}")
            return False
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test connection and return database metadata"""
        try:
            if not self.engine:
                await self.connect()
                
            with self.engine.connect() as conn:
                # Get database version
                version_query = self._get_version_query()
                result = conn.execute(text(version_query)).fetchone()
                version = result[0] if result else "Unknown"
                
                response = {
                    "status": "success",
                    "database_type": self.db_type,
                    "version": version,
                    "connection_info": self.get_connection_info()
                }
                
                # If database is specified, get table count
                if self.connection_config.get('database'):
                    inspector = inspect(self.engine)
                    table_names = inspector.get_table_names()
                    response["table_count"] = len(table_names)
                else:
                    # If no database specified, return available databases
                    databases = await self.get_available_databases()
                    response["available_databases"] = databases
                    response["database_count"] = len(databases)
                
                return response
                
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_schema_info(self) -> List[Dict[str, Any]]:
        """Get database schema information"""
        try:
            if not self.engine:
                await self.connect()
                
            inspector = inspect(self.engine)
            schema_info = []
            
            for table_name in inspector.get_table_names():
                columns = inspector.get_columns(table_name)
                
                schema_info.append({
                    "name": table_name,
                    "type": "table",
                    "columns": [
                        {
                            "name": col["name"],
                            "type": str(col["type"]),
                            "nullable": col.get("nullable", True),
                            "primary_key": col.get("primary_key", False)
                        }
                        for col in columns
                    ],
                    "row_count": await self.get_record_count(table_name)
                })
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to get schema info: {str(e)}")
            return []
    
    async def extract_data(
        self,
        source: str,
        extraction_config: Dict[str, Any],
        chunk_size: Optional[int] = None
    ) -> Iterator[pd.DataFrame]:
        """Extract data from database table"""
        try:
            if not self.engine:
                await self.connect()
                
            config = ExtractionConfig.from_dict(extraction_config)
            chunk_size = chunk_size or config.chunk_size
            
            # Build the query
            query = self._build_extraction_query(source, config)
            
            # Execute query in chunks
            with self.engine.connect() as conn:
                for chunk_df in pd.read_sql(
                    query,
                    conn,
                    chunksize=chunk_size
                ):
                    yield chunk_df
                    
        except Exception as e:
            logger.error(f"Failed to extract data from {source}: {str(e)}")
            raise
    
    async def get_record_count(self, source: str, filters: Optional[Dict] = None) -> int:
        """Get record count for a table"""
        try:
            if not self.engine:
                await self.connect()
                
            with self.engine.connect() as conn:
                query = f"SELECT COUNT(*) FROM {source}"
                if filters:
                    where_clause = self._build_where_clause(filters)
                    if where_clause:
                        query += f" WHERE {where_clause}"
                        
                result = conn.execute(text(query)).fetchone()
                return result[0] if result else 0
                
        except Exception as e:
            logger.error(f"Failed to get record count: {str(e)}")
            return 0
    
    def get_required_config_fields(self) -> List[str]:
        """Return required configuration fields"""
        if self.db_type == 'sqlite':
            return ['type', 'database']  # SQLite only needs database file path
        else:
            return ['type', 'host', 'username']  # Password and database are optional
    
    async def supports_incremental_extraction(self) -> bool:
        """Relational databases support incremental extraction"""
        return True
    
    async def get_incremental_key_columns(self, source: str) -> List[str]:
        """Get columns suitable for incremental extraction"""
        try:
            if not self.engine:
                await self.connect()
                
            inspector = inspect(self.engine)
            columns = inspector.get_columns(source)
            
            incremental_columns = []
            for col in columns:
                col_type = str(col["type"]).lower()
                # Look for timestamp, datetime, or auto-incrementing columns
                if any(t in col_type for t in ['timestamp', 'datetime', 'date', 'serial', 'auto_increment']):
                    incremental_columns.append(col["name"])
                elif col.get("primary_key") and 'int' in col_type:
                    incremental_columns.append(col["name"])
                    
            return incremental_columns
            
        except Exception as e:
            logger.error(f"Failed to get incremental columns: {str(e)}")
            return []
    
    async def get_available_databases(self) -> List[str]:
        """Get list of available databases"""
        try:
            if not self.engine:
                await self.connect()
                
            with self.engine.connect() as conn:
                if self.db_type == 'mysql':
                    result = conn.execute(text("SHOW DATABASES"))
                    databases = [row[0] for row in result]
                    # Filter out system databases
                    return [db for db in databases if db not in ['information_schema', 'mysql', 'performance_schema', 'sys']]
                    
                elif self.db_type == 'postgresql':
                    result = conn.execute(text(
                        "SELECT datname FROM pg_database WHERE datistemplate = false"
                    ))
                    return [row[0] for row in result]
                    
                elif self.db_type == 'mssql':
                    result = conn.execute(text(
                        "SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')"
                    ))
                    return [row[0] for row in result]
                    
                elif self.db_type == 'oracle':
                    # Oracle uses schemas instead of databases
                    result = conn.execute(text(
                        "SELECT username FROM all_users WHERE username NOT IN "
                        "('SYS', 'SYSTEM', 'DBSNMP', 'SYSMAN', 'OUTLN', 'FLOWS_FILES', 'MDSYS', 'ORDSYS', 'EXFSYS', 'CTXSYS', 'XDB', 'ANONYMOUS')"
                    ))
                    return [row[0] for row in result]
                    
                else:
                    return []
                    
        except Exception as e:
            logger.error(f"Failed to get available databases: {str(e)}")
            return []
    
    def _build_connection_string(self) -> str:
        """Build database connection string"""
        config = self.connection_config
        
        if self.db_type == 'sqlite':
            return f"sqlite:///{config['database']}"
        
        # Encode password to handle special characters
        password = quote_plus(config['password']) if config.get('password') else ''
        username = config['username']
        host = config['host']
        port = config.get('port', self._get_default_port())
        database = config.get('database', '')  # Database is optional
        
        # Build auth part
        auth = f"{username}:{password}" if password else username
        
        if self.db_type == 'mysql':
            driver = config.get('driver', 'pymysql')
            base_url = f"mysql+{driver}://{auth}@{host}:{port}"
            return f"{base_url}/{database}" if database else base_url
        elif self.db_type == 'postgresql':
            driver = config.get('driver', 'psycopg2')
            base_url = f"postgresql+{driver}://{auth}@{host}:{port}"
            # PostgreSQL requires at least a database name (default to 'postgres' for listing)
            return f"{base_url}/{database}" if database else f"{base_url}/postgres"
        elif self.db_type == 'mssql':
            driver = config.get('driver', 'pyodbc')
            base_url = f"mssql+{driver}://{auth}@{host}:{port}"
            url = f"{base_url}/{database}" if database else base_url
            return f"{url}?driver=ODBC+Driver+17+for+SQL+Server"
        elif self.db_type == 'oracle':
            driver = config.get('driver', 'cx_oracle')
            base_url = f"oracle+{driver}://{auth}@{host}:{port}"
            return f"{base_url}/{database}" if database else base_url
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    def _get_default_port(self) -> int:
        """Get default port for database type"""
        ports = {
            'mysql': 3306,
            'postgresql': 5432,
            'mssql': 1433,
            'oracle': 1521
        }
        return ports.get(self.db_type, 5432)
    
    def _get_version_query(self) -> str:
        """Get version query for different database types"""
        queries = {
            'mysql': "SELECT VERSION()",
            'postgresql': "SELECT version()",
            'sqlite': "SELECT sqlite_version()",
            'mssql': "SELECT @@VERSION",
            'oracle': "SELECT * FROM v$version WHERE banner LIKE 'Oracle%'"
        }
        return queries.get(self.db_type, "SELECT 'Unknown' as version")
    
    def _build_extraction_query(self, source: str, config: ExtractionConfig) -> str:
        """Build SQL query for data extraction"""
        # Select columns
        if config.columns:
            columns_str = ", ".join(config.columns)
        else:
            columns_str = "*"
            
        query = f"SELECT {columns_str} FROM {source}"
        
        # Add WHERE clause for filters and incremental extraction
        where_conditions = []
        
        if config.filters:
            filter_clause = self._build_where_clause(config.filters)
            if filter_clause:
                where_conditions.append(filter_clause)
        
        if config.mode == "incremental" and config.incremental_column and config.last_value:
            where_conditions.append(f"{config.incremental_column} > '{config.last_value}'")
        
        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)
        
        # Add ORDER BY
        if config.order_by:
            query += f" ORDER BY {config.order_by}"
        elif config.incremental_column:
            query += f" ORDER BY {config.incremental_column}"
            
        return query
    
    def _build_where_clause(self, filters: Dict[str, Any]) -> str:
        """Build WHERE clause from filters"""
        conditions = []
        
        for column, value in filters.items():
            if isinstance(value, dict):
                # Handle operators like {"gt": 100}, {"in": [1,2,3]}
                for op, val in value.items():
                    if op == "gt":
                        conditions.append(f"{column} > '{val}'")
                    elif op == "lt":
                        conditions.append(f"{column} < '{val}'")
                    elif op == "gte":
                        conditions.append(f"{column} >= '{val}'")
                    elif op == "lte":
                        conditions.append(f"{column} <= '{val}'")
                    elif op == "in" and isinstance(val, list):
                        val_str = "', '".join(str(v) for v in val)
                        conditions.append(f"{column} IN ('{val_str}')")
                    elif op == "like":
                        conditions.append(f"{column} LIKE '{val}'")
            else:
                conditions.append(f"{column} = '{value}'")
                
        return " AND ".join(conditions)