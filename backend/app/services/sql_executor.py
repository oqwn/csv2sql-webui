from typing import Dict, List, Any, Optional
import time
from sqlalchemy import create_engine, text, inspect
from app.services.data_extraction.extraction_manager import DataExtractionManager

class DataSourceSQLExecutor:
    """Execute SQL queries against a connected data source"""
    
    def __init__(self, data_source_type: str, connection_config: Dict[str, Any]):
        self.data_source_type = data_source_type
        self.connection_config = connection_config
        self.extraction_manager = DataExtractionManager()
        
    async def execute_query(self, query: str) -> Dict[str, Any]:
        """Execute a SQL query and return results"""
        start_time = time.time()
        
        try:
            # Get connector
            connector = self.extraction_manager.get_connector(
                self.data_source_type,
                self.connection_config
            )
            
            # For relational databases, we can execute SQL directly
            if self.data_source_type in ['mysql', 'postgresql', 'sqlite', 'mssql', 'oracle']:
                await connector.connect()
                
                with connector.engine.connect() as conn:
                    result = conn.execute(text(query))
                    
                    # Get column names
                    columns = list(result.keys()) if hasattr(result, 'keys') else []
                    
                    # Fetch rows
                    rows = []
                    for row in result:
                        rows.append(list(row))
                    
                    execution_time = time.time() - start_time
                    
                    await connector.disconnect()
                    
                    return {
                        'columns': columns,
                        'rows': rows,
                        'row_count': len(rows),
                        'execution_time': execution_time,
                        'error': None
                    }
            else:
                # For non-relational databases, we need different handling
                return {
                    'columns': [],
                    'rows': [],
                    'row_count': 0,
                    'execution_time': 0,
                    'error': f"SQL execution not supported for {self.data_source_type}. Use the data extraction features instead."
                }
                
        except Exception as e:
            execution_time = time.time() - start_time
            return {
                'columns': [],
                'rows': [],
                'row_count': 0,
                'execution_time': execution_time,
                'error': str(e)
            }
    
    async def list_tables(self) -> List[Dict[str, Any]]:
        """List all tables/collections in the data source"""
        try:
            # Get connector
            connector = self.extraction_manager.get_connector(
                self.data_source_type,
                self.connection_config
            )
            
            # Get schema info
            schema_info = await connector.get_schema_info()
            
            # Format as table info
            tables = []
            for item in schema_info:
                tables.append({
                    'name': item['name'],
                    'type': item.get('type', 'table'),
                    'row_count': item.get('row_count'),
                    'columns': item.get('columns', [])
                })
            
            return tables
            
        except Exception as e:
            print(f"Error listing tables: {str(e)}")
            return []
    
    async def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific table"""
        try:
            tables = await self.list_tables()
            for table in tables:
                if table['name'] == table_name:
                    return table
            return None
            
        except Exception as e:
            print(f"Error getting table info: {str(e)}")
            return None