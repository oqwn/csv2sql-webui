from typing import Dict, List, Any, Optional, TYPE_CHECKING
import time
from sqlalchemy import text
from app.services.data_extraction.extraction_manager import DataExtractionManager

if TYPE_CHECKING:
    from app.services.transaction_manager import TransactionContext

class DataSourceSQLExecutor:
    """Execute SQL queries against a connected data source"""
    
    def __init__(self, data_source_type: str, connection_config: Dict[str, Any], transaction_context: Optional['TransactionContext'] = None):
        self.data_source_type = data_source_type
        self.connection_config = connection_config
        self.extraction_manager = DataExtractionManager()
        self.transaction_context = transaction_context
        
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
                    # Begin transaction explicitly
                    trans = conn.begin()
                    
                    try:
                        result = conn.execute(text(query))
                        
                        # Check if this is a query that returns data
                        query_upper = query.strip().upper()
                        is_select_query = query_upper.startswith('SELECT') or query_upper.startswith('WITH')
                        
                        if is_select_query:
                            # Get column names
                            columns = list(result.keys()) if hasattr(result, 'keys') else []
                            
                            # Fetch rows
                            rows = []
                            for row in result:
                                rows.append(list(row))
                            
                            row_count = len(rows)
                        else:
                            # DDL/DML statements (INSERT, UPDATE, DELETE, DROP, CREATE, etc.)
                            columns = []
                            rows = []
                            row_count = result.rowcount if hasattr(result, 'rowcount') else 0
                        
                        # Commit the transaction for DDL/DML statements
                        if not is_select_query:
                            trans.commit()
                        else:
                            trans.rollback()  # Don't commit SELECT queries
                        
                        execution_time = time.time() - start_time
                        
                    except Exception as e:
                        trans.rollback()
                        raise e
                    
                    await connector.disconnect()
                    
                    return {
                        'columns': columns,
                        'rows': rows,
                        'row_count': row_count,
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
    
    async def begin_transaction(self) -> Any:
        """Begin a database transaction and return the connection"""
        if self.data_source_type not in ['mysql', 'postgresql', 'sqlite', 'mssql', 'oracle']:
            raise ValueError(f"Transactions not supported for {self.data_source_type}")
        
        # Get connector
        connector = self.extraction_manager.get_connector(
            self.data_source_type,
            self.connection_config
        )
        
        await connector.connect()
        connection = connector.engine.connect()
        transaction = connection.begin()
        
        return {
            'connection': connection,
            'transaction': transaction,
            'connector': connector
        }
    
    async def execute_in_transaction(self, query: str, connection_info: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a query within an existing transaction"""
        start_time = time.time()
        
        try:
            connection = connection_info['connection']
            result = connection.execute(text(query))
            
            # Check if this is a query that returns data
            query_upper = query.strip().upper()
            is_select_query = query_upper.startswith('SELECT') or query_upper.startswith('WITH')
            
            if is_select_query:
                # Get column names
                columns = list(result.keys()) if hasattr(result, 'keys') else []
                
                # Fetch rows
                rows = []
                for row in result:
                    rows.append(list(row))
                
                row_count = len(rows)
            else:
                # DDL/DML statements
                columns = []
                rows = []
                row_count = result.rowcount if hasattr(result, 'rowcount') else 0
            
            execution_time = time.time() - start_time
            
            return {
                'columns': columns,
                'rows': rows,
                'row_count': row_count,
                'execution_time': execution_time,
                'error': None
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
    
    async def commit_transaction(self, connection_info: Dict[str, Any]):
        """Commit the transaction and cleanup resources"""
        try:
            transaction = connection_info['transaction']
            connection = connection_info['connection']
            connector = connection_info['connector']
            
            transaction.commit()
            connection.close()
            await connector.disconnect()
        except Exception as e:
            raise e
    
    async def rollback_transaction(self, connection_info: Dict[str, Any]):
        """Rollback the transaction and cleanup resources"""
        try:
            transaction = connection_info['transaction']
            connection = connection_info['connection']
            connector = connection_info['connector']
            
            transaction.rollback()
            connection.close()
            await connector.disconnect()
        except Exception as e:
            # Still try to cleanup even if rollback fails
            try:
                connection.close()
                await connector.disconnect()
            except:
                pass
            raise e