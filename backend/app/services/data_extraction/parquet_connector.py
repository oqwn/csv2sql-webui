import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from typing import Dict, Any, List, Optional, Iterator
from .base_connector import DataSourceConnector, ExtractionConfig
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    pq = None
    pa = None


class ParquetConnector(DataSourceConnector):
    """Connector for Apache Parquet files"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        if not PYARROW_AVAILABLE:
            raise ImportError("PyArrow is required for Parquet support. Install with: pip install pyarrow")
        
        super().__init__(connection_config)
        self.parquet_file = None
        self.table = None
        
    async def connect(self) -> bool:
        """Test connection to Parquet file."""
        try:
            file_path = self.connection_config.get('file_path')
            if not file_path:
                logger.error("Parquet file path not specified")
                return False
            
            if not Path(file_path).exists():
                logger.error(f"Parquet file not found: {file_path}")
                return False
            
            # Try to read parquet metadata
            self.parquet_file = pq.ParquetFile(file_path)
            self.table = self.parquet_file.read()
            
            logger.info(f"Successfully connected to Parquet file: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Parquet file: {str(e)}")
            return False
    
    async def disconnect(self) -> bool:
        """Clean up Parquet connection"""
        self.parquet_file = None
        self.table = None
        return True
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test Parquet file connection and return metadata"""
        try:
            if not await self.connect():
                return {
                    "status": "error",
                    "error": "Failed to connect to Parquet file"
                }
            
            # Get parquet metadata
            metadata = self.parquet_file.metadata
            schema = self.parquet_file.schema
            
            return {
                "status": "success",
                "database_type": "parquet",
                "file_size": metadata.serialized_size,
                "row_count": self.table.num_rows,
                "column_count": self.table.num_columns,
                "num_row_groups": metadata.num_row_groups,
                "columns": [col.name for col in schema],
                "parquet_version": metadata.format_version,
                "connection_info": self.get_connection_info()
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_schema_info(self) -> List[Dict[str, Any]]:
        """Get Parquet file schema information."""
        try:
            if not self.table:
                await self.connect()
            
            file_path = self.connection_config.get('file_path')
            file_name = Path(file_path).stem if file_path else 'parquet_data'
            
            schema_info = [{
                'name': file_name,
                'type': 'parquet_table',
                'row_count': self.table.num_rows,
                'column_count': self.table.num_columns,
                'description': f'Parquet file with {self.table.num_rows} rows and {self.table.num_columns} columns'
            }]
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to get Parquet schema: {str(e)}")
            return []
    
    async def preview_data(self, source_name: str, limit: int = 100) -> Dict[str, Any]:
        """Preview data from Parquet file."""
        try:
            if not self.table:
                await self.connect()
            
            # Convert to pandas for preview (limited rows)
            df = self.table.slice(0, min(limit, self.table.num_rows)).to_pandas()
            
            # Get column information with Parquet-specific types
            columns_info = []
            schema = self.parquet_file.schema
            
            for i, col in enumerate(schema):
                pandas_col = df.columns[i]
                columns_info.append({
                    'name': col.name,
                    'parquet_type': str(col.physical_type),
                    'logical_type': str(col.logical_type) if col.logical_type else None,
                    'sql_type': self._arrow_to_sql_type(col.physical_type)
                })
            
            # Convert to records for JSON serialization
            sample_data = df.to_dict(orient='records')
            
            return {
                'status': 'success',
                'columns': columns_info,
                'sample_data': sample_data,
                'row_count': len(df)
            }
            
        except Exception as e:
            logger.error(f"Failed to preview Parquet data: {str(e)}")
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
        """Extract data from Parquet file"""
        try:
            if not self.table:
                await self.connect()
            
            config = ExtractionConfig.from_dict(extraction_config)
            chunk_size = chunk_size or config.chunk_size
            
            # Select specific columns if specified
            columns = None
            if config.columns:
                available_columns = [col for col in config.columns if col in self.table.column_names]
                if available_columns:
                    columns = available_columns
            
            # Read data in chunks using PyArrow's batch reading
            total_rows = self.table.num_rows
            
            for start_idx in range(0, total_rows, chunk_size):
                end_idx = min(start_idx + chunk_size, total_rows)
                
                # Slice the table
                table_slice = self.table.slice(start_idx, end_idx - start_idx)
                
                # Select columns if specified
                if columns:
                    table_slice = table_slice.select(columns)
                
                # Convert to pandas
                df = table_slice.to_pandas()
                
                # Apply filters if specified
                if config.filters:
                    df = self._apply_filters(df, config.filters)
                
                # Skip empty chunks
                if not df.empty:
                    yield df
            
        except Exception as e:
            logger.error(f"Failed to extract Parquet data: {str(e)}")
            raise
    
    async def supports_incremental_extraction(self) -> bool:
        """Parquet files support incremental extraction if they have timestamp columns."""
        try:
            if not self.table:
                await self.connect()
            
            # Check if any columns look like timestamps
            schema = self.parquet_file.schema
            for col in schema:
                if 'timestamp' in str(col.physical_type).lower() or 'date' in str(col.physical_type).lower():
                    return True
                if any(keyword in col.name.lower() for keyword in ['time', 'date', 'created', 'updated']):
                    return True
            
            return False
            
        except Exception:
            return False
    
    async def supports_real_time_sync(self) -> bool:
        """Parquet files don't support real-time sync."""
        return False
    
    async def get_incremental_key_columns(self, source: str) -> List[str]:
        """Get columns suitable for incremental extraction."""
        try:
            if not self.table:
                await self.connect()
            
            incremental_columns = []
            schema = self.parquet_file.schema
            
            for col in schema:
                # Check data type
                if 'timestamp' in str(col.physical_type).lower() or 'date' in str(col.physical_type).lower():
                    incremental_columns.append(col.name)
                # Check column name patterns
                elif any(keyword in col.name.lower() for keyword in ['time', 'date', 'created', 'updated', 'timestamp']):
                    incremental_columns.append(col.name)
            
            return incremental_columns
            
        except Exception as e:
            logger.error(f"Failed to get incremental columns: {str(e)}")
            return []
    
    async def get_record_count(self, source: str, filters: Optional[Dict] = None) -> int:
        """Get record count for Parquet file"""
        try:
            if not self.table:
                await self.connect()
            
            if not filters:
                return self.table.num_rows
            
            # For filtered count, we need to read and filter
            # This could be optimized with PyArrow compute functions
            df = self.table.to_pandas()
            df = self._apply_filters(df, filters)
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to get record count: {str(e)}")
            return 0
    
    def get_required_config_fields(self) -> List[str]:
        """Return required configuration fields for Parquet"""
        return ['file_path']
    
    def _arrow_to_sql_type(self, arrow_type) -> str:
        """Convert PyArrow type to SQL type"""
        type_str = str(arrow_type).lower()
        
        if 'int' in type_str:
            return 'INTEGER'
        elif 'float' in type_str or 'double' in type_str:
            return 'REAL'
        elif 'bool' in type_str:
            return 'BOOLEAN'
        elif 'timestamp' in type_str or 'date' in type_str:
            return 'TIMESTAMP'
        elif 'time' in type_str:
            return 'TIME'
        elif 'string' in type_str or 'utf8' in type_str:
            return 'TEXT'
        elif 'binary' in type_str:
            return 'BLOB'
        else:
            return 'TEXT'
    
    def _apply_filters(self, df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
        """Apply basic filters to DataFrame"""
        for column, value in filters.items():
            if column in df.columns:
                df = df[df[column] == value]
        return df