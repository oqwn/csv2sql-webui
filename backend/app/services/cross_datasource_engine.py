import pandas as pd
from typing import Dict, Any, List

from app.services.local_storage import local_storage
from app.services.sql_executor import DataSourceSQLExecutor
from app.services.transformation_engine import TransformationEngine
from app.models.transformation_types import (
    TransformationType, TransformationStep, JoinConfig, JoinType
)


class CrossDataSourceEngine:
    """Engine for handling cross-datasource operations like joins"""
    
    def __init__(self):
        self.transformation_engine = TransformationEngine()
    
    async def execute_pipeline(self, source_config: Dict[str, Any], steps: List[TransformationStep]) -> pd.DataFrame:
        """Execute a transformation pipeline that may include cross-datasource operations"""
        
        # First, load the primary source data
        primary_df = await self._load_source_data(source_config)
        current_df = primary_df
        
        # Process each transformation step
        for step in steps:
            if step.type == TransformationType.JOIN:
                # Handle cross-datasource join
                current_df = await self._apply_cross_join(current_df, step.config, source_config)
            else:
                # Use regular transformation engine for other operations
                current_df = await self.transformation_engine.apply_transformations(
                    current_df, [step]
                )
        
        return current_df
    
    async def _load_source_data(self, source_config: Dict[str, Any]) -> pd.DataFrame:
        """Load data from a single data source"""
        data_source = local_storage.get_data_source(source_config['datasource_id'])
        if not data_source:
            raise ValueError(f"Data source {source_config['datasource_id']} not found")
        
        executor = DataSourceSQLExecutor(
            data_source['type'],
            data_source['connection_config']
        )
        
        # Build query
        if 'query' in source_config:
            query = source_config['query']
        else:
            table_name = source_config['table_name']
            query = f'SELECT * FROM "{table_name}"'
        
        # Execute query
        result = await executor.execute_query(query)
        if result['error']:
            raise ValueError(f"Failed to load data: {result['error']}")
        
        # Convert to DataFrame
        return pd.DataFrame(result['rows'], columns=result['columns'])
    
    async def _apply_cross_join(self, left_df: pd.DataFrame, config: Dict[str, Any], 
                               left_source_config: Dict[str, Any]) -> pd.DataFrame:
        """Apply join operation across different data sources"""
        join_config = JoinConfig(**config)
        
        # Determine if this is a cross-datasource join
        left_datasource_id = join_config.left_source.get('datasource_id', left_source_config.get('datasource_id'))
        right_datasource_id = join_config.right_source['datasource_id']
        
        # Load right side data
        right_df = await self._load_source_data(join_config.right_source)
        
        # Prepare join keys
        left_keys = []
        right_keys = []
        for condition in join_config.join_conditions:
            left_keys.append(condition['left'])
            right_keys.append(condition['right'])
        
        # Validate join columns exist
        for key in left_keys:
            if key not in left_df.columns:
                raise ValueError(f"Join column '{key}' not found in left dataset")
        
        for key in right_keys:
            if key not in right_df.columns:
                raise ValueError(f"Join column '{key}' not found in right dataset")
        
        # Add source indicators if joining same column names
        if left_datasource_id != right_datasource_id:
            # Prefix columns to avoid conflicts (except join keys)
            left_prefix = f"ds{left_datasource_id}_"
            right_prefix = f"ds{right_datasource_id}_"
            
            # Rename columns that are not join keys
            left_rename = {col: f"{left_prefix}{col}" for col in left_df.columns if col not in left_keys}
            right_rename = {col: f"{right_prefix}{col}" for col in right_df.columns if col not in right_keys}
            
            left_df = left_df.rename(columns=left_rename)
            right_df = right_df.rename(columns=right_rename)
        
        # Perform join based on type
        if join_config.join_type == JoinType.INNER:
            result_df = pd.merge(
                left_df, right_df,
                left_on=left_keys, right_on=right_keys,
                how='inner'
            )
        elif join_config.join_type == JoinType.LEFT:
            result_df = pd.merge(
                left_df, right_df,
                left_on=left_keys, right_on=right_keys,
                how='left'
            )
        elif join_config.join_type == JoinType.RIGHT:
            result_df = pd.merge(
                left_df, right_df,
                left_on=left_keys, right_on=right_keys,
                how='right'
            )
        elif join_config.join_type == JoinType.FULL:
            result_df = pd.merge(
                left_df, right_df,
                left_on=left_keys, right_on=right_keys,
                how='outer'
            )
        elif join_config.join_type == JoinType.CROSS:
            # Cross join (cartesian product)
            left_df['_cross_join_key'] = 1
            right_df['_cross_join_key'] = 1
            result_df = pd.merge(
                left_df, right_df,
                on='_cross_join_key',
                how='inner'
            )
            result_df = result_df.drop('_cross_join_key', axis=1)
        else:
            raise ValueError(f"Unsupported join type: {join_config.join_type}")
        
        # Remove duplicate key columns if they have different names
        if left_keys != right_keys:
            # Keep only the left side keys
            cols_to_drop = [col for col in right_keys if col in result_df.columns and col not in left_keys]
            if cols_to_drop:
                result_df = result_df.drop(columns=cols_to_drop)
        
        return result_df
    
    async def validate_join_compatibility(self, join_config: JoinConfig) -> Dict[str, Any]:
        """Validate if a join operation is feasible between two data sources"""
        validation_result = {
            "valid": True,
            "warnings": [],
            "errors": [],
            "estimated_size": None
        }
        
        try:
            # Load sample data from both sources
            left_sample = await self._load_source_data({
                **join_config.left_source,
                'query': f'SELECT * FROM "{join_config.left_source["table_name"]}" LIMIT 100'
            })
            
            right_sample = await self._load_source_data({
                **join_config.right_source,
                'query': f'SELECT * FROM "{join_config.right_source["table_name"]}" LIMIT 100'
            })
            
            # Validate join columns exist
            for condition in join_config.join_conditions:
                if condition['left'] not in left_sample.columns:
                    validation_result['valid'] = False
                    validation_result['errors'].append(
                        f"Column '{condition['left']}' not found in left table"
                    )
                
                if condition['right'] not in right_sample.columns:
                    validation_result['valid'] = False
                    validation_result['errors'].append(
                        f"Column '{condition['right']}' not found in right table"
                    )
            
            # Check data type compatibility
            for condition in join_config.join_conditions:
                if (condition['left'] in left_sample.columns and 
                    condition['right'] in right_sample.columns):
                    
                    left_dtype = left_sample[condition['left']].dtype
                    right_dtype = right_sample[condition['right']].dtype
                    
                    if left_dtype != right_dtype:
                        validation_result['warnings'].append(
                            f"Data type mismatch: '{condition['left']}' ({left_dtype}) "
                            f"vs '{condition['right']}' ({right_dtype})"
                        )
            
            # Estimate result size
            if join_config.join_type == JoinType.CROSS:
                estimated_rows = len(left_sample) * len(right_sample)
                validation_result['warnings'].append(
                    f"Cross join will produce approximately {estimated_rows} rows"
                )
            
            # Check for potential performance issues
            if (join_config.left_source.get('datasource_id') != 
                join_config.right_source.get('datasource_id')):
                validation_result['warnings'].append(
                    "Cross-datasource join may be slower than same-datasource joins"
                )
            
        except Exception as e:
            validation_result['valid'] = False
            validation_result['errors'].append(str(e))
        
        return validation_result