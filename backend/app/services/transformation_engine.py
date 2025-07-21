import pandas as pd
import numpy as np
import re
import ast
import json
from typing import List, Dict, Any, Optional, Tuple, Union
from datetime import datetime
import sqlparse
from io import StringIO

from app.models.transformation_types import (
    TransformationType, TransformationStep, FilterRule, FilterOperator,
    AggregationConfig, ColumnSplitConfig, ColumnMergeConfig, 
    TypeConversionConfig, CleaningRule
)


class TransformationEngine:
    """Engine for applying data transformations"""
    
    def __init__(self):
        self.supported_transformations = {
            TransformationType.FILTER: self._apply_filter,
            TransformationType.CLEAN: self._apply_cleaning,
            TransformationType.AGGREGATE: self._apply_aggregation,
            TransformationType.SPLIT_COLUMN: self._apply_column_split,
            TransformationType.MERGE_COLUMN: self._apply_column_merge,
            TransformationType.TYPE_CONVERSION: self._apply_type_conversion,
            TransformationType.RENAME: self._apply_rename,
            TransformationType.DROP: self._apply_drop,
            TransformationType.FILL_NULL: self._apply_fill_null,
            TransformationType.CUSTOM_SQL: self._apply_custom_sql,
            TransformationType.CUSTOM_PYTHON: self._apply_custom_python,
        }
    
    async def apply_transformations(self, df: pd.DataFrame, steps: List[TransformationStep]) -> pd.DataFrame:
        """Apply a series of transformation steps to a DataFrame"""
        result_df = df.copy()
        
        for step in steps:
            if step.type not in self.supported_transformations:
                raise ValueError(f"Unsupported transformation type: {step.type}")
            
            transform_func = self.supported_transformations[step.type]
            result_df = await transform_func(result_df, step.config)
            
            # Validate result
            if result_df is None or result_df.empty:
                raise ValueError(f"Transformation '{step.name}' resulted in empty dataset")
        
        return result_df
    
    async def _apply_filter(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Apply filtering rules to DataFrame"""
        result_df = df.copy()
        
        # Parse filter rules
        if 'rules' in config:
            rules = [FilterRule(**rule) for rule in config['rules']]
        else:
            # Single rule
            rules = [FilterRule(**config)]
        
        # Apply filters
        for rule in rules:
            if rule.column not in result_df.columns:
                raise ValueError(f"Column '{rule.column}' not found in data")
            
            col_data = result_df[rule.column]
            
            if rule.operator == FilterOperator.EQUALS:
                mask = col_data == rule.value
            elif rule.operator == FilterOperator.NOT_EQUALS:
                mask = col_data != rule.value
            elif rule.operator == FilterOperator.GREATER_THAN:
                mask = col_data > rule.value
            elif rule.operator == FilterOperator.LESS_THAN:
                mask = col_data < rule.value
            elif rule.operator == FilterOperator.GREATER_EQUAL:
                mask = col_data >= rule.value
            elif rule.operator == FilterOperator.LESS_EQUAL:
                mask = col_data <= rule.value
            elif rule.operator == FilterOperator.IN:
                mask = col_data.isin(rule.value)
            elif rule.operator == FilterOperator.NOT_IN:
                mask = ~col_data.isin(rule.value)
            elif rule.operator == FilterOperator.CONTAINS:
                mask = col_data.astype(str).str.contains(rule.value, case=not rule.case_sensitive, na=False)
            elif rule.operator == FilterOperator.NOT_CONTAINS:
                mask = ~col_data.astype(str).str.contains(rule.value, case=not rule.case_sensitive, na=False)
            elif rule.operator == FilterOperator.STARTS_WITH:
                mask = col_data.astype(str).str.startswith(rule.value, na=False)
            elif rule.operator == FilterOperator.ENDS_WITH:
                mask = col_data.astype(str).str.endswith(rule.value, na=False)
            elif rule.operator == FilterOperator.IS_NULL:
                mask = col_data.isna()
            elif rule.operator == FilterOperator.NOT_NULL:
                mask = col_data.notna()
            else:
                raise ValueError(f"Unknown filter operator: {rule.operator}")
            
            # Apply filter based on logical operator
            if config.get('logical_operator', 'AND') == 'AND':
                result_df = result_df[mask]
            else:  # OR
                if len(result_df) == len(df):  # First filter
                    result_df = result_df[mask]
                else:
                    result_df = pd.concat([result_df, df[mask]]).drop_duplicates()
        
        return result_df
    
    async def _apply_cleaning(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Apply cleaning rules to DataFrame"""
        result_df = df.copy()
        
        # Parse cleaning rules
        if 'rules' in config:
            rules = [CleaningRule(**rule) for rule in config['rules']]
        else:
            # Single rule
            rules = [CleaningRule(**config)]
        
        for rule in rules:
            if rule.column not in result_df.columns:
                raise ValueError(f"Column '{rule.column}' not found in data")
            
            col = rule.column
            
            if rule.rule_type == "trim":
                result_df[col] = result_df[col].astype(str).str.strip()
            
            elif rule.rule_type == "remove_special":
                pattern = rule.parameters.get('pattern', r'[^a-zA-Z0-9\s]') if rule.parameters else r'[^a-zA-Z0-9\s]'
                result_df[col] = result_df[col].astype(str).str.replace(pattern, '', regex=True)
            
            elif rule.rule_type == "lowercase":
                result_df[col] = result_df[col].astype(str).str.lower()
            
            elif rule.rule_type == "uppercase":
                result_df[col] = result_df[col].astype(str).str.upper()
            
            elif rule.rule_type == "remove_numbers":
                result_df[col] = result_df[col].astype(str).str.replace(r'\d+', '', regex=True)
            
            elif rule.rule_type == "remove_spaces":
                result_df[col] = result_df[col].astype(str).str.replace(r'\s+', '', regex=True)
            
            elif rule.rule_type == "normalize_whitespace":
                result_df[col] = result_df[col].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
            
            elif rule.rule_type == "remove_punctuation":
                result_df[col] = result_df[col].astype(str).str.replace(r'[^\w\s]', '', regex=True)
            
            elif rule.rule_type == "remove_html":
                result_df[col] = result_df[col].astype(str).str.replace(r'<[^>]+>', '', regex=True)
            
            elif rule.rule_type == "remove_urls":
                result_df[col] = result_df[col].astype(str).str.replace(
                    r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
                    '', regex=True
                )
            
            elif rule.rule_type == "custom_regex":
                if rule.parameters and 'pattern' in rule.parameters:
                    replacement = rule.parameters.get('replacement', '')
                    result_df[col] = result_df[col].astype(str).str.replace(
                        rule.parameters['pattern'], replacement, regex=True
                    )
            
            else:
                raise ValueError(f"Unknown cleaning rule: {rule.rule_type}")
        
        return result_df
    
    async def _apply_aggregation(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Apply aggregation operations"""
        agg_config = AggregationConfig(**config)
        
        # Validate columns
        for col in agg_config.group_by:
            if col not in df.columns:
                raise ValueError(f"Group by column '{col}' not found in data")
        
        # Build aggregation dictionary
        agg_dict = {}
        for agg in agg_config.aggregations:
            col = agg['column']
            func = agg['function']
            alias = agg.get('alias', f"{col}_{func}")
            
            if col not in df.columns:
                raise ValueError(f"Aggregation column '{col}' not found in data")
            
            # Map to pandas aggregation functions
            if func == 'count_distinct':
                agg_dict[alias] = pd.NamedAgg(column=col, aggfunc='nunique')
            elif func == 'median':
                agg_dict[alias] = pd.NamedAgg(column=col, aggfunc='median')
            elif func == 'std':
                agg_dict[alias] = pd.NamedAgg(column=col, aggfunc='std')
            elif func == 'var':
                agg_dict[alias] = pd.NamedAgg(column=col, aggfunc='var')
            else:
                agg_dict[alias] = pd.NamedAgg(column=col, aggfunc=func)
        
        # Perform aggregation
        if agg_config.group_by:
            result_df = df.groupby(agg_config.group_by).agg(**agg_dict).reset_index()
        else:
            # Aggregate without grouping
            result_dict = {}
            for alias, named_agg in agg_dict.items():
                result_dict[alias] = [df[named_agg.column].agg(named_agg.aggfunc)]
            result_df = pd.DataFrame(result_dict)
        
        # Apply HAVING clause if specified
        if agg_config.having:
            for rule in agg_config.having:
                # Apply filter rules to aggregated data
                result_df = await self._apply_filter(result_df, rule.dict())
        
        return result_df
    
    async def _apply_column_split(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Split a column into multiple columns"""
        split_config = ColumnSplitConfig(**config)
        result_df = df.copy()
        
        if split_config.column not in result_df.columns:
            raise ValueError(f"Column '{split_config.column}' not found in data")
        
        # Perform split
        if split_config.delimiter:
            # Split by delimiter
            split_data = result_df[split_config.column].astype(str).str.split(
                split_config.delimiter, expand=True, n=len(split_config.new_columns)-1
            )
        elif split_config.pattern:
            # Split by regex pattern
            split_data = result_df[split_config.column].astype(str).str.extract(
                split_config.pattern, expand=True
            )
        else:
            raise ValueError("Either delimiter or pattern must be specified for column split")
        
        # Assign to new columns
        for i, new_col in enumerate(split_config.new_columns):
            if i < split_data.shape[1]:
                result_df[new_col] = split_data[i]
            else:
                result_df[new_col] = None
        
        # Drop original column if requested
        if not split_config.keep_original:
            result_df = result_df.drop(columns=[split_config.column])
        
        return result_df
    
    async def _apply_column_merge(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Merge multiple columns into one"""
        merge_config = ColumnMergeConfig(**config)
        result_df = df.copy()
        
        # Validate columns
        for col in merge_config.columns:
            if col not in result_df.columns:
                raise ValueError(f"Column '{col}' not found in data")
        
        # Merge columns
        result_df[merge_config.new_column] = result_df[merge_config.columns].astype(str).apply(
            lambda x: merge_config.separator.join(x.dropna()), axis=1
        )
        
        # Drop original columns if requested
        if merge_config.drop_original:
            result_df = result_df.drop(columns=merge_config.columns)
        
        return result_df
    
    async def _apply_type_conversion(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Convert column data types"""
        conv_config = TypeConversionConfig(**config)
        result_df = df.copy()
        
        if conv_config.column not in result_df.columns:
            raise ValueError(f"Column '{conv_config.column}' not found in data")
        
        col = conv_config.column
        target_type = conv_config.target_type
        
        try:
            if target_type == "integer":
                result_df[col] = pd.to_numeric(result_df[col], errors='coerce').astype('Int64')
            
            elif target_type == "float":
                result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
            
            elif target_type == "string":
                result_df[col] = result_df[col].astype(str)
            
            elif target_type == "date":
                result_df[col] = pd.to_datetime(
                    result_df[col], 
                    format=conv_config.format, 
                    errors='coerce'
                ).dt.date
            
            elif target_type == "datetime":
                result_df[col] = pd.to_datetime(
                    result_df[col], 
                    format=conv_config.format, 
                    errors='coerce'
                )
            
            elif target_type == "boolean":
                # Handle various boolean representations
                true_values = ['true', 'True', 'TRUE', '1', 't', 'T', 'yes', 'Yes', 'YES']
                result_df[col] = result_df[col].astype(str).isin(true_values)
            
            elif target_type == "json":
                def safe_json_parse(x):
                    try:
                        return json.loads(x) if pd.notna(x) else None
                    except:
                        return None
                result_df[col] = result_df[col].apply(safe_json_parse)
            
            else:
                raise ValueError(f"Unsupported target type: {target_type}")
            
            # Handle default values for failed conversions
            if conv_config.default_value is not None:
                result_df[col] = result_df[col].fillna(conv_config.default_value)
            
        except Exception as e:
            raise ValueError(f"Type conversion failed: {str(e)}")
        
        return result_df
    
    async def _apply_rename(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Rename columns"""
        result_df = df.copy()
        rename_map = config.get('rename_map', {})
        
        # Validate columns
        for old_name in rename_map:
            if old_name not in result_df.columns:
                raise ValueError(f"Column '{old_name}' not found in data")
        
        result_df = result_df.rename(columns=rename_map)
        return result_df
    
    async def _apply_drop(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Drop columns"""
        result_df = df.copy()
        columns_to_drop = config.get('columns', [])
        
        # Validate columns
        for col in columns_to_drop:
            if col not in result_df.columns:
                raise ValueError(f"Column '{col}' not found in data")
        
        result_df = result_df.drop(columns=columns_to_drop)
        return result_df
    
    async def _apply_fill_null(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Fill null values"""
        result_df = df.copy()
        
        if 'column' in config:
            # Fill specific column
            column = config['column']
            if column not in result_df.columns:
                raise ValueError(f"Column '{column}' not found in data")
            
            method = config.get('method', 'value')
            
            if method == 'value':
                result_df[column] = result_df[column].fillna(config.get('value', ''))
            elif method == 'forward':
                result_df[column] = result_df[column].fillna(method='ffill')
            elif method == 'backward':
                result_df[column] = result_df[column].fillna(method='bfill')
            elif method == 'mean':
                if pd.api.types.is_numeric_dtype(result_df[column]):
                    result_df[column] = result_df[column].fillna(result_df[column].mean())
            elif method == 'median':
                if pd.api.types.is_numeric_dtype(result_df[column]):
                    result_df[column] = result_df[column].fillna(result_df[column].median())
            elif method == 'mode':
                mode_value = result_df[column].mode()
                if len(mode_value) > 0:
                    result_df[column] = result_df[column].fillna(mode_value[0])
        else:
            # Fill all columns
            method = config.get('method', 'value')
            value = config.get('value', '')
            
            if method == 'value':
                result_df = result_df.fillna(value)
            elif method == 'forward':
                result_df = result_df.fillna(method='ffill')
            elif method == 'backward':
                result_df = result_df.fillna(method='bfill')
        
        return result_df
    
    async def _apply_custom_sql(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Apply custom SQL transformation using pandas SQL capabilities"""
        import sqlite3
        import tempfile
        
        sql_query = config.get('script', '')
        if not sql_query:
            raise ValueError("SQL script is required")
        
        # Create temporary SQLite database
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            conn = sqlite3.connect(tmp.name)
            
            try:
                # Write DataFrame to SQLite
                df.to_sql('source_data', conn, index=False, if_exists='replace')
                
                # Execute SQL query
                result_df = pd.read_sql_query(sql_query, conn)
                
                return result_df
            finally:
                conn.close()
    
    async def _apply_custom_python(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Apply custom Python transformation"""
        script = config.get('script', '')
        if not script:
            raise ValueError("Python script is required")
        
        # Validate script first
        is_valid, error = await self.validate_python_script(script)
        if not is_valid:
            raise ValueError(f"Invalid Python script: {error}")
        
        # Create execution context
        context = {
            'df': df.copy(),
            'pd': pd,
            'np': np,
            'datetime': datetime,
            're': re,
            'json': json,
            'result': None
        }
        
        # Add parameters if provided
        if 'parameters' in config:
            context.update(config['parameters'])
        
        try:
            # Execute script
            exec(script, context)
            
            # Get result
            if 'result' in context and isinstance(context['result'], pd.DataFrame):
                return context['result']
            elif isinstance(context['df'], pd.DataFrame):
                return context['df']
            else:
                raise ValueError("Script must return a DataFrame as 'result' or modify 'df'")
                
        except Exception as e:
            raise ValueError(f"Script execution failed: {str(e)}")
    
    async def validate_python_script(self, script: str) -> Tuple[bool, Optional[str]]:
        """Validate a Python transformation script"""
        try:
            # Parse the script to check syntax
            ast.parse(script)
            
            # Check for dangerous operations
            dangerous_keywords = [
                'exec', 'eval', '__import__', 'open', 'file',
                'compile', 'globals', 'locals', 'vars',
                'os.', 'sys.', 'subprocess', 'socket'
            ]
            
            for keyword in dangerous_keywords:
                if keyword in script:
                    return False, f"Dangerous operation '{keyword}' not allowed"
            
            # Check that it's not trying to import dangerous modules
            tree = ast.parse(script)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name in ['os', 'sys', 'subprocess', 'socket']:
                            return False, f"Import of '{alias.name}' not allowed"
                elif isinstance(node, ast.ImportFrom):
                    if node.module in ['os', 'sys', 'subprocess', 'socket']:
                        return False, f"Import from '{node.module}' not allowed"
            
            return True, None
            
        except SyntaxError as e:
            return False, f"Syntax error: {str(e)}"
        except Exception as e:
            return False, str(e)
    
    async def validate_sql_script(self, script: str) -> Tuple[bool, Optional[str]]:
        """Validate a SQL transformation script"""
        try:
            # Parse SQL to check syntax
            parsed = sqlparse.parse(script)
            if not parsed:
                return False, "Invalid SQL syntax"
            
            # Check for dangerous operations
            dangerous_keywords = [
                'DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE',
                'INSERT', 'UPDATE', 'GRANT', 'REVOKE'
            ]
            
            formatted = sqlparse.format(script, keyword_case='upper')
            for keyword in dangerous_keywords:
                if keyword in formatted:
                    return False, f"Operation '{keyword}' not allowed in transformation SQL"
            
            # Ensure it's a SELECT statement
            if not formatted.strip().upper().startswith('SELECT'):
                return False, "Only SELECT statements are allowed"
            
            return True, None
            
        except Exception as e:
            return False, str(e)
    
    async def save_to_table(self, df: pd.DataFrame, table_name: str, executor, if_exists: str = 'replace'):
        """Save DataFrame to database table"""
        from app.services.import_utils import prepare_dataframe_for_import, generate_insert_sql
        
        if if_exists == 'replace':
            # Drop table if exists
            drop_query = f'DROP TABLE IF EXISTS "{table_name}"'
            await executor.execute_query(drop_query)
        
        # Detect column types
        from app.services.type_detection import detect_column_type
        column_types = {}
        for col in df.columns:
            sql_type, _ = detect_column_type(df[col])
            column_types[col] = sql_type
        
        # Prepare for import
        create_table_sql, insert_columns, _, _ = prepare_dataframe_for_import(
            df, table_name, column_types
        )
        
        # Create table
        result = await executor.execute_query(create_table_sql)
        if result['error']:
            raise Exception(f"Failed to create table: {result['error']}")
        
        # Insert data
        insert_statements = generate_insert_sql(df, table_name, insert_columns)
        for insert_sql in insert_statements:
            result = await executor.execute_query(insert_sql)
            if result['error']:
                raise Exception(f"Failed to insert data: {result['error']}")
    
    async def export_data(self, df: pd.DataFrame, format: str, filename: Optional[str] = None) -> str:
        """Export DataFrame to file"""
        import tempfile
        import os
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"transformation_result_{timestamp}"
        
        # Create temp directory if not exists
        temp_dir = os.path.join(tempfile.gettempdir(), 'csv2sql_exports')
        os.makedirs(temp_dir, exist_ok=True)
        
        if format == 'csv':
            filepath = os.path.join(temp_dir, f"{filename}.csv")
            df.to_csv(filepath, index=False)
        elif format == 'excel':
            filepath = os.path.join(temp_dir, f"{filename}.xlsx")
            df.to_excel(filepath, index=False)
        else:
            raise ValueError(f"Unsupported export format: {format}")
        
        return filepath