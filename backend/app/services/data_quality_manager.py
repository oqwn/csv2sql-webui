"""
Data Quality Management System
Handles error isolation, dirty data quarantine, and data validation
"""

import uuid
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Callable, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import logging

from app.services.local_storage import LocalStorage

logger = logging.getLogger(__name__)


class DataQualityIssueType(Enum):
    """Types of data quality issues"""
    NULL_VALUE = "null_value"
    TYPE_MISMATCH = "type_mismatch"
    CONSTRAINT_VIOLATION = "constraint_violation"
    DUPLICATE_KEY = "duplicate_key"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    FORMAT_ERROR = "format_error"
    RANGE_VIOLATION = "range_violation"
    CUSTOM_VALIDATION = "custom_validation"


class DataQualitySeverity(Enum):
    """Severity levels for data quality issues"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class DataQualityIssue:
    """Represents a data quality issue"""
    issue_id: str
    issue_type: DataQualityIssueType
    severity: DataQualitySeverity
    column_name: Optional[str]
    row_index: Optional[int]
    value: Any
    expected_value: Optional[Any]
    message: str
    timestamp: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataQualityReport:
    """Report of data quality issues found in a dataset"""
    dataset_id: str
    total_rows: int
    total_columns: int
    issues: List[DataQualityIssue]
    clean_rows: int
    dirty_rows: int
    timestamp: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataQualityRule:
    """Base class for data quality validation rules"""
    
    def __init__(self, rule_id: str, name: str, severity: DataQualitySeverity = DataQualitySeverity.MEDIUM):
        self.rule_id = rule_id
        self.name = name
        self.severity = severity
    
    def validate(self, df: pd.DataFrame, column: Optional[str] = None) -> List[DataQualityIssue]:
        """Validate data and return list of issues"""
        raise NotImplementedError


class NotNullRule(DataQualityRule):
    """Rule to check for null values in specified columns"""
    
    def __init__(self, columns: List[str], severity: DataQualitySeverity = DataQualitySeverity.MEDIUM):
        super().__init__("not_null", "Not Null Check", severity)
        self.columns = columns
    
    def validate(self, df: pd.DataFrame, column: Optional[str] = None) -> List[DataQualityIssue]:
        issues = []
        check_columns = [column] if column else self.columns
        
        for col in check_columns:
            if col not in df.columns:
                continue
                
            null_indices = df[df[col].isna()].index
            for idx in null_indices:
                issues.append(DataQualityIssue(
                    issue_id=str(uuid.uuid4()),
                    issue_type=DataQualityIssueType.NULL_VALUE,
                    severity=self.severity,
                    column_name=col,
                    row_index=idx,
                    value=None,
                    expected_value="non-null value",
                    message=f"Null value found in column '{col}' at row {idx}",
                    timestamp=datetime.now()
                ))
        
        return issues


class DataTypeRule(DataQualityRule):
    """Rule to check data types match expected types"""
    
    def __init__(self, column_types: Dict[str, str], severity: DataQualitySeverity = DataQualitySeverity.HIGH):
        super().__init__("data_type", "Data Type Check", severity)
        self.column_types = column_types
    
    def validate(self, df: pd.DataFrame, column: Optional[str] = None) -> List[DataQualityIssue]:
        issues = []
        check_columns = {column: self.column_types[column]} if column and column in self.column_types else self.column_types
        
        for col, expected_type in check_columns.items():
            if col not in df.columns:
                continue
            
            # Check each value in the column
            for idx, value in df[col].items():
                if pd.isna(value):
                    continue
                
                is_valid = True
                if expected_type == 'int':
                    is_valid = isinstance(value, (int, np.integer)) or (isinstance(value, str) and value.isdigit())
                elif expected_type == 'float':
                    is_valid = isinstance(value, (int, float, np.number))
                elif expected_type == 'str':
                    is_valid = isinstance(value, str)
                elif expected_type == 'datetime':
                    is_valid = isinstance(value, (datetime, pd.Timestamp))
                
                if not is_valid:
                    issues.append(DataQualityIssue(
                        issue_id=str(uuid.uuid4()),
                        issue_type=DataQualityIssueType.TYPE_MISMATCH,
                        severity=self.severity,
                        column_name=col,
                        row_index=idx,
                        value=value,
                        expected_value=expected_type,
                        message=f"Type mismatch in column '{col}' at row {idx}: expected {expected_type}, got {type(value).__name__}",
                        timestamp=datetime.now()
                    ))
        
        return issues


class RangeRule(DataQualityRule):
    """Rule to check values are within specified ranges"""
    
    def __init__(self, column_ranges: Dict[str, Tuple[Any, Any]], severity: DataQualitySeverity = DataQualitySeverity.MEDIUM):
        super().__init__("range", "Range Check", severity)
        self.column_ranges = column_ranges
    
    def validate(self, df: pd.DataFrame, column: Optional[str] = None) -> List[DataQualityIssue]:
        issues = []
        check_columns = {column: self.column_ranges[column]} if column and column in self.column_ranges else self.column_ranges
        
        for col, (min_val, max_val) in check_columns.items():
            if col not in df.columns:
                continue
            
            out_of_range_mask = (df[col] < min_val) | (df[col] > max_val)
            out_of_range_indices = df[out_of_range_mask].index
            
            for idx in out_of_range_indices:
                value = df.loc[idx, col]
                issues.append(DataQualityIssue(
                    issue_id=str(uuid.uuid4()),
                    issue_type=DataQualityIssueType.RANGE_VIOLATION,
                    severity=self.severity,
                    column_name=col,
                    row_index=idx,
                    value=value,
                    expected_value=f"between {min_val} and {max_val}",
                    message=f"Value {value} in column '{col}' at row {idx} is outside range [{min_val}, {max_val}]",
                    timestamp=datetime.now()
                ))
        
        return issues


class UniqueRule(DataQualityRule):
    """Rule to check for duplicate values in specified columns"""
    
    def __init__(self, columns: List[str], severity: DataQualitySeverity = DataQualitySeverity.HIGH):
        super().__init__("unique", "Uniqueness Check", severity)
        self.columns = columns
    
    def validate(self, df: pd.DataFrame, column: Optional[str] = None) -> List[DataQualityIssue]:
        issues = []
        check_columns = [column] if column else self.columns
        
        for col in check_columns:
            if col not in df.columns:
                continue
            
            # Find duplicate values
            duplicated_mask = df.duplicated(subset=[col], keep=False)
            duplicated_indices = df[duplicated_mask].index
            
            for idx in duplicated_indices:
                value = df.loc[idx, col]
                issues.append(DataQualityIssue(
                    issue_id=str(uuid.uuid4()),
                    issue_type=DataQualityIssueType.DUPLICATE_KEY,
                    severity=self.severity,
                    column_name=col,
                    row_index=idx,
                    value=value,
                    expected_value="unique value",
                    message=f"Duplicate value '{value}' found in column '{col}' at row {idx}",
                    timestamp=datetime.now()
                ))
        
        return issues


class DataQualityManager:
    """Manager for data quality validation and error isolation"""
    
    def __init__(self):
        self.storage = LocalStorage()
        self.rules: List[DataQualityRule] = []
        self.quarantine_data: Dict[str, List[Dict[str, Any]]] = {}
        
    def add_rule(self, rule: DataQualityRule):
        """Add a data quality validation rule"""
        self.rules.append(rule)
    
    def remove_rule(self, rule_id: str):
        """Remove a data quality validation rule"""
        self.rules = [rule for rule in self.rules if rule.rule_id != rule_id]
    
    def validate_dataset(self, df: pd.DataFrame, dataset_id: str = None) -> DataQualityReport:
        """Validate a dataset against all configured rules"""
        if not dataset_id:
            dataset_id = str(uuid.uuid4())
        
        all_issues = []
        
        # Run all validation rules
        for rule in self.rules:
            issues = rule.validate(df)
            all_issues.extend(issues)
        
        # Calculate statistics
        dirty_row_indices = set(issue.row_index for issue in all_issues if issue.row_index is not None)
        dirty_rows = len(dirty_row_indices)
        clean_rows = len(df) - dirty_rows
        
        report = DataQualityReport(
            dataset_id=dataset_id,
            total_rows=len(df),
            total_columns=len(df.columns),
            issues=all_issues,
            clean_rows=clean_rows,
            dirty_rows=dirty_rows,
            timestamp=datetime.now()
        )
        
        return report
    
    def isolate_dirty_data(self, df: pd.DataFrame, report: DataQualityReport, 
                          isolation_strategy: str = "quarantine") -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Isolate dirty data based on quality report"""
        
        if isolation_strategy == "quarantine":
            # Remove rows with any issues
            dirty_row_indices = set(issue.row_index for issue in report.issues if issue.row_index is not None)
            clean_df = df.drop(index=dirty_row_indices)
            dirty_df = df.loc[list(dirty_row_indices)] if dirty_row_indices else pd.DataFrame()
            
        elif isolation_strategy == "fix_and_continue":
            # Try to fix issues automatically and continue
            clean_df = df.copy()
            dirty_df = pd.DataFrame()
            
            for issue in report.issues:
                if issue.issue_type == DataQualityIssueType.NULL_VALUE:
                    # Fill null values with default
                    if issue.row_index is not None and issue.column_name:
                        clean_df.loc[issue.row_index, issue.column_name] = ""
                        
        elif isolation_strategy == "ignore":
            # Continue with all data, log issues only
            clean_df = df
            dirty_df = pd.DataFrame()
            
        else:
            raise ValueError(f"Unknown isolation strategy: {isolation_strategy}")
        
        # Store dirty data in quarantine
        if not dirty_df.empty:
            quarantine_id = f"{report.dataset_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            self.quarantine_data[quarantine_id] = {
                'data': dirty_df.to_dict('records'),
                'issues': [self._issue_to_dict(issue) for issue in report.issues],
                'timestamp': datetime.now().isoformat(),
                'dataset_id': report.dataset_id
            }
        
        return clean_df, dirty_df
    
    def get_quarantine_data(self, quarantine_id: Optional[str] = None) -> Dict[str, Any]:
        """Get quarantined data"""
        if quarantine_id:
            return self.quarantine_data.get(quarantine_id, {})
        else:
            return self.quarantine_data
    
    def create_validation_profile(self, df: pd.DataFrame, sample_size: int = 1000) -> Dict[str, DataQualityRule]:
        """Automatically create data quality rules based on dataset analysis"""
        sample_df = df.sample(min(sample_size, len(df))) if len(df) > sample_size else df
        rules = {}
        
        for column in sample_df.columns:
            col_data = sample_df[column].dropna()
            
            if len(col_data) == 0:
                continue
            
            # Check if column has null values in full dataset
            if df[column].isna().any():
                rules[f"{column}_not_null"] = NotNullRule([column])
            
            # Detect data type
            if col_data.dtype in ['int64', 'int32']:
                rules[f"{column}_type"] = DataTypeRule({column: 'int'})
                # Add range rule for numeric data
                min_val, max_val = col_data.min(), col_data.max()
                rules[f"{column}_range"] = RangeRule({column: (min_val, max_val)})
                
            elif col_data.dtype in ['float64', 'float32']:
                rules[f"{column}_type"] = DataTypeRule({column: 'float'})
                min_val, max_val = col_data.min(), col_data.max()
                rules[f"{column}_range"] = RangeRule({column: (min_val, max_val)})
                
            elif col_data.dtype == 'object':
                rules[f"{column}_type"] = DataTypeRule({column: 'str'})
            
            # Check for uniqueness (if column looks like an ID)
            if 'id' in column.lower() or len(col_data.unique()) == len(col_data):
                rules[f"{column}_unique"] = UniqueRule([column])
        
        return rules
    
    def _issue_to_dict(self, issue: DataQualityIssue) -> Dict[str, Any]:
        """Convert DataQualityIssue to dictionary"""
        return {
            'issue_id': issue.issue_id,
            'issue_type': issue.issue_type.value,
            'severity': issue.severity.value,
            'column_name': issue.column_name,
            'row_index': issue.row_index,
            'value': issue.value,
            'expected_value': issue.expected_value,
            'message': issue.message,
            'timestamp': issue.timestamp.isoformat(),
            'metadata': issue.metadata
        }
    
    def export_report(self, report: DataQualityReport, format: str = "json") -> str:
        """Export data quality report to file"""
        import tempfile
        import os
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_dir = os.path.join(tempfile.gettempdir(), 'data_quality_reports')
        os.makedirs(temp_dir, exist_ok=True)
        
        if format == "json":
            import json
            filepath = os.path.join(temp_dir, f"quality_report_{timestamp}.json")
            
            report_data = {
                'dataset_id': report.dataset_id,
                'total_rows': report.total_rows,
                'total_columns': report.total_columns,
                'clean_rows': report.clean_rows,
                'dirty_rows': report.dirty_rows,
                'timestamp': report.timestamp.isoformat(),
                'issues': [self._issue_to_dict(issue) for issue in report.issues],
                'metadata': report.metadata
            }
            
            with open(filepath, 'w') as f:
                json.dump(report_data, f, indent=2, default=str)
                
        elif format == "csv":
            filepath = os.path.join(temp_dir, f"quality_issues_{timestamp}.csv")
            
            issues_data = []
            for issue in report.issues:
                issues_data.append(self._issue_to_dict(issue))
            
            issues_df = pd.DataFrame(issues_data)
            issues_df.to_csv(filepath, index=False)
            
        else:
            raise ValueError(f"Unsupported export format: {format}")
        
        return filepath


# Global data quality manager instance
data_quality_manager = DataQualityManager()