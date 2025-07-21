from typing import Any, List, Dict, Optional
from pydantic import BaseModel, Field
from enum import Enum


class TransformationType(str, Enum):
    FILTER = "filter"
    CLEAN = "clean"
    AGGREGATE = "aggregate"
    JOIN = "join"
    SPLIT_COLUMN = "split_column"
    MERGE_COLUMN = "merge_column"
    CUSTOM_SQL = "custom_sql"
    CUSTOM_PYTHON = "custom_python"
    TYPE_CONVERSION = "type_conversion"
    RENAME = "rename"
    DROP = "drop"
    FILL_NULL = "fill_null"


class FilterOperator(str, Enum):
    EQUALS = "="
    NOT_EQUALS = "!="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_EQUAL = ">="
    LESS_EQUAL = "<="
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    IN = "in"
    NOT_IN = "not_in"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    REGEX = "regex"


class AggregateFunction(str, Enum):
    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    MEDIAN = "median"
    STD = "std"
    VAR = "var"
    FIRST = "first"
    LAST = "last"
    NUNIQUE = "nunique"


class JoinType(str, Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    OUTER = "outer"
    CROSS = "cross"


class CleaningRule(BaseModel):
    column: str
    rule_type: str = Field(..., description="Type of cleaning rule")
    parameters: Dict[str, Any] = Field(default_factory=dict)


class FilterRule(BaseModel):
    column: str
    operator: FilterOperator
    value: Any
    case_sensitive: bool = True


class AggregationConfig(BaseModel):
    group_by: List[str] = Field(default_factory=list)
    aggregations: Dict[str, str] = Field(..., description="Column to function mapping")


class JoinConfig(BaseModel):
    right_table: str
    join_type: JoinType = JoinType.INNER
    left_on: List[str]
    right_on: List[str]
    suffixes: Optional[List[str]] = ["_left", "_right"]


class ColumnSplitConfig(BaseModel):
    column: str
    delimiter: str
    new_columns: List[str]
    max_splits: Optional[int] = None


class ColumnMergeConfig(BaseModel):
    columns: List[str]
    new_column: str
    separator: str = " "


class TypeConversionConfig(BaseModel):
    column: str
    target_type: str
    format_string: Optional[str] = None
    errors: str = "coerce"


class CustomScriptConfig(BaseModel):
    script: str
    language: str = "python"
    output_columns: Optional[List[str]] = None


class TransformationStep(BaseModel):
    id: str
    name: str
    type: TransformationType
    enabled: bool = True
    config: Dict[str, Any] = Field(default_factory=dict)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    
    # Type-specific configs (optional, for backward compatibility)
    filter_rules: Optional[List[FilterRule]] = None
    cleaning_rules: Optional[List[CleaningRule]] = None
    aggregation_config: Optional[AggregationConfig] = None
    join_config: Optional[JoinConfig] = None
    split_config: Optional[ColumnSplitConfig] = None
    merge_config: Optional[ColumnMergeConfig] = None
    type_conversion_config: Optional[TypeConversionConfig] = None
    custom_script_config: Optional[CustomScriptConfig] = None


class TransformationPipeline(BaseModel):
    id: Optional[str] = None
    name: str
    description: Optional[str] = None
    source_config: Dict[str, Any]
    steps: List[TransformationStep] = Field(default_factory=list)
    output_config: Optional[Dict[str, Any]] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class TransformationPreviewRequest(BaseModel):
    source_config: Dict[str, Any]
    steps: List[TransformationStep]
    preview_rows: int = 100


class TransformationExecuteRequest(BaseModel):
    pipeline_id: Optional[str] = None
    source_config: Optional[Dict[str, Any]] = None
    steps: Optional[List[TransformationStep]] = None
    output_config: Dict[str, Any]