export enum TransformationType {
  FILTER = 'filter',
  CLEAN = 'clean',
  AGGREGATE = 'aggregate',
  JOIN = 'join',
  SPLIT_COLUMN = 'split_column',
  MERGE_COLUMN = 'merge_column',
  CUSTOM_SQL = 'custom_sql',
  CUSTOM_PYTHON = 'custom_python',
  TYPE_CONVERSION = 'type_conversion',
  RENAME = 'rename',
  DROP = 'drop',
  FILL_NULL = 'fill_null',
}

export enum FilterOperator {
  EQUALS = '=',
  NOT_EQUALS = '!=',
  GREATER_THAN = '>',
  LESS_THAN = '<',
  GREATER_EQUAL = '>=',
  LESS_EQUAL = '<=',
  IN = 'in',
  NOT_IN = 'not_in',
  CONTAINS = 'contains',
  NOT_CONTAINS = 'not_contains',
  STARTS_WITH = 'starts_with',
  ENDS_WITH = 'ends_with',
  IS_NULL = 'is_null',
  NOT_NULL = 'not_null',
}

export enum AggregateFunction {
  SUM = 'sum',
  COUNT = 'count',
  COUNT_DISTINCT = 'count_distinct',
  AVG = 'avg',
  MIN = 'min',
  MAX = 'max',
  MEDIAN = 'median',
  STD = 'std',
  VAR = 'var',
}

export enum JoinType {
  INNER = 'inner',
  LEFT = 'left',
  RIGHT = 'right',
  FULL = 'full',
  CROSS = 'cross',
}

export interface FilterRule {
  column: string;
  operator: FilterOperator;
  value?: any;
  case_sensitive?: boolean;
}

export interface CleaningRule {
  column: string;
  rule_type: string;
  parameters?: Record<string, any>;
}

export interface AggregationConfig {
  group_by: string[];
  aggregations: Array<{
    column: string;
    function: string;
    alias?: string;
  }>;
  having?: FilterRule[];
}

export interface JoinConfig {
  left_source: {
    datasource_id?: number;
    table_name?: string;
    query?: string;
  };
  right_source: {
    datasource_id: number;
    table_name?: string;
    query?: string;
  };
  join_type: JoinType;
  join_conditions: Array<{
    left: string;
    right: string;
  }>;
}

export interface ColumnSplitConfig {
  column: string;
  delimiter?: string;
  pattern?: string;
  new_columns: string[];
  keep_original?: boolean;
}

export interface ColumnMergeConfig {
  columns: string[];
  separator?: string;
  new_column: string;
  drop_original?: boolean;
}

export interface TypeConversionConfig {
  column: string;
  target_type: string;
  format?: string;
  default_value?: any;
}

export interface TransformationStep {
  id?: string;
  name: string;
  type: TransformationType;
  config: Record<string, any>;
  description?: string;
}

export interface TransformationPipeline {
  id?: string;
  name: string;
  description?: string;
  source_config: {
    datasource_id: number;
    table_name?: string;
    query?: string;
  };
  steps: TransformationStep[];
  output_config?: {
    type: 'table' | 'export';
    datasource_id?: number;
    table_name?: string;
    format?: 'csv' | 'excel';
    filename?: string;
    if_exists?: 'replace' | 'append' | 'upsert' | 'merge' | 'fail';
    primary_key_columns?: string[];
  };
  created_at?: string;
  updated_at?: string;
}

export interface TransformationPreviewRequest {
  source_config: {
    datasource_id: number;
    table_name?: string;
    query?: string;
  };
  steps: TransformationStep[];
  preview_rows?: number;
}

export interface TransformationExecuteRequest {
  pipeline_id?: string;
  source_config?: {
    datasource_id: number;
    table_name?: string;
    query?: string;
  };
  steps?: TransformationStep[];
  output_config: {
    type: 'table' | 'export';
    datasource_id?: number;
    table_name?: string;
    format?: 'csv' | 'excel';
    filename?: string;
    if_exists?: 'replace' | 'append' | 'upsert' | 'merge' | 'fail';
    primary_key_columns?: string[];
  };
  execute_async?: boolean;
}

export interface TransformationPreviewResponse {
  status: string;
  original_shape: {
    rows: number;
    columns: number;
  };
  transformed_shape: {
    rows: number;
    columns: number;
  };
  columns: string[];
  data_types: Record<string, string>;
  preview: Record<string, any>[];
  transformations_applied: number;
}

export interface TransformationExecuteResponse {
  status: string;
  message: string;
  rows_processed: number;
  columns: string[];
  output_location: {
    type: string;
    datasource_id?: number;
    table_name?: string;
    format?: string;
    path?: string;
  };
}