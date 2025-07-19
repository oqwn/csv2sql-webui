import api from './api';

export interface DataSource {
  id: number;
  name: string;
  type: string;
  connection_config: Record<string, any>;
  extraction_config?: Record<string, any>;
  is_active: boolean;
  created_at: string;
  updated_at: string;
  last_sync_at?: string;
  description?: string;
}

export interface SupportedDataSource {
  type: string;
  name: string;
  category: string;
  description: string;
  supports_incremental: boolean;
  supports_real_time: boolean;
}

export interface ConnectionTestRequest {
  type: string;
  connection_config: Record<string, any>;
}

export interface SchemaInfo {
  name: string;
  type: string;
  columns?: Array<{
    name: string;
    type: string;
    nullable: boolean;
    primary_key?: boolean;
  }>;
  fields?: Array<{
    name: string;
    type: string;
    nullable: boolean;
  }>;
  row_count?: number;
  document_count?: number;
  size?: number;
}

export interface DataPreview {
  status: string;
  columns: Array<{
    name: string;
    type: string;
    sql_type: string;
    null_count: number;
    unique_count: number;
  }>;
  sample_data: Array<Record<string, any>>;
  row_count: number;
  error?: string;
}

export interface ExtractionJob {
  id: number;
  data_source_id: number;
  job_name: string;
  extraction_mode: string;
  source_query?: string;
  target_table: string;
  status: string;
  records_processed: number;
  error_message?: string;
  started_at?: string;
  completed_at?: string;
  created_at: string;
  config?: Record<string, any>;
}

export interface ExtractionJobCreate {
  job_name: string;
  extraction_mode: string;
  source_query?: string;
  target_table: string;
  config?: Record<string, any>;
}

export const dataSourceAPI = {
  // Get supported data source types
  getSupportedDataSources: () => 
    api.get<SupportedDataSource[]>('/data-sources/supported'),

  // Test connection to a data source
  testConnection: (request: ConnectionTestRequest) =>
    api.post('/data-sources/test-connection', request),

  // Get schema information from a data source
  getSchema: (request: ConnectionTestRequest) =>
    api.post<SchemaInfo[]>('/data-sources/schema', request),

  // Preview data from a source
  previewData: (type: string, connection_config: Record<string, any>, source_name: string, limit = 100) =>
    api.post<DataPreview>('/data-sources/preview', {
      type,
      connection_config,
      source_name,
      limit
    }),

  // Get incremental extraction info
  getIncrementalInfo: (type: string, connection_config: Record<string, any>, source_name: string) =>
    api.post('/data-sources/incremental-info', {
      type,
      connection_config,
      source_name
    }),

  // CRUD operations for data sources
  getDataSources: (skip = 0, limit = 100) =>
    api.get<DataSource[]>(`/data-sources/?skip=${skip}&limit=${limit}`),

  createDataSource: (dataSource: Omit<DataSource, 'id' | 'created_at' | 'updated_at' | 'last_sync_at'>) =>
    api.post<DataSource>('/data-sources/', dataSource),

  getDataSource: (id: number) =>
    api.get<DataSource>(`/data-sources/${id}`),

  updateDataSource: (id: number, dataSource: Partial<DataSource>) =>
    api.put<DataSource>(`/data-sources/${id}`, dataSource),

  deleteDataSource: (id: number) =>
    api.delete(`/data-sources/${id}`),

  // Extraction jobs
  extractData: (dataSourceId: number, job: ExtractionJobCreate) =>
    api.post(`/data-sources/${dataSourceId}/extract`, job),

  getExtractionJobs: (dataSourceId: number) =>
    api.get<ExtractionJob[]>(`/data-sources/${dataSourceId}/jobs`),

  getExtractionJob: (jobId: number) =>
    api.get<ExtractionJob>(`/data-sources/jobs/${jobId}`)
};