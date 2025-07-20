import { sqlAPI, importAPI, exportAPI } from './api';
import api from './api';

// Types for data source management
export interface DataSource {
  id: number;
  name: string;
  type: string;
  description?: string;
  connection_config: Record<string, any>;
  created_at: string;
  updated_at: string;
  is_active: boolean;
}

export interface SchemaInfo {
  name: string;
  type: string;
  row_count?: number;
  document_count?: number;
  columns?: Array<{
    name: string;
    type: string;
    sql_type: string;
  }>;
}

export interface DataPreview {
  status: string;
  error?: string;
  columns: Array<{
    name: string;
    type: string;
    sql_type: string;
  }>;
  sample_data: Record<string, any>[];
  row_count: number;
}

export interface SupportedDataSource {
  type: string;
  name: string;
  description: string;
  category?: string;
  supports_incremental?: boolean;
  supports_real_time?: boolean;
  fields: Array<{
    name: string;
    label: string;
    type: string;
    required: boolean;
    default?: any;
    options?: string[];
  }>;
}

export interface ConnectionTestRequest {
  type: string;
  connection_config: Record<string, any>;
}

// This service provides helpful error messages when data source is not selected
class DataSourceRequiredError extends Error {
  constructor() {
    super('Data source connection required. Please connect to a data source first before performing this operation.');
    this.name = 'DataSourceRequiredError';
  }
}

// Higher-level API that validates data source selection
export const createDataSourceAwareAPI = (getSelectedDataSourceId: () => number | null) => {
  const requireDataSource = (): number => {
    const dataSourceId = getSelectedDataSourceId();
    if (!dataSourceId) {
      throw new DataSourceRequiredError();
    }
    return dataSourceId;
  };

  return {
    sql: {
      executeQuery: (sql: string) => {
        const dataSourceId = requireDataSource();
        return sqlAPI.executeQuery(dataSourceId, sql);
      },
      
      getTables: () => {
        const dataSourceId = requireDataSource();
        return sqlAPI.getTables(dataSourceId);
      },
      
      getTableInfo: (tableName: string) => {
        const dataSourceId = requireDataSource();
        return sqlAPI.getTableInfo(dataSourceId, tableName);
      },
      
      validateQuery: (sql: string) => {
        const dataSourceId = requireDataSource();
        return sqlAPI.validateQuery(dataSourceId, sql);
      }
    },

    import: {
      uploadCSV: (file: File, tableName?: string, createTable: boolean = true, detectTypes: boolean = true) => {
        const dataSourceId = requireDataSource();
        return importAPI.uploadCSV(dataSourceId, file, tableName, createTable, detectTypes);
      },

      previewCSV: (file: File, tableName?: string, sampleSize: number = 10) => {
        // CSV preview doesn't require data source
        return importAPI.previewCSV(file, tableName, sampleSize);
      },

      importCSVWithSQL: (file: File, createTableSQL: string, tableName: string, columnMapping?: Record<string, string>) => {
        const dataSourceId = requireDataSource();
        return importAPI.importCSVWithSQL(dataSourceId, file, createTableSQL, tableName, columnMapping);
      },

      uploadCSVBatch: (files: File[], createTable: boolean = true, detectTypes: boolean = true) => {
        const dataSourceId = requireDataSource();
        return importAPI.uploadCSVBatch(dataSourceId, files, createTable, detectTypes);
      },

      importCSVWithConfig: (file: File, config: any) => {
        const dataSourceId = requireDataSource();
        return importAPI.importCSVWithConfig(dataSourceId, file, config);
      },

      uploadExcel: (file: File, tableName?: string, sheetName?: string, importAllSheets: boolean = false, createTable: boolean = true, detectTypes: boolean = true) => {
        const dataSourceId = requireDataSource();
        return importAPI.uploadExcel(dataSourceId, file, tableName, sheetName, importAllSheets, createTable, detectTypes);
      },

      previewExcel: (file: File, sheetName?: string, rows: number = 10) => {
        // Excel preview doesn't require data source
        return importAPI.previewExcel(file, sheetName, rows);
      },

      getExcelSheets: (file: File) => {
        // Getting Excel sheets doesn't require data source
        return importAPI.getExcelSheets(file);
      },

      importExcelWithSQL: (file: File, createTableSQL: string, tableName: string, sheetName?: string, columnMapping?: Record<string, string>) => {
        const dataSourceId = requireDataSource();
        return importAPI.importExcelWithSQL(dataSourceId, file, createTableSQL, tableName, sheetName, columnMapping);
      }
    },

    export: {
      exportData: (data: any) => {
        // Export doesn't require data source as it works with already fetched data
        return exportAPI.exportData(data);
      }
    }
  };
};

// Data source management API
export const dataSourceAPI = {
  // Get all data sources
  getDataSources: () => api.get('/data-sources/'),
  
  // Create a new data source
  createDataSource: (dataSource: Omit<DataSource, 'id' | 'created_at' | 'updated_at'>) => 
    api.post('/data-sources/', dataSource),
  
  // Delete a data source
  deleteDataSource: (id: number) => api.delete(`/data-sources/${id}`),
  
  // Test connection to a data source
  testConnection: (connectionData: { type: string; connection_config: Record<string, any> }) =>
    api.post('/data-sources/test-connection', connectionData),
  
  // Get supported data source types
  getSupportedDataSources: () => api.get('/data-sources/supported'),
  
  // Get schema information for a data source
  getSchema: (connectionData: { type: string; connection_config: Record<string, any> }) =>
    api.post('/data-sources/schema', connectionData),
  
  // Preview data from a data source
  previewData: (
    type: string, 
    connection_config: Record<string, any>, 
    source_name: string, 
    limit: number = 100
  ) => api.post('/data-sources/preview', {
    type,
    connection_config,
    source_name,
    limit
  }),
  
  // Extract data from a data source
  extractData: (dataSourceId: number, jobConfig: any) =>
    api.post(`/data-sources/${dataSourceId}/extract`, jobConfig)
};

export { DataSourceRequiredError };