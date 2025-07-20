import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api/v1';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// SQL API - requires data source
export const sqlAPI = {
  executeQuery: (dataSourceId: number, sql: string) => 
    api.post('/sql/execute', { data_source_id: dataSourceId, query: sql }),
  
  getTables: (dataSourceId: number) => 
    api.post('/sql/tables', { data_source_id: dataSourceId }),
  
  getTableInfo: (dataSourceId: number, tableName: string) => 
    api.post('/sql/table-info', { data_source_id: dataSourceId, table_name: tableName }),
  
  validateQuery: (dataSourceId: number, sql: string) =>
    api.post('/sql/validate', { data_source_id: dataSourceId, query: sql }),
};

// Import API - requires data source
export const importAPI = {
  uploadCSV: (dataSourceId: number, file: File, tableName?: string, createTable: boolean = true, detectTypes: boolean = true) => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('data_source_id', String(dataSourceId));
    if (tableName) {
      formData.append('table_name', tableName);
    }
    formData.append('create_table', String(createTable));
    formData.append('detect_types', String(detectTypes));
    return api.post('/import/csv', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },

  previewCSV: (file: File, tableName?: string, sampleSize: number = 10) => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('sample_size', String(sampleSize));
    if (tableName) {
      formData.append('table_name', tableName);
    }
    
    return api.post('/import/csv/preview', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },

  importCSVWithSQL: (dataSourceId: number, file: File, createTableSQL: string, tableName: string, columnMapping?: Record<string, string>) => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('data_source_id', String(dataSourceId));
    formData.append('create_table_sql', createTableSQL);
    formData.append('table_name', tableName);
    if (columnMapping) {
      formData.append('column_mapping', JSON.stringify(columnMapping));
    }
    
    return api.post('/import/csv/import-with-sql', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },

  uploadCSVBatch: (dataSourceId: number, files: File[], createTable: boolean = true, detectTypes: boolean = true) => {
    const formData = new FormData();
    files.forEach(file => {
      formData.append('files', file);
    });
    formData.append('data_source_id', String(dataSourceId));
    formData.append('create_table', String(createTable));
    formData.append('detect_types', String(detectTypes));
    
    return api.post('/import/csv/batch', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },

  importCSVWithConfig: (dataSourceId: number, file: File, config: {
    table_name: string;
    columns: Array<{
      name: string;
      type: string;
      nullable?: boolean;
      primary_key?: boolean;
      unique?: boolean;
      default_value?: string;
    }>;
  }) => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('data_source_id', String(dataSourceId));
    formData.append('config', JSON.stringify(config));
    
    return api.post('/import/csv/import-with-config', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },
  
  uploadExcel: (dataSourceId: number, file: File, tableName?: string, sheetName?: string, importAllSheets: boolean = false, createTable: boolean = true, detectTypes: boolean = true) => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('data_source_id', String(dataSourceId));
    if (tableName) formData.append('table_name', tableName);
    if (sheetName) formData.append('sheet_name', sheetName);
    formData.append('import_all_sheets', String(importAllSheets));
    formData.append('create_table', String(createTable));
    formData.append('detect_types', String(detectTypes));
    return api.post('/import/excel', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },
  
  previewExcel: (file: File, sheetName?: string, rows: number = 10) => {
    const formData = new FormData();
    formData.append('file', file);
    if (sheetName) formData.append('sheet_name', sheetName);
    formData.append('rows', String(rows));
    return api.post('/import/excel/preview', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },
  
  getExcelSheets: (file: File) => {
    const formData = new FormData();
    formData.append('file', file);
    return api.post('/import/excel/sheets', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },

  importExcelWithSQL: (dataSourceId: number, file: File, createTableSQL: string, tableName: string, sheetName?: string, columnMapping?: Record<string, string>) => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('data_source_id', String(dataSourceId));
    formData.append('create_table_sql', createTableSQL);
    formData.append('table_name', tableName);
    if (sheetName) {
      formData.append('sheet_name', sheetName);
    }
    if (columnMapping) {
      formData.append('column_mapping', JSON.stringify(columnMapping));
    }
    
    return api.post('/import/excel/import-with-sql', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },
};

export const exportAPI = {
  exportData: (data: { data: any[], columns: string[], format: 'csv' | 'excel', filename: string }) => 
    api.post('/export/data', data, {
      responseType: 'blob',
    }),
};

export const tableAPI = {
  getTableData: (dataSourceId: number, params: {
    table_name: string;
    page: number;
    page_size: number;
    search_column?: string;
    search_value?: string;
    order_by?: string;
    order_direction?: 'ASC' | 'DESC';
  }) => api.post('/tables/data', { data_source_id: dataSourceId, ...params }),
  
  createRecord: (dataSourceId: number, table_name: string, data: Record<string, any>) => 
    api.post('/tables/record', { data_source_id: dataSourceId, table_name, data }),
  
  updateRecord: (dataSourceId: number, table_name: string, primary_key_column: string, primary_key_value: any, data: Record<string, any>) => {
    const payload = { data_source_id: dataSourceId, table_name, primary_key_column, primary_key_value, data };
    console.log('API updateRecord payload:', payload);
    return api.put('/tables/record', payload);
  },
  
  deleteRecord: (dataSourceId: number, table_name: string, primary_key_column: string, primary_key_value: any) =>
    api.post('/tables/record/delete', { data_source_id: dataSourceId, table_name, primary_key_column, primary_key_value }),
  
  getTableInfo: (dataSourceId: number, table_name: string) =>
    api.post('/sql/table-info', { data_source_id: dataSourceId, table_name }),
  
  deleteTable: (dataSourceId: number, table_name: string) => {
    const formData = new FormData();
    formData.append('data_source_id', String(dataSourceId));
    return api.delete(`/tables/table/${table_name}`, {
      data: formData,
      headers: { 'Content-Type': 'multipart/form-data' }
    });
  },
};

export default api;