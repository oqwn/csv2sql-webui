import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api/v1';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// No authentication needed

// Authentication endpoints removed

export const sqlAPI = {
  executeQuery: (sql: string) => api.post('/sql/execute', { sql }),
  getTables: () => api.get('/sql/tables'),
  getTableColumns: (tableName: string) => api.get(`/sql/tables/${tableName}/columns`),
};

export const importAPI = {
  uploadCSV: (file: File, tableName?: string, createTable: boolean = true, detectTypes: boolean = true) => {
    const formData = new FormData();
    formData.append('file', file);
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

  importCSVWithSQL: (file: File, createTableSQL: string, tableName: string) => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('create_table_sql', createTableSQL);
    formData.append('table_name', tableName);
    
    return api.post('/import/csv/import-with-sql', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },

  uploadCSVBatch: (files: File[], createTable: boolean = true, detectTypes: boolean = true) => {
    const formData = new FormData();
    files.forEach(file => {
      formData.append('files', file);
    });
    formData.append('create_table', String(createTable));
    formData.append('detect_types', String(detectTypes));
    
    return api.post('/import/csv/batch', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },

  importCSVWithConfig: (file: File, config: {
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
    formData.append('config', JSON.stringify(config));
    
    return api.post('/import/csv/import-with-config', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },
  
  uploadExcel: (file: File, tableName?: string, sheetName?: string, importAllSheets: boolean = false, createTable: boolean = true, detectTypes: boolean = true) => {
    const formData = new FormData();
    formData.append('file', file);
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
};

export const exportAPI = {
  exportData: (data: { data: any[], columns: string[], format: 'csv' | 'excel', filename: string }) => 
    api.post('/export/data', data, {
      responseType: 'blob',
    }),
};

export const tableAPI = {
  getTableData: (params: {
    table_name: string;
    page: number;
    page_size: number;
    search_column?: string;
    search_value?: string;
    order_by?: string;
    order_direction?: 'ASC' | 'DESC';
  }) => api.post('/tables/data', params),
  
  createRecord: (table_name: string, data: Record<string, any>) => 
    api.post('/tables/record', { table_name, data }),
  
  updateRecord: (table_name: string, primary_key_column: string, primary_key_value: any, data: Record<string, any>) => {
    const payload = { table_name, primary_key_column, primary_key_value, data };
    console.log('API updateRecord payload:', payload);
    return api.put('/tables/record', payload);
  },
  
  deleteRecord: (table_name: string, primary_key_column: string, primary_key_value: any) =>
    api.post('/tables/record/delete', { table_name, primary_key_column, primary_key_value }),
  
  getTableInfo: (table_name: string) =>
    api.get(`/tables/table/${table_name}/info`),
  
  deleteTable: (table_name: string) =>
    api.delete(`/tables/table/${table_name}`),
};

export default api;