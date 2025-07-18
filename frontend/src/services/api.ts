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
  uploadCSV: (file: File, tableName?: string) => {
    const formData = new FormData();
    formData.append('file', file);
    if (tableName) {
      formData.append('table_name', tableName);
    }
    return api.post('/import/csv', formData, {
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

export default api;