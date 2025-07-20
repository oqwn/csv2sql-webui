import { sqlAPI, importAPI, exportAPI } from './api';

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

export { DataSourceRequiredError };