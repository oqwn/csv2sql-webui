import { SQLParser, ParsedSQL } from './sqlParser';
import { SQLValidator } from './sqlValidator';

export interface ExecutionResult {
  success: boolean;
  data?: any;
  error?: string;
  executionTime: number;
  rowsAffected?: number;
}

export interface TableSchema {
  name: string;
  columns: Array<{
    name: string;
    type: string;
    constraints?: string[];
  }>;
}

export interface QueryPlan {
  operation: string;
  tableName?: string;
  estimatedRows?: number;
  cost?: number;
  details?: string;
}

export class SQLExecutionEngine {
  private tables: Map<string, TableSchema>;
  private tableData: Map<string, any[]>;

  constructor() {
    this.tables = new Map();
    this.tableData = new Map();
  }

  async execute(sql: string): Promise<ExecutionResult> {
    const startTime = performance.now();

    try {
      // Validate SQL
      const validation = SQLValidator.validate(sql);
      if (!validation.isValid) {
        return {
          success: false,
          error: validation.errors.join('; '),
          executionTime: performance.now() - startTime
        };
      }

      // Parse SQL
      const parsed = SQLParser.parse(sql);
      if (!parsed.isValid) {
        return {
          success: false,
          error: parsed.error || 'Failed to parse SQL',
          executionTime: performance.now() - startTime
        };
      }

      // Execute based on statement type
      let result: ExecutionResult;
      switch (parsed.type) {
        case 'CREATE_TABLE':
          result = await this.executeCreateTable(parsed);
          break;
        case 'INSERT':
          result = await this.executeInsert(parsed);
          break;
        case 'SELECT':
          result = await this.executeSelect(parsed);
          break;
        case 'UPDATE':
          result = await this.executeUpdate(parsed);
          break;
        case 'DELETE':
          result = await this.executeDelete(parsed);
          break;
        case 'DROP_TABLE':
          result = await this.executeDropTable(parsed);
          break;
        case 'ALTER_TABLE':
          result = await this.executeAlterTable(parsed);
          break;
        default:
          result = {
            success: false,
            error: `Unsupported operation: ${parsed.type}`,
            executionTime: 0
          };
      }

      result.executionTime = performance.now() - startTime;
      return result;
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error occurred',
        executionTime: performance.now() - startTime
      };
    }
  }

  private async executeCreateTable(parsed: ParsedSQL): Promise<ExecutionResult> {
    if (!parsed.tableName || !parsed.columns) {
      return {
        success: false,
        error: 'Invalid CREATE TABLE statement',
        executionTime: 0
      };
    }

    if (this.tables.has(parsed.tableName.toLowerCase())) {
      return {
        success: false,
        error: `Table '${parsed.tableName}' already exists`,
        executionTime: 0
      };
    }

    // Create table schema
    const schema: TableSchema = {
      name: parsed.tableName,
      columns: parsed.columns.map(col => ({
        name: col.name,
        type: col.type || 'TEXT', // Default type if not specified
        constraints: col.constraints
      }))
    };

    this.tables.set(parsed.tableName.toLowerCase(), schema);
    this.tableData.set(parsed.tableName.toLowerCase(), []);

    return {
      success: true,
      data: { message: `Table '${parsed.tableName}' created successfully` },
      executionTime: 0
    };
  }

  private async executeInsert(parsed: ParsedSQL): Promise<ExecutionResult> {
    if (!parsed.tableName) {
      return {
        success: false,
        error: 'Invalid INSERT statement',
        executionTime: 0
      };
    }

    const tableName = parsed.tableName.toLowerCase();
    const schema = this.tables.get(tableName);
    
    if (!schema) {
      return {
        success: false,
        error: `Table '${parsed.tableName}' does not exist`,
        executionTime: 0
      };
    }

    // For demo purposes, just store the values
    const tableData = this.tableData.get(tableName) || [];
    const newRow: any = {};

    if (parsed.columns && parsed.values) {
      parsed.columns.forEach((col, index) => {
        if (index < parsed.values!.length) {
          newRow[col.name] = parsed.values![index];
        }
      });
    } else if (parsed.values) {
      // Insert values in order of schema columns
      schema.columns.forEach((col, index) => {
        if (index < parsed.values!.length) {
          newRow[col.name] = parsed.values![index];
        }
      });
    }

    tableData.push(newRow);
    this.tableData.set(tableName, tableData);

    return {
      success: true,
      data: { message: '1 row inserted' },
      rowsAffected: 1,
      executionTime: 0
    };
  }

  private async executeSelect(parsed: ParsedSQL): Promise<ExecutionResult> {
    if (!parsed.tableName) {
      return {
        success: false,
        error: 'Invalid SELECT statement',
        executionTime: 0
      };
    }

    const tableName = parsed.tableName.toLowerCase();
    const schema = this.tables.get(tableName);
    
    if (!schema) {
      return {
        success: false,
        error: `Table '${parsed.tableName}' does not exist`,
        executionTime: 0
      };
    }

    const tableData = this.tableData.get(tableName) || [];

    return {
      success: true,
      data: {
        columns: schema.columns.map(c => c.name),
        rows: tableData.map(row => schema.columns.map(c => row[c.name] ?? null)),
        rowCount: tableData.length
      },
      executionTime: 0
    };
  }

  private async executeUpdate(parsed: ParsedSQL): Promise<ExecutionResult> {
    if (!parsed.tableName) {
      return {
        success: false,
        error: 'Invalid UPDATE statement',
        executionTime: 0
      };
    }

    const tableName = parsed.tableName.toLowerCase();
    
    if (!this.tables.has(tableName)) {
      return {
        success: false,
        error: `Table '${parsed.tableName}' does not exist`,
        executionTime: 0
      };
    }

    // For demo purposes, just return success
    return {
      success: true,
      data: { message: '0 rows updated' },
      rowsAffected: 0,
      executionTime: 0
    };
  }

  private async executeDelete(parsed: ParsedSQL): Promise<ExecutionResult> {
    if (!parsed.tableName) {
      return {
        success: false,
        error: 'Invalid DELETE statement',
        executionTime: 0
      };
    }

    const tableName = parsed.tableName.toLowerCase();
    
    if (!this.tables.has(tableName)) {
      return {
        success: false,
        error: `Table '${parsed.tableName}' does not exist`,
        executionTime: 0
      };
    }

    // For demo purposes, clear all data if no WHERE clause
    if (!parsed.conditions || !parsed.conditions.includes('WHERE')) {
      const rowCount = this.tableData.get(tableName)?.length || 0;
      this.tableData.set(tableName, []);
      
      return {
        success: true,
        data: { message: `${rowCount} rows deleted` },
        rowsAffected: rowCount,
        executionTime: 0
      };
    }

    return {
      success: true,
      data: { message: '0 rows deleted' },
      rowsAffected: 0,
      executionTime: 0
    };
  }

  private async executeDropTable(parsed: ParsedSQL): Promise<ExecutionResult> {
    if (!parsed.tableName) {
      return {
        success: false,
        error: 'Invalid DROP TABLE statement',
        executionTime: 0
      };
    }

    const tableName = parsed.tableName.toLowerCase();
    
    if (!this.tables.has(tableName)) {
      return {
        success: false,
        error: `Table '${parsed.tableName}' does not exist`,
        executionTime: 0
      };
    }

    this.tables.delete(tableName);
    this.tableData.delete(tableName);

    return {
      success: true,
      data: { message: `Table '${parsed.tableName}' dropped successfully` },
      executionTime: 0
    };
  }

  private async executeAlterTable(parsed: ParsedSQL): Promise<ExecutionResult> {
    if (!parsed.tableName) {
      return {
        success: false,
        error: 'Invalid ALTER TABLE statement',
        executionTime: 0
      };
    }

    const tableName = parsed.tableName.toLowerCase();
    
    if (!this.tables.has(tableName)) {
      return {
        success: false,
        error: `Table '${parsed.tableName}' does not exist`,
        executionTime: 0
      };
    }

    // For demo purposes, just return success
    return {
      success: true,
      data: { message: `Table '${parsed.tableName}' altered successfully` },
      executionTime: 0
    };
  }

  getQueryPlan(sql: string): QueryPlan | null {
    const parsed = SQLParser.parse(sql);
    if (!parsed.isValid) return null;

    const plan: QueryPlan = {
      operation: parsed.type,
      tableName: parsed.tableName
    };

    switch (parsed.type) {
      case 'SELECT': {
        const tableData = this.tableData.get(parsed.tableName?.toLowerCase() || '');
        plan.estimatedRows = tableData?.length || 0;
        plan.cost = 1; // Simple cost model
        plan.details = 'Full table scan';
        break;
      }
      case 'INSERT':
        plan.estimatedRows = 1;
        plan.cost = 1;
        plan.details = 'Single row insert';
        break;
      case 'CREATE_TABLE':
        plan.cost = 10;
        plan.details = 'Create new table structure';
        break;
      default:
        plan.cost = 5;
    }

    return plan;
  }

  getTables(): string[] {
    return Array.from(this.tables.keys());
  }

  getTableSchema(tableName: string): TableSchema | undefined {
    return this.tables.get(tableName.toLowerCase());
  }

  exportToJSON(): string {
    const data = {
      tables: Array.from(this.tables.entries()).map(([name, schema]) => ({
        name,
        schema,
        data: this.tableData.get(name) || []
      }))
    };
    return JSON.stringify(data, null, 2);
  }

  importFromJSON(json: string): boolean {
    try {
      const data = JSON.parse(json);
      this.tables.clear();
      this.tableData.clear();

      for (const table of data.tables) {
        this.tables.set(table.name.toLowerCase(), table.schema);
        this.tableData.set(table.name.toLowerCase(), table.data);
      }

      return true;
    } catch {
      return false;
    }
  }
}