import { SQLParser, ParsedSQL } from './sqlParser';

export interface ValidationResult {
  isValid: boolean;
  errors: string[];
  warnings: string[];
  suggestions: string[];
}

export class SQLValidator {
  private static reservedWords = new Set([
    'ADD', 'ALL', 'ALTER', 'AND', 'ANY', 'AS', 'ASC', 'BACKUP', 'BETWEEN', 'CASE',
    'CHECK', 'COLUMN', 'CONSTRAINT', 'CREATE', 'DATABASE', 'DEFAULT', 'DELETE',
    'DESC', 'DISTINCT', 'DROP', 'EXEC', 'EXISTS', 'FOREIGN', 'FROM', 'FULL',
    'GROUP', 'HAVING', 'IN', 'INDEX', 'INNER', 'INSERT', 'INTO', 'IS', 'JOIN',
    'KEY', 'LEFT', 'LIKE', 'LIMIT', 'NOT', 'NULL', 'OR', 'ORDER', 'OUTER',
    'PRIMARY', 'PROCEDURE', 'RIGHT', 'ROWNUM', 'SELECT', 'SET', 'TABLE', 'TOP',
    'TRUNCATE', 'UNION', 'UNIQUE', 'UPDATE', 'VALUES', 'VIEW', 'WHERE'
  ]);

  private static dangerousPatterns = [
    /;\s*DROP\s+/i,
    /;\s*DELETE\s+/i,
    /;\s*TRUNCATE\s+/i,
    /;\s*UPDATE\s+/i,
    /--.*$/gm,
    /\/\*[\s\S]*?\*\//g,
    /\bEXEC\b/i,
    /\bEXECUTE\b/i,
    /\bxp_\w+/i,
    /\bsp_\w+/i
  ];

  static validate(sql: string): ValidationResult {
    const errors: string[] = [];
    const warnings: string[] = [];
    const suggestions: string[] = [];

    // Check for empty query
    if (!sql || sql.trim().length === 0) {
      errors.push('SQL query cannot be empty');
      return { isValid: false, errors, warnings, suggestions };
    }

    // Check for dangerous patterns (SQL injection attempts)
    for (const pattern of this.dangerousPatterns) {
      if (pattern.test(sql)) {
        warnings.push('Query contains potentially dangerous patterns');
        break;
      }
    }

    // Parse the SQL
    const parsed = SQLParser.parse(sql);
    
    if (!parsed.isValid) {
      errors.push(parsed.error || 'Invalid SQL syntax');
      return { isValid: false, errors, warnings, suggestions };
    }

    // Validate based on statement type
    switch (parsed.type) {
      case 'CREATE_TABLE':
        this.validateCreateTable(parsed, errors, warnings, suggestions);
        break;
      case 'INSERT':
        this.validateInsert(parsed, errors, warnings, suggestions);
        break;
      case 'SELECT':
        this.validateSelect(parsed, errors, warnings, suggestions);
        break;
      case 'UPDATE':
        this.validateUpdate(parsed, errors, warnings, suggestions);
        break;
      case 'DELETE':
        this.validateDelete(parsed, errors, warnings, suggestions);
        break;
      case 'DROP_TABLE':
        this.validateDrop(parsed, errors, warnings, suggestions);
        break;
      case 'ALTER_TABLE':
        this.validateAlter(parsed, errors, warnings, suggestions);
        break;
    }

    // Check for multiple statements
    if (sql.trim().split(';').filter(s => s.trim()).length > 1) {
      warnings.push('Multiple statements detected. Only the first statement will be executed.');
    }

    return {
      isValid: errors.length === 0,
      errors,
      warnings,
      suggestions
    };
  }

  private static validateCreateTable(parsed: ParsedSQL, errors: string[], warnings: string[], suggestions: string[]) {
    // Validate table name
    if (!parsed.tableName) {
      errors.push('Table name is required');
      return;
    }

    if (this.reservedWords.has(parsed.tableName.toUpperCase())) {
      errors.push(`"${parsed.tableName}" is a reserved SQL keyword and cannot be used as a table name`);
      suggestions.push(`Consider using a different name or wrapping it in quotes`);
    }

    // Validate columns
    if (!parsed.columns || parsed.columns.length === 0) {
      errors.push('At least one column must be defined');
      return;
    }

    const columnNames = new Set<string>();
    let hasPrimaryKey = false;

    parsed.columns.forEach((column) => {
      // Check for duplicate column names
      if (columnNames.has(column.name.toLowerCase())) {
        errors.push(`Duplicate column name: "${column.name}"`);
      }
      columnNames.add(column.name.toLowerCase());

      // Check for reserved words in column names
      if (this.reservedWords.has(column.name.toUpperCase())) {
        warnings.push(`Column "${column.name}" uses a reserved SQL keyword`);
        suggestions.push(`Consider using a different name or wrapping it in quotes`);
      }

      // Check for data type
      if (!column.type) {
        errors.push(`Column "${column.name}" is missing a data type`);
      }

      // Check for primary key
      if (column.constraints?.includes('PRIMARY KEY')) {
        if (hasPrimaryKey) {
          errors.push('Multiple PRIMARY KEY definitions found. A table can only have one primary key.');
        }
        hasPrimaryKey = true;
      }

      // Validate column name
      if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(column.name)) {
        warnings.push(`Column name "${column.name}" contains special characters. Consider using only letters, numbers, and underscores.`);
      }
    });

    if (!hasPrimaryKey) {
      suggestions.push('Consider adding a PRIMARY KEY to your table for better performance and data integrity');
    }

    // Check table name format
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(parsed.tableName)) {
      warnings.push(`Table name "${parsed.tableName}" contains special characters. Consider using only letters, numbers, and underscores.`);
    }
  }

  private static validateInsert(parsed: ParsedSQL, errors: string[], warnings: string[], suggestions: string[]) {
    if (!parsed.tableName) {
      errors.push('Table name is required for INSERT statement');
    }

    if (!parsed.values || parsed.values.length === 0) {
      errors.push('VALUES clause is required with at least one value');
    }

    if (parsed.columns && parsed.values && parsed.columns.length !== parsed.values.length) {
      errors.push(`Column count (${parsed.columns.length}) does not match value count (${parsed.values.length})`);
    }

    if (!parsed.columns) {
      warnings.push('No column list specified. Values will be inserted in the order of table columns.');
      suggestions.push('Consider specifying column names for clarity and safety');
    }
  }

  private static validateSelect(parsed: ParsedSQL, errors: string[], _warnings: string[], suggestions: string[]) {
    if (!parsed.tableName) {
      errors.push('Table name is required for SELECT statement');
    }

    // Check for SELECT *
    const sql = parsed.conditions || '';
    if (sql.includes('*')) {
      suggestions.push('Consider selecting specific columns instead of using * for better performance');
    }
  }

  private static validateUpdate(parsed: ParsedSQL, errors: string[], warnings: string[], suggestions: string[]) {
    if (!parsed.tableName) {
      errors.push('Table name is required for UPDATE statement');
    }

    if (!parsed.conditions || !parsed.conditions.includes('WHERE')) {
      warnings.push('UPDATE without WHERE clause will modify all rows in the table');
      suggestions.push('Add a WHERE clause to target specific rows');
    }
  }

  private static validateDelete(parsed: ParsedSQL, errors: string[], warnings: string[], suggestions: string[]) {
    if (!parsed.tableName) {
      errors.push('Table name is required for DELETE statement');
    }

    if (!parsed.conditions || !parsed.conditions.includes('WHERE')) {
      warnings.push('DELETE without WHERE clause will remove all rows from the table');
      suggestions.push('Add a WHERE clause to target specific rows, or use TRUNCATE for better performance if you want to delete all rows');
    }
  }

  private static validateDrop(parsed: ParsedSQL, errors: string[], warnings: string[], _suggestions: string[]) {
    if (!parsed.tableName) {
      errors.push('Table name is required for DROP TABLE statement');
    }

    warnings.push('DROP TABLE will permanently delete the table and all its data');
  }

  private static validateAlter(parsed: ParsedSQL, errors: string[], _warnings: string[], suggestions: string[]) {
    if (!parsed.tableName) {
      errors.push('Table name is required for ALTER TABLE statement');
    }

    suggestions.push('Make sure to backup your data before altering table structure');
  }

  static getSuggestions(sql: string, cursorPosition: number): string[] {
    const suggestions: string[] = [];
    const beforeCursor = sql.substring(0, cursorPosition).toUpperCase();
    const lastWord = beforeCursor.split(/\s+/).pop() || '';

    // Context-based suggestions
    if (beforeCursor.endsWith('CREATE ')) {
      suggestions.push('TABLE', 'DATABASE', 'INDEX', 'VIEW');
    } else if (beforeCursor.endsWith('DROP ')) {
      suggestions.push('TABLE', 'DATABASE', 'INDEX', 'VIEW');
    } else if (beforeCursor.endsWith('ALTER ')) {
      suggestions.push('TABLE');
    } else if (beforeCursor.endsWith('INSERT ')) {
      suggestions.push('INTO');
    } else if (beforeCursor.endsWith('DELETE ')) {
      suggestions.push('FROM');
    } else if (beforeCursor.endsWith('SELECT ')) {
      suggestions.push('*', 'DISTINCT', 'COUNT(*)', 'SUM(', 'AVG(', 'MAX(', 'MIN(');
    } else if (beforeCursor.includes('FROM ') && !beforeCursor.includes('WHERE')) {
      suggestions.push('WHERE', 'ORDER BY', 'GROUP BY', 'LIMIT');
    } else if (beforeCursor.includes('WHERE ')) {
      suggestions.push('AND', 'OR', 'NOT', 'IN', 'BETWEEN', 'LIKE', 'IS NULL', 'IS NOT NULL');
    } else if (beforeCursor.includes('ORDER BY ')) {
      suggestions.push('ASC', 'DESC');
    } else if (lastWord.length > 0) {
      // Keyword suggestions based on partial input
      const keywords = Array.from(this.reservedWords);
      suggestions.push(...keywords.filter(k => k.startsWith(lastWord)));
    }

    return suggestions.slice(0, 10); // Limit suggestions
  }
}