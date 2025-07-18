export interface ParsedSQL {
  type: 'CREATE_TABLE' | 'INSERT' | 'SELECT' | 'UPDATE' | 'DELETE' | 'DROP_TABLE' | 'ALTER_TABLE' | 'UNKNOWN';
  tableName?: string;
  columns?: Array<{
    name: string;
    type?: string;
    constraints?: string[];
  }>;
  values?: any[];
  conditions?: string;
  isValid: boolean;
  error?: string;
}

export interface SQLToken {
  type: 'KEYWORD' | 'IDENTIFIER' | 'OPERATOR' | 'STRING' | 'NUMBER' | 'PUNCTUATION' | 'WHITESPACE' | 'COMMENT';
  value: string;
  position: number;
}

export class SQLParser {
  private static keywords = new Set([
    'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'CREATE', 'TABLE',
    'DROP', 'ALTER', 'UPDATE', 'DELETE', 'SET', 'AND', 'OR', 'NOT', 'NULL',
    'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'UNIQUE', 'DEFAULT', 'CHECK',
    'IN', 'BETWEEN', 'LIKE', 'EXISTS', 'JOIN', 'LEFT', 'RIGHT', 'INNER',
    'OUTER', 'ON', 'AS', 'ORDER', 'BY', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET',
    'ASC', 'DESC', 'DISTINCT', 'ALL', 'UNION', 'EXCEPT', 'INTERSECT',
    'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'CAST', 'CONSTRAINT', 'INDEX',
    'IF', 'EXISTS', 'CASCADE', 'RESTRICT', 'COLUMN', 'ADD', 'MODIFY',
    'AUTO_INCREMENT', 'AUTOINCREMENT'
  ]);

  private static dataTypes = new Set([
    'INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'DECIMAL', 'NUMERIC',
    'REAL', 'FLOAT', 'DOUBLE', 'BIT', 'BOOLEAN', 'BOOL', 'DATE', 'TIME',
    'DATETIME', 'TIMESTAMP', 'YEAR', 'CHAR', 'VARCHAR', 'TEXT', 'TINYTEXT',
    'MEDIUMTEXT', 'LONGTEXT', 'BINARY', 'VARBINARY', 'BLOB', 'TINYBLOB',
    'MEDIUMBLOB', 'LONGBLOB', 'JSON', 'UUID', 'SERIAL'
  ]);

  private static operators = new Set([
    '=', '!=', '<>', '<', '>', '<=', '>=', '+', '-', '*', '/', '%'
  ]);

  static tokenize(sql: string): SQLToken[] {
    const tokens: SQLToken[] = [];
    let i = 0;

    while (i < sql.length) {
      // Skip whitespace
      if (/\s/.test(sql[i])) {
        const start = i;
        while (i < sql.length && /\s/.test(sql[i])) i++;
        tokens.push({ type: 'WHITESPACE', value: sql.slice(start, i), position: start });
        continue;
      }

      // Comments
      if (sql.slice(i, i + 2) === '--') {
        const start = i;
        while (i < sql.length && sql[i] !== '\n') i++;
        tokens.push({ type: 'COMMENT', value: sql.slice(start, i), position: start });
        continue;
      }

      // String literals
      if (sql[i] === "'" || sql[i] === '"') {
        const quote = sql[i];
        const start = i;
        i++;
        while (i < sql.length && sql[i] !== quote) {
          if (sql[i] === '\\') i++; // Skip escaped characters
          i++;
        }
        i++; // Include closing quote
        tokens.push({ type: 'STRING', value: sql.slice(start, i), position: start });
        continue;
      }

      // Numbers
      if (/[0-9]/.test(sql[i]) || (sql[i] === '.' && i + 1 < sql.length && /[0-9]/.test(sql[i + 1]))) {
        const start = i;
        while (i < sql.length && (/[0-9.]/.test(sql[i]) || sql[i].toLowerCase() === 'e')) i++;
        tokens.push({ type: 'NUMBER', value: sql.slice(start, i), position: start });
        continue;
      }

      // Operators
      const twoCharOp = sql.slice(i, i + 2);
      if (this.operators.has(twoCharOp)) {
        tokens.push({ type: 'OPERATOR', value: twoCharOp, position: i });
        i += 2;
        continue;
      }
      if (this.operators.has(sql[i])) {
        tokens.push({ type: 'OPERATOR', value: sql[i], position: i });
        i++;
        continue;
      }

      // Punctuation
      if (/[(),;.]/.test(sql[i])) {
        tokens.push({ type: 'PUNCTUATION', value: sql[i], position: i });
        i++;
        continue;
      }

      // Identifiers and keywords
      if (/[a-zA-Z_]/.test(sql[i])) {
        const start = i;
        while (i < sql.length && /[a-zA-Z0-9_]/.test(sql[i])) i++;
        const value = sql.slice(start, i);
        const upperValue = value.toUpperCase();
        
        if (this.keywords.has(upperValue) || this.dataTypes.has(upperValue)) {
          tokens.push({ type: 'KEYWORD', value: upperValue, position: start });
        } else {
          tokens.push({ type: 'IDENTIFIER', value, position: start });
        }
        continue;
      }

      // Unknown character
      i++;
    }

    return tokens.filter(t => t.type !== 'WHITESPACE');
  }

  static parse(sql: string): ParsedSQL {
    const tokens = this.tokenize(sql.trim());
    
    if (tokens.length === 0) {
      return { type: 'UNKNOWN', isValid: false, error: 'Empty query' };
    }

    const firstToken = tokens[0];
    
    if (firstToken.type !== 'KEYWORD') {
      return { type: 'UNKNOWN', isValid: false, error: 'Query must start with a SQL keyword' };
    }

    switch (firstToken.value) {
      case 'CREATE':
        return this.parseCreateTable(tokens);
      case 'INSERT':
        return this.parseInsert(tokens);
      case 'SELECT':
        return this.parseSelect(tokens);
      case 'UPDATE':
        return this.parseUpdate(tokens);
      case 'DELETE':
        return this.parseDelete(tokens);
      case 'DROP':
        return this.parseDrop(tokens);
      case 'ALTER':
        return this.parseAlter(tokens);
      default:
        return { type: 'UNKNOWN', isValid: false, error: `Unsupported statement type: ${firstToken.value}` };
    }
  }

  private static parseCreateTable(tokens: SQLToken[]): ParsedSQL {
    try {
      let i = 0;
      
      // CREATE TABLE
      if (tokens[i]?.value !== 'CREATE' || tokens[i + 1]?.value !== 'TABLE') {
        return { type: 'CREATE_TABLE', isValid: false, error: 'Expected CREATE TABLE' };
      }
      i += 2;

      // Optional IF NOT EXISTS
      if (tokens[i]?.value === 'IF' && tokens[i + 1]?.value === 'NOT' && tokens[i + 2]?.value === 'EXISTS') {
        i += 3;
      }

      // Table name
      if (tokens[i]?.type !== 'IDENTIFIER') {
        return { type: 'CREATE_TABLE', isValid: false, error: 'Expected table name' };
      }
      const tableName = tokens[i].value;
      i++;

      // Opening parenthesis
      if (tokens[i]?.value !== '(') {
        return { type: 'CREATE_TABLE', isValid: false, error: 'Expected opening parenthesis' };
      }
      i++;

      // Parse columns
      const columns: Array<{ name: string; type?: string; constraints?: string[] }> = [];
      
      while (i < tokens.length && tokens[i]?.value !== ')') {
        // Column name
        if (tokens[i]?.type !== 'IDENTIFIER') {
          return { type: 'CREATE_TABLE', isValid: false, error: 'Expected column name' };
        }
        const columnName = tokens[i].value;
        i++;

        // Data type
        let dataType = '';
        if (tokens[i]?.type === 'KEYWORD' && this.dataTypes.has(tokens[i].value)) {
          dataType = tokens[i].value;
          i++;

          // Handle types with size like VARCHAR(255)
          if (tokens[i]?.value === '(') {
            dataType += '(';
            i++;
            while (i < tokens.length && tokens[i]?.value !== ')') {
              dataType += tokens[i].value;
              i++;
            }
            if (tokens[i]?.value === ')') {
              dataType += ')';
              i++;
            }
          }
        }

        // Constraints
        const constraints: string[] = [];
        while (i < tokens.length && tokens[i]?.value !== ',' && tokens[i]?.value !== ')') {
          if (tokens[i]?.type === 'KEYWORD') {
            let constraint = tokens[i].value;
            i++;

            // Handle multi-word constraints
            if (constraint === 'PRIMARY' && tokens[i]?.value === 'KEY') {
              constraint = 'PRIMARY KEY';
              i++;
            } else if (constraint === 'FOREIGN' && tokens[i]?.value === 'KEY') {
              constraint = 'FOREIGN KEY';
              i++;
            } else if (constraint === 'NOT' && tokens[i]?.value === 'NULL') {
              constraint = 'NOT NULL';
              i++;
            }

            constraints.push(constraint);
          } else {
            i++;
          }
        }

        columns.push({ name: columnName, type: dataType, constraints });

        // Skip comma
        if (tokens[i]?.value === ',') {
          i++;
        }
      }

      // Closing parenthesis
      if (tokens[i]?.value !== ')') {
        return { type: 'CREATE_TABLE', isValid: false, error: 'Expected closing parenthesis' };
      }

      return {
        type: 'CREATE_TABLE',
        tableName,
        columns,
        isValid: true
      };
    } catch {
      return { type: 'CREATE_TABLE', isValid: false, error: 'Failed to parse CREATE TABLE statement' };
    }
  }

  private static parseInsert(tokens: SQLToken[]): ParsedSQL {
    try {
      let i = 0;
      
      // INSERT INTO
      if (tokens[i]?.value !== 'INSERT' || tokens[i + 1]?.value !== 'INTO') {
        return { type: 'INSERT', isValid: false, error: 'Expected INSERT INTO' };
      }
      i += 2;

      // Table name
      if (tokens[i]?.type !== 'IDENTIFIER') {
        return { type: 'INSERT', isValid: false, error: 'Expected table name' };
      }
      const tableName = tokens[i].value;
      i++;

      // Column list (optional)
      const columns: Array<{ name: string }> = [];
      if (tokens[i]?.value === '(') {
        i++;
        while (i < tokens.length && tokens[i]?.value !== ')') {
          if (tokens[i]?.type === 'IDENTIFIER') {
            columns.push({ name: tokens[i].value });
          }
          i++;
        }
        i++; // Skip closing parenthesis
      }

      // VALUES keyword
      if (tokens[i]?.value !== 'VALUES') {
        return { type: 'INSERT', isValid: false, error: 'Expected VALUES keyword' };
      }
      i++;

      // Values
      const values: any[] = [];
      if (tokens[i]?.value === '(') {
        i++;
        while (i < tokens.length && tokens[i]?.value !== ')') {
          if (tokens[i]?.type === 'STRING' || tokens[i]?.type === 'NUMBER' || 
              (tokens[i]?.type === 'KEYWORD' && tokens[i]?.value === 'NULL')) {
            values.push(tokens[i].value);
          }
          i++;
        }
      }

      return {
        type: 'INSERT',
        tableName,
        columns: columns.length > 0 ? columns : undefined,
        values,
        isValid: true
      };
    } catch {
      return { type: 'INSERT', isValid: false, error: 'Failed to parse INSERT statement' };
    }
  }

  private static parseSelect(tokens: SQLToken[]): ParsedSQL {
    // Basic SELECT parsing
    let i = 0;
    
    if (tokens[i]?.value !== 'SELECT') {
      return { type: 'SELECT', isValid: false, error: 'Expected SELECT' };
    }
    i++;

    // Find FROM clause
    const fromIndex = tokens.findIndex((t, idx) => idx > i && t.value === 'FROM');
    if (fromIndex === -1) {
      return { type: 'SELECT', isValid: false, error: 'Expected FROM clause' };
    }

    // Get table name
    if (fromIndex + 1 >= tokens.length || tokens[fromIndex + 1]?.type !== 'IDENTIFIER') {
      return { type: 'SELECT', isValid: false, error: 'Expected table name after FROM' };
    }

    const tableName = tokens[fromIndex + 1].value;

    return {
      type: 'SELECT',
      tableName,
      isValid: true
    };
  }

  private static parseUpdate(tokens: SQLToken[]): ParsedSQL {
    // Basic UPDATE parsing
    let i = 0;
    
    if (tokens[i]?.value !== 'UPDATE') {
      return { type: 'UPDATE', isValid: false, error: 'Expected UPDATE' };
    }
    i++;

    if (tokens[i]?.type !== 'IDENTIFIER') {
      return { type: 'UPDATE', isValid: false, error: 'Expected table name' };
    }

    const tableName = tokens[i].value;

    return {
      type: 'UPDATE',
      tableName,
      isValid: true
    };
  }

  private static parseDelete(tokens: SQLToken[]): ParsedSQL {
    // Basic DELETE parsing
    let i = 0;
    
    if (tokens[i]?.value !== 'DELETE') {
      return { type: 'DELETE', isValid: false, error: 'Expected DELETE' };
    }
    i++;

    if (tokens[i]?.value === 'FROM') {
      i++;
    }

    if (tokens[i]?.type !== 'IDENTIFIER') {
      return { type: 'DELETE', isValid: false, error: 'Expected table name' };
    }

    const tableName = tokens[i].value;

    return {
      type: 'DELETE',
      tableName,
      isValid: true
    };
  }

  private static parseDrop(tokens: SQLToken[]): ParsedSQL {
    // Basic DROP parsing
    let i = 0;
    
    if (tokens[i]?.value !== 'DROP') {
      return { type: 'DROP_TABLE', isValid: false, error: 'Expected DROP' };
    }
    i++;

    if (tokens[i]?.value !== 'TABLE') {
      return { type: 'DROP_TABLE', isValid: false, error: 'Expected TABLE after DROP' };
    }
    i++;

    // Optional IF EXISTS
    if (tokens[i]?.value === 'IF' && tokens[i + 1]?.value === 'EXISTS') {
      i += 2;
    }

    if (tokens[i]?.type !== 'IDENTIFIER') {
      return { type: 'DROP_TABLE', isValid: false, error: 'Expected table name' };
    }

    const tableName = tokens[i].value;

    return {
      type: 'DROP_TABLE',
      tableName,
      isValid: true
    };
  }

  private static parseAlter(tokens: SQLToken[]): ParsedSQL {
    // Basic ALTER parsing
    let i = 0;
    
    if (tokens[i]?.value !== 'ALTER') {
      return { type: 'ALTER_TABLE', isValid: false, error: 'Expected ALTER' };
    }
    i++;

    if (tokens[i]?.value !== 'TABLE') {
      return { type: 'ALTER_TABLE', isValid: false, error: 'Expected TABLE after ALTER' };
    }
    i++;

    if (tokens[i]?.type !== 'IDENTIFIER') {
      return { type: 'ALTER_TABLE', isValid: false, error: 'Expected table name' };
    }

    const tableName = tokens[i].value;

    return {
      type: 'ALTER_TABLE',
      tableName,
      isValid: true
    };
  }
}