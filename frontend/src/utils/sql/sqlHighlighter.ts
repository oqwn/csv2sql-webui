import { SQLParser, SQLToken } from './sqlParser';

export interface HighlightTheme {
  keyword: string;
  identifier: string;
  string: string;
  number: string;
  operator: string;
  comment: string;
  punctuation: string;
  error: string;
}

export const defaultTheme: HighlightTheme = {
  keyword: '#0000FF',      // Blue
  identifier: '#000000',   // Black
  string: '#008000',       // Green
  number: '#FF6600',       // Orange
  operator: '#808080',     // Gray
  comment: '#008080',      // Teal
  punctuation: '#000000',  // Black
  error: '#FF0000'         // Red
};

export const darkTheme: HighlightTheme = {
  keyword: '#569CD6',      // Light Blue
  identifier: '#D4D4D4',   // Light Gray
  string: '#CE9178',       // Light Brown
  number: '#B5CEA8',       // Light Green
  operator: '#D4D4D4',     // Light Gray
  comment: '#6A9955',      // Green
  punctuation: '#D4D4D4',  // Light Gray
  error: '#F44747'         // Light Red
};

export interface HighlightedSegment {
  text: string;
  color: string;
  type: SQLToken['type'] | 'ERROR';
  start: number;
  end: number;
}

export class SQLHighlighter {
  private theme: HighlightTheme;

  constructor(theme: HighlightTheme = defaultTheme) {
    this.theme = theme;
  }

  setTheme(theme: HighlightTheme) {
    this.theme = theme;
  }

  highlight(sql: string): HighlightedSegment[] {
    if (!sql) return [];

    try {
      const tokens = SQLParser.tokenize(sql);
      const segments: HighlightedSegment[] = [];
      let lastEnd = 0;

      // Add whitespace and tokens
      for (const token of tokens) {
        // Add any whitespace before this token
        if (token.position > lastEnd) {
          const whitespace = sql.substring(lastEnd, token.position);
          segments.push({
            text: whitespace,
            color: this.theme.identifier,
            type: 'WHITESPACE',
            start: lastEnd,
            end: token.position
          });
        }

        // Add the token
        segments.push({
          text: token.value,
          color: this.getTokenColor(token),
          type: token.type,
          start: token.position,
          end: token.position + token.value.length
        });

        lastEnd = token.position + token.value.length;
      }

      // Add any remaining whitespace
      if (lastEnd < sql.length) {
        segments.push({
          text: sql.substring(lastEnd),
          color: this.theme.identifier,
          type: 'WHITESPACE',
          start: lastEnd,
          end: sql.length
        });
      }

      return segments;
    } catch {
      // If parsing fails, return the entire text as an error
      return [{
        text: sql,
        color: this.theme.error,
        type: 'ERROR',
        start: 0,
        end: sql.length
      }];
    }
  }

  private getTokenColor(token: SQLToken): string {
    switch (token.type) {
      case 'KEYWORD':
        return this.theme.keyword;
      case 'IDENTIFIER':
        return this.theme.identifier;
      case 'STRING':
        return this.theme.string;
      case 'NUMBER':
        return this.theme.number;
      case 'OPERATOR':
        return this.theme.operator;
      case 'COMMENT':
        return this.theme.comment;
      case 'PUNCTUATION':
        return this.theme.punctuation;
      default:
        return this.theme.identifier;
    }
  }

  static toHTML(segments: HighlightedSegment[]): string {
    return segments
      .map(segment => {
        const escapedText = segment.text
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#039;');
        
        return `<span style="color: ${segment.color}">${escapedText}</span>`;
      })
      .join('');
  }

  static toReactElements(segments: HighlightedSegment[]): React.ReactElement[] {
    // This method requires React to be imported at the component level
    // The actual implementation is done in the component using this highlighter
    return segments.map(() => null as unknown as React.ReactElement);
  }
}

// Utility function for Monaco Editor tokenization
export function getMonacoTokensProvider(_theme: HighlightTheme = defaultTheme) {
  return {
    tokenizer: {
      root: [
        // Comments
        [/--.*$/, 'comment'],
        [/\/\*/, 'comment', '@comment'],
        
        // Strings
        [/'[^']*'/, 'string'],
        [/"[^"]*"/, 'string'],
        
        // Numbers
        [/\d+(\.\d+)?/, 'number'],
        
        // Keywords
        [/\b(SELECT|FROM|WHERE|INSERT|INTO|VALUES|CREATE|TABLE|DROP|ALTER|UPDATE|DELETE|SET|AND|OR|NOT|NULL|PRIMARY|KEY|FOREIGN|REFERENCES|UNIQUE|DEFAULT|CHECK|IN|BETWEEN|LIKE|EXISTS|JOIN|LEFT|RIGHT|INNER|OUTER|ON|AS|ORDER|BY|GROUP|HAVING|LIMIT|OFFSET|ASC|DESC|DISTINCT|ALL|UNION|EXCEPT|INTERSECT|CASE|WHEN|THEN|ELSE|END|CAST|CONSTRAINT|INDEX|IF|CASCADE|RESTRICT|COLUMN|ADD|MODIFY|AUTO_INCREMENT|AUTOINCREMENT)\b/i, 'keyword'],
        
        // Data types
        [/\b(INT|INTEGER|BIGINT|SMALLINT|TINYINT|DECIMAL|NUMERIC|REAL|FLOAT|DOUBLE|BIT|BOOLEAN|BOOL|DATE|TIME|DATETIME|TIMESTAMP|YEAR|CHAR|VARCHAR|TEXT|TINYTEXT|MEDIUMTEXT|LONGTEXT|BINARY|VARBINARY|BLOB|TINYBLOB|MEDIUMBLOB|LONGBLOB|JSON|UUID|SERIAL)\b/i, 'keyword'],
        
        // Operators
        [/[<>]=?/, 'operator'],
        [/!=|<>/, 'operator'],
        [/[+\-*/]/, 'operator'],
        [/=/, 'operator'],
        
        // Identifiers
        [/[a-zA-Z_]\w*/, 'identifier'],
        
        // Punctuation
        [/[(),;.]/, 'delimiter'],
      ],
      
      comment: [
        [/[^/*]+/, 'comment'],
        [/\*\//, 'comment', '@pop'],
        [/[/*]/, 'comment']
      ]
    }
  };
}