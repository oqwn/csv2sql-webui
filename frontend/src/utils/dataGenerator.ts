// Realistic data sets for different column types
const FIRST_NAMES = [
  'James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda',
  'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica',
  'Thomas', 'Sarah', 'Charles', 'Karen'
];

const LAST_NAMES = [
  'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
  'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas',
  'Taylor', 'Moore', 'Jackson', 'Martin'
];

const USERNAMES = [
  'cooluser123', 'techguru', 'datawizard', 'codemaster', 'webdev2024', 'sqlpro', 'devops_ninja', 'fullstack_dev',
  'backend_expert', 'frontend_ace', 'cloud_architect', 'data_scientist', 'ml_engineer', 'system_admin', 'net_admin',
  'security_expert', 'crypto_trader', 'game_developer', 'mobile_dev', 'ai_researcher'
];

const CITIES = [
  'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego',
  'Dallas', 'San Jose', 'Austin', 'Jacksonville', 'Fort Worth', 'Columbus', 'San Francisco', 'Charlotte',
  'Indianapolis', 'Seattle', 'Denver', 'Washington'
];

const STREETS = [
  'Main St', 'First Ave', 'Oak St', 'Elm St', 'Market St', 'Broadway', 'Park Ave', 'Second St',
  'Washington St', 'Maple Ave', 'Cedar St', 'Third Ave', 'Pine St', 'Lincoln Ave', 'Lake St', 'Hill St',
  'Madison Ave', 'Church St', 'Spring St', 'North St'
];

const IP_ADDRESSES = [
  '192.168.1.1', '10.0.0.1', '172.16.0.1', '192.168.0.100', '10.10.10.10', '172.31.255.255', '192.168.100.1', '10.0.1.1',
  '172.20.10.1', '192.168.2.100', '10.1.1.1', '172.16.1.100', '192.168.50.1', '10.0.0.100', '172.30.1.1', '192.168.1.100',
  '10.20.30.40', '172.16.100.1', '192.168.200.1', '10.100.100.100'
];

const TITLES = [
  'Getting Started Guide', 'Advanced Tutorial', 'Best Practices', 'Quick Reference', 'User Manual', 'API Documentation', 'Security Guidelines', 'Performance Tips',
  'Troubleshooting Guide', 'Installation Instructions', 'Configuration Guide', 'Developer Handbook', 'Admin Guide', 'Release Notes', 'Feature Overview', 'System Requirements',
  'Migration Guide', 'Integration Tutorial', 'Deployment Guide', 'Maintenance Manual'
];

const DESCRIPTIONS = [
  'A comprehensive overview of the system', 'Detailed explanation of core features', 'Step-by-step instructions included', 'Essential information for beginners', 
  'Advanced techniques and strategies', 'Optimized for maximum performance', 'Industry best practices applied', 'Fully tested and verified',
  'Regular updates and improvements', 'User-friendly interface design', 'Scalable architecture implementation', 'Security-first approach',
  'Cloud-native solution', 'Mobile-responsive design', 'Real-time data processing', 'AI-powered capabilities',
  'Automated workflow management', 'Enterprise-grade reliability', 'Cost-effective solution', 'Seamless integration support'
];

const TAGS = [
  'important', 'urgent', 'pending', 'completed', 'review', 'approved', 'draft', 'final',
  'todo', 'in-progress', 'testing', 'production', 'development', 'staging', 'archived', 'active',
  'deprecated', 'beta', 'stable', 'experimental'
];

const TOPICS = [
  'Technology', 'Business', 'Science', 'Education', 'Healthcare', 'Finance', 'Marketing', 'Design',
  'Engineering', 'Research', 'Analytics', 'Security', 'Cloud', 'Mobile', 'Web', 'Database',
  'AI/ML', 'DevOps', 'Blockchain', 'IoT'
];

const GENERIC_STRINGS = [
  'Lorem ipsum', 'Sample text', 'Test data', 'Example content', 'Demo value', 'Placeholder text', 'Generic string', 'Default value',
  'Sample entry', 'Test record', 'Data point', 'Information', 'Content here', 'Text value', 'String data', 'Entry value',
  'Record data', 'Item detail', 'Field value', 'Data entry'
];

const EMAILS = [
  'user@example.com', 'admin@company.com', 'test@test.com', 'info@business.com', 'contact@website.com',
  'support@service.com', 'hello@startup.com', 'team@project.com', 'dev@tech.com', 'sales@corp.com',
  'hr@organization.com', 'marketing@brand.com', 'ceo@enterprise.com', 'cto@innovation.com', 'customer@client.com',
  'partner@alliance.com', 'investor@capital.com', 'press@media.com', 'legal@law.com', 'finance@accounting.com'
];

// Helper functions
function randomElement<T>(array: T[]): T {
  return array[Math.floor(Math.random() * array.length)];
}

function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomFloat(min: number, max: number, decimals: number = 2): number {
  const value = Math.random() * (max - min) + min;
  return parseFloat(value.toFixed(decimals));
}

function generateName(): string {
  return `${randomElement(FIRST_NAMES)} ${randomElement(LAST_NAMES)}`;
}

function generateAddress(): string {
  return `${randomInt(1, 9999)} ${randomElement(STREETS)}, ${randomElement(CITIES)}`;
}

function generateDate(startYear: number = 2020, endYear: number = 2024): string {
  const start = new Date(startYear, 0, 1);
  const end = new Date(endYear, 11, 31);
  const date = new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
  return date.toISOString().split('T')[0];
}

function generateTimestamp(): string {
  const date = new Date(Date.now() - Math.random() * 365 * 24 * 60 * 60 * 1000); // Random date within last year
  return date.toISOString().replace('T', ' ').slice(0, 19);
}

// Main function to generate value based on column name and type
export function generateValue(columnName: string, columnType: string): string {
  const colLower = columnName.toLowerCase();
  const typeLower = columnType.toLowerCase();
  
  // Handle specific column names
  if (colLower.includes('email')) {
    return randomElement(EMAILS);
  }
  
  if (colLower.includes('username') || colLower.includes('user_name')) {
    return randomElement(USERNAMES);
  }
  
  if (colLower.includes('name') || colLower.includes('fullname') || colLower.includes('full_name')) {
    return generateName();
  }
  
  if (colLower.includes('first_name') || colLower.includes('firstname')) {
    return randomElement(FIRST_NAMES);
  }
  
  if (colLower.includes('last_name') || colLower.includes('lastname')) {
    return randomElement(LAST_NAMES);
  }
  
  if (colLower.includes('address') || colLower.includes('location')) {
    return generateAddress();
  }
  
  if (colLower.includes('city')) {
    return randomElement(CITIES);
  }
  
  if (colLower.includes('ip_address') || colLower.includes('ip')) {
    return randomElement(IP_ADDRESSES);
  }
  
  if (colLower.includes('title')) {
    return randomElement(TITLES);
  }
  
  if (colLower.includes('description') || colLower.includes('desc')) {
    return randomElement(DESCRIPTIONS);
  }
  
  if (colLower.includes('tag') || colLower.includes('label')) {
    return randomElement(TAGS);
  }
  
  if (colLower.includes('topic') || colLower.includes('category')) {
    return randomElement(TOPICS);
  }
  
  if (colLower.includes('status')) {
    return randomElement(['active', 'inactive', 'pending', 'completed', 'cancelled']);
  }
  
  if (colLower.includes('phone')) {
    return `+1-${randomInt(200, 999)}-${randomInt(100, 999)}-${randomInt(1000, 9999)}`;
  }
  
  if (colLower.includes('price') || colLower.includes('cost') || colLower.includes('amount')) {
    return randomFloat(10, 1000, 2).toString();
  }
  
  if (colLower.includes('quantity') || colLower.includes('qty') || colLower.includes('count')) {
    return randomInt(1, 100).toString();
  }
  
  if (colLower.includes('age')) {
    return randomInt(18, 80).toString();
  }
  
  if (colLower.includes('year')) {
    return randomInt(2000, 2024).toString();
  }
  
  // Handle by data type
  if (typeLower.includes('int') || typeLower.includes('serial')) {
    return randomInt(1, 10000).toString();
  }
  
  if (typeLower.includes('decimal') || typeLower.includes('numeric') || typeLower.includes('float') || typeLower.includes('real') || typeLower.includes('double')) {
    return randomFloat(0, 1000, 2).toString();
  }
  
  if (typeLower.includes('bool')) {
    return Math.random() > 0.5 ? 'true' : 'false';
  }
  
  if (typeLower.includes('date') && !typeLower.includes('timestamp')) {
    return generateDate();
  }
  
  if (typeLower.includes('timestamp') || typeLower.includes('datetime')) {
    return generateTimestamp();
  }
  
  if (typeLower.includes('time') && !typeLower.includes('timestamp')) {
    return `${randomInt(0, 23).toString().padStart(2, '0')}:${randomInt(0, 59).toString().padStart(2, '0')}:${randomInt(0, 59).toString().padStart(2, '0')}`;
  }
  
  if (typeLower.includes('uuid')) {
    return 'gen_random_uuid()'; // PostgreSQL function
  }
  
  if (typeLower.includes('json')) {
    return JSON.stringify({ id: randomInt(1, 100), value: randomElement(GENERIC_STRINGS) });
  }
  
  // Default for string types
  if (typeLower.includes('varchar') || typeLower.includes('text') || typeLower.includes('char')) {
    return randomElement(GENERIC_STRINGS);
  }
  
  // Fallback
  return randomElement(GENERIC_STRINGS);
}

// Generate multiple INSERT statements
export function generateBulkInsertSQL(
  tableName: string,
  columns: Array<{ name: string; type: string; nullable: boolean; default: string | null }>,
  rowCount: number,
  includeNullableColumns: boolean = false
): string {
  // Filter columns - exclude auto-generated and optionally nullable columns
  const columnsToInsert = columns.filter(col => {
    const isAutoGenerated = col.default?.includes('nextval') || col.default?.includes('sequence');
    if (isAutoGenerated) return false;
    if (!includeNullableColumns && col.nullable && !col.default) return false;
    return true;
  });
  
  if (columnsToInsert.length === 0) {
    return '-- No columns to insert';
  }
  
  const columnNames = columnsToInsert.map(col => col.name).join(', ');
  const values: string[] = [];
  
  for (let i = 0; i < rowCount; i++) {
    const rowValues = columnsToInsert.map(col => {
      const value = generateValue(col.name, col.type);
      
      // Handle special cases
      if (value === 'gen_random_uuid()') {
        return value; // Don't quote PostgreSQL functions
      }
      
      // Check if it's a number or boolean
      if (col.type.toLowerCase().includes('int') || 
          col.type.toLowerCase().includes('decimal') || 
          col.type.toLowerCase().includes('numeric') || 
          col.type.toLowerCase().includes('float') || 
          col.type.toLowerCase().includes('real') || 
          col.type.toLowerCase().includes('double') ||
          col.type.toLowerCase().includes('serial') ||
          value === 'true' || value === 'false') {
        return value;
      }
      
      // Quote string values
      return `'${value.replace(/'/g, "''")}'`;
    });
    
    values.push(`(${rowValues.join(', ')})`);
  }
  
  // PostgreSQL supports multi-row INSERT
  return `INSERT INTO ${tableName} (${columnNames})\nVALUES\n${values.join(',\n')};`;
}