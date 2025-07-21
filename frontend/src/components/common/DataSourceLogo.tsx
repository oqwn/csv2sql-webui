import React from 'react';
import { Box, Avatar } from '@mui/material';
import { 
  Storage as DatabaseIcon,
  Description as FileIcon,
  Cloud as CloudIcon,
  Api as ApiIcon,
  Queue as QueueIcon
} from '@mui/icons-material';

// Data source logo mappings
const DATA_SOURCE_LOGOS = {
  // Relational Databases
  postgresql: {
    icon: '/logos/postgresql.svg',
    fallback: '/logos/postgresql.png',
    color: '#336791',
    name: 'PostgreSQL'
  },
  mysql: {
    icon: '/logos/mysql.svg',
    fallback: '/logos/mysql.png',
    color: '#4479A1',
    name: 'MySQL'
  },
  sqlite: {
    icon: '/logos/sqlite.svg',
    fallback: '/logos/sqlite.png',
    color: '#003B57',
    name: 'SQLite'
  },
  mssql: {
    icon: '/logos/mssql.svg',
    fallback: '/logos/mssql.png',
    color: '#CC2927',
    name: 'SQL Server'
  },
  oracle: {
    icon: '/logos/oracle.svg',
    fallback: '/logos/oracle.png',
    color: '#F80000',
    name: 'Oracle'
  },

  // NoSQL Databases
  mongodb: {
    icon: '/logos/mongodb.svg',
    fallback: '/logos/mongodb.png',
    color: '#47A248',
    name: 'MongoDB'
  },
  redis: {
    icon: '/logos/redis.svg',
    fallback: '/logos/redis.png',
    color: '#DC382D',
    name: 'Redis'
  },
  elasticsearch: {
    icon: '/logos/elasticsearch.svg',
    fallback: '/logos/elasticsearch.png',
    color: '#005571',
    name: 'Elasticsearch'
  },
  cassandra: {
    icon: '/logos/cassandra.svg',
    fallback: '/logos/cassandra.png',
    color: '#1287B1',
    name: 'Cassandra'
  },

  // Cloud Storage
  s3: {
    icon: '/logos/aws-s3.svg',
    fallback: '/logos/aws-s3.png',
    color: '#FF9900',
    name: 'Amazon S3'
  },
  gcs: {
    icon: '/logos/gcp-storage.svg',
    fallback: '/logos/gcp-storage.png',
    color: '#4285F4',
    name: 'Google Cloud Storage'
  },
  azure_blob: {
    icon: '/logos/azure-blob.svg',
    fallback: '/logos/azure-blob.png',
    color: '#0078D4',
    name: 'Azure Blob Storage'
  },

  // File Formats
  json: {
    icon: '/logos/json.svg',
    fallback: '/logos/json.png',
    color: '#000000',
    name: 'JSON'
  },
  parquet: {
    icon: '/logos/parquet.svg',
    fallback: '/logos/parquet.png',
    color: '#50ABF1',
    name: 'Apache Parquet'
  },
  csv: {
    icon: '/logos/csv.svg',
    fallback: '/logos/csv.png',
    color: '#217346',
    name: 'CSV'
  },
  excel: {
    icon: '/logos/excel.svg',
    fallback: '/logos/excel.png',
    color: '#217346',
    name: 'Microsoft Excel'
  },

  // Message Queues
  kafka: {
    icon: '/logos/kafka.svg',
    fallback: '/logos/kafka.png',
    color: '#000000',
    name: 'Apache Kafka'
  },
  rabbitmq: {
    icon: '/logos/rabbitmq.svg',
    fallback: '/logos/rabbitmq.png',
    color: '#FF6600',
    name: 'RabbitMQ'
  },

  // APIs
  rest_api: {
    icon: '/logos/rest-api.svg',
    fallback: '/logos/rest-api.png',
    color: '#61DAFB',
    name: 'REST API'
  },
  graphql: {
    icon: '/logos/graphql.svg',
    fallback: '/logos/graphql.png',
    color: '#E10098',
    name: 'GraphQL'
  },

  // Data Warehouses
  snowflake: {
    icon: '/logos/snowflake.svg',
    fallback: '/logos/snowflake.png',
    color: '#29B5E8',
    name: 'Snowflake'
  },
  bigquery: {
    icon: '/logos/bigquery.svg',
    fallback: '/logos/bigquery.png',
    color: '#4285F4',
    name: 'Google BigQuery'
  },
  redshift: {
    icon: '/logos/redshift.svg',
    fallback: '/logos/redshift.png',
    color: '#8C4FFF',
    name: 'Amazon Redshift'
  }
};

// Category fallback icons
const CATEGORY_ICONS = {
  relational: DatabaseIcon,
  nosql: DatabaseIcon,
  file: FileIcon,
  cloud: CloudIcon,
  api: ApiIcon,
  message_queue: QueueIcon,
  data_warehouse: CloudIcon
};

interface DataSourceLogoProps {
  type: string;
  size?: number | string;
  showName?: boolean;
  variant?: 'square' | 'circular';
  category?: string;
}

const DataSourceLogo: React.FC<DataSourceLogoProps> = ({ 
  type, 
  size = 40, 
  showName = false, 
  variant = 'square',
  category = 'relational' 
}) => {
  const logoConfig = DATA_SOURCE_LOGOS[type as keyof typeof DATA_SOURCE_LOGOS];
  const [imageError, setImageError] = React.useState(false);
  const [fallbackError, setFallbackError] = React.useState(false);

  const handleImageError = () => {
    if (!imageError) {
      setImageError(true);
    } else if (!fallbackError) {
      setFallbackError(true);
    }
  };

  const renderLogo = () => {
    if (!logoConfig || (imageError && fallbackError)) {
      // Use category-based Material-UI icon
      const CategoryIcon = CATEGORY_ICONS[category as keyof typeof CATEGORY_ICONS] || DatabaseIcon;
      
      return (
        <Avatar
          variant={variant}
          sx={{
            width: size,
            height: size,
            backgroundColor: logoConfig?.color || '#666666',
            color: 'white'
          }}
        >
          <CategoryIcon sx={{ fontSize: typeof size === 'number' ? size * 0.6 : size }} />
        </Avatar>
      );
    }

    // Try to load the logo image
    const imageSrc = imageError ? logoConfig.fallback : logoConfig.icon;

    return (
      <Avatar
        variant={variant}
        src={imageSrc}
        onError={handleImageError}
        sx={{
          width: size,
          height: size,
          backgroundColor: 'transparent',
          border: '1px solid #e0e0e0'
        }}
        alt={logoConfig.name}
      >
        {logoConfig.name.charAt(0)}
      </Avatar>
    );
  };

  if (showName) {
    return (
      <Box display="flex" alignItems="center" gap={1}>
        {renderLogo()}
        <Box>
          <Box fontWeight="medium" fontSize="0.875rem">
            {logoConfig?.name || type.toUpperCase()}
          </Box>
        </Box>
      </Box>
    );
  }

  return renderLogo();
};

export default DataSourceLogo;

// Export the logo configuration for use in other components
export { DATA_SOURCE_LOGOS };