import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Grid,
  Card,
  CardContent,
  Typography,
  Box,
  Chip,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Alert,
  CircularProgress
} from '@mui/material';
import { dataSourceAPI, SupportedDataSource, ConnectionTestRequest } from '../../services/dataSourceAPI';

interface Props {
  open: boolean;
  onClose: () => void;
  onDataSourceCreated: (dataSource: any) => void;
}

const DataSourceSelector: React.FC<Props> = ({ open, onClose, onDataSourceCreated }) => {
  const [supportedSources, setSupportedSources] = useState<SupportedDataSource[]>([]);
  const [selectedSource, setSelectedSource] = useState<SupportedDataSource | null>(null);
  const [connectionConfig, setConnectionConfig] = useState<Record<string, any>>({});
  const [loading, setLoading] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<any>(null);
  const [error, setError] = useState<string>('');

  useEffect(() => {
    if (open) {
      loadSupportedSources();
    }
  }, [open]);

  const loadSupportedSources = async () => {
    try {
      const response = await dataSourceAPI.getSupportedDataSources();
      setSupportedSources(response.data);
    } catch (err: any) {
      setError('Failed to load supported data sources');
    }
  };

  const handleSourceSelect = (source: SupportedDataSource) => {
    setSelectedSource(source);
    setConnectionConfig(getDefaultConfig(source.type));
    setTestResult(null);
    setError('');
  };

  const getDefaultConfig = (sourceType: string): Record<string, any> => {
    const defaults: Record<string, Record<string, any>> = {
      mysql: {
        host: 'localhost',
        port: 3306,
        username: '',
        password: '',
        database: ''
      },
      postgresql: {
        host: 'localhost',
        port: 5432,
        username: '',
        password: '',
        database: ''
      },
      mongodb: {
        host: 'localhost',
        port: 27017,
        username: '',
        password: '',
        database: '',
        auth_source: 'admin'
      },
      redis: {
        host: 'localhost',
        port: 6379,
        password: '',
        database: 0
      },
      kafka: {
        bootstrap_servers: 'localhost:9092',
        security_protocol: 'PLAINTEXT',
        sasl_mechanism: 'PLAIN',
        sasl_username: '',
        sasl_password: '',
        consumer_group: 'csv2sql_consumer',
        auto_offset_reset: 'earliest'
      },
      rabbitmq: {
        host: 'localhost',
        port: 5672,
        username: 'guest',
        password: 'guest',
        virtual_host: '/',
        exchange: '',
        queue_prefix: 'csv2sql'
      },
      elasticsearch: {
        host: 'localhost',
        port: 9200,
        username: '',
        password: '',
        use_ssl: false,
        verify_certs: true,
        api_key: ''
      },
      cassandra: {
        host: 'localhost',
        port: 9042,
        username: '',
        password: '',
        keyspace: '',
        datacenter: '',
        protocol_version: 4
      },
      rest_api: {
        base_url: '',
        auth_type: 'none',
        api_key: '',
        token: ''
      }
    };
    
    return defaults[sourceType] || {};
  };

  const handleConfigChange = (field: string, value: any) => {
    setConnectionConfig(prev => ({
      ...prev,
      [field]: value
    }));
    setTestResult(null);
  };

  const testConnection = async () => {
    if (!selectedSource) return;
    
    setTesting(true);
    setError('');
    
    try {
      const request: ConnectionTestRequest = {
        type: selectedSource.type,
        connection_config: connectionConfig
      };
      
      const response = await dataSourceAPI.testConnection(request);
      setTestResult(response.data);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Connection test failed');
      setTestResult(null);
    } finally {
      setTesting(false);
    }
  };

  const createDataSource = async () => {
    if (!selectedSource || !testResult || testResult.status !== 'success') {
      setError('Please test the connection successfully before creating');
      return;
    }
    
    setLoading(true);
    setError('');
    
    try {
      const dataSource = {
        name: `${selectedSource.name} - ${connectionConfig.host || connectionConfig.base_url || 'Connection'}`,
        type: selectedSource.type,
        connection_config: connectionConfig,
        description: `${selectedSource.description} connection`,
        is_active: true
      };
      
      const response = await dataSourceAPI.createDataSource(dataSource);
      onDataSourceCreated(response.data);
      handleClose();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to create data source');
    } finally {
      setLoading(false);
    }
  };

  const handleClose = () => {
    setSelectedSource(null);
    setConnectionConfig({});
    setTestResult(null);
    setError('');
    onClose();
  };

  const renderConnectionForm = () => {
    if (!selectedSource) return null;

    const fields = getConfigFields(selectedSource.type);
    
    return (
      <Box sx={{ mt: 2 }}>
        <Typography variant="h6" gutterBottom>
          Connection Configuration
        </Typography>
        
        <Grid container spacing={2}>
          {fields.map((field) => (
            <Grid item xs={12} sm={6} key={field.name}>
              {field.type === 'select' ? (
                <FormControl fullWidth>
                  <InputLabel>{field.label}</InputLabel>
                  <Select
                    value={connectionConfig[field.name] || ''}
                    onChange={(e) => handleConfigChange(field.name, e.target.value)}
                    label={field.label}
                  >
                    {field.options?.map((option: any) => (
                      <MenuItem key={option.value} value={option.value}>
                        {option.label}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              ) : (
                <TextField
                  fullWidth
                  label={field.label}
                  type={field.type}
                  value={connectionConfig[field.name] || ''}
                  onChange={(e) => handleConfigChange(field.name, e.target.value)}
                  required={field.required}
                  helperText={field.helperText}
                />
              )}
            </Grid>
          ))}
        </Grid>
        
        <Box sx={{ mt: 2, display: 'flex', gap: 2, alignItems: 'center' }}>
          <Button
            variant="outlined"
            onClick={testConnection}
            disabled={testing || !isConfigValid()}
          >
            {testing ? <CircularProgress size={20} /> : 'Test Connection'}
          </Button>
          
          {testResult && (
            <Chip
              label={testResult.status === 'success' ? 'Connection Successful' : 'Connection Failed'}
              color={testResult.status === 'success' ? 'success' : 'error'}
              variant="outlined"
            />
          )}
        </Box>
        
        {testResult && testResult.status === 'success' && (
          <Alert severity="success" sx={{ mt: 2 }}>
            <Typography variant="body2">
              Successfully connected to {testResult.database_type || selectedSource.name}
              {testResult.version && ` (Version: ${testResult.version})`}
              {testResult.table_count !== undefined && ` - ${testResult.table_count} tables found`}
              {testResult.collection_count !== undefined && ` - ${testResult.collection_count} collections found`}
              {testResult.database_count !== undefined && ` - ${testResult.database_count} databases found`}
            </Typography>
            
            {testResult.available_databases && testResult.available_databases.length > 0 && (
              <Box sx={{ mt: 2 }}>
                <Typography variant="subtitle2" gutterBottom>
                  Available Databases:
                </Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                  {testResult.available_databases.map((db: string) => (
                    <Chip
                      key={db}
                      label={db}
                      variant="outlined"
                      size="small"
                      onClick={() => handleConfigChange('database', db)}
                      sx={{ cursor: 'pointer' }}
                    />
                  ))}
                </Box>
                <Typography variant="caption" sx={{ mt: 1, display: 'block' }}>
                  Click on a database to select it
                </Typography>
              </Box>
            )}
          </Alert>
        )}
        
        {error && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {error}
          </Alert>
        )}
      </Box>
    );
  };

  const getConfigFields = (sourceType: string) => {
    const fieldConfigs: Record<string, any[]> = {
      mysql: [
        { name: 'host', label: 'Host', type: 'text', required: true },
        { name: 'port', label: 'Port', type: 'number', required: true },
        { name: 'username', label: 'Username', type: 'text', required: true },
        { name: 'password', label: 'Password', type: 'password', required: false, helperText: 'Optional' },
        { name: 'database', label: 'Database', type: 'text', required: false, helperText: 'Optional - Leave empty to list available databases' }
      ],
      postgresql: [
        { name: 'host', label: 'Host', type: 'text', required: true },
        { name: 'port', label: 'Port', type: 'number', required: true },
        { name: 'username', label: 'Username', type: 'text', required: true },
        { name: 'password', label: 'Password', type: 'password', required: false, helperText: 'Optional' },
        { name: 'database', label: 'Database', type: 'text', required: false, helperText: 'Optional - Leave empty to list available databases' }
      ],
      mongodb: [
        { name: 'host', label: 'Host', type: 'text', required: true },
        { name: 'port', label: 'Port', type: 'number', required: true },
        { name: 'username', label: 'Username', type: 'text', required: false },
        { name: 'password', label: 'Password', type: 'password', required: false },
        { name: 'database', label: 'Database', type: 'text', required: false, helperText: 'Optional - Leave empty to list available databases' },
        { name: 'auth_source', label: 'Auth Source', type: 'text', required: false, helperText: 'Default: admin' }
      ],
      redis: [
        { name: 'host', label: 'Host', type: 'text', required: true },
        { name: 'port', label: 'Port', type: 'number', required: true },
        { name: 'password', label: 'Password', type: 'password', required: false },
        { name: 'database', label: 'Database Number', type: 'number', required: false, helperText: 'Default: 0' }
      ],
      kafka: [
        { name: 'bootstrap_servers', label: 'Bootstrap Servers', type: 'text', required: true, helperText: 'Comma-separated list (e.g., localhost:9092)' },
        { 
          name: 'security_protocol', 
          label: 'Security Protocol', 
          type: 'select', 
          required: true,
          options: [
            { value: 'PLAINTEXT', label: 'PLAINTEXT' },
            { value: 'SASL_PLAINTEXT', label: 'SASL_PLAINTEXT' },
            { value: 'SASL_SSL', label: 'SASL_SSL' },
            { value: 'SSL', label: 'SSL' }
          ]
        },
        { name: 'sasl_mechanism', label: 'SASL Mechanism', type: 'text', required: false, helperText: 'e.g., PLAIN, SCRAM-SHA-256' },
        { name: 'sasl_username', label: 'SASL Username', type: 'text', required: false },
        { name: 'sasl_password', label: 'SASL Password', type: 'password', required: false },
        { name: 'consumer_group', label: 'Consumer Group', type: 'text', required: false, helperText: 'Default: csv2sql_consumer' },
        { 
          name: 'auto_offset_reset', 
          label: 'Auto Offset Reset', 
          type: 'select', 
          required: false,
          options: [
            { value: 'earliest', label: 'Earliest' },
            { value: 'latest', label: 'Latest' }
          ]
        }
      ],
      rabbitmq: [
        { name: 'host', label: 'Host', type: 'text', required: true },
        { name: 'port', label: 'Port', type: 'number', required: true },
        { name: 'username', label: 'Username', type: 'text', required: false, helperText: 'Default: guest' },
        { name: 'password', label: 'Password', type: 'password', required: false, helperText: 'Default: guest' },
        { name: 'virtual_host', label: 'Virtual Host', type: 'text', required: false, helperText: 'Default: /' },
        { name: 'exchange', label: 'Exchange', type: 'text', required: false },
        { name: 'queue_prefix', label: 'Queue Prefix', type: 'text', required: false, helperText: 'Default: csv2sql' }
      ],
      elasticsearch: [
        { name: 'host', label: 'Host', type: 'text', required: true },
        { name: 'port', label: 'Port', type: 'number', required: true },
        { name: 'username', label: 'Username', type: 'text', required: false },
        { name: 'password', label: 'Password', type: 'password', required: false },
        { name: 'use_ssl', label: 'Use SSL', type: 'checkbox', required: false },
        { name: 'verify_certs', label: 'Verify Certificates', type: 'checkbox', required: false },
        { name: 'api_key', label: 'API Key', type: 'password', required: false }
      ],
      cassandra: [
        { name: 'host', label: 'Host', type: 'text', required: true },
        { name: 'port', label: 'Port', type: 'number', required: true },
        { name: 'username', label: 'Username', type: 'text', required: false },
        { name: 'password', label: 'Password', type: 'password', required: false },
        { name: 'keyspace', label: 'Keyspace', type: 'text', required: false },
        { name: 'datacenter', label: 'Datacenter', type: 'text', required: false },
        { name: 'protocol_version', label: 'Protocol Version', type: 'number', required: false, helperText: 'Default: 4' }
      ],
      rest_api: [
        { name: 'base_url', label: 'Base URL', type: 'url', required: true },
        { 
          name: 'auth_type', 
          label: 'Authentication Type', 
          type: 'select', 
          required: true,
          options: [
            { value: 'none', label: 'None' },
            { value: 'bearer', label: 'Bearer Token' },
            { value: 'api_key', label: 'API Key' }
          ]
        },
        { name: 'token', label: 'Bearer Token', type: 'password', required: false },
        { name: 'api_key', label: 'API Key', type: 'password', required: false },
        { name: 'api_key_header', label: 'API Key Header', type: 'text', required: false, helperText: 'Default: X-API-Key' }
      ]
    };
    
    return fieldConfigs[sourceType] || [];
  };

  const isConfigValid = () => {
    if (!selectedSource) return false;
    
    const fields = getConfigFields(selectedSource.type);
    const requiredFields = fields.filter(f => f.required);
    
    return requiredFields.every(field => 
      connectionConfig[field.name] && connectionConfig[field.name].toString().trim()
    );
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="md" fullWidth>
      <DialogTitle>Add Data Source</DialogTitle>
      <DialogContent>
        {!selectedSource ? (
          <Box>
            <Typography variant="body1" gutterBottom>
              Select a data source type to connect to:
            </Typography>
            
            <Grid container spacing={2} sx={{ mt: 1 }}>
              {supportedSources.map((source) => (
                <Grid item xs={12} sm={6} md={4} key={source.type}>
                  <Card 
                    sx={{ 
                      cursor: 'pointer',
                      '&:hover': { elevation: 4 }
                    }}
                    onClick={() => handleSourceSelect(source)}
                  >
                    <CardContent>
                      <Typography variant="h6" gutterBottom>
                        {source.name}
                      </Typography>
                      <Typography variant="body2" color="text.secondary" gutterBottom>
                        {source.description}
                      </Typography>
                      <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', mt: 1 }}>
                        <Chip
                          label={source.category}
                          size="small"
                          color="primary"
                          variant="outlined"
                        />
                        {source.supports_incremental && (
                          <Chip
                            label="Incremental"
                            size="small"
                            color="secondary"
                            variant="outlined"
                          />
                        )}
                        {source.supports_real_time && (
                          <Chip
                            label="Real-time"
                            size="small"
                            color="success"
                            variant="outlined"
                          />
                        )}
                      </Box>
                    </CardContent>
                  </Card>
                </Grid>
              ))}
            </Grid>
          </Box>
        ) : (
          <Box>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 2 }}>
              <Button
                variant="outlined"
                size="small"
                onClick={() => setSelectedSource(null)}
              >
                ← Back
              </Button>
              <Typography variant="h6">
                Configure {selectedSource.name}
              </Typography>
            </Box>
            
            {renderConnectionForm()}
          </Box>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose}>Cancel</Button>
        {selectedSource && (
          <Button
            variant="contained"
            onClick={createDataSource}
            disabled={loading || !testResult || testResult.status !== 'success'}
          >
            {loading ? <CircularProgress size={20} /> : 'Create Data Source'}
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
};

export default DataSourceSelector;