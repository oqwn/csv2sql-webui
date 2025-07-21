import React from 'react';
import {
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Box,
  Typography,
  Chip,
  Button,
  Alert,
  AlertTitle,
  IconButton,
  Tooltip
} from '@mui/material';
import { 
  Add as AddIcon, 
  Storage as StorageIcon,
  Refresh as RefreshIcon
} from '@mui/icons-material';
import { useNavigate } from 'react-router-dom';
import { useDataSource } from '../../contexts/DataSourceContext';

interface DataSourceSelectorProps {
  required?: boolean;
  size?: 'small' | 'medium';
  variant?: 'standard' | 'outlined' | 'filled';
  showAddButton?: boolean;
  showRequiredMessage?: boolean;
  fullWidth?: boolean;
  label?: string;
  helperText?: string;
}

const DataSourceSelector: React.FC<DataSourceSelectorProps> = ({
  required = true,
  size = 'medium',
  variant = 'outlined',
  showAddButton = true,
  showRequiredMessage = true,
  fullWidth = true,
  label = 'Data Source',
  helperText
}) => {
  const navigate = useNavigate();
  const { 
    selectedDataSource, 
    setSelectedDataSource, 
    dataSources, 
    loading,
    isConnected,
    fetchDataSources
  } = useDataSource();

  const handleChange = (event: any) => {
    const dataSourceId = event.target.value;
    const dataSource = dataSources.find(ds => ds.id === dataSourceId);
    setSelectedDataSource(dataSource || null);
  };

  const handleAddDataSource = () => {
    navigate('/data-sources');
  };

  const getTypeColor = (type: string) => {
    const typeColors: Record<string, string> = {
      mysql: '#00758F',
      postgresql: '#336791',
      mongodb: '#47A248',
      redis: '#DC382D',
      kafka: '#231F20',
      rabbitmq: '#FF6600',
      elasticsearch: '#005571',
      rest_api: '#6BA644',
    };
    return typeColors[type] || '#666';
  };

  // Show warning if no data sources exist AND it's required
  if (!loading && dataSources.length === 0 && required && showRequiredMessage) {
    return (
      <Alert severity="warning" sx={{ mb: 2 }}>
        <AlertTitle>No Data Sources Found</AlertTitle>
        <Typography variant="body2" sx={{ mb: 2 }}>
          You need to connect to a data source before you can use SQL operations, table management, or data import features.
        </Typography>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={handleAddDataSource}
          size="small"
        >
          Add Data Source
        </Button>
      </Alert>
    );
  }

  // Show connection required message for operations that need it
  if (required && showRequiredMessage && !isConnected && dataSources.length > 0) {
    return (
      <Alert severity="info" sx={{ mb: 2 }}>
        <AlertTitle>Data Source Required</AlertTitle>
        <Typography variant="body2" sx={{ mb: 2 }}>
          Please select a data source to continue with this operation.
        </Typography>
        <DataSourceSelector 
          required={false} 
          showRequiredMessage={false}
          showAddButton={showAddButton}
        />
      </Alert>
    );
  }

  return (
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: required && !isConnected ? 2 : 0 }}>
      <FormControl 
        variant={variant} 
        size={size} 
        fullWidth={fullWidth}
        error={required && !selectedDataSource}
      >
        <InputLabel>{label}</InputLabel>
        <Select
          value={selectedDataSource?.id || ''}
          label={label}
          onChange={handleChange}
          disabled={loading}
          startAdornment={<StorageIcon sx={{ mr: 1, color: 'action.active' }} />}
        >
          {dataSources.length === 0 ? (
            <MenuItem disabled>
              <Typography variant="body2" color="text.secondary">
                No data sources configured
              </Typography>
            </MenuItem>
          ) : (
            dataSources.map((dataSource) => (
              <MenuItem key={dataSource.id} value={dataSource.id}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                  <Box
                    sx={{
                      width: 8,
                      height: 8,
                      borderRadius: '50%',
                      backgroundColor: getTypeColor(dataSource.type),
                    }}
                  />
                  <Typography variant="body2" sx={{ fontWeight: 'medium', flexGrow: 1 }}>
                    {dataSource.name}
                  </Typography>
                  <Chip 
                    label={dataSource.type.toUpperCase()} 
                    size="small" 
                    color="primary" 
                    variant="outlined"
                    sx={{ height: 20 }}
                  />
                </Box>
              </MenuItem>
            ))
          )}
        </Select>
        {helperText && (
          <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5 }}>
            {helperText}
          </Typography>
        )}
      </FormControl>
      
      {showAddButton && (
        <Tooltip title="Add New Data Source">
          <IconButton
            onClick={handleAddDataSource}
            color="primary"
            size={size}
          >
            <AddIcon />
          </IconButton>
        </Tooltip>
      )}
      
      <Tooltip title="Refresh Data Sources">
        <IconButton
          onClick={fetchDataSources}
          disabled={loading}
          size={size}
        >
          <RefreshIcon />
        </IconButton>
      </Tooltip>
    </Box>
  );
};

export default DataSourceSelector;