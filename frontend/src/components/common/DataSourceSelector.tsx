import React, { useState, useEffect } from 'react';
import {
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  SelectChangeEvent,
  Box,
  Chip,
  Typography,
  Button,
  IconButton,
  Tooltip,
} from '@mui/material';
import {
  Add as AddIcon,
  Refresh as RefreshIcon,
  Storage as StorageIcon,
} from '@mui/icons-material';
import { dataSourceAPI } from '../../services/dataSourceAPI';
import DataSourceSelector from '../data-sources/DataSourceSelector';

interface DataSourceSelectorProps {
  value: number | null;
  onChange: (dataSourceId: number | null) => void;
  required?: boolean;
  fullWidth?: boolean;
  size?: 'small' | 'medium';
  label?: string;
  helperText?: string;
}

interface DataSource {
  id: number;
  name: string;
  type: string;
  is_active: boolean;
}

const DataSourceSelectorComponent: React.FC<DataSourceSelectorProps> = ({
  value,
  onChange,
  required = true,
  fullWidth = true,
  size = 'medium',
  label = 'Data Source',
  helperText,
}) => {
  const [dataSources, setDataSources] = useState<DataSource[]>([]);
  const [loading, setLoading] = useState(false);
  const [createDialogOpen, setCreateDialogOpen] = useState(false);

  const loadDataSources = async () => {
    setLoading(true);
    try {
      const response = await dataSourceAPI.getDataSources();
      setDataSources(response.data.filter((ds: DataSource) => ds.is_active));
    } catch (error) {
      console.error('Failed to load data sources:', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadDataSources();
  }, []);

  const handleChange = (event: SelectChangeEvent<number>) => {
    const newValue = event.target.value as number;
    onChange(newValue === 0 ? null : newValue);
  };

  const handleDataSourceCreated = () => {
    loadDataSources();
    setCreateDialogOpen(false);
  };

  const getTypeIcon = (type: string) => {
    const typeColors: Record<string, string> = {
      mysql: '#00758F',
      postgresql: '#336791',
      mongodb: '#47A248',
      redis: '#DC382D',
      kafka: '#231F20',
      rabbitmq: '#FF6600',
      elasticsearch: '#005571',
      cassandra: '#1287B6',
      rest_api: '#6BA644',
    };

    return (
      <Box
        sx={{
          width: 8,
          height: 8,
          borderRadius: '50%',
          backgroundColor: typeColors[type] || '#666',
          mr: 1,
        }}
      />
    );
  };

  return (
    <>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <FormControl fullWidth={fullWidth} size={size} required={required}>
          <InputLabel>{label}</InputLabel>
          <Select
            value={value || 0}
            onChange={handleChange}
            label={label}
            disabled={loading}
            startAdornment={<StorageIcon sx={{ mr: 1, color: 'action.active' }} />}
          >
            {!required && (
              <MenuItem value={0}>
                <em>None</em>
              </MenuItem>
            )}
            {dataSources.map((ds) => (
              <MenuItem key={ds.id} value={ds.id}>
                <Box sx={{ display: 'flex', alignItems: 'center', width: '100%' }}>
                  {getTypeIcon(ds.type)}
                  <Typography sx={{ flexGrow: 1 }}>{ds.name}</Typography>
                  <Chip
                    label={ds.type.toUpperCase()}
                    size="small"
                    sx={{ ml: 1, fontSize: '0.7rem' }}
                  />
                </Box>
              </MenuItem>
            ))}
          </Select>
          {helperText && (
            <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5 }}>
              {helperText}
            </Typography>
          )}
        </FormControl>
        
        <Tooltip title="Add New Data Source">
          <IconButton onClick={() => setCreateDialogOpen(true)} color="primary">
            <AddIcon />
          </IconButton>
        </Tooltip>
        
        <Tooltip title="Refresh">
          <IconButton onClick={loadDataSources} disabled={loading}>
            <RefreshIcon />
          </IconButton>
        </Tooltip>
      </Box>

      <DataSourceSelector
        open={createDialogOpen}
        onClose={() => setCreateDialogOpen(false)}
        onDataSourceCreated={handleDataSourceCreated}
      />
    </>
  );
};

export default DataSourceSelectorComponent;