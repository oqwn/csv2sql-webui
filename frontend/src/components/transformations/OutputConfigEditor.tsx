import React, { useState, useEffect } from 'react';
import {
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField,
  Box,
  Grid,
  RadioGroup,
  FormControlLabel,
  Radio,
  Alert,
  Chip,
  FormHelperText,
} from '@mui/material';
import { dataSourceService } from '../../services/api';

interface Props {
  value: any;
  onChange: (value: any) => void;
  sourceDataSourceId?: number;
}

const OutputConfigEditor: React.FC<Props> = ({ value, onChange, sourceDataSourceId }) => {
  const [outputType, setOutputType] = useState(value?.type || 'table');
  const [dataSources, setDataSources] = useState<any[]>([]);
  const [targetDataSource, setTargetDataSource] = useState(
    value?.datasource_id || sourceDataSourceId || ''
  );
  const [tableName, setTableName] = useState(value?.table_name || '');
  const [ifExists, setIfExists] = useState(value?.if_exists || 'replace');
  const [primaryKeyColumns, setPrimaryKeyColumns] = useState<string[]>(value?.primary_key_columns || []);
  const [exportFormat, setExportFormat] = useState(value?.format || 'csv');
  const [filename, setFilename] = useState(value?.filename || '');

  useEffect(() => {
    loadDataSources();
  }, []);

  useEffect(() => {
    updateConfig();
  }, [outputType, targetDataSource, tableName, ifExists, primaryKeyColumns, exportFormat, filename]);

  const loadDataSources = async () => {
    try {
      const response = await dataSourceService.listDataSources();
      setDataSources(response);
    } catch (error) {
      console.error('Failed to load data sources:', error);
    }
  };

  const updateConfig = () => {
    if (outputType === 'table') {
      onChange({
        type: 'table',
        datasource_id: targetDataSource,
        table_name: tableName,
        if_exists: ifExists,
        ...((['upsert', 'merge'].includes(ifExists) && primaryKeyColumns.length > 0) && { primary_key_columns: primaryKeyColumns }),
      });
    } else if (outputType === 'export') {
      onChange({
        type: 'export',
        format: exportFormat,
        filename: filename,
      });
    }
  };

  return (
    <Box>
      <RadioGroup
        row
        value={outputType}
        onChange={(e) => setOutputType(e.target.value)}
        sx={{ mb: 3 }}
      >
        <FormControlLabel value="table" control={<Radio />} label="Save to Table" />
        <FormControlLabel value="export" control={<Radio />} label="Export to File" />
      </RadioGroup>

      {outputType === 'table' ? (
        <Grid container spacing={2}>
          <Grid item xs={12}>
            <FormControl fullWidth>
              <InputLabel>Target Data Source</InputLabel>
              <Select
                value={targetDataSource}
                onChange={(e) => setTargetDataSource(e.target.value)}
                label="Target Data Source"
              >
                {dataSources.map((ds) => (
                  <MenuItem key={ds.id} value={ds.id}>
                    {ds.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={12}>
            <TextField
              fullWidth
              label="Table Name"
              value={tableName}
              onChange={(e) => setTableName(e.target.value)}
              helperText="Name of the table to create or update"
              required
            />
          </Grid>
          <Grid item xs={12}>
            <FormControl fullWidth>
              <InputLabel>If Table Exists</InputLabel>
              <Select
                value={ifExists}
                onChange={(e) => setIfExists(e.target.value)}
                label="If Table Exists"
              >
                <MenuItem value="replace">Replace (Drop and recreate)</MenuItem>
                <MenuItem value="append">Append (Add new rows)</MenuItem>
                <MenuItem value="upsert">Upsert (Update or insert)</MenuItem>
                <MenuItem value="merge">Merge (Advanced update logic)</MenuItem>
                <MenuItem value="fail">Fail (Show error)</MenuItem>
              </Select>
            </FormControl>
          </Grid>
          {(['upsert', 'merge'].includes(ifExists)) && (
            <Grid item xs={12}>
              <FormControl fullWidth>
                <InputLabel>Primary Key Columns (for upsert/merge)</InputLabel>
                <Select
                  multiple
                  value={primaryKeyColumns}
                  onChange={(e) => setPrimaryKeyColumns(typeof e.target.value === 'string' ? e.target.value.split(',') : e.target.value)}
                  label="Primary Key Columns (for upsert/merge)"
                  renderValue={(selected) => (
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                      {selected.map((value) => (
                        <Chip key={value} label={value} size="small" />
                      ))}
                    </Box>
                  )}
                >
                  {/* Will be populated with actual column names from source data */}
                  <MenuItem value="id">id</MenuItem>
                  <MenuItem value="username">username</MenuItem>
                  <MenuItem value="email">email</MenuItem>
                </Select>
                <FormHelperText>
                  Select columns that uniquely identify records. Leave empty to auto-detect.
                </FormHelperText>
              </FormControl>
            </Grid>
          )}
        </Grid>
      ) : (
        <Grid container spacing={2}>
          <Grid item xs={12}>
            <FormControl fullWidth>
              <InputLabel>Export Format</InputLabel>
              <Select
                value={exportFormat}
                onChange={(e) => setExportFormat(e.target.value)}
                label="Export Format"
              >
                <MenuItem value="csv">CSV</MenuItem>
                <MenuItem value="excel">Excel</MenuItem>
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={12}>
            <TextField
              fullWidth
              label="Filename (optional)"
              value={filename}
              onChange={(e) => setFilename(e.target.value)}
              helperText="Leave empty to auto-generate filename with timestamp"
            />
          </Grid>
        </Grid>
      )}

      {outputType === 'table' && (
        <Alert severity="info" sx={{ mt: 2 }}>
          The transformed data will be saved to the specified table in the selected data source.
        </Alert>
      )}

      {outputType === 'export' && (
        <Alert severity="info" sx={{ mt: 2 }}>
          The transformed data will be exported to a file that can be downloaded.
        </Alert>
      )}
    </Box>
  );
};

export default OutputConfigEditor;