import React, { useState, useEffect } from 'react';
import {
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Box,
  Grid,
  Typography,
  RadioGroup,
  FormControlLabel,
  Radio,
  Alert,
} from '@mui/material';
import CodeEditor from '@uiw/react-textarea-code-editor';
import { dataSourceService } from '../../services/api';
import { tableService } from '../../services/api';

interface Props {
  value: any;
  onChange: (value: any) => void;
}

const DataSourceSelector: React.FC<Props> = ({ value, onChange }) => {
  const [sourceType, setSourceType] = useState(value?.query ? 'query' : 'table');
  const [dataSources, setDataSources] = useState<any[]>([]);
  const [tables, setTables] = useState<string[]>([]);
  const [selectedDataSource, setSelectedDataSource] = useState(value?.datasource_id || '');
  const [selectedTable, setSelectedTable] = useState(value?.table_name || '');
  const [query, setQuery] = useState(value?.query || '');

  useEffect(() => {
    loadDataSources();
  }, []);

  useEffect(() => {
    if (selectedDataSource) {
      loadTables();
    }
  }, [selectedDataSource]);

  useEffect(() => {
    if (sourceType === 'table' && selectedDataSource && selectedTable) {
      onChange({
        datasource_id: selectedDataSource,
        table_name: selectedTable,
      });
    } else if (sourceType === 'query' && selectedDataSource && query) {
      onChange({
        datasource_id: selectedDataSource,
        query: query,
      });
    }
  }, [sourceType, selectedDataSource, selectedTable, query]);

  const loadDataSources = async () => {
    try {
      const response = await dataSourceService.listDataSources();
      setDataSources(response);
    } catch (error) {
      console.error('Failed to load data sources:', error);
    }
  };

  const loadTables = async () => {
    try {
      const response = await tableService.getTables(selectedDataSource);
      setTables(response.tables.map((t: any) => t.name));
    } catch (error) {
      console.error('Failed to load tables:', error);
      setTables([]);
    }
  };

  return (
    <Box>
      <RadioGroup
        row
        value={sourceType}
        onChange={(e) => setSourceType(e.target.value)}
        sx={{ mb: 3 }}
      >
        <FormControlLabel value="table" control={<Radio />} label="Select Table" />
        <FormControlLabel value="query" control={<Radio />} label="Custom Query" />
      </RadioGroup>

      <Grid container spacing={2}>
        <Grid item xs={12}>
          <FormControl fullWidth>
            <InputLabel>Data Source</InputLabel>
            <Select
              value={selectedDataSource}
              onChange={(e) => {
                setSelectedDataSource(e.target.value);
                setSelectedTable('');
              }}
              label="Data Source"
            >
              {dataSources.map((ds) => (
                <MenuItem key={ds.id} value={ds.id}>
                  {ds.name}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Grid>

        {sourceType === 'table' ? (
          <Grid item xs={12}>
            <FormControl fullWidth>
              <InputLabel>Table</InputLabel>
              <Select
                value={selectedTable}
                onChange={(e) => setSelectedTable(e.target.value)}
                label="Table"
                disabled={!selectedDataSource || tables.length === 0}
              >
                {tables.map((table) => (
                  <MenuItem key={table} value={table}>
                    {table}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
        ) : (
          <Grid item xs={12}>
            <Typography variant="subtitle2" gutterBottom>
              SQL Query
            </Typography>
            <CodeEditor
              value={query}
              language="sql"
              placeholder="SELECT * FROM your_table WHERE ..."
              onChange={(evn) => setQuery(evn.target.value)}
              padding={15}
              style={{
                fontSize: 12,
                backgroundColor: '#f5f5f5',
                fontFamily:
                  'ui-monospace,SFMono-Regular,SF Mono,Consolas,Liberation Mono,Menlo,monospace',
              }}
              minHeight={150}
            />
            <Alert severity="info" sx={{ mt: 2 }}>
              Write a SELECT query to define the source data for transformations
            </Alert>
          </Grid>
        )}
      </Grid>
    </Box>
  );
};

export default DataSourceSelector;