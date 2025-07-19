import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Button,
  Grid,
  Card,
  CardContent,
  CardActions,
  Chip,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Alert,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Tabs,
  Tab,
  CircularProgress,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  FormControlLabel,
  Switch,
  Divider
} from '@mui/material';
import {
  Add as AddIcon,
  Delete as DeleteIcon,
  Refresh as RefreshIcon,
  PlayArrow as ExtractIcon,
  Visibility as PreviewIcon,
  Storage as DatabaseIcon
} from '@mui/icons-material';
import DataSourceSelector from '../components/data-sources/DataSourceSelector';
import { dataSourceAPI, DataSource, SchemaInfo, DataPreview } from '../services/dataSourceAPI';

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel({ children, value, index }: TabPanelProps) {
  return (
    <div hidden={value !== index}>
      {value === index && <Box sx={{ p: 3 }}>{children}</Box>}
    </div>
  );
}

const DataSourcesPage: React.FC = () => {
  const [dataSources, setDataSources] = useState<DataSource[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>('');
  const [showSelector, setShowSelector] = useState(false);
  const [selectedDataSource, setSelectedDataSource] = useState<DataSource | null>(null);
  const [schemaInfo, setSchemaInfo] = useState<SchemaInfo[]>([]);
  const [previewData, setPreviewData] = useState<DataPreview | null>(null);
  const [previewSource, setPreviewSource] = useState<string>('');
  const [tabValue, setTabValue] = useState(0);
  const [extractionDialog, setExtractionDialog] = useState(false);
  const [extractionForm, setExtractionForm] = useState({
    job_name: '',
    target_table: '',
    extraction_mode: 'full',
    source_query: '',
    chunk_size: 10000,
    incremental_column: '',
    incremental_last_value: '',
    enable_real_time: false,
    batch_size: 1000,
    max_parallel_chunks: 4
  });
  const [incrementalColumns, setIncrementalColumns] = useState<string[]>([]);
  const [supportsIncremental, setSupportsIncremental] = useState(false);
  const [supportsRealTime, setSupportsRealTime] = useState(false);

  useEffect(() => {
    loadDataSources();
  }, []);

  const loadDataSources = async () => {
    try {
      setLoading(true);
      const response = await dataSourceAPI.getDataSources();
      setDataSources(response.data);
    } catch (err: any) {
      setError('Failed to load data sources');
    } finally {
      setLoading(false);
    }
  };

  const handleDataSourceCreated = (dataSource: DataSource) => {
    setDataSources(prev => [...prev, dataSource]);
    setShowSelector(false);
  };

  const handleDeleteDataSource = async (id: number) => {
    if (!window.confirm('Are you sure you want to delete this data source?')) {
      return;
    }

    try {
      await dataSourceAPI.deleteDataSource(id);
      setDataSources(prev => prev.filter(ds => ds.id !== id));
    } catch (err: any) {
      setError('Failed to delete data source');
    }
  };

  const loadSchemaInfo = async (dataSource: DataSource) => {
    try {
      setSelectedDataSource(dataSource);
      setTabValue(0);
      
      const response = await dataSourceAPI.getSchema({
        type: dataSource.type,
        connection_config: dataSource.connection_config
      });
      setSchemaInfo(response.data);
    } catch (err: any) {
      setError('Failed to load schema information');
    }
  };

  const handlePreviewData = async (sourceName: string) => {
    if (!selectedDataSource) return;

    try {
      setPreviewSource(sourceName);
      
      const response = await dataSourceAPI.previewData(
        selectedDataSource.type,
        selectedDataSource.connection_config,
        sourceName,
        100
      );
      setPreviewData(response.data);
      
      // Load incremental extraction capabilities
      await loadIncrementalInfo(sourceName);
    } catch (err: any) {
      setError('Failed to preview data');
      setPreviewData({ status: 'error', error: 'Failed to load preview', columns: [], sample_data: [], row_count: 0 });
    }
  };

  const loadIncrementalInfo = async (sourceName: string) => {
    if (!selectedDataSource) return;

    try {
      // This would be a new API endpoint to get incremental extraction info
      const response = await fetch('/api/v1/data-sources/incremental-info', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          type: selectedDataSource.type,
          connection_config: selectedDataSource.connection_config,
          source_name: sourceName
        })
      });
      
      if (response.ok) {
        const data = await response.json();
        setSupportsIncremental(data.supports_incremental);
        setSupportsRealTime(data.supports_real_time);
        setIncrementalColumns(data.incremental_columns || []);
      }
    } catch (err) {
      console.warn('Failed to load incremental info:', err);
    }
  };

  const handleStartExtraction = async () => {
    if (!selectedDataSource || !previewSource) return;

    try {
      if (extractionForm.extraction_mode === 'real_time') {
        // Start real-time sync
        const syncConfig = {
          transformations: {},
          soft_delete: true,
          ...(extractionForm.incremental_column && {
            incremental_column: extractionForm.incremental_column,
            last_value: extractionForm.incremental_last_value
          })
        };

        const response = await fetch(`/api/v1/data-sources/${selectedDataSource.id}/sync/start`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            source_name: previewSource,
            target_table: extractionForm.target_table,
            sync_config: syncConfig
          })
        });

        if (!response.ok) {
          throw new Error('Failed to start real-time sync');
        }

        const result = await response.json();
        setExtractionDialog(false);
        alert(`Real-time sync started! Sync ID: ${result.sync_id}`);
      } else {
        // Regular extraction job
        const job = {
          job_name: extractionForm.job_name,
          extraction_mode: extractionForm.extraction_mode,
          source_query: previewSource,
          target_table: extractionForm.target_table,
          config: {
            chunk_size: extractionForm.chunk_size,
            batch_size: extractionForm.batch_size,
            max_parallel_chunks: extractionForm.max_parallel_chunks,
            ...(extractionForm.extraction_mode === 'incremental' && {
              incremental_column: extractionForm.incremental_column,
              last_value: extractionForm.incremental_last_value
            })
          }
        };

        await dataSourceAPI.extractData(selectedDataSource.id, job);
        setExtractionDialog(false);
        alert(`${extractionForm.extraction_mode.charAt(0).toUpperCase() + extractionForm.extraction_mode.slice(1)} extraction job started successfully!`);
      }
    } catch (err: any) {
      setError(`Failed to start ${extractionForm.extraction_mode} extraction: ${err.message}`);
    }
  };

  const getStatusColor = (isActive: boolean) => {
    return isActive ? 'success' : 'default';
  };


  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h4" component="h1" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <DatabaseIcon /> Data Sources
        </Typography>
        <Box sx={{ display: 'flex', gap: 1 }}>
          <Button
            variant="outlined"
            onClick={loadDataSources}
            startIcon={<RefreshIcon />}
          >
            Refresh
          </Button>
          <Button
            variant="contained"
            onClick={() => setShowSelector(true)}
            startIcon={<AddIcon />}
          >
            Add Data Source
          </Button>
        </Box>
      </Box>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError('')}>
          {error}
        </Alert>
      )}

      <Grid container spacing={3}>
        {/* Data Sources List */}
        <Grid item xs={12} md={selectedDataSource ? 6 : 12}>
          <Typography variant="h6" gutterBottom>
            Connected Data Sources ({dataSources.length})
          </Typography>
          
          {dataSources.length === 0 ? (
            <Card sx={{ textAlign: 'center', p: 4 }}>
              <Typography variant="body1" color="text.secondary">
                No data sources configured yet.
              </Typography>
              <Button
                variant="contained"
                onClick={() => setShowSelector(true)}
                sx={{ mt: 2 }}
                startIcon={<AddIcon />}
              >
                Add Your First Data Source
              </Button>
            </Card>
          ) : (
            <Grid container spacing={2}>
              {dataSources.map((dataSource) => (
                <Grid item xs={12} key={dataSource.id}>
                  <Card>
                    <CardContent>
                      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                        <Box>
                          <Typography variant="h6" gutterBottom>
                            {dataSource.name}
                          </Typography>
                          <Typography variant="body2" color="text.secondary" gutterBottom>
                            {dataSource.description}
                          </Typography>
                          <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
                            <Chip
                              label={dataSource.type}
                              size="small"
                              color="primary"
                              variant="outlined"
                            />
                            <Chip
                              label={dataSource.is_active ? 'Active' : 'Inactive'}
                              size="small"
                              color={getStatusColor(dataSource.is_active)}
                            />
                          </Box>
                        </Box>
                        <CardActions>
                          <IconButton
                            size="small"
                            onClick={() => loadSchemaInfo(dataSource)}
                            title="Explore Schema"
                          >
                            <PreviewIcon />
                          </IconButton>
                          <IconButton
                            size="small"
                            onClick={() => handleDeleteDataSource(dataSource.id)}
                            title="Delete"
                            color="error"
                          >
                            <DeleteIcon />
                          </IconButton>
                        </CardActions>
                      </Box>
                    </CardContent>
                  </Card>
                </Grid>
              ))}
            </Grid>
          )}
        </Grid>

        {/* Schema Explorer */}
        {selectedDataSource && (
          <Grid item xs={12} md={6}>
            <Paper sx={{ height: '70vh', display: 'flex', flexDirection: 'column' }}>
              <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
                <Tabs value={tabValue} onChange={(_, v) => setTabValue(v)}>
                  <Tab label="Schema" />
                  <Tab label="Preview" disabled={!previewData} />
                </Tabs>
              </Box>

              <TabPanel value={tabValue} index={0}>
                <Typography variant="h6" gutterBottom>
                  {selectedDataSource.name} - Schema
                </Typography>
                
                {schemaInfo.length === 0 ? (
                  <Typography variant="body2" color="text.secondary">
                    No schema information available
                  </Typography>
                ) : (
                  <Box sx={{ maxHeight: '500px', overflow: 'auto' }}>
                    {schemaInfo.map((item, index) => (
                      <Card key={index} sx={{ mb: 1 }}>
                        <CardContent sx={{ pb: '16px !important' }}>
                          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                            <Box>
                              <Typography variant="subtitle1">
                                {item.name}
                              </Typography>
                              <Typography variant="caption" color="text.secondary">
                                {item.type} • {item.row_count || item.document_count || 0} records
                              </Typography>
                            </Box>
                            <Button
                              size="small"
                              onClick={() => handlePreviewData(item.name)}
                              startIcon={<PreviewIcon />}
                            >
                              Preview
                            </Button>
                          </Box>
                        </CardContent>
                      </Card>
                    ))}
                  </Box>
                )}
              </TabPanel>

              <TabPanel value={tabValue} index={1}>
                {previewData && (
                  <Box>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                      <Typography variant="h6">
                        {previewSource} - Data Preview
                      </Typography>
                      <Button
                        variant="contained"
                        size="small"
                        onClick={() => setExtractionDialog(true)}
                        startIcon={<ExtractIcon />}
                      >
                        Extract Data
                      </Button>
                    </Box>

                    {previewData.status === 'error' ? (
                      <Alert severity="error">{previewData.error}</Alert>
                    ) : (
                      <TableContainer component={Paper} sx={{ maxHeight: 400 }}>
                        <Table stickyHeader size="small">
                          <TableHead>
                            <TableRow>
                              {previewData.columns.map((col) => (
                                <TableCell key={col.name}>
                                  <Typography variant="subtitle2">{col.name}</Typography>
                                  <Typography variant="caption" color="text.secondary">
                                    {col.sql_type}
                                  </Typography>
                                </TableCell>
                              ))}
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {previewData.sample_data.map((row, idx) => (
                              <TableRow key={idx}>
                                {previewData.columns.map((col) => (
                                  <TableCell key={col.name}>
                                    {String(row[col.name] || '')}
                                  </TableCell>
                                ))}
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </TableContainer>
                    )}
                  </Box>
                )}
              </TabPanel>
            </Paper>
          </Grid>
        )}
      </Grid>

      {/* Data Source Selector Dialog */}
      <DataSourceSelector
        open={showSelector}
        onClose={() => setShowSelector(false)}
        onDataSourceCreated={handleDataSourceCreated}
      />

      {/* Extraction Dialog */}
      <Dialog open={extractionDialog} onClose={() => setExtractionDialog(false)} maxWidth="md" fullWidth>
        <DialogTitle>Configure Data Extraction</DialogTitle>
        <DialogContent>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3, mt: 1 }}>
            
            {/* Basic Configuration */}
            <Box>
              <Typography variant="h6" gutterBottom>Basic Configuration</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} sm={6}>
                  <TextField
                    label="Job Name"
                    value={extractionForm.job_name}
                    onChange={(e) => setExtractionForm(prev => ({ ...prev, job_name: e.target.value }))}
                    required
                    fullWidth
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <TextField
                    label="Target Table Name"
                    value={extractionForm.target_table}
                    onChange={(e) => setExtractionForm(prev => ({ ...prev, target_table: e.target.value }))}
                    required
                    fullWidth
                  />
                </Grid>
                <Grid item xs={12}>
                  <FormControl fullWidth>
                    <InputLabel>Extraction Mode</InputLabel>
                    <Select
                      value={extractionForm.extraction_mode}
                      onChange={(e) => setExtractionForm(prev => ({ ...prev, extraction_mode: e.target.value }))}
                      label="Extraction Mode"
                    >
                      <MenuItem value="full">Full Extraction</MenuItem>
                      {supportsIncremental && <MenuItem value="incremental">Incremental Extraction</MenuItem>}
                      <MenuItem value="chunked">Chunked Extraction</MenuItem>
                      {supportsRealTime && <MenuItem value="real_time">Real-time Sync</MenuItem>}
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
            </Box>

            <Divider />

            {/* Performance Configuration */}
            <Box>
              <Typography variant="h6" gutterBottom>Performance Settings</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} sm={6}>
                  <TextField
                    label="Chunk Size"
                    type="number"
                    value={extractionForm.chunk_size}
                    onChange={(e) => setExtractionForm(prev => ({ ...prev, chunk_size: parseInt(e.target.value) }))}
                    helperText="Number of records per chunk"
                    fullWidth
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <TextField
                    label="Batch Size"
                    type="number"
                    value={extractionForm.batch_size}
                    onChange={(e) => setExtractionForm(prev => ({ ...prev, batch_size: parseInt(e.target.value) }))}
                    helperText="Records per database insert"
                    fullWidth
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <TextField
                    label="Max Parallel Chunks"
                    type="number"
                    value={extractionForm.max_parallel_chunks}
                    onChange={(e) => setExtractionForm(prev => ({ ...prev, max_parallel_chunks: parseInt(e.target.value) }))}
                    helperText="Maximum concurrent processing"
                    fullWidth
                  />
                </Grid>
              </Grid>
            </Box>

            {/* Incremental Configuration */}
            {(extractionForm.extraction_mode === 'incremental' && supportsIncremental) && (
              <>
                <Divider />
                <Box>
                  <Typography variant="h6" gutterBottom>Incremental Settings</Typography>
                  <Grid container spacing={2}>
                    <Grid item xs={12} sm={6}>
                      <FormControl fullWidth>
                        <InputLabel>Incremental Column</InputLabel>
                        <Select
                          value={extractionForm.incremental_column}
                          onChange={(e) => setExtractionForm(prev => ({ ...prev, incremental_column: e.target.value }))}
                          label="Incremental Column"
                        >
                          {incrementalColumns.map((col) => (
                            <MenuItem key={col} value={col}>{col}</MenuItem>
                          ))}
                        </Select>
                      </FormControl>
                    </Grid>
                    <Grid item xs={12} sm={6}>
                      <TextField
                        label="Last Value"
                        value={extractionForm.incremental_last_value}
                        onChange={(e) => setExtractionForm(prev => ({ ...prev, incremental_last_value: e.target.value }))}
                        helperText="Leave empty to start from beginning"
                        fullWidth
                      />
                    </Grid>
                  </Grid>
                </Box>
              </>
            )}

            {/* Real-time Configuration */}
            {(extractionForm.extraction_mode === 'real_time' && supportsRealTime) && (
              <>
                <Divider />
                <Box>
                  <Typography variant="h6" gutterBottom>Real-time Settings</Typography>
                  <Alert severity="info" sx={{ mb: 2 }}>
                    Real-time sync will continuously monitor changes and sync them to the target table.
                  </Alert>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={extractionForm.enable_real_time}
                        onChange={(e) => setExtractionForm(prev => ({ ...prev, enable_real_time: e.target.checked }))}
                      />
                    }
                    label="Enable Real-time Synchronization"
                  />
                </Box>
              </>
            )}

            {/* Chunked Configuration */}
            {extractionForm.extraction_mode === 'chunked' && (
              <>
                <Divider />
                <Box>
                  <Typography variant="h6" gutterBottom>Chunked Extraction</Typography>
                  <Alert severity="info" sx={{ mb: 2 }}>
                    Chunked extraction processes large datasets in smaller batches to optimize memory usage and performance.
                  </Alert>
                  <Typography variant="body2" color="text.secondary">
                    Chunk size: {extractionForm.chunk_size.toLocaleString()} records<br/>
                    Parallel chunks: {extractionForm.max_parallel_chunks}<br/>
                    Estimated memory usage: ~{Math.round(extractionForm.chunk_size * extractionForm.max_parallel_chunks / 1000)}K records in memory
                  </Typography>
                </Box>
              </>
            )}

          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setExtractionDialog(false)}>Cancel</Button>
          <Button
            variant="contained"
            onClick={handleStartExtraction}
            disabled={
              !extractionForm.job_name || 
              !extractionForm.target_table ||
              (extractionForm.extraction_mode === 'incremental' && !extractionForm.incremental_column)
            }
          >
            {extractionForm.extraction_mode === 'real_time' ? 'Start Real-time Sync' : 'Start Extraction'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default DataSourcesPage;