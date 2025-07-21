import React, { useState, useEffect } from 'react';
import {
  Box,
  Container,
  Paper,
  Typography,
  Button,
  Tabs,
  Tab,
  Alert,
  Snackbar,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import TransformationPipelineBuilder from '../components/transformations/TransformationPipelineBuilder';
import TransformationPipelineList from '../components/transformations/TransformationPipelineList';
import TransformationPreview from '../components/transformations/TransformationPreview';
import { transformationService } from '../services/transformationService';
import { TransformationPipeline, TransformationStep } from '../types/transformation.types';

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

const TabPanel: React.FC<TabPanelProps> = ({ children, value, index, ...other }) => {
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`transformation-tabpanel-${index}`}
      aria-labelledby={`transformation-tab-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ p: 3 }}>{children}</Box>}
    </div>
  );
};

const TransformationsPage: React.FC = () => {
  const [tabValue, setTabValue] = useState(0);
  const [pipelines, setPipelines] = useState<TransformationPipeline[]>([]);
  const [selectedPipeline, setSelectedPipeline] = useState<TransformationPipeline | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [showPreview, setShowPreview] = useState(false);
  const [previewData, setPreviewData] = useState<any>(null);

  useEffect(() => {
    loadPipelines();
  }, []);

  const loadPipelines = async () => {
    try {
      setLoading(true);
      const response = await transformationService.listPipelines();
      setPipelines(response.pipelines);
    } catch (error: any) {
      setError(error.message || 'Failed to load pipelines');
    } finally {
      setLoading(false);
    }
  };

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  const handleCreatePipeline = () => {
    setSelectedPipeline(null);
    setTabValue(1);
  };

  const handleEditPipeline = (pipeline: TransformationPipeline) => {
    setSelectedPipeline(pipeline);
    setTabValue(1);
  };

  const handleDeletePipeline = async (pipelineId: string) => {
    try {
      await transformationService.deletePipeline(pipelineId);
      setSuccess('Pipeline deleted successfully');
      loadPipelines();
    } catch (error: any) {
      setError(error.message || 'Failed to delete pipeline');
    }
  };

  const handleSavePipeline = async (pipeline: TransformationPipeline) => {
    try {
      setLoading(true);
      if (pipeline.id) {
        await transformationService.updatePipeline(pipeline.id, pipeline);
        setSuccess('Pipeline updated successfully');
      } else {
        await transformationService.createPipeline(pipeline);
        setSuccess('Pipeline created successfully');
      }
      loadPipelines();
      setTabValue(0);
    } catch (error: any) {
      setError(error.message || 'Failed to save pipeline');
    } finally {
      setLoading(false);
    }
  };

  const handlePreviewTransformation = async (sourceConfig: any, steps: TransformationStep[]) => {
    try {
      setLoading(true);
      const response = await transformationService.previewTransformation(sourceConfig, steps);
      setPreviewData(response);
      setShowPreview(true);
    } catch (error: any) {
      setError(error.message || 'Failed to preview transformation');
    } finally {
      setLoading(false);
    }
  };

  const handleExecutePipeline = async (pipeline: TransformationPipeline) => {
    try {
      setLoading(true);
      const response = await transformationService.executePipeline(
        pipeline.id!, 
        pipeline.output_config || {
          type: 'table',
          datasource_id: pipeline.source_config?.datasource_id,
          table_name: `${pipeline.name.toLowerCase().replace(/\s+/g, '_')}_result`,
          if_exists: 'replace'
        }
      );
      setSuccess(`Pipeline executed successfully! ${response.message}`);
    } catch (error: any) {
      setError(error.message || 'Failed to execute pipeline');
    } finally {
      setLoading(false);
    }
  };

  return (
      <Container maxWidth="xl">
        <Box sx={{ mb: 4 }}>
          <Typography variant="h4" component="h1" gutterBottom>
            Data Transformations
          </Typography>
          <Typography variant="body1" color="text.secondary">
            Create and manage data transformation pipelines
          </Typography>
        </Box>

        <Paper sx={{ width: '100%' }}>
          <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
            <Tabs value={tabValue} onChange={handleTabChange}>
              <Tab label="Pipelines" />
              <Tab label="Pipeline Builder" />
            </Tabs>
          </Box>

          <TabPanel value={tabValue} index={0}>
            <Box sx={{ mb: 2 }}>
              <Button
                variant="contained"
                startIcon={<AddIcon />}
                onClick={handleCreatePipeline}
              >
                Create New Pipeline
              </Button>
            </Box>

            <TransformationPipelineList
              pipelines={pipelines}
              onEdit={handleEditPipeline}
              onDelete={handleDeletePipeline}
              onExecute={handleExecutePipeline}
              loading={loading}
            />
          </TabPanel>

          <TabPanel value={tabValue} index={1}>
            <TransformationPipelineBuilder
              pipeline={selectedPipeline}
              onSave={handleSavePipeline}
              onPreview={handlePreviewTransformation}
              onCancel={() => setTabValue(0)}
            />
          </TabPanel>
        </Paper>

        {showPreview && previewData && (
          <TransformationPreview
            open={showPreview}
            onClose={() => setShowPreview(false)}
            previewData={previewData}
          />
        )}

        <Snackbar
          open={!!error}
          autoHideDuration={6000}
          onClose={() => setError(null)}
        >
          <Alert severity="error" onClose={() => setError(null)}>
            {error}
          </Alert>
        </Snackbar>

        <Snackbar
          open={!!success}
          autoHideDuration={6000}
          onClose={() => setSuccess(null)}
        >
          <Alert severity="success" onClose={() => setSuccess(null)}>
            {success}
          </Alert>
        </Snackbar>
      </Container>
  );
};

export default TransformationsPage;