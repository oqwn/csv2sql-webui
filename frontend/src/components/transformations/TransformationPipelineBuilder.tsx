import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  TextField,
  Button,
  Grid,
  Typography,
  IconButton,
  Alert,
  Stepper,
  Step,
  StepLabel,
  Card,
  CardContent,
  Chip,
} from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import EditIcon from '@mui/icons-material/Edit';
import PreviewIcon from '@mui/icons-material/Preview';
import SaveIcon from '@mui/icons-material/Save';
import DragIndicatorIcon from '@mui/icons-material/DragIndicator';
// import { DragDropContext, Droppable, Draggable } from 'react-beautiful-dnd';
import { TransformationPipeline, TransformationStep, TransformationType } from '../../types/transformation.types';
import TransformationStepEditor from './TransformationStepEditor';
import DataSourceSelector from './DataSourceSelector';
import OutputConfigEditor from './OutputConfigEditor';

interface Props {
  pipeline: TransformationPipeline | null;
  onSave: (pipeline: TransformationPipeline) => void;
  onPreview: (sourceConfig: any, steps: TransformationStep[]) => void;
  onCancel: () => void;
}

const TransformationPipelineBuilder: React.FC<Props> = ({
  pipeline,
  onSave,
  onPreview,
  onCancel,
}) => {
  const [activeStep, setActiveStep] = useState(0);
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [sourceConfig, setSourceConfig] = useState<any>(null);
  const [steps, setSteps] = useState<TransformationStep[]>([]);
  const [outputConfig, setOutputConfig] = useState<any>(null);
  const [editingStep, setEditingStep] = useState<TransformationStep | null>(null);
  const [editingStepIndex, setEditingStepIndex] = useState<number>(-1);
  const [errors, setErrors] = useState<Record<string, string>>({});

  useEffect(() => {
    if (pipeline) {
      setName(pipeline.name);
      setDescription(pipeline.description || '');
      setSourceConfig(pipeline.source_config);
      // Ensure all steps have IDs
      const stepsWithIds = pipeline.steps.map((step, index) => ({
        ...step,
        id: step.id || `step-${Date.now()}-${index}-${Math.random().toString(36).substr(2, 9)}`
      }));
      setSteps(stepsWithIds);
      setOutputConfig(pipeline.output_config || { type: 'table' });
    }
  }, [pipeline]);

  const handleNext = () => {
    setActiveStep((prevActiveStep) => prevActiveStep + 1);
  };

  const handleBack = () => {
    setActiveStep((prevActiveStep) => prevActiveStep - 1);
  };

  const handleAddStep = (type: TransformationType) => {
    const newStep: TransformationStep = {
      id: `step-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      name: `New ${type} transformation`,
      type,
      config: {},
    };
    setEditingStep(newStep);
    setEditingStepIndex(-1);
  };

  const handleEditStep = (index: number) => {
    setEditingStep(steps[index]);
    setEditingStepIndex(index);
  };

  const handleDeleteStep = (index: number) => {
    const newSteps = [...steps];
    newSteps.splice(index, 1);
    setSteps(newSteps);
  };

  const handleSaveStep = (step: TransformationStep) => {
    if (editingStepIndex === -1) {
      setSteps([...steps, step]);
    } else {
      const newSteps = [...steps];
      newSteps[editingStepIndex] = step;
      setSteps(newSteps);
    }
    setEditingStep(null);
    setEditingStepIndex(-1);
  };

  const handleMoveStep = (fromIndex: number, toIndex: number) => {
    const items = Array.from(steps);
    const [reorderedItem] = items.splice(fromIndex, 1);
    items.splice(toIndex, 0, reorderedItem);
    setSteps(items);
  };

  const handlePreview = () => {
    if (!sourceConfig) {
      setErrors({ source: 'Please select a data source' });
      return;
    }
    onPreview(sourceConfig, steps);
  };

  const handleSave = () => {
    const validationErrors: Record<string, string> = {};

    if (!name.trim()) {
      validationErrors.name = 'Pipeline name is required';
    }
    if (!sourceConfig) {
      validationErrors.source = 'Data source is required';
    }

    if (Object.keys(validationErrors).length > 0) {
      setErrors(validationErrors);
      return;
    }

    const pipelineData: TransformationPipeline = {
      ...(pipeline?.id && { id: pipeline.id }),
      name,
      description,
      source_config: sourceConfig,
      steps,
      output_config: outputConfig,
    };

    onSave(pipelineData);
  };

  const transformationTypes = [
    { type: TransformationType.FILTER, label: 'Filter', icon: '🔍' },
    { type: TransformationType.CLEAN, label: 'Clean', icon: '🧹' },
    { type: TransformationType.AGGREGATE, label: 'Aggregate', icon: '📊' },
    { type: TransformationType.JOIN, label: 'Join', icon: '🔗' },
    { type: TransformationType.SPLIT_COLUMN, label: 'Split Column', icon: '✂️' },
    { type: TransformationType.MERGE_COLUMN, label: 'Merge Columns', icon: '🔀' },
    { type: TransformationType.TYPE_CONVERSION, label: 'Convert Type', icon: '🔄' },
    { type: TransformationType.RENAME, label: 'Rename', icon: '✏️' },
    { type: TransformationType.DROP, label: 'Drop Columns', icon: '🗑️' },
    { type: TransformationType.FILL_NULL, label: 'Fill Nulls', icon: '📝' },
    { type: TransformationType.CUSTOM_SQL, label: 'Custom SQL', icon: '💾' },
    { type: TransformationType.CUSTOM_PYTHON, label: 'Custom Python', icon: '🐍' },
  ];

  const stepLabels = ['Pipeline Info', 'Data Source', 'Transformations', 'Output'];

  return (
    <Box>
      <Stepper activeStep={activeStep} sx={{ mb: 4 }}>
        {stepLabels.map((label) => (
          <Step key={label}>
            <StepLabel>{label}</StepLabel>
          </Step>
        ))}
      </Stepper>

      {activeStep === 0 && (
        <Paper sx={{ p: 3 }}>
          <Typography variant="h6" gutterBottom>
            Pipeline Information
          </Typography>
          <Grid container spacing={3}>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Pipeline Name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                error={!!errors.name}
                helperText={errors.name}
                required
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Description"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                multiline
                rows={3}
              />
            </Grid>
          </Grid>
          <Box sx={{ mt: 3, display: 'flex', justifyContent: 'flex-end' }}>
            <Button onClick={onCancel} sx={{ mr: 1 }}>
              Cancel
            </Button>
            <Button variant="contained" onClick={handleNext}>
              Next
            </Button>
          </Box>
        </Paper>
      )}

      {activeStep === 1 && (
        <Paper sx={{ p: 3 }}>
          <Typography variant="h6" gutterBottom>
            Select Data Source
          </Typography>
          {errors.source && (
            <Alert severity="error" sx={{ mb: 2 }}>
              {errors.source}
            </Alert>
          )}
          <DataSourceSelector
            value={sourceConfig}
            onChange={setSourceConfig}
          />
          <Box sx={{ mt: 3, display: 'flex', justifyContent: 'space-between' }}>
            <Button onClick={handleBack}>
              Back
            </Button>
            <Button
              variant="contained"
              onClick={handleNext}
              disabled={!sourceConfig}
            >
              Next
            </Button>
          </Box>
        </Paper>
      )}

      {activeStep === 2 && (
        <Box>
          <Paper sx={{ p: 3, mb: 3 }}>
            <Typography variant="h6" gutterBottom>
              Add Transformations
            </Typography>
            <Grid container spacing={2}>
              {transformationTypes.map(({ type, label, icon }) => (
                <Grid item key={type}>
                  <Button
                    variant="outlined"
                    startIcon={<span>{icon}</span>}
                    onClick={() => handleAddStep(type)}
                  >
                    {label}
                  </Button>
                </Grid>
              ))}
            </Grid>
          </Paper>

          <Typography variant="h6" gutterBottom>
            Transformation Steps
          </Typography>
          
          {steps.length === 0 ? (
            <Alert severity="info">
              No transformation steps added yet. Click on a transformation type above to add one.
            </Alert>
          ) : (
            <Box>
              {steps.map((step, index) => (
                <Card key={index} sx={{ mb: 2 }}>
                  <CardContent>
                    <Box display="flex" alignItems="center">
                      <Box sx={{ mr: 2 }}>
                        <DragIndicatorIcon color="action" />
                      </Box>
                      <Box flex={1}>
                        <Typography variant="subtitle1">
                          {step.name}
                        </Typography>
                        <Chip
                          label={step.type}
                          size="small"
                          color="primary"
                          sx={{ mt: 1 }}
                        />
                      </Box>
                      {index > 0 && (
                        <IconButton 
                          onClick={() => handleMoveStep(index, index - 1)}
                          size="small"
                          title="Move Up"
                        >
                          ⬆️
                        </IconButton>
                      )}
                      {index < steps.length - 1 && (
                        <IconButton 
                          onClick={() => handleMoveStep(index, index + 1)}
                          size="small"
                          title="Move Down"
                        >
                          ⬇️
                        </IconButton>
                      )}
                      <IconButton onClick={() => handleEditStep(index)}>
                        <EditIcon />
                      </IconButton>
                      <IconButton onClick={() => handleDeleteStep(index)}>
                        <DeleteIcon />
                      </IconButton>
                    </Box>
                  </CardContent>
                </Card>
              ))}
            </Box>
          )}

          <Box sx={{ mt: 3, display: 'flex', justifyContent: 'space-between' }}>
            <Button onClick={handleBack}>
              Back
            </Button>
            <Box>
              <Button
                variant="outlined"
                startIcon={<PreviewIcon />}
                onClick={handlePreview}
                sx={{ mr: 1 }}
                disabled={steps.length === 0}
              >
                Preview
              </Button>
              <Button variant="contained" onClick={handleNext}>
                Next
              </Button>
            </Box>
          </Box>
        </Box>
      )}

      {activeStep === 3 && (
        <Paper sx={{ p: 3 }}>
          <Typography variant="h6" gutterBottom>
            Output Configuration
          </Typography>
          <OutputConfigEditor
            value={outputConfig || { type: 'table' }}
            onChange={setOutputConfig}
            sourceDataSourceId={sourceConfig?.datasource_id}
          />
          <Box sx={{ mt: 3, display: 'flex', justifyContent: 'space-between' }}>
            <Button onClick={handleBack}>
              Back
            </Button>
            <Box>
              <Button onClick={onCancel} sx={{ mr: 1 }}>
                Cancel
              </Button>
              <Button
                variant="contained"
                startIcon={<SaveIcon />}
                onClick={handleSave}
              >
                Save Pipeline
              </Button>
            </Box>
          </Box>
        </Paper>
      )}

      {editingStep && (
        <TransformationStepEditor
          open={true}
          step={editingStep}
          onSave={handleSaveStep}
          onCancel={() => {
            setEditingStep(null);
            setEditingStepIndex(-1);
          }}
          sourceConfig={sourceConfig}
        />
      )}
    </Box>
  );
};

export default TransformationPipelineBuilder;