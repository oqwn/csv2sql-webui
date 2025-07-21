import React from 'react';
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  IconButton,
  Chip,
  Typography,
  Skeleton,
  Tooltip,
} from '@mui/material';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import { format } from 'date-fns';
import { TransformationPipeline } from '../../types/transformation.types';

interface Props {
  pipelines: TransformationPipeline[];
  onEdit: (pipeline: TransformationPipeline) => void;
  onDelete: (pipelineId: string) => void;
  onExecute: (pipeline: TransformationPipeline) => void;
  loading?: boolean;
}

const TransformationPipelineList: React.FC<Props> = ({
  pipelines,
  onEdit,
  onDelete,
  onExecute,
  loading,
}) => {
  if (loading) {
    return (
      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Description</TableCell>
              <TableCell>Steps</TableCell>
              <TableCell>Source</TableCell>
              <TableCell>Last Updated</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {[1, 2, 3].map((i) => (
              <TableRow key={i}>
                <TableCell>
                  <Skeleton />
                </TableCell>
                <TableCell>
                  <Skeleton />
                </TableCell>
                <TableCell>
                  <Skeleton />
                </TableCell>
                <TableCell>
                  <Skeleton />
                </TableCell>
                <TableCell>
                  <Skeleton />
                </TableCell>
                <TableCell>
                  <Skeleton />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    );
  }

  if (pipelines.length === 0) {
    return (
      <Paper sx={{ p: 4, textAlign: 'center' }}>
        <Typography variant="body1" color="text.secondary">
          No transformation pipelines found. Create your first pipeline to get started.
        </Typography>
      </Paper>
    );
  }

  return (
    <TableContainer component={Paper}>
      <Table>
        <TableHead>
          <TableRow>
            <TableCell>Name</TableCell>
            <TableCell>Description</TableCell>
            <TableCell>Steps</TableCell>
            <TableCell>Source</TableCell>
            <TableCell>Last Updated</TableCell>
            <TableCell align="right">Actions</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {pipelines.map((pipeline) => (
            <TableRow key={pipeline.id}>
              <TableCell>
                <Typography variant="subtitle2">{pipeline.name}</Typography>
              </TableCell>
              <TableCell>
                <Typography variant="body2" color="text.secondary">
                  {pipeline.description || '-'}
                </Typography>
              </TableCell>
              <TableCell>
                <Chip label={`${pipeline.steps.length} steps`} size="small" />
              </TableCell>
              <TableCell>
                <Typography variant="body2">
                  {pipeline.source_config?.table_name || 'Custom Query'}
                </Typography>
              </TableCell>
              <TableCell>
                {pipeline.updated_at && (
                  <Typography variant="body2" color="text.secondary">
                    {format(new Date(pipeline.updated_at), 'PPp')}
                  </Typography>
                )}
              </TableCell>
              <TableCell align="right">
                <Tooltip title="Execute Pipeline">
                  <IconButton
                    size="small"
                    onClick={() => onExecute(pipeline)}
                    color="primary"
                  >
                    <PlayArrowIcon />
                  </IconButton>
                </Tooltip>
                <Tooltip title="Edit Pipeline">
                  <IconButton
                    size="small"
                    onClick={() => onEdit(pipeline)}
                  >
                    <EditIcon />
                  </IconButton>
                </Tooltip>
                <Tooltip title="Delete Pipeline">
                  <IconButton
                    size="small"
                    onClick={() => onDelete(pipeline.id!)}
                    color="error"
                  >
                    <DeleteIcon />
                  </IconButton>
                </Tooltip>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
};

export default TransformationPipelineList;