import React from 'react';
import {
  Grid,
  Paper,
  Typography,
  Box,
  Card,
  CardContent,
  CardActions,
  Button,
} from '@mui/material';
import {
  Storage as StorageIcon,
  Code as CodeIcon,
  CloudUpload as CloudUploadIcon,
  Schedule as ScheduleIcon,
} from '@mui/icons-material';
import { useNavigate } from 'react-router-dom';

const DashboardPage: React.FC = () => {
  const navigate = useNavigate();

  const features = [
    {
      title: 'SQL Editor',
      description: 'Write and execute SQL queries with syntax highlighting',
      icon: <CodeIcon sx={{ fontSize: 40 }} />,
      action: () => navigate('/sql-editor'),
    },
    {
      title: 'Import Data',
      description: 'Import CSV and Excel files into your database',
      icon: <CloudUploadIcon sx={{ fontSize: 40 }} />,
      action: () => navigate('/import'),
    },
    {
      title: 'Data Catalog',
      description: 'Browse and search your database schema',
      icon: <StorageIcon sx={{ fontSize: 40 }} />,
      action: () => console.log('Coming soon'),
    },
    {
      title: 'Batch Scheduling',
      description: 'Schedule SQL queries to run automatically',
      icon: <ScheduleIcon sx={{ fontSize: 40 }} />,
      action: () => console.log('Coming soon'),
    },
  ];

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Welcome to SQL WebUI
      </Typography>
      <Typography variant="body1" color="text.secondary" paragraph>
        A powerful web-based SQL management tool
      </Typography>
      
      <Grid container spacing={3} sx={{ mt: 2 }}>
        {features.map((feature, index) => (
          <Grid item xs={12} sm={6} md={3} key={index}>
            <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
              <CardContent sx={{ flexGrow: 1 }}>
                <Box sx={{ display: 'flex', justifyContent: 'center', mb: 2 }}>
                  {feature.icon}
                </Box>
                <Typography gutterBottom variant="h5" component="h2">
                  {feature.title}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  {feature.description}
                </Typography>
              </CardContent>
              <CardActions>
                <Button size="small" onClick={feature.action}>
                  Open
                </Button>
              </CardActions>
            </Card>
          </Grid>
        ))}
      </Grid>

      <Paper sx={{ mt: 4, p: 3 }}>
        <Typography variant="h6" gutterBottom>
          Quick Stats
        </Typography>
        <Grid container spacing={2}>
          <Grid item xs={12} sm={4}>
            <Typography variant="body2" color="text.secondary">
              Total Tables
            </Typography>
            <Typography variant="h4">0</Typography>
          </Grid>
          <Grid item xs={12} sm={4}>
            <Typography variant="body2" color="text.secondary">
              Recent Queries
            </Typography>
            <Typography variant="h4">0</Typography>
          </Grid>
          <Grid item xs={12} sm={4}>
            <Typography variant="body2" color="text.secondary">
              Scheduled Jobs
            </Typography>
            <Typography variant="h4">0</Typography>
          </Grid>
        </Grid>
      </Paper>
    </Box>
  );
};

export default DashboardPage;