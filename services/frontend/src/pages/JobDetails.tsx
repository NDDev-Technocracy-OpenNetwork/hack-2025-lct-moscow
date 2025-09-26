import React, { useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  Box,
  Card,
  CardContent,
  Typography,
  Button,
  Grid,
  Chip,
  LinearProgress,
  Alert,
  Paper,
  Divider,
} from '@mui/material'
import {
  ArrowBack as ArrowBackIcon,
  Timeline as TimelineIcon,
} from '@mui/icons-material'
import { useJobStore } from '../store/jobStore'

export const JobDetails: React.FC = () => {
  const { jobId } = useParams<{ jobId: string }>()
  const navigate = useNavigate()
  const {
    currentJob,
    jobStatuses,
    isLoading,
    error,
    fetchJob,
    subscribeToJob,
    unsubscribeFromJob,
  } = useJobStore()

  useEffect(() => {
    if (jobId) {
      fetchJob(jobId)
      subscribeToJob(jobId)

      return () => {
        unsubscribeFromJob(jobId)
      }
    }
  }, [jobId, fetchJob, subscribeToJob, unsubscribeFromJob])

  if (!jobId) {
    return (
      <Alert severity="error">
        Invalid job ID
      </Alert>
    )
  }

  if (isLoading && !currentJob) {
    return (
      <Box display="flex" justifyContent="center" mt={4}>
        <LinearProgress sx={{ width: 200 }} />
      </Box>
    )
  }

  if (error) {
    return (
      <Alert severity="error">
        Error: {error}
      </Alert>
    )
  }

  if (!currentJob) {
    return (
      <Alert severity="warning">
        Job not found
      </Alert>
    )
  }

  const status = jobStatuses[jobId] || {
    job_id: jobId,
    status: currentJob.status,
    records_processed: currentJob.records_processed,
    records_total: currentJob.records_total || 0,
    progress_percentage: 0,
  }

  const formatFileSize = (bytes: number) => {
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    if (bytes === 0) return '0 Byte'
    const i = Math.floor(Math.log(bytes) / Math.log(1024))
    return Math.round((bytes / Math.pow(1024, i)) * 100) / 100 + ' ' + sizes[i]
  }

  const formatDate = (dateString: string | undefined) => {
    return dateString ? new Date(dateString).toLocaleString() : 'N/A'
  }

  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case 'completed':
        return 'success'
      case 'failed':
      case 'cancelled':
        return 'error'
      case 'processing':
      case 'analyzing':
        return 'info'
      case 'pending':
        return 'warning'
      default:
        return 'default'
    }
  }

  return (
    <Box>
      <Box display="flex" alignItems="center" mb={3}>
        <Button
          startIcon={<ArrowBackIcon />}
          onClick={() => navigate('/')}
          sx={{ mr: 2 }}
        >
          Back to Dashboard
        </Button>
        <Typography variant="h4" component="h1">
          Job Details
        </Typography>
        <Button
          startIcon={<TimelineIcon />}
          onClick={() => navigate(`/flow/${jobId}`)}
          sx={{ ml: 'auto' }}
          variant="outlined"
        >
          View Process Flow
        </Button>
      </Box>

      <Grid container spacing={3}>
        {/* Job Information */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Job Information
              </Typography>
              <Box display="flex" flexDirection="column" gap={2}>
                <Box>
                  <Typography variant="body2" color="text.secondary">
                    Job ID
                  </Typography>
                  <Typography variant="body1" fontFamily="monospace">
                    {currentJob.job_id}
                  </Typography>
                </Box>

                <Box>
                  <Typography variant="body2" color="text.secondary">
                    File Name
                  </Typography>
                  <Typography variant="body1">
                    {currentJob.filename}
                  </Typography>
                </Box>

                <Box>
                  <Typography variant="body2" color="text.secondary">
                    File Size
                  </Typography>
                  <Typography variant="body1">
                    {formatFileSize(currentJob.file_size)}
                  </Typography>
                </Box>

                <Box>
                  <Typography variant="body2" color="text.secondary">
                    Format & Destination
                  </Typography>
                  <Box display="flex" gap={1} mt={0.5}>
                    <Chip
                      label={currentJob.file_format.toUpperCase()}
                      size="small"
                      variant="outlined"
                    />
                    <Chip
                      label={currentJob.destination_type}
                      size="small"
                      color="primary"
                      variant="outlined"
                    />
                  </Box>
                </Box>

                <Box>
                  <Typography variant="body2" color="text.secondary">
                    Status
                  </Typography>
                  <Chip
                    label={status.status}
                    color={getStatusColor(status.status) as any}
                    sx={{ mt: 0.5 }}
                  />
                </Box>
              </Box>
            </CardContent>
          </Card>
        </Grid>

        {/* Progress Information */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Processing Progress
              </Typography>
              <Box display="flex" flexDirection="column" gap={2}>
                <Box>
                  <Typography variant="body2" color="text.secondary" gutterBottom>
                    Overall Progress
                  </Typography>
                  <LinearProgress
                    variant="determinate"
                    value={status.progress_percentage}
                    sx={{ height: 8, borderRadius: 4 }}
                  />
                  <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                    {Math.round(status.progress_percentage)}%
                  </Typography>
                </Box>

                <Box>
                  <Typography variant="body2" color="text.secondary">
                    Records Processed
                  </Typography>
                  <Typography variant="h6">
                    {status.records_processed.toLocaleString()}
                    {status.records_total > 0 && (
                      <Typography component="span" variant="body2" color="text.secondary">
                        {' / ' + status.records_total.toLocaleString()}
                      </Typography>
                    )}
                  </Typography>
                </Box>

                {status.current_stage && (
                  <Box>
                    <Typography variant="body2" color="text.secondary">
                      Current Stage
                    </Typography>
                    <Typography variant="body1">
                      {status.current_stage}
                    </Typography>
                  </Box>
                )}

                {status.error_message && (
                  <Alert severity="error" size="small">
                    {status.error_message}
                  </Alert>
                )}
              </Box>
            </CardContent>
          </Card>
        </Grid>

        {/* Timestamps */}
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Timestamps
              </Typography>
              <Grid container spacing={3}>
                <Grid item xs={12} sm={6} md={3}>
                  <Typography variant="body2" color="text.secondary">
                    Created
                  </Typography>
                  <Typography variant="body1">
                    {formatDate(currentJob.created_at)}
                  </Typography>
                </Grid>
                <Grid item xs={12} sm={6} md={3}>
                  <Typography variant="body2" color="text.secondary">
                    Started
                  </Typography>
                  <Typography variant="body1">
                    {formatDate(currentJob.started_at)}
                  </Typography>
                </Grid>
                <Grid item xs={12} sm={6} md={3}>
                  <Typography variant="body2" color="text.secondary">
                    Last Updated
                  </Typography>
                  <Typography variant="body1">
                    {formatDate(currentJob.updated_at)}
                  </Typography>
                </Grid>
                <Grid item xs={12} sm={6} md={3}>
                  <Typography variant="body2" color="text.secondary">
                    Completed
                  </Typography>
                  <Typography variant="body1">
                    {formatDate(currentJob.completed_at)}
                  </Typography>
                </Grid>
              </Grid>
            </CardContent>
          </Card>
        </Grid>

        {/* Generated Scripts */}
        {(currentJob.ddl_script || currentJob.etl_script) && (
          <Grid item xs={12}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Generated Scripts
                </Typography>

                {currentJob.ddl_script && (
                  <Box mb={3}>
                    <Typography variant="subtitle1" gutterBottom>
                      DDL Script
                    </Typography>
                    <Paper variant="outlined" sx={{ p: 2, backgroundColor: 'grey.50' }}>
                      <Typography
                        component="pre"
                        variant="body2"
                        fontFamily="monospace"
                        sx={{ whiteSpace: 'pre-wrap', margin: 0 }}
                      >
                        {currentJob.ddl_script}
                      </Typography>
                    </Paper>
                  </Box>
                )}

                {currentJob.etl_script && (
                  <Box>
                    <Typography variant="subtitle1" gutterBottom>
                      ETL Script
                    </Typography>
                    <Paper variant="outlined" sx={{ p: 2, backgroundColor: 'grey.50' }}>
                      <Typography
                        component="pre"
                        variant="body2"
                        fontFamily="monospace"
                        sx={{ whiteSpace: 'pre-wrap', margin: 0 }}
                      >
                        {currentJob.etl_script}
                      </Typography>
                    </Paper>
                  </Box>
                )}
              </CardContent>
            </Card>
          </Grid>
        )}
      </Grid>
    </Box>
  )
}