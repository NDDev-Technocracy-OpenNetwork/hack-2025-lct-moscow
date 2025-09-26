import React, { useEffect } from 'react'
import {
  Box,
  Grid,
  Card,
  CardContent,
  Typography,
  Button,
  Chip,
  LinearProgress,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  IconButton,
  Tooltip,
} from '@mui/material'
import {
  Upload as UploadIcon,
  Visibility as ViewIcon,
  Cancel as CancelIcon,
  Refresh as RefreshIcon,
  PlayArrow as PlayIcon,
} from '@mui/icons-material'
import { useNavigate } from 'react-router-dom'
import { useJobStore } from '../store/jobStore'
import { Job } from '../types'

export const Dashboard: React.FC = () => {
  const navigate = useNavigate()
  const {
    jobs,
    jobStatuses,
    isLoading,
    error,
    fetchJobs,
    cancelJob,
    clearError,
  } = useJobStore()

  useEffect(() => {
    fetchJobs()
  }, [fetchJobs])

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

  const getStatusIcon = (status: string) => {
    switch (status.toLowerCase()) {
      case 'processing':
      case 'analyzing':
        return <PlayIcon fontSize="small" />
      default:
        return null
    }
  }

  const formatFileSize = (bytes: number) => {
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    if (bytes === 0) return '0 Byte'
    const i = Math.floor(Math.log(bytes) / Math.log(1024))
    return Math.round((bytes / Math.pow(1024, i)) * 100) / 100 + ' ' + sizes[i]
  }

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleString()
  }

  const getProgress = (job: Job) => {
    const status = jobStatuses[job.job_id]
    if (status && status.records_total > 0) {
      return (status.records_processed / status.records_total) * 100
    }
    return 0
  }

  const handleCancelJob = async (jobId: string) => {
    if (window.confirm('Are you sure you want to cancel this job?')) {
      await cancelJob(jobId)
    }
  }

  const getJobStats = () => {
    const total = jobs.length
    const completed = jobs.filter(job => job.status === 'completed').length
    const processing = jobs.filter(job => ['processing', 'analyzing'].includes(job.status)).length
    const failed = jobs.filter(job => ['failed', 'cancelled'].includes(job.status)).length

    return { total, completed, processing, failed }
  }

  const stats = getJobStats()

  if (error) {
    return (
      <Box>
        <Typography color="error" gutterBottom>
          Error: {error}
        </Typography>
        <Button onClick={clearError} variant="contained">
          Clear Error
        </Button>
      </Box>
    )
  }

  return (
    <Box>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Typography variant="h4" component="h1">
          BigData Processing Dashboard
        </Typography>
        <Box>
          <Button
            variant="outlined"
            startIcon={<RefreshIcon />}
            onClick={fetchJobs}
            sx={{ mr: 2 }}
            disabled={isLoading}
          >
            Refresh
          </Button>
          <Button
            variant="contained"
            startIcon={<UploadIcon />}
            onClick={() => navigate('/upload')}
          >
            Upload File
          </Button>
        </Box>
      </Box>

      {/* Statistics Cards */}
      <Grid container spacing={3} mb={3}>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Total Jobs
              </Typography>
              <Typography variant="h4" component="div">
                {stats.total}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Completed
              </Typography>
              <Typography variant="h4" component="div" color="success.main">
                {stats.completed}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Processing
              </Typography>
              <Typography variant="h4" component="div" color="info.main">
                {stats.processing}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Failed
              </Typography>
              <Typography variant="h4" component="div" color="error.main">
                {stats.failed}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Jobs Table */}
      <Card>
        <CardContent>
          <Typography variant="h6" gutterBottom>
            Recent Jobs
          </Typography>

          {isLoading && <LinearProgress sx={{ mb: 2 }} />}

          <TableContainer component={Paper} variant="outlined">
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>File</TableCell>
                  <TableCell>Format</TableCell>
                  <TableCell>Size</TableCell>
                  <TableCell>Destination</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Progress</TableCell>
                  <TableCell>Created</TableCell>
                  <TableCell align="right">Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {jobs.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={8} align="center">
                      <Typography color="textSecondary" py={4}>
                        No jobs found. Upload a file to get started.
                      </Typography>
                    </TableCell>
                  </TableRow>
                ) : (
                  jobs.map((job) => {
                    const progress = getProgress(job)
                    const status = jobStatuses[job.job_id]

                    return (
                      <TableRow key={job.job_id} hover>
                        <TableCell>
                          <Typography variant="body2" fontWeight="medium">
                            {job.filename}
                          </Typography>
                          <Typography variant="caption" color="textSecondary">
                            {job.job_id.substring(0, 8)}...
                          </Typography>
                        </TableCell>
                        <TableCell>
                          <Chip
                            label={job.file_format.toUpperCase()}
                            size="small"
                            variant="outlined"
                          />
                        </TableCell>
                        <TableCell>{formatFileSize(job.file_size)}</TableCell>
                        <TableCell>
                          <Chip
                            label={job.destination_type}
                            size="small"
                            color="primary"
                            variant="outlined"
                          />
                        </TableCell>
                        <TableCell>
                          <Chip
                            icon={getStatusIcon(job.status)}
                            label={job.status}
                            size="small"
                            color={getStatusColor(job.status) as any}
                          />
                        </TableCell>
                        <TableCell>
                          <Box display="flex" alignItems="center" gap={1}>
                            {progress > 0 && (
                              <>
                                <LinearProgress
                                  variant="determinate"
                                  value={progress}
                                  sx={{ width: 80, height: 6 }}
                                />
                                <Typography variant="caption">
                                  {Math.round(progress)}%
                                </Typography>
                              </>
                            )}
                            {status && (
                              <Typography variant="caption" color="textSecondary">
                                {status.records_processed} / {status.records_total || 0}
                              </Typography>
                            )}
                          </Box>
                        </TableCell>
                        <TableCell>
                          <Typography variant="body2">
                            {formatDate(job.created_at)}
                          </Typography>
                        </TableCell>
                        <TableCell align="right">
                          <Tooltip title="View Details">
                            <IconButton
                              size="small"
                              onClick={() => navigate(`/job/${job.job_id}`)}
                            >
                              <ViewIcon />
                            </IconButton>
                          </Tooltip>
                          <Tooltip title="View Flow">
                            <IconButton
                              size="small"
                              onClick={() => navigate(`/flow/${job.job_id}`)}
                            >
                              <PlayIcon />
                            </IconButton>
                          </Tooltip>
                          {['pending', 'processing', 'analyzing'].includes(job.status) && (
                            <Tooltip title="Cancel Job">
                              <IconButton
                                size="small"
                                onClick={() => handleCancelJob(job.job_id)}
                                color="error"
                              >
                                <CancelIcon />
                              </IconButton>
                            </Tooltip>
                          )}
                        </TableCell>
                      </TableRow>
                    )
                  })
                )}
              </TableBody>
            </Table>
          </TableContainer>
        </CardContent>
      </Card>
    </Box>
  )
}