import React, { useState } from 'react'
import {
  Box,
  Paper,
  Typography,
  Button,
  Alert,
  CircularProgress,
} from '@mui/material'
import { CloudUpload } from '@mui/icons-material'
import { useNavigate } from 'react-router-dom'
import { useDropzone } from 'react-dropzone'
import { useJobStore } from '../store/jobStore'

export const FileUpload: React.FC = () => {
  const navigate = useNavigate()
  const { uploadFile, isLoading, error, clearError } = useJobStore()
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [uploadProgress, setUploadProgress] = useState(0)

  const onDrop = (acceptedFiles: File[]) => {
    if (acceptedFiles.length > 0) {
      setSelectedFile(acceptedFiles[0])
      clearError()
    }
  }

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'text/csv': ['.csv'],
      'application/json': ['.json'],
      'text/xml': ['.xml'],
      'application/xml': ['.xml'],
    },
    maxFiles: 1,
    maxSize: 10 * 1024 * 1024 * 1024, // 10GB
  })

  const handleUpload = async () => {
    if (!selectedFile) return

    const destinationType = 'postgresql' // Default for MVP
    const destinationConfig = {
      host: 'postgres',
      port: 5432,
      database: 'bigdata_db',
      schema_name: 'public',
      table_name: selectedFile.name.split('.')[0].toLowerCase().replace(/[^a-z0-9_]/g, '_'),
      username: 'bigdata_user',
      password: 'bigdata_pass',
    }

    const jobId = await uploadFile(selectedFile, destinationType, destinationConfig)

    if (jobId) {
      navigate(`/job/${jobId}`)
    }
  }

  const formatFileSize = (bytes: number) => {
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    if (bytes === 0) return '0 Byte'
    const i = Math.floor(Math.log(bytes) / Math.log(1024))
    return Math.round((bytes / Math.pow(1024, i)) * 100) / 100 + ' ' + sizes[i]
  }

  return (
    <Box maxWidth="md" mx="auto">
      <Typography variant="h4" component="h1" gutterBottom>
        Upload File for Processing
      </Typography>

      <Paper sx={{ p: 4, mb: 3 }}>
        <Box
          {...getRootProps()}
          sx={{
            border: '2px dashed',
            borderColor: isDragActive ? 'primary.main' : 'grey.300',
            borderRadius: 2,
            p: 6,
            textAlign: 'center',
            cursor: 'pointer',
            transition: 'all 0.3s ease',
            '&:hover': {
              borderColor: 'primary.main',
              backgroundColor: 'action.hover',
            },
          }}
        >
          <input {...getInputProps()} />
          <CloudUpload sx={{ fontSize: 60, color: 'text.secondary', mb: 2 }} />

          {selectedFile ? (
            <Box>
              <Typography variant="h6" gutterBottom>
                Selected File:
              </Typography>
              <Typography variant="body1" fontWeight="medium">
                {selectedFile.name}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                {formatFileSize(selectedFile.size)} • {selectedFile.type || 'Unknown type'}
              </Typography>
            </Box>
          ) : isDragActive ? (
            <Typography variant="h6">
              Drop the file here...
            </Typography>
          ) : (
            <Box>
              <Typography variant="h6" gutterBottom>
                Drag & drop a file here, or click to select
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Supported formats: CSV, JSON, XML • Max size: 10GB
              </Typography>
            </Box>
          )}
        </Box>

        {error && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {error}
          </Alert>
        )}

        <Box mt={3} display="flex" gap={2}>
          <Button
            variant="contained"
            onClick={handleUpload}
            disabled={!selectedFile || isLoading}
            startIcon={isLoading ? <CircularProgress size={20} /> : <CloudUpload />}
          >
            {isLoading ? 'Uploading...' : 'Upload & Process'}
          </Button>

          <Button
            variant="outlined"
            onClick={() => navigate('/')}
          >
            Back to Dashboard
          </Button>
        </Box>
      </Paper>

      {/* Instructions */}
      <Paper sx={{ p: 3 }}>
        <Typography variant="h6" gutterBottom>
          Processing Information
        </Typography>
        <Typography variant="body2" paragraph>
          • Files will be processed automatically using streaming ETL
        </Typography>
        <Typography variant="body2" paragraph>
          • Data will be loaded into PostgreSQL by default
        </Typography>
        <Typography variant="body2" paragraph>
          • You can monitor real-time progress on the job details page
        </Typography>
        <Typography variant="body2">
          • Large files (up to 10GB) are supported with memory-efficient processing
        </Typography>
      </Paper>
    </Box>
  )
}