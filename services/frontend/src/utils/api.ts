import axios from 'axios'
import { Job, JobStatus, FileUploadResponse } from '../types'

const API_BASE_URL = import.meta.env.VITE_API_URL || window.location.origin

const api = axios.create({
  baseURL: `${API_BASE_URL}/api/v1`,
  timeout: 30000,
})

// Request interceptor
api.interceptors.request.use(
  (config) => {
    console.log('API Request:', config.method?.toUpperCase(), config.url)
    return config
  },
  (error) => {
    console.error('API Request Error:', error)
    return Promise.reject(error)
  }
)

// Response interceptor
api.interceptors.response.use(
  (response) => {
    console.log('API Response:', response.status, response.config.url)
    return response
  },
  (error) => {
    console.error('API Response Error:', error.response?.status, error.response?.data)
    return Promise.reject(error)
  }
)

// Jobs API
export const jobsApi = {
  // Get all jobs
  getAll: (): Promise<Job[]> =>
    api.get('/jobs').then((response) => response.data),

  // Get specific job
  getById: (jobId: string): Promise<Job> =>
    api.get(`/jobs/${jobId}`).then((response) => response.data),

  // Get job status
  getStatus: (jobId: string): Promise<JobStatus> =>
    api.get(`/jobs/${jobId}/status`).then((response) => response.data),

  // Cancel job
  cancel: (jobId: string): Promise<{ message: string }> =>
    api.delete(`/jobs/${jobId}`).then((response) => response.data),

  // Get DDL scripts
  getDDL: (jobId: string): Promise<{ job_id: string; ddl_script: string; etl_script: string }> =>
    api.get(`/jobs/${jobId}/ddl`).then((response) => response.data),

  // Upload file
  upload: (
    file: File,
    destinationType: string,
    destinationConfig: Record<string, any>
  ): Promise<FileUploadResponse> => {
    const formData = new FormData()
    formData.append('file', file)
    formData.append('destination_type', destinationType)
    formData.append('destination_config', JSON.stringify(destinationConfig))

    return api
      .post('/jobs/upload', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
        timeout: 60000, // 1 minute timeout for file upload
        onUploadProgress: (progressEvent) => {
          if (progressEvent.total) {
            const progress = (progressEvent.loaded / progressEvent.total) * 100
            console.log('Upload progress:', Math.round(progress), '%')
          }
        },
      })
      .then((response) => response.data)
  },
}

// Files API
export const filesApi = {
  // Get file metadata
  getMetadata: (jobId: string) =>
    api.get(`/files/${jobId}/metadata`).then((response) => response.data),

  // Get all file metadata
  getAllMetadata: () =>
    api.get('/files').then((response) => response.data),
}

// Destinations API
export const destinationsApi = {
  // Get available destinations
  getAvailable: () =>
    api.get('/destinations/available').then((response) => response.data),

  // Test destination connection
  testConnection: (destinationType: string, config: Record<string, any>) =>
    api
      .post('/destinations/test-connection', {
        destination_type: destinationType,
        config,
      })
      .then((response) => response.data),
}

// Health check
export const healthApi = {
  check: () => api.get('/health', { baseURL: API_BASE_URL }).then((response) => response.data),
}

export { api }
export default api
