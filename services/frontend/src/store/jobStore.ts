import { create } from 'zustand'
import { Job, JobStatus } from '../types'
import { jobsApi } from '../utils/api'
import websocketService from '../utils/websocket'

interface JobStore {
  jobs: Job[]
  currentJob: Job | null
  jobStatuses: Record<string, JobStatus>
  isLoading: boolean
  error: string | null

  // Actions
  fetchJobs: () => Promise<void>
  fetchJob: (jobId: string) => Promise<Job | null>
  fetchJobStatus: (jobId: string) => Promise<JobStatus | null>
  uploadFile: (file: File, destinationType: string, destinationConfig: Record<string, any>) => Promise<string | null>
  cancelJob: (jobId: string) => Promise<boolean>
  setCurrentJob: (job: Job | null) => void
  updateJobStatus: (jobId: string, status: JobStatus) => void
  addJob: (job: Job) => void
  updateJob: (jobId: string, updates: Partial<Job>) => void
  clearError: () => void

  // WebSocket
  subscribeToJob: (jobId: string) => void
  unsubscribeFromJob: (jobId: string) => void
}

export const useJobStore = create<JobStore>((set, get) => ({
  jobs: [],
  currentJob: null,
  jobStatuses: {},
  isLoading: false,
  error: null,

  fetchJobs: async () => {
    set({ isLoading: true, error: null })
    try {
      const jobs = await jobsApi.getAll()
      set({ jobs, isLoading: false })
    } catch (error: any) {
      set({
        error: error.response?.data?.detail || error.message || 'Failed to fetch jobs',
        isLoading: false
      })
    }
  },

  fetchJob: async (jobId: string) => {
    set({ isLoading: true, error: null })
    try {
      const job = await jobsApi.getById(jobId)
      set({ currentJob: job, isLoading: false })

      // Update job in jobs array if it exists
      const state = get()
      const jobIndex = state.jobs.findIndex(j => j.job_id === jobId)
      if (jobIndex >= 0) {
        const updatedJobs = [...state.jobs]
        updatedJobs[jobIndex] = job
        set({ jobs: updatedJobs })
      }

      return job
    } catch (error: any) {
      set({
        error: error.response?.data?.detail || error.message || 'Failed to fetch job',
        isLoading: false
      })
      return null
    }
  },

  fetchJobStatus: async (jobId: string) => {
    try {
      const status = await jobsApi.getStatus(jobId)
      set(state => ({
        jobStatuses: { ...state.jobStatuses, [jobId]: status }
      }))
      return status
    } catch (error: any) {
      console.error('Failed to fetch job status:', error)
      return null
    }
  },

  uploadFile: async (file: File, destinationType: string, destinationConfig: Record<string, any>) => {
    set({ isLoading: true, error: null })
    try {
      const response = await jobsApi.upload(file, destinationType, destinationConfig)

      // Create a new job object from the response
      const newJob: Job = {
        id: 0, // Will be updated when we fetch the actual job
        job_id: response.job_id,
        filename: response.filename,
        file_size: response.file_size,
        file_format: response.file_format,
        destination_type: destinationType,
        destination_config: destinationConfig,
        status: 'pending',
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        records_processed: 0,
      }

      // Add the new job to the store
      set(state => ({
        jobs: [newJob, ...state.jobs],
        isLoading: false
      }))

      // Subscribe to job updates
      get().subscribeToJob(response.job_id)

      return response.job_id
    } catch (error: any) {
      set({
        error: error.response?.data?.detail || error.message || 'Failed to upload file',
        isLoading: false
      })
      return null
    }
  },

  cancelJob: async (jobId: string) => {
    try {
      await jobsApi.cancel(jobId)

      // Update job status in store
      set(state => ({
        jobs: state.jobs.map(job =>
          job.job_id === jobId
            ? { ...job, status: 'cancelled', updated_at: new Date().toISOString() }
            : job
        ),
        jobStatuses: {
          ...state.jobStatuses,
          [jobId]: { ...state.jobStatuses[jobId], status: 'cancelled' }
        }
      }))

      return true
    } catch (error: any) {
      set({ error: error.response?.data?.detail || error.message || 'Failed to cancel job' })
      return false
    }
  },

  setCurrentJob: (job: Job | null) => {
    set({ currentJob: job })
  },

  updateJobStatus: (jobId: string, status: JobStatus) => {
    set(state => ({
      jobStatuses: { ...state.jobStatuses, [jobId]: status }
    }))

    // Also update the job in the jobs array
    set(state => ({
      jobs: state.jobs.map(job =>
        job.job_id === jobId
          ? {
              ...job,
              status: status.status,
              records_processed: status.records_processed,
              records_total: status.records_total,
              error_message: status.error_message,
              updated_at: new Date().toISOString()
            }
          : job
      )
    }))
  },

  addJob: (job: Job) => {
    set(state => ({
      jobs: [job, ...state.jobs.filter(j => j.job_id !== job.job_id)]
    }))
  },

  updateJob: (jobId: string, updates: Partial<Job>) => {
    set(state => ({
      jobs: state.jobs.map(job =>
        job.job_id === jobId
          ? { ...job, ...updates, updated_at: new Date().toISOString() }
          : job
      ),
      currentJob: state.currentJob?.job_id === jobId
        ? { ...state.currentJob, ...updates, updated_at: new Date().toISOString() }
        : state.currentJob
    }))
  },

  clearError: () => {
    set({ error: null })
  },

  subscribeToJob: (jobId: string) => {
    websocketService.subscribeToJob(jobId)

    // Set up listener for progress_update events
    websocketService.on('progress_update', (message) => {
      if (message.job_id === jobId || message.data?.job_id === jobId) {
        console.log('[Store] Progress update for job:', jobId, message)

        const data = message.data || message

        // Update job status with real-time data
        get().updateJobStatus(jobId, {
          job_id: jobId,
          status: data.status || 'processing',
          records_processed: data.records_processed || 0,
          records_total: data.total_records || data.records_total || 0,
          progress_percentage: data.progress_percent || data.progress_percentage || 0,
          current_stage: data.stage || data.current_stage,
          error_message: data.error_message
        })

        // Update job in jobs list and currentJob
        const jobUpdates: Partial<Job> = {
          status: data.status || 'processing',
          records_processed: data.records_processed || 0,
          records_total: data.total_records || data.records_total || 0
        }

        // If DDL/ETL scripts are in the message, save them
        if (data.ddl_script) {
          jobUpdates.ddl_script = data.ddl_script
        }
        if (data.etl_script) {
          jobUpdates.etl_script = data.etl_script
        }

        get().updateJob(jobId, jobUpdates)
      }
    })

    // Listen for job status updates (complete job information)
    websocketService.on('job_status_update', (message) => {
      if (message.data?.job_id === jobId) {
        console.log('[Store] Job status update for:', jobId, message)
        const data = message.data

        // Update job with complete information
        get().updateJob(jobId, {
          status: data.status,
          records_processed: data.records_processed,
          started_at: data.started_at,
          completed_at: data.completed_at,
          error_message: data.error_message
        })

        // Update job status
        get().updateJobStatus(jobId, {
          job_id: jobId,
          status: data.status,
          records_processed: data.records_processed || 0,
          records_total: data.records_processed || 0,
          progress_percentage: data.status === 'completed' ? 100 : 0,
          current_stage: data.status === 'completed' ? 'Completed' : undefined,
          error_message: data.error_message
        })

        // If DDL/ETL are available, trigger a refresh
        if (data.has_ddl || data.has_etl) {
          console.log('[Store] DDL/ETL scripts available, refreshing job data')
          setTimeout(() => get().fetchJob(jobId), 500)
        }
      }
    })

    // Also listen for job-specific updates
    websocketService.onJobUpdate(jobId, (message) => {
      console.log('[Store] Job update for:', jobId, message)
      const data = message.data || message

      // If this is a DDL/ETL availability notification
      if (data.ddl_available || data.etl_available) {
        console.log('[Store] DDL/ETL scripts generated, refreshing job data')
        setTimeout(() => get().fetchJob(jobId), 500)
        return
      }

      get().updateJobStatus(jobId, {
        job_id: jobId,
        status: data.status || 'processing',
        records_processed: data.records_processed || 0,
        total_records: data.total_records || data.records_total || 0,
        progress_percentage: data.progress_percent || data.progress_percentage || 0,
        current_stage: data.stage || data.current_stage,
        error_message: data.error_message
      })
    })
  },

  unsubscribeFromJob: (jobId: string) => {
    websocketService.unsubscribeFromJob(jobId)
  },
}))

// Initialize WebSocket connection immediately when store is created
const connectPromise = websocketService.connect()
if (connectPromise) {
  connectPromise.catch(error => {
    console.error('Failed to initialize WebSocket connection:', error)
  })
}
