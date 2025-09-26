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

    // Set up listener for job updates
    websocketService.onJobUpdate(jobId, (message) => {
      console.log('Job update received in store:', message)

      const { data } = message

      // Update job status if available
      if (data.status) {
        get().updateJobStatus(jobId, {
          job_id: jobId,
          status: data.status,
          records_processed: data.records_processed || 0,
          records_total: data.total_records || data.records_total || 0,
          progress_percentage: data.progress_percent || 0,
          current_stage: data.stage,
          error_message: data.error_message
        })
      }

      // Update job details if available
      if (data.stage || data.message) {
        get().updateJob(jobId, {
          status: data.status || 'processing'
        })
      }
    })
  },

  unsubscribeFromJob: (jobId: string) => {
    websocketService.unsubscribeFromJob(jobId)
  },
}))

// Initialize WebSocket connection when store is created
websocketService.connect().catch(error => {
  console.error('Failed to initialize WebSocket connection:', error)
})