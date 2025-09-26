import { io, Socket } from 'socket.io-client'
import { WebSocketMessage } from '../types'

const WS_BASE_URL = import.meta.env.VITE_WS_URL || 'http://localhost:8000'

class WebSocketService {
  private socket: Socket | null = null
  private clientId: string
  private reconnectAttempts = 0
  private maxReconnectAttempts = 5
  private listeners = new Map<string, Set<(data: any) => void>>()

  constructor() {
    this.clientId = this.generateClientId()
  }

  private generateClientId(): string {
    return 'client_' + Math.random().toString(36).substr(2, 9) + '_' + Date.now()
  }

  connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        if (this.socket?.connected) {
          resolve()
          return
        }

        console.log('Connecting to WebSocket...', `${WS_BASE_URL}/ws/${this.clientId}`)

        this.socket = io(WS_BASE_URL, {
          transports: ['websocket', 'polling'],
          timeout: 10000,
          forceNew: true,
        })

        this.socket.on('connect', () => {
          console.log('WebSocket connected:', this.socket?.id)
          this.reconnectAttempts = 0
          resolve()
        })

        this.socket.on('disconnect', (reason) => {
          console.log('WebSocket disconnected:', reason)
          if (reason === 'io server disconnect') {
            // Server disconnected, try to reconnect
            this.attemptReconnect()
          }
        })

        this.socket.on('connect_error', (error) => {
          console.error('WebSocket connection error:', error)
          this.attemptReconnect()
          reject(error)
        })

        this.socket.on('message', (data: WebSocketMessage) => {
          console.log('WebSocket message received:', data)
          this.handleMessage(data)
        })

        // Handle job-specific messages
        this.socket.on('job_update', (data: WebSocketMessage) => {
          console.log('Job update received:', data)
          this.handleMessage(data)
        })

        this.socket.on('progress_update', (data: WebSocketMessage) => {
          console.log('Progress update received:', data)
          this.handleMessage(data)
        })

      } catch (error) {
        console.error('Error creating WebSocket connection:', error)
        reject(error)
      }
    })
  }

  private attemptReconnect(): void {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.error('Max reconnection attempts reached')
      return
    }

    this.reconnectAttempts++
    const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000) // Exponential backoff, max 30s

    console.log(`Attempting to reconnect in ${delay}ms (attempt ${this.reconnectAttempts})`)

    setTimeout(() => {
      this.connect().catch((error) => {
        console.error('Reconnection failed:', error)
      })
    }, delay)
  }

  private handleMessage(data: WebSocketMessage): void {
    const { type, job_id } = data

    // Notify type-specific listeners
    const typeListeners = this.listeners.get(type)
    if (typeListeners) {
      typeListeners.forEach(listener => listener(data))
    }

    // Notify job-specific listeners
    const jobListeners = this.listeners.get(`job:${job_id}`)
    if (jobListeners) {
      jobListeners.forEach(listener => listener(data))
    }

    // Notify general listeners
    const generalListeners = this.listeners.get('*')
    if (generalListeners) {
      generalListeners.forEach(listener => listener(data))
    }
  }

  subscribeToJob(jobId: string): void {
    if (this.socket?.connected) {
      this.socket.emit('subscribe_job', { job_id: jobId })
      console.log('Subscribed to job:', jobId)
    }
  }

  unsubscribeFromJob(jobId: string): void {
    if (this.socket?.connected) {
      this.socket.emit('unsubscribe_job', { job_id: jobId })
      console.log('Unsubscribed from job:', jobId)
    }
  }

  // Add listener for specific message types
  on(event: string, callback: (data: WebSocketMessage) => void): () => void {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set())
    }
    this.listeners.get(event)!.add(callback)

    // Return unsubscribe function
    return () => {
      const listeners = this.listeners.get(event)
      if (listeners) {
        listeners.delete(callback)
        if (listeners.size === 0) {
          this.listeners.delete(event)
        }
      }
    }
  }

  // Add listener for job-specific updates
  onJobUpdate(jobId: string, callback: (data: WebSocketMessage) => void): () => void {
    return this.on(`job:${jobId}`, callback)
  }

  // Add listener for all messages
  onAll(callback: (data: WebSocketMessage) => void): () => void {
    return this.on('*', callback)
  }

  sendMessage(message: any): void {
    if (this.socket?.connected) {
      this.socket.emit('message', message)
    } else {
      console.warn('WebSocket not connected, cannot send message:', message)
    }
  }

  disconnect(): void {
    if (this.socket) {
      console.log('Disconnecting WebSocket...')
      this.socket.disconnect()
      this.socket = null
    }
    this.listeners.clear()
  }

  isConnected(): boolean {
    return this.socket?.connected || false
  }

  getClientId(): string {
    return this.clientId
  }
}

// Singleton instance
const websocketService = new WebSocketService()

export default websocketService