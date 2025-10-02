import { io, Socket } from 'socket.io-client'
import { WebSocketMessage } from '../types'

const WS_BASE_URL = import.meta.env.VITE_WS_URL || window.location.origin

class WebSocketService {
  private socket: Socket | null = null
  private clientId: string
  private reconnectAttempts = 0
  private maxReconnectAttempts = 5
  private listeners = new Map<string, Set<(data: any) => void>>()
  private pendingSubscriptions: Set<string> = new Set()
  private connectionPromise: Promise<void> | null = null

  constructor() {
    this.clientId = this.generateClientId()
  }

  private generateClientId(): string {
    return 'client_' + Math.random().toString(36).substr(2, 9) + '_' + Date.now()
  }

  connect(): Promise<void> | null {
    if (this.socket?.connected) {
      return null
    }

    if (this.connectionPromise) {
      return this.connectionPromise
    }

    this.connectionPromise = new Promise((resolve, reject) => {
      try {

        console.log('[WS] Connecting to:', WS_BASE_URL)

        this.socket = io(WS_BASE_URL, {
          path: '/socket.io/',
          transports: ['websocket', 'polling'],
          timeout: 10000,
          reconnection: true,
          reconnectionAttempts: 5,
          reconnectionDelay: 1000,
        })

        this.socket.on('connect', () => {
          console.log('[WS] Connected:', this.socket?.id)
          this.reconnectAttempts = 0
          this.connectionPromise = null

          // Re-subscribe to all pending subscriptions
          this.pendingSubscriptions.forEach(jobId => {
            if (this.socket?.connected) {
              this.socket.emit('subscribe_job', { job_id: jobId })
              console.log('[WS] Re-subscribed to job:', jobId)
            }
          })

          resolve()
        })

        this.socket.on('disconnect', (reason) => {
          console.log('[WS] Disconnected:', reason)
          this.connectionPromise = null
          if (reason === 'io server disconnect') {
            this.attemptReconnect()
          }
        })

        this.socket.on('connect_error', (error) => {
          console.log('[WS] Connection error:', error.message)
          this.connectionPromise = null
          if (this.reconnectAttempts === 0) {
            reject(error)
          }
          this.attemptReconnect()
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
      console.log('[WS] Max reconnection attempts reached')
      return
    }

    this.reconnectAttempts++
    const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000)

    console.log(`[WS] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`)

    setTimeout(() => {
      this.connect().catch((error) => {
        console.log('[WS] Reconnection failed:', error.message)
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
    this.pendingSubscriptions.add(jobId)

    if (this.socket?.connected) {
      this.socket.emit('subscribe_job', { job_id: jobId })
      console.log('[WS] Subscribed to job:', jobId)
    } else {
      console.log('[WS] Queued subscription for job:', jobId)
      const connectPromise = this.connect()
      if (connectPromise) {
        connectPromise.then(() => {
          if (this.socket?.connected && this.pendingSubscriptions.has(jobId)) {
            this.socket.emit('subscribe_job', { job_id: jobId })
            console.log('[WS] Deferred subscription sent for job:', jobId)
          }
        }).catch(err => {
          console.error('[WS] Failed to connect for subscription:', err)
        })
      }
    }
  }

  unsubscribeFromJob(jobId: string): void {
    this.pendingSubscriptions.delete(jobId)

    if (this.socket?.connected) {
      this.socket.emit('unsubscribe_job', { job_id: jobId })
      console.log('[WS] Unsubscribed from job:', jobId)
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

export default websocketService// Build timestamp: Wed, Oct  1, 2025  6:29:18 AM
