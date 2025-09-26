import React from 'react'
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import { Container, AppBar, Toolbar, Typography, Box } from '@mui/material'
import { Dashboard } from './pages/Dashboard'
import { FileUpload } from './pages/FileUpload'
import { JobDetails } from './pages/JobDetails'
import { ProcessFlow } from './pages/ProcessFlow'

function App() {
  return (
    <Router>
      <Box sx={{ flexGrow: 1 }}>
        <AppBar position="static">
          <Toolbar>
            <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
              BigData Processing System
            </Typography>
          </Toolbar>
        </AppBar>

        <Container maxWidth="xl" sx={{ mt: 4, mb: 4 }}>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/upload" element={<FileUpload />} />
            <Route path="/job/:jobId" element={<JobDetails />} />
            <Route path="/flow/:jobId" element={<ProcessFlow />} />
          </Routes>
        </Container>
      </Box>
    </Router>
  )
}

export default App