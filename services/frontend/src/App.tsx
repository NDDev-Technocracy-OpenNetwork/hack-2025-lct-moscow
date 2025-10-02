import React from 'react'
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom'
import { Container, AppBar, Toolbar, Typography, Box, Button } from '@mui/material'
import { Dashboard } from './pages/Dashboard'
import { FileUpload } from './pages/FileUpload'
import { JobDetails } from './pages/JobDetails'
import { ProcessFlow } from './pages/ProcessFlow'
import { DatabaseSchema } from './pages/DatabaseSchema'
import { PipelineVisualization } from './pages/PipelineVisualization'
import StorageIcon from '@mui/icons-material/Storage'
import AccountTreeIcon from '@mui/icons-material/AccountTree'
import DashboardIcon from '@mui/icons-material/Dashboard'

function App() {
  return (
    <Router>
      <Box sx={{ flexGrow: 1 }}>
        <AppBar position="static">
          <Toolbar>
            <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
              BigData Processing System
            </Typography>
            <Button color="inherit" component={Link} to="/" startIcon={<DashboardIcon />}>
              Dashboard
            </Button>
            <Button color="inherit" component={Link} to="/schema" startIcon={<StorageIcon />}>
              Database Schema
            </Button>
          </Toolbar>
        </AppBar>

        <Container maxWidth="xl" sx={{ mt: 4, mb: 4 }}>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/upload" element={<FileUpload />} />
            <Route path="/job/:jobId" element={<JobDetails />} />
            <Route path="/flow/:jobId" element={<ProcessFlow />} />
            <Route path="/schema" element={<DatabaseSchema />} />
            <Route path="/pipeline/:jobId" element={<PipelineVisualization />} />
          </Routes>
        </Container>
      </Box>
    </Router>
  )
}

export default App// Rebuild 1759324986
