#!/usr/bin/env python3
"""
DLT Worker - ETL Processing Service
"""

import os
import sys
import json
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main worker process"""
    logger.info("DLT Worker starting...")

    # Check environment
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Working directory: {os.getcwd()}")
    logger.info(f"Data directory exists: {Path('/app/data').exists()}")

    # Worker ready to process jobs
    logger.info("DLT Worker is ready to process ETL jobs")

    # Keep worker alive
    import time
    while True:
        time.sleep(30)
        logger.info("DLT Worker heartbeat")

if __name__ == "__main__":
    main()